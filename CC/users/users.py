from flask import Flask,render_template,jsonify,abort,request
from flask_sqlalchemy import SQLAlchemy

import enum
import requests
import re
import json



app = Flask(__name__)


app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
db = SQLAlchemy(app)


class User(db.Model):
        username = db.Column(db.String, unique=True, nullable=False, primary_key=True)
        password = db.Column(db.String(40),nullable=False)

        def __repr__(self):
                return "\n{username : %s , password : %s}" % (self.username,self.password)

class Counter(db.Model):
	c_id = db.Column(db.Integer, primary_key=True)
	c_val = db.Column(db.Integer)


http_count = 0
#databse write operation
@app.route("/api/v1/db/write",methods=["POST","DELETE"])
def write():
	if request.method == "POST":
		data=request.get_json()["insert"]
		column=request.get_json()["column"]
		table=request.get_json()["table"]
		data1=[]
		for i in data:
		    if type(i) is str:
		        print(type(i))
		        data1.append("\'"+i+"\'")
		    else:
		        data1.append(str(i))
		#data=list(map(lambda x:'\''+x+'\'' if isinstance(x,str),data))	#inserting single qoute '' to data
		column=",".join(column)	#combining column with join
		data1=",".join(data1)#combining data with join
		try:
			qry="insert into "+table+"("+column+") values("+data1+");"
			db.session.execute(qry)
			db.session.commit()
			return {}
		except Exception as e:
			print(e)
			abort(400)
	elif request.method == "DELETE":
		table=request.get_json()["table"]
		where=request.get_json()["where"]
		qry="delete from "+table+" where "+where+";"       
		try:
			db.session.execute(qry)
			db.session.commit()
			return {}
		except Exception as e:
			print(e)
			abort(400)
	else:
		abort(405)

@app.route("/api/v1/db/read",methods=["POST","DELETE"])
def read():
	if request.method=="POST":	#to read from data
		a={}
		table=request.get_json()["table"]
		column=request.get_json()["columns"]
		where=request.get_json()["where"]
		column=",".join(column)
		qry="select "+column+" from "+table+ " where "+where+ ";"
		print(qry)
		try:
			result=db.session.execute(qry)
			count=0
			for i in result:
				a[count]=list(i)
				count+=1
			return jsonify(a)
		except Exception as e:
			print(e)
			abort(400)
	else:
		abort(405)

@app.route("/api/v1/users",methods=["PUT","GET","POST","DELETE"])
def adduser():
	global http_count
	http_count=http_count+1
	"""
	count = Counter.query.filter_by(c_id=1).first()
	count.c_val = Counter.c_val + 1
	db.session.commit()
	"""
	if request.method == "PUT":
		uname = request.get_json()["username"]
		passwd = request.get_json()["password"]
		if(re.match(r'^[0-9a-fA-F]{40}$',passwd) is None):
			return jsonify({"flag":"password missmatch"}),400
		u_req=requests.get("http://3.222.161.224/api/v1/users",json={})
		if(uname in u_req.text):
			return jsonify({"flag":"user already exists"}),400
		
		a={"insert":[uname,passwd],"column":["username","password"],"table":"User"}
		resp=requests.post("http://18.209.199.196/api/v1/db/write",json=a)
		if resp.status_code==200:
			return {},201
		elif resp.status_code==400:
			return {'flag':'couldnot insert into db'},400
	#list user api        
	elif request.method == "GET":
		print("entered list user api")
		a={"table":"User","columns":["username"],"where":"username like '%'"}
		resp1=requests.post("http://18.209.199.196/api/v1/db/read",json=a)
		print("response from DBaaS %s " % resp1.json())
		users=resp1.json()
		if(len(users)<=0):
			return {},204
		query=list(users.values())
		l =[]
		for i in query:
			l.append(i[0])
		return jsonify(l),200
	else:
		return {},405


#preforms cascade delete
@app.route("/api/v1/users/<uname>",methods=["DELETE","PUT","GET","POST"])
def removeuser(uname):
	global http_count
	http_count=http_count+1
	"""
	count = Counter.query.filter_by(c_id=1).first()
	count.c_val = Counter.c_val + 1
	db.session.commit()
	"""
	if request.method == "DELETE":
		try:
			a={"table":"User","columns":["username"],"where":"username='"+uname+"'"}
			req=requests.get("http://3.222.161.224/api/v1/users",json={})
			if uname not in req.text:
				return jsonify({"flag":"user doesn't exist"}),400
			resp=requests.delete("http://18.209.199.196/api/v1/db/write",json=a)
			if resp.status_code==200:
				return jsonify({}),200
			else:
				abort(resp.status_code)
		except KeyError:
			return {},400
		except:
			return {},500
	else:
		return {},405

@app.route("/api/v1/db/clear",methods=["POST","GET","PUT","DELETE"])
def clearDB():
	if request.method == "POST":
		is_empty=not(bool(request.json))
		if(is_empty):
			a={"table":"User","columns":["*"],"where":"username like '%'"}
			resp1=requests.delete("http://18.209.199.196/api/v1/db/write",json=a)
			resp2=requests.post("http://34.200.182.72/api/v1/db/clear",json={})
			if((resp1.status_code==200) and (resp2.status_code==200) ):
				return {},200
			else:
				abort(400)
		else:
			return {},400
	else:
		return {},405

#HTTP count and reset api's
@app.route("/api/v1/_count",methods=["GET","PUT","POST","DELETE"])
def count():
	global http_count
	#count = Counter.query.filter_by(c_id=1).first()
	if request.method=="GET":
		l=[]
		l.append(http_count)
		return jsonify(l),200
	elif request.method=="DELETE":
		http_count=0
		#count.c_val = 0
		db.session.commit()
		return {},200
	else:
		return {},405

if __name__=='__main__':
        db.create_all()
        app.run(host='0.0.0.0',port=80,debug=True)
