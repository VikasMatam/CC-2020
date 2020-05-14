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


http_count = 0;
@app.route("/api/v1/users")
def adduser():
	global http_count
	http_count=http_count+1
	if request.method == "PUT":
		uname = request.get_json()["username"]
		passwd = request.get_json()["password"]
		flag = User.query.filter_by(username=uname).first()
		if(flag is not None):
			return jsonify({"flag":"user exists"}),400
		if(re.match(r'^[0-9a-fA-F]{40}$',passwd) is None):
			return jsonify({"flag":"password missmatch"}),400

		"""r = requests.post("http://3.233.19.138/write/adduser",json = request.get_json())
		return jsonify(r.json())"""
		db.session.add(User(username=request.get_json()["username"],password=request.get_json()["password"]))
		db.session.commit()
		return {},201

	else:
		return {},405
	

#I wouldn't allow to delete user who has created atleast 1 ride.
@app.route("/api/v1/users/<uname>")
def removeuser(uname):
	global http_count
	http_count=http_count+1
	if request.method == "DELETE":
		try:
			#you cann't delete the guy who has created the ride.
			#rflag = Ride.query.filter_by(created_by=uname).first()
			uflag = User.query.filter_by(username=uname).first()
			if uflag is None:
				return jsonify({}),400

			is_creator =requests.get("http://34.200.182.72:80/api/v1/db/read/{}".format(uname),json={})
			if uname == is_creator.text:
				return jsonify({"flag":"user has created rides"}),400

			db.session.delete(uflag)
			#ride_user.query.filter_by(username=uname).delete()
			db.session.commit()
			
			return jsonify({}),200
			
		except KeyError:
			return {},400
		except:
			return {},500
	else:
		return {},405


@app.route("/api/v1/users")
def listUsers():
	global http_count
	http_count=http_count+1
	if request.method == "GET":
		query = db.session.execute("select username from User")
		l=[]
		
		for i in query:
			l.append(i[0])
		
		if len(l) == 0:
			return {},204
		return jsonify(l),200
	else:
		return {},405

@app.route("/api/v1/db/clear",methods=["GET","POST","DELETE"])
def clearDB():
	if request.method == "POST":
		is_empty=not(bool(request.json))
		if(is_empty):
			User.query.delete()	# User DB clearing
			#Ride.query.delete()	# Ride DB clearing
			#ride_user.query.delete() # Ride DB Clearing
			db.session.commit()
			return {},200
		else:
			return {},400
	else:
		return {},405

@app.route("/api/v1/_count")
def count():
	global http_count
	if request.method=="GET":
		l=[]
		l.append(http_count)
		return jsonify(l),200
	else:
		return {},405

@app.route("/api/v1/_count")
def reset():
	global http_count
	if request.method == "DELETE":
		http_count=0
		return {},200
	else:
		return {},405


if __name__=='__main__':
	db.create_all()
	app.run(host='0.0.0.0',port=8080,debug=True)

