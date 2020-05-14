from flask import Flask,render_template,jsonify,abort,request
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

import enum
import requests
import re
import json
from date_validate import *
app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///rides.db'
db = SQLAlchemy(app)
http_count_rides=0
class Ride(db.Model):
        r_id = db.Column(db.Integer, primary_key=True)
        time= db.Column(db.String,nullable=False)
        src = db.Column(db.Integer,nullable=False)
        dest = db.Column(db.Integer,nullable=False)
        created_by = db.Column(db.String, nullable=False)

        def __repr__(self):
                return '\n{"rideID": %d ,"username": %s, "timestamp" : %s}' % (self.r_id,self.created_by,self.time)

class ride_user(db.Model):

        r_id = db.Column(db.Integer, nullable=False,primary_key=True)
        username = db.Column(db.String, nullable=False,primary_key=True)


        def __repr__(self):
                return "\n{RideId : %d , User : %s}" % (self.r_id,self.username)

class Counter(db.Model):
	c_id = db.Column(db.Integer, primary_key=True)
	c_val = db.Column(db.Integer)

def is_present_areanumber(num):
        f=open("AreaNameEnum.csv","r")
        H={}
        i=0
        for line in f:
                if i==0:                        # first line heading of the attribute
                        i=i+1
                else:
                        l=line.split(',')       #values of the place in integer
                        H[int(l[0])]=int(l[0])
        f.close()
        return (num in H)

@app.route("/api/v1/db/write",methods=["POST","DELETE"])
def write():
	if request.method=="POST":	#to read from data
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
	elif request.method=="DELETE":	#to delete from table
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
	
@app.route("/api/v1/db/read",methods=["POST"])
def read():
    if request.method=="POST":	#to read from data
        a={}
        table=request.get_json()["table"]
        column=request.get_json()["columns"]
        where=request.get_json()["where"]
        column=",".join(column)
        qry="select "+column+" from "+table+ " where "+where+ ";"
        print("-----This is the query {}-----".format(qry))
        try:
            result=db.session.execute(qry)
            count=0
            for i in result:
                a[count]=list(i)
                count+=1
            print("-------Result of query {}--------".format(a))			
            return jsonify(a)
        except Exception as e:
            print(e)
            abort(400)


@app.route("/api/v1/rides/<rideId>",methods=["POST","GET","DELETE","PUT"])
def Ridelistings(rideId):
	#Join an existing ride
	global http_count_rides
	http_count_rides =http_count_rides + 1
	"""
	count = Counter.query.filter_by(c_id=1).first()
	count.c_val = Counter.c_val + 1
	db.session.commit()
	"""
	if request.method == "POST":
		a = {"table":"Ride","columns":["*"],"where":"r_id = {}".format(rideId)}
		res = requests.post("http://18.209.199.196/api/v1/db/read",json=a)

		uflag=requests.get("http://Load-Balancer-18906539.us-east-1.elb.amazonaws.com/api/v1/users",json={})
		u = request.get_json()["username"]

		b = {"table":"ride_user","columns":["*"],"where":"r_id = {} and username = '{}' ".format(rideId,u)}
		res2 = requests.post("http://18.209.199.196/api/v1/db/read",json=b)
		print("---------------{} {} {}".format((u not in uflag.text),res.json(),res2.json()))
		temp=res.json()
		if u not in uflag.text or (len(res.json()) < 1 ) or (len(res2.json()) > 0 ):
			 return {"flag":"user/is isn't there"},400
		
		elif(u in temp['0']):
			return {"flag":"creater of ride can't join same ride"},400
		else:
			qry={"insert":[rideId,u],"column":["r_id","username"],"table":"ride_user"}
			res=requests.post("http://18.209.199.196/api/v1/db/write",json=qry)
			if(res.status_code==200):
				return {}
			else:
				abort(400)
	
	#delete a ride of given rideId
	elif request.method == "DELETE":
		a = {"table":"Ride","columns":["*"],"where":"r_id = {} ".format(rideId)}
		#b = {"table":"ride_user","columns":["*"],"where":"r_id = {}".format(rideId)}
		read1 = requests.post("http://18.209.199.196/api/v1/db/read",json=a)
		#read2 = requests.post("http://orchestrator/api/v1/db/read",json=b)
		if len(read1.json()) > 0:		
			res = requests.delete("http://18.209.199.196/api/v1/db/write",json=a)
			#res2 = requests.delete("http://orchestrator/api/v1/db/write",json=b)
		
			if res.status_code==200:
				return {},200
			else:
				return {},res.status_code
		else:
			return {'flag':'rideId dosenot exsists'},400

    #List all the details of a given ride
	elif request.method == "GET":
		try:
		        a = {"table":"Ride","columns":["r_id","created_by","time","src","dest"],"where":"r_id = {}".format(rideId)}
		        res = requests.post("http://18.209.199.196/api/v1/db/read",json=a)
		        if(len(res.json())>0):
		            b = {"table":"ride_user","columns":["username"],"where":"r_id = {}".format(rideId)}
		            res2 =  requests.post("http://18.209.199.196/api/v1/db/read",json=b)
		            ridedetails = list(res.json().values())[0]
		            listuser = list(res2.json().values())
		            print("--------{} {}".format(ridedetails,res2.json().values()))
		            d = {}
		            d["rideId"] = ridedetails[0]
		            d["created_by"]=ridedetails[1]
		            d["Timestamp"]=ridedetails[2]
		            d["source"]=ridedetails[3]
		            d["destination"]=ridedetails[4]
		            l = []
		            for i in listuser:
		                l.append(i[0])
		            d["users"]=l					
		            return jsonify(d),200
		        else:
		            return {},204
		except KeyError:
		        return {},400
		except:
		        return {},500

	else:
		return {},405

#3 and 4th
@app.route("/api/v1/rides",methods=["POST","GET","DELETE","PUT"])
def rides():
	"""
	count = Counter.query.filter_by(c_id=1).first()
	count.c_val = Counter.c_val + 1
	db.session.commit()
	"""
	global http_count_rides
	http_count_rides =http_count_rides + 1
	try:
		if request.method == "POST":
			uname = request.get_json()["created_by"]
			#flag =  User.query.filter_by(username=uname).first()
			flag = requests.get("http://Load-Balancer-18906539.us-east-1.elb.amazonaws.com/api/v1/users",json={})
			if uname not in flag.text:
					return jsonify({"flag":"user doesnt exist"}),400

		    # use enum to check !!
			src  =  int(request.get_json()["source"])
			dest =  int(request.get_json()["destination"])
			t    =  str(request.get_json()["timestamp"])
			res=date_and_time_validate(t)
			print(res)
			if(res!=True): #either date or time is in invalid format
				return jsonify({"flag":"date or time invalid"}),400
			if((is_present_areanumber(src)) and (is_present_areanumber(dest)) and (src != dest )):
				a = {"insert":[uname,t,src,dest],\
                            "column":["created_by","time","src","dest"],"table":"Ride"}
				resp=requests.post("http://18.209.199.196/api/v1/db/write",json=a)
				if resp.status_code==200:
					return {'flag':'added'},201
				else:
					return {"flag":"cann't be added"},500

			else:
				return jsonify({"flag":"invalid input"}),400

		#List all upcoming rides for a given source and destination
		elif request.method == "GET":
				tm=FormatTheDate(str(datetime.datetime.now()))
				src = int(request.args.get("source"))
				dest = int(request.args.get("destination"))
				if (( is_present_areanumber(src) ) and ( is_present_areanumber(dest)) and (src != dest )):
					a={"columns":["*"],"table":"Ride","where":"src='"+str(src)+"' and dest='"+str(dest)+"'"}
					rides=requests.post("http://18.209.199.196/api/v1/db/read",json=a)
					if rides.status_code==200:
						rides=list(rides.json().values())
						print('-------list of rides %s ' % rides) 
						l = []
						for row in rides:
							d = {}
							d["rideId"]=row[0]
							d["username"]=row[1]
							if(isupcoming(tm,row[2])):      #only if the ride is upcoming
									d["time"]=row[2]        #insert it into the res list
									l.append(d)
						if len(l)==0:
							return {},204
						else:
							return jsonify(l),200
					else:
						return {},204
						
				else:
					return jsonify({"flag":"either is not integer"}),400

		else:
			return {},405
	except Exception as e:
		print(e)
		return {},500
@app.route("/api/v1/rides/count",methods=["POST","GET","DELETE","PUT"])
def rides_count():
	"""
	count = Counter.query.filter_by(c_id=1).first()
	count.c_val = Counter.c_val + 1
	db.session.commit()
	"""
	global http_count_rides
	http_count_rides =http_count_rides + 1
	try:
		if request.method == "GET":
			a={"table":"Ride","columns":["r_id"],"where":"r_id like '%'"}
			res=requests.post("http://18.209.199.196/api/v1/db/read",json=a)
			lis=[]
			lis.append(len(res.json()))
			return jsonify(lis),200
		else:
		        return {},405
	except:
		return {},400

@app.route("/api/v1/db/clear",methods=["POST","GET","DELETE","PUT"])
def clearDB():
        if request.method == "POST":
                is_empty=not(bool(request.json))
                if(is_empty):
                        a={"table":"ride_user","columns":["*"],"where":"r_id like '%'"} 
                        res = requests.delete("http://18.209.199.196/api/v1/db/write",json=a)
                        b={"table":"Ride","columns":["*"],"where":"r_id like '%'"}   
                        res2 = requests.delete("http://18.209.199.196/api/v1/db/write",json=b)
                        if res.status_code==200 and res2.status_code==200: 
                                return {},200
                        else:
                                return {'flag':'couldnot delete'},400
                else:
                        return {},400
        else:
                return {},405

@app.route("/api/v1/db/read/<name>",methods=["GET"])
def view(name):
        flag = rflag = Ride.query.filter_by(created_by=name).first()
        print(flag)
        if flag:
                return name

        ride_user.query.filter_by(username=name).delete()
        db.session.commit()

        return "ok"


#HTTP requests count and reset
@app.route("/api/v1/_count",methods=["POST","GET","DELETE","PUT"])
def count():
	#count = Counter.query.filter_by(c_id=1).first()
	global http_count_rides
	if request.method=="GET":
		l=[]
		l.append(http_count_rides)
		return jsonify(l),200
	elif request.method=="DELETE":
		http_count_rides=0
		db.session.commit()
		return {},200
	else:
		return {},405
"""
@app.route("/api/v1/_count",methods=["POST","GET","DELETE","PUT"])
def reset():
        global http_count_rides
        if request.method=="DELETE":
                http_count_rides
                return {},200
        else:
                return {},405
"""
if __name__== '__main__':
        db.create_all()
        app.debug=True
        app.run(host='0.0.0.0',port=80)
