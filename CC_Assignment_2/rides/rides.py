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



def is_present_areanumber(num):
	f=open("AreaNameEnum.csv","r")
	H={}
	i=0
	for line in f:
		if i==0:			# first line heading of the attribute
			i=i+1
		else:
			l=line.split(',')	#values of the place in integer
			H[int(l[0])]=int(l[0])
	f.close()
	return (num in H)
	


@app.route("/api/v1/rides/<rideId>",methods=["POST","GET","DELETE"])
def Ridelistings(rideId):
	#Join an existing ride
	if request.method == "POST":
		flag = Ride.query.filter_by(r_id=rideId).first()
		#uflag= User.query.filter_by(username=request.get_json()["username"]).first()
		uflag=requests.get("http://3.213.122.148:8080/api/v1/users",json={})
		does_exists = ride_user.query.filter(ride_user.r_id==rideId,ride_user.username==request.get_json()["username"]).first()
		u = request.get_json()["username"]
		#if flag is None or u not in uflag.text or does_exists is not None:
		#print(u not in uflag.text)
		#print(u)
		if flag is None or u not in uflag.text or does_exists is not None:
			return {},400
		else:
			shared = ride_user(r_id = rideId , username = request.get_json()["username"])
			db.session.add(shared)
			db.session.commit()
			return {},200

	elif request.method == "DELETE":

		try:
			flag = Ride.query.filter_by(r_id=rideId).first()
			if flag is None:
				return {},405
			else:
				db.session.delete(flag)
				ride_user.query.filter_by(r_id=rideId).delete()
				db.session.commit()
				return {},200
		except:
			return {},500

	#List all the details of a given ride
	elif request.method == "GET":
		try:
			flag = Ride.query.filter_by(r_id=rideId).first()
			if flag is None:
				return {},400
			r = db.session.execute('select r_id,created_by,time,src,dest from Ride where r_id = {}'.format(rideId))
			d = {}
			for i in r:
				d["rideId"] = i[0]
				d["created_by"]=i[1]
				d["Timestamp"]=i[2]
				d["source"]=i[3]
				d["destination"]=i[4]
		
		
			res = db.session.execute('select username from ride_user where r_id = {}'.format(rideId))
			l = []		
			for i in res:
				l.append(i[0])
			d["users"]=l
			return jsonify(d),200	
		except KeyError:
			return {},400
		except:
			return {},500	

	else:
		return {},405

#3 and 4th
@app.route("/api/v1/rides",methods=["POST","GET"])
def rides():
	#Create a new ride
	try:
		if request.method == "POST":
			uname = request.get_json()["created_by"]
			#flag =  User.query.filter_by(username=uname).first()
			flag = requests.get("http://3.213.122.148:8080/api/v1/users",json={})
			if uname not in flag.text:
				return jsonify({"flag":"user doesnt exist"}),400

			# use enum to check !!
			src =  int(request.get_json()["source"])
			dest =  int(request.get_json()["destination"])
			t=str(request.get_json()["timestamp"])
			res=date_and_time_validate(t)
			print(res)
			if(res!=True): #either date or time is in invalid format
				return jsonify({"flag":"date or time invalid"}),400
			if ((is_present_areanumber(src)) and (is_present_areanumber(dest)) and (src != dest )):
				"""r = requests.post("http://3.233.19.138/write/newride",json = request.get_json())
				if r.text == "ok":

					return {"flag":"added"},200
				else: 
					return {"flag":"not added"},500"""
				r = Ride(src=request.get_json()["source"],dest=request.get_json()["destination"],created_by=request.get_json()["created_by"],time=t)
				db.session.add(r)
				db.session.commit()
				return {"flag":"added"},201
				
			else:
				return jsonify({"flag":"invalid input"}),400

		#List all upcoming rides for a given source and destination
		elif request.method == "GET":
			tm=FormatTheDate(str(datetime.datetime.now()))
			src = int(request.args.get("source"))
			dest = int(request.args.get("destination"))
			if (( is_present_areanumber(src) ) and ( is_present_areanumber(dest)) and (src != dest )):
				query = db.session.execute('select r_id,created_by,time from Ride where src={} and dest={}'.format(src,dest))
				l = []
				for row in query:
					d = {}
					d["rideId"]=row[0]
					d["username"]=row[1]
					if(isupcoming(tm,row[2])):	#only if the ride is upcoming
						d["time"]=row[2]	#insert it into the res list
						l.append(d)
				if len(l)==0:
					return {},204
				else:
					return jsonify(l),200
			else:
				return jsonify({"flag":"either is not integer"}),400

		else:
			return {},405
	except Exception as e:
		print(e)
		return {},500

@app.route("/api/v1/db/clear",methods=["POST"])
def clearDB():
	if request.method == "POST":
		is_empty=not(bool(request.json))
		if(is_empty):
			Ride.query.delete()	# Ride DB clearing
			ride_user.query.delete() # Ride DB Clearing
			db.session.commit()
			return {},200
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


if __name__== '__main__':
	db.create_all()
	app.debug=True
	app.run(host='0.0.0.0',port=8000)
