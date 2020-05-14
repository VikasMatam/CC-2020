import datetime

# d1-->current time
# d2 --> expected to be an upcoming time
def compare_upcoming_time(time1,time2):
	smhh1=time1.split('-')
	smhh2=time2.split('-')
	l=list(zip(smhh1,smhh2))
	l.reverse()
	for (j,i) in l:
		i=int(i)
		j=int(j)
		if(i>j):
			return True
		elif i<j:
			return False
	return False
	
def compare_upcoming_date(date1,date2):
	ddmmyy1=date1.split('-')
	ddmmyy2=date2.split('-')
	res=-1
	l=list(zip(ddmmyy1,ddmmyy2))
	l.reverse()
	for (j,i) in l:
		i=int(i)
		j=int(j)
		if(i>j):
			res=1
			break
		elif i==j:
			res=0
		else:
			res=-1
			break
	return res
def isupcoming(d1,d2):
	l1=d1.split(':')
	l2=d2.split(':')
	date1=l1[0]
	time1=l1[1]
	date2=l2[0]
	time2=l2[1]
	res1=compare_upcoming_date(date1,date2)
	if(res1==1):
		return True
	elif res1==0:
		return compare_upcoming_time(time1,time2)	
	else:
		return False
	
	


def FormatTheDate(s):
	l=s.split(' ')
	yymmdd=l[0].split('-')
	hhms=l[1].split(':')
	yy=yymmdd[0]
	mm=yymmdd[1]
	dd=yymmdd[2]
	hh=hhms[0]
	m=hhms[1]
	s=str(int(float(hhms[2])))
	return (dd+'-'+mm+'-'+yy+':'+s+'-'+m+'-'+hh)
	



'''
def convert_timestamp_for_databases(s):
	l=s.split(':')
	ddmmyy=l[0].split('-')
	smhh=l[1].split('-')
	dd=ddmmyy[0]
	mm=ddmmyy[1]
	yy=ddmmyy[2]
	s=smhh[0]
	m=smhh[1]
	hh=smhh[2]
	return (yy+"-"+mm+"-"+dd+" "+hh+":"+m+":"+s)

'''
	
def date_and_time_validate(s):
	l=s.split(":")
	dt=l[0]
	tm=l[1]
	p1=time_validate(tm)
	p2=date_validate(dt)
	if(p1=="Invalid Format of Time" and p2=="Invalid Format of Date"):
		return "Invalid Date and Time Format"
	elif p1=="Invalid Format of Time":
		return p1
	elif p2=="Invalid Format of Date":
		return p2
	else:
		return (p1 and p2)
	
def time_validate(s):
	try:
		ss,mm,hh=s.split('-')
		if(len(ss)!=2 or len(mm)!=2 or len(hh)!=2):
			return "Invalid Format of Time"
		ss=int(ss)
		mm=int(mm)
		hh=int(hh)
		return (0<=ss and ss<=59 and 0<=mm and mm<=59 and 0<=hh and hh<=23)
		
	except:
		return "Invalid Format of Time"
def date_validate(s):
	day,month,year = s.split('-')
	if(len(day)!=2 or len(month)!=2 or len(year)!=4):
		return "Invalid Format of Date"

	isValidDate = True
	try :
		res=datetime.datetime(int(year),int(month),int(day))
	except ValueError :
		isValidDate = False
	if(isValidDate) :
		return True
	else :
		return False
'''
print(1,date_validate("30-92-111"))
print(2,date_validate("30-02-1115"))
print(3,date_validate("30-02-1116"))
print(4,date_validate("20-02-1116"))
print(5,date_validate("3000-0200-001116"))
print(6,date_validate("29-02-1120"))
'''

'''
print(time_validate("22-111-77"))
print(time_validate("122-11-77"))
print(time_validate("12-11-777"))
print(time_validate("12-11-12"))
print(time_validate("1220-12-18"))
print(time_validate("22-15-19"))
print(time_validate("78-11-19"))
print(time_validate("99-11-11"))
print(time_validate("01-01-01"))
print(time_validate("59-07-15"))
print(time_validate("60-07-15"))
print(time_validate("59-59-25"))
'''

'''
d1="10-04-2018:15-20-11"
d2="10-04-2018:15-20-11"
print(d1,"\n",d2,"\n",isupcoming(d1,d2))

d1="10-05-2018:15-20-11"
d2="10-04-2018:15-20-11"
print(d1,"\n",d2,"\n",isupcoming(d1,d2))

d1="10-04-2018:15-20-11"
d2="10-05-2018:15-20-11"
print(d1,"\n",d2,"\n",isupcoming(d1,d2))

d1="10-04-2018:15-20-11"
d2="12-04-2018:15-20-11"
print(d1,"\n",d2,"\n",isupcoming(d1,d2))

d1="14-04-2018:15-20-11"
d2="12-04-2018:15-20-11"
print(d1,"\n",d2,"\n",isupcoming(d1,d2))

d1="12-04-2019:15-20-11"
d2="12-04-2018:15-20-11"
print(d1,"\n",d2,"\n",isupcoming(d1,d2))

d1="14-04-2018:15-20-11"
d2="14-04-2020:15-20-11"
print(d1,"\n",d2,"\n",isupcoming(d1,d2))
'''
