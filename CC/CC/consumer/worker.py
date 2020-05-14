import pika
import json
import sqlite3
from threading import Thread
import os
import sys
import time
import docker
import requests

def call_slave():
        t2 = readprc()
        t3 = syncwrite()
        t3.start()
        t2.start()
        time.sleep(4)
        t2.stop()
        t3.stop()
        t2.join()
        t3.join()


def call_master():
        t1 = writeprc()
        t1.start()
        time.sleep(4)
        t1.stop()
        t1.join()



#creates a table with the given schema if it dosen't exists 
def createTable():
        conn=sqlite3.connect("rideShare.db")
        c=conn.cursor()
        c.execute("create table if not exists User(username varchar not null,\
                                                            password varchar not null,\
                                                            primary key(username));")
        c.execute("create table if not exists Ride(r_id integer not null,\
                                                            created_by varchar not null,\
                                                            time varchar not null,\
                                                            src varchar not null,\
                                                            dest varchar not null,\
                                                            primary key(r_id),\
                                                            foreign key(created_by) references User(username) on delete cascade);")
        c.execute("create table if not exists ride_user(r_id integer not null,\
                                                                   username varchar not null,\
                                                                   primary key(r_id,username),\
                                                                   foreign key(r_id) references Ride(r_id) on delete cascade,\
                                                                   foreign key(username) references User(username) on delete cascade);")
        conn.commit()
        conn.close()

#all the db operatinos are done here by read_db , write_db , delete_db functions which accepts , a JSON blob
#this converts the qry JSON blob into a serial string which is then excuted.
def read_db(qry):
        a={}
        table=qry["table"]
        column=qry["columns"]
        where=qry["where"]
        column=",".join(column)
        qry_str="select "+column+" from "+table+ " where "+where+ ";"
        print(qry_str)
        with sqlite3.connect("rideShare.db") as conn:
                try:
                        c=conn.cursor()
                        c.execute(qry_str)
                        count=0
                        for i in c.fetchall():
                                a[count]=list(i)
                                count+=1
                        return a
                except Exception as e:
                        print(e)
                        return {}

def write_db(qry):
        data=qry["insert"]
        column=qry["column"]
        table=qry["table"]
        data1=[]
        for i in data:
                if type(i) is str:
                        data1.append("\'"+i+"\'")
                else:
                        data1.append(str(i))
        column=",".join(column) #combining column with join
        data1=",".join(data1)#combining data with join
        with sqlite3.connect("rideShare.db") as conn:
                try:
                        qry_str="insert into "+table+"("+column+") values("+data1+");"
                        print(qry_str)
                        c=conn.cursor()
                        c.execute(qry_str)
                        conn.commit()
                        print('commited to db')
                        return {"status":"ok"}
                except Exception as e:
                        print('The error is %s ' % e)
                        return {}

def delete_db(qry_str):
        table=qry_str["table"]
        where=qry_str["where"]
        qry="delete from "+table+" where "+where+";"
        with sqlite3.connect("rideShare.db") as conn:
                try:
                        c=conn.cursor()
                        c.execute("PRAGMA foreign_keys = ON;")#by default foregin keys constraint will not be supported in sqlite3
                        c.execute(qry)
                        conn.commit()
                        print('deleted from db')
                        return {"status":"ok"}
                except Exception as e:
                        print(e)
                        return {}

#this thread class establishes connection with rabbitmq , consumes readQueue and dumps result in responseQueue.
class readprc(Thread):
        def __init__(self):
                Thread.__init__(self)
                self._is_interrupted = False

        def stop(self):
                self._is_interrupted = True

        def run(self):
                self.connection=pika.BlockingConnection(
                  pika.ConnectionParameters(host='rmq'))
                channel = self.connection.channel()
                channel.queue_declare(queue='responseQueue')
                channel.queue_declare(queue='readQueue')
                channel.basic_qos(prefetch_count=1)

                for message  in channel.consume('readQueue', inactivity_timeout=1):
                        if self._is_interrupted:
                                break
                        if message[2]==None:
                                continue

                        method_frame,props,body = message
                        qry_str = json.loads(body.decode())
                        resp = read_db(qry_str)
                        print("response from read_db {}".format(resp))
                        channel.basic_publish(exchange='',
                                                 routing_key='responseQueue',
                                                 properties=pika.BasicProperties(correlation_id = \
                                                 props.correlation_id),
                                                 body=json.dumps(resp))
                        channel.basic_ack(method_frame.delivery_tag)

                        print("data has been sent to the responseQueue")
                requeued_messages = channel.cancel()
                print('Requeued %i messages' % requeued_messages)
                channel.close()
                self.connection.close()

class writeprc(Thread):
        def __init__(self):
                Thread.__init__(self)
                self._is_interrupted = False


        def stop(self):
                self._is_interrupted = True

        def run(self):
                self.connection=pika.BlockingConnection(
                  pika.ConnectionParameters(host='rmq'))
                channel = self.connection.channel()
                channel.queue_declare(queue='writeQueue')
                channel.exchange_declare(exchange='write_exchange', exchange_type='fanout')
                channel.basic_qos(prefetch_count=1)

                for message in channel.consume('writeQueue', inactivity_timeout=1):

                        if self._is_interrupted:
                                break
                        if message[2]==None:
                                continue

                        method_frame,props,body = message
                        #print(message)
                        qry_str = json.loads(body.decode())
                        print(qry_str)
                        if qry_str["flag"] == "write":
                                response = write_db(qry_str)
                        elif qry_str["flag"] == "delete":
                                response = delete_db(qry_str)
                        else:
                                response = {}
                        channel.basic_publish(exchange='',
                        routing_key=props.reply_to,
                        properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),
                        body=json.dumps(response))
                        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                        channel.basic_publish(exchange='write_exchange', routing_key='', body=body)

                requeued_messages = channel.cancel()
                print('Requeued %i messages' % requeued_messages)
                channel.close()
                self.connection.close()


class syncwrite(Thread):
        def __init__(self):
                Thread.__init__(self)
                self._is_interrupted = False


        def stop(self):
                self._is_interrupted = True

        def run(self):
                self.connection=pika.BlockingConnection(
                  pika.ConnectionParameters(host='rmq'))
                channel = self.connection.channel()
                channel.exchange_declare(exchange='write_exchange', exchange_type='fanout')
                result = channel.queue_declare(queue='', exclusive=True)
                queue_name = result.method.queue
                channel.queue_bind(exchange='write_exchange', queue=queue_name)

                #channel.basic_qos(prefetch_count=1)

                for message in channel.consume(queue_name, inactivity_timeout=1):
                        if self._is_interrupted:
                                break
                        if message[2]==None:
                                continue

                        method_frame,props,body = message
                        qry_str = json.loads(body.decode())
                        print(qry_str)

                        if qry_str["flag"] == "write":
                                response = write_db(qry_str)
                        elif qry_str["flag"] == "delete":
                                response = delete_db(qry_str)
                        else:
                                response = {}
                        print('Response from db %s ' % response)
                requeued_messages = channel.cancel()
                print('Requeued %i messages' % requeued_messages)
                channel.close()
                self.connection.close()

if __name__=="__main__":
        time.sleep(10)#delay for orchestrator to be setup first , due to certain dependencies
        createTable()
        os.system("uname -n > abc.txt")#write container id to file
        f=open("abc.txt","r")#read container id from file
        s=f.read()
        print(s)
        sys.stdout.flush()
        client = docker.DockerClient(base_url='unix://var/run/docker.sock')
        cont=client.containers.get(s[0:-2])
        pid=cont.attrs["State"]["Pid"]


        while True:
                flag=-1# not computed yet
                r=requests.get('http://18.209.199.196/api/v1/get_master_info')
                try:
                        d=r.json()
                except Exception as e:
                        print(e.__repr__())
                        d={'pid':-1}
                print(d,type(d))
                sys.stdout.flush()
                print(pid,type(pid))
                sys.stdout.flush()
                flag=(d['pid']==pid)

                if flag:
                        print('running write thread')
                        call_master()
                        print('exited clean write thread')
                else:
                        print('running read/sync thread')
                        call_slave()
                        print('exited clean read/sync thread')
