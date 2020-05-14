import pika
import uuid
import sys
from flask import Flask,render_template,jsonify,abort,request
from flask_sqlalchemy import SQLAlchemy
import time
import enum
import requests
import re
import json
import os
import docker
from threading import Thread,Timer
import math

# connecting to kazoo
import logging
from kazoo.client import KazooClient
from kazoo.client import KazooState
logging.basicConfig()
zk = KazooClient(hosts='zoo:2181')
print('ye kaha aa gaye hum')
sys.stdout.flush()
#start the zookeeper
zk.start()
time.sleep(30)
print('bistarr chodo jago utho')
sys.stdout.flush()
read_count=0
app = Flask(__name__)


# zoo_waley_babu/slave/pid
# zoo_waley_babu/master/pid
# zoo_waley_babu/watch/slave/pid
# zoo_waley_babu/watch/master/pid
def callback_master_crash(mandatory):   #watch function when master crashes
        global zk
        p=mandatory.path
        print("start of callback master crash")
        #start of callback master crash
        sys.stdout.flush()
        l=p.split("/")
        p="zoo_waley_babu/watch/master/"+l[-1]
        data,stat=zk.get(p)
        data=data.decode()
        print("data master crash is",data)
        sys.stdout.flush()
        if(data=="1"):
                watch_master_crash()
        zk.delete(p)
        print("end of callback master crash")
        sys.stdout.flush()

def callback_slave_crash(mandatory):
        global zk
        p=mandatory.path
        l=p.split("/")
        print("start of callback slave crash")
        sys.stdout.flush()
        #start of callback slave crash
        p="zoo_waley_babu/watch/slave/"+l[-1]
        data,stat=zk.get(p)
        data=data.decode()
        print("data slave crash is",data)
        sys.stdout.flush()
        #data slave crash is data
        # bring up new slave container
        if(data=="1"):
                watch_slave_crash()
        zk.delete(p)
        print("end of callback slave crash")
        sys.stdout.flush()

# increases a slave by running a slave container
def watch_slave_crash():#utility function when slave container crashes
        print("start of Watch slave crash")
        sys.stdout.flush()
        global zk
        print('Todo : Create a New Slave Container and insert a znode for the container with slave crash handler')
        sys.stdout.flush()
        #Todo : Create a New Slave Container and insert a znode for the container with slave crash handler
        increase_slaves_by(1)
        print("end of Watch slave crash")
        sys.stdout.flush()

def watch_master_crash():# utility function for callback master crash
        global zk
        print("start of Watch master crash")
        sys.stdout.flush()
        print('Todo : Elect new master and set proper environment variables and ')
        sys.stdout.flush()
        d=get_slave_zookeeper(zk)
        l=list(d.keys())
        l.sort()
        pid=l[0]#smallest pid slave becomes master
        con=d[l[0]]#corresponding container
        if(len(l)==1):
                increase_slaves_by(1)
                zk.set('zoo_waley_babu/watch/slave/'+str(pid),bytes("0",'utf-8'))
        zk.delete('zoo_waley_babu/slave/'+str(pid))     #perform deletion of slave znode, will call the new slave creation watch handler
        #run the container con as master , need to update master_info
        # the least pid znode is deleted from slave and put into master branch
        # the master watch znode is also added to this elected master

        zk.set('zoo_waley_babu/master_info',bytes(str(con),'utf-8'))
        zk.create('zoo_waley_babu/master/'+str(pid),bytes(str(con),'utf-8'))
        if(zk.exists('zoo_waley_babu/watch/master/'+str(pid))):
                zk.set('zoo_waley_babu/watch/master/'+str(pid),bytes("1",'utf-8'))
        else:
                zk.create('zoo_waley_babu/watch/master/'+str(pid),bytes("1",'utf-8'))
        zk.exists('zoo_waley_babu/master/'+str(pid),watch=callback_master_crash)
        print("End of Watch master crash")
        sys.stdout.flush()

#ensure the old znodes if any are deleted and fresh new zookeeper paths are set as mentioned earlier
def initialize_zookeeper(zk):#initializes master and slave znodes initially
        if(zk.exists('zoo_waley_babu/watch/master')):
                ch=zk.get_children('zoo_waley_babu/watch/master')
                for pid in ch:
                        zk.set('zoo_waley_babu/watch/master/'+pid,bytes("0",'utf-8'))
        if(zk.exists('zoo_waley_babu/watch/slave')):
                ch=zk.get_children('zoo_waley_babu/watch/slave')
                for pid in ch:
                        zk.set('zoo_waley_babu/watch/slave/'+pid,bytes("0",'utf-8'))
        if(zk.exists('zoo_waley_babu/master')):
                ch=zk.get_children('zoo_waley_babu/master')
                for pid in ch:
                        zk.delete('zoo_waley_babu/master/'+pid)
        if(zk.exists('zoo_waley_babu/slave')):
                ch=zk.get_children('zoo_waley_babu/slave')
                for pid in ch:
                        zk.delete('zoo_waley_babu/slave/'+pid)
        zk.ensure_path('zoo_waley_babu/')

        if(not(zk.exists('zoo_waley_babu/master'))):
                zk.ensure_path('zoo_waley_babu/master')
        if(not(zk.exists('zoo_waley_babu/watch/slave'))):
                zk.ensure_path('zoo_waley_babu/watch/slave')
        if(not(zk.exists('zoo_waley_babu/watch/master'))):
                zk.ensure_path('zoo_waley_babu/watch/master')
        if(not(zk.exists('zoo_waley_babu/slave'))):
                zk.ensure_path('zoo_waley_babu/slave')
        if(zk.exists('zoo_waley_babu/master_info')):
                zk.delete('zoo_waley_babu/master_info',recursive=True)

        client = docker.DockerClient(base_url='unix://var/run/docker.sock')
        con=client.containers.get('master')
        pid=con.attrs["State"]["Pid"]
        print(str(con))
        sys.stdout.flush()
        print(pid)
        sys.stdout.flush()
        zk.create('zoo_waley_babu/master_info',bytes(str(con),'utf-8'))
        zk.create('zoo_waley_babu/master/'+str(pid),bytes(str(con),'utf-8'))
        zk.exists('zoo_waley_babu/master/'+str(pid),watch=callback_master_crash)
        zk.create('zoo_waley_babu/watch/master/'+str(pid),bytes("1",'utf-8'))
        con=client.containers.get('slave')
        pid=con.attrs["State"]["Pid"]
        print(str(con))
        sys.stdout.flush()
        print(pid)
        sys.stdout.flush()
        zk.create('zoo_waley_babu/slave/'+str(pid),bytes(str(con),'utf-8'))
        zk.exists('zoo_waley_babu/slave/'+str(pid),watch=callback_slave_crash)
        zk.create('zoo_waley_babu/watch/slave/'+str(pid),bytes("1",'utf-8'))
        print('Finished initializing zookeeper')
        sys.stdout.flush()

# refer the zookeeper path for master branch and get the details like pid and container id of the running master
def get_master_zookeeper(zk):#returns master which is queried using znodes along the path
        d={}
        client = docker.DockerClient(base_url='unix://var/run/docker.sock')
        m=zk.get_children('zoo_waley_babu/master',include_data=False)# m is a list of znode names , pid is the znode name and it is a string
        n=[]
        for pid in m:
                container_id,stat=zk.get("zoo_waley_babu/master/"+pid)
                container_id=container_id.decode()
                #print(container_id)
                #sys.stdout.flush()
                #print(pid)
                #sys.stdout.flush()
                container_id=container_id[12:-1]
                cont=client.containers.get(container_id)
                n.append(cont)
        for i in range(len(m)):
                d[int(m[i])]=n[i]
        return d
# this api is used by the worker to communicate to zookeeper to know if they are slave or master
@app.route('/api/v1/get_master_info',methods=["GET"])
def send_details_to_worker():
        global zk
        d=get_master_zookeeper(zk)
        print(d,type(d))
        sys.stdout.flush()
        if(len(d)==0):
                return jsonify({'pid':-1}),200
        pid=min(d.keys())
        con=d[pid]
        con=str(con)
        con=con[12:-1]
        return jsonify({'pid':pid}),200
def get_slave_zookeeper(zk):#returns slaves which is queried using znodes along the path
        d={}
        client = docker.DockerClient(base_url='unix://var/run/docker.sock')
        m=zk.get_children('zoo_waley_babu/slave',include_data=False)# m is a list of znode names , pid is the znode name and it is a string
        n=[]
        for pid in m:
                container_id,stat=zk.get("zoo_waley_babu/slave/"+pid)
                container_id=container_id.decode()
                container_id=container_id[12:-1]
                cont=client.containers.get(container_id)
                n.append(cont)
        for i in range(len(m)):
                d[int(m[i])]=n[i]
        return d



def show_master_zookeeper(zk):#shows the master as nominated in znode of master
        d=get_master_zookeeper(zk)
        i=0
        for pid in d:   # ( str pid , container con) dictionary
                print(pid,type(pid))
                sys.stdout.flush()
                print(d[pid],type(d[pid]))
                sys.stdout.flush()
                i=1
        if(i==0):
                print("No master")
                sys.stdout.flush()


def show_slave_zookeeper(zk):#shows the slave as nominated in znode of slave
        d=get_slave_zookeeper(zk)
        i=0
        for pid in d:   # ( int pid , container con) dictionary
                print(pid,type(pid))
                sys.stdout.flush()
                print(d[pid],type(d[pid]))
                sys.stdout.flush()
                i=1
        if(i==0):
                print("No slave")
                sys.stdout.flush()


initialize_zookeeper(zk)
show_master_zookeeper(zk)
show_slave_zookeeper(zk)
"""this class creates a rpc method object and connects to rabbitmq ,
if the queue name is not specified as to where to dump the requests, this class creates a  exclusive queue with slave , 
and discrads the queue automatically when queue has been consumed. Only those reply will be accepted whose correlation_id matches
"""

class RpcClient(object):

    def __init__(self,call_back_queue_name=''):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq'))

        self.channel = self.connection.channel()
        self.call_back_queue_name = call_back_queue_name
        if self.call_back_queue_name:
            result = self.channel.queue_declare(queue=self.call_back_queue_name)
        else:
            result = self.channel.queue_declare(queue=self.call_back_queue_name,exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = json.loads(body.decode())

    def call(self,queue_name,qry):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id),
                    body=qry)
        while self.response is None:
            self.connection.process_data_events()
        requeued_messages = self.channel.cancel()
        self.connection.close()
        return self.response

@app.route("/api/v1/db/read",methods=["POST"])
def read():
    print("yaha waha")
    sys.stdout.flush()
    global read_count
    read_count += 1

    try:
        global clock
        clock.start()
    except:
        pass

    queue_name = 'readQueue'
    json_obj = request.get_json()
    qry = json.dumps(json_obj)
    print("-------------request to  slave {}".format(json_obj))
    read_rpc = RpcClient('responseQueue')#established connection with rabbitmq , declares a queue responseQueue 
    response = read_rpc.call(queue_name,qry)#dumps requests (qry) , onto the queue , readQueue and accepts results in responseQueue
    print("-------------response from slave {}".format(response))
    if len(response) > 0:
        return jsonify(response),200
    else:
        return jsonify(response),500

@app.route("/api/v1/db/write",methods=["POST","DELETE"])
def modify():
    if request.method == "POST":
        queue_name = 'writeQueue'
        json_obj = request.get_json()
        print('entered orchestrator with {}'.format(json_obj))
        json_obj.update({"flag":"write"})
        qry = json.dumps(json_obj)
        read_rpc = RpcClient()#established connection with rabbitmq,accepts messages onto machine generated exclusive queue name 
        response = read_rpc.call(queue_name,qry)
        print('response from worker {}'.format(response))
        if len(response) > 0:
            return response,200
        else:
            return response,500

    elif request.method == "DELETE":
        queue_name = 'writeQueue'
        json_obj = request.get_json()
        json_obj.update({"flag":"delete"})
        qry = json.dumps(json_obj)

        read_rpc = RpcClient()
        response = read_rpc.call(queue_name,qry)
        print('response from worker {}'.format(response))
        if len(response) > 0:
            return response,200
        else:
            return response,500

# list all workers master as well as slaves refer to zookeeper to get details of running container
@app.route("/api/v1/worker/list",methods=["GET"])
def list_workers():
    list_pid = []
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    try:
        global zk
        d1=get_master_zookeeper(zk)
        d2=get_slave_zookeeper(zk)
        l1=list(d1.keys())
        l2=list(d2.keys())
        l=l1+l2
        l.sort()
        return jsonify(l),200
    except Exception as e:
        print(e)
        return jsonify({'flag':'is not working'}),500

# api to crash the master
@app.route("/api/v1/crash/master",methods=["POST"])
def crash_master():
        print('here')
        sys.stdout.flush()
        client = docker.DockerClient(base_url='unix://var/run/docker.sock')
        try:
                global zk
                d=get_master_zookeeper(zk)
                l=list(d.keys())
                pid=l[0]
                c=d[pid]
                zk.delete('zoo_waley_babu/master/'+str(pid))
                c.stop()
                print("Deleted master znode")
                sys.stdout.flush()
                return jsonify([pid]),200
        except:
                return {'flag':'sthing is messed up'},500

# api to crash the slave
@app.route("/api/v1/crash/slave",methods=["POST"])
def crash_slave():
        print('here')
        sys.stdout.flush()
        try:
                global zk
                slaves = get_slave_zookeeper(zk)
                print(slaves,type(slaves))
                sys.stdout.flush()
                client = docker.DockerClient(base_url='unix://var/run/docker.sock')
                c = slaves.pop(max(slaves.keys()))#crash the slave with highest pid
                print(type(c))
                sys.stdout.flush()
                l=list(slaves.keys())
                print(l)
                sys.stdout.flush()
                pid = c.attrs["State"]["Pid"]
                if(len(l)==0):
                        #Only one slave at crash time
                        print("Only one slave at crash time")
                        sys.stdout.flush()
                        increase_slaves_by(1)
                        print("Yaha")
                        zk.set('zoo_waley_babu/watch/slave/'+str(pid),bytes("0",'utf-8'))
                print("Waha")
                print("Attempt to run a new slave container")
                sys.stdout.flush()
                print("Attempt to delete the znode for crashed container")
                sys.stdout.flush()
                #Attempt to run a new slave container
                #Attempt to delete the znode for crashed container
                if(zk.exists('zoo_waley_babu/slave/'+str(pid))):
                        print("Trying to delete slave crashed znode")
                        sys.stdout.flush()
                        zk.delete('zoo_waley_babu/slave/'+str(pid))

                print("Attempt to stop crashed container")
                sys.stdout.flush()
                c.stop()
                #Stopped Successfully
                print("Stopped Successfully")
                sys.stdout.flush()
                return jsonify([pid]),200
        except:
                return {'flag':'sthing is messed up'},500

# api to scale down by decreasing running slaves
def decrease_slaves_by(num):
        global zk

        slaves = get_slave_zookeeper(zk)
        client = docker.DockerClient(base_url='unix://var/run/docker.sock')
        #zk.stop()
        for i in range(num):
                pid=max(slaves.keys())
                slaves.pop(pid).stop()
                zk.set('zoo_waley_babu/watch/slave/'+str(pid),bytes("0",'utf-8'))
                zk.delete('zoo_waley_babu/slave/'+str(pid))

# scale up by increasing number of running containers
def increase_slaves_by(num):
        print("Helper for slave addition")
        sys.stdout.flush()
        #Helper for slave addition
        global zk
        slaves = get_slave_zookeeper(zk)
        print(slaves,type(slaves))
        sys.stdout.flush()
        print("length of slaves = ",len(slaves))
        sys.stdout.flush()
        l=slaves.keys()
        print(l)
        sys.stdout.flush()
        print("Display list of slaves")
        sys.stdout.flush()
        print("Try to get min pid znode")
        sys.stdout.flush()
        _id = slaves[min(slaves.keys())].id
        print("Got min pid znode")
        sys.stdout.flush()
        client = docker.DockerClient(base_url='unix://var/run/docker.sock')
        for i in range(num):
                print("Try to run a new container as slave")
                sys.stdout.flush()
                #Try to run a new container as slave
                container = client.containers.get(_id)
                new_image = container.commit(tag='latest')
                new_container = client.containers.run(new_image,command="python3 worker.py",
                              environment=["PYTHONUNBUFFERED=1"],
                              volumes={"/var/run/docker.sock":{"bind":"/var/run/docker.sock","mode":"rw"}},
                                                          network="cc_default",
                                                          links={'27ca921b4206':'rmq','bf1536023573':'zoo'},detach=True)
                print("ran a new container as slave")
                sys.stdout.flush()
                print("Initialize slaves successful")
                sys.stdout.flush()
                #ran a new container as slave
                #Initialize slaves successful
                pid=-1
                #Trying to extract pid of newly run container
                print("Trying to extract pid of newly run container")
                sys.stdout.flush()
                cont_id=str(new_container)
                cont_id=cont_id[12:-1]
                con=client.containers.get(cont_id)
                pid=con.attrs["State"]["Pid"]
                #print("Container with pid ",pid," will run no logs shall be shown (detach mode)")
                #Trying to insert a new znode for newly run slave
                print("Container with pid ",pid," will run no logs shall be shown (detach mode)")
                sys.stdout.flush()
                print("Trying to insert a new znode for newly run slave")
                sys.stdout.flush()
                zk.create('zoo_waley_babu/slave/'+str(pid),bytes(str(new_container),'utf-8'))
                zk.exists('zoo_waley_babu/slave/'+str(pid),callback_slave_crash)
                zk.create('zoo_waley_babu/watch/slave/'+str(pid),bytes("1",'utf-8'))
                print("Finished Inserting znode for newly run slave")
                sys.stdout.flush()

# timer count of HTTP read requests
def read_req_count():
    global read_count
    print("read count now ",read_count)
    sys.stdout.flush()
    req = math.ceil(read_count/20)
    if(req==0):
        req=1
    read_count = 0

    slaves = get_slave_zookeeper(zk)
    scale_factor =  req - len(slaves)

    if scale_factor > 0:
        increase_slaves_by(scale_factor)
    elif scale_factor < 0:
        decrease_slaves_by(-scale_factor)
    else:
        pass

# class to start a timer of 120 sec
class mytimerclass(Thread):
    def __int__(self):
        Thread.__init__(self)
    def run(self):
        while True:
            time = Timer(120,read_req_count)
            time.start()
            time.join()



# main program logic
if __name__=='__main__':
    clock = mytimerclass()
    app.run(host='0.0.0.0',port=80,debug=True,use_reloader=False)#localhost ka 9000
