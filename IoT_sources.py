#!/bin/python
import pubsub
import json
import mosquitto
from InfluxDBInterface import InfluxDBlayer
import pandas as pd
import uuid
from influxdb import InfluxDBClient
#import re
import urlparse
import mosquitto


class Basics():



    def __init__(self,Name = None,UUID = None):

        if not hasattr(self.__class__,"name_counter"):
            self.__class__.name_counter = 0

        if Name != None:
            self.Name = Name
        else:
            self.Name = "Nameless %i" % self.__class__.name_counter
            self.__class__.name_counter += 1

        #Generate new UUID if nessesary
        if UUID != None:
            self.UUID = UUID
        else:
            self.UUID = str(uuid.uuid1())

        return

    def to_JSON(self):
        return json.dumps(self.to_dict())

    def from_JSON(self, jsondata):
        return self.from_dict(json.loads(jsondata))

    def to_dict(self):
        config = {}

        #config["class"] = self.__class__.__name__
        config["Name"] = self.Name
        config["UUID"] = self.UUID

        return config

    def from_dict(self):
        raise NotImplementedError

    def to_File(self,Filename="source.json"):
        file = open(Filename,"w")
        json.dump(self.to_dict(),file,indent=4)
        file.close()
        return

    def from_File(self,Filename="source.json"):
        file = open(Filename,"r")
        d = json.load(file)
        file.close()

        self.from_dict(d)


    def __str__(self):
        s = "<Class: %s | Name: %s | UUID: %s>" % (self.__class__.__name__,self.Name,self.UUID)
        return s

    def __repr__(self):
        return self.__str__()

#An collection of basics
class Collection:
    def __init__(self,class_names=[]):
        self.objects={}
        self.classes = class_names

    def add(self,obj):
        self.objects[obj.UUID] = obj
        try:
            vars(self)[obj.Name.replace(" ", "_")] = obj
        except:
            pass

    def remove(self,obj):
        del self.objects[obj.UUID]
        try:
            del vars(self)[obj.Name.replace(" ", "_")]
        except:
            pass

    def remove_by_UUID(self,UUID):
        self.remove(self.objects[UUID])

    def to_dict(self):

        l = []

        for item in self.objects:
            l.append(self.objects[item].to_dict())

        return l

    def remove_all(self):
        for item in self.objects:
            self.remove(self.objects[item])



    def from_dict(self,l):

        self.remove_all()

        #This needs to be a list and not actually a dictionary since its an collection.
        if type(l) != list:
            raise Exception("Collections needs a list")
        for item in l:
            if item["class"] in self.classes:
                obj = globals()[item["class"]]()
                obj.from_dict(item)
                self.add(obj)
        return


class Source(Basics):
    def __init__(self,Name=None,UUID=None,url=None):
        Basics.__init__(self,Name,UUID)
        self.url = url
        self.realtime = False

        #Parse the url and add defaults if necessary.
        try:
            o = urlparse.urlparse(self.url)
        except:
            raise Exception("Incorrect url for source.")

        self.s_type = o.scheme
        self.s_hostname = o.hostname
        self.s_port = o.port
        self.s_username = o.username
        self.s_password = o.password
        self.s_path = o.path
        self.s_params = o.params


        return

    def to_dict(self):

        config = Basics.to_dict(self)

        config["class"] = self.__class__.__name__

        config["url"] = self.url

        return config

    def from_dict(self,d):

        if True: #try:
            if d["class"] != self.__class__.__name__:
                raise Exception("Wrong class type")

            Name = d["Name"]
            UUID = d["UUID"]
            url = d["url"]

        #except:
         #   raise Exception("Error in source definition.")

        self.__init__(Name,UUID,url)

class RealtimeSource(Source):
    def __init__(self,Name=None,UUID=None,url=None):
        Source.__init__(self,Name,UUID,url)

        self.subscribers = {}

    def Subscribe(self,property,callback):
        if property not in self.subscribers:
            self.subscribers[property] = []

        self.subscribers[property].append(callback)

    def Unsubscribe(self,property,callback):
        #if property not in self.subscribers:
        #    return
        self.subscribers[property].remove(callback)


class QuerySource(Source):
    pass

class MultiSource(RealtimeSource,QuerySource):
    pass

#########################################
# SPECIFIC SOURCE IMPLEMENTATIONS BELOW.#
#########################################

#InfluxDB
class InfluxSource(QuerySource,InfluxDBlayer):

    Identifier = "influxdb"

    def __init__(self,Name=None,UUID=None,url="influx://root:root@localhost:8086/test"):

        QuerySource.__init__(self,Name,UUID,url)

        if self.s_hostname == None:
            self.s_hostname = "localhost"

        if self.s_port == None:
            self.s_port = 8086

        if self.s_username == None:
            self.s_username = "root"

        if self.s_password == None:
            self.s_password = "root"

        if self.s_path == None:
            self.s_path = "default"

        if self.s_type != "influx":
            raise Exception("Wrong type if source")

        self.s_db = self.s_path.strip("/")

        #init database instance.
        InfluxDBlayer.__init__(self,self.s_hostname,self.s_port,self.s_username,self.s_password,self.s_db)

#Mosquitto MQTT
class MQTTSource(RealtimeSource,mosquitto.Mosquitto):

    def __init__(self,Name=None,UUID=None,url="mqtt://localhost/#"):

        RealtimeSource.__init__(self,Name,UUID,url)
        mosquitto.Mosquitto.__init__(self,self.UUID)

        if self.s_hostname == None:
            self.s_hostname = "localhost"

        if self.s_type != "mqtt":
            raise Exception("Wrong type if source")

        if self.s_username != None:
            self.username_pw_set(self.s_username,self.s_password)


        self.will_set( topic =  "system/MQTTSource/" + self.Name, payload="Offline", qos=1, retain=False)
        print "Connecting"

        self.connect(self.s_hostname,keepalive=10)

        if self.s_path != None:
            self.subscribe(self.s_path, 0)

        self.on_connect = self.mqtt_on_connect
        self.on_message = self.mqtt_on_message
        self.publish(topic = "system/MQTTSource/"+ self.Name, payload="Online", qos=1, retain=True)

        self.loop_start()


    def mqtt_on_connect(self, selfX,mosq, result):
        print "Class %s, instance: %s: Connected!" % (self.__class__.__name__,self.Name)

        if self.s_path != None:
            self.subscribe(self.s_path, 0)

    def mqtt_on_message(self, selfX,mosq, msg):
        #print("RECIEVED MQTT MESSAGE: "+msg.topic + " " + str(msg.payload))

        if msg.topic in self.subscribers:
            #print "Hit"
            callback_list = self.subscribers[msg.topic]

            for callback in callback_list:

                if True: #try:
                    callback(msg.topic,msg.payload)
                #except:
                    #

    def Subscribe(self,property,callback):

        #Add to callback list
        RealtimeSource.Subscribe(self,property,callback)

        #Add to mosquitto
        self.subscribe(property)

    def __del__(self):
        self.loop_stop()
        time.sleep(5)
        self.loop_stop(True)

















