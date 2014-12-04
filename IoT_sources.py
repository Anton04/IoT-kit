#!/bin/python
import pubsub
import json
import mosquitto
#from InfluxDBInterface import InfluxDBInterface
import pandas as pd
import uuid
from influxdb import InfluxDBClient
import re
import urlparse


class Basics():
    def __init__(self,Name = None,UUID = None):

        if Name != None:
            self.Name = Name
        else:
            self.Name = "Nameless"

        #Generate new UUID if nessesary
        if UUID != None:
            self.UUID = UUID
        else:
            self.UUID = uuid.uuid1()

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

    def to_File(self,Filename="~/source.json"):
        pass

        return

class Source(Basics):
    def __init__(self,Name=None,UUID=None,url=None):
        Basics.__init__(self,Name,UUID)
        self.url = url
        self.realtime = False

        return

    def to_dict(self):

        config = Source.to_dict(self)

        config["class"] = self.__class__.__name__

        config["url"] = url
        config["user"] = user
        config["passwd"] = passwd


        return config


#########################################
# SPECIFIC SOURCE IMPLEMENTATIONS BELOW.#
#########################################

#InfluxDB
class InfluxSource(Source,InfluxDBClient):
    def __init__(self,Name=None,UUID=None,url="localhost/test",user="root",passwd="root"):
        Source.__init__(self,Name,UUID,url,user,passwd)


        #Parse the url and add defaults if necessary.
        o = urlparse.urlparse(self.url)

        if o.hostname == None:
            hostname = "localhost"
        else:
            hostname = o.hostname

        if o.port == None:
            port = 8086
        else:
            port = o.port

        if o.username == None:
            username = "root"
        else:
            username = o.username

        if o.password == None:
            password = "root"
        else:
            password = o.password

        if o.path == None:
            db = "default"
        else:
            db = o.path.strip("/")

        #init database instance.
        InfluxDBClient.__init__(self,hostname,port,username,password,db)

    def spliturl(self,url):

        #Remove http:// of any
        url = re.sub("http://","",url)

        list = url.split("/")

        if len(list) != 2:
            raise Exception("Incorrect url for influxdb source.")

        if list[0].find(":") == -1:
            port = 8086
            hostname = list[0]
        else:
            parts = list[0].split(":")
            hostname = parts[0]

        return (hostname,list[1],port)

    def to_dict(self):

        config = Source.to_dict(self)

        config["class"] = self.__class__.__name__

        return config
