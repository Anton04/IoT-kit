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

    def to_File(self,Filename="~/source.json"):
        pass

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


#########################################
# SPECIFIC SOURCE IMPLEMENTATIONS BELOW.#
#########################################

#InfluxDB
class InfluxSource(Source,InfluxDBClient):
    def __init__(self,Name=None,UUID=None,url="mqtt://root:root@localhost:8086/test"):

        Source.__init__(self,Name,UUID,url)

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

        self.s_db = self.s_path.strip("/")

        #init database instance.
        InfluxDBClient.__init__(self,self.s_hostname,self.s_port,self.s_username,self.s_password,self.s_db)

    def to_dict(self):

        config = Source.to_dict(self)

        config["class"] = self.__class__.__name__

        return config
