#!/bin/python

from IoT_sources import *
from IoT_feeds import *

__author__ = 'anton'

#Class implementing a feed universe defined by feed definitions a realtime data access and longterm strorage.
class Universe(Basics):
    def __init__(self,Name = None,UUID = None):

        Basics.__init__(self,Name,UUID)

        self.Feeds = Collection()
        self.Sources = Collection(["InfluxSource"])
        self.Drivers = Collection()


        return


    def to_dict(self):
        d = Basics.to_dict(self)
        d["Sources"] = self.Sources.to_dict()
        d["Feeds"] = self.Feeds.to_dict()
        d["Drivers"] = self.Drivers.to_dict()

        return d

    def from_dict(self,d):

        Basics.from_dict(self,d)
        self.Feeds.from_dict(d["Feeds"])
        self.Sources.from_dict(d["Sources"])
        self.Drivers.from_dict(d["Drivers"])

