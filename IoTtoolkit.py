#!/bin/python
#from pubsub import pub
import json
import mosquitto 
from InfluxDBInterface import InfluxDBInterface
import pandas as pd
import uuid

#Abstraction layer for accessing long time storage
class LTSInterface():
  def __init__(self):
    return
    
#Abstraction layer for accessing realtime system    
class RTSInterface():
  def __init__(self):
    return

#Abstraction layer for accessing a feed repository    
class REPInterface():
  def __init__(self):
    return


    
#Class implementing access to MQTT via mosquitto    
class Mosquitto_RTSLink(RTSInterface):
  def __init__(self):
    return
  
  


#Class implementing a feed universe defined by feed definitions a realtime data access and longterm strorage.
class Universe:
  def __init__(self,Defenition="Universe.json",AutoloadFeeds = True,Active = False):
    
    self.Active = Active
    self.Feeds = []
    
    if Defenition == None:
      #Produce new Universe
      #Not implemented
      return
      
    elif type(Defenition) == type(""):
      File = open(Defenition,"r")
      self.DefDict = json.load(File)
      File.close()
      self.ID = DefDict["ID"]
      
      if AutoloadFeeds:
        self.LoadFeedsFromFile(DefDict["Feeds"])
      
    elif type(Defenition) == type({}):
      self.DefDict = Defenition

    self.Name = self.DefDict["Name"]
    self.ID = self.DefDict["ID"]
      
    if self.DefDict["LTS TYPE"] == "InfluxDB":
      self.LTS = InfluxDBInterface(self.DefDict["LTS DATA"])

    if AutoloadFeeds == True:
      self.FeedFile = self.DefDict[FEEDS]
      self.LoadFeedsFromFile(self.FeedFile)
      
    return
  
  def GetFeedFromDefenition(self,Defenition):
    if type(Defenition) == type(""):
      File = open(Defenition,"r")
      self.DefDict = json.load(File)
      File.close()
    elif type(Defenition) == type({}):
      self.DefDict = Defenition
      
  def LoadFeedsFromFile(self,FileName):
    File = open(FileName,"r")
    FeedsList = json.load(File)
    File.close()
    self.LoadFeedsFromList(FeedList)
    
  def LoadFeedsFromList(self,FeedsDsc):

    FeedList = FeedsDsc["FEEDLIST"]

    #Check if Feeds are compatible with this universe.
    if FeedsDsc["Universe"] != self.ID:
      print "ERROR: Feed description file belong to other universe"
      return

    #Add all the feeds
    for FeedDsc in FeedList:
      self.Feeds.append(Feed(FeedDsc,self))
      
    return
  
  
  def SaveFeedsToFile(self,FileName):
    return









#///////////////

class FeedBuffer():
  def __init__(self,Feed,Size,AutoDecompress=True):
    self.Feed = Feed
    self.Size = Size
    self.EOF = False
    self.Data = pd.DataFrame(columns = self.Feed.DataStreams.columns)
    self.AutoDecompress = AutoDecompress

    #
    #self.Seek(Position)

  def Next(self):

    #Load raw buffer
    self.Data = self.Feed.LoadBuffer(self.EndPosition,self.Size)

    #Update position
    self.Position = self.EndPosition
    self.Values = self.NextValues

    #New position
    (self.EndPosition,self.NextValues) = self.NextPointerAndValues()

    #Decompress
    if self.AutoDecompress:
      self.Decompress()

    #Check if we are at the end of the feed.
    if self.Position == self.Data.index[-1]:
      self.EOF = True
      return None

    return self.Data


  def Compress(self):
    
    #Compress everything except first row.   
    self.Data = (self.Data.diff()!= 0).replace(False,float("NaN")) * self.Data

    #Check with values if first row can be compressed.
    FirstRow = self.Data.iloc[0]

    for Name in FirstRow.index:
      Value =  FirstRow[Name]
      Timestamp = FirstRow.name
    
      #If an original point is on this timestamp leave it.
      if self.Values[Name]["Timestamp"] == Timestamp:
        continue
        
      #If its an other value than the last known also save it. 
      if Value != self.Values[Name]["Value"]:
        continue
        
      #If the same value as the one before remove it. 
      FirstRow[Name] = float("NaN")

    return self.Data


  def Decompress(self):

    RowsToBeDecompressed = self.Feed.DataStreams.columns[list(self.Feed.DataStreams.loc["Compressed"].values)]

    #Update first row with missing data.
    #self.Data.iloc[0] = self.NextValues.loc["Value"].values
    for Value in self.Values:
        self.Data.loc[self.Data.index[0],Value] = self.Values[Value].Value

    #Forwardfill the rest
    self.Data[RowsToBeDecompressed] = self.Data[RowsToBeDecompressed].ffill()

    return self.Data

  def Save(self):
    #Warn if duplicates.
    for Key in self.Feed.NameDict:
      if len(self.Feed.NameDict[Key]) > 1:
        print "Warning columns with the same source exists. Saving all of them will give unpredictable results."
        break;


    #Split up into induvidial dataframes in accorance with source dictionary.
    for Key in self.Feed.SourceToNamesDict:
      Names = self.Feed.SourceToNamesDict[Key]
      Database = Key[0]
      Serie = Key[1]

      #Get the ones belonging to this source. 
      df = self.Data[Names]

      #Write over old data in the database
      Database.Replace(Serie,dfcomp,'s',False) 
      
    return 

  def Seek(self,Position = 0):

    if Position == 0:
      Position = self.Feed.StartsAt()
    
    self.Position = Position
    self.EndPosition = Position

    df = self.Feed.GetValuesAt(Position)

    #Align columns with the stream descriptor.
    df = df.reindex_axis(self.Feed.DataStreams.columns, axis=1)

    StartsAt = df.loc["Timestamp"].min()

    self.NextValues = df

    self.Next()

    return self.Data

#This function look inside the current buffer to see if we can move the Seek point without calling the database.
#This should be used when moving the seek point small steps. For example when re-sampeling the stream.
  def MoveSeek(self,Period):

    #TODO

    NewPosition = self.Position + Period

    #Iterate the columns.
    for item in self.Feed.DataStreams.columns:
        break

    return


  def NextPointerAndValues(self):

    Values = pd.DataFrame(index = ["Timestamp","Value"])

    for (Name,Column) in self.Data.iteritems():
      Stream = Column.dropna()

      #If only NaNs use old one. 
      if len(Stream) == 0:
        Values[Name] = self.Values[Name]
        continue

      StreamTime = Stream.index[-1]
      StreamValue = Stream.values[-1]

      Values[Name] = [StreamTime,StreamValue]

    #Calculate new pointer data. 
    Pointer = self.Data.index[-1]

    return (Pointer,Values)

  def Reset(self):
    return self.Seek(0)

  def __repr__(self):
    return self.Data.__repr__()

  #def _repr_pretty_(self):
  #  return self.Data._repr_pretty_()


#***************

class ResampleFeedBuffer(FeedBuffer):
  def __init__(self,Feed,Start,Period,Samples):

    FeedBuffer.__init__(self,Feed,10,AutoDecompress=False)

    #self.Type = Type
    self.ResamplePeriod = Period
    self.ResampleColumns = {}
    self.ResampleBufferSize = Samples
    self.ResampleBuffer = None
    self.ResampleStart = Start
    self.ResampleStop = (Samples*Period)+Start




  def AddResampleColumn(self,Name,Inputs,Function):
    self.ResampleColumns[Name] = (Inputs,Function)
    return

  def ResampleFrames(self,Start,Stop,Period):

        self.ResampleStart = Start
        self.ResampleStop = Stop
        self.ResamplePeriod = Period

        Values = pd.DataFrame(columns = self.Feed.DataStreams.columns)
        Times = pd.DataFrame(columns = self.Feed.DataStreams.columns)

        TimeStamp = Start - Period

        while(TimeStamp <= Stop + Period):

            #Loop through all.
            for (Name,Properties) in self.Feed.DataStreams.iteritems():
                  Database = Properties["Database"]
                  Serie = Properties["Serie"]
                  Property = Properties["Property"]


                  (StreamTime,StreamValue) = Database.GetPrecedingValue(int(TimeStamp),Serie,Property)

                  if StreamTime != None:
                        Values.loc[int(TimeStamp),Name] = StreamValue
                        Times.loc[int(TimeStamp),Name] = TimeStamp - StreamTime




            TimeStamp += Period


        return Values,Times

  def Interpolate(self):

        (Values,Times) = self.ResampleFrames(self.ResampleStart,self.ResampleStop,self.ResamplePeriod)

        self.ResampleValuesBefore = Values
        self.ResampleTimesBefore = Times

        Data = pd.DataFrame(columns = self.ResampleColumns.keys())

        for item in self.ResampleColumns:

            Data[item] = self.ResampleColumns[item][1](self.ResampleColumns[item][0]).iloc[1:-1]

        self.Interpolation = Data

        return Data


  def InterpolatePowerFromCounter(ResampleObj,Inputs):

        s1 = ResampleObj.ResampleValuesBefore[Inputs]
        return s1.diff().shift(-1)

  def InterpolateCounter(ResampleObj,Inputs):

        s1 = ResampleObj.ResampleValuesBefore[Inputs]
        s2 = ResampleObj.ResampleTimesBefore[Inputs]

        #return (s2 < 3600).replace(False,float("nan")) * s1
        return s1



  def Save(self,database,series):
      print "Saving is disabled for this class"
      return





#***************






#Class implementing a feed     
class Feed():
  def __init__(self,Universe = None):

    self.Universe = Universe

    self.DataStreams = pd.DataFrame(index = ["Database","Serie","Property","Timeout","TOMarker","Type","Compressed"])
    self.Pointer = None
    self.Buffer = None

  def GetResampleBuffer(self,Start,Period,Samples):

    fb = ResampleFeedBuffer(self,Start,Period,Samples)
    return fb



  def AddStream(self,Name = None,Database = None, Series = None,Property = None,Timeout = None,TOMarker = None, Type = None,Compressed = True,Single= True):
    if Name == None:
      Name = uuid.uuid1()


    self.DataStreams[Name] = pd.Series([Database,Series,Property,Timeout,TOMarker,Type,Compressed],index=self.DataStreams.index)

    if Single:
        self.UpdateSourceAndNameDirectories()

    return self.DataStreams

  def CombineStreamsFromMulipleSources(self,Name = None,Database = None, Series = None,Property = None,Timeout = None,TOMarker = None, Type = None,Compressed = True,Single = True):

    NameList = []

    for Serie in Series:
      Name2 = Name + str(self.DataStreams.shape[1])
      self.AddStream(Name2,Database, Serie,Property,Timeout,TOMarker, Type,Compressed,Single = False)
      NameList.append(Name2)


    if Single:
        self.UpdateSourceAndNameDirectories()
    
    return NameList

  def AddSeveralStreamsWithinSameSource(self,Database = None, Serie = None,Properties = None,Timeout = None,TOMarker = None, Type = None,Compressed = True):

    NameList = []

    for Prop in Properties:
      Name = Prop 

      while Name in self.DataStreams.columns:
        Name += str(self.DataStreams.shape[1])

      self.AddStream(Name,Database,Serie,Prop,Timeout,TOMarker, Type,Compressed)
      NameList.append(Name)
    
    return NameList


  def RemoveStream(self,Id = None):
    if Id == None:
      return False

    try:

      if type(Id) == str:
        Stream = self.DataStreams[Id]
        self.DataStreams = self.DataStreams.drop([Id],axis=1)
      elif type(Id) == int:
        Stream = self.DataStreams[self.DataStreams.columns[Id]]
        self.DataStreams = self.DataStreams.drop(self.DataStreams.columns[Id],axis=1)
    except KeyError:
      return False

    self.UpdateSourceAndNameDirectories()

    return Stream

  def UpdateSourceAndNameDirectories(self):

    SourceDict = {}
    NameDict = {}
    SourceToNamesDict = {}
 
    #Sort dbs and series.
    for Stream in self.DataStreams.iteritems():
      Database = Stream[1]["Database"]
      Series = Stream[1]["Serie"]
      Property = Stream[1]["Property"]
      Name = Stream[0]

      #Sources
      Key = (Database,Series)

      if not Key in SourceDict:
        SourceDict[Key] = []
        SourceToNamesDict[Key] = []

      SourceDict[Key].append(Property)
      SourceToNamesDict[Key].append(Name)

      #Names
      Key2 = (Database,Series,Property)

      if not NameDict.has_key(Key2):
        NameDict[Key2] = []
      
      NameDict[Key2].append(Name)
      

    #Save
    self.SourceDict = SourceDict
    self.NameDict = NameDict
    self.SourceToNamesDict = SourceToNamesDict

    return (SourceDict,NameDict)


  def LoadBuffer(self,Start=None,Length=10,Reverse = False):

    Sources = self.SourceDict

    #Make a complete copy of names. 
    Names = self.NameDict.copy() 

    for Key in self.NameDict:
      Names[Key] = self.NameDict[Key][:]
      Names[Key] = self.NameDict[Key][:]


    #Load each source into a frame. 
    Frames = []

    for Key in Sources:
      (Database,Series) = Key
      if Reverse:
        Res = Database.GetDataBeforeTime(Series,Sources[Key],Start,Length)
      else:
        Res = Database.GetDataAfterTime(Series,Sources[Key],Start,Length)

      if type(Res) != pd.DataFrame:
        continue

      #Replace properties with names
      NameList = []

      for Property in Res.columns:
        NameList.append(Names[((Database,Series,Property))].pop())

      Res.columns = NameList

      Frames.append(Res)

    #Join all frames
    MainFrame = Frames[0]

    for Frame in Frames[1:]:
        MainFrame = MainFrame.join(Frame, how='outer')

    if MainFrame.shape[0] > Length:
      MainFrame = MainFrame.iloc[:Length]

    return self.AlignFrame(MainFrame)

  def GetValuesAt(self,TimeStamp=None):

    Values = pd.DataFrame(index = ["Timestamp","Value"])

    #Loop through all. 
    for (Name,Properties) in self.DataStreams.iteritems():
      Database = Properties["Database"]
      Serie = Properties["Serie"]
      Property = Properties["Property"]


      (StreamTime,StreamValue) = Database.GetPrecedingValue(TimeStamp,Serie,Property)

      if StreamTime == None:
        StreamValue = float("NaN")
      #  (StreamTime,StreamValue) = Database.GetSuccedingValue(Serie,Property,TimeStamp)

      Values[Name] = [StreamTime,StreamValue]

    return Values



  def StartsAt(self): 

    FirstTimestamp = 9999999999999
    Timestamp = 0

    #Loop through all and look for the stream that starts first. 
    for (Name,Properties) in self.DataStreams.iteritems():
      Database = Properties["Database"]
      Serie = Properties["Serie"]
      Property = Properties["Property"]

      Timestamp = Database.GetFirstTimestamp(Serie,Property)

      if Timestamp < FirstTimestamp:
        FirstTimestamp = Timestamp
      
    return Timestamp /1000.0


  #Align columns with the stream decriptor
  def AlignFrame(self,df):

    df = df.reindex_axis(self.DataStreams.columns, axis=1)

    return df

  #Returns a buffer from where the pointer was set and updates the pointer. 
  def GetBuffer(self,Size = 10,AutoDecompress = True):

    fb = FeedBuffer(self,Size,AutoDecompress)
    return fb


#Class implementing a stream.    
class Stream:
  def __init__(self,Defenition,Feed):
    self.Feed = Feed
    self.ID = Defenition["ID"]
    self.Name = Defenition["Name"]
    self.RTS_ID = Defenition["RTS ID"]
    self.LTS_ID = Defenition["LTS ID"]

    if Defenition.has_key("KeepAlive"):
      self.KeepAlive = Defenition["KeepAlive"]

    if Defenition.has_key("Derrived"):
      self.Derrived = Defenition["Derrived"]
    else:
      self.Derrived = False

    if Defenition.has_key("Function"):
      self.Function = Defenition["Function"]
    else:
      self.Function = False

    #self.Expires = "1y"
    #self.SampleRate = Defenition["Name"] None
    #self.Threshhold = Defenition["Name"] None
    
    return
  
  def GetStreamDefenition(self):
    Defenition = {}
    Defenition["Name"] = self.Name
    Defenition["RTS ID"] = self.RTS_ID
    Defenition["LTS ID"] = self.LTS_ID
    
    return Defenition
    
    
  
if __name__ == "__main__":
  MyUniverse = Universe(UniverseDef,AutoloadFeeds = True)
  
  MyUniverse.LoadFeedsFromFile("SLBFeeds.json")
  
  MyUniverse.GetFeedByName("").UpdateLTS()
  
  
  
  
  
  
  
