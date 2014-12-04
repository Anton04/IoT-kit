#!/bin/python
from influxdb import InfluxDBClient
import json
import pandas as pd
import numpy


class InfluxDBlayer(InfluxDBClient):  


  def ProcessSeriesParameter(self,series):
    #Handle indexing instead of name
    if type(series) == int:
        Series = self.ListSeries()
        #Series.sort()
        series = Series[series]
        print "Series %s selected" % series

    #If its a list process each element and concat to commma separated string 
    elif type(series) == list:
        combined = ""
        for part in series:
            
            #Recursive call
            ppart = self.ProcessSeriesParameter(part)
    	
            combined += ppart + ", "

        return combined[:-2]

    return series

  def ProcessPropParameter(self,properties):
    if type(properties) == type([]):
        combined = ""
        for part in properties:
            combined += part + ", "

        return combined[:-2]

    return properties

  def GetLastTimestamp(self,series,property = "*",time_precision='m'):
    return self.GetLastValue(series,property,time_precision)[0]

  def GetLastTimeStamp(self,FluxId):
      return self.GetLastValue(FluxId,"*","m")[0]/1000.0

  def GetFirstTimestamp(self,series,property = "*",time_precision='m'):
    return self.GetFirstValue(series,property,time_precision)[0]

  def ListSeries(self):
    res = self.query("list series")

    ret = []

    for series in res[0]["points"]:
       ret.append(series[1])

    ret.sort()

    return ret

  def GetProperties(self,series):

    try:
        res = self.query("select * from \"%s\" limit 1" % series )
    except Exception, err:
        if err.message.find("400: Couldn't find series:") != -1:
            return []
        else:
            raise err
    if res == []:
       return []
    return res[0]["columns"][2:]




  def GetDataAfterTime(self,series,properties="*",timestamp=None,limit=10,time_precision='s'):
    
    #Handle indexing instead of name
    series = self.ProcessSeriesParameter(series)    

    #If no time specified start from the beginning. 
    if timestamp == None:
        timestamp = self.GetFirstTimestamp(series,properties,'m')/1000.0

    pproperties = self.ProcessPropParameter(properties)

    #DUE to a bug in influx db we mush query all properties and then select the ones we want. 
    qstring = "select %s from \"%s\" where time > %i order asc limit %i" % ("*",series,int(timestamp*1000000000),limit)

    try:
        res = self.query(qstring,time_precision)
    except Exception, err:
        if err.message.find("400: Couldn't find series:") != -1:
            return None
        else:
            raise err

    #print res

    df =  self.ResultToDataframe(res)

    if type(df) != pd.DataFrame:
      return None

    if type(properties) == list:
      #Add missing parameters
      for prop in properties:
        if not prop in df.columns:
          df[prop] = float("NaN")

      df = df[properties]
    elif properties != "*":
      df = df[[properties]]

    #Return empty 
    if type(df) != pd.DataFrame:
      return pd.DataFrame()

    #Cut lenght  
    if df.shape[0] > limit:
      df = df.iloc[:limit]

    #print "*"*20
    #print df

    if type(df) == pd.core.frame.DataFrame:
    	df.series = series
    	df.properties = properties 

    return df




  def GetNextNRows(self,df,N=100,time_precision='s'):

    #Get the last timestamp
    lasttime = pd.Series(df.index).max()   

    return self.GetDataAfterTime(df.series,df.properties,lasttime,N,time_precision)

  def ResultToDataframe(self,result):
    if result == []:
      return None

    ret = []

    for res in result:	

      df = pd.read_json(json.dumps(res["points"]))
      df.columns = res["columns"]
      df.index = df["time"]

      try:
        df = df.drop(["sequence_number"],1)
      except:
          pass

      df = df.drop(["time"],1)

      df = df.reset_index().groupby(df.index.names).first()

      df.series = res["name"]

      ret.append(df)

    if len(ret) == 1:
        return ret[0]

    #Induvidualiase properties
    for df in ret:
	df.columns = df.series + "/" + df.columns

    #Merge all dataframes
    
    df_ret = ret[0]

    for df in ret[1:]:
        df_ret = df_ret.join(df, how='outer')

    return df_ret

  def QueryDf(self,query,resolution='s'):
      try:
        df = self.query(query,resolution)
      except Exception, err:
        if err.message.find("400: Couldn't find series:") != -1:
            return None
        else:
            raise err


      return self.ResultToDataframe(df)


  def GetDataPeriod(self,series,properties,start,lenght=60*60*24*7,limit=1000,time_precision='s'):
    series = self.ProcessSeriesParameter(series)

    start = int(start*1000000)
    lenght = int(lenght*1000000)
    stop = start + lenght

    properties = self.ProcessPropParameter(properties)

    qstring = "select %s from \"%s\" where time > %iu and time < %iu limit %i" %(properties,series,start,stop,limit)

    try:
        res = self.query(qstring,time_precision)
    except Exception, err:
        if err.message.find("400: Couldn't find series:") != -1:
            return None
        else:
            raise err

    return self.ResultToDataframe(res)


	

  def GetPropertiesPartiallyMatchingAbutNotB(self,series,keyA,keyB):

    series = self.ProcessSeriesParameter(series)

    res = self.GetPropertiesPartiallyMatching(series,keyA)

    ret = []

    for property in res:
       if property.find(keyB) != -1:
           continue
       ret.append(property)

    return ret


  def GetPropertiesPartiallyMatching(self,series,key):
    series = self.ProcessSeriesParameter(series)
    properties = self.GetProperties(series)

    ret = []

    for property in properties:
        if property.find(key) == -1:
            continue

        ret.append(property)

    return ret

  def GetLastValue(self,series,properties="*",time_precision='m'):
    series = self.ProcessSeriesParameter(series)
    properties = self.ProcessPropParameter(properties)



    try:
        result = self.query('select %s from \"%s\" order desc limit 1;' % (properties,series), time_precision)
    except Exception, err:
        if err.message.find("400: Couldn't find series:") != -1:
            return (None,None)
        else:
            raise err

    #print result

    timestamp = 0 

    try:
      #If serveral results return the last one. 
      for item in result:
        t_prop = item["points"][0][2:]
        t_timestamp = item["points"][0][0]

        if t_timestamp > timestamp:
            timestamp = t_timestamp
            ret = t_prop


      #ret = result[0]["points"][0][2:]
      #time = result[0]["points"][0][0]
      if len(ret) == 1:
          return (timestamp,ret[0])
      elif len(ret) == 0:
          return (None,None)
      else:
          return (timestamp,ret)
    except:
      return (None,None)

  def GetPrecedingValue(self,At,series,properties="*",time_precision='m'):

    series = self.ProcessSeriesParameter(series)
    properties = self.ProcessPropParameter(properties)

    query = "select %s from \"%s\" where time < %i limit 1;" % (properties,series,int(At * 1000000000))

    try:
        result = self.query(query, time_precision)
    except Exception, err:
        if err.message.find("400: Couldn't find series:") != -1:
            return (None,None)
        else:
            raise err

    timestamp = 0 

    try:
      #If serveral results return the last one. 
      for item in result:
        t_prop = item["points"][0][2:]
        t_timestamp = item["points"][0][0]

        if t_timestamp > timestamp:
          timestamp = t_timestamp
          ret = t_prop


      #ret = result[0]["points"][0][2:]
      #time = result[0]["points"][0][0]
      if len(ret) == 1:
          return (timestamp/1000.0,ret[0])
      elif len(ret) == 0:
          return (None,None)
      else:
          return (timestamp/1000.0,ret)
    except:
      return (None,None)

  def GetFirstValue(self,series,properties="*",time_precision='m'):
    series = self.ProcessSeriesParameter(series)
    properties = self.ProcessPropParameter(properties)

    try:
        result = self.query('select %s from \"%s\" order asc limit 1;' % (properties,series), time_precision)
    except Exception, err:
        if err.message.find("400: Couldn't find series:") != -1:
            return (None,None)
        else:
            raise err
    #print result

    timestamp = 9999999999999999

    try:
      #If serveral results return the first one. 
      for item in result:
        t_prop = item["points"][0][2:]
        t_timestamp = item["points"][0][0]

        if t_timestamp < timestamp:
                timestamp = t_timestamp
                ret = t_prop
   
      if len(ret) == 1:
          return (timestamp,ret[0])
      elif len(ret) == 0:
          return (None,None)
      else:
          return (timestamp,ret)
    except:
      return (None,None)

  def Replace(self,series,DataFrame,time_precision = 's',Compressed=True):
    series = self.ProcessSeriesParameter(series)

    From = DataFrame.index[0]
    To = DataFrame.index[-1]

    self.ClearPeriod(series,From,To,time_precision)

    if Compressed:
      self.SaveCompressed(series,DataFrame,time_precision)
    else:  
      self.Save(series,DataFrame,time_precision)

  def ClearPeriod(self,series,From,To,time_precision = 's'):
    series = self.ProcessSeriesParameter(series)

    if From > To:
      tmp = From
      From = To
      To = tmp

    if time_precision == 's':
        factor = 1000000000
    elif time_precision == 'm':
        factor = 1000000
    elif time_precision == 'u':
        factor = 1000
    else: 
        return 

    try:
        self.query("delete from \"%s\" where time > %i and time < %i" %(series,From*factor,To*factor) )
    except Exception, err:
        if err.message.find("400: Couldn't find series:") != -1:
            return
        else:
            raise err

    

  def Save(self,series,DataFrame,time_precision = 's'):
    series = self.ProcessSeriesParameter(series)

    #Series name
    #series = FeedId + "/raw_data" 

    rows = 0

    #Save each row
    for r in range(0,DataFrame.shape[0]):
      timestamp = DataFrame.index[r]
      column = ["time"]
      data = [int(timestamp)]


      #Iterate each value and remove NANs
      for c in range(0,DataFrame.shape[1]):
        key = DataFrame.columns[c]
        value = DataFrame.iloc[r,c]
        #print value 
        #print timestamp,key
 
        if pd.isnull(value):
          continue

        #Add key
        column.append(key)
        data.append(value)

      #If there where only nan on this row continue to next row. 
      if len(column) == 1:
        continue

      fdata = [{
          "points": [data],
          "name": series,
          "columns": column
          }]

      self.write_points_with_precision(fdata,time_precision)

      rows += 1

    return rows


  def SaveCompressed(self,series,DataFrame,time_precision = 's'):
    series = self.ProcessSeriesParameter(series)

    #Series name
    #series = FeedId + "/raw_data" 

    rows = 0

    LastValues = {}

    #Save each row
    for timestamp in DataFrame.index:
      column = ["time"]
      data = [int(timestamp)]


      #Iterate each value and remove NANs
      for key in DataFrame.columns:
        value = DataFrame.loc[timestamp,key]
        #print value 
        #print timestamp,key

        if pd.isnull(value):
          continue

	if not key in LastValues:
	  LastValues[key] = value
        elif LastValues[key] != value:
          LastValues[key] = value
	else:
          continue

        #Add key
        column.append(key)
        data.append(value)

      #If there where only nan on this row continue to next row. 
      if len(column) == 1:
        continue

      fdata = [{
          "points": [data],
          "name": series,
          "columns": column
          }]

      self.write_points_with_precision(fdata,time_precision)

      rows += 1

    return rows

  def SendToInfluxDB(self,df,FeedId):
    #Series name
    #series = FeedId + "/raw_data"

    rows = 0

    #Save each row
    for i in range(0,df.shape[0]):
      timestamp = df.irow(i)[0]
      column = ["time"]
      data = [int(timestamp*1000)]


      #Iterate each value and remove NANs and fix floats.
      for j in range(1,df.shape[1]):
        value = df.iloc[i,j]

        #Float
        if type(value) == str:
            if value.find(",") != -1:
                value = float(value.replace(",","."))
        #Nan
        elif numpy.isnan(value):
            continue
        #Add key
        column.append(df.keys()[j])
        data.append(value)

      #If there where only nan on this row continue to next row.
      if len(column) == 1:
        continue

      fdata = [{
          "points": [data],
          "name": FeedId,
          "columns": column
          }]

      self.write_points_with_precision(fdata,"m")

      rows += 1

    return rows




#********************************************


#Class implementing access to influxDB    
class InfluxDBInterface():
  def __init__(self,config_param="influxInterfaceCredentials.json"):
    
    if type(config_param) == type(""):
      #Load database credentials from file
      fp = open(config_param,"r")
      self.config = json.load(fp)
      fp.close()
    elif type(config_param) == type({}):
      self.config = config_param
    
    #Connect
    #print self.config

    self.databases = {}

    for db in self.config:
    	database = InfluxDBlayer(db["host"], db["port"], db["user"], db["password"], db["database"])
	self.databases[db["database"]]=database

    return 

  def GetDatabaseFromTopicPath(self,topic):
    dbname = topic.split("/")[0]
    dbname = dbname.strip("/")

    if dbname in self.databases:
	return self.databases[dbname]
    
    return None

  def GetLastTimeStamp(self,topic):

    try:
        result = self.GetDatabaseFromTopicPath(topic).query('select time from \"%s\" order desc limit 1;' % topic, time_precision='m')
    except Exception, err:
        if err.message.find("400: Couldn't find series:") != -1:
            return None
        else:
            raise err

    try:
      return float(result[0]["points"][0][0])/1000.0
    except:
      return 0.0

  def GetLastTimeStamp2(self,database,series):

    result = self.databases[database].query('select time from \"%s\" order desc limit 1;' % series, time_precision='m')

    try:
      return float(result[0]["points"][0][0])/1000.0                
    except:
      return 0.0

  def GetLastValue3(self,database,series,property):
  
    result = self.databases[database].query('select %s from \"%s\" order desc limit 1;' % (property,series), time_precision='m')
    
    #print result
    
    try:
      ret = result[0]["points"][0][2:]
      if len(ret) == 1:
          return ret[0]
      elif len(ret) == 0:
          return None
      else:
          return ret
    except:
      return None

  def GetLastTimeStamp3(self,database,series,property):

    result = self.databases[database].query('select %s from \"%s\" order desc limit 1;' % (property,series), time_precision='m')

    #print result 

    try:
      return float(result[0]["points"][0][0])/1000.0
    except:
      return 0.0

  def GetLastValue3(self,database,series,property):

    result = self.databases[database].query('select %s from \"%s\" order desc limit 1;' % (property,series), time_precision='m')

    #print result

    try:
      ret = result[0]["points"][0][2:]
      if len(ret) == 1:
          return ret[0]
      elif len(ret) == 0:
          return None
      else: 
          return ret
    except:
      return None


  def listdataseries(self):
    series = []
    for dbname in self.databases:
        database = self.databases[dbname]
	result = database.query("list series;")

	for item in result:
            series.append(dbname + "/" +item["name"])

    return series

