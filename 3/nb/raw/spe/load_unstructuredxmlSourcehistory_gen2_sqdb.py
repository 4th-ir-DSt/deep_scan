# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_unstructuredxmlSourcehistory_gen2_sqdb</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Identify any new data in unstructuredxmlSource raw source historic objects and store the dateTime in the metadata database </td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2020/06/17</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC ## Notebook Parameters
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Parameter Name</th>
# MAGIC     <th>Parameter Description</th>
# MAGIC     <th>Example Parameter</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchId</td>
# MAGIC     <td>@batchId to retrieve batch details</td>
# MAGIC     <td>@batchId = 1</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchTaskId</td>
# MAGIC     <td>@batchTaskId to retrieve batchTask details</td>
# MAGIC     <td>@batchTaskId = 1</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>adfPipelineName</td>
# MAGIC     <td>@adfPipelineName to retrieve DataFactory pipelinename details</td>
# MAGIC     <td>@adfPipelineName = 'testpipepline'</td>
# MAGIC   </tr>
# MAGIC    <tr>
# MAGIC     <td>notebookName</td>
# MAGIC     <td>@notebookName to retrieve notebookname </td>
# MAGIC     <td>@notebookName = 'testnotebook'</td>
# MAGIC   </tr>
# MAGIC     <tr>
# MAGIC     <td>clusterId</td>
# MAGIC     <td>@clusterId to retrieve cluster details </td>
# MAGIC     <td>@clusterId = '12345'</td>
# MAGIC   </tr>
# MAGIC    <tr>
# MAGIC     <td>sourceId</td>
# MAGIC     <td>@sourceId to retrieve source details </td>
# MAGIC     <td>@sourceId = '1'</td>
# MAGIC   </tr>
# MAGIC   </table>
# MAGIC 
# MAGIC ## Change Log
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Date</th>
# MAGIC     <th>Changed By</th>
# MAGIC     <th>Change Description</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2019/01/01</td>
# MAGIC     <td>Framework/td>
# MAGIC     <td>updated getHistoricRawEventhubObjectLocations function call to include batch_id</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC 
# MAGIC ##General Standards applying to each notebook
# MAGIC <ol>
# MAGIC   <li>Above information completed</li>
# MAGIC   <li>Deployed to approriate workspace folder path</li>
# MAGIC   <li>Commited to source control</li>
# MAGIC   <li>Code tested</li>
# MAGIC   <li>Only include libraries and imports that are being used - i.e. don't include something if it is not used</li>
# MAGIC   <li>Code commented</li>
# MAGIC   <li>Code Peer reviewed</li>
# MAGIC </ol>

# COMMAND ----------

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run Miscellaneous Function notebook
# MAGIC %run ../../util/spe/misc_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  from datetime import datetime, timedelta
  import pyspark.sql.functions as F
  from pyspark.sql.functions import lit,split,col 
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
  from pyspark.sql import Row
  import os
  import numpy as np
  import heapq
  import json

except Exception as e:
  errorMessage = "Exception occurred while importing modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise variables
try:
  #variable for current_time
  currentTs=datetime.now()
  #get the last 3 positions of the microseconds as we need to remove them
  currentTsMicroseconds = int(str(currentTs.strftime('%f'))[3:6])
  #take away the last three microseconds
  currentTs = currentTs - timedelta(microseconds=currentTsMicroseconds)
  
  #get the date as an int format
  createdDate = int(currentTs.strftime('%Y%m%d'))
  #get the hour
  createdHour = currentTs.hour
  createdTimestamp = currentTs
  lastUpdatedTimestamp = currentTs
  #use the same time for the date also
  date=currentTs
  
 #parameter for LOG_ERROR
  errorMessage = ''
  adfPipelineName = ''
  clusterId = ''
  notebookName = ''
  batchId = -1
  batchTaskId = -1
  
  #parameter for log_task_end
  batchTaskSourceRows = ''
  batchTaskRowsLoaded = ''
  batchTaskRejectRows = '' 
  batchTaskResultLocation = ''
  batchTaskResult = '' 
  
except Exception as e:
  errorMessage = 'Exception occurred while variable declaration' + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try: 
  
  #get batchId from widgets
  dbutils.widgets.text('batchId','')
  batchId = dbutils.widgets.get('batchId')
  
  #get batchTaskId from widgets
  dbutils.widgets.text('batchTaskId','')
  batchTaskId = dbutils.widgets.get('batchTaskId')
  
  #get sourceId from widgets
  dbutils.widgets.text('sourceId','')
  sourceId = dbutils.widgets.get('sourceId')
  
  #get adfPipelineName from widgets
  dbutils.widgets.text('adfPipelineName','')
  adfPipelineName = dbutils.widgets.get('adfPipelineName')
  
  #get notebookName from widgets
  dbutils.widgets.text('notebookName','')
  notebookName = dbutils.widgets.get('notebookName')
  
  #get clusterId from widgets
  dbutils.widgets.text('clusterId','')
  clusterId = dbutils.widgets.get('clusterId')
  
  #call the GET_LOGGING_PATH function to create a log file path as a string and store it in a variable 
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')

except Exception as e:
  errorMessage = 'Exception occurred while getting parameters and initialising error log location ' + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn = dbutils.secrets.get(scope = 'data-scope-01', key = 'sql-dbrks-connection-01')
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = 'Exception occurred while connecting to database: ' + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Get raw object information
# get the object level information using the getRawEventhubObjectLocationsfunction 
try :
  rawLocation = getHistoricRawEventhubObjectLocations(conn
                                                      ,cursor
                                                      ,batchId
                                                      ,batchTaskId
                                                      ,adfPipelineName
                                                      ,clusterId
                                                      ,notebookName
                                                      ,errorLogFileLocation)

except Exception as e:
  errorMessage = 'Exception occurred while executing the function getHistoricRawEventhubObjectLocations: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Transform metadata into spark dataframe
# transfer the rawLocation to spark dataframe
try :
  rawLocationDetails = convertPandasToSparkDfLoadNaGen2Sqdb(rawLocation,
                                                            cursor, 
                                                            batchTaskId,
                                                            adfPipelineName,
                                                            clusterId,
                                                            notebookName,
                                                            errorLogFileLocation)
except Exception as e:
  errorMessage = 'Exception occurred while Transforming metadata into spark dataframe:' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Update latest date time entry into tbl_object_dates_availability table
# Makes the latest date time entry into the tbl_object_dates_availability table for all the raw zone objects 
try :
  rawObjectHistoricDateTime(rawLocationDetails,
                          cursor,
                          batchTaskId,
                          adfPipelineName,
                          clusterId,
                          notebookName,
                          errorLogFileLocation)
except Exception as e:
  errorMessage = 'Exception occurred while calling function rawObjectLatestDateTime : ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Complete task and close the Database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,
                            adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False