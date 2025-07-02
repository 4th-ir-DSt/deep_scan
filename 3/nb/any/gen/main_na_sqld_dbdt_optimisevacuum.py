# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>main_na_sqld_dbdt_optimisevacuum</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>optimizing and vacuuming the delta table</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2020/04/29</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC 
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Parameter Name</th>
# MAGIC     <th>Parameter Description</th>
# MAGIC     <th>Example Parameter</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchId</td>
# MAGIC     <td>@batchId to retrieve batch details</td>
# MAGIC     <td>@batchId = 7</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchTaskId</td>
# MAGIC     <td>@batchTaskId to retrieve batchTask details</td>
# MAGIC     <td>@batchTaskId = 10</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>adfPipeLineName</td>
# MAGIC     <td>@adfPipeLineName to retrieve DataFactory pipelinename details</td>
# MAGIC     <td>@adfPipeLineName = 'testpipepline'</td>
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
# MAGIC     <td>Framework</td>
# MAGIC     <td> Added udf created code on helper function to get maxDate and maxHour
# MAGIC          <br> Modified the notebook to add data retention functions 
# MAGIC          <br> Added check for retention to perform or not 
# MAGIC     </td>
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

# DBTITLE 1,Run log function notebook
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run standard function notebook
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run Miscellaneous Functions notebook
# MAGIC %run ../../util/spe/misc_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import os
  import glob
  from pathlib import Path
  import pandas as pd
  import pyodbc
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
  from datetime import datetime,timedelta
  from pyspark.sql.functions import lit,col,explode,concat_ws,struct,collect_list,concat,substring_index,length,locate,max,udf
  from functools import reduce
  from itertools import chain  
except Exception as e:
  errorMessage = "Exception occurred while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise variables
try :
  #variable for current timestamp
  currentTs = datetime.now()
  #get the last 3 positions of the microseconds as we need to remove them
  currentTsMicroseconds = int(str(currentTs.strftime('%f'))[3:6])
  #take away the last three microseconds
  currentTs = currentTs - timedelta(microseconds = currentTsMicroseconds)
  #get the date as an int format
  createdDate = int(currentTs.strftime('%Y%m%d'))
  #get the hour
  createdHour = currentTs.hour
  createdTimestamp = currentTs
  lastUpdatedTimestamp = currentTs
  #use the same time for the date also
  date = currentTs
  
  #PARAMETER FOR logError
  errorMessage = ''
  adfPipelineName = ''
  clusterId = ''
  notebookName = ''
  batchId = -1
  batchTaskId = -1

  #parameter for log_task_end
  batchTaskStatus = ''
  batchTaskSourceRows = 0
  batchTaskRowsLoaded = 0
  batchTaskRejectRows = 0
  batchTaskResult = ''
  batchTaskResultLocation = ''
  batchTaskProgressMessage = '' 
  
except Exception as e:
  errorMessage = "Exception occurred while variable initialisation :" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables and initialise error log location
try:
  #GET batchTaskId FROM WIDGETS   
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")

  #GET batchId FROM WIDGETS
  dbutils.widgets.text("batchId","")
  batchId = dbutils.widgets.get("batchId")
  
  #GET sourceId  FROM WIDGETS   
  dbutils.widgets.text("sourceId","")
  sourceId  = dbutils.widgets.get("sourceId")

  #GET adfPipelineName  FROM WIDGETS
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName  = dbutils.widgets.get("adfPipelineName")
  
  #GET notebookName  FROM WIDGETS   
  dbutils.widgets.text("notebookName","")
  notebookName  = dbutils.widgets.get("notebookName")

  #GET clusterId  FROM WIDGETS
  dbutils.widgets.text("clusterId","")
  clusterId  = dbutils.widgets.get("clusterId")
  
  #assigned the source and batch to other variables that are referenced from the metadata
  SourceID = sourceId
  CreatedBatchID = batchId
  LastUpdatedBatchID = batchId
  
  #call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')  
except Exception as e:
  errorMessage = "Exception occurred while getting parameters and initiliasing error log location: " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn=dbutils.secrets.get(scope="lza-da-kv-001-d", key="lza-dp-sqlacct-001-databricks-sql-connection")

#call function sqlDbConn to establish Database connection with given scope and key values
try:
  conn,cursor = sqlDbConn(dbconn,
                          batchTaskId,                          
                          adfPipelineName,
                          clusterId,
                          notebookName,
                          errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Successfully Established SQL Connection")
except Exception as e:
  errorMessage = "unable to establish DB connection: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Get delete statement by executing usp_get_object_retention store procedure 
# getObjectRetention executes usp_get_object_retention store procedure and returns the delete statement for the object related to the current batch 
try:
    objectRetention = getObjectRetention(conn
                                         ,cursor
                                         ,batchTaskId
                                         ,adfPipelineName
                                         ,clusterId
                                         ,notebookName
                                         ,errorLogFileLocation)
    applyRetention=objectRetention.shape[0]
    logTaskProgress(cursor,batchTaskId,"Successfully executed getObjectRetention function")
except Exception as e:
  errorMessage = "Exception occurred while executing getObjectRetention function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

objectRetention

# COMMAND ----------

# DBTITLE 1,Convert pandas to spark dataframe
#Call convertSinglePandasToSparkDf function to convert the usp_get_object_retention store procedure result into spark dataframe 
try:
  if applyRetention>0:
    retentionDeleteDetails = convertSinglePandasToSparkDf(objectRetention
                                                          ,cursor
                                                          ,batchTaskId
                                                          ,adfPipelineName
                                                          ,clusterId
                                                          ,notebookName
                                                          ,errorLogFileLocation)
    logTaskProgress(cursor,batchTaskId,"Successfully executed convertSinglePandasToSparkDf function")
except Exception as e:
  errorMessage = "Exception occurred while executing convertSinglePandasToSparkDf function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

# COMMAND ----------

# DBTITLE 1,Delete the retention data 
# deleteRetentionData function will delete the retention data for the object related to current batch
try:
  if applyRetention>0:
    deleteRetentionData(retentionDeleteDetails
                        ,cursor
                        ,batchTaskId
                        ,errorMessage
                        ,adfPipelineName
                        ,clusterId,notebookName
                        ,errorLogFileLocation)
    logTaskProgress(cursor,batchTaskId,"Successfully completed data retention")
except Exception as e:
  errorMessage = "Exception occurred running deleteRetentionData function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Create Udf on helper function to get max date and hour details function 
try:
  getEndingUdf = udf(getEnding, StringType())
  logTaskProgress(cursor,batchTaskId,"successfully created udf on helper function getEnding")
except Exception as e:
  errorMessage = "Exception occurred while creating udf on helper function getEnding: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Optimise and Vacuum the delta tables
try:
  #Call deltaTableOptimiseVacuum function to optimise and vacuum the delta tables for given batch_task_id
  deltaTableOptimiseVacuum(cursor
                           ,batchTaskId
                           ,errorMessage
                           ,adfPipelineName
                           ,clusterId
                           ,notebookName
                           ,errorLogFileLocation)

  logTaskProgress(cursor,batchTaskId,"successfully optimized and vacuumed the delta table")
except Exception as e:
  errorMessage = "Exception occurred optimizing and vacuuming the delta table : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Complete task and close the Database connection
#call taskEndAndCloseConn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,
                      conn,
                      batchTaskId,
                      batchTaskSourceRows,
                      batchTaskRowsLoaded,
                      batchTaskRejectRows,
                      batchTaskResult,
                      batchTaskResultLocation,
                      adfPipelineName,
                      clusterId,
                      notebookName,
                      errorLogFileLocation)
except:
  assert False