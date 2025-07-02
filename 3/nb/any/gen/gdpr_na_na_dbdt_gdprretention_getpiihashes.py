# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>gdpr_na_na_dbdt_gdprretention_getpiihashes</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td> this notebook will be responsible for creation of superset pii hash and active pii hash table dataframes </td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2020/06/10</td></tr>
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
# MAGIC 
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
# MAGIC     <td>Changes to the table temp_otherPiiHA
# MAGIC          <br>Changes to the temp deltables for gdpr retention
# MAGIC          <br>Removed errorMessage from the parameters of getGDPRdataRetentionSpExec
# MAGIC          <br>Correct obejct name of otherPiiObjectName from temp_otherPIIH to temp_otherPIIHA
# MAGIC          <br>Create and write supersetPii dataframe - removed SourceID and StagingTableName columns from supersetPiiDF. Created specific path with sourceid and stagingtablename in the path and parquet format variable. Passed corrected variable parameters into writeToTarget
# MAGIC          <br>create and write ActivePii dataframe - created otherPiiObjectLocationSpecific variable to hold the specific location with partition on StagingTableName.  Additional passed in variables supersetPiiLocationSpecific and otherPiiObjectLocationSpecific into gdprFunctions.createActivePiiDf. Passed corrected variable parameters into writeToTarget</td>
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

# DBTITLE 1,Run GDPR Functions notebook
# MAGIC %run ../../util/spe/gdpr_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  from datetime import datetime, timedelta
  from pyspark.sql.functions import lit,col,broadcast,lower
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType

except Exception as e:
  errorMessage="Exception occured while import modules " + str(e)
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
  SourceID=int(sourceId)
  CreatedBatchID=batchId
  LastUpdatedBatchID=batchId
  
  #call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')  
except Exception as e:
  errorMessage = "Exception occurred while getting parameters and initiliasing error log location: " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn=dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")

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

# DBTITLE 1,Get staging object details
try:
  stagingObjectDf=getMaintenanceObjectsSpExec(conn,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  stagingObjectName=stagingObjectDf['object_name'][0]

except Exception as e:
  errorMessage = 'Exception occurred while executing the getManitenanceObjectsSp: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Get reference object details
try:
  #run the getGDPRdataRetentionSpExec to get reference object details
  retentionObjectDf=gdprFunctions.getGDPRdataRetentionSpExec(conn,cursor,sourceId,stagingObjectName,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  referenceObjectName=retentionObjectDf['reference_table_name'][0]
  stagingObjectAttributeName=retentionObjectDf['object_attribute_name'][0]
  referenceAttributeName=retentionObjectDf['reference_attribute_name'][0]
except Exception as e:
  errorMessage = 'Exception occurred while executing the getGDPRdataRetentionSp: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Assign static variables for pii columns and where condition
#Assign the static variables and where clause
piiColumnsList=['PiiTraceabilityHash','PiiHash','PiiHashVersion']
whereCondition='Processed=false'
supersetPiiObjectName='temp_supersetTempKeysPIIHA'
supersetPiiLocation="/mnt/temp/gdprRetention/tables/temp_supersetTempKeysPIIHA"
format='Delta'
activePiiObjectLocation="/mnt/temp/gdprRetention/tables/temp_inUsePIIHA"
activePiiObjectName='temp_inUsePIIHA' 
otherPiiObjectName='temp_otherPIIHA'
otherPiiObjectLocation='/mnt/temp/gdprRetention/tables/temp_otherPIIHA'

# COMMAND ----------

# DBTITLE 1,Create reference and staging dataframes
try:
  #Create staging and reference dataframes
  refDf,stagingDf=gdprFunctions.getReferenceAndStagingDataFrames(conn 
                                               ,cursor
                                               ,referenceObjectName
                                               ,whereCondition
                                               ,referenceAttributeName
                                               ,stagingObjectName
                                               ,stagingObjectAttributeName
                                               ,piiColumnsList
                                               ,errorMessage
                                               ,adfPipelineName
                                               ,clusterId
                                               ,notebookName
                                               ,errorLogFileLocation)  
except Exception as e:
  errorMessage = 'Exception occurred while executing the getReferenceAndStagingDataFrames function: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Create and write supersetPii dataframe
try:
  #Create superset pii 
  supersetPiiDf=gdprFunctions.createSupersetPiiDf(conn
                       ,cursor
                       ,refDf   
                       ,stagingDf
                       ,piiColumnsList        
                       ,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

  #Add audit columns
  supersetPiiDf=(supersetPiiDf
                 .withColumn('ReferenceTableName',lit(referenceObjectName))
                 .withColumn('ReferenceTableAttributeName',lit(referenceAttributeName))
                )

  formatParquet = 'parquet'
  #create specific path including the source id and staging table name to create the partitions
  supersetPiiLocationSpecific = supersetPiiLocation + '/SourceID={}'.format(SourceID) + '/StagingTableName={}'.format(stagingObjectName)
  
  #Write to the target
  writeToTarget(supersetPiiDf
                ,supersetPiiLocationSpecific
                ,supersetPiiObjectName
                ,formatParquet
                ,cursor
                ,batchTaskId
                ,adfPipelineName
                ,clusterId
                ,notebookName
                ,errorLogFileLocation) 
except Exception as e:
  errorMessage = 'Exception occurred while while create and writing  the supersetPii: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,create and write ActivePii dataframe
try:
  otherPiiObjectLocationSpecific = otherPiiObjectLocation + '/StagingTableName={}'.format(stagingObjectName)
  
  #Create active piihashes thesere are the traceability hasesh from superset and present in different join key eg:policynumber
  activePiiDf=gdprFunctions.createActivePiiDf(conn
                       ,cursor
                       ,refDf  
                       ,stagingDf
                       ,piiColumnsList 
                       ,supersetPiiObjectName
                       ,supersetPiiLocationSpecific
                       ,stagingObjectName
                       ,otherPiiObjectName
                       ,otherPiiObjectLocationSpecific
                       ,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)


  #Add audit columns
  activePiiDf=(activePiiDf
               .withColumn('ReferenceTableName',lit(referenceObjectName))
               .withColumn('ReferenceTableAttributeName',lit(referenceAttributeName))
              )

  formatParquet = 'parquet'
  #create specific path including the source id and staging table name to create the partitions
  activePiiObjectLocationSpecific = activePiiObjectLocation + '/SourceID={}'.format(SourceID) + '/StagingTableName={}'.format(stagingObjectName)
  
 #Write to the target
  writeToTarget(activePiiDf
                ,activePiiObjectLocationSpecific
                ,activePiiObjectName
                ,formatParquet
                ,cursor
                ,batchTaskId
                ,adfPipelineName
                ,clusterId
                ,notebookName
                ,errorLogFileLocation)
  
except Exception as e:
  errorMessage = 'Exception occurred while create and writing the active pii df: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Close Database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False