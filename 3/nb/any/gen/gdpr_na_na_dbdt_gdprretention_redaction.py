# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>gdpr_na_na_dbdt_gdprretention_redaction</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>This notebook redacts the pii hashes for a given source based on a temporay table</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2020/06/08</td></tr>
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
# MAGIC     <td>Framework</td>
# MAGIC     <td>Changed the removePiiDf join to use the PiiTraceabilityHash instead
# MAGIC          <br>Changed the get the reference tables from the removePiiDf dataframe
# MAGIC          <br>Changes to the temp deltables for gdpr retention
# MAGIC          <br>Changes to use the JoinKey in the retention redaction
# MAGIC          <br>Changes on the join to calculate temp_ToRemove table. Changes in the update redaction_state to use the sourceId
# MAGIC          <br>Changes in the update redaction_state to use usp_gdpr_get_reference_tables
# MAGIC          <br>updated updateReferenceProcessed function call to include the batchID parameter
# MAGIC          <br>Incorporated function convertPandasToSparkDfWithSchema
# MAGIC          <br>Calculate the difference between the piis to redact and the piis in use - created variables supersetPiiLocation and activePiiObjectLocation to hold the parquet path of temp_supersetTempKeysPIIHA and temp_inUsePIIHA data respectively. Changed the supersetPiiDf and activePiiDf dataframes to read from parquet locations of new variables (menioned in previous sentence)
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

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  import sys
  import copy
  from datetime import datetime, timedelta
  from decimal import Decimal
  from pyspark.sql.functions import lit,col,broadcast,concat_ws,concat,when,lower
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType

except Exception as e:
  errorMessage = "Exception occured while importing modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run GDPR Function notebook
# MAGIC %run ../../util/spe/gdpr_functions

# COMMAND ----------

# DBTITLE 1,Initialise variables
try:
  
  #variable current_time
  currentTs = datetime.now()
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
  date = currentTs
  
 #parameter for logError
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
  
  #call the errorLogFileLocation function to create a log file path as a string and store it in a variable 
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

# DBTITLE 1,Calculate the difference between the piis to redact and the piis in use
#Calculate the piis to remove by making the difference between temp_supersetTempKeysPIIHA and temp_inUsePIIHA
try:
  supersetPiiObjectName='temp_supersetTempKeysPIIHA'
  #location of temp_supersetTempKeysPIIHA parquet files
  supersetPiiLocation="/mnt/temp/gdprRetention/tables/temp_supersetTempKeysPIIHA"
  
  activePiiObjectName='temp_inUsePIIHA' 
  #location of temp_inUsePIIHA parquet files
  activePiiObjectLocation="/mnt/temp/gdprRetention/tables/temp_inUsePIIHA"
  
  toRemovePiiObjectName='temp_toRemovePIIHA' 
  
  #table select with parquet locations  
  supersetPiiDf=spark.read.format('parquet').load(supersetPiiLocation).where("SourceID={}".format(sourceId))
  activePiiDf=spark.read.format('parquet').load(activePiiObjectLocation).where("sourceID={}".format(sourceId))
  
  #Getting the piis to remove
  removePiiDf=(supersetPiiDf.join(activePiiDf,supersetPiiDf['PiiTraceabilityHash'] ==activePiiDf['PiiTraceabilityHash'],how='left')
               .select(supersetPiiDf['*'],activePiiDf['PiiTraceabilityHash'] )
               .withColumn('InUse', when(activePiiDf['PiiTraceabilityHash'].isNull(), lit(False)).otherwise(lit(True)))
               .drop(activePiiDf['PiiTraceabilityHash']))

  removePiiDf.distinct().write.format("delta").mode("append").saveAsTable(toRemovePiiObjectName)
  removePiiDf=spark.read.table(toRemovePiiObjectName).where("SourceID={}".format(sourceId))
                    
  logTaskProgress(cursor,batchTaskId,'Successfully calculated the piis ro remove')
  
except Exception as e:
  errorMessage = 'Exception occurred calculating the piis to remove:' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Apply redaction
#apply rectification to the piiHashes in the table piiHashesTable for source sourceId
try:
  gdprFunctions.gdprRetentionRedactionHashes(removePiiDf,sourceId,conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Successfully executed gdprRetentionRedactionHashes function')
except Exception as e:
  errorMessage = 'Exception occurred while executing gdprRetentionRedactionHashes:' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Update the processed reference tables
#apply rectification to the piiHashes in the table piiHashesTable for source sourceId
try:
  referenceTablesPd = pd.read_sql_query("exec [config].[usp_gdpr_get_reference_tables] {}".format(sourceId), conn)
  schema = StructType([StructField("ReferenceTableName", StringType(), True)])
  
  referenceTablesDF = convertPandasToSparkDfWithSchema(referenceTablesPd,schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
  gdprFunctions.updateReferenceProcessed(referenceTablesDF,sourceId,conn,cursor,batchId,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  logTaskProgress(cursor,batchTaskId,'Successfully updated the processed reference tables')
except Exception as e:
  errorMessage = 'Exception occurred while updating the processed reference tables:' + str(e)
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