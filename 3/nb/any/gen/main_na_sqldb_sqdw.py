# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>main_na_sqldb_sqdw</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Genrate sql scripts to create synapse objects </td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Karthik</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/01/12</td></tr>
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
# MAGIC     <td>@batchId = N/A</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchTaskId</td>
# MAGIC     <td>@batchTaskId to retrieve batchTask details</td>
# MAGIC     <td>@batchTaskId = N/A</td>
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
# MAGIC     <td>@sourceId = N/A</td>
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
# MAGIC     <td>2022-01-19</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>Moved generate scripts code to misc_functions notebook</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022-08-31</td>
# MAGIC     <td>Manmohan</td>
# MAGIC     <td>Added filter option to generate scripts specific to PhaseA Staging or Others </td>
# MAGIC   </tr> 
# MAGIC  
# MAGIC </table>  
# MAGIC   
# MAGIC ## General Standards applying to each notebook
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

# DBTITLE 1,Run Miscellaneous Functions notebook
# MAGIC %run ../../util/spe/misc_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  from datetime import datetime, timedelta
  from pyspark.sql.functions import concat_ws,col,lit, split, concat_ws,collect_list, first 
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
except Exception as e:
  errorMessage = "Exception occurred while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise variables
try: 
  #varible for current_time
  currentTs=datetime.now()
  #get the last 3 positions of the microseconds as we need to remove them
  currentTsMicroseconds = int(str(currentTs.strftime('%f'))[3:6])
  #take away the last three microseconds
  currentTs = currentTs - timedelta(microseconds=currentTsMicroseconds)
  
  #get the date as an int format
  createdDate = int(currentTs.strftime('%Y%m%d'))
  lastUpdateDate = createdDate
  #get the hour
  createdHour = currentTs.hour
  createdTimestamp = currentTs
  lastUpdateTimestamp = currentTs
  #use the same time for the date also
  date=currentTs
  
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
  
  #variable to retrieve the batch rows loaded or not
  retrieveBatchRowsLoaded = False

  scriptsBasePath = '/mnt/synapsescripts'
  dataSource = 'AzureDataLakeExtTables'
  fileFormat = 'IncrementalParquet'
    
    
except Exception as e:
  errorMessage = "Exception occured while variable initialisation :" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables and initialise error log location
try:
  #GET batchTaskId FROM WIDGETS   
  dbutils.widgets.text("batchTaskId","1")
  batchTaskId = dbutils.widgets.get("batchTaskId")

  #GET batchId FROM WIDGETS
  dbutils.widgets.text("batchId","1")
  batchId = dbutils.widgets.get("batchId")
  
  #GET sourceId  FROM WIDGETS   
  dbutils.widgets.text("sourceId","1")
  sourceId  = dbutils.widgets.get("sourceId")

  #GET adfPipelineName  FROM WIDGETS
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName  = dbutils.widgets.get("adfPipelineName")
  
  #GET notebookName  FROM WIDGETS   
  dbutils.widgets.text("notebookName","")
  notebookName  = dbutils.widgets.get("notebookName")

  #GET clusterId  FROM WIDGETS
  clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
  dbutils.widgets.text("clusterId",clusterId)
#   clusterId  =  dbutils.widgets.get("clusterId")
  
  #assigned the source and batch to other variables that are referenced from the metadata
  sourceId=sourceId
  createdBatchId=batchId
  lastUpdateBatchId=batchId
  
  #call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')  
except Exception as e:
  errorMessage = "Exception occured while getting parameters and initiliasing error log location: " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')

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
  errorMessage="unable to establish DB connection: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Get synapse table details
try:
  #  get active synapse table details in pandas dataframe
  activeSynapseTablesPandasDF = getActiveSynapseTablesSpExec(conn,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  #convert pandas data frame to spark dataframe along with the data converted to required format
  tableDetailsDf = getSynapseTableDetailsDf(activeSynapseTablesPandasDF,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

  logTaskProgress(cursor,batchTaskId,"table details obtained successfully")
except Exception as e:
  errorMessage="Exception occurred calling function getActiveSynapseTablesSpExec : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Filter for PhaseA or Bordereaux objects
# IMPORTANT : Use data_model_group='staging' to generate scripts for PhaseA objects, which use UPSERT statement generator for Staging Stored Procs. Any other object will continue to use previously defined MERGE statements.
tableDetailsDf = tableDetailsDf.where("lower(data_model_group)='staging'")
# display(tableDetailsDf)

# COMMAND ----------

# DBTITLE 1,Clear synapse scripts container 
try:    
    dbutils.fs.rm(scriptsBasePath, True)
    logTaskProgress(cursor,batchTaskId,"Successfully cleared synapse scripts container")
except Exception as e:
    errorMessage="Error while clearing synapse scripts container: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False    


# COMMAND ----------

# DBTITLE 1,Generate sql scripts
try:    
    generateSynapseScripts(tableDetailsDf,scriptsBasePath,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)        
    logTaskProgress(cursor,batchTaskId,"Successfully generated sql scripts")
except Exception as e:
  errorMessage="Exception occurred while generating sql scripts : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Complete task and close connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor
                      ,conn
                      ,batchTaskId
                      ,batchTaskSourceRows
                      ,batchTaskRowsLoaded
                      ,batchTaskRejectRows
                      ,batchTaskResult
                      ,batchTaskResultLocation
                      ,adfPipelineName
                      ,clusterId
                      ,notebookName
                      ,errorLogFileLocation)
except:
  assert False