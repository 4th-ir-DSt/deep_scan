# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>Main_na_sqld_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td> Creation of Databricks delta tables if tables where they not existing </td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2019/12/23</td></tr>
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
# MAGIC     <td>Try catch block changed with new function
# MAGIC          <br> Dev rework changes for camel case function names, date variables and removal of error line
# MAGIC          <br> Moved function definitions to misc_functions notebook
# MAGIC          <br> Modified call to getTableHighAndLowLevelDF function and also, necessary parameter changes to tableStructureCheck
# MAGIC          <br> import json
# MAGIC          <br> import regexp_replace
# MAGIC          <br> import numpy
# MAGIC          <br> created create tables only mode for non etl workspaces</td>
# MAGIC   </tr>   
# MAGIC     <tr>
# MAGIC     <td>2022/01/14</td>
# MAGIC     <td>Framework</td>
# MAGIC     <td>Try catch block changed with new function
# MAGIC          <br>Updates required to handle object with database names
# MAGIC          </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/11/23</td>
# MAGIC     <td>Manmohan</td>
# MAGIC     <td>Added Dataframe filter to exclude objects with complext column datatypes<br>
# MAGIC         Filter added for raw_fx_rate.exchange_rate_daily
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

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run Miscellaneous Function notebook
# MAGIC %run  ../../util/spe/misc_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import numpy as np
  import pyodbc
  import json
  from datetime import datetime, timedelta
  from pyspark.sql.functions import asc, col, concat, concat_ws, when, upper,split,lower, lit, col, collect_list, max, regexp_replace,translate
  from pyspark.sql import Window
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
except Exception as e:
  errorMessage = "Exception occurred while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialize variables
try:
  #varible for current_time
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
  errorMessage="Exception occurred while variable declaration " + str(e)
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
  dbconn = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = 'Exception occurred while connecting to database: ' + str(e)
  print(errorMessage)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Get active databricks table
try:
  #get_active_databricks_tables_sp_execution function execute usp_get_active_databricks_tables and get the active table details  
  activeDatabricksTablesPandasDF = getActiveDatabricksTablesSpExec(conn,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except Exception as e:
  errorMessage="Exception occurred calling function get_active_databricks_tables_sp_exec : " + str(e)
  # print(errorMessage)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  


# COMMAND ----------

# MAGIC %md
# MAGIC ## DO NOT REMOVE BELOW FILTER
# MAGIC <span style="color:red">Below filtering is performed to avoid creation/updation of Delta tables with Complex/structured column datatypes.</span></br>
# MAGIC Their creation and updation to be performed manually during releases.

# COMMAND ----------

# DBTITLE 1,Exclude DeltaTables with Complex datatypes from being created with Metadata datatypes
#DO NOT REMOVE THESE FILTERS, RUNNING THIS NOTEBOOK WITHOUT THIS FILTER WILL BREAK FEW TABLES

#Remove raw_fx_rate.exchange_rate_daily from Pandas Dataframe
activeDatabricksTablesPandasDF = activeDatabricksTablesPandasDF[(activeDatabricksTablesPandasDF['object_name'].str.lower()) != "raw_fx_rate.exchange_rate_daily"]


# COMMAND ----------

# DBTITLE 1,Get High and low level table pyspark dataframes
try:
  #getTableHighAndLowLevelDF function returns the table high level dataframe and the low level (columns) dataframe
  tableHighDetailsDf,tableLowDetailsDf = getTableHighAndLowLevelDF(activeDatabricksTablesPandasDF,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except Exception as e:
  errorMessage="Exception occurred calling function getUniqueTableName : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

#this should be False for the ETL workspace
createTableOnly = False

#set the where clause if you require filtering
where_clause = ""

# If you are creating the tables for a passthrough workspace (i.e. not the ETL cluster)
# Then only create for sensitive, refined and person tables only)
# Also create the tables with createTableOnly = True

# sensitiveRefinedPersonTablesOnly = "lower(object_name) like 'refined%' or lower(object_name) like 'sensitive%' or lower(object_name) like 'person%'"
# where_clause = sensitiveRefinedPersonTablesOnly
# createTableOnly = True

#assign 1=1 to the where clause if nothing has been provided
where_clause = "1=1" if len(where_clause) == 0 else where_clause
#print(where_clause)
tableHighDetailsDf = tableHighDetailsDf.where(where_clause)
# tableHighDetailsDf.display()
# tableHighDetailsDf.withColumn('location',translate('location', '\'', "/")).display()



# COMMAND ----------

# display(tableHighDetailsDf)

# COMMAND ----------

# DBTITLE 1,Create  Database If not Exists
try:
  databaseCheck(tableHighDetailsDf,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except Exception as e:
  errorMessage="Exception occurred calling function databaseCheck : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Check the table structure matches the metadata structure 
try:
  #Call tableStructureCheck function to check the table is a proper format else alter it 
  tableStructureCheck( tableHighDetailsDf
                      ,tableLowDetailsDf
                      ,createTableOnly
                      ,cursor
                      ,batchTaskId
                      ,errorMessage
                      ,adfPipelineName
                      ,clusterId
                      ,notebookName
                      ,errorLogFileLocation)
  
except Exception as e:
  errorMessage="Exception occurred calling function tableStructureCheck : " + str(e)
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