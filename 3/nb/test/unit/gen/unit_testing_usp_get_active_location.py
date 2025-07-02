# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_get_active_locations</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test stored procedure usp_get_active_locations</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2019/01/01</td></tr>
# MAGIC </table>
# MAGIC 
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
# MAGIC     <td>Dev rework, function names, sql db connection, dates</td>
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
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Import Module
try:
  import pandas as pd
  import pyodbc
  from datetime import datetime, timedelta
except Exception as e:
  assert False 

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try :
  #varible for current_time
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

  #PARAMETER FOR LOG_ERROR
  errorLine = ''
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
 
  #Parameter for logging into tbl_unit_test_result  
  testObject = 'stored procedure'
  testObjectName = 'usp_get_active_location'  
  requiredInputParameter = 'NA'
  sampleOutputLocation = 'NA'
  
  #path for datasets used in stored procedure 
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  
except Exception as e:
  errorMessage = "Exception occured while variable declaration " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
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
  clusterId = dbutils.widgets.get("clusterId")
 
  #call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occured while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish Database Connection
try:
  dbconn=dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = "Exception occured while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup - previous run
#script for  metadata cleanup
try: 
  metadatacleanup = open("{}GetActiveLocationObjectsCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed cleanup scripts successfully')     
except Exception as e:
  errorMessage = "Exception occured while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 1 - Data preparation
# scripts to insert an object and location into the tbl_object where is_active as 1
try:  
  dataPreparation = open("{}GetActiveLocationObjectsPrep1.sql".format(testInputPath), "r").read()
  dataPreparationResults = pd.read_sql_query(dataPreparation,conn).astype(str)
  
  #get the object name
  testObject = dataPreparationResults.at[0,'object_name']
  
  logTaskProgress(cursor,batchTaskId,' Executed initial scripts successfully')     
except Exception as e:
  errorMessage = "Exception occured while executing initial scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 1 - Check only active objects are returned
#to check whether stored procedure has returned the active data based on is_active is 1
try:
  #Declare variables
  testCaseScenario = 'active location availability check based on is_active = 1'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  
  #execute stored procedure to get active locations
  spOutput = pd.read_sql_query("exec config.usp_get_active_location",conn)
  
  #Read object name from the stored procedure output as pandas dataframe
  spExpectedOutput= spOutput[spOutput['object_name'] == testObject]
  
  #check if inserted active object count is 1.
  if spExpectedOutput.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario1 test case for stored procedure usp_get_active_location')  
except Exception as e:
  errorMessage = "Exception occured while executing scenario1 test case for stored procedure usp_get_active_location: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 2 - Data preparation
# scripts to update an object and location in the tbl_object where is_active as 0
try: 
  dataPreparation = open("{}GetActiveLocationObjectsPrep2.sql".format(testInputPath), "r").read()
  cursor.execute(dataPreparation)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed data preparation scripts successfully')     
except Exception as e:
  errorMessage = "Exception occured while executing data preparation scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 2 -Procedure should not return the object if is_active status is 0
#stored procedure should not return the data if is_active = 0
try:
  #Declare variables
  testCaseScenario = 'negative test case for data check'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
 
  #executing stored procedure to get active locations
  spOutput = pd.read_sql_query("exec config.usp_get_active_location",conn)
  
  #Read expected output from sql statement as pandas dataframe
  spExpectedOutput= spOutput[spOutput['object_name'] == testObject]
  if spExpectedOutput.shape[0] == 0:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
 
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario2 test case for stored procedure usp_get_active_location')  
except Exception as e:
  errorMessage = "Exception occured while executing scenario2 test case for stored procedure usp_get_active_location: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#script for  metadata cleanup
try: 
  metadatacleanup = open("{}GetActiveLocationObjectsCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed cleanup scripts successfully')     
except Exception as e:
  errorMessage = "Exception occured while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Close database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor, conn,batchTaskId , batchTaskSourceRows,
                      batchTaskRowsLoaded, batchTaskRejectRows, batchTaskResult,
                      batchTaskResultLocation, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
except:
  assert False