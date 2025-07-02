# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_log_task_start</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test stored procedure usp_log_task_start</td></tr>
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
# MAGIC     <td>NotebookName</td>
# MAGIC     <td>@NotebookName to retrieve notebookname </td>
# MAGIC     <td>@NotebookName = 'testnotebook'</td>
# MAGIC   </tr>
# MAGIC     <tr>
# MAGIC     <td>ClusterID</td>
# MAGIC     <td>@ClusterID to retrieve cluster details </td>
# MAGIC     <td>@ClusterID = '12345'</td>
# MAGIC   </tr>
# MAGIC    <tr>
# MAGIC     <td>sourceID</td>
# MAGIC     <td>@sourceID to retrieve source details </td>
# MAGIC     <td>@sourceID = '1'</td>
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
# MAGIC     <td>CFramework</td>
# MAGIC     <td>Dev rework changes for camel case function names, date variables and removal of error line
# MAGIC         <br>pdated in title from updation to update and added errorMessage in import modules cell</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>  </td>
# MAGIC     <td>  </td>
# MAGIC     <td>  </td>
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
  errorMessage = "Exception occured while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try :
  #variable for CURRENT_TIME
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

  #parameter for LOG_ERROR
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
  testObjectName = 'usp_log_task_start'  
  requiredInputParameter = 'batchTaskId'
  testCaseScenario = ''
  executionOutputStatus = ''
  sampleOutputLocation = 'NA'
  #path for datasets used in stored procedure 
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = 'dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
except Exception as e:
  errorMessage = "Exception occured while variable declaration " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try:
  #get batchTaskId from  widgets   
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")

  #get batchId from  widgets
  dbutils.widgets.text("batchId","")
  batchId = dbutils.widgets.get("batchId")
 
  #get sourceId from  widgets   
  dbutils.widgets.text("sourceId","")
  sourceId  = dbutils.widgets.get("sourceId")

  #get adfPipelineName from  widgets
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName  = dbutils.widgets.get("adfPipelineName")
  
  #get notebookName from  widgets   
  dbutils.widgets.text("notebookName","")
  notebookName  = dbutils.widgets.get("notebookName")

  #get clusterId from  widgets
  dbutils.widgets.text("clusterId","")
  clusterId  = dbutils.widgets.get("clusterId")
  
  #call the getLoggingPath function to create a log file path as a string and store in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occurred while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish DB Connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage="Exception occurred while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Testing scenario prep1 script exeuction
try:
  prep1 = open("{}LogTaskStartPrep1.sql".format(testInputPath), "r").read()
  prep1 = prep1.replace('<batch_id>', str(batchId))

  #execute the prep1 script and get the first row
  prep1Resutls = pd.read_sql_query(prep1,conn).iloc[0]

  #get the first value of the row and cast as int
  newBatchTaskID = int(prep1Resutls[0])
  logTaskProgress(cursor,batchTaskId,'Execution of prep1 script to generate a new batch task id for testing. Batch task id generated is {}'.format(newBatchTaskID))
except Exception as e:
  errorMessage = "Exception occurred while 'Executing of prep1 script to generate a new batch task id for testing': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Execute log task start stored procedure
#execute stored procedure to make insert new entry in tbl_batch_task_progress and update status running in batch_task_table
try:
  #executing stored procedure using our new batch task id
  cursor.execute("exec audit.usp_log_task_start ?",newBatchTaskID)
  logTaskProgress(cursor,batchTaskId,'Successfully executed the stored procedure usp_log_task_start')
  while cursor.nextset():
      x = 1
except Exception as e:
  errorMessage = "Exception occurred while executing stored procedure usp_log_task_start: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 1-Check for the update of batch_task_status = 'running'
#stored procedure should have updated the batch_task_status as 'running' in batch_task_table,  checking if count of record with status = running if 1 is returned then confirms the testCaseStatus as success
try:
  #Declare variables
  testCaseScenario = 'update check on batch task status as running'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  #reading stored procedure result as pandas dataframe
  spOutput = pd.read_sql_query("select * from audit.tbl_batch_task where batch_task_id = {} and batch_task_status = 'running'".format(newBatchTaskID),conn)
  #read count of updated record in tbl_batch_task if count is 1 then testCaseStatus is success
  if spOutput.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario1 test case for stored procedure usp_log_task_start')
except Exception as e:
  errorMessage = "Exception occurred while executing scenario1 test case for stored procedure usp_log_task_start: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario 2-Check for the update of batch_task_status != 'running'
#to check whether stored procedure has correctly updated the batch_task_status as running, if not updated check the row count 
try:
  #Declare variables
  testCaseScenario = 'Negative test case for update statement'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  #reading stored procedure result as pandas dataframe
  spOut = pd.read_sql_query("select * from audit.tbl_batch_task where batch_task_id = {} and batch_task_status != 'running'".format(newBatchTaskID),conn)
  #read count of non updated record in tbl_batch_task if count is 0 then testCaseStatus is success
  if spOut.shape[0] == 0:
    executionOutputStatus='As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario2 test case for stored procedure usp_log_task_start')
except Exception as e:
  errorMessage = "Exception occurred while executing scenario2 test case for stored procedure usp_log_task_start: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 3-Check for insert row in tbl_batch_task_progress
#stored procedure should have inserted new record in tbl_batch_task_progress table checking if count of row is 1 then confirms the testCaseStatus as success
try:
  #Declare variables
  testCaseScenario = 'insert new row check in tbl_batch_task_progress'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  #reading stored procedure result as pandas dataframe
  spOut=pd.read_sql_query("select * from audit.tbl_batch_task_progress where batch_task_id = {} and batch_task_progress_message = 'running' ".format(newBatchTaskID),conn)
  #read count of inserted record in tbl_batch_task_progress if count is 1 then testCaseStatus is success
  if spOut.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario3 test case for stored procedure usp_log_task_start')
except Exception as e:
  errorMessage = "Exception occurred while executing scenario3 test case for stored procedure usp_log_task_start: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup
try:
  cleanup = open("{}LogTaskStartCleanup.sql".format(testInputPath), "r").read()
  cleanup = cleanup.replace('<batch_task_id>', str(newBatchTaskID))

  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1

  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for usp_log_task_start')
except Exception as e:
  errorMessage = "Exception occurred while 'Executing of cleanup script for usp_log_task_start': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False    

# COMMAND ----------

# DBTITLE 1,close database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                          batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                          batchTaskResultLocation,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False