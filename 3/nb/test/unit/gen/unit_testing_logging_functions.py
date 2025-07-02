# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_logging function</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Test the logging functions</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2019/01/01</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC Notebook Parameters
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
# MAGIC   Change Log
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Date</th>
# MAGIC     <th>Changed By</th>
# MAGIC     <th>Change Description</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2019/01/01</td>
# MAGIC     <td>Framework</td>
# MAGIC     <td>Added source object id to markDatesLoaded
# MAGIC       <br>Added Test case for logTaskProgressWithRetry</td>
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

# DBTITLE 1,Run log function Notebook
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Import Module
try:
  import pandas as pd
  import pyodbc
  from datetime import datetime,timedelta
  from os import path
  import os
except Exception as e:
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try:
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

  #parameters for log_error
  errorLine = ''
  errorMessage = ''
  adfPipelineName = ''
  clusterId = ''
  notebookName = ''
  batchId = -1
  batchTaskId = -1
  
  #parameter for logTaskEnd function  
  batchTaskStatus = ''
  batchTaskSourceRows = 0
  batchTaskRowsLoaded = 0
  batchTaskRejectRows = 0
  batchTaskResult = ''
  batchTaskResultLocation = '' 
  
  #parameter for logging into tbl_unit_test_result  
  testObject = 'notebook'
  testObjectName = 'loggingFunction'  
  executionOutputStatus = 'As Expected'
  sampleOutputLocation = 'NA'
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)

except Exception as e:
  errorMessage = "Exception occurred while variable declaration " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try:
  dbutils.widgets.text("batchId","")
  batchId = dbutils.widgets.get("batchId")
  
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName = dbutils.widgets.get("adfPipelineName")
  
  dbutils.widgets.text("clusterId","")
  clusterId = dbutils.widgets.get("clusterId")
  
  dbutils.widgets.text("notebookName","")
  notebookName = dbutils.widgets.get("notebookName")
  
  dbutils.widgets.text("sourceId","")
  sourceId = dbutils.widgets.get("sourceId")
   
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")
 
  #Call the getLoggingPath function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')
  
except Exception as e:
  errorMessage = "Exception occurred while initialising error log location and fetching parameter" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully for logging functions')
except Exception as e:
  errorMessage = "Exception occurred while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup for previous run
#run cleanup script
try:
  cleanup = open("{}loggingFunctionsCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed cleanup scripts')   
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Generate input parameter to test logging functions
#script that populate data in object and batch_task table and generate new object_id and batch_task_id
try:
  inputParameter = open("{}loggingFunctionsInput.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>',str(batchId))
  inputParameterResults = pd.read_sql_query(inputParameter,conn)
  
  #fetch parameter values from scripts
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id']) 
  sourceObjectId = int(inputParameterResults.at[0,'src_object_id'])
  destinationObjectId = int(inputParameterResults.at[0,'dest_object_id'])
  newDatetimeTo = str(inputParameterResults.at[0,'datetime_to'])[0:23]
  markDatetimeFrom = str(inputParameterResults.at[0,'mark_datetime_from'])[0:23]
  markDatetimeTo = str(inputParameterResults.at[0,'mark_datetime_to'])[0:23]
  
  logTaskProgress(cursor,batchTaskId,'Execution of script to generate input parameter for logging functions')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter for logging function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test on Function logTaskStart
#the function is called in the start of the task,which updates batch_task_status as running in batch_task_table
#the test fetches the record in batch_task table with batch_task_status as running
try:
  #Declare variables
  testCaseScenario = 'Log task start-Check for updating the batch_task_status as running in batch_task table'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  requiredInputParameter = 'cursor,batchTaskId'
  
  #function call
  logTaskStart(cursor,newBatchTaskId)
  
  #read output as pandas dataframe
  fnUpdateOut = pd.read_sql_query("SELECT * FROM audit.tbl_batch_task WHERE batch_task_id = {} AND batch_task_status = 'running'".format(newBatchTaskId),conn)
  
  #check for the updated record count
  if fnUpdateOut.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert into tbl_unit_test on the status of test_case 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test on log_task_start function performed successfully')
except Exception as e:
  errorMessage = "Exception occurred while testing on log_task_start function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test on function getLoggingPath
#the getLoggingPath function constructs log file path as string based on the input parameter
#created path format looks similar to '/mnt/log/databricks/error/date=2020-01-20/batchId=1/batchTaskId=1/12:30:54/'
try:
  #declare variables
  testCaseScenario = 'getLoggingPath-Check if the log_file_path is created in proper format'
  testCaseStatus = 'failed'
  executionOutputStatus ='Mismatched'
  requiredInputParameter = 'batchId,date,log_type,batchTaskId'
  
  #function call
  actualPathCreated = getLoggingPath(batchId,newBatchTaskId,date,'error')
  
  #the path is created in this format
  expectedPathFormat = '/dbfs/mnt/log/databricks/error/date={}/batchId={}/batchTaskId={}/{}/'.format(date.strftime("%Y-%m-%d"),str(batchId),str(newBatchTaskId),date.strftime("%H:%M:%S"))
  
  #check created filePath matches the format 
  if actualPathCreated == expectedPathFormat:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert into tbl_unit_test on the status of test_case 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                        testCaseScenario,executionOutputStatus,sampleOutputLocation,
                        datetime.now(),testCaseStatus)                                                                                                           
  logTaskProgress(cursor,batchTaskId,'unit test on getLoggingPath function performed successfully')
except Exception as e:
  errorMessage = "Exception occurred while testing on getLoggingPath function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test on function logTaskProgress
#function that inserts new row in logTaskProgress table which is called in try block
#test that takes count of new row that has been inserted in logTaskProgress table
try:
  #declare variables
  testCaseScenario = 'Log task progress-Check for row insertion of in logTaskProgress table'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  requiredInputParameter = 'cursor,batchTaskProgressMessage,batchTaskId'
  
  #function call
  batchTaskProgressMessage = 'Test progress message in logging functions{}'.format(date)
  logTaskProgress(cursor,newBatchTaskId,batchTaskProgressMessage)
  
  #reads result as pandas dataframe
  fnInsertOutput = pd.read_sql_query("SELECT * FROM audit.tbl_batch_task_progress WHERE batch_task_id = {} AND batch_task_progress_message = '{}'".format(newBatchTaskId,batchTaskProgressMessage),conn)
  #compares the count of inserted record in tbl_batch_task_progress
  if fnInsertOutput.shape[0] >= 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert into tbl_unit_test on the status of test_case  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)  
  logTaskProgress(cursor,batchTaskId,'Unit test on logTaskProgress function performed successfully')
except Exception as e:
  errorMessage = "Exception occurred while testing on logTaskProgress function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test on function logTaskProgressWithRetry
#function that inserts new row in logTaskProgress table which is called in try block
#test that takes count of new row that has been inserted in logTaskProgress table
try:
  #declare variables
  testCaseScenario = 'Log task progress with Retry-Check for row insertion of in logTaskProgress table'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  requiredInputParameter = '(dbconn, conn,cursor,,batchTaskId,batchTaskProgressMessage)'
  #function call
  batchTaskProgressMessage = 'Test progress message for Log task progress with Retry-Check in logging functions{}'.format(date)
  #Break the connection manually by assigning randon text to connection
  cursor='Break Connection'
 #call the functon with Retry  
  conn,cursor= logTaskProgressWithRetry(dbconn, conn,cursor,newBatchTaskId,batchTaskProgressMessage)
#   print(newBatchTaskId,batchTaskProgressMessage)
  #reads result as pandas dataframe
  fnInsertOutput = pd.read_sql_query("SELECT * FROM audit.tbl_batch_task_progress WHERE batch_task_id = {} AND batch_task_progress_message = '{}'".format(newBatchTaskId,batchTaskProgressMessage),conn)
  #compares the count of inserted record in tbl_batch_task_progress
  if fnInsertOutput.shape[0] >= 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert into tbl_unit_test on the status of test_case  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)  
  logTaskProgress(cursor,batchTaskId,'Unit test on logTaskProgress function performed successfully')
except Exception as e:
  errorMessage = "Exception occurred while testing on logTaskProgress function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test on function logToFile
#this function creates a file and logs the error message. This function is called in exception block of log_error function.
#the test checks the path exists and reads the file from the location
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'logToFile-Check creation of log_file and logging error_message'
  requiredInputParameter = 'errorMessage,logFileLocation'
  
  #specify the message that has to be written into the log_file
  errorLogMessage = 'Test on creation of log file {}'.format(date)
  logFileLocation = getLoggingPath(batchId,newBatchTaskId,date,'error')
  
  #function call which creates a file and logs error message.
  logToFile(logFileLocation,errorLogMessage)
  
  #check if the file path exists
  if path.exists(logFileLocation) == True:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert into tbl_unit_test on the status of test_case   
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus) 
  logTaskProgress(cursor,batchTaskId,'Unit test on logToFile function performed successfully')
except Exception as e:
  errorMessage = "Exception occurred while testing on logToFile function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test on function logError
#this function is called in main exception block, that updates batch_task_status as failed in batch_task_table and makes an entry into error_log table.
#the test takes the count of inserted rows in error_log table and checks the update batch_task_status as failed in batch_task_table
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'log_error-Check if error message is inserted into error_log table,update of batch_task_status as failed in batch_task table'
  requiredInputParameter = 'cursor,batchTaskId,errorLine,errorMessage,adfPipelineName,clusterId,notebookName,logFileLocation'
  
  #parameter values for function
  errorLogMessage = 'Test on insertion of error message {}'.format(date)
  errorLine = '1'
  adfPipelineName = 'testpipeline'
  clusterId = '12345-abcde'
  notebookName = 'unit test logging function'
  logFileLocation = getLoggingPath(batchId,newBatchTaskId,date,'error')
  
  #function call
  logError(cursor,newBatchTaskId,
           errorLogMessage,adfPipelineName,
           clusterId,notebookName,logFileLocation) 

  #check for the batch_task status as failed in batch_task table
  fnUpdateOut = pd.read_sql_query("SELECT * FROM audit.tbl_batch_task WHERE batch_task_id = {} AND batch_task_status = 'failed'".format(newBatchTaskId),conn)

  #select the data inserted in error_log table 
  fnInsertOut = pd.read_sql_query("SELECT * FROM audit.tbl_error_log WHERE batch_task_id = {} AND error_message = '{}'".format(newBatchTaskId,errorLogMessage),conn)

  #compare the count of inserted record in error_log table and updated record in batch_task table
  if fnInsertOut.shape[0] == 1 and fnUpdateOut.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert into tbl_unit_test on the status of test_case   
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  
  logTaskProgress(cursor,batchTaskId,'Unit test on log_error function performed successfully')
except Exception as e:
  errorMessage = "Exception occurred while testing on log_error function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Negative test case on function logError
#the log_error function has try and except block. in try block it logs error into database when it fails the exception block executes logToFile function.
#the test scenario is when an invalid cursor parameter is passed into the function it fails to log the error message in database but writes into file by calling logToFile.

try:
  #Declare variables
  testCaseScenario = 'log_error-Negative test case for validating creation of log file'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  requiredInputParameter = 'cursor,batchTaskId,errorLine,errorMessage,adfPipelineName,clusterId,notebookName,logFileLocation'
  
  #error value for cursor which should execute exception block and call logToFile function
  errorCursor = 'abdc'
  errorLogMessage = 'negative test scenario of log_error function which fails to write error_message into database but logs in a file'
  
  
 #specify the path of log_file 
  loggingPath = getLoggingPath(batchId,newBatchTaskId,date,'error')
  print(loggingPath)
  #error parameter for cursor value which fails log into database and log into file
  logError(errorCursor
           ,newBatchTaskId
           ,errorLogMessage
           ,adfPipelineName
           ,clusterId
           ,notebookName
           ,loggingPath)
  
  #check if the log_file is created
  if bool(dbutils.fs.ls(loggingPath[5:])) == True:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert into tbl_unit_test on the status of test_case   
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Negative test case on log_error function performed successfully')
except Exception as e:
  errorMessage = "Exception occurred while performing negative test case on log_error function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test on function logAvailabilityDates
#the function that inserts a new record in tbl_object_dates_availability with given output_datetime_to for the specified object_id
#the test checks the count of newly inserted record and confirms the success of the function
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'logAvailabilityDates-check for insertion of record in object_dates_availability table'
  requiredInputParameter = 'cursor,batchTaskId,objectId,datetimeTo'
  
  #function call
  logAvailabilityDates(cursor,newBatchTaskId,destinationObjectId,newDatetimeTo)
  
  #read sql statement as pandas dataframe
  fnInsertOut = pd.read_sql_query("SELECT * FROM audit.tbl_object_dates_availability WHERE batch_task_id = {} AND output_object_id = {} AND output_datetime_to = '{}'".format(newBatchTaskId,destinationObjectId,newDatetimeTo),conn)
  
 #compare the count of inserted record
  if fnInsertOut.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert into tbl_unit_test on the status of test_case    
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)  
  logTaskProgress(cursor,batchTaskId,'Unit test on logAvailabilityDates function performed successfully')
except Exception as e:
  errorMessage = "Exception occurred while performing unit test case on logAvailabilityDates function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Data Preparation-logMarkDatesLoaded
#execute scripts to insert values into tbl_object_dates_availability
try:
  updateScript = open("{}loggingFunctionsInsert.sql".format(testInputPath),"r").read()
  updateScript = updateScript.replace('<batch_task_id>',str(newBatchTaskId)).replace('<input_object_id>',str(sourceObjectId)).replace('<output_object_id>',str(destinationObjectId))
  cursor.execute(updateScript)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed data preparation scripts for logTaskEnd function')
except Exception as e:
  errorMessage = "Exception occurred while execution of data preparation scripts for logTaskEnd function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test on function logMarkDatesLoaded
#the function updates the record in tbl_object_dates_availability, for output_datetime_from and output_datetime_to columns for the given object_id.
#the test selects the updated record and checks the count of it.
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'logMarksDatesLoaded - Check for updation of record in tbl_object_dates_availability'
  requiredInputParameter = 'cursor,batchTaskId,destinationObjectId, sourceObjectId,outputDatetimeFrom,outputDatetimeTo'
  
  #function call
  
  logMarkDatesLoaded(cursor,newBatchTaskId, destinationObjectId, sourceObjectId, markDatetimeFrom, markDatetimeTo)
  
  #read sql statement as pandas dataframe which checks the record has been updated
  fnUpdateOut = pd.read_sql_query("SELECT * FROM audit.tbl_object_dates_availability WHERE batch_task_id = {} AND input_object_id = {} AND output_object_id = {} AND output_datetime_from = '{}' AND output_datetime_to = '{}' AND load_successful = 1".format(newBatchTaskId, sourceObjectId, destinationObjectId, markDatetimeFrom, markDatetimeTo),conn)
  
  #compare the record count
  if fnUpdateOut.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert into tbl_unit_test on the status of test_case  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                        testCaseScenario,executionOutputStatus,sampleOutputLocation,
                        datetime.now(),testCaseStatus) 
  logTaskProgress(cursor,batchTaskId,'Unit test on logMarksDatesLoaded function performed successfully')
except Exception as e:
  errorMessage = "Exception occurred while performing unit test case on logMarksDatesLoaded function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Data Preparation-logTaskEnd
#execute scripts to update status of batch_task as running
try:
  updateScript = open("{}loggingFunctionsUpdate.sql".format(testInputPath),"r").read()
  updateScript = updateScript.replace('<batch_task_id>',str(newBatchTaskId))
  cursor.execute(updateScript)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed data preparation scripts for logTaskEnd function')
except Exception as e:
  errorMessage = "Exception occurred while execution of data preparation scripts for logTaskEnd function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test on function logTaskEnd
#the function is called at end of the each notebook which marks batch_task_status as completed/cancelled in batch_task table
#the test takes the count of record where the batch_task_status is completed
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'logTaskEnd - checks on batch_task_status as completed in batch_task table'
  requiredInputParameter = 'cursor,batchTaskId,batchTaskStatus,batchTaskSourceRows,batchTaskRowsLoaded, batchTaskRejectRows,batchTaskResult,batchTaskResultLocation' 
  
  #parameter values for logTaskEnd
  batchTaskStatus = 'completed'
  
  #function call
  logTaskEnd(cursor,newBatchTaskId,batchTaskStatus,
                batchTaskSourceRows,batchTaskRowsLoaded,
                batchTaskRejectRows,batchTaskResult,
                batchTaskResultLocation)
  
  #check the batch_task_status as completed in batch_task_table
  fnUpdateOut = pd.read_sql_query("SELECT * FROM audit.tbl_batch_task WHERE batch_task_id = {} AND batch_task_status = 'completed'".format(newBatchTaskId),conn)
  
  #check the count of updated record
  if fnUpdateOut.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert into tbl_unit_test on the status of test_case   
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test on logTaskEnd function performed successfully')
except Exception as e:
  errorMessage = "Exception occurred while performing unit test case on logTaskEnd function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test on function logUnitTestStatus
#the function is called to make an entry about the status of unit_test
#the test check for the inserted row in tbl_unit_test_result and confirm the success
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'logUnitTestStatus - check for insertion of record in unit_test_status_table'
  requiredInputParameter = 'testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,sampleOutputLocation,exceutionDatetime,testCaseStatus'
  
  #parameters for the functions
  sampleTestCaseScenario = 'check for test status insertion {}'.format(date)
  sampleExecutionOutputStatus = 'As expected'
  sampleTestCaseStatus = 'success'
  
  #function call
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                        sampleTestCaseScenario,sampleExecutionOutputStatus,sampleOutputLocation,
                        datetime.now(),sampleTestCaseStatus)
  
  #read sql as pandas dataframe to check a record has been inserted
  fnInsertOut = pd.read_sql_query("SELECT * FROM audit.tbl_unit_test_result WHERE test_case_scenario='{}'".format(sampleTestCaseScenario),conn)
  
  #check the row count
  if fnInsertOut.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert into tbl_unit_test on the status of test_case   
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                        testCaseScenario,executionOutputStatus,sampleOutputLocation,
                        datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test on logUnitTestStatus function performed successfully')
except Exception as e:
  errorMessage = "Exception occurred while performing unit test case on logUnitTestStatus function: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}loggingFunctionsCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  
  #remove the test file that has been created in mnt location
  dbutils.fs.rm('/mnt/log/databricks/error/date={}/'.format(date.strftime("%Y-%m-%d")),True)  
  
  logTaskProgress(cursor,batchTaskId,'Successfully executed cleanup scripts')   
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Close database connection
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