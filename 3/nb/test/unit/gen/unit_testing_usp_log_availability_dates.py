# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_log_availability_dates</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test on stored procedure usp_log_availability_dates</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2019/01/01</td></tr>
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
# MAGIC    <tr>
# MAGIC     <td>2019/01/01</td>
# MAGIC     <td>Framework</td>
# MAGIC     <td>stored procedure tweak-correcting column name from object_id to output_object_id</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td> </td>
# MAGIC     <td> </td>
# MAGIC     <td> </td>
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
  from datetime import datetime
except Exception as e:
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try :
  #varible for current_time
  currentTs=datetime.now()
  #get the last 3 positions of the microseconds as we need to remove them
  currentTsMicroseconds = int(str(currentTs.strftime('%f'))[3:6])
  #take away the last three microseconds
  currentTs = currentTs - timedelta(microseconds=currentTsMicroseconds)
  #get the date as an int format
  CreatedDate = int(currentTs.strftime('%Y%m%d'))
  #get the hour
  CreatedHour = currentTs.hour
  CreatedTimestamp = currentTs
  LastUpdatedTimestamp = currentTs
  #use the same time for the date also
  date = currentTs
  
  #parameter for logError
  errorLine = ''
  errorMessage = ''
  adfPipelineName = ''
  clusterId = ''
  notebookName = ''
  batchId = -1
  batchTaskId = -1
  
  #parameter for logTaskEnd
  batchTaskSourceRows = ''
  batchTaskRowsLoaded = ''
  batchTaskRejectRows = '' 
  batchTaskResultLocation = ''
  batchTaskResult = '' 
  
  #Parameter for logging into tbl_unit_test_result  
  testObject = 'stored procedure'
  testObjectName = 'uspLogAvailabilityDates'  
  requiredInputParameter = 'batchTaskId,objectId,datetimeTo'
  sampleOutputLocation = 'NA'
  
  #path for datasets used in stored procedure 
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = 'dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
except Exception as e:
  errorMessage = "Exception occurred while variable declaration " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try:   
  #get batchId from widgets
  dbutils.widgets.text("batchId","")
  batchId = dbutils.widgets.get("batchId")
  
  #get adfPipelineName from widgets
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName = dbutils.widgets.get("adfPipelineName")
  
  #get clusterId from widgets
  dbutils.widgets.text("clusterId","")
  clusterId = dbutils.widgets.get("clusterId")
  
  #get notebookName from widgets
  dbutils.widgets.text("notebookName","")
  notebookName = dbutils.widgets.get("notebookName")
  
  #get sourceId from widgets
  dbutils.widgets.text("sourceId","")
  sourceId = dbutils.widgets.get("sourceId")
  
  #get batchTaskId from widgets
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")
 
  #Call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occurred while getting parameters and initialise error_log_location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Established Database connection successfully')
except Exception as e:
  errorMessage = "Exception occurred while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup for previous run
#run cleanup script
try:
  cleanup = open("{}uspLogObjectDatesAvailabilityCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'successfully executed cleanup script for usp_log_availability_dates')
except Exception as e:
  errorMessage = "Exception occurred while 'Executing of cleanup script for usp_log_availability_dates': " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Generate input parameter for stored procedure
#script that insert active object in object table and generate new object_id as parameter
try:
  inputParameter = open("{}uspLogObjectDatesAvailability.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>',str(batchId))
  inputParameterResults = pd.read_sql_query(inputParameter,conn)
  
  #fetch the input parameter from the scripts
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])  
  objectId = int(inputParameterResults.at[0,'output_object_id'])
  
  #trim datetime to first 23 characters to get the proper format
  newDatetimeTo = str(inputParameterResults.at[0,'datetime_to'])[0:23]
  latestDatetimeTo = str(inputParameterResults.at[0,'latest_datetime_to'])[0:23]
  
  logTaskProgress(cursor,batchTaskId,'Execution of script to generate input parameter for usp_log_availability_dates')
except Exception as e:
  errorMessage = "Exception occurred while Executing script to generate input parameter for usp_log_availability_dates: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario 1-Check value of datetime_from for newly inserted record
#execute stored procedure with object,a newly inserted record has default output_datetime_from '2000-01-01 00:00:01.000' 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check for the default date "2000-01-01 00:00:01.000" in output_datetime_from column for new object entry in the log object_date_availability table'
  defaultDateTime = '2000-01-01 00:00:01.000'
  
  #execute stored procedure with output_object_id
  cursor.execute("exec audit.usp_log_availability_dates ?,?,?",newBatchTaskId,objectId,newDatetimeTo)
  while cursor.nextset():
    x = 1
  
  #read entry of object from object_dates_availability table as pandas dataframe
  objectEntry = pd.read_sql_query("SELECT * FROM audit.tbl_object_dates_availability WHERE output_object_id = {} AND batch_task_id = {} ".format(objectId,newBatchTaskId),conn).astype(str)
  

  #compare the output_datetime_to column value with default datetime
  if objectEntry.at[0,'output_datetime_from'] == defaultDateTime:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function to log the test_case_status
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)    
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario 1 test case for usp_log_availability_dates')
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 1 for usp_log_availability dates: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False     

# COMMAND ----------

# DBTITLE 1,Scenario 2-Compare the date of output_datetime_from column for existing record
#when the object already exists in object_date_availabilty table,then for newly inserted record with same object_id and batch_id the output_datetime_from is populated with previous entry of output_datetime_to column
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Compare the value of previous record datetime_to and latest record datetime_from column' 
  
  #check whether object already present in tbl_object_dates_availability if so take the latest inserted record to compare datatime_from column
  previousRecord = pd.read_sql_query("SELECT * FROM audit.tbl_object_dates_availability WHERE batch_task_id = {} AND output_object_id = {} AND output_datetime_to = '{}'".format(newBatchTaskId,objectId,newDatetimeTo),conn).astype(str)
  
  #execute stored procedure to makes an insert for already existing object_id
  cursor.execute("exec audit.usp_log_availability_dates ?,?,?",newBatchTaskId,objectId,latestDatetimeTo)
  while cursor.nextset():
      x = 1
  
  #fetch latest inserted record
  latestRecord = pd.read_sql_query("SELECT * FROM audit.tbl_object_dates_availability WHERE batch_task_id = {} AND output_object_id = {} AND output_datetime_to = '{}'".format(newBatchTaskId,objectId,latestDatetimeTo),conn).astype(str)
  
  #compare the dates between previous datetime_to and latest datetime_from
  if latestRecord.at[0,'output_datetime_from'] == previousRecord.at[0,'output_datetime_to']:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function to log the test_case_status
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                     testCaseScenario,executionOutputStatus,sampleOutputLocation,
                     datetime.now(),testCaseStatus)    
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario 2 test case for stored procedure usp_log_availability_dates')
except Exception as e:
  errorMessage = "Exception occurred while executing scenario 2 test case for stored procedure usp_log_availability_dates: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False
  

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}uspLogObjectDatesAvailabilityCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for usp_log_availability_dates')
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup script for usp_log_availability_dates': " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Close Database connection
# call taskEndAndCloseConn function to close the database connection and mark end of task in batch_task_table
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

# COMMAND ----------

