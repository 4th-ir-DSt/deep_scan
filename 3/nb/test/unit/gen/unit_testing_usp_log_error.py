# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_log_error</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test for stored procedure usp_log_error check for insertion and update of record in respective tables</td></tr>
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
# MAGIC     <td>NotebookName</td>
# MAGIC     <td>@NotebookName to retrieve notebookname </td>
# MAGIC     <td>@NotebookName = 'testnotebook'</td>
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
# MAGIC     <td>Dev rework changes for camel case function names, date variables and removal of error line
# MAGIC         <br>Added parameter and modified command title
# MAGIC         <br>Made stored procedure calls with named parameters</td>
# MAGIC   </tr>
# MAGIC   
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

# DBTITLE 1,Run logging function notebook
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
  from pyspark.sql.types import StringType,IntegerType
except Exception as e:
  errorMessage = "Exception occured while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try:
  #VARIBLE FOR CURRENT_TIME
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
  testCaseScenario = ''
  sampleOutputLocation = ''
  testObject = 'stored procedure'
  testObjectName = 'uspLogError'
  requiredInputParameter = 'batchId,batchTaskId,errorLine,errorMessage,adfPipelineName,clusterID,notebookName'
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
  # Get input parameters for stored procedure
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
   
  #GET batchTaskId FROM WIDGETS
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")
 
  #Call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occured while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish DataBase Connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = "Exception occured while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Generate Input parameter for stored procedure through scripts
try:
  inputParameter = open("{}uspLogErrorPrep.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>', str(batchId))

  #execute the prep1 script and get the first row
  inputParameterResults = pd.read_sql_query(inputParameter,conn).iloc[0]

  #get the first value of the row and cast as int
  newBatchTaskId = int(inputParameterResults[0])
  logTaskProgress(cursor,batchTaskId,'Execution of script to generate a new batch task id for testing. Batch task id generated is {}'.format(newBatchTaskId))
except Exception as e:
  errorMessage = "Exception occured while 'Executing script to generate a new batch task id for testing': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Execute usp_log_error stored procedure
#execute stored procedure to make insert new entry in error_log_table and update status failed in batch_task_table
try:
  #execute log_task_start procedure to update batch status as running so log error stored procedure can update the status
  cursor.execute("exec audit.usp_log_task_start ?",newBatchTaskId)
  #execute stored procedure with the error message
  errorMessage = 'UnitTesting on LogError StoredProcedure {}'.format(date)
  cursor.execute("exec audit.usp_log_error @batch_task_id = ?, @error_message = ?, @adf_pipeline_name = ?, @cluster_id = ?, @notebook_name = ?",newBatchTaskId,errorMessage,adfPipelineName,clusterId,notebookName)  
  while cursor.nextset():
      x = 1
  logTaskProgress(cursor,batchTaskId,'Executed usp_log_error successfully')    
except Exception as e:
  errorMessage = 'Exception occured while executing stored procedure usp_log_error: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 1-Check for the update of batch_task_status = 'failed'
#stored procedure should have updated batch_task_status as 'failed' in batch_task_table,  checking if count of record with status failed if 1 is returned then confirms the success
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'update check on batch task status as failed'
  #reading stored procedure as pandas dataframe the select statement counts the entry if 1 then the status has been updated by stored procedure successfully
  spUpdateOut = pd.read_sql_query("select * from audit.tbl_batch_task where batch_task_id = {} and batch_task_status = 'failed'".format(newBatchTaskId),conn)
  #compare the count of dataframe if 1 then execute stored procedure as success else with failed status
  if spUpdateOut.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario1 test case for stored procedure usp_log_error') 
except Exception as e:
  errorMessage = "Exception occured while executing scenario1 test case for stored procedure usp_log_error: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 2-Check for insert of row in tbl_error_log
#stored procedure should have inserted new record in error log table checking if count of row is 1 then confirms the success
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  #use this error message to compare
  errorMessage = 'UnitTesting on LogError StoredProcedure {}'.format(date)
  testCaseScenario = 'insert new row check in error_log_table'
  #read count of inserted data into error log table by the stored procedure 
  spInsertOut = pd.read_sql_query("select * from audit.tbl_error_log where batch_task_id = {} and error_message = '{}'".format(newBatchTaskId,errorMessage),conn)
  #when the value is equal to 1 it indicates that a new record is inserted
  if spInsertOut.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to log the test case status into tbl_unit_test 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario2 test case for stored procedure usp_log_error') 
except Exception as e:
  errorMessage = "Exception occured while executing scenario2 test case for stored procedure usp_log_error: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 3-Data Preparation
#execute scripts that update batch_task status as 'pending'
try:
  updateScript = open("{}uspLogErrorScript1.sql".format(testInputPath), "r").read()
  updateScript = updateScript.replace('<batch_task_id>', str(newBatchTaskId))
  cursor.execute(updateScript)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed update scripts for test scenario 3')   
except Exception as e:
  errorMessage = "Exception occured while executing update scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 3-Check for the failure of update in batch_task_table
#Testing the update of batch_task_status with pending as previous state. it should fail as stored procedure marks status ='failed' only when previous status is 'running'
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Negative test case for update statement'
  #run the procedure again to test that status is not updated as 'failed'
  cursor.execute("exec audit.usp_log_error @batch_task_id = ?, @error_message = ?, @adf_pipeline_name = ?, @cluster_id = ?, @notebook_name = ?",newBatchTaskId,errorMessage,adfPipelineName,clusterId,notebookName)
  while cursor.nextset():
      x = 1
  #trying to read table whether update happens, then stored procedure fails.   
  spUpdateNegOut = pd.read_sql_query("select * from audit.tbl_batch_task where batch_task_id = {} and batch_task_status = 'failed'".format(newBatchTaskId),conn)
  #compare the count of row in dataframe if count is equal to 0 then success
  if spUpdateNegOut.shape[0] == 0:
    executionOutputStatus='As expected'
    testCaseStatus='success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario3 test case for stored procedure usp_log_error') 
except Exception as e:
  errorMessage = "Exception occured while executing scenario3 test case for stored procedure usp_log_error: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}uspLogErrorScript1CleanUp.sql".format(testInputPath), "r").read()
  cleanup = cleanup.replace('<batch_task_id>', str(newBatchTaskId))
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for usp_log_error')
except Exception as e:
  errorMessage = "Exception occured while 'Executing of cleanup script for usp_log_error': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False     

# COMMAND ----------

# DBTITLE 1,Close Database connection
# call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows,batchTaskResult,
                            batchTaskResultLocation,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False