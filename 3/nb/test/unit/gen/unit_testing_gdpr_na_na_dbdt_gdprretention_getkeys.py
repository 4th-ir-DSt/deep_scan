# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_gdpr_na_na_dbdt_gdprretention_getkeys</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Unit test for gdpr_na_na_dbdt_gdprretention_getkeys</td></tr>
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
# MAGIC     <td>Changes to use the object name instead of the source id for the reference tables
# MAGIC       <br>Changed the column ProcessedTimestamp to RedactedTimestamp
# MAGIC       <br>Changes to the temp deltables for gdpr retention
# MAGIC       <br>Added batchId in getRetentionKeys function and changed GdprRetentionGetKeysTestgdpr_object table structure</td>
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
  from datetime import datetime, timedelta
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType

except Exception as e:
  errorMessage = "Exception occured while importing modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run Miscellaneous Functions notebook
# MAGIC %run ../../../util/spe/misc_functions

# COMMAND ----------

# DBTITLE 1,Run GDPR Function notebook
# MAGIC %run ../../../util/spe/gdpr_functions

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
  
  #Parameter for logging into tbl_unit_test_result  
  testObject = 'notebook'
  testObjectName = 'gdprRetentionGetKeys'  
  requiredInputParameter = 'sourceId'
  sampleOutputLocation = 'NA'
  
  #path for datasets used in stored procedure 
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = 'dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  
  #Paths to create table
  tableInputPath = testInputPath.replace('/dbfs','')
  
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

# DBTITLE 1,Drop the delta table if it exists
# MAGIC %sql
# MAGIC drop table if exists GdprRetentionGetKeysTestgdpr_object;
# MAGIC drop table if exists GdprRetentionGetKeysTestgdpr_reference_object;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(tableInputPath + '/GdprRetentionGetTestgdpr_object',recurse=True)
dbutils.fs.rm(tableInputPath + '/GdprRetentionGetKeysTestgdpr_reference_object',recurse=True)

# COMMAND ----------

# DBTITLE 1,Metadata cleanup for previous run
#run cleanup script
try:
  cleanup = open("{}GdprRetentionGetKeysCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'successfully executed cleanup script for GdprRetentionGetKeysCleanup')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for GdprRetentionGetKeysCleanup': " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Create delta tables
# MAGIC %sql
# MAGIC create table GdprRetentionGetKeysTestgdpr_reference_object(
# MAGIC row_id                                int,
# MAGIC GdprRetentionGetKeysTestattribute     string,
# MAGIC ExpiryDate                            timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRetentionGetKeys/input/GdprRetentionGetKeysTestgdpr_reference_object";
# MAGIC 
# MAGIC -- insert data into table
# MAGIC insert into GdprRetentionGetKeysTestgdpr_reference_object  values(1,'a',current_timestamp());
# MAGIC insert into GdprRetentionGetKeysTestgdpr_reference_object  values(2,'b',date_sub(current_timestamp(),200));
# MAGIC insert into GdprRetentionGetKeysTestgdpr_reference_object  values(3,'c',date_sub(current_timestamp(),200));
# MAGIC 
# MAGIC create table GdprRetentionGetKeysTestgdpr_object(
# MAGIC row_id                                int,
# MAGIC GdprRetentionGetKeysTestattribute     string,
# MAGIC ExpiryDate                            timestamp,
# MAGIC Processed                             boolean,
# MAGIC RedactedTimestamp                     timestamp,
# MAGIC CreatedBatchID                        int,
# MAGIC CreatedTimestamp                      timestamp,
# MAGIC LastUpdatedTimestamp                  timestamp,
# MAGIC LastUpdatedBatchID                    int,
# MAGIC SourceID                              int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRetentionGetKeys/input/GdprRetentionGetKeysTestgdpr_object";
# MAGIC -- insert data into table
# MAGIC insert into GdprRetentionGetKeysTestgdpr_object  values(1,'c',date_sub(current_timestamp(),200),true,current_timestamp(),999,current_timestamp(),current_timestamp(),888,5);

# COMMAND ----------

# DBTITLE 1,Data preparation for testing
#script that generate inputs for the stored procedure
try:
  inputParameter = open("{}GdprRetentionGetKeys.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>', str(batchId))
  
  inputParameterResults = pd.read_sql_query(inputParameter,conn).astype(str)

  #fetch the input parameters
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])
  sourceId = inputParameterResults.at[0,'source_id']
   
  logTaskProgress(cursor,batchTaskId,'Successfully execution of script to generate input parameter for uspGetGdprRetentionReferenceTables')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter for uspGetGdprRetentionReferenceTables: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Get reference table object details
try:
  referenceObjectDf=getMaintenanceObjectsSpExec(conn,cursor,newBatchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  referenceObjectName=referenceObjectDf['object_name'][0]

except Exception as e:
  errorMessage = 'Exception occurred while executing the getManitenanceObjectsSp: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Drop and create temp tables
try:
  if referenceObjectName == 'Staging_Reference_unstructuredSource_MuPolicyHistory':
    gdprFunctions.createPIItempTables(conn,cursor,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)


except Exception as e:
  errorMessage = 'Exception occurred while dropping\creating the temporary tables: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1, Execute the usp_get_gdpr_retention_reference_tables stored procedure
#getRetentionReferenceTable function executes the usp_get_gdpr_retention_reference_tables stored procedure to get the reference tables
try:
  referenceTablesDF = gdprFunctions.getRetentionReferenceTable(referenceObjectName,conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Successfully executed getRetentionReferenceTable function')
  
except Exception as e:
  errorMessage = 'Exception occurred while executing getRetentionReferenceTable:' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Set the reference keys to redact
#get the expired keys to apply retention
try:
  gdprFunctions.getRetentionKeys(referenceTablesDF,conn,cursor,batchId,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Successfully executed getRetentionKeys function')
except Exception as e:
  errorMessage = 'Exception occurred while executing getRetentionKeys:' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-1 Verify that the expired key was inserted in the object table
#Test if the key GdprRetentionGetKeysTestattribute 'b' was inserted in the object table as not processed
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify that the expired key was inserted in the object table' 
  objectTable = 'GdprRetentionGetKeysTestgdpr_object' 
  keyColumn = 'GdprRetentionGetKeysTestattribute'
  expiredValue = "'b'"
  #If count of table = 1 then the result is as expected
  funcOutput = spark.sql("select * from {} where {}={} AND Processed=false".format(objectTable, keyColumn, expiredValue))
  if funcOutput.count() == 1 :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 1 notebook gdprretention_getkeys')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 1 notebook gdprretention_getkeys: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-2 Verify that the non-expired key was not inserted
#Test if the key GdprRetentionGetKeysTestattribute 'a' was not inserted since it was already there
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify that the non-expired key was not inserted in the object table' 
  objectTable = 'GdprRetentionGetKeysTestgdpr_object' 
  keyColumn = 'GdprRetentionGetKeysTestattribute'
  nonExpiredValue = "'a'"
  #If count of table = 0 then the result is as expected
  funcOutput = spark.sql("select * from {} where {}={}".format(objectTable, keyColumn, nonExpiredValue))
  if funcOutput.count() == 0 :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 2 notebook gdprretention_getkeys')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 2 notebook gdprretention_getkeys: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-3 Verify that the already processed key was keeped as processed
#Test if the key GdprRetentionGetKeysTestattribute 'b' was inserted in the object table as not processed
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify that the already processed key was keeped as processed' 
  objectTable = 'GdprRetentionGetKeysTestgdpr_object' 
  keyColumn = 'GdprRetentionGetKeysTestattribute'
  processedValue = "'c'"
  #If count of table = 1 then the result is as expected
  funcOutput = spark.sql("select * from {} where {}={} AND Processed=true".format(objectTable, keyColumn, processedValue))
  if funcOutput.count() == 1 :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 3 notebook gdprretention_getkeys')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 notebook gdprretention_getkeys: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}GdprRetentionGetKeysCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'successfully executed cleanup script for GdprRetentionGetKeysCleanup')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for GdprRetentionGetKeysCleanup': " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Drop the delta table if it exists
# MAGIC %sql
# MAGIC drop table if exists GdprRetentionGetKeysTestgdpr_object;
# MAGIC drop table if exists GdprRetentionGetKeysTestgdpr_reference_object;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(tableInputPath + '/GdprRetentionGetKeysTestgdpr_object',recurse=True)
dbutils.fs.rm(tableInputPath + '/GdprRetentionGetKeysTestgdpr_reference_object',recurse=True)

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