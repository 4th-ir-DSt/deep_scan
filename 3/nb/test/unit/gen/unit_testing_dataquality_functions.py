# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>Unit test for unit_testing_dataquality_functions</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Unit test for dataquality_functions notebook</td></tr>
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
# MAGIC </table> 
# MAGIC   
# MAGIC ## Change Log
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Date</th>
# MAGIC     <th>Changed By</th>
# MAGIC     <th>Change Description</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC     <td></td>
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

# DBTITLE 1,Run dataquality_functions notebook
# MAGIC %run ../../../util/spe/dataquality_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Import Module
try:
  import pandas as pd
  import pyodbc
  import sys
  import copy
  from datetime import datetime, timedelta
  from decimal import Decimal
  from pyspark.sql.functions import lit,col,broadcast,concat_ws,concat,when,sum, avg, max, min
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType

except Exception as e:
  errorMessage = "Exception occurred while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise Variables
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
  batchTaskSourceRows = ''
  batchTaskRowsLoaded = ''
  batchTaskRejectRows = '' 
  batchTaskResultLocation = ''
  batchTaskResult = '' 
    
  #Parameter for logging into tbl_unit_test_result  
  testCaseScenario = ''
  outputLocation = ''
  testObject = 'notebook'
  testObjectName = 'dataqualityFunctions'
  requiredInputParameter = 'batchTaskId'
  #path for datasets used in Standard functions
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  #Paths to create table
  tableInputPath = testInputPath.replace('/dbfs','')
  tableOutputPath = testOutputPath.replace('/dbfs','')
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')
  
except Exception as e:
  errorMessage = "Exception occurred while variable declaration " + str(e)
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
 
  #Call the getLoggingPath function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occurred while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish Database Connection
try:
  dbConn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbConn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = "Exception occurred while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Delete the table if exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Source ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Target ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Source1 ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Target1 ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Source2 ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Target2 ;

# COMMAND ----------

# DBTITLE 1,Metadata cleanup for previous runs
try:
  #run cleanup script
  cleanupMetadata = open("{}DataQualityFunctionsCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanupMetadata)
  while cursor.nextset():
    x = 1 
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')
except Exception as e:
  errorMessage = "Exception occurred while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Remove the folders if it exists
try :
  #clean up the delta table folders for testing
  dbutils.fs.rm("/mnt/dataquality/unit_tests/dataqualityFunctions/tables" , True);
except Exception as e:
  errorMessage = 'Exception occurred while deleting the delta table folders: ' + str(e)
  print(errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Create source and target table 
# MAGIC %sql
# MAGIC 
# MAGIC create table TestDataQualityFunctions_Source (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/dataqualityFunctions/tables/TestDataQualityFunctions_Source";
# MAGIC 
# MAGIC create table TestDataQualityFunctions_Target (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/dataqualityFunctions/tables/TestDataQualityFunctions_Target";
# MAGIC 
# MAGIC create table TestDataQualityFunctions_Source1 (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/dataqualityFunctions/tables/TestDataQualityFunctions_Source1";
# MAGIC 
# MAGIC create table TestDataQualityFunctions_Target1 (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/dataqualityFunctions/tables/TestDataQualityFunctions_Target1";
# MAGIC 
# MAGIC create table TestDataQualityFunctions_Source2 (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/dataqualityFunctions/tables/TestDataQualityFunctions_Source2";
# MAGIC 
# MAGIC create table TestDataQualityFunctions_Target2 (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/dataqualityFunctions/tables/TestDataQualityFunctions_Target2";

# COMMAND ----------

# DBTITLE 1,Insert data into tables
# MAGIC %sql
# MAGIC --Source
# MAGIC insert into TestDataQualityFunctions_Source  values(111,'aaa',4000);
# MAGIC insert into TestDataQualityFunctions_Source  values(222,'bbb',5000);
# MAGIC insert into TestDataQualityFunctions_Source  values(333,'ccc',6000);
# MAGIC insert into TestDataQualityFunctions_Source  values(444,'ddd',7000);
# MAGIC 
# MAGIC --Destination
# MAGIC insert into TestDataQualityFunctions_Target  values(333,'ccc',6000);
# MAGIC insert into TestDataQualityFunctions_Target  values(444,'ddd',7000);
# MAGIC 
# MAGIC --Source1
# MAGIC insert into TestDataQualityFunctions_Source1  values(111,'aaa',4000);
# MAGIC insert into TestDataQualityFunctions_Source1  values(222,'bbb',5000);
# MAGIC insert into TestDataQualityFunctions_Source1  values(333,'ccc',6000);
# MAGIC insert into TestDataQualityFunctions_Source1  values(444,'ddd',7000);
# MAGIC 
# MAGIC --Destination1
# MAGIC insert into TestDataQualityFunctions_Target1  values(333,'ccc',6000);
# MAGIC insert into TestDataQualityFunctions_Target1  values(444,'ddd',7000);
# MAGIC 
# MAGIC --Source2
# MAGIC insert into TestDataQualityFunctions_Source2  values(111,'aaa',4000);
# MAGIC insert into TestDataQualityFunctions_Source2  values(222,'bbb',5000);
# MAGIC insert into TestDataQualityFunctions_Source2  values(333,'ccc',6000);
# MAGIC insert into TestDataQualityFunctions_Source2  values(444,'ddd',7000);
# MAGIC 
# MAGIC --Destination2
# MAGIC insert into TestDataQualityFunctions_Target2  values(333,'ccc',6000);
# MAGIC insert into TestDataQualityFunctions_Target2  values(444,'ddd',7000);

# COMMAND ----------

# DBTITLE 1,Data preparation for test
#execute scripts to
try:
  #inserting data into tbl_task and tbl_batch_task
  inputParameter = open('{}DataQualityFunctionsDataPrep1.sql'.format(testInputPath), "r").read()
  #execute the prep1 script and get the first row
  inputParameterResults = pd.read_sql_query(inputParameter,conn).iloc[0]

  #get the first value of the row and cast as int
  newBatchTaskId = int(inputParameterResults[0])
  logTaskProgress(cursor,batchTaskId,'Execution of script to generate for input parameter')
except Exception as e:
  errorMessage = "Exception occured while executing of test scenario-1 data preparation: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Execute the getDataQualityTest function
try:
  dataqualityTestsDf = dataqualityFunctions.getDataQualityTest( conn
                                                              , cursor
                                                              , newBatchTaskId
                                                              , adfPipelineName
                                                              , clusterId
                                                              , notebookName
                                                              , errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Function getDataQualityTest executed successfully')
except Exception as e:
  errorMessage = "Exception occured while executing function getDataQualityTest: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Execute the executeDataQualityTests function
try:
  dataqualityFunctions.executeDataQualityTests( dataqualityTestsDf
                                              , cursor
                                              , newBatchTaskId
                                              , adfPipelineName
                                              , clusterId
                                              , notebookName
                                              , errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Execution of function executeDataQualityTests completed successfully')
except Exception as e:
  errorMessage = "Exception occured while executing function executeDataQualityTests: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-1 Verify tbl_data_quality_test_result status to check 'RowCountComparison'
# While preparing data for source and target tables by using where condition created tables source and destination.while inserting data in source salary >5000 record count 2 and target also salary > 5000 record count 2.So,This executeDataQualityTests function has to return success status.
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify tbl_data_quality_test_result status to check "RowCountComparison"' 
  #searchstring in tbl_data_quality_test_result column
  testComparisionString = 'Row counts between tables'
  testBatchTaskId = newBatchTaskId
  # Execute the query to get output
  outputStatus = pd.read_sql_query("select data_quality_test_result from audit.tbl_data_quality_test_result where batch_task_id = '{}' and data_quality_test_message like '{}%'".format(testBatchTaskId,testComparisionString), conn)
  #If query output is succeeded then make test case as success
  if outputStatus.at[0,'data_quality_test_result'] == 'succeeded':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 1 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 1 : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-2 Verify tbl_data_quality_test_result status to check 'AggregationValuesComparison'
# While preparing data for source and target tables by using where condition created tables source and destination.while inserting data in source salary >5000 sum value and destination is same.So,This executeDataQualityTests function has to return success status.
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify tbl_data_quality_test_result status to check "AggregationValuesComparison"' 
  #searchstring in tbl_data_quality_test_result column
  testComparisionString = 'Aggregated values between tables'
  testBatchTaskId = newBatchTaskId
  # Execute the query to get output
  outputStatus = pd.read_sql_query("select data_quality_test_result from audit.tbl_data_quality_test_result where batch_task_id = '{}' and data_quality_test_message like '{}%'".format(testBatchTaskId,testComparisionString), conn)

  #If query output is succeeded then make test case as success
  if outputStatus.at[0,'data_quality_test_result'] == 'succeeded':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 2 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 2 : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-3 Verify tbl_data_quality_test_result status to check 'NullValuesCheck'
# While preparing data for target salary coulmn has no Null values.So,This executeDataQualityTests function has to return success status.
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify tbl_data_quality_test_result status to check "NullValuesCheck"' 
  #searchstring in tbl_data_quality_test_result column
  testComparisionString = 'Null counts check for table'
  testBatchTaskId = newBatchTaskId
  # Execute the query to get output
  outputStatus = pd.read_sql_query("select data_quality_test_result from audit.tbl_data_quality_test_result where batch_task_id = '{}' and data_quality_test_message like '{}%'".format(testBatchTaskId,testComparisionString), conn)

  #If query output is succeeded then make test case as success
  if outputStatus.at[0,'data_quality_test_result'] == 'succeeded':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 3 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup
try:
  #run cleanup script
  cleanupMetadata = open("{}DataQualityFunctionsCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanupMetadata)
  while cursor.nextset():
    x = 1 
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')
except Exception as e:
  errorMessage = "Exception occurred while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Delete the table if exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Source ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Target ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Source1 ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Target1 ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Source2 ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Target2 ;

# COMMAND ----------

# DBTITLE 1,Remove the folders if it exists
try :
  #clean up the delta table folders for testing
  dbutils.fs.rm("/mnt/dataquality/unit_tests/dataqualityFunctions/tables" , True);
except Exception as e:
  errorMessage = 'Exception occurred while deleting the delta table folders: ' + str(e)
  print(errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Create source and target table 
# MAGIC %sql
# MAGIC 
# MAGIC create table TestDataQualityFunctions_Source (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/dataqualityFunctions/tables/TestDataQualityFunctions_Source";
# MAGIC 
# MAGIC create table TestDataQualityFunctions_Target (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/dataqualityFunctions/tables/TestDataQualityFunctions_Target";
# MAGIC 
# MAGIC create table TestDataQualityFunctions_Source1 (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/dataqualityFunctions/tables/TestDataQualityFunctions_Source1";
# MAGIC 
# MAGIC create table TestDataQualityFunctions_Target1 (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/dataqualityFunctions/tables/TestDataQualityFunctions_Target1";
# MAGIC 
# MAGIC create table TestDataQualityFunctions_Source2 (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/dataqualityFunctions/tables/TestDataQualityFunctions_Source2";
# MAGIC 
# MAGIC create table TestDataQualityFunctions_Target2 (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/dataqualityFunctions/tables/TestDataQualityFunctions_Target2";

# COMMAND ----------

# DBTITLE 1,Insert data into tables
# MAGIC %sql
# MAGIC --Source
# MAGIC insert into TestDataQualityFunctions_Source  values(111,'aaa',4000);
# MAGIC insert into TestDataQualityFunctions_Source  values(222,'bbb',5000);
# MAGIC insert into TestDataQualityFunctions_Source  values(333,'ccc',6000);
# MAGIC insert into TestDataQualityFunctions_Source  values(444,'ddd',7000);
# MAGIC 
# MAGIC --Destination
# MAGIC insert into TestDataQualityFunctions_Target  values(333,'ccc',6000);
# MAGIC 
# MAGIC --Source1
# MAGIC insert into TestDataQualityFunctions_Source1  values(111,'aaa',4000);
# MAGIC insert into TestDataQualityFunctions_Source1  values(222,'bbb',5000);
# MAGIC insert into TestDataQualityFunctions_Source1  values(333,'ccc',6000);
# MAGIC insert into TestDataQualityFunctions_Source1  values(444,'ddd',7000);
# MAGIC 
# MAGIC --Destination1
# MAGIC insert into TestDataQualityFunctions_Target1  values(333,'ccc',5000);
# MAGIC insert into TestDataQualityFunctions_Target1  values(444,'ddd',7000);
# MAGIC 
# MAGIC --Source2
# MAGIC insert into TestDataQualityFunctions_Source2  values(111,'aaa',4000);
# MAGIC insert into TestDataQualityFunctions_Source2  values(222,'bbb',5000);
# MAGIC insert into TestDataQualityFunctions_Source2  values(333,'ccc',6000);
# MAGIC insert into TestDataQualityFunctions_Source2  values(444,'ddd',7000);
# MAGIC 
# MAGIC --Destination2
# MAGIC insert into TestDataQualityFunctions_Target2  values(333,'ccc',NULL);
# MAGIC insert into TestDataQualityFunctions_Target2  values(444,'ddd',7000);

# COMMAND ----------

# DBTITLE 1,Data preparation for test
#execute scripts to
try:
  #inserting data into tbl_task and tbl_batch_task
  inputParameter = open('{}DataQualityFunctionsDataPrep1.sql'.format(testInputPath), "r").read()
  #execute the prep1 script and get the first row
  inputParameterResults = pd.read_sql_query(inputParameter,conn).iloc[0]

  #get the first value of the row and cast as int
  newBatchTaskId = int(inputParameterResults[0])
  logTaskProgress(cursor,batchTaskId,'Execution of script to generate for input parameter')
except Exception as e:
  errorMessage = "Exception occured while executing of test scenario-1 data preparation: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Execute the getDataQualityTest function
try:
  dataqualityTestsDf = dataqualityFunctions.getDataQualityTest( conn
                                                              , cursor
                                                              , newBatchTaskId
                                                              , adfPipelineName
                                                              , clusterId
                                                              , notebookName
                                                              , errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Function getDataQualityTest executed successfully')
except Exception as e:
  errorMessage = "Exception occured while executing function getDataQualityTest: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Execute the executeDataQualityTests function
try:
  dataqualityFunctions.executeDataQualityTests( dataqualityTestsDf
                                              , cursor
                                              , newBatchTaskId
                                              , adfPipelineName
                                              , clusterId
                                              , notebookName
                                              , errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Execution of function executeDataQualityTests completed successfully')
except Exception as e:
  errorMessage = "Exception occured while executing function executeDataQualityTests: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-4 Verify tbl_data_quality_test_result for 'RowCountComparison' if row count mismatch
# While preparing data for source and target tables by using where condition created tables source and destination.while inserting data in source salary >5000 record count 2 and for target salary > 5000 record count 1.So,This executeDataQualityTests function has to log failed status.
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify tbl_data_quality_test_result status to check "RowCountComparison"' 
  #searchstring in tbl_data_quality_test_result column
  testComparisionString = 'Row counts between tables'
  testBatchTaskId = newBatchTaskId
  # Execute the query to get output
  outputStatus = pd.read_sql_query("select data_quality_test_result from audit.tbl_data_quality_test_result where batch_task_id = '{}' and data_quality_test_message like '{}%'".format(testBatchTaskId,testComparisionString), conn)
  #If query output is succeeded then make test case as success
  if outputStatus.at[0,'data_quality_test_result'] == 'failed':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 4 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 4 : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-5 Verify data_quality_test_result for  'AggregationValuesComparison' if sum mismatch
# While preparing data for source and target tables by using where condition created tables source and destination.while inserting data in source table and destination salaries are change.So,This executeDataQualityTests function has to log failed status.
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify tbl_data_quality_test_result status to check "AggregationValuesComparison"' 
  #searchstring in tbl_data_quality_test_result column
  testComparisionString = 'Aggregated values between tables'
  testBatchTaskId = newBatchTaskId
  # Execute the query to get output
  outputStatus = pd.read_sql_query("select data_quality_test_result from audit.tbl_data_quality_test_result where batch_task_id = '{}' and data_quality_test_message like '{}%'".format(testBatchTaskId,testComparisionString), conn)

  #If query output is succeeded then make test case as success
  if outputStatus.at[0,'data_quality_test_result'] == 'failed':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 5 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 5 : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-6 Verify tbl_data_quality_test_result for 'NullValuesCheck' if nulls present in column
# While preparing data for target salary coulmn has  Null values.So,This executeDataQualityTests function has to log failed status.
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify tbl_data_quality_test_result status to check "NullValuesCheck" if nulls present' 
  #searchstring in tbl_data_quality_test_result column
  testComparisionString = 'Null counts check for table'
  testBatchTaskId = newBatchTaskId
  # Execute the query to get output
  outputStatus = pd.read_sql_query("select data_quality_test_result from audit.tbl_data_quality_test_result where batch_task_id = '{}' and data_quality_test_message like '{}%'".format(testBatchTaskId,testComparisionString), conn)

  #If query output is succeeded then make test case as success
  if outputStatus.at[0,'data_quality_test_result'] == 'failed':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 6 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 6 : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup
try:
  #run cleanup script
  cleanupMetadata = open("{}DataQualityFunctionsCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanupMetadata)
  while cursor.nextset():
    x = 1 
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')
except Exception as e:
  errorMessage = "Exception occurred while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Delete the table if exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Source ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Target ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Source1 ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Target1 ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Source2 ;
# MAGIC DROP TABLE IF EXISTS TestDataQualityFunctions_Target2 ;

# COMMAND ----------

# DBTITLE 1,Remove the folders if it exists
try :
  #clean up the delta table folders for testing
  dbutils.fs.rm("/mnt/dataquality/unit_tests/dataqualityFunctions/tables" , True);
except Exception as e:
  errorMessage = 'Exception occurred while deleting the delta table folders: ' + str(e)
  print(errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,close database connection
#call taskEndAndCloseConn function to close the database connection and mark end of task in batch_task_table
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