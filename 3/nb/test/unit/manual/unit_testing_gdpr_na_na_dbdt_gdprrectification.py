# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC #Instructions to run the unit testing notebook
# MAGIC 
# MAGIC This notebook is responsible for rectification pii values from a specific table and downstream tables containing that PII and column
# MAGIC <ul>
# MAGIC <li>Run all the notebook steps sequentially</li>

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_gdpr_na_na_dbdt_gdprrectification</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td> unit testing of gdpr_na_na_dbdt_gdprrectification notebook</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Praveen</td></tr>
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
# MAGIC     <td>Added new test cases Scenarios-7,8,9 and 10
# MAGIC       <br>Added the coalesce function import. Changed the PiiHash and PiiTraceabilityHash to bigint.
# MAGIC       <br>Changed data preparation PiiTraceabilityHash to bigint for testcase-10</td>
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
  from pyspark.sql.functions import lit,col,broadcast,concat_ws,concat,when,coalesce
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType

except Exception as e:
  errorMessage = "Exception occurred while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run notebook gdpr_functions
# MAGIC %run ../../../util/spe/gdpr_functions

# COMMAND ----------

# DBTITLE 1,Initialise variables
try:
  
  #variable CURRENT_TIME
  currentTs = datetime.now()
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
  
  #Parameter and paths
  scheduleReference = 'GDPR_Rectification'
  testObject = 'Notebook'
  testObjectName = 'gdprRectification'
  outputLocation = ''
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  requiredInputParameter = 'batchTaskId,errorMessage,adfPipelineName,clusterID,notebookName,keyColumnName,keyTableName,notebookName,stagingTableName'
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')
  executionDateTime = currentTs
  
except Exception as e:
  errorMessage = 'Exception occurred while variable declaration' + str(e)
  print(errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn = pyodbc.connect(dbconn, autocommit = True)
  cursor = conn.cursor()
except Exception as e:
  errorMessage = 'unable to establish SQL Database connection '
  print(errorMessage + str(e))
  assert False

# COMMAND ----------

# DBTITLE 1,Cleanup Script
try:
  #check the status of current batch only when it is in running status the execute the below scripts
  cleanupMetadata1 = open(testInputPath + "gdprRectificationCleanUp1.sql", "r").read()
  cursor.execute(cleanupMetadata1)
  cleanupMetadata2 = open(testInputPath + "gdprRectificationCleanUp2.sql", "r").read()
  cursor.execute(cleanupMetadata2)
  while cursor.nextset():
    x = 1

except Exception as e:
  errorMessage = "Exception occurred while performing metadata cleanup: " + str(e)
  print(errorMessage)
  assert False 

# COMMAND ----------

# DBTITLE 1,Delete the table if exists
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Staging ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Refined ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Person ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Staging1 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Refined1 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Person1 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Staging2 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Refined2 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Person2 ;

# COMMAND ----------

# DBTITLE 1,Remove the folders if it exists
try :
  #clean up the delta table folders for testing
  dbutils.fs.rm("/mnt/dataquality/unit_tests/gdprRectification/tables" , True);
except Exception as e:
  errorMessage = 'Exception occurred while deleting the delta table folders: ' + str(e)
  print(errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Create staging table and the downstream table 
# MAGIC %sql
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Staging (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_uid string,
# MAGIC UUID int,
# MAGIC PiiHash bigint,
# MAGIC PiiTraceabilityHash bigint,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC PiiHashVersion string,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Staging";
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Refined (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_id string,
# MAGIC UUID string,
# MAGIC PiiHash bigint,
# MAGIC PiiTraceabilityHash bigint,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC PiiHashVersion int,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Refined";
# MAGIC 
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Person (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_id string,
# MAGIC gdprRectificationUnitTestObject_name string,
# MAGIC UUID string,
# MAGIC SourceID string,
# MAGIC --StagingCreatedTimestamp timestamp,
# MAGIC PiiHash bigint,
# MAGIC PiiTraceabilityHash bigint,
# MAGIC PiiHashVersion int,
# MAGIC StagingTableName string,
# MAGIC StagingCreatedBatchId int,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC StagingCreatedTimeStamp timestamp,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Person";
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Person2 (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_id string,
# MAGIC gdprRectificationUnitTestObject_name string,
# MAGIC UUID string,
# MAGIC SourceID string,
# MAGIC --StagingCreatedTimestamp timestamp,
# MAGIC PiiHash bigint,
# MAGIC PiiTraceabilityHash bigint,
# MAGIC PiiHashVersion int,
# MAGIC StagingTableName string,
# MAGIC StagingCreatedBatchId int,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC StagingCreatedTimeStamp timestamp,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Person2";

# COMMAND ----------

# DBTITLE 1,Data Preparation for unit test
# Insert the staging table and down stream details in metadatatable 
try:
  #run data preparation scripts 
  dataPrepScenario1 = open(testInputPath + "gdprRectificationDataPrep1.sql", "r").read()
  cursor.execute(dataPrepScenario1)
  while cursor.nextset():
    x = 1
    
except Exception as e:
  errorMessage = "Exception occurred while preparing the data for gdprRectification unit test scenario - 1: " + str(e)
  print(errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Insert data into staging table and the downstream table 
# MAGIC %sql
# MAGIC --Staging
# MAGIC insert into gdprRectificationUnitTestObject_Staging  values(1111,'a',101,111,111,20200101,10,'1','2020-01-01 10:10:10');
# MAGIC insert into gdprRectificationUnitTestObject_Staging  values(2222,'b',102,222,222,20200101,10,'1','2020-01-01 10:10:10');
# MAGIC 
# MAGIC --Refined
# MAGIC insert into gdprRectificationUnitTestObject_Refined  values(1111,'a',101,111,111,20200101,10,1,'2020-01-01 10:10:10');
# MAGIC insert into gdprRectificationUnitTestObject_Refined  values(2222,'b',102,333,333,20200101,10,2,'2020-01-01 10:10:10');
# MAGIC 
# MAGIC --Person
# MAGIC insert into gdprRectificationUnitTestObject_Person  values(1111,'a','xxx',101,842,111,111,1,'gdprRectificationUnitTestObject_Staging',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10');
# MAGIC insert into gdprRectificationUnitTestObject_Person  values(2222,'b','yyy',102,842,444,444,1,'gdprRectificationUnitTestObject_Staging',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10');
# MAGIC 
# MAGIC --Person
# MAGIC insert into gdprRectificationUnitTestObject_Person2 values(1111,'a','zzz',103,842,555,555,1,'gdprRectificationUnitTestObject_Staging',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10');

# COMMAND ----------

# DBTITLE 1,Before executing gdprRectification function - table details
#Test rectification is happening for given pii details. while preparing metadata UUID = 101 pii values is given as pii_Values and 'gdprRectificationUnitTestObject_id' as pii column to rectification function
rectificationHashesUUID = 101
#Get the hash values before rectification function run 
#Staging
beforeRectificationStagingTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Staging WHERE UUID = {}".format(rectificationHashesUUID))
beforeRectificationStagingTableDfPiiHash = beforeRectificationStagingTableDf.select('PiiHash').collect()[0][0]
beforeRectificationStagingTableDfPiiTraceabilityHash = beforeRectificationStagingTableDf.select('PiiTraceabilityHash').collect()[0][0]
beforeRectificationStagingTableDfPiiColumns = beforeRectificationStagingTableDf.select('gdprRectificationUnitTestObject_uid').collect()[0][0]
#Refined
beforeRectificationRefineTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Refined WHERE UUID = {}".format(rectificationHashesUUID))
beforeRectificationRefineTableDfPiiHash = beforeRectificationRefineTableDf.select('PiiHash').collect()[0][0]
beforeRectificationRefineTableDfPiiTraceabilityHash = beforeRectificationRefineTableDf.select('PiiTraceabilityHash').collect()[0][0]
#Person
beforeRectificationPersonTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Person WHERE UUID = {}".format(rectificationHashesUUID)) 
beforeRectificationPersonTableDfPiiHash = beforeRectificationPersonTableDf.select('PiiHash').collect()[0][0]
beforeRectificationPersonTableDfPiiTraceabilityHash = beforeRectificationPersonTableDf.select('PiiTraceabilityHash').collect()[0][0]
beforeRectificationPersonTableDfPiiColumns = beforeRectificationPersonTableDf.select('gdprRectificationUnitTestObject_id').collect()[0][0]

nonRectificationHashesUUID = 103
#Person2
beforeRectificationPerson2TableDf = spark.sql("select * from gdprRectificationUnitTestObject_Person2 WHERE UUID = {}".format(nonRectificationHashesUUID)) 
beforeRectificationPerson2TableDfPiiHash = beforeRectificationPerson2TableDf.select('PiiHash').collect()[0][0]
beforeRectificationPerson2TableDfPiiTraceabilityHash = beforeRectificationPerson2TableDf.select('PiiTraceabilityHash').collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Get all pii columns list
connRectification, cursorRectification, allPiiFieldsDF = getPiiFields()

# COMMAND ----------

# DBTITLE 1,specify personal values as dictionary
personal_values = {
  "gdprRectificationUnitTestObject_id" : "Z"
}

# COMMAND ----------

# DBTITLE 1,Specify pii values to rectification
pii_values = [('111','111', 1)
             ]

# COMMAND ----------

# DBTITLE 1,Execute gdprRectification function
gdprFunctions.gdprRectification(personal_values, allPiiFieldsDF, pii_values, connRectification, cursorRectification)

# COMMAND ----------

# DBTITLE 1,Get the batch_id and batch_task_id
#select the new batch_id and batch_task_id inserted 
try :
  currentBatch = pd.read_sql_query("SELECT MAX(ba.batch_id) AS batch_id, MAX(tbt.batch_task_id) AS batch_task_id FROM audit.tbl_batch  ba JOIN config.tbl_schedule sc ON ba.schedule_id = sc.schedule_id JOIN audit.tbl_batch_task tbt ON ba.batch_id = tbt.batch_id WHERE sc.schedule_reference = '{}'".format(scheduleReference), conn)
  currentBatchDF = spark.createDataFrame(currentBatch)
  currentBatchDic = currentBatchDF.collect()[0].asDict() 
  batchId = currentBatchDic['batch_id']
  batchTaskId = currentBatchDic['batch_task_id'] 
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')
except Exception as e:
  errorMessage = 'batchStatusCheck function call failed '
  print(errorMessage + str(e))
  assert False

# COMMAND ----------

# DBTITLE 1,scenario - 1 Verify pii values of staging table for given pii_hash and column details
#Test rectification is happening for given pii details. while preparing metadata UUID = 101 pii values is given as pii_Values and 'gdprRectificationUnitTestObject_id' as pii column to rectification function
try:
  testCaseScenario ='Verify pii values of staging table for given pii_hash and column details'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  rectificationHashesUUID = 101
  #Get the hash values after rectification function run 
  afterRectificationStagingTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Staging where UUID = {}".format(rectificationHashesUUID))
  afterRectificationStagingTableDfPiiHash = afterRectificationStagingTableDf.select('PiiHash').collect()[0][0]
  afterRectificationStagingTableDfPiiTraceabilityHash = afterRectificationStagingTableDf.select('PiiTraceabilityHash').collect()[0][0]
  afterRectificationStagingTableDfPiiColumns = afterRectificationStagingTableDf.select('gdprRectificationUnitTestObject_uid').collect()[0][0]
  #Check before and after rectification function run 
  if afterRectificationStagingTableDfPiiHash != beforeRectificationStagingTableDfPiiHash and afterRectificationStagingTableDfPiiTraceabilityHash != beforeRectificationStagingTableDfPiiTraceabilityHash and afterRectificationStagingTableDfPiiColumns == 'Z':
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log the test result in database
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully tested scenario-1 for staging table')
except Exception as e:
    errorMessage='Exception occurred while testing scenario-1 for staging table: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 2 Verify pii values of refine table for given pii_hash and column details 
#Test rectification is happening for given pii details. while preparing metadata UUID = 101 pii values is given as pii_Values and 'gdprRectificationUnitTestObject_id' as pii column to rectification function
try:
  testCaseScenario ='Verify pii values of refine table for given pii_hash and column details'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  rectificationHashesUUID = 101
  #Get the hash values after rectification function run 
  afterRectificationRefineTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Refined where UUID = {}".format(rectificationHashesUUID))
  afterRectificationRefineTableDfPiiHash = afterRectificationRefineTableDf.select('PiiHash').collect()[0][0]
  afterRectificationRefineTableDfPiiTraceabilityHash = afterRectificationRefineTableDf.select('PiiTraceabilityHash').collect()[0][0]
  #Check before and after rectification function run 
  if afterRectificationRefineTableDfPiiHash != beforeRectificationRefineTableDfPiiHash and afterRectificationRefineTableDfPiiTraceabilityHash != beforeRectificationRefineTableDfPiiTraceabilityHash :
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log the test result in database
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully tested scenario-2 for refined table')
except Exception as e:
    errorMessage='Exception occurred while testing scenario-2 for refined table: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 3 Verify pii values of person table for given pii_hash and column details
#Test rectification is happening for given pii details. while preparing metadata UUID = 101 pii values is given as pii_Values and 'gdprRectificationUnitTestObject_id' as pii column to rectification function
try:
  testCaseScenario ='Verify pii values of person table for given pii_hash and column details'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  rectificationHashesUUID = 101
  #Get the hash values after rectification function run 
  afterRectificationPersonTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Person where UUID = {}".format(rectificationHashesUUID))
  afterRectificationPersonTableDfPiiHash = afterRectificationPersonTableDf.select('PiiHash').collect()[0][0]
  afterRectificationPersonTableDfPiiTraceabilityHash = afterRectificationPersonTableDf.select('PiiTraceabilityHash').collect()[0][0]
  afterRectificationPersonTableDfPiiColumns = afterRectificationPersonTableDf.select('gdprRectificationUnitTestObject_id').collect()[0][0]
  #Check before and after rectification function run 
  if afterRectificationPersonTableDfPiiHash != beforeRectificationPersonTableDfPiiHash and afterRectificationPersonTableDfPiiTraceabilityHash != beforeRectificationPersonTableDfPiiTraceabilityHash and afterRectificationPersonTableDfPiiColumns == 'Z':
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log the test result in database
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully tested scenario-3 for person table')
except Exception as e:
    errorMessage='Exception occurred while testing scenario-3 for person table:' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 4 Verify pii values of staging table for other records(not in pii_values and columns list)
#Test rectification is happening for given pii details. while preparing metadata UUID = 102 pii values is not given as pii_Values to rectification function
try:
  testCaseScenario ='Verify pii values of staging table for other records(not in pii_values and columns list)'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  nonRectificationHashesUUID = 102
  #For uuid 102 for column 'gdprRectificationUnitTestObject_id' value is 'b'
  valueOfNonPII = 'b'
  #Get the hash values after rectification function run 
  afterRectificationStagingTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Staging where UUID = {}".format(nonRectificationHashesUUID))
  afterRectificationStagingTableDfPiiHash = afterRectificationStagingTableDf.select('PiiHash').collect()[0][0]
  afterRectificationStagingTableDfPiiTraceabilityHash = afterRectificationStagingTableDf.select('PiiTraceabilityHash').collect()[0][0]
  afterRectificationStagingTableDfPiiColumns = afterRectificationStagingTableDf.select('gdprRectificationUnitTestObject_uid').collect()[0][0]
  #Check hash values after rectification function run for other records
  if afterRectificationStagingTableDfPiiColumns == valueOfNonPII:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log the test result in database
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully tested scenario-5 for staging table')
except Exception as e:
    errorMessage='Exception occurred while testing scenario-5 for staging table: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 5 Verify pii values of refine table for other records(not in pii_values and columns list)
#Test rectification is happening for given pii details. while preparing metadata UUID = 102 pii values is not given as pii_Values to rectification function
try:
  testCaseScenario ='Verify pii values of refine table for other records(not in pii_values and columns list)'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  nonRectificationHashesUUID = 102
  #For uuid 102 for column 'gdprRectificationUnitTestObject_id' value is 'b'
  valueOfNonPII = 'b'
  #Get the hash values after rectification function run 
  afterRectificationRefineTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Refined where UUID = {}".format(nonRectificationHashesUUID))
  afterRectificationRefineTableDfPiiHash = afterRectificationRefineTableDf.select('PiiHash').collect()[0][0]
  afterRectificationRefineTableDfPiiTraceabilityHash = afterRectificationRefineTableDf.select('PiiTraceabilityHash').collect()[0][0]
  afterRectificationRefineTableDfPiiColumns = afterRectificationRefineTableDf.select('gdprRectificationUnitTestObject_id').collect()[0][0]
  #Check hash values after rectification function run for other records
  if afterRectificationRefineTableDfPiiColumns == valueOfNonPII:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log the test result in database
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully tested scenario-6 for refined table')
except Exception as e:
    errorMessage='Exception occurred while testing scenario-6 for refined table: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,scenario - 6 Verify pii values of person table for other records(not in pii_values and columns list)
#Test rectification is happening for given pii details. while preparing metadata UUID = 102 pii values is not given as pii_Values to rectification function
try:
  testCaseScenario ='Verify pii values of person table for other records(not in pii_values and columns list)'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  nonRectificationHashesUUID = 102
  #For uuid 102 for column 'gdprRectificationUnitTestObject_id' value is 'b'
  valueOfNonPII = 'b'
  #Get the hash values after rectification function run 
  afterRectificationPersonTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Person where UUID = {}".format(nonRectificationHashesUUID))
  afterRectificationPersonTableDfPiiHash = afterRectificationPersonTableDf.select('PiiHash').collect()[0][0]
  afterRectificationPersonTableDfPiiTraceabilityHash = afterRectificationPersonTableDf.select('PiiTraceabilityHash').collect()[0][0]
  afterRectificationPersonTableDfPiiColumns = afterRectificationPersonTableDf.select('gdprRectificationUnitTestObject_id').collect()[0][0]
  #Check hash values after rectification function run for other records
  if afterRectificationPersonTableDfPiiColumns == valueOfNonPII:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log the test result in database
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully tested scenario-6 for person table')
except Exception as e:
    errorMessage='Exception occurred while testing scenario-6 for person table: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-7 Verify if 2 person tables for staging and rectification records from only one table
#Test redaction is happening for given pii details. while preparing metadata UUID = 103 pii values from person2 table is not given to redaction function
try:
    testCaseScenario ='Verify if 2 person tables for staging and redaction records from only one table'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    nonRedactionHashesUUID = 103
    #Get the hash values after redaction function run 
    afterRectificationPerson2TableDf = spark.sql("select * from gdprRectificationUnitTestObject_Person2 where UUID = {}".format(nonRedactionHashesUUID))
    afterRectificationPerson2TableDfPiiHash = afterRectificationPerson2TableDf.select('PiiHash').collect()[0][0]
    afterRectificationPerson2TableDfPiiTraceabilityHash = afterRectificationPerson2TableDf.select('PiiTraceabilityHash').collect()[0][0]
    #Check hash values after redaction function run for other records
    if afterRectificationPerson2TableDfPiiHash == beforeRectificationPerson2TableDfPiiHash and afterRectificationPerson2TableDfPiiTraceabilityHash == beforeRectificationPerson2TableDfPiiTraceabilityHash and afterRectificationPersonTableDfPiiHash != beforeRectificationPersonTableDfPiiHash and afterRectificationStagingTableDfPiiHash != beforeRectificationStagingTableDfPiiHash:
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    #Log the test result in database
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully tested scenario-7 for person table')
except Exception as e:
    errorMessage='Exception occurred while testing scenario-7 for person table: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-8 Verify if staging and person has different attribute names for mapped columns
#Test rectification is happening for given pii details. while preparing metadata UUID = 101 pii values from person table is  given to rectification function,In staging table and person table column names are mapped to same hash_attribute_name.So both columns has rectificate 
try:
    testCaseScenario ='Verify if staging and person has different attribute names for mapped columns'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    nonRedactionHashesUUID = 101
    #Get the hash values after redaction function run 
    afterRectificationPersonTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Person where UUID = {}".format(nonRedactionHashesUUID))
    afterRectificationPersonTableDfId = afterRectificationPersonTableDf.select('gdprRectificationUnitTestObject_id').collect()[0][0]
    #Get the hash values after redaction function run 
    afterRectificationStagingTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Staging where UUID = {}".format(nonRedactionHashesUUID))
    afterRectificationStgingTableDfUid = afterRectificationStagingTableDf.select('gdprRectificationUnitTestObject_uid').collect()[0][0]
    #Check hash values after redaction function run for other records
    if afterRectificationStgingTableDfUid == afterRectificationPersonTableDfId and afterRectificationPersonTableDfId != beforeRectificationPersonTableDfPiiColumns and afterRectificationStgingTableDfUid != beforeRectificationStagingTableDfPiiColumns:
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    #Log the test result in database
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully tested scenario-8')
except Exception as e:
    errorMessage='Exception occurred while testing scenario-8: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Cleanup Script
try:
  #check the status of current batch only when it is in running status the execute the below scripts
  cleanupMetadata1 = open(testInputPath + "gdprRectificationCleanUp1.sql", "r").read()
  cursor.execute(cleanupMetadata1)
  cleanupMetadata2 = open(testInputPath + "gdprRectificationCleanUp2.sql", "r").read()
  cursor.execute(cleanupMetadata2)
  while cursor.nextset():
    x = 1

except Exception as e:
  errorMessage = "Exception occurred while performing metadata cleanup: " + str(e)
  print(errorMessage)
  assert False 

# COMMAND ----------

# DBTITLE 1,Delete the table if exists
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Staging ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Refined ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Person ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Staging1 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Refined1 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Person1 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Staging2 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Refined2 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Person2 ;

# COMMAND ----------

# DBTITLE 1,Remove test folders
try :
  #clean up the delta table folders for testing
  dbutils.fs.rm("/mnt/dataquality/unit_tests/gdprRectification/tables" , True);
  logTaskProgress(cursor,batchTaskId,'Deleted the delta table folders successfully')
except Exception as e:
  errorMessage = 'Exception occurred while deleting the delta table folders: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Create staging table and the downstream table 
# MAGIC %sql
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Staging (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_uid string,
# MAGIC UUID int,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC PiiHashVersion string,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Staging";
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Refined (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_id string,
# MAGIC UUID string,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC PiiHashVersion int,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Refined";
# MAGIC 
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Person (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_id string,
# MAGIC gdprRectificationUnitTestObject_name string,
# MAGIC UUID string,
# MAGIC SourceID string,
# MAGIC --StagingCreatedTimestamp timestamp,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC PiiHashVersion int,
# MAGIC StagingTableName string,
# MAGIC StagingCreatedBatchId int,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC StagingCreatedTimeStamp timestamp,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Person";
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Person2 (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_id string,
# MAGIC gdprRectificationUnitTestObject_name string,
# MAGIC UUID string,
# MAGIC SourceID string,
# MAGIC --StagingCreatedTimestamp timestamp,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC PiiHashVersion int,
# MAGIC StagingTableName string,
# MAGIC StagingCreatedBatchId int,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC StagingCreatedTimeStamp timestamp,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Person2";

# COMMAND ----------

# DBTITLE 1,Insert data into staging table and the downstream table 
# MAGIC %sql
# MAGIC --Staging
# MAGIC insert into gdprRectificationUnitTestObject_Staging  values(1111,'a',101,'133530402335195561','ForenameInitial6',20200101,10,'1','2020-01-01 10:10:10');
# MAGIC insert into gdprRectificationUnitTestObject_Staging  values(2222,'b',102,'qwqaqlptre','ForenameInitial1',20200101,10,'1','2020-01-01 10:10:10');
# MAGIC 
# MAGIC --Refined
# MAGIC insert into gdprRectificationUnitTestObject_Refined  values(1111,'a',101,'133530402335195561','ForenameInitial6',20200101,10,1,'2020-01-01 10:10:10');
# MAGIC insert into gdprRectificationUnitTestObject_Refined  values(2222,'b',102,'opjlptre','LicenseDate',20200101,10,2,'2020-01-01 10:10:10');
# MAGIC 
# MAGIC --Person
# MAGIC insert into gdprRectificationUnitTestObject_Person  values(1111,'a','xxx',101,842,'133530402335195561','ForenameInitial6',1,'gdprRectificationUnitTestObject_Staging',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10');
# MAGIC insert into gdprRectificationUnitTestObject_Person  values(2222,'b','yyy',102,842,'aqlfdfpt','ForenameInitia',1,'gdprRectificationUnitTestObject_Staging',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10');
# MAGIC 
# MAGIC --Person
# MAGIC insert into gdprRectificationUnitTestObject_Person2 values(1111,'a','zzz',103,842,'fdfptre','nameInitial6',1,'gdprRectificationUnitTestObject_Staging',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10');

# COMMAND ----------

# DBTITLE 1,Data Preparation for unit test to test scenario-9
# Insert the staging table and down stream details in metadatatable 
try:
  #run data preparation scripts 
  dataPrepScenario1 = open(testInputPath + "gdprRectificationDataPrep1.sql", "r").read()
  cursor.execute(dataPrepScenario1)
  while cursor.nextset():
    x = 1
    
except Exception as e:
  errorMessage = "Exception occurred while preparing the data for gdprRectification unit test scenario - 9: " + str(e)
  print(errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Before rectification
#Person
rectificationHashesUUID = 101
beforeRectificationPersonTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Person WHERE UUID = {}".format(rectificationHashesUUID)) 
beforeRectificationPersonTableDfPiiHash = beforeRectificationPersonTableDf.select('PiiHash').collect()[0][0]
beforeRectificationPersonTableDfPiiTraceabilityHash = beforeRectificationPersonTableDf.select('PiiTraceabilityHash').collect()[0][0]
beforeRectificationPersonTableDfPiiColumns = beforeRectificationPersonTableDf.select('gdprRectificationUnitTestObject_name').collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Get all pii columns list
connRectification, cursorRectification, allPiiFieldsDF = getPiiFields()

# COMMAND ----------

# DBTITLE 1,specify personal values as dictionary
personal_values = {
   "gdprRectificationUnitTestObject_name" : "Z"
}

# COMMAND ----------

# DBTITLE 1,Specify pii values to rectification
pii_values = [(beforeRectificationPersonTableDfPiiHash,beforeRectificationPersonTableDfPiiTraceabilityHash, 1)
             ]

# COMMAND ----------

# DBTITLE 1,Execute gdprRectification function
gdprFunctions.gdprRectification(personal_values, allPiiFieldsDF, pii_values, connRectification, cursorRectification)

# COMMAND ----------

# DBTITLE 1,Scenario-9 Verify person table has extra column to rectification compare to staging
#Test rectification is happening for given pii details. while preparing metadata UUID = 101 pii values from person table is  given to rectification function,So extra column 'name' has to rectificate 
try:
    testCaseScenario ='Verify person table has extra column to rectification compare to staging'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    nonRedactionHashesUUID = 101
    #Get the hash values after redaction function run 
    afterRectificationPersonTableDf = spark.sql("select * from gdprRectificationUnitTestObject_Person where UUID = {}".format(nonRedactionHashesUUID))
    afterRectificationPersonTableDfName = afterRectificationPersonTableDf.select('gdprRectificationUnitTestObject_name').collect()[0][0]
    afterRectificationPersonTableDfPiiHash = afterRectificationPersonTableDf.select('PiiHash').collect()[0][0]
    #Check hash values after redaction function run for other records
    if afterRectificationPersonTableDfName == 'Z' and afterRectificationPersonTableDfPiiHash == beforeRectificationPersonTableDfPiiHash:
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    #Log the test result in database
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully tested scenario-9')
except Exception as e:
    errorMessage='Exception occurred while testing scenario-9: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Cleanup script
try:
  #run cleanup script
  cleanupMetadata1 = open(testInputPath + "gdprRectificationCleanUp1.sql", "r").read()
  cursor.execute(cleanupMetadata1)
  cleanupMetadata2 = open(testInputPath + "gdprRectificationCleanUp2.sql", "r").read()
  cursor.execute(cleanupMetadata2)
  while cursor.nextset():
    x = 1

  logTaskProgress(cursor,batchTaskId,'Successfully performed metadata cleanup')
except Exception as e:
  errorMessage = "Exception occurred while performing metadata cleanup: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Delete the table if exists
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Staging ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Refined ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Person ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Staging1 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Refined1 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Person1 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Staging2 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Refined2 ;
# MAGIC DROP TABLE IF EXISTS gdprRectificationUnitTestObject_Person2 ;

# COMMAND ----------

# DBTITLE 1,Remove test folders
try :
  #clean up the delta table folders for testing
  dbutils.fs.rm("/mnt/dataquality/unit_tests/gdprRectification/tables" , True);
  logTaskProgress(cursor,batchTaskId,'Deleted the delta table folders successfully')
except Exception as e:
  errorMessage = 'Exception occurred while deleting the delta table folders: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Data Preparation for unit test to test scenario-10
# Insert the staging table and down stream details in metadatatable 
try:
  #run data preparation scripts 
  dataPrepScenario1 = open(testInputPath + "gdprRectificationDataPrep2.sql", "r").read()
  cursor.execute(dataPrepScenario1)
  while cursor.nextset():
    x = 1
    
except Exception as e:
  errorMessage = "Exception occurred while preparing the data for gdprRectification unit test scenario - 1: " + str(e)
  print(errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Create staging table and the downstream table 
# MAGIC %sql
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Staging1 (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_id string,
# MAGIC gdprRectificationUnitTestObject_name string,
# MAGIC UUID int,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC PiiHashVersion string,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Staging1";
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Refined1 (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_id string,
# MAGIC gdprRectificationUnitTestObject_name string,
# MAGIC UUID string,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC PiiHashVersion int,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Refined1";
# MAGIC 
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Person1 (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_id string,
# MAGIC gdprRectificationUnitTestObject_name string,
# MAGIC UUID string,
# MAGIC SourceID string,
# MAGIC --StagingCreatedTimestamp timestamp,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC PiiHashVersion int,
# MAGIC StagingTableName string,
# MAGIC StagingCreatedBatchId int,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC StagingCreatedTimeStamp timestamp,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Person1";
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Staging2 (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_id string,
# MAGIC UUID int,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC PiiHashVersion string,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Staging2";
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Refined2 (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_id string,
# MAGIC UUID string,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC PiiHashVersion int,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Refined2";
# MAGIC 
# MAGIC create table gdprRectificationUnitTestObject_Person2 (
# MAGIC LastUpdatedBatchID int,
# MAGIC gdprRectificationUnitTestObject_id string,
# MAGIC UUID string,
# MAGIC SourceID string,
# MAGIC --StagingCreatedTimestamp timestamp,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC PiiHashVersion int,
# MAGIC StagingTableName string,
# MAGIC StagingCreatedBatchId int,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC StagingCreatedTimeStamp timestamp,
# MAGIC LastUpdatedTimestamp timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRectification/tables/gdprRectificationUnitTestObject_Person2";

# COMMAND ----------

# DBTITLE 1,Insert data into staging table and the downstream table 
# MAGIC %sql
# MAGIC --Staging
# MAGIC insert into gdprRectificationUnitTestObject_Staging1  values(1111,'a','xxx',101,'aqlfdfptre','12345',20200101,10,'1','2020-01-01 10:10:10');
# MAGIC insert into gdprRectificationUnitTestObject_Staging1  values(2222,'b','yyy',102,'qwqaqlptre','6789',20200101,10,'1','2020-01-01 10:10:10');
# MAGIC 
# MAGIC --Refined
# MAGIC insert into gdprRectificationUnitTestObject_Refined1  values(1111,'a','xxx',101,'aqlfdfptre','12345',20200101,10,1,'2020-01-01 10:10:10');
# MAGIC insert into gdprRectificationUnitTestObject_Refined1  values(2222,'b','yyy',102,'opjlptre','678',20200101,10,2,'2020-01-01 10:10:10');
# MAGIC 
# MAGIC --Person
# MAGIC insert into gdprRectificationUnitTestObject_Person1  values(1111,'a','xxx',101,842,'aqlfdfptre','12345',1,'gdprRectificationUnitTestObject_Staging1',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10');
# MAGIC insert into gdprRectificationUnitTestObject_Person1  values(2222,'b','yyy',102,842,'aqlfdfpt','789',1,'gdprRectificationUnitTestObject_Staging1',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10');
# MAGIC 
# MAGIC --Staging
# MAGIC insert into gdprRectificationUnitTestObject_Staging2  values(1111,'a',103,'zzzaqlfdfptre','9999',20200101,10,'1','2020-01-01 10:10:10');
# MAGIC insert into gdprRectificationUnitTestObject_Staging2  values(2222,'b',104,'zzzqwqaqlptre','888',20200101,10,'1','2020-01-01 10:10:10');
# MAGIC 
# MAGIC --Refined
# MAGIC insert into gdprRectificationUnitTestObject_Refined2  values(1111,'a',103,'zzzaqlfdfptre','9999',20200101,10,1,'2020-01-01 10:10:10');
# MAGIC insert into gdprRectificationUnitTestObject_Refined2  values(2222,'b',104,'zzzopjlptre','777',20200101,10,2,'2020-01-01 10:10:10');
# MAGIC 
# MAGIC --Person
# MAGIC insert into gdprRectificationUnitTestObject_Person2  values(1111,'a',103,842,'zzzaqlfdfptre','9999',1,'gdprRectificationUnitTestObject_Staging2',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10');
# MAGIC insert into gdprRectificationUnitTestObject_Person2  values(2222,'b',104,842,'zzzaqlfdfpt','666',1,'gdprRectificationUnitTestObject_Staging2',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10');

# COMMAND ----------

# DBTITLE 1,Before executing gdprRectification function - table details
#Test rectification is happening for given pii details. while preparing metadata UUID = 101 pii values is given as pii_Values and 'gdprRectificationUnitTestObject_id' from source-1
rectificationHashesUUID = 101
#Get the hash values before rectification function run 
#Staging
beforeRectificationStaging1TableDf = spark.sql("select * from gdprRectificationUnitTestObject_Staging1 WHERE UUID = {}".format(rectificationHashesUUID))
beforeRectificationStaging1TableDfPiiHash = beforeRectificationStaging1TableDf.select('PiiHash').collect()[0][0]
#Person
beforeRectificationPerson1TableDf = spark.sql("select * from gdprRectificationUnitTestObject_Person1 WHERE UUID = {}".format(rectificationHashesUUID)) 
beforeRectificationPerson1TableDfPiiHash = beforeRectificationPerson1TableDf.select('PiiHash').collect()[0][0]

#Test rectification is happening for given pii details. while preparing metadata UUID = 103 pii values is given as pii_Values and 'gdprRectificationUnitTestObject_id' from source-2
rectificationHashesUUID = 103
#Get the hash values before rectification function run 
#Staging
beforeRectificationStaging2TableDf = spark.sql("select * from gdprRectificationUnitTestObject_Staging2 WHERE UUID = {}".format(rectificationHashesUUID))
beforeRectificationStaging2TableDfPiiHash = beforeRectificationStaging2TableDf.select('PiiHash').collect()[0][0]
#Person
beforeRectificationPerson2TableDf = spark.sql("select * from gdprRectificationUnitTestObject_Person2 WHERE UUID = {}".format(rectificationHashesUUID)) 
beforeRectificationPerson2TableDfPiiHash = beforeRectificationPerson2TableDf.select('PiiHash').collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Get all pii columns list
connRectification, cursorRectification, allPiiFieldsDF = getPiiFields()

# COMMAND ----------

# DBTITLE 1,specify personal values as dictionary
personal_values = {
  "gdprRectificationUnitTestObject_id" : "Z",
  "gdprRectificationUnitTestObject_name" : "Z"

}

# COMMAND ----------

# DBTITLE 1,Specify pii values to rectification
pii_values = [('aqlfdfptre','12345', 1),
              ('zzzaqlfdfptre','9999', 1)
             ]

# COMMAND ----------

# DBTITLE 1,Execute gdprRectification function
gdprFunctions.gdprRectification(personal_values, allPiiFieldsDF, pii_values, connRectification, cursorRectification)

# COMMAND ----------

# DBTITLE 1,Scenario-10 Verify Rectification for tables from multiple sources

#Test redaction is happening for given pii details. while preparing metadata UUID = 103 pii values from person2 table is not given to redaction function
try:
    testCaseScenario ='Verify Rectification for tables from multiple sources'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    redactionHashesUUIDSource2 = 103
    #Get the hash values after redaction function run 
    afterRectificationPerson2TableDf = spark.sql("select * from gdprRectificationUnitTestObject_Person2 where UUID = {}".format(redactionHashesUUIDSource2))
    afterRectificationPerson2TableDfPiiHash = afterRectificationPerson2TableDf.select('PiiHash').collect()[0][0]
    afterRectificationPerson2TableDfId = afterRectificationPerson2TableDf.select('gdprRectificationUnitTestObject_id').collect()[0][0]
    #Staging after rectification
    afterRectificationStaging2TableDf = spark.sql("select * from gdprRectificationUnitTestObject_Staging2 where UUID = {}".format(redactionHashesUUIDSource2))
    afterRectificationStaging2TableDfPiiHash = afterRectificationStaging2TableDf.select('PiiHash').collect()[0][0]
    afterRectificationStaging2TableDfId = afterRectificationStaging2TableDf.select('gdprRectificationUnitTestObject_id').collect()[0][0]
    redactionHashesUUIDSource1 = 101
    afterRectificationPerson1TableDf = spark.sql("select * from gdprRectificationUnitTestObject_Person1 where UUID = {}".format(redactionHashesUUIDSource1))
    afterRectificationPerson1TableDfPiiHash = afterRectificationPerson1TableDf.select('PiiHash').collect()[0][0]
    afterRectificationPerson1TableDfId = afterRectificationPerson1TableDf.select('gdprRectificationUnitTestObject_id').collect()[0][0]
    #Staging after rectification
    afterRectificationStaging1TableDf = spark.sql("select * from gdprRectificationUnitTestObject_Staging1 where UUID = {}".format(redactionHashesUUIDSource1))
    afterRectificationStaging1TableDfPiiHash = afterRectificationStaging1TableDf.select('PiiHash').collect()[0][0]
    afterRectificationStaging1TableDfId = afterRectificationStaging1TableDf.select('gdprRectificationUnitTestObject_id').collect()[0][0]
    #Check hash values after redaction function 

    if afterRectificationPerson1TableDfPiiHash != beforeRectificationPerson1TableDfPiiHash and afterRectificationPerson2TableDfPiiHash != beforeRectificationPerson2TableDfPiiHash and afterRectificationStaging1TableDfPiiHash != beforeRectificationStaging1TableDfPiiHash and afterRectificationStaging2TableDfPiiHash != beforeRectificationStaging2TableDfPiiHash and afterRectificationPerson2TableDfId == afterRectificationStaging2TableDfId == afterRectificationPerson1TableDfId == afterRectificationStaging1TableDfId == 'Z':
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    #Log the test result in database
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully tested scenario-10')
except Exception as e:
    errorMessage='Exception occurred while testing scenario-10: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Cleanup script
try:
  #run cleanup script
  cleanupMetadata1 = open(testInputPath + "gdprRectificationCleanUp1.sql", "r").read()
  cursor.execute(cleanupMetadata1)
  cleanupMetadata2 = open(testInputPath + "gdprRectificationCleanUp2.sql", "r").read()
  cursor.execute(cleanupMetadata2)
  while cursor.nextset():
    x = 1

  logTaskProgress(cursor,batchTaskId,'Successfully performed metadata cleanup')
except Exception as e:
  errorMessage = "Exception occurred while performing metadata cleanup: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Close Database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False