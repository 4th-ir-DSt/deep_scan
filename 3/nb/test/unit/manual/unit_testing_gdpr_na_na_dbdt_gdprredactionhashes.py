# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC #Instructions to run the unit testing notebook
# MAGIC 
# MAGIC This notebook is responsible for redaction hash values from a specific table and downstream tables containing that PII
# MAGIC <ul>
# MAGIC <li>Run all the notebook steps sequentially</li>

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_gdpr_na_na_dbdt_gdprredactionhashes</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td> unit testing of gdpr_na_na_dbdt_gdprredactionhashes notebook</td></tr>
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
# MAGIC     <td>Added new test case Scenario-8
# MAGIC       <br>Changes to set the redacted PiiHashes and PiitraceabilityHashes to -1 and to add the LastUpdatedTimestamp and LastUpdatedBatchId</td>
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
  import sys
  import pyodbc
  import collections
  from datetime import datetime,timedelta
  from pyspark.sql.functions import lit,col,concat_ws,concat
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
  from pyspark.sql.functions import broadcast

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
  batchTaskStatus = 'completed'
  
  #Parameter and paths
  scheduleReference = 'GDPR_Redaction'
  testObject = 'Notebook'
  testObjectName = 'gdprRedactionHashes'
  outputLocation = ''
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  requiredInputParameter = 'batchTaskId,errorMessage,adfPipelineName,clusterID,notebookName,keyColumnName,keyTableName,notebookName,stagingTableName'
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
  cleanupMetadata = open(testInputPath + "gdprRedactionHashesCleanUp1.sql", "r").read()
  cursor.execute(cleanupMetadata)
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
# MAGIC DROP TABLE IF EXISTS gdprRedactionHashesUnitTestObject_Staging ;
# MAGIC DROP TABLE IF EXISTS gdprRedactionHashesUnitTestObject_Refined ;
# MAGIC DROP TABLE IF EXISTS gdprRedactionHashesUnitTestObject_Person ;
# MAGIC DROP TABLE IF EXISTS gdprRedactionHashesUnitTestObject_Person2 ;

# COMMAND ----------

# DBTITLE 1,Remove the folders if it exists
try :
  #clean up the delta table folders for testing
  dbutils.fs.rm("/mnt/dataquality/unit_tests/gdprRedactionHashes/tables" , True);
except Exception as e:
  errorMessage = 'Exception occurred while deleting the delta table folders: ' + str(e)
  print(errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Create staging table and the downstream table 
# MAGIC %sql
# MAGIC 
# MAGIC create table gdprRedactionHashesUnitTestObject_Staging (
# MAGIC row_id string,
# MAGIC UUID int,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC PiiHashVersion string,
# MAGIC LastUpdatedTimestamp timestamp,
# MAGIC LastUpdatedBatchID int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRedactionHashes/tables/gdprRedactionHashesUnitTestObject_Staging";
# MAGIC 
# MAGIC create table gdprRedactionHashesUnitTestObject_Refined (
# MAGIC row_id string,
# MAGIC UUID string,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC CreatedDate int,
# MAGIC CreatedHour int,
# MAGIC PiiHashVersion int,
# MAGIC LastUpdatedTimestamp timestamp,
# MAGIC LastUpdatedBatchID int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRedactionHashes/tables/gdprRedactionHashesUnitTestObject_Refined";
# MAGIC 
# MAGIC 
# MAGIC create table gdprRedactionHashesUnitTestObject_Person (
# MAGIC row_id string,
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
# MAGIC LastUpdatedTimestamp timestamp,
# MAGIC LastUpdatedBatchID int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRedactionHashes/tables/gdprRedactionHashesUnitTestObject_Person";
# MAGIC 
# MAGIC create table gdprRedactionHashesUnitTestObject_Person2 (
# MAGIC row_id string,
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
# MAGIC LastUpdatedTimestamp timestamp,
# MAGIC LastUpdatedBatchID int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRedactionHashes/tables/gdprRedactionHashesUnitTestObject_Person2";

# COMMAND ----------

# DBTITLE 1,Data Preparation for unit test
# Insert the staging table and down stream details in metadatatable 
try:
  #run data preparation scripts 
  dataPrepScenario1 = open(testInputPath + "gdprRedactionHashesDataPrep1.sql", "r").read()
  cursor.execute(dataPrepScenario1)
  while cursor.nextset():
    x = 1
    
except Exception as e:
  errorMessage = "Exception occurred while preparing the data for gdprReducionHashes unit test " + str(e)
  print(errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Insert data into staging table and the downstream table 
# MAGIC %sql
# MAGIC --Staging
# MAGIC insert into gdprRedactionHashesUnitTestObject_Staging  values('a',101,'aqlfdfptre','ForenameInitial6',20200101,10,'1','2020-01-01 10:10:10',1);
# MAGIC insert into gdprRedactionHashesUnitTestObject_Staging  values('a',102,'qwqaqlptre','ForenameInitial1',20200101,10,'1','2020-01-01 10:10:10',1);
# MAGIC 
# MAGIC --Refined
# MAGIC insert into gdprRedactionHashesUnitTestObject_Refined  values('a',101,'aqlfdfptre','ForenameInitial6',20200101,10,1,'2020-01-01 10:10:10',1);
# MAGIC insert into gdprRedactionHashesUnitTestObject_Refined  values('a',102,'opjlptre','LicenseDate',20200101,10,2,'2020-01-01 10:10:10',1);
# MAGIC 
# MAGIC --Person
# MAGIC insert into gdprRedactionHashesUnitTestObject_Person  values('a',101,842,'aqlfdfptre','ForenameInitial6',1,'gdprRedactionHashesUnitTestObject_Staging',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10',1);
# MAGIC insert into gdprRedactionHashesUnitTestObject_Person  values('a',102,842,'aqlfdfpt','ForenameInitia',1,'gdprRedactionHashesUnitTestObject_Staging',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10',1);
# MAGIC 
# MAGIC --Person2
# MAGIC insert into gdprRedactionHashesUnitTestObject_Person2  values('b',103,842,'dfptre','nameInitial6',1,'gdprRedactionHashesUnitTestObject_Staging',180,20200101,10,'2020-01-01 10:10:10','2020-01-01 10:10:10',1);

# COMMAND ----------

# DBTITLE 1,Before executing gdprRedactionHashes function - table details
#Test redaction is happening for given pii details. while preparing metadata UUID = 101 pii values is given as pii_Values to redaction function
redactionHashesUUID = 101
#Get the hash values before redaction function run 
#Staging
beforeRedactionStagingTableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Staging WHERE UUID = {}".format(redactionHashesUUID))
beforeRedactionStagingTableDfPiiHash = beforeRedactionStagingTableDf.select('PiiHash').collect()[0][0]
beforeRedactionStagingTableDfPiiTraceabilityHash = beforeRedactionStagingTableDf.select('PiiTraceabilityHash').collect()[0][0]
beforeRedactionStagingTableDfPiiColumns = beforeRedactionStagingTableDf.select('row_id').collect()[0][0]
#Refined
beforeRedactionRefineTableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Refined WHERE UUID = {}".format(redactionHashesUUID))
beforeRedactionRefineTableDfPiiHash = beforeRedactionRefineTableDf.select('PiiHash').collect()[0][0]
beforeRedactionRefineTableDfPiiTraceabilityHash = beforeRedactionRefineTableDf.select('PiiTraceabilityHash').collect()[0][0]
#Person
beforeRedactionPersonTableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Person WHERE UUID = {}".format(redactionHashesUUID)) 
beforeRedactionPersonTableDfCount = beforeRedactionPersonTableDf.count()
#Person2
nonRedactionUUIDForPerson2 = 103
beforeRedactionPerson2TableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Person2 WHERE UUID = {}".format(nonRedactionUUIDForPerson2))
beforeRedactionPerson2TableDfPiiHash = beforeRedactionPerson2TableDf.select('PiiHash').collect()[0][0]
beforeRedactionPerson2TableDfPiiTraceabilityHash = beforeRedactionPerson2TableDf.select('PiiTraceabilityHash').collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Specify redaction record pii_values
pii_values = [('aqlfdfptre','ForenameInitial6', 1)
             ]

# COMMAND ----------

# DBTITLE 1,Execute the gdprRedactionHashes
#Run function to redact hashes for the given pii_values
gdprFunctions.gdprRedactionHashes(pii_values)

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

# DBTITLE 1,scenario - 1 Verify hash values of staging table for given pii_hash details
#Test redaction is happening for given pii details. while preparing metadata UUID = 101 pii values is given as pii_Values to redaction function
try:
    testCaseScenario ='Verify values of staging table for given pii details'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    redactionHashesUUID = 101
    #Get the hash values after redaction function run 
    afterRedactionStagingTableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Staging where UUID = {}".format(redactionHashesUUID))
    afterRedactionStagingTableDfPiiHash = afterRedactionStagingTableDf.select('PiiHash').collect()[0][0]
    afterRedactionStagingTableDfPiiTraceabilityHash = afterRedactionStagingTableDf.select('PiiTraceabilityHash').collect()[0][0]
    #Check before and after redaction function run 
    if afterRedactionStagingTableDfPiiHash == '-1' and afterRedactionStagingTableDfPiiTraceabilityHash == '-1' and beforeRedactionStagingTableDfPiiHash != '-1' and beforeRedactionStagingTableDfPiiTraceabilityHash != '-1' :
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

# DBTITLE 1,Scenario - 2 Verify hash values of refine table for given pii_hash details 
#Test redaction is happening for given pii details. while preparing metadata UUID = 101 pii values is given as pii_Values to redaction function
try:
    testCaseScenario ='Verify values of refine  table for given pii details'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    redactionHashesUUID = 101
    #Get the hash values after redaction function run 
    afterRedactionRefineTableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Refined where UUID = {}".format(redactionHashesUUID))
    afterRedactionRefineTableDfPiiHash = afterRedactionRefineTableDf.select('PiiHash').collect()[0][0]
    afterRedactionRefineTableDfPiiTraceabilityHash = afterRedactionRefineTableDf.select('PiiTraceabilityHash').collect()[0][0]
    #Check before and after redaction function run 
    if afterRedactionRefineTableDfPiiHash == '-1' and afterRedactionRefineTableDfPiiTraceabilityHash == '-1' and beforeRedactionRefineTableDfPiiHash != '-1' and beforeRedactionRefineTableDfPiiTraceabilityHash != '-1' :
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

# DBTITLE 1,Scenario - 3 Verify hash values of person table for given pii_hash details
#Test redaction is happening for given pii details. while preparing metadata UUID = 101 pii values is given as pii_Values to redaction function
try:
    testCaseScenario ='Verify values of person table for given pii details'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    redactionHashesUUID = 101
    #Get the hash values after redaction function run 
    afterRedactionPersonTableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Person where UUID = {}".format(redactionHashesUUID))
    afterRedactionPersonTableDfCount = afterRedactionPersonTableDf.count()
    #Check before and after redaction function run 
    if beforeRedactionPersonTableDfCount - 1 == afterRedactionPersonTableDfCount:
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

# DBTITLE 1,Scenario - 4 Verify extra pii columns of staging table for given pii_hash details
#Test redaction is happening for given pii details. while preparing metadata UUID = 101 pii values is given as pii_Values to redaction function
try:
    testCaseScenario ='Verify extra pii columns of staging table for given pii_hash details'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    redactionHashesUUID = 101
    #Get the hash values after redaction function run 
    afterRedactionStagingTableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Staging where UUID = {}".format(redactionHashesUUID))
    afterRedactionStagingTableDfPiiColumns = afterRedactionStagingTableDf.select('row_id').collect()[0][0]
    #Check before and after redaction function run 
    if afterRedactionStagingTableDfPiiColumns == 'REDACTED' and beforeRedactionStagingTableDfPiiColumns != 'REDACTED':
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    #Log the test result in database
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully tested scenario-4 for staging table')
except Exception as e:
    errorMessage='Exception occurred while testing scenario-4 for staging table: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 5 Verify hash values of staging table for other records(not in pii_values list)
#Test redaction is happening for given pii details. while preparing metadata UUID = 102 pii values is not given to redaction function
try:
    testCaseScenario ='Verify hash values of staging table for other records(not in pii_values list)'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    nonRedactionHashesUUID = 102
    #Get the hash values after redaction function run 
    afterRedactionStagingTableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Staging where UUID = {}".format(nonRedactionHashesUUID))
    afterRedactionStagingTableDfPiiHash = afterRedactionStagingTableDf.select('PiiHash').collect()[0][0]
    afterRedactionStagingTableDfPiiTraceabilityHash = afterRedactionStagingTableDf.select('PiiTraceabilityHash').collect()[0][0]
    #Check hash values after redaction function run for other records
    if afterRedactionStagingTableDfPiiHash != 'REDACTED' and afterRedactionStagingTableDfPiiTraceabilityHash != 'REDACTED':
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

# DBTITLE 1,Scenario - 6 Verify hash values of refine table for other records(not in pii_values list) 
#Test redaction is happening for given pii details. while preparing metadata UUID = 102 pii values is not given to redaction function
try:
    testCaseScenario ='Verify hash values of refine table for other records(not in pii_values list)'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    nonRedactionHashesUUID = 102
    #Get the hash values after redaction function run 
    afterRedactionRefineTableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Refined where UUID = {}".format(nonRedactionHashesUUID))
    afterRedactionRefineTableDfPiiHash = afterRedactionRefineTableDf.select('PiiHash').collect()[0][0]
    afterRedactionRefineTableDfPiiTraceabilityHash = afterRedactionRefineTableDf.select('PiiTraceabilityHash').collect()[0][0]
    #Check hash values after redaction function run for other records
    if afterRedactionRefineTableDfPiiHash != 'REDACTED' and afterRedactionRefineTableDfPiiTraceabilityHash != 'REDACTED':
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

# DBTITLE 1,scenario - 7 Verify hash values of person table for other records(not in pii_values list)
#Test redaction is happening for given pii details. while preparing metadata UUID = 102 pii values is not given to redaction function
try:
    testCaseScenario ='Verify hash values of person table for other records(not in pii_values list)'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    nonRedactionHashesUUID = 102
    #Get the hash values after redaction function run 
    afterRedactionPersonTableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Person where UUID = {}".format(nonRedactionHashesUUID))
    afterRedactionPersonTableDfPiiHash = afterRedactionPersonTableDf.select('PiiHash').collect()[0][0]
    afterRedactionPersonTableDfPiiTraceabilityHash = afterRedactionPersonTableDf.select('PiiTraceabilityHash').collect()[0][0]
    #Check hash values after redaction function run for other records
    if afterRedactionPersonTableDfPiiHash != 'REDACTED' and afterRedactionPersonTableDfPiiTraceabilityHash != 'REDACTED':
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

# DBTITLE 1,Scenario - 8 Verify if 2 person tables for staging and redaction records from only one table
#Test redaction is happening for given pii details. while preparing metadata UUID = 103 pii values from person2 table is not given to redaction function
try:
    testCaseScenario ='Verify if 2 person tables for staging and redaction records from only one table'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    redactionHashesUUID = 101
    #Get the hash values after redaction function run 
    afterRedactionPersonTableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Person where UUID = {}".format(redactionHashesUUID))
    afterRedactionPersonTableDfCount = afterRedactionPersonTableDf.count()
    afterRedactionStagingTableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Staging where UUID = {}".format(redactionHashesUUID))
    afterRedactionStagingTableDfPiiHash = afterRedactionStagingTableDf.select('PiiHash').collect()[0][0]    
    nonRedactionHashesUUID = 103
    afterRedactionPerson2TableDf = spark.sql("select * from gdprRedactionHashesUnitTestObject_Person2 where UUID = {}".format(nonRedactionHashesUUID))
    #Get the hash values after redaction function run
    afterRedactionPerson2TableDfPiiHash = afterRedactionPerson2TableDf.select('PiiHash').collect()[0][0]
    afterRedactionPerson2TableDfPiiTraceabilityHash = afterRedactionPerson2TableDf.select('PiiTraceabilityHash').collect()[0][0]
    #Check hash values after redaction function run for other records
    if afterRedactionPerson2TableDfPiiHash == beforeRedactionPerson2TableDfPiiHash and afterRedactionPerson2TableDfPiiTraceabilityHash == beforeRedactionPerson2TableDfPiiTraceabilityHash and beforeRedactionPersonTableDfCount - 1 == afterRedactionPersonTableDfCount and afterRedactionStagingTableDfPiiHash != beforeRedactionStagingTableDfPiiHash :
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    #Log the test result in database
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully tested scenario-8 for person table')
except Exception as e:
    errorMessage='Exception occurred while testing scenario-8 for person table: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Cleanup script
try:
    #run cleanup script
    cleanupMetadata = open(testInputPath + "gdprRedactionHashesCleanUp1.sql", "r").read()
    cursor.execute(cleanupMetadata)
    while cursor.nextset():
      x = 1

    logTaskProgress(cursor,batchTaskId,'Successfully performed metadata cleanup')
except Exception as e:
  errorMessage = "Exception occurred while performing metadata cleanup: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Remove test folders
try :
    #clean up the delta table folders for testing
    dbutils.fs.rm("/mnt/dataquality/unit_tests/gdprRedactionHashes/tables" , True);
    logTaskProgress(cursor,batchTaskId,'Deleted the delta table folders successfully')
except Exception as e:
  errorMessage = 'Exception occurred while deleting the delta table folders: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Drop the delta table created for testing
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS gdprRedactionHashesUnitTestObject_Staging ;
# MAGIC DROP TABLE IF EXISTS gdprRedactionHashesUnitTestObject_Refined ;
# MAGIC DROP TABLE IF EXISTS gdprRedactionHashesUnitTestObject_Person ;
# MAGIC DROP TABLE IF EXISTS gdprRedactionHashesUnitTestObject_Person2 ;

# COMMAND ----------

# DBTITLE 1,Close Database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False