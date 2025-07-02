# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_gdpr_na_na_dbdt_gdprretention_redaction</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Unit test for gdpr_na_na_dbdt_gdprretention_redaction</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework/td></tr>
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
# MAGIC     <td>Changed to reflect the change on the notebook of getting the reference tables from the removePii dataframe
# MAGIC       <br>Added LastUpdatedTimestamp and LastUpdatedBatchId
# MAGIC       <br>Changes to the temp deltables for gdpr retention
# MAGIC       <br>Changes to use the JoinKey in the retention redaction
# MAGIC       <br>Changed the gdprretentionredactiontestgdpr_object table structure and added batchId in updateReferenceProcessed function</td>
# MAGIC   </tr>
# MAGIC   
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
  from pyspark.sql.functions import lit,col,broadcast,concat_ws,concat,when,lower
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
  testObjectName = 'gdprRetentionRedaction'  
  requiredInputParameter = 'sourceId, batchId'
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
# MAGIC drop table if exists GdprRetentionRedactionTestgdpr_object;
# MAGIC drop table if exists GdprRetentionRedactionTestgdpr_staging;
# MAGIC drop table if exists GdprRetentionRedactionTestgdpr_person;
# MAGIC drop table if exists temp_supersetTempKeysPIIHA_gdprRetentionTest;
# MAGIC drop table if exists temp_inUsePIIHA_gdprRetentionTest;
# MAGIC drop table if exists temp_toRemovePIIHA_gdprRetentionTest;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(tableInputPath + '/GdprRetentionRedactionTestgdpr_object',recurse=True)
dbutils.fs.rm(tableInputPath + '/GdprRetentionRedactionTestgdpr_staging',recurse=True)
dbutils.fs.rm(tableInputPath + '/GdprRetentionRedactionTestgdpr_person',recurse=True)
dbutils.fs.rm(tableInputPath + '/temp_supersetTempKeysPIIHA_gdprRetentionTest',recurse=True)
dbutils.fs.rm(tableInputPath + '/temp_inUsePIIHA_gdprRetentionTest',recurse=True)
dbutils.fs.rm(tableInputPath + '/temp_toRemovePIIHA_gdprRetentionTest',recurse=True)

# COMMAND ----------

# DBTITLE 1,Metadata cleanup for previous run
#run cleanup script
try:
  cleanup = open("{}GdprRetentionRedactionCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'successfully executed cleanup script for GdprRetentionRedactionCleanup')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for GdprRetentionRedactionCleanup': " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Data preparation for testing
#script that generate inputs for the stored procedure
try:
  #execute the script and get batch_task_id and task_id
  inputParameter = open("{}GdprRetentionRedaction.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>', str(batchId))
  
  inputParameterResults = pd.read_sql_query(inputParameter,conn).astype(str)
  
  #fetch the input parameters
  sourceId = inputParameterResults.at[0,'source_id']

  logTaskProgress(cursor,batchTaskId,'Successful execution of script GdprRetentionRedaction')
except Exception as e:
  errorMessage = "Exception occurred while executing script GdprRetentionRedaction': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Create delta tables
# MAGIC %sql
# MAGIC create table GdprRetentionRedactionTestgdpr_object(
# MAGIC row_id                                int,
# MAGIC GdprRetentionRedactionTestattribute   string,
# MAGIC ExpiryDate                            timestamp,
# MAGIC Processed                             boolean,
# MAGIC RedactedTimestamp                     timestamp,
# MAGIC LastUpdatedBatchID                    int,
# MAGIC LastUpdatedTimestamp                  timestamp,
# MAGIC sourceId                              int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRetentionRedaction/input/GdprRetentionRedactionTestgdpr_object";
# MAGIC -- insert data into table
# MAGIC insert into GdprRetentionRedactionTestgdpr_object  values(1,'a',date_sub(current_timestamp(),200),false,null,999,current_timestamp(),5);
# MAGIC insert into GdprRetentionRedactionTestgdpr_object  values(2,'b',date_sub(current_timestamp(),200),false,null,999,current_timestamp(),5);
# MAGIC insert into GdprRetentionRedactionTestgdpr_object  values(3,'d',date_sub(current_timestamp(),200),false,null,999,current_timestamp(),5);
# MAGIC insert into GdprRetentionRedactionTestgdpr_object  values(4,'g',date_sub(current_timestamp(),200),true,current_timestamp(),999,current_timestamp(),5);
# MAGIC 
# MAGIC create table GdprRetentionRedactionTestgdpr_staging(
# MAGIC row_id                                                  int,
# MAGIC GdprRetentionRedactionTestgdpr_staging_key              string,
# MAGIC GdprRetentionRedactionTestgdpr_staging_pii_attribute    string,
# MAGIC SourceID                                                int,
# MAGIC PiiTraceabilityHash                                     bigint,
# MAGIC PiiHash                                                 bigint,
# MAGIC PiiHashVersion                                          int,
# MAGIC CreatedDate                                             int,
# MAGIC LastUpdatedTimestamp                                    timestamp,
# MAGIC LastUpdatedBatchID                                      int)
# MAGIC using DELTA
# MAGIC PARTITIONED BY (CreatedDate)
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRetentionRedaction/input/GdprRetentionRedactionTestgdpr_staging";
# MAGIC 
# MAGIC create table GdprRetentionRedactionTestgdpr_person(
# MAGIC row_id                                                  int,
# MAGIC GdprRetentionRedactionTestgdpr_person_key               string,
# MAGIC GdprRetentionRedactionTestgdpr_person_pii_attribute     string,
# MAGIC SourceID                                                int, 
# MAGIC StagingTableName                                        string, 
# MAGIC StagingCreatedBatchId                                   int,
# MAGIC StagingCreatedTimestamp                                 timestamp,
# MAGIC PiiTraceabilityHash                                     bigint,
# MAGIC PiiHash                                                 bigint,
# MAGIC PiiHashVersion                                          int,
# MAGIC CreatedDate                                             int,
# MAGIC LastUpdatedTimestamp                                    timestamp,
# MAGIC LastUpdatedBatchID                                      int)
# MAGIC using DELTA
# MAGIC PARTITIONED BY (CreatedDate)
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRetentionRedaction/input/GdprRetentionRedactionTestgdpr_person";
# MAGIC 
# MAGIC CREATE TABLE temp_supersetTempKeysPIIHA_gdprRetentionTest(
# MAGIC PiiTraceabilityHash bigint,
# MAGIC PiiHash bigint,
# MAGIC PiiHashVersion int,
# MAGIC JoinKey string,
# MAGIC ReferenceTableName string,
# MAGIC ReferenceTableAttributeName string,
# MAGIC SourceID int,
# MAGIC StagingTableName string)
# MAGIC USING Delta
# MAGIC LOCATION "/mnt/dataquality/unit_tests/gdprRetentionRedaction/input/temp_supersetTempKeysPIIHA_gdprRetentionTest";
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS temp_inUsePIIHA_gdprRetentionTest(  
# MAGIC JoinKey string,
# MAGIC PiiTraceabilityHash bigint,
# MAGIC ReferenceTableName string,
# MAGIC ReferenceTableAttributeName string,
# MAGIC SourceID int,
# MAGIC StagingTableName string)
# MAGIC USING Delta
# MAGIC LOCATION "/mnt/dataquality/unit_tests/gdprRetentionRedaction/input/temp_inUsePIIHA_gdprRetentionTest";
# MAGIC 
# MAGIC CREATE TABLE temp_toRemovePIIHA_gdprRetentionTest(
# MAGIC PiiTraceabilityHash bigint,
# MAGIC PiiHash bigint,
# MAGIC PiiHashVersion int,
# MAGIC JoinKey string,
# MAGIC ReferenceTableName string,
# MAGIC ReferenceTableAttributeName string,
# MAGIC SourceID int,
# MAGIC StagingTableName string,
# MAGIC InUse boolean)
# MAGIC USING Delta
# MAGIC LOCATION "/mnt/dataquality/unit_tests/gdprRetentionRedaction/input/temp_toRemovePIIHA_gdprRetentionTest";

# COMMAND ----------

# DBTITLE 1,Insert rows into staging, person and temp tables
#insert into staging
spark.sql("insert into GdprRetentionRedactionTestgdpr_staging  values(1,'a','Name1',{},1,1,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
spark.sql("insert into GdprRetentionRedactionTestgdpr_staging  values(2,'b','Name2',{},2,2,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
spark.sql("insert into GdprRetentionRedactionTestgdpr_staging  values(3,'c','Name2',{},2,2,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
spark.sql("insert into GdprRetentionRedactionTestgdpr_staging  values(4,'d','Name3',{},3,3,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
spark.sql("insert into GdprRetentionRedactionTestgdpr_staging  values(5,'e','Name3',{},3,3,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
spark.sql("insert into GdprRetentionRedactionTestgdpr_staging  values(6,'f','Name4',{},4,4,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
spark.sql("insert into GdprRetentionRedactionTestgdpr_staging  values(7,'g','Name5',{},5,5,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
#insert into person
spark.sql("insert into GdprRetentionRedactionTestgdpr_person  values(1,'a','Name1',{},'GdprRetentionRedactionTestgdpr_staging',1,current_timestamp(),1,1,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
spark.sql("insert into GdprRetentionRedactionTestgdpr_person  values(2,'b','Name2',{},'GdprRetentionRedactionTestgdpr_staging',1,current_timestamp(),2,2,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
spark.sql("insert into GdprRetentionRedactionTestgdpr_person  values(3,'c','Name2',{},'GdprRetentionRedactionTestgdpr_staging',1,current_timestamp(),2,2,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
spark.sql("insert into GdprRetentionRedactionTestgdpr_person  values(4,'d','Name3',{},'GdprRetentionRedactionTestgdpr_staging',1,current_timestamp(),3,3,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
spark.sql("insert into GdprRetentionRedactionTestgdpr_person  values(5,'e','Name3',{},'GdprRetentionRedactionTestgdpr_staging',1,current_timestamp(),3,3,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
spark.sql("insert into GdprRetentionRedactionTestgdpr_person  values(6,'f','Name4',{},'GdprRetentionRedactionTestgdpr_staging',1,current_timestamp(),4,4,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
spark.sql("insert into GdprRetentionRedactionTestgdpr_person  values(7,'g','Name5',{},'GdprRetentionRedactionTestgdpr_staging',1,current_timestamp(),5,5,1,cast(date_format(current_timestamp(), 'yyyyMMdd') AS int),'2020-01-01 10:10:10',1)".format(sourceId))
#insert into temp_supersetTempKeysPIIHA_gdprRetentionTest
spark.sql("insert into temp_supersetTempKeysPIIHA_gdprRetentionTest  values(1,1,1,'a','GdprRetentionRedactionTestgdpr_object','GdprRetentionRedactionTestattribute',{},'GdprRetentionRedactionTestgdpr_staging')".format(sourceId))
spark.sql("insert into temp_supersetTempKeysPIIHA_gdprRetentionTest  values(2,2,1,'b','GdprRetentionRedactionTestgdpr_object','GdprRetentionRedactionTestattribute',{},'GdprRetentionRedactionTestgdpr_staging')".format(sourceId))
spark.sql("insert into temp_supersetTempKeysPIIHA_gdprRetentionTest  values(3,3,1,'d','GdprRetentionRedactionTestgdpr_object','GdprRetentionRedactionTestattribute',{},'GdprRetentionRedactionTestgdpr_staging')".format(sourceId))
#insert into temp_inUsePIIHA_gdprRetentionTest
spark.sql("insert into temp_inUsePIIHA_gdprRetentionTest  values('c',2,'GdprRetentionRedactionTestgdpr_object','GdprRetentionRedactionTestattribute',{},'GdprRetentionRedactionTestgdpr_staging')".format(sourceId))
spark.sql("insert into temp_inUsePIIHA_gdprRetentionTest  values('e',3,'GdprRetentionRedactionTestgdpr_object','GdprRetentionRedactionTestattribute',{},'GdprRetentionRedactionTestgdpr_staging')".format(sourceId))

# COMMAND ----------

# DBTITLE 1,Calculate the difference between the piis to redact and the piis in use
#Calculate the piis to remove by making the difference between temp_supersetTempKeysPIIHA and temp_inUsePIIHA
try:
  supersetPiiObjectName='temp_supersetTempKeysPIIHA_gdprRetentionTest'
  activePiiObjectName='temp_inUsePIIHA_gdprRetentionTest' 
  toRemovePiiObjectName='temp_toRemovePIIHA_gdprRetentionTest' 
  
  supersetPiiDf=spark.read.table(supersetPiiObjectName).where("SourceID={}".format(sourceId))
  activePiiDf=spark.read.table(activePiiObjectName).where("sourceID={}".format(sourceId))
  
  #Getting the piis to remove
  removePiiDf=supersetPiiDf.join(activePiiDf,['PiiTraceabilityHash'],how='left').select(supersetPiiDf['*'], activePiiDf['JoinKey']).withColumn('InUse', when(activePiiDf['JoinKey'].isNull(), lit(False)).otherwise(lit(True))).drop(activePiiDf['JoinKey'])
  removePiiDf.distinct().write.format("delta").mode("append").saveAsTable(toRemovePiiObjectName)
  removePiiDf=spark.read.table(toRemovePiiObjectName).where("SourceID={}".format(sourceId))
  
  logTaskProgress(cursor,batchTaskId,'Successfully calculated the piis ro remove')
except Exception as e:
  errorMessage = 'Exception occurred calculating the piis to remove:' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-1 Verify the removePiiDf dataframe
#Test if removePiiDf  contains the expected row for the inserted SourceID, ReferenceTableName and StagingTableName and the PiiTraceabilityHash 1
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify the removePiiDf dataframe' 
  referenceTableName = 'GdprRetentionRedactionTestgdpr_object'
  stagingTableName = 'GdprRetentionRedactionTestgdpr_staging'
  funcOutput = removePiiDf.filter((col('SourceID')==sourceId) & (col('ReferenceTableName')==referenceTableName) & (col('StagingTableName')==stagingTableName) & (col('PiiTraceabilityHash')==1) & (col('InUse')==False))
  if funcOutput.count() == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 1 notebook gdprretention_redaction')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 1 notebook gdprretention_redaction: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Apply redaction
#apply rectification to the piiHashes in the table piiHashesTable for source sourceId
try:
  gdprFunctions.gdprRetentionRedactionHashes(removePiiDf,sourceId,conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Successfully executed gdprRetentionRedactionHashes function')
except Exception as e:
  errorMessage = 'Exception occurred while executing gdprRetentionRedactionHashes:' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-2 Verify the staging and person tables
#Test if the record for the key 'a' was set to 'REDACTED' in the staging table and deleted in the person table
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify the staging and person tables' 
  stagingTableName = 'GdprRetentionRedactionTestgdpr_staging'
  stagingKey = 'GdprRetentionRedactionTestgdpr_staging_key'
  stagingPiiAttribute = 'GdprRetentionRedactionTestgdpr_staging_pii_attribute'
  personTableName = 'GdprRetentionRedactionTestgdpr_person'
  personKey = 'GdprRetentionRedactionTestgdpr_person_key'
  personPiiAttribute = 'GdprRetentionRedactionTestgdpr_person_pii_attribute'
  stagingDf = spark.sql("select * from {} where  {}='REDACTED' and PiiTraceabilityHash='-1' and PiiHash='-1' and PiiHashVersion='-1'".format(stagingTableName, stagingPiiAttribute, stagingPiiAttribute))
  personDf = spark.sql("select * from {} where {}='a'".format(personTableName, personKey))
  if stagingDf.count() == 3 and personDf.count() == 0:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 2 notebook gdprretention_redaction')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 2 notebook gdprretention_redaction: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Update the processed reference tables
#apply rectification to the piiHashes in the table piiHashesTable for source sourceId
try:
  referenceTablesDF = removePiiDf.select("ReferenceTableName","ReferenceTableAttributeName").distinct()
  dataPrepSourceId = 5
  
  gdprFunctions.updateReferenceProcessed(referenceTablesDF,dataPrepSourceId,conn,cursor,batchId,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  logTaskProgress(cursor,batchTaskId,'Successfully updated the processed reference tables')
except Exception as e:
  errorMessage = 'Exception occurred while updating the processed reference tables:' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-3 Verify redaction state table
#Test if the redaction state table was set to processed
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify temp and redaction state tables' 
  redactionSTableName = 'GdprRetentionRedactionTestgdpr_object'
  redactionSKey = 'GdprRetentionRedactionTestattribute'

  redactionSDf = spark.sql("select * from {} where {}='a' and Processed = true".format(redactionSTableName, redactionSKey))
  
  if redactionSDf.count() == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 3 notebook gdprretention_redaction')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 notebook gdprretention_redaction: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Drop tables
# MAGIC %sql
# MAGIC drop table if exists GdprRetentionRedactionTestgdpr_object;
# MAGIC drop table if exists GdprRetentionRedactionTestgdpr_staging;
# MAGIC drop table if exists GdprRetentionRedactionTestgdpr_person;
# MAGIC drop table if exists temp_supersetTempKeysPIIHA_gdprRetentionTest;
# MAGIC drop table if exists temp_inUsePIIHA_gdprRetentionTest;
# MAGIC drop table if exists temp_toRemovePIIHA_gdprRetentionTest;

# COMMAND ----------

# DBTITLE 1,Delete directories
dbutils.fs.rm(tableInputPath + '/GdprRetentionRedactionTestgdpr_object',recurse=True)
dbutils.fs.rm(tableInputPath + '/GdprRetentionRedactionTestgdpr_staging',recurse=True)
dbutils.fs.rm(tableInputPath + '/GdprRetentionRedactionTestgdpr_person',recurse=True)
dbutils.fs.rm(tableInputPath + '/temp_supersetTempKeysPIIHA_gdprRetentionTest',recurse=True)
dbutils.fs.rm(tableInputPath + '/temp_inUsePIIHA_gdprRetentionTest',recurse=True)
dbutils.fs.rm(tableInputPath + '/temp_toRemovePIIHA_gdprRetentionTest',recurse=True)

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}GdprRetentionRedactionCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'successfully executed cleanup script for GdprRetentionRedactionCleanup')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for GdprRetentionRedactionCleanup': " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

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