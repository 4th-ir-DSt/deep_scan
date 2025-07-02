# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_gdpr_na_na_dbdt_gdprretention_getpiihashes</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Unit test for gdpr_na_na_dbdt_gdprretention_getpiihashes</td></tr>
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
# MAGIC 
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
# MAGIC     <td>Changed the column ProcessedTimestamp to RedactedTimestamp
# MAGIC       <br>Changes to the table temp_otherPiiHA
# MAGIC       <br>Changes to the temp deltables for gdpr retention
# MAGIC       <br>Removed JoinKey column from temp_otherPIIHA</td>
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

# DBTITLE 1,Run log function notebook
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run standard function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run Miscellaneous Functions notebook
# MAGIC %run ../../../util/spe/misc_functions

# COMMAND ----------

# DBTITLE 1,Run GDPR Functions notebook
# MAGIC %run ../../../util/spe/gdpr_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  from datetime import datetime, timedelta
  from pyspark.sql.functions import lit,col,broadcast
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType

except Exception as e:
  errorMessage="Exception occured while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise variables
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
  batchTaskStatus = ''
  batchTaskSourceRows = 0
  batchTaskRowsLoaded = 0
  batchTaskRejectRows = 0
  batchTaskResult = ''
  batchTaskResultLocation = ''
  batchTaskProgressMessage = '' 
  
  #Parameter for logging into tbl_unit_test_result  
  testObject = 'notebook'
  testObjectName = 'gdprRetentionGetPiiHashes'  
  requiredInputParameter = 'sourceId, batchId'
  sampleOutputLocation = 'NA'
  
  #path for datasets used in stored procedure 
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = 'dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  
  #Paths to create table
  tableInputPath = testInputPath.replace('/dbfs','')
  
except Exception as e:
  errorMessage = "Exception occurred while variable initialisation :" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables and initialise error log location
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
  clusterId  = dbutils.widgets.get("clusterId")
  
  #assigned the source and batch to other variables that are referenced from the metadata
  SourceID=int(sourceId)
  CreatedBatchID=batchId
  LastUpdatedBatchID=batchId
  
  #call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')  
except Exception as e:
  errorMessage = "Exception occurred while getting parameters and initiliasing error log location: " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn=dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")

#call function sqlDbConn to establish Database connection with given scope and key values
try:
  conn,cursor = sqlDbConn(dbconn,
                          batchTaskId,                          
                          adfPipelineName,
                          clusterId,
                          notebookName,
                          errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Successfully Established SQL Connection")
except Exception as e:
  errorMessage = "unable to establish DB connection: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Drop the delta table if it exists
# MAGIC %sql
# MAGIC drop table if exists GdprRetentionPiiHashesTestgdpr_object;
# MAGIC drop table if exists GdprRetentionPiiHashesTestgdpr_target_object;
# MAGIC drop table if exists temp_supersetTempKeysPIIHA_gdprRetentionTest;
# MAGIC drop table if exists temp_inUsePIIHA_gdprRetentionTest;
# MAGIC drop table if exists temp_otherPIIH_gdprRetentionTest;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(tableInputPath + '/GdprRetentionPiiHashesTestgdpr_object',recurse=True)
dbutils.fs.rm(tableInputPath + '/GdprRetentionPiiHashesTestgdpr_target_object',recurse=True)
dbutils.fs.rm(tableInputPath + '/temp_supersetTempKeysPIIHA_gdprRetentionTest',recurse=True)
dbutils.fs.rm(tableInputPath + '/temp_inUsePIIHA_gdprRetentionTest',recurse=True)
dbutils.fs.rm(tableInputPath + '/temp_otherPIIH_gdprRetentionTest',recurse=True)

# COMMAND ----------

# DBTITLE 1,Metadata cleanup for previous run
#run cleanup script
try:
  cleanup = open("{}GdprRetentionGetPiiHashesCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'successfully executed cleanup script for GdprRetentionGetPiiHashesCleanup')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for GdprRetentionGetPiiHashesCleanup': " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Create delta tables
# MAGIC %sql
# MAGIC create table GdprRetentionPiiHashesTestgdpr_target_object(
# MAGIC row_id                                                  int,
# MAGIC GdprRetentionPiiHashesTestgdpr_target_object_key        string,
# MAGIC GdprRetentionPiiHashesTestgdpr_target_object_name       string,
# MAGIC PiiTraceabilityHash                                     bigint,
# MAGIC PiiHash                                                 bigint,
# MAGIC PiiHashVersion                                          int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRetentionGetPiiHashes/input/GdprRetentionPiiHashesTestgdpr_target_object";
# MAGIC 
# MAGIC -- insert data into table
# MAGIC insert into GdprRetentionPiiHashesTestgdpr_target_object  values(1,'a','Name1',1,1,1);
# MAGIC insert into GdprRetentionPiiHashesTestgdpr_target_object  values(2,'b','Name2',2,2,1);
# MAGIC insert into GdprRetentionPiiHashesTestgdpr_target_object  values(3,'c','Name2',2,2,1);
# MAGIC insert into GdprRetentionPiiHashesTestgdpr_target_object  values(4,'d','Name3',3,3,1);
# MAGIC insert into GdprRetentionPiiHashesTestgdpr_target_object  values(5,'e','Name3',3,3,1);
# MAGIC insert into GdprRetentionPiiHashesTestgdpr_target_object  values(6,'f','Name4',4,4,1);
# MAGIC insert into GdprRetentionPiiHashesTestgdpr_target_object  values(7,'g','Name5',5,5,1);
# MAGIC              
# MAGIC create table GdprRetentionPiiHashesTestgdpr_object(
# MAGIC row_id                                int,
# MAGIC GdprRetentionPiiHashesTestattribute   string,
# MAGIC ExpiryDate                            timestamp,
# MAGIC Processed                             boolean,
# MAGIC RedactedTimestamp                     timestamp)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprRetentionGetPiiHashes/input/GdprRetentionPiiHashesTestgdpr_object";
# MAGIC -- insert data into table
# MAGIC insert into GdprRetentionPiiHashesTestgdpr_object  values(1,'a',date_sub(current_timestamp(),200),false,null);
# MAGIC insert into GdprRetentionPiiHashesTestgdpr_object  values(2,'b',date_sub(current_timestamp(),200),false,null);
# MAGIC insert into GdprRetentionPiiHashesTestgdpr_object  values(3,'d',date_sub(current_timestamp(),200),false,null);
# MAGIC insert into GdprRetentionPiiHashesTestgdpr_object  values(4,'g',date_sub(current_timestamp(),200),true,current_timestamp());

# COMMAND ----------

# DBTITLE 1,Data preparation for testing
#script that generate inputs for the stored procedure
try:
  #execute the script and get batch_task_id and task_id
  inputParameter = open("{}GdprRetentionGetPiiHashes.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>', str(batchId))
  
  inputParameterResults = pd.read_sql_query(inputParameter,conn).astype(str)
  
  #fetch the input parameters
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])
  sourceId = inputParameterResults.at[0,'source_id']
  SourceID = int(sourceId)
  
  logTaskProgress(cursor,batchTaskId,'Successful execution of script GdprRetentionGetPiiHashes')
except Exception as e:
  errorMessage = "Exception occurred while executing script GdprRetentionGetPiiHashes': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Get staging object details
try:
  stagingObjectDf=getMaintenanceObjectsSpExec(conn,cursor,newBatchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  stagingObjectName=stagingObjectDf['object_name'][0]

except Exception as e:
  errorMessage = 'Exception occurred while executing the getManitenanceObjectsSp: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Get reference object details
try:
  #run the getGDPRdataRetentionSpExec to get reference object details
  retentionObjectDf=gdprFunctions.getGDPRdataRetentionSpExec(conn,cursor,sourceId,stagingObjectName,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  referenceObjectName=retentionObjectDf['reference_table_name'][0]
  stagingObjectAttributeName=retentionObjectDf['object_attribute_name'][0]
  referenceAttributeName=retentionObjectDf['reference_attribute_name'][0]
except Exception as e:
  errorMessage = 'Exception occurred while executing the getGDPRdataRetentionSp: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Assign static variables for pii columns and where condition
#Assign the static variables and where clause
piiColumnsList=['PiiTraceabilityHash','PiiHash','PiiHashVersion']
whereCondition='Processed=false'
supersetPiiObjectName='temp_supersetTempKeysPIIHA_gdprRetentionTest'
supersetPiiLocation="/mnt/dataquality/unit_tests/gdprRetentionGetPiiHashes/input/temp_supersetTempKeysPIIHA_gdprRetentionTest"
format='Delta'
activePiiObjectLocation="/mnt/dataquality/unit_tests/gdprRetentionGetPiiHashes/input/temp_inUsePIIHA_gdprRetentionTest"
activePiiObjectName='temp_inUsePIIHA_gdprRetentionTest'
otherPiiObjectName='temp_otherPIIH_gdprRetentionTest'

# COMMAND ----------

# DBTITLE 1,Scenario-1 Verify that the temp tables exist 
#Test if the temp tables temp_supersetTempKeysPIIHA and temp_inUsePIIHA exist (this tables won't be used in the unit test)
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify that the temp tables exist' 
  tempTableSuperset = 'temp_supersettempkeyspiiha'
  tempTableInUse = 'temp_inusepiiha'
  #If count of table = 2 then the result is as expected
  funcOutput = spark.sql("show tables")
  funcOutput = funcOutput.filter((col('tableName')==tempTableSuperset) | (col('tableName')==tempTableInUse))
  if funcOutput.count() == 2 :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 1 notebook gdprretention_getpiihashes')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 1 notebook gdprretention_getpiihashes: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

try: 
  #This function will create the temporary static tables required for gdpr redaction
  supersetPIIHAsql='''CREATE TABLE IF NOT EXISTS temp_supersetTempKeysPIIHA_gdprRetentionTest(
                                                                              PiiTraceabilityHash bigint
                                                                             ,PiiHash bigint
                                                                             ,PiiHashVersion int
                                                                             ,JoinKey string
                                                                             ,ReferenceTableName string
                                                                             ,ReferenceTableAttributeName string
                                                                             ,SourceID int
                                                                             ,StagingTableName string)
                      USING Delta
                      LOCATION "/mnt/dataquality/unit_tests/gdprRetentionGetPiiHashes/input/temp_supersetTempKeysPIIHA_gdprRetentionTest"'''
  activePIIHAsql='''CREATE TABLE IF NOT EXISTS temp_inUsePIIHA_gdprRetentionTest(  
                                                                JoinKey string
                                                               ,PiiTraceabilityHash bigint
                                                               ,ReferenceTableName string
                                                               ,ReferenceTableAttributeName string
                                                               ,SourceID int
                                                               ,StagingTableName string)
                      USING Delta
                      LOCATION "/mnt/dataquality/unit_tests/gdprRetentionGetPiiHashes/input/temp_inUsePIIHA_gdprRetentionTest"'''
  otherPIIHAsql='''CREATE TABLE IF NOT EXISTS temp_otherPIIHA(  
                                                                PiiTraceabilityHash bigint
                                                               ,StagingTableName string)
                      USING Delta
                      LOCATION "/mnt/dataquality/unit_tests/gdprRetentionGetPiiHashes/input/temp_otherPIIH_gdprRetentionTest"'''
  spark.sql(activePIIHAsql)
  spark.sql(supersetPIIHAsql)
  spark.sql(otherPIIHAsql)
    
except Exception as e: 
  errorMessage="Exception occurred while createing PII Temp tables: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Create reference and staging dataframes
try:
  #Create staging and reference dataframes
  refDf,stagingDf=gdprFunctions.getReferenceAndStagingDataFrames(conn 
                                               ,cursor
                                               ,referenceObjectName
                                               ,whereCondition
                                               ,referenceAttributeName
                                               ,stagingObjectName
                                               ,stagingObjectAttributeName
                                               ,piiColumnsList
                                               ,errorMessage
                                               ,adfPipelineName
                                               ,clusterId
                                               ,notebookName
                                               ,errorLogFileLocation)  
except Exception as e:
  errorMessage = 'Exception occurred while executing the getReferenceAndStagingDataFrames function: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-2 Verify reference and staging dataframes
#Test if the refDF and stagingDF have the expected number of rows, 3 for refDF and 7 for stagingDF
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify reference and staging dataframes' 
  if refDf.count() == 3 and stagingDf.count() == 7:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 2 notebook gdprretention_getpiihashes')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 2 notebook gdprretention_getpiihashes: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Create and write supersetPii dataframe
try:
  #Create superset pii 
  supersetPiiDf=gdprFunctions.createSupersetPiiDf(conn
                       ,cursor
                       ,refDf   
                       ,stagingDf
                       ,piiColumnsList        
                       ,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

  #Add audit columns
  supersetPiiDf=(supersetPiiDf
                 .withColumn('ReferenceTableName',lit(referenceObjectName))
                 .withColumn('ReferenceTableAttributeName',lit(referenceAttributeName))
                 .withColumn('SourceID',lit(SourceID))
                 .withColumn('StagingTableName',lit(stagingObjectName)))
  #Write to the target
  writeToTarget(supersetPiiDf
                ,supersetPiiLocation
                ,supersetPiiObjectName
                ,format
                ,cursor
                ,batchTaskId
                ,adfPipelineName
                ,clusterId
                ,notebookName
                ,errorLogFileLocation) 
except Exception as e:
  errorMessage = 'Exception occurred while while create and writing  the supersetPii: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-3 Verify the super set table
#Test if temp_supersetTempKeysPIIHA contains the expected rows for the inserted SourceID, ReferenceTableName and StagingTableName and the keys a,b and d
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify the super set table' 
  referenceTableName = 'GdprRetentionPiiHashesTestgdpr_object'
  stagingTableName = 'GdprRetentionPiiHashesTestgdpr_target_object'
  funcOutput = spark.sql("select * from {} where SourceID={} and ReferenceTableName='{}' and StagingTableName='{}'".format(supersetPiiObjectName, sourceId, referenceTableName, stagingTableName))
  if funcOutput.count() == 3 and funcOutput.filter(col('JoinKey')=='a').count() == 1 and funcOutput.filter(col('JoinKey')=='b').count() == 1 and funcOutput.filter(col('JoinKey')=='d').count() == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 3 notebook gdprretention_getpiihashes')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 notebook gdprretention_getpiihashes: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,create and write ActivePii dataframe
try:
  #Create active piihashes thesere are the traceability hasesh from superset and present in different join key eg:policynumber
  activePiiDf=gdprFunctions.createActivePiiDf(conn
                       ,cursor
                       ,refDf  
                       ,stagingDf
                       ,piiColumnsList 
                       ,supersetPiiObjectName
                       ,stagingObjectName
                       ,otherPiiObjectName
                       ,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

  #Add audit columns
  activePiiDf=(activePiiDf
               .withColumn('ReferenceTableName',lit(referenceObjectName))
               .withColumn('ReferenceTableAttributeName',lit(referenceAttributeName))
               .withColumn('SourceID',lit(SourceID))
               .withColumn('StagingTableName',lit(stagingObjectName)))

 #Write to the target
  writeToTarget(activePiiDf
                ,activePiiObjectLocation
                ,activePiiObjectName
                ,format
                ,cursor
                ,batchTaskId
                ,adfPipelineName
                ,clusterId
                ,notebookName
                ,errorLogFileLocation)

except Exception as e:
  errorMessage = 'Exception occurred while create and writing the active pii df: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-4 Verify the active pii table
#Test if temp_supersetTempKeysPIIHA contains the expected rows for the inserted SourceID, ReferenceTableName and StagingTableName and the PiiTraceabilityHashes 2 and 3
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify the active pii table' 
  referenceTableName = 'GdprRetentionPiiHashesTestgdpr_object'
  stagingTableName = 'GdprRetentionPiiHashesTestgdpr_target_object'
  funcOutput = spark.sql("select * from {} where SourceID={} and ReferenceTableName='{}' and StagingTableName='{}'".format(activePiiObjectName, sourceId, referenceTableName, stagingTableName))
  if funcOutput.count() == 2 and funcOutput.filter(col('PiiTraceabilityHash')==2).count() == 1 and funcOutput.filter(col('PiiTraceabilityHash')==3).count() == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 4 notebook gdprretention_getpiihashes')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 4 notebook gdprretention_getpiihashes: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}GdprRetentionGetPiiHashesCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'successfully executed cleanup script for GdprRetentionGetPiiHashesCleanup')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for GdprRetentionGetPiiHashesCleanup': " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Drop the delta table if it exists
# MAGIC %sql
# MAGIC drop table if exists GdprRetentionPiiHashesTestgdpr_object;
# MAGIC drop table if exists GdprRetentionPiiHashesTestgdpr_target_object;
# MAGIC drop table if exists temp_supersetTempKeysPIIHA_gdprRetentionTest;
# MAGIC drop table if exists temp_inUsePIIHA_gdprRetentionTest;
# MAGIC drop table if exists temp_otherPIIH_gdprRetentionTest;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(tableInputPath + '/GdprRetentionPiiHashesTestgdpr_object',recurse=True)
dbutils.fs.rm(tableInputPath + '/GdprRetentionPiiHashesTestgdpr_target_object',recurse=True)
dbutils.fs.rm(tableInputPath + '/temp_supersetTempKeysPIIHA_gdprRetentionTest',recurse=True)
dbutils.fs.rm(tableInputPath + '/temp_inUsePIIHA_gdprRetentionTest',recurse=True)
dbutils.fs.rm(tableInputPath + '/temp_otherPIIH_gdprRetentionTest',recurse=True)

# COMMAND ----------

# DBTITLE 1,Close Database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False