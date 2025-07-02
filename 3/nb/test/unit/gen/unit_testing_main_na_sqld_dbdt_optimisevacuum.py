# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>Unit test for unit_testing_main_na_sqld_dbdt_optimisevacuum</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Unit test for main_na_sqld_dbdt_optimisevacuum notebook </td></tr>
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
# MAGIC     <td>2019/01/01</td>
# MAGIC     <td>Framework</td>
# MAGIC     <td>Added new test case for empty table
# MAGIC       <br>Added test case for data retention functions </td>
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

# DBTITLE 1,Run Miscellaneous Functions notebook
# MAGIC %run ../../../util/spe/misc_functions

# COMMAND ----------

# DBTITLE 1,Import Module
try:
  import os
  import glob
  from pathlib import Path
  import pandas as pd
  import pyodbc
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
  from datetime import datetime,timedelta
  from time import sleep
  from pyspark.sql.functions import lit,col,explode,concat_ws,struct,collect_list,concat,substring_index,length,locate,max,udf
  from functools import reduce
  from itertools import chain  
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
  testObjectName = 'mainNaSqldDbdtOptimiseVacuum'
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

# DBTITLE 1,Metadata cleanup for previous runs
try:
  #run cleanup script for OptimiseVacuum
  cleanupMetadata = open("{}MainNaSqldDbdtOptimiseVacuumCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanupMetadata)
  #run cleanup script for DataRetention
  cleanup1 = open("{}MainNaSqlDbdtDataRetentionCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup1)
  while cursor.nextset():
    x = 1 
  logTaskProgress(cursor,batchTaskId,'Successfully executed cleanup scripts')
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Drop the delta table if it exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdtOptimiseVacuum_Partial;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdtOptimiseVacuum_Full;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdtOptimiseVacuum_EmptyPartial;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdtDataRetention_object ;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(tableInputPath + '/TestMainNaSqldDbdtOptimiseVacuum_Partial',recurse=True)
dbutils.fs.rm(tableInputPath + '/TestMainNaSqldDbdtOptimiseVacuum_Full',recurse=True)
dbutils.fs.rm(tableInputPath + '/TestMainNaSqldDbdtOptimiseVacuum_EmptyPartial',recurse=True)
dbutils.fs.rm(tableInputPath + '/TestMainNaSqldDbdtDataRetention_object',recurse=True)

# COMMAND ----------

# DBTITLE 1,Create source delta table
# MAGIC %sql
# MAGIC CREATE TABLE TestMainNaSqldDbdtOptimiseVacuum_Partial(
# MAGIC row_id string,
# MAGIC id     string,
# MAGIC Channel string,
# MAGIC CDate string,
# MAGIC CHour String)
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (Channel,CDate,CHour)
# MAGIC TBLPROPERTIES(delta.deletedFileRetentionDuration='interval 2 Seconds',delta.logRetentionDuration='interval 2 Seconds')
# MAGIC LOCATION "/mnt/dataquality/unit_tests/mainNaSqldDbdtOptimiseVacuum/input/TestMainNaSqldDbdtOptimiseVacuum_Partial";
# MAGIC 
# MAGIC CREATE TABLE TestMainNaSqldDbdtOptimiseVacuum_Full(
# MAGIC row_id string,
# MAGIC id     string,
# MAGIC Channel string,
# MAGIC CDate string,
# MAGIC CHour String)
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES(delta.deletedFileRetentionDuration='interval 2 Seconds',delta.logRetentionDuration='interval 2 Seconds')
# MAGIC LOCATION "/mnt/dataquality/unit_tests/mainNaSqldDbdtOptimiseVacuum/input/TestMainNaSqldDbdtOptimiseVacuum_Full";
# MAGIC 
# MAGIC CREATE TABLE TestMainNaSqldDbdtOptimiseVacuum_EmptyPartial(
# MAGIC row_id string,
# MAGIC id     string,
# MAGIC Channel string,
# MAGIC CDate string,
# MAGIC CHour String)
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (Channel,CDate,CHour)
# MAGIC TBLPROPERTIES(delta.deletedFileRetentionDuration='interval 2 Seconds',delta.logRetentionDuration='interval 2 Seconds')
# MAGIC LOCATION "/mnt/dataquality/unit_tests/mainNaSqldDbdtOptimiseVacuum/input/TestMainNaSqldDbdtOptimiseVacuum_EmptyPartial";
# MAGIC 
# MAGIC --Table created for Data retention
# MAGIC CREATE TABLE TestMainNaSqldDbdtDataRetention_object(
# MAGIC TestRowId        string,
# MAGIC TestCreatedDate  string,
# MAGIC TestSalesChannel string)
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/dataquality/unit_tests/mainNaSqldDbdtOptimiseVacuum/input/TestMainNaSqldDbdtDataRetention_object";

# COMMAND ----------

# DBTITLE 1,update the record to get new version of delta table
# MAGIC %sql
# MAGIC -- insert data into TestMainNaSqldDbdtOptimiseVacuum_Partial table
# MAGIC INSERT INTO TestMainNaSqldDbdtOptimiseVacuum_Partial  VALUES('aaa','a','X','20200101','10');
# MAGIC -- insert data into TestMainNaSqldDbdtOptimiseVacuum_Full table
# MAGIC INSERT INTO TestMainNaSqldDbdtOptimiseVacuum_Full  VALUES('aaa','a','X','20200101','10');
# MAGIC 
# MAGIC --To test vacuum function update the existing record 
# MAGIC UPDATE TestMainNaSqldDbdtOptimiseVacuum_Partial SET row_id = 'ccc' WHERE row_id = 'aaa';
# MAGIC UPDATE TestMainNaSqldDbdtOptimiseVacuum_Full SET row_id = 'ccc' WHERE row_id = 'aaa';
# MAGIC 
# MAGIC --To test different version in the table inserting few more records 
# MAGIC INSERT INTO TestMainNaSqldDbdtOptimiseVacuum_Partial VALUES('bbb','b','Y','20200102','11');
# MAGIC INSERT INTO TestMainNaSqldDbdtOptimiseVacuum_Full  VALUES('bbb','b','Y','20200102','11');

# COMMAND ----------

# DBTITLE 1,Insert data into TestMainNaSqldDbdtDataRetention_object
#Test the data retention for different dates
dateMinus60 = (currentTs + timedelta(days=-60)).strftime('%Y%m%d') 
datePlus20  = (currentTs + timedelta(days=+20)).strftime('%Y%m%d')

#Insert into TestMainNaSqldDbdtDataRetention_object for data retention
spark.sql("INSERT INTO TestMainNaSqldDbdtDataRetention_object VALUES('201','{}', 'Direct')".format(dateMinus60))
spark.sql("INSERT INTO TestMainNaSqldDbdtDataRetention_object VALUES('201','{}', 'Direct')".format(datePlus20))

# COMMAND ----------

# DBTITLE 1,Scenario - 1 Data preparation for data retention 
#execute scripts to prepare test data
try:
  inputParameter = open('{}MainNaSqlDbdtDataRetentionDataPrep.sql'.format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>' , str(batchId))
  
  #execute the script 
  inputParameterResults = pd.read_sql_query(inputParameter,conn).iloc[0]
  #get the value for testing 
  newBatchTaskIdDataRetention = int(inputParameterResults[0])
  
  logTaskProgress(cursor,batchTaskId,'Successfully executed the script to generate input parameter')
except Exception as e:
  errorMessage = "Exception occured while executing data preparation script : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Test function getObjectRetention
#test the getObjectRetention is executing the usp_get_object_retention store procedure and returing result 
try:
  testCaseScenario ='Verify if getObjectRetention function return expected result'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  
  #Call the getObjectRetention which returns a dataframe 
  retentionDetails = getObjectRetention(conn
                                      , cursor
                                      , newBatchTaskIdDataRetention
                                      , adfPipelineName
                                      , clusterId
                                      , notebookName
                                      , errorLogFileLocation)
  
  #Check if the count of rows is equal to one 
  if retentionDetails.shape[0] == 1 :
    testCaseStatus = 'success'
    executionOutputStatus = 'As Expected'
    
  #Log the test result in database
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  
  logTaskProgress(cursor,batchTaskId,'Successfully Tested getObjectRetention function')
except Exception as e:
    errorMessage='Exception occurred while testing getObjectRetention function: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 1 Test if the retention data are deleted by deleteRetentionData function call
#delete the retention data using the delete statement retrieved from usp_get_object_retention procedure
try:
  testCaseScenario ='Verify if deleteRetentionData function deletes the retention data'
  testCaseStatus = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  
  #Converting the query result to spark dataframe 
  retentionDeleteDetails = convertSinglePandasToSparkDf(retentionDetails
                                                        ,cursor
                                                        ,batchTaskId
                                                        ,adfPipelineName
                                                        ,clusterId
                                                        ,notebookName
                                                        ,errorLogFileLocation)
 
  
  #calling the function deleteRetentionData to delete the retention data 
  deleteRetentionData(retentionDeleteDetails
                      ,cursor
                      ,batchTaskId
                      ,errorMessage
                      ,adfPipelineName
                      ,clusterId,notebookName
                      ,errorLogFileLocation)
  
  #Fetch the delete statement from the stored procedure result 
  retentionDeleteDetailstDic = retentionDeleteDetails.collect()[0].asDict()
  deleteStatement = retentionDeleteDetailstDic['delete_statement']
  
  #Replace the 'Delete' to 'Select' in the delete statement to check if the data is present in delta table 
  selectStatement = deleteStatement.replace("DELETE" , "SELECT *" ,1)
  
  #execute the select statement to check if the retention data is removed 
  retentionDeltaTable = sqlContext.sql("{}".format(selectStatement))

 #Check the count of rows in the dataframe ,which should be zero
  if retentionDeltaTable.count() == 0 :
    testCaseStatus = 'success'
    executionOutputStatus = 'As Expected'
    
  #Log the test result in database
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
   
  logTaskProgress(cursor,batchTaskId,'Successfully tested deleteRetentionData function')
  
except Exception as e:
    errorMessage = 'Exception occurred while testing deleteRetentionData function: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 
  

# COMMAND ----------

# DBTITLE 1,Scenario - 2,3&4 Data preparation to test Partial optimise and Vacuum
#execute scripts to generate test data 
try:
  #Populate data in different table for testing 
  inputParameter = open('{}MainNaSqldDbdtOptimiseVacuumDataPrep1.sql'.format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>' , str(batchId))
  #Object name replaced as partial
  partialStatus = "'_Partial'"
  inputParameter = inputParameter.replace('<partial_status>' ,  str(partialStatus))

  
  
  #execute the script and get the first row
  inputParameterResults = pd.read_sql_query(inputParameter,conn).iloc[0]

  #get the first value of the row and cast as int
  newBatchTaskId = int(inputParameterResults[0])
  
  logTaskProgress(cursor,batchTaskId,'Successfully executed script to generate input parameter')
except Exception as e:
  errorMessage = "Exception occured while executing of test data preparation for scenario-2,3&4 : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 2 Fetch the versions of the table before vacuum 
#After creating table inserted some data and updated record to get multiple versions.While creating table deletedFileRetentionDuration gave 2 seconds.Before performing Vacuum it will allow to query all versions 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify able to fetch versions before vacuum the delta table' 
  destinationTable = 'TestMainNaSqldDbdtOptimiseVacuum_Partial' 
  
  #Describe the table history to get version level details
  tableVersionsDf = spark.sql("DESCRIBE HISTORY {}".format(destinationTable))
  
  #Get number of versions
  noOfVersions = tableVersionsDf.select([max("version")]).collect()[0][0]
  
  #Get previous version to fetch the data ,the destinationTable table has 4 version (create,insert,update,insert )
  previousVersion = noOfVersions - 2
  prevVersionDf = spark.sql("select * from TestMainNaSqldDbdtOptimiseVacuum_Partial@v{}".format(previousVersion))
  
  #If able to fetch the data before vacuum make test case is pass
  if prevVersionDf.count() > 0 :
    executionOutputStatus = 'As Expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 2 function deltaTableOptimiseVacuum') 
  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 2 function deltaTableOptimiseVacuum : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Create Udf on helper function to get max date and hour details function 
try:
  getEndingUdf = udf(getEnding, StringType())
  logTaskProgress(cursor,batchTaskId,"successfully created udf on helper function getEnding")
except Exception as e:
  errorMessage = "Exception occurred while creating udf on helper function getEnding: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario - 3,4 Call deltaTableOptimiseVacuum function to Optimise and Vacuum the delta table
try:
  #Call deltaTableOptimiseVacuum function to optimise and vacuum the delta tables for given batch_task_id
  deltaTableOptimiseVacuum(cursor
                          ,newBatchTaskId
                          ,errorMessage
                          ,adfPipelineName
                          ,clusterId
                          ,notebookName
                          ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"successfully optimized and vacuumed all the tables")
except Exception as e:
  errorMessage = "Exception occurred calling function deltaTableOptimiseVacuum : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Scenario - 3 Test deltaTableOptimiseVacuum function for Vacuum
# Created delta table(TestMainNaSqldDbdtOptimiseVacuum_Partial) with partition 
# inserted some data and updated record to get multiple versions
# performing Vacuum on the table
# after performing Vacuum it will not allow to query previous version, deltaTableOptimiseVacuum function will create one more version.

try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify after performing vacuum on table able to access the version' 
  destinationTable = 'TestMainNaSqldDbdtOptimiseVacuum_Partial' 
  try:
    tableVersionsDf = spark.sql("DESCRIBE HISTORY {}".format(destinationTable))
    noOfVersions = tableVersionsDf.select([max("version")]).collect()[0][0]
    previousVersion = noOfVersions - 2
    
    #If it fails while trying to query the table then make test case as pass
    spark.sql("select * from TestMainNaSqldDbdtOptimiseVacuum_Partial@v{}".format(previousVersion)).collect()
  except Exception as e:
    executionOutputStatus = 'As Expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 3 function deltaTableVacuum')  
  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 function deltaTableVacuum : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario - 4 Test deltaTableOptimiseVacuum function for optimise(partial)
# Created delta table(TestMainNaSqldDbdtOptimiseVacuum_Partial) with partition 
# Insert few data into the table
# After performing optimize verify tbl_log_optimise value for specific object in tbl_optimise_state if 'completed' then make test case as success

try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = "Test deltaTableOptimiseVacuum function for optimise status in tbl_log_optimise table " 
  testTable = 'TestMainNaSqldDbdtOptimiseVacuum_Partial'
  
  #Get object optimise state 
  optimiseState = spark.createDataFrame(pd.read_sql_query("SELECT optimise_state FROM audit.tbl_log_optimise tlo JOIN  config.tbl_object cto ON tlo.object_id = cto.object_id WHERE object_name = '{}'".format(testTable),conn)).collect()[0][0]
  
  #If state is 'completed' then make testcase as success
  if optimiseState == "completed":
    executionOutputStatus = 'As Expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 4 function deltaTableOptimize')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 4 function deltaTableOptimize : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup 
try:
  #run cleanup script
  cleanupMetadata = open("{}MainNaSqldDbdtOptimiseVacuumCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanupMetadata)
  while cursor.nextset():
    x = 1 
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')
except Exception as e:
  errorMessage = "Exception occurred while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 5 Data preparation to test optimise(Full)
#execute scripts to generate test data 
try:
  #Populate data in different table for testing 
  inputParameter = open('{}MainNaSqldDbdtOptimiseVacuumDataPrep2.sql'.format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>' , str(batchId))
  
  #execute the script and get the first row
  inputParameterResults = pd.read_sql_query(inputParameter,conn).iloc[0]

  #get the first value of the row and cast as int
  newBatchTaskId = int(inputParameterResults[0])
  logTaskProgress(cursor,batchTaskId,'Successfully executed script to generate input parameter')
  
except Exception as e:
  errorMessage = "Exception occured while executing of test scenario-5 data preparation: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 5 Call deltaTableOptimiseVacuum function to Optimise and Vacuum the delta table
try:
  #Call deltaTableOptimiseVacuum function to optimise and vacuum the delta tables for given batch_task_id
  deltaTableOptimiseVacuum(cursor
                          ,newBatchTaskId
                          ,errorMessage
                          ,adfPipelineName
                          ,clusterId
                          ,notebookName
                          ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"successfully optimized and vacuumed all the tables")
except Exception as e:
  errorMessage = "Exception occurred calling function deltaTableOptimiseVacuum : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Scenario - 5 Test deltaTableOptimiseVacuum function for optimise(Full)
# Created delta table(TestMainNaSqldDbdtOptimiseVacuum_Full) without partition 
# inserted some data  into the table
# after performing optimize verify tbl_log_optimise value for specific object in tbl_optimise_state if 'completed' then make test case as success
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = "Verify delta optimise function state for fullorpartial value is 'FULL'" 
  testTable = 'TestMainNaSqldDbdtOptimiseVacuum_Full'
  #Get object optimise state
  optimiseState = spark.createDataFrame(pd.read_sql_query("SELECT optimise_state FROM audit.tbl_log_optimise tlo JOIN  config.tbl_object cto ON tlo.object_id = cto.object_id WHERE object_name = '{}'".format(testTable),conn)).collect()[0][0]
  #If state is 'completed' then make test case as success
  if optimiseState == "completed":
    executionOutputStatus = 'As Expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 5 function deltaTableOptimize')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 5function deltaTableOptimize : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup
try:
  #run cleanup script
  cleanupMetadata = open("{}MainNaSqldDbdtOptimiseVacuumCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanupMetadata)
  while cursor.nextset():
    x = 1 
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')
except Exception as e:
  errorMessage = "Exception occurred while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 6 Data preparation to test optimise on a empty table
#execute scripts to generate test data 
try:
  #Populate data in different table for testing 
  inputParameter = open('{}MainNaSqldDbdtOptimiseVacuumDataPrep1.sql'.format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>' , str(batchId))
  #Object name replaced as EmptyPartial
  partialStatus = "'_EmptyPartial'"
  inputParameter = inputParameter.replace('<partial_status>' , str(partialStatus))
  
  
  #execute the script and get the first row
  inputParameterResults = pd.read_sql_query(inputParameter,conn).iloc[0]

  #get the first value of the row and cast as int
  newBatchTaskId = int(inputParameterResults[0])
  logTaskProgress(cursor,batchTaskId,'Successfully executed script to generate input parameter')
  
except Exception as e:
  errorMessage = "Exception occured while executing of test scenario-6 data preparation: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 6 Call deltaTableOptimiseVacuum function to Optimise and Vacuum the delta table
try:
  #Call deltaTableOptimiseVacuum function to optimise and vacuum the delta tables for given batch_task_id
  deltaTableOptimiseVacuum(cursor
                          ,newBatchTaskId
                          ,errorMessage
                          ,adfPipelineName
                          ,clusterId
                          ,notebookName
                          ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"successfully optimized and vacuumed all the tables")
except Exception as e:
  errorMessage = "Exception occurred calling function deltaTableOptimiseVacuum : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Scenario - 6 Test deltaTableOptimiseVacuum function for optimise(Full) on a empty table
# Created delta table(TestMainNaSqldDbdtOptimiseVacuum_EmptyPartial) without partition 
# do not insert any data into the table
# For partial type if table is empty then it will not make any entry in tbl_log_optimise
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = "Verify delta optimise function state for empty table and fullorpartial value is  'PARTIAL'" 
  testTable = 'TestMainNaSqldDbdtOptimiseVacuum_EmptyPartial'
  #Get object optimise state 
  optimiseStateCount = spark.createDataFrame(pd.read_sql_query("SELECT count(*) FROM audit.tbl_log_optimise tlo JOIN  config.tbl_object cto ON tlo.object_id = cto.object_id WHERE object_name = '{}' and optimise_state = 'completed'".format(testTable),conn)).collect()[0][0]
  #If record count is 0 then make testcase as success
  if optimiseStateCount == 0:
    executionOutputStatus = 'As Expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 6 function deltaTableOptimize')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 6 function deltaTableOptimize : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Drop the delta table if it exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdtOptimiseVacuum_Partial;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdtOptimiseVacuum_Full;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdtOptimiseVacuum_EmptyPartial;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdtDataRetention_object ;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(tableInputPath + '/TestMainNaSqldDbdtOptimiseVacuum_Partial',recurse=True)
dbutils.fs.rm(tableInputPath + '/TestMainNaSqldDbdtOptimiseVacuum_Full',recurse=True)
dbutils.fs.rm(tableInputPath + '/TestMainNaSqldDbdtOptimiseVacuum_EmptyPartial',recurse=True)
dbutils.fs.rm(tableInputPath + '/TestMainNaSqldDbdtDataRetention_object',recurse=True)


# COMMAND ----------

# DBTITLE 1,Metadata Cleanup
try:
  #run cleanup script
  cleanupMetadata = open("{}MainNaSqldDbdtOptimiseVacuumCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanupMetadata)
  
  #run cleanup script for DataRetention
  cleanup1 = open("{}MainNaSqlDbdtDataRetentionCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup1)
  
  while cursor.nextset():
    x = 1 
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')
except Exception as e:
  errorMessage = "Exception occurred while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Close database connection
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