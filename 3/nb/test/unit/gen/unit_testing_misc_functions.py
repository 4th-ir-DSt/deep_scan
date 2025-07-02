# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_misc_functions</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test notebook misc_functions</td></tr>
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
# MAGIC   
# MAGIC ## Change Log
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Date</th>
# MAGIC     <th>Changed By</th>
# MAGIC     <th>Change Description</th>
# MAGIC   </tr>
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

# DBTITLE 1,Run Miscellaneous Function notebook
# MAGIC %run ../../../util/spe/misc_functions

# COMMAND ----------

# DBTITLE 1,Import Module
try:
  import os
  import pandas as pd
  import pyodbc
  from datetime import datetime,timedelta
  import pyspark.sql.functions as F
  from pyspark.sql import Window
  from pyspark.sql.functions import asc, col, concat, concat_ws, when, upper, lit, col, collect_list, max
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
except Exception as e:
  errorMessage = "Exception occured while import modules " + str(e)
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
  testObject = 'notebook'
  testObjectName = 'miscellaneousFunctions'  
  requiredInputParameter = 'batchTaskId'
  testCaseScenario = ''
  executionDateTime = datetime.now()
  sampleOutputLocation = 'NA'
  
  #path for datasets used in stored procedure 
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = 'dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  testDirPath = testInputPath.replace('/dbfs','')
except Exception as e:
  errorMessage = "Exception occured while variable declaration " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try:
  #Get batchTaskId from widgets  
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")

  #Get batchId from widgets
  dbutils.widgets.text("batchId","")
  batchId = dbutils.widgets.get("batchId")
 
  #Get sourceId from widgets   
  dbutils.widgets.text("sourceId","")
  sourceId  = dbutils.widgets.get("sourceId")

  #Get adfPipelineName from widgets
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName  = dbutils.widgets.get("adfPipelineName")
  
  #Get notebookName from widgets   
  dbutils.widgets.text("notebookName","")
  notebookName  = dbutils.widgets.get("notebookName")

  #Get clusterId from widgets
  dbutils.widgets.text("clusterId","")
  clusterId  = dbutils.widgets.get("clusterId")
  
  #call the getLoggingPath function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occured while getting parameters and initilising error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish Database Connection
try:
  dbConn = dbutils.secrets.get(scope="lza-da-kv-001-d", key="lza-dp-sqlacct-001-databricks-sql-connection")
  conn,cursor = sqlDbConn(dbConn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = "Exception occured while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Perform cleanup from previous runs 
#run cleanup script
try:
  #Cleanup the previous created directories
  dbutils.fs.rm('/mnt/raw/LoadNaGen2Sqdb',recurse = True)
  #cleanup the previous inserted metadata for createConformedMasterView
  cleanup5 = open("{}miscCreateConformedMasterViewCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup5)
  #Cleanup the previous inserted metadata for main_na_Sqld_dbdt
  cleanup1 = open("{}MiscMainNaSqldDbdtCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup1)
  #Cleanup the previous inserted metadata for main_na_sqld_gen2
  cleanup2 = open("{}MainNaSqldGen2Cleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup2)
  #cleanup the previous inserted metadata for main_na_gen2_sqdb
  cleanup3 = open("{}LoadNaGen2SqdbCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup3)
  #cleanup the previous inserted metadata for dare_na_sqld_dbdt
  cleanup4 = open("{}dareNaSqldDbdtCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup4)
  while cursor.nextset():
    x = 1  
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage = "Exception occured while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Data preparation for main_sqld_dbdt functions
#execute scripts to insert data into tbl_object
try:
  #inserting data into tbl_object
  inputParameter = open('{}MiscMainNaSqldDbdtDataPrep1.sql'.format(testInputPath), "r").read()
  #execute the prep1 script 
  cursor.execute(inputParameter)
  while cursor.nextset():
    x = 1
  
  logTaskProgress(cursor,batchTaskId,'Execution of script to insert records into tbl_object')
except Exception as e:
  errorMessage = "Exception occurred while Executing of test scenario-1 data preparation: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Test functions getActiveDatabricksTablesSpExec and getTableHighAndLowLevelDF
#
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test functions getActiveDatabricksTablesSpExec and getTableHighAndLowLevelDF'
  #Active object while preparing data for tbl_object (data preparation script-1)
  testTableList = ["miscellaneousFunctions_a","miscellaneousFunctions_b","miscellaneousFunctions_c"]
  activeObjects = ["miscellaneousFunctions_a","miscellaneousFunctions_c"]
  #Get active object from the function, to test getActiveLocation function
  funcOutput = getActiveDatabricksTablesSpExec( conn
                                             ,cursor
                                             ,batchTaskId
                                             ,errorMessage
                                             ,adfPipelineName
                                             ,clusterId
                                             ,notebookName
                                             ,errorLogFileLocation)
  tableHighDetailsDf,tableLowDetailsDf = getTableHighAndLowLevelDF( funcOutput
                                                     ,cursor
                                                     ,batchTaskId
                                                     ,errorMessage
                                                     ,adfPipelineName
                                                     ,clusterId
                                                     ,notebookName
                                                     ,errorLogFileLocation)

  #Filter data for inserted ojects while data preparation both active and inActive 
  tableHighDetailsDf = tableHighDetailsDf.selectExpr("*").where(col('object_name').isin(testTableList))
  tableLowDetailsDf = tableLowDetailsDf.selectExpr("*").where(col('object_name').isin(testTableList)) 
  activeObjectFromSp = tableLowDetailsDf.select("object_name").distinct().collect()
  #check the active objects from prepared data and function data
  if len(activeObjects) == len(activeObjectFromSp):
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for functions getActiveDatabricksTablesSpExec and getTableHighAndLowLevelDF')  
except Exception as e:
  errorMessage = "Exception occured while testing functions getActiveDatabricksTablesSpExec and getTableHighAndLowLevelDF : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Drop the table if exist for previous runs
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS miscellaneousFunctions_a;
# MAGIC DROP TABLE IF EXISTS miscellaneousFunctions_c;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(testDirPath+'miscellaneousFunctions_a',recurse=True)
dbutils.fs.rm(testDirPath+'miscellaneousFunctions_c',recurse=True)

# COMMAND ----------

# DBTITLE 1,Create table to test scenario
# MAGIC %sql
# MAGIC create table miscellaneousFunctions_a(
# MAGIC miscellaneousFunctions_name STRING,
# MAGIC miscellaneousFunctions_id INT
# MAGIC )
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/miscellaneousFunctions/input/miscellaneousFunctions_a"

# COMMAND ----------

# DBTITLE 1,Get test active objects 
try:
  testTableList = ["miscellaneousFunctions_a","miscellaneousFunctions_b","miscellaneousFunctions_c"]
  activeObjects = ["miscellaneousFunctions_a","miscellaneousFunctions_c"]
  #Get active object from the function, to test getActiveLocation function
  funcOutput = getActiveDatabricksTablesSpExec( conn
                                             ,cursor
                                             ,batchTaskId
                                             ,errorMessage
                                             ,adfPipelineName
                                             ,clusterId
                                             ,notebookName
                                             ,errorLogFileLocation)
  tableHighDetailsDf,tableLowDetailsDf = getTableHighAndLowLevelDF( funcOutput
                                                     ,cursor
                                                     ,batchTaskId
                                                     ,errorMessage
                                                     ,adfPipelineName
                                                     ,clusterId
                                                     ,notebookName
                                                     ,errorLogFileLocation)

  #Filter data for inserted ojects while data preparation both active and inActive 
  tableHighDetailsDf = tableHighDetailsDf.selectExpr("*").where(col('object_name').isin(testTableList))
  tableLowDetailsDf = tableLowDetailsDf.selectExpr("*").where(col('object_name').isin(testTableList)) 

  logTaskProgress(cursor,batchTaskId,'Get active objects to test scenarios')  
except Exception as e:
  errorMessage = "Exception occured while getting active objects to test scenarios : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test function concatColumnDatatype and tableStructureCheck
#Table manually created with 2 columns in metada gave 6 columns, functions has to add extra four columns to the table 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function concatColumnDatatype and tableStructureCheck' 
  testTable = 'miscellaneousFunctions_a' 
  #Get table columns before function run
  tableCols = spark.sql("show columns from {}".format(testTable)).select('col_name').rdd.flatMap(lambda x: x).collect()
  tableStructureCheck(tableHighDetailsDf
                      ,tableLowDetailsDf,
                        cursor,
                        batchTaskId,
                        errorMessage,
                        adfPipelineName,
                        clusterId,
                        notebookName,
                        errorLogFileLocation)
  #Get table columns after function run
  updatedTableCols = spark.sql("show columns from {}".format(testTable)).select('col_name').rdd.flatMap(lambda x: x).collect()
  if len(updatedTableCols) > len(tableCols):
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed to test functions concatColumnDatatype and tableStructureCheck')  
except Exception as e:
  errorMessage = "Exception occurred while testing functions concatColumnDatatype and tableStructureCheck : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Drop the input Delta table if it exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS miscellaneousFunctions_a;
# MAGIC DROP TABLE IF EXISTS miscellaneousFunctions_c;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(testDirPath+'miscellaneousFunctions_a',recurse=True)
dbutils.fs.rm(testDirPath+'miscellaneousFunctions_c',recurse=True)

# COMMAND ----------

# DBTITLE 1,Create table to test scenario
# MAGIC %sql
# MAGIC create table miscellaneousFunctions_a(
# MAGIC miscellaneousFunctions_xyz STRING,
# MAGIC miscellaneousFunctions_id STRING)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/miscellaneousFunctions/input/miscellaneousFunctions_a"

# COMMAND ----------

# DBTITLE 1,Test function performTableAlterationsAndReload and calculateTableDifferences
#While creating the table first column is 'mainNaSqldDbdt_xyz' in metadata to the particulr table first column is 'mainNaSqldDbdt_name' it has to rename as 'mainNaSqldDbdt_name'
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function performTableAlterationsAndReload and calculateTableDifferences' 
  testTable = 'miscellaneousFunctions_a' 
  #Get columns from the table before function run
  TableCols = spark.sql("show columns from {}".format(testTable)).select('col_name').rdd.flatMap(lambda x: x).collect()
  tableStructureCheck(tableHighDetailsDf
                      ,tableLowDetailsDf,
                        cursor,
                        batchTaskId,
                        errorMessage,
                        adfPipelineName,
                        clusterId,
                        notebookName,
                        errorLogFileLocation)
  #Get columns from the table after function run
  updatedTableCols = spark.sql("show columns from {}".format(testTable)).select('col_name').rdd.flatMap(lambda x: x).collect()
  if 'miscellaneousFunctions_xyz' not in updatedTableCols and 'miscellaneousFunctions_name' in updatedTableCols:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on function performTableAlterationsAndReload and calculateTableDifferences')  
except Exception as e:
  errorMessage = "Exception occurred while testing performTableAlterationsAndReload and calculateTableDifferences : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Drop the input Delta table if it exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS miscellaneousFunctions_a;
# MAGIC DROP TABLE IF EXISTS miscellaneousFunctions_c;
# MAGIC DROP TABLE IF EXISTS dareNaSqldDbdt_a;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(testDirPath+'miscellaneousFunctions_a',recurse=True)
dbutils.fs.rm(testDirPath+'miscellaneousFunctions_c',recurse=True)
dbutils.fs.rm(testDirPath+'dareNaSqldDbdt_a',recurse=True)

# COMMAND ----------

# DBTITLE 1,Data preparation to test main_na_sqld_gen2
#execute scripts to insert data into tbl_object
try:
  #inserting data into tbl_object
  inputParameter = open('{}MainNaSqldGen2DataPrep1.sql'.format(testInputPath), "r").read()
  #execute the prep1 script 
  cursor.execute(inputParameter)
  while cursor.nextset():
    x = 1  
  logTaskProgress(cursor,batchTaskId,'Execution of script to insert records into tbl_object')
except Exception as e:
  errorMessage = "Exception occured while Executing of test scenario-1 data preparation: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Test function getActiveLocation and convertPandasToSparkDfMainNaSqldGen2
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function getActiveLocation and convertPandasToSparkDfMainNaSqldGen2'
  #Active object while preparing data for tbl_object (data preparation script-1)
  ActiveObject = 'mainNaSqldGen2Active'  
  #Get active object from the function, to test getActiveLocation function
  activeLocations = getActiveLocation(conn
                                      ,cursor
                                      ,batchTaskId
                                      ,errorMessage
                                      ,adfPipelineName
                                      ,clusterId
                                      ,notebookName
                                      ,errorLogFileLocation)
  #Converting pandas dataframe to spark dataframe, to test convertPandasToSparkDf function
  activeLocationsDf = convertPandasToSparkDfMainNaSqldGen2(activeLocations
                                                           ,cursor
                                                           ,batchTaskId
                                                           ,errorMessage
                                                           ,adfPipelineName
                                                           ,clusterId
                                                           ,notebookName
                                                           ,errorLogFileLocation)
  #Filter data for inserted ojects while data preparation both active and inActive 
  activeObject = activeLocationsDf.selectExpr("object_name","location").where('object_name="mainNaSqldGen2Active"' or 'object_name="mainNaSqldGen2InActive"')
  activeObjectFromSp = activeObject.collect()[0][0]
  activeLocationFromSp = activeObject.collect()[0][1]
  #check the active objects from prepared data and function data
  if ActiveObject == activeObjectFromSp:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully tested getActiveLocation and convertPandasToSparkDfMainNaSqldGen2')  
except Exception as e:
  errorMessage = "Exception occured while testing getActiveLocation and convertPandasToSparkDfMainNaSqldGen2 : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test function createDirectoryIfNotExists
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function createDirectoryIfNotExists'  
  #Get active object from the function, to test getActiveLocation function
  createDirectoryIfNotExists(activeObject
                               ,cursor
                               ,batchTaskId
                               ,errorMessage
                               ,adfPipelineName
                               ,clusterId
                               ,notebookName
                               ,errorLogFileLocation)
  if bool(os.path.exists('/dbfs' + activeLocationFromSp)) == True:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully tested function createDirectoryIfNotExists')  
except Exception as e:
  errorMessage = "Exception occured while testing function createDirectoryIfNotExists : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Data preparation to test load_na_gen2_dbdt
#Preparing parameters for stored procedure test
try:
  #Create date and time level directories for test scenarios
  #Specify paths for date and time level directories
  mainDirTime = ["/mnt/raw/LoadNaGen2Sqdb/eh_LoadNaGen2Sqdb/input/dateTimeTestFiles"] 
  commonDirTime = ["hasFilesInFirstDateAndFirstTime", "hasFilesInFirstDateAndSecondTime", "hasFilesInSecondDateAndFirstTime", "hasFilesInSecondDateAndSecondTime", "hasNoFiles"] 
  commonDirDateTime = ["date=20200213", "date=20200214"]
  commonDirDateDateTime = ["time=131712", "time=181712"]
  createDateTimeDirectory(mainDirTime,commonDirTime,commonDirDateTime,commonDirDateDateTime)
  #Create date level directories for test scenarios
  #Specify paths for date level directories
  mainDirDate = ["/mnt/raw/LoadNaGen2Sqdb/eh_LoadNaGen2Sqdb/input/dateTestFiles"] 
  commonDirDate = ["hasFilesInFirstDate", "hasFilesInSecondDate", "hasNoFiles"]
  commonDirDateDate = ["date=20200213", "date=20200214"]
  createDateDirectory(mainDirDate,commonDirDate,commonDirDateDate)
  #Copy files in different directories to test different scenarios 
  dataPrepInsert()
  insertData = open('{}LoadNaGen2SqdbDataPrep.sql'.format(testInputPath), "r").read()
  cursor.execute(insertData) #Execute the script
  while cursor.nextset():
    x = 1  
    
  logTaskProgress(cursor,batchTaskId,'Successfully prepared data for test scenario')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Test function getRawEventhubObjectLocations 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test functions getRawEventhubObjectLocations and convertPandasToSparkDfLoadNaGen2Sqdb' 
  #Get active object from the function, to test getActiveLocation function
  activeLocations = getRawEventhubObjectLocations(conn
                                                  ,cursor
                                                  ,batchTaskId
                                                  ,adfPipelineName
                                                  ,clusterId
                                                  ,notebookName
                                                  ,errorLogFileLocation)
  #Converting pandas dataframe to spark dataframe, to test convertPandasToSparkDfLoadNaGen2Sqdb function
  spOutput = convertPandasToSparkDfLoadNaGen2Sqdb(activeLocations
                                                  ,cursor
                                                  ,batchTaskId
                                                  ,adfPipelineName
                                                  ,clusterId
                                                  ,notebookName
                                                  ,errorLogFileLocation)
  #Filter data for inserted ojects while data preparation both active and inActive 
  #Filter to take only testing objects
  testScenariosDf = spOutput.selectExpr('*').where(spOutput['location'].like('/mnt/raw/LoadNaGen2Sqdb/eh_LoadNaGen2Sqdb/input/%'))
  if testScenariosDf.count() > 0:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully tested functions getRawEventhubObjectLocations and convertPandasToSparkDfLoadNaGen2Sqdb')
except Exception as e:
  errorMessage = "Exception occured while testing functions getRawEventhubObjectLocations and convertPandasToSparkDfLoadNaGen2Sqdb : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test function rawObjectLatestDateTime
#Execute function rawObjectLatestDateTime
#If data is in first date it has to return first date, here created 2 dates 20200214,20200213 it has to give 20200214  
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify if file is in first date folder it has to return first date timestamp' 
  rawObjectLatestDateTime(testScenariosDf,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  #Test object while preparing data data for scenario
  testObjectScenario = 'LoadNaGen2Sqdb_dateTestFiles_hasFilesInFirstDate'
  #Get object_id for testObject
  testObjectId = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObjectScenario),conn)).collect()[0][0]
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select output_datetime_to from audit.tbl_object_dates_availability where object_id = '{}'".format(testObjectId),conn)).collect()[0][0]
  if str(dateFromFunction) == '2020-02-14 00:00:00':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-1 notebook load_na_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-1 notebook load_na_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Test function getMaxNonNullTimePath
#Execute function getMaxNonNullTimePath to get file available date and time
#While preparing data file kept in date = '20200214' and time = '181712' folders
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function getMaxNonNullTimePath' 
  partitionScheme = 'date,time'
  testPath = '/mnt/raw/LoadNaGen2Sqdb/eh_LoadNaGen2Sqdb/input/dateTimeTestFiles/hasFilesInFirstDateAndFirstTime'
  foundDataInFolder,date,time = getMaxNonNullTimePath(testPath,partitionScheme)
  if foundDataInFolder is True and date == '20200214' and time == '181712':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed function getMaxNonNullTimePath')
except Exception as e:
  errorMessage = "Exception occurred while executing function getMaxNonNullTimePath" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Test function getMaxNonNullDatePath
#Execute function getMaxNonNullDatePath to get file available date
#While preparing data file kept in date = '20200214'
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function getMaxNonNullDatePath'
  testPath = '/mnt/raw/LoadNaGen2Sqdb/eh_LoadNaGen2Sqdb/input/dateTestFiles/hasFilesInFirstDate'
  partitionScheme = 'date'
  foundDataInFolder,date = getMaxNonNullDatePath(testPath,partitionScheme)
  if foundDataInFolder is True and date == '20200214':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed function getMaxNonNullDatePath')
except Exception as e:
  errorMessage = "Exception occurred while executing function getMaxNonNullDatePath" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Test function getListOfDir
#Execute function to get the list of directories
#In testPath 2 date directories created while executing function it has to give 2 date directories
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function getMaxNonNullDatePath'
  testPath = '/mnt/raw/LoadNaGen2Sqdb/eh_LoadNaGen2Sqdb/input/dateTestFiles/hasFilesInFirstDate'
  partitionScheme = 'date'
  dirList = getListOfDir(testPath,partitionScheme)
  #If length of directories list is 2 then make test case as success
  if len(dirList) == 2:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed function getListOfDir')
except Exception as e:
  errorMessage = "Exception occurred while executing function getListOfDir" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Prepare object name variables for testing dare_na_sql_dbdt
# intialized variables that needs to be passed as widgets to gdpr_na_na_dbdt_gdprredactionkeys notebook 
try:
  objectBaseName = 'dareNaSqldDbdt'
  objectName = objectBaseName + '_a'

except Exception as e:
  errorMessage = 'Exception occurred while getting parameters and initialising error log location ' + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Data Preparation for dare_na_sql_dbdt
try:
  inputParameter = open(testInputPath + "dareNaSqldDbdtDataPrepration.sql", "r").read()
  cursor.execute(inputParameter)
  while cursor.nextset():
    x = 1
  #Log into log file   
  logTaskProgress(cursor,batchTaskId,'Successfully performed metadata cleanup')
except Exception as e:
  errorMessage = "Exception occurred while performing metadata cleanup: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Delete the delta table if exists
# MAGIC %sql
# MAGIC create table dareNaSqldDbdt_a (
# MAGIC Inserted_Date  int,
# MAGIC salesChannel string )
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/miscellaneousFunctions/input/dareNaSqldDbdt_a";
# MAGIC 
# MAGIC insert into dareNaSqldDbdt_a  values(20200228 ,'Direct');
# MAGIC insert into dareNaSqldDbdt_a  values(20200309 ,'Direct');
# MAGIC insert into dareNaSqldDbdt_a  values(20200228 ,'InDirect');
# MAGIC insert into dareNaSqldDbdt_a  values(20200321 ,'InDirect');

# COMMAND ----------

# DBTITLE 1,Test function getObjectRetention
#test the getObjectRetention is executing the usp_get_object_retention store procedure and returing result 
try:
  testCaseScenario ='Verify if getObjectRetention function return proper result'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  
  #Call the getObjectRetention which returns a dataframe 
  retentionDetails = getObjectRetention(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  #Check if the count of rows in each dataframe is greater than zero
  if retentionDetails.shape[0] > 0 :
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log the test result in database
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully Tested getObjectRetention function')
except Exception as e:
    errorMessage='Exception occurred while testing getObjectRetention function: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Test function deleteRetentionData
# transfer the retentionDetails to spark dataframe
try:
  testCaseScenario ='Verify if deleteRetentionData function return proper result'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  
  #Call the convertSinglePandasToSparkDf function to convert panda to spark dataframe
  retentionDeleteDetails = convertSinglePandasToSparkDf(retentionDetails,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  #deleteRetentionData function delete operation the delta table 
  deleteRetentionData(retentionDeleteDetails,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

  #Convert dataframe to dic and get the delete statement in a string 
  deleteStatementDic = retentionDeleteDetails.collect()[0].asDict()
  deleteStatement = deleteStatementDic['delete_statement']
  
#   Replace the delete to Select in the delete statement to check in the delta table 
  selectStatement = deleteStatement.replace("DELETE" , "SELECT *" ,1)
  
#  execute the select statement in the to check if the retention data is removed 
  retentionDeltaTable = sqlContext.sql("{}".format(selectStatement))


 #   Check if the count of rows in  dataframe should be zero
  if retentionDeltaTable.count() == 0 :
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log the test result in database
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully Tested deleteRetentionData function')
  
except Exception as e:
  errorMessage = 'Exception occurred while executing deleteRetentionData:' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Data Preperation to test function createConformedMasterView
try:
  inputParameter = open(testInputPath + "miscCreateConformedMasterViewDataPrep.sql", "r").read()
  cursor.execute(inputParameter)
  while cursor.nextset():
    x = 1
  #Log into log file   
  logTaskProgress(cursor,batchTaskId,'Successfully performed data preop for createConformedMasterView')
except Exception as e:
  errorMessage = "Exception occurred while performing data preop for createConformedMasterView: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Test function createConformedMasterView
#test the createConformedMasterView is creating required dataframe/result
try:
    #Declare variables
    executionOutputStatus = 'Mismatched'
    testCaseStatus = 'failed'
    testCaseScenario = 'Test function createConformedMasterView'
    table_exist = False
    
    #Call the functions and ensure that entry is there in all tables for conformed.masterviewtest2_conformedmasterview1
    spark.sql("""CREATE OR REPLACE TABLE Conformed.TblTest1 ( Id integer,Name string )""")
    spark.sql("""Delete from Conformed.TblTest1""")
    spark.sql("""Insert into Conformed.TblTest1 values(1,'Chitransh'),(2,'Ishan'),(3,'Akhilesh'),(4,'Chris'),(5,'Phil')""")
    spark.sql("""CREATE OR REPLACE TABLE Conformed.TblTest2 ( Id integer,Age integer )""")
    spark.sql("""Delete from Conformed.TblTest2""")
    spark.sql("""Insert into Conformed.TblTest2 values(1,30),(2,16),(3,35),(4,25),(5,33)""")
    spark.sql("""CREATE OR REPLACE TABLE Conformed.TblTest3 ( Id integer,Country string )""")
    spark.sql("""Delete from Conformed.TblTest3""")
    spark.sql("""Insert into Conformed.TblTest3 values(1,'India'),(2,'India'),(3,'UK'),(4,'France'),(5,'Germany')""")
    spark.sql("""CREATE OR REPLACE VIEW Conformed.VwTest3 AS select * from Conformed.TblTest3""")

    lst = 'ConformedMasterTestView1'
     #execute the stored procedure and copy the data to dataframe ---- the procedure joins tbl_task, tbl_object, tbl_task_object_map, tbl_task_type and tbl_object_definition and provides mapping between master conforemd view and underlying objects
    config = pd.read_sql_query("exec config.usp_get_source_to_master_view_details",conn)
    dfConfig = spark.createDataFrame(config[config["tgt_obj_nm"] == "Conformed.ConformedMasterTestView1"])
    logTaskProgress(cursor,batchTaskId,"execution of usp_get_source_to_master_view_details stored procedures completed ")
    
    #start a loop for each task_id and identify the object_id associated with it. Store the create script in "result" variable
    tmp_lstTaskId = dfConfig.select("task_id").distinct().collect()
    for taskId in range(dfConfig.select("task_id").distinct().count()):
        lstTaskId = tmp_lstTaskId[taskId][0]
        #print("task_id:"+str(lstTaskId))
        result = []
        result0 = "create or replace view "+str(dfConfig.where("task_id = '{}'".format(lstTaskId)).collect()[0][6])+" as"
        result.append(result0)
        #start a loop for each object id and create dataframes for source as well as target attributes
        for objectId in range(dfConfig.where("task_id = '{}'".format(lstTaskId)).select("src_obj_id").distinct().count()):
            #print(" object_counter:"+str(objectId))
            lstObjectId = (dfConfig.where("task_id = '{}'".format(lstTaskId)).select("src_obj_id")
                           .distinct().orderBy("src_obj_id").collect()[objectId][0])
            #print(" object_id:"+str(lstObjectId))
            dfSrcCol = (dfConfig.where("src_obj_id = '{}'".format(lstObjectId)).where("task_id = '{}'".format(lstTaskId))
                       .select("src_obj_id","src_obj_nm","src_attb_nm"))
            lstObjectNm = str(dfSrcCol.select("src_obj_nm").distinct().collect()[0][0])
            #print(" object_name:"+str(lstObjectNm))
            dfTgtCol = (dfConfig.where("task_id = '{}'".format(lstTaskId)).select("tgt_attb_nm","tgt_order").distinct())
            dfCol = dfSrcCol.join(dfTgtCol,dfSrcCol.src_attb_nm == dfTgtCol.tgt_attb_nm,"right").orderBy("tgt_order")
            #logic to add source name in column list
            #result1 = str("'"+'{}'.format(lstObjectNm)+"'")+" as source, "
            #print(result1)
            result1 = " select "
            result.append(result1)
            #generate source to target column level mapping for each object
            tmp_col = dfCol.collect()
            for columnId in range(dfCol.count()):
                #print("  column_counter:"+str(objectId))
                if columnId != dfCol.count()-1:
                    result2 = str(tmp_col[columnId][2])+" as "+str(tmp_col[columnId][3])+", "
                else:
                    result2 = str(tmp_col[columnId][2])+" as "+str(tmp_col[columnId][3])+" "
                #print("  column_name:"+result2)
                result.append(result2.replace("None","NULL"))
            result3 = "from "
            result.append(result3+lstObjectNm)
            #add union all clause between each select statement, if there is no more object_id then union all clause is not required
            if objectId != dfConfig.select("src_obj_id").distinct().count()-1:
                result4 = " union all "
                result.append(result4)
        sqlCreateView = str(result).replace("', '","").replace("'","").replace("[","").replace("]","")
        #print(sqlCreateView)
        #execute the create view script
        spark.sql(sqlCreateView)
        logTaskProgress(cursor,batchTaskId,"Master View"+lstObjectNm+"created successfully")
        #print("MasterView "+str(dfTaskObj.collect()[taskId][3])+" created successfully")
    
    cnt = spark.sql("select count(1) from conformed.{}".format(lst))
    if cnt.count() >= 1:
        executionOutputStatus = 'As expected'
        testCaseStatus = 'success'
    
    #function call to insert a row into tbl_unit_test on the test case status  
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)
    logTaskProgress(cursor,batchTaskId,'Successfully executed function createConformedMasterView')

except Exception as e:
    errorMessage = "Exception occurred while executing function createConformedMasterView" + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Cleanup metadata
#run cleanup scripts
try:
  #Cleanup the created directories
  dbutils.fs.rm('/mnt/raw/LoadNaGen2Sqdb',recurse = True)
  #cleanup the previous inserted metadata for createConformedMasterView
  cleanup5 = open("{}miscCreateConformedMasterViewCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup5)
  #cleanup the inserted metadata values main_na_Sqld_dbdt
  cleanup1 = open("{}MiscMainNaSqldDbdtCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup1)
  #cleanup the inserted metadata values for main_na_sqld_gen2
  cleanup2 = open("{}MainNaSqldGen2Cleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup2)
  #cleanup the inserted metadata values for main_na_gen2_sqdb
  cleanup3 = open("{}LoadNaGen2SqdbCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup3)
  #cleanup the inserted metadata values for dare_na_sqld_dbdt
  cleanup4 = open("{}dareNaSqldDbdtCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup4)
  
  while cursor.nextset():
    x = 1  
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage = "Exception occured while executing clean up scripts: " + str(e)
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