# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_main_na_sqld_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test notebook main_na_sqld_dbdt</td></tr>
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
# MAGIC     <tr>
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
  from pyspark.sql import Window
  import pyspark.sql.functions as F
  from datetime import datetime,timedelta
  from pyspark.sql.types import StructType,StructField,StringType
  from pyspark.sql.functions import asc, col, concat, concat_ws, when, upper, lit, col, collect_list, max
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
  testObject = 'notebook'
  testObjectName = 'mainNaSqldDbdt'  
  requiredInputParameter = ''
  testCaseScenario = ''
  executionDateTime = datetime.now()
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
  errorMessage = "Exception occurred while getting parameters and initilising error log location " + str(e)
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

# DBTITLE 1,Perform cleanup from previous runs
#run cleanup script
try:
  cleanup = open("{}MainNaSqldDbdtCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage = "Exception occurred while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario 1-Data preparation 
#execute scripts to insert data into tbl_object
try:
  #inserting data into tbl_object
  inputParameter = open('{}MainNaSqldDbdtDataPrep1.sql'.format(testInputPath), "r").read()
  #execute the prep1 script 
  print(inputParameter)
  cursor.execute(inputParameter)
  while cursor.nextset():
    x = 1
  
  logTaskProgress(cursor,batchTaskId,'Execution of script to insert records into tbl_object')
except Exception as e:
  errorMessage = "Exception occurred while Executing of test scenario-1 data preparation: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-1 verify function has to take active objects only
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check function has to take active objects only'
  #Active object while preparing data for tbl_object (data preparation script-1)
  testTableList = ["mainNaSqldDbdt_a","mainNaSqldDbdt_b","mainNaSqldDbdt_c"]
  activeObjects = ["mainNaSqldDbdt_a","mainNaSqldDbdt_c"]
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
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 1 notebook getActiveDatabricksTablesSpExec')  
except Exception as e:
  errorMessage = "Exception occured while testing scenario 1 notebook getActiveDatabricksTablesSpExec : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

display(tableHighDetailsDf)

# COMMAND ----------

# DBTITLE 1,Drop the table if exist for previous runs
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS mainNaSqldDbdt_a;
# MAGIC DROP TABLE IF EXISTS mainNaSqldDbdt_c;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm('/mnt/dataquality/unit_tests/mainNaSqldDbdt/input/mainNaSqldDbdt_a',recurse=True)
dbutils.fs.rm('/mnt/dataquality/unit_tests/mainNaSqldDbdt/input/mainNaSqldDbdt_c',recurse=True)

# COMMAND ----------

# DBTITLE 1,Create table to test scenario-2 
# MAGIC %sql
# MAGIC create table mainNaSqldDbdt_a(
# MAGIC mainNaSqldDbdt_name STRING,
# MAGIC mainNaSqldDbdt_id INT
# MAGIC )
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/mainNaSqldDbdt/input/mainNaSqldDbdt_a"

# COMMAND ----------

# DBTITLE 1,Get active objects
try:
  testTableList = ["mainNaSqldDbdt_a","mainNaSqldDbdt_b","mainNaSqldDbdt_c"]
  activeObjects = ["mainNaSqldDbdt_a","mainNaSqldDbdt_c"]
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

# DBTITLE 1,Scenario-2 verify extra columns are adding to table
#Table manually created with 2 columns in metada gave 6 columns, functions has to add extra four columns to the table 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check table if function is adding extra columns gave in metadata' 
  testTable = 'mainNaSqldDbdt_a' 
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
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 2 function tableStructureCheck')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 2 function tableStructureCheck : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-3 Verify new columns are adding last to the table
#Table manually created with 2 columns in metada gave 6 columns, functions has to add extra four columns to the table last
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify new columns are adding last to the table' 
  testTable = 'mainNaSqldDbdt_a' 
  #If first two columns match with updated table, make testcase as success

  if updatedTableCols[0:2] == tableCols:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 3 function tableStructureCheck')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 function tableStructureCheck : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Drop the table if exist for previous runs
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS mainNaSqldDbdt_a;
# MAGIC DROP TABLE IF EXISTS mainNaSqldDbdt_c;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm('/mnt/dataquality/unit_tests/mainNaSqldDbdt/input/mainNaSqldDbdt_a',recurse=True)
dbutils.fs.rm('/mnt/dataquality/unit_tests/mainNaSqldDbdt/input/mainNaSqldDbdt_c',recurse=True)

# COMMAND ----------

# DBTITLE 1,Create table to test scenario-4 
# MAGIC %sql
# MAGIC create table mainNaSqldDbdt_a(
# MAGIC mainNaSqldDbdt_xyz STRING,
# MAGIC mainNaSqldDbdt_id STRING)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/mainNaSqldDbdt/input/mainNaSqldDbdt_a"

# COMMAND ----------

# DBTITLE 1,Scenario-4 Verify column renaming
#While creating the table first column is 'mainNaSqldDbdt_xyz' in metadata to the particulr table first column is 'mainNaSqldDbdt_name' it has to rename as 'mainNaSqldDbdt_name'
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check column renaming' 
  testTable = 'mainNaSqldDbdt_a' 
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
  if 'mainNaSqldDbdt_xyz' not in updatedTableCols and 'mainNaSqldDbdt_name' in updatedTableCols:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 4 function tableStructureCheck')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 4 function tableStructureCheck : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Drop the table if exist for previous runs
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS mainNaSqldDbdt_a;
# MAGIC DROP TABLE IF EXISTS mainNaSqldDbdt_c;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm('/mnt/dataquality/unit_tests/mainNaSqldDbdt/input/mainNaSqldDbdt_a',recurse=True)
dbutils.fs.rm('/mnt/dataquality/unit_tests/mainNaSqldDbdt/input/mainNaSqldDbdt_c',recurse=True)

# COMMAND ----------

# DBTITLE 1,Create table to test scenario-5
# MAGIC %sql
# MAGIC create table mainNaSqldDbdt_c(
# MAGIC mainNaSqldDbdt_value INT,
# MAGIC mainNaSqldDbdt_city STRING,
# MAGIC mainNaSqldDbdt_area STRING)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/mainNaSqldDbdt/input/mainNaSqldDbdt_c"

# COMMAND ----------

# DBTITLE 1,Get active objects
try:
  testTableList = ["mainNaSqldDbdt_a","mainNaSqldDbdt_b","mainNaSqldDbdt_c"]
  activeObjects = ["mainNaSqldDbdt_a","mainNaSqldDbdt_c"]
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

# DBTITLE 1,Scenario-5 Verify table properties update
#While creating table manually no table properties.In metadata table properties 'logRetentionDuration' gave.after updating table properties has to update
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify table properties update' 
  testTable = 'mainNaSqldDbdt_c' 
  #Get table properties before function execution
  tblPartition = spark.sql("desc detail {}".format(testTable)).select('partitionColumns').rdd.flatMap(lambda x: x).collect()
  tableProps = spark.sql("desc detail {}".format(testTable)).select('properties').rdd.flatMap(lambda x: x).collect()
  tableStructureCheck(tableHighDetailsDf
                      ,tableLowDetailsDf,
                        cursor,
                        batchTaskId,
                        errorMessage,
                        adfPipelineName,
                        clusterId,
                        notebookName,
                        errorLogFileLocation)
  updatedTableProps = spark.sql("desc detail {}".format(testTable)).select('properties').rdd.flatMap(lambda x: x).collect()
  updatedTablePartition = spark.sql("desc detail {}".format(testTable)).select('partitionColumns').rdd.flatMap(lambda x: x).collect()
  
  if 'logRetentionDuration' not in str(tableProps) and 'logRetentionDuration' in str(updatedTableProps):
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 6 function tableStructureCheck')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 6 function tableStructureCheck : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-6 Verify table partition update
#While creating table manually no table partition.In metadata table partition column  'mainNaSqldDbdt_value' gave.after updating table partition has to update
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify table partition update' 
  testTable = 'mainNaSqldDbdt_c' 
  #If first two columns match with updated table, make testcase as success

  if 'mainNaSqldDbdt_value' not in str(tblPartition) and 'mainNaSqldDbdt_value' in str(updatedTablePartition):
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 3 function tableStructureCheck')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 function tableStructureCheck : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Drop the table if exist for previous runs
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS mainNaSqldDbdt_a;
# MAGIC DROP TABLE IF EXISTS mainNaSqldDbdt_c;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm('/mnt/dataquality/unit_tests/mainNaSqldDbdt/input/mainNaSqldDbdt_a',recurse=True)
dbutils.fs.rm('/mnt/dataquality/unit_tests/mainNaSqldDbdt/input/mainNaSqldDbdt_c',recurse=True)

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
try:
  #run cleanup script
  cleanupMetadata = open("{}MainNaSqldDbdtCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanupMetadata)
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