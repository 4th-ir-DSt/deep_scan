# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_get_raw_eventhub_object_locations</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test stored procedure usp_get_raw_eventhub_object_locations</td></tr>
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
# MAGIC     <td>2019/01/01</td>
# MAGIC     <td>Framework</td>
# MAGIC     <td>Incorporated function convertPandasToSparkDfWithSchema</td>
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
  from datetime import datetime,timedelta
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
  from pyspark.sql.functions import col
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
  testObject = 'stored procedure'
  testObjectName = 'uspGetRawEventHubObjectLocations'  
  baseObjectName = 'GetRawEventHubObjectLocations'
  requiredInputParameter = ''
  testCaseScenario = ''
  executionDateTime = datetime.now()
  sampleOutputLocation = 'NA'
  
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
  dbConn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
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
  cleanup = open("{}GetRawEventHubObjectLocationsCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage = "Exception occured while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Data preparation for test scenario-1
#Preparing parameters for stored procedure test
try:
  insertData = open('{}GetRawEventHubObjectLocationsDataPrep.sql'.format(testInputPath), "r").read()
  cursor.execute(insertData) #Execute the script
  while cursor.nextset():
    x = 1  
    
  logTaskProgress(cursor,batchTaskId,'Successfully prepared data for test scenario')
except Exception as e:
  errorMessage = "Exception occured while prepared data for test scenario " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Schema for stored procedure output
try:
  schema = StructType([StructField("object_id"                   , IntegerType(), True), 
                       StructField("location"                    , StringType() , True),
                       StructField("format"                      , StringType() , True),
                       StructField("partition_date_time_columns" , StringType() , True),
                       StructField("infile_datetime_column"      , StringType() , True)])
  logTaskProgress(cursor,batchTaskId,'Successfully prepared Schema for dataframe')
except Exception as e:
  errorMessage = "Exception occured while preparing schema for test " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 
  

# COMMAND ----------

# DBTITLE 1,Scenario-1 Verify stored procedure retrieves active and raw object
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify stored procedure retrieves active and raw object'
  #Execute stored procedure
  spOutput = convertPandasToSparkDfWithSchema(pd.read_sql_query("exec config.usp_get_raw_eventhub_object_locations",conn),schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  #Get records for the inserted test_object where record is inserted while data preparation for positive case
  #If count = 1 then it is taking active object with structured location for Raw object
  spInsertedResult = spOutput.select('object_id').where('location = "/mnt/raw/{}/eh_samplePasitiveTest"'.format(baseObjectName))
  #Verify record inserted through stored procedure, If shape is 1 then testcase is success 
  if spInsertedResult.count() == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 1 stored procedure usp_get_raw_eventhub_object_locations')  
except Exception as e:
  errorMessage = "Exception occured while testing scenario 1 stored procedure usp_get_raw_eventhub_object_locations : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-2 Negative test case by passing inactive object 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify Negative test case by passing inactive object '
  #Execute stored procedure
  spOutput = convertPandasToSparkDfWithSchema(pd.read_sql_query("exec config.usp_get_raw_eventhub_object_locations",conn),schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  #Get records for the inserted test_object where record is inserted while data preparation for positive case
  #If count = 0 then it is taking for passed loaction is in active 
  spInsertedResult = spOutput.select('object_id').where('location = "/mnt/raw/{}/eh_sampleNegativeTest"'.format(baseObjectName))
  #Verify record inserted through stored procedure, If shape is 0 then testcase is success 
  if spInsertedResult.count() == 0:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 2 stored procedure usp_get_raw_eventhub_object_locations')  
except Exception as e:
  errorMessage = "Exception occured while testing scenario 2 stored procedure usp_get_raw_eventhub_object_locations : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
try:
  #run cleanup script
  cleanupMetadata = open("{}GetRawEventHubObjectLocationsCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanupMetadata)
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