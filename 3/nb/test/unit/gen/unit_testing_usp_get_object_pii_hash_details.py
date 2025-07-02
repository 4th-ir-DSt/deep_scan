# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_get_object_pii_hash_details</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test stored procedure usp_get_object_pii_hash_details</td></tr>
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

# DBTITLE 1,Import Module
try:
  import pandas as pd
  import pyodbc
  from datetime import datetime,timedelta
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
  from pyspark.sql.functions import col
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
  testObject = 'stored procedure'
  testObjectName = 'uspGetObjectPiiHashDetails'  
  requiredInputParameter = 'objectId,sourceId'
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
  cleanup = open("{}GetObjectPiiHashDetailsCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage = "Exception occurred while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Data preparation for test scenario
#Preparing parameters for stored procedure test
try:
  insertData = open('{}GetObjectPiiHashDetailsDataPrep.sql'.format(testInputPath), "r").read()
  cursor.execute(insertData) #Execute the script
  while cursor.nextset():
    x = 1  
    
  logTaskProgress(cursor,batchTaskId,'Successfully prepared data for test scenario')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-1 Verify stored procedure is retrieving active hash versions 
#Stored procedure has to retrieve active versions only. While preparing data 2 versions inserted version-1 is inactive and version-2 is active
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify stored procedure is retrieving active hash versions'
  #Test object while preparing data data for scenario
  testObject = 'GetObjectPiiHashDetails_a'
  sourceID = 10
  #While preparing data version 2 is the active version
  activeVersion = 2
  #Get object_id for testObject
  testcaseInputDf = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObject),conn))
  testObjectId = testcaseInputDf.collect()[0][0]
  #Execute stored procedure
  spOutput = spark.createDataFrame(pd.read_sql_query("exec config.usp_get_object_pii_hash_details {},{}".format(testObjectId,sourceID), conn))
  spAactiveVersion = spOutput.collect()[0][0]
  #Verify if stored procedure output and active version match then make test case as success
  if spAactiveVersion == activeVersion:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 1 stored procedure usp_get_object_pii_hash_details')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 1 stored procedure usp_get_object_pii_hash_details : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-2 Verify piiExpression is substring of piiTreaceabilityExpression
#Stored procedure will return 3 columns hash_version,piiExpression and piiTreaceabilityExpression. piiTreaceabilityExpression column contains piiExpression along with parameters passed to stored procedure  
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify piiExpression is substring of piiTreaceabilityExpression'
  #Execute stored procedure
  spOutput = spark.createDataFrame(pd.read_sql_query("exec config.usp_get_object_pii_hash_details {},{}".format(testObjectId,sourceID), conn))
  piiExpression = spOutput.collect()[0][1]
  piiTreaceabilityExpression = spOutput.collect()[0][2]
  #Verify stored procedure if piiExpression in piiTreaceabilityExpression then make test case as success 
  if piiExpression in piiTreaceabilityExpression:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 2 stored procedure usp_get_object_pii_hash_details')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 2 stored procedure usp_get_object_pii_hash_details : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-3 Verify piiExpression to check order of hash_attribute_type_code_id
#If multiple hash_attribute_type_code_id values for object then all attribute names will construct piiExpression value based on ascending order of hash_attribute_type_code_id
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify piiExpression to check order of hash_attribute_type_code_id'
  #hash_attribute_type_code_id,names while inserting data in table
  hashAttributeTypeCodeId = ['ZX','YX']
  hashAttributeNames = ['GetObjectPiiHashDetails_id','GetObjectPiiHashDetails_name']
  #Expected output will come based on hash_attribute_type_code_id ascending order 
  expectedOutput = "col(str('GetObjectPiiHashDetails_name')),col(str('GetObjectPiiHashDetails_id'))"
  #Execute stored procedure
  spOutput = spark.createDataFrame(pd.read_sql_query("exec config.usp_get_object_pii_hash_details {},{}".format(testObjectId,sourceID), conn))
  piiExpression = spOutput.collect()[0][1]
  #If expectedOutput and stored procedure output same then testcase is success 
  if piiExpression == expectedOutput:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 3 stored procedure usp_get_object_pii_hash_details')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 stored procedure usp_get_object_pii_hash_details : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-4 Verify piiTreaceabilityExpression is contain stored proc parameters
#piiTreaceabilityExpression contains piiExpression along with all parameters passed to stored procedure(including optional).
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify piiTreaceabilityExpression is contain stored proc parameters'
  #hash_attribute_type_code_id,names while inserting data in table
  sourceId = 1001
  tableName = 'GetObjectPiiHashDetails_a'
  #Execute stored procedure
  spOutput = spark.createDataFrame(pd.read_sql_query("exec config.usp_get_object_pii_hash_details {},{},{}".format(testObjectId,sourceId,tableName), conn))
  piiTreaceabilityExpression = spOutput.collect()[0][2]
  #Verify sourceId and tableName parameters in piiTreaceabilityExpression then make test case as success
  #IF there is no value for optional parameter it will add its default name
  if str(sourceId) in piiTreaceabilityExpression and tableName in piiTreaceabilityExpression and 'staging_created_batch_id' in piiTreaceabilityExpression:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 4 stored procedure usp_get_object_pii_hash_details')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 4 stored procedure usp_get_object_pii_hash_details : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-5 Negative test case with missing parameters
#Stored procedure need 2 parameters and 3 optional parameters. ObjectId and sourceId must need.If passing one parameter it has to fail
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Negative test case with missing parameters'
  #Test by passing less parameters, if pass less parameters it has to fail and will go to except block
  try:
    spOutput = spark.createDataFrame(pd.read_sql_query("exec config.usp_get_object_pii_hash_details {}".format(testObjectId), conn))
  except Exception as e:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 5 stored procedure usp_get_object_pii_hash_details')    
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 5 stored procedure usp_get_object_pii_hash_details : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
try:
  #run cleanup script
  cleanupMetadata = open("{}GetObjectPiiHashDetailsCleanUp.sql".format(testInputPath), "r").read()
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