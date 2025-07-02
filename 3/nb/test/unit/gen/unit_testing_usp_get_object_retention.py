# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_get_object_retention</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test the usp_get_object_retention store procedure </td></tr>
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
# MAGIC     <td>Updated stored procedure to add batch_task_id as an input parameter changed the test scenario for it </td>
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

# DBTITLE 1,Import Module
try:
  import pandas as pd
  import pyodbc
  from datetime import datetime,timedelta
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
  import fnmatch
except Exception as e:
  assert False 

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try :
 
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
  testObject = 'stored procedure'
  testObjectName = 'uspGetObjectRetention'
  sampleOutputLocation = ''
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  requiredInputParameter = 'batchTaskId,errorMessage,adfPipelineName,clusterID,notebookName,keyColumnName,keyTableName,notebookName,stagingTableName'
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')
  executionDateTime = currentTs
  
except Exception as e:
  errorMessage = 'Exception occurred while variable declaration' + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try:
  #GET batchTaskId FROM WIDGETS
  dbutils.widgets.text("batchId","")
  batchId = dbutils.widgets.get("batchId")
  
  #GET batchTaskId FROM WIDGETS
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")
  
  #GET sourceId FROM WIDGETS
  dbutils.widgets.text("sourceId","")
  sourceId = dbutils.widgets.get("sourceId")
  
  #GET adfPipelineName FROM WIDGETS
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName = dbutils.widgets.get("adfPipelineName")
  
  #GET notebookName FROM WIDGETS
  dbutils.widgets.text("notebookName","")
  notebookName = dbutils.widgets.get("notebookName")
  
  #GET clusterId FROM WIDGETS
  dbutils.widgets.text("clusterId","")
  clusterId = dbutils.widgets.get("clusterId")
 
 
  #CALL THE GET_LOGGING_PATH FUNCTION TO CREATE A LOG FILE PATH AS A STRING AND STORE IT IN A VARIABLE
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')

except Exception as e:
  errorMessage = "Exception occurred while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn=dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Established Database connection successfully')
except Exception as e:
  errorMessage = 'Exception occurred while connecting to database: ' + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Cleanup data from previous TaskEnd unit testing
#run cleanup script
try:
  cleanup = open("{}uspGetObjectRetentionCleanUp.sql".format(testInputPath), "r").read()
  
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts for usp_get_object_retention')   
except Exception as e:
  errorMessage = "Exception occurred while execcuting clean up scripts for usp_get_object_retention: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Scenario 1 - Data preparation
# scripts to insert an object into the tbl_object , tbl_object_definition and tbl_object_retention_basic 
try:
  
   #execute the script and get batch_task_id and task_id
  inputParmeter = open("{}uspGetObjectRetentionDataPrepration1.sql".format(testInputPath), "r").read()
  inputParmeter = inputParmeter.replace('<batch_id>', str(batchId))
  
  inputParameterResults = pd.read_sql_query(inputParmeter,conn).astype(str)
  
  #get the value for testing 
  newBatchTaskId = inputParameterResults.at[0,'new_batch_task_id']
  objectName = inputParameterResults.at[0,'object_name']
  objectAttributeName = inputParameterResults.at[0,'object_attribute_name']
  
  logTaskProgress(cursor,batchTaskId,"Executed insertScript scripts successfully for unit_testing_usp_get_object_retention")     
except Exception as e:
  errorMessage = "Exception occurred while executing insertScript scripts for unit_testing_usp_get_object_retention: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 1 - Check Procedure return value
# insert an object into the tbl_object , tbl_object_definition and tbl_object_retention_basic where active flag is 1 in object and object_retention_basic table check if the values returned by store procedure are correct 

try :
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  testCaseScenario = 'Result should return one active object'
   
  #execute the procedure  
  retentionObject = pd.read_sql_query("exec config.usp_get_object_retention {} ".format(newBatchTaskId), conn)
   
  #check it the store procedure returns one row 
  if retentionObject.shape[0] == 1 :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,"Successfully executed scenario1 test case for stored procedure usp_get_object_retention")  
except Exception as e:
  errorMessage = "Exception occurred while executing scenario1 test case for stored procedure usp_get_object_retention: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 2 - Check the delete statement returned by the store procedure 
# Check the delete statement returned by the store procedure 
try :
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  testCaseScenario = 'Check the delete statement returned by the store procedure  '


  #create a spark dataframe from store procedure result 
  retentionObjectDf = spark.createDataFrame(retentionObject)
  
  #convert the dataframe to Dic and variables to check it the delete statement returned is in correct syntax 
  retentionObjectDic = retentionObjectDf.collect()[0].asDict()
  deleteStatement =  retentionObjectDic['delete_statement']

  #Construct the delete statement string
  deleteString = 'DELETE FROM ' +objectName +' WHERE ' + objectAttributeName +' <= *'
  
  #check if delete statement is in correct format
  #fnmatch function is used to test whether the string matches the pattern string
  if (fnmatch.fnmatch(deleteStatement, deleteString)) :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,"Successfully executed scenario2 test case for stored procedure usp_get_object_retention")  
except Exception as e:
  errorMessage = "Exception occurred while executing scenario2 test case for stored procedure usp_get_object_retention: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}uspGetObjectRetentionCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,"Successfully executed clean up scripts for usp_get_object_retention")   
except Exception as e:
  errorMessage="Exception occured while execcuting clean up scripts for usp_get_object_retention: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False    

# COMMAND ----------

# DBTITLE 1,Close database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False