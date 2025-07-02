# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_object_to_object_low_level</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test stored procedure usp_object_to_object_low_level</td></tr>
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
  import datetime
  from datetime import datetime
  from pyspark.sql.functions import col
except Exception as e:
  errorMessage="Exception occured while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try :
  #variable for current timestamp
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
  testObjectName = 'uspGetObjectToObjectLowLevel'  
  requiredInputParameter = 'batchTaskId'
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
  errorMessage = "Exception occured while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish Database Connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage="Exception occured while connecting to database: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Perform cleanup from previous runs
#run cleanup script
try:
  cleanup = open("{}GetObjectToObjectLowLevelCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage = "Exception occured while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario 1-Data preparation for input parameter
#execute scripts to
try:
  #inserting data into tbl_task and tbl_batch_task
  inputParameter = open('{}GetObjectToObjectLowLevelInsertPrep1.sql'.format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>',str(batchId))
  #execute the prep1 script and get the first row
  inputParameterResults = pd.read_sql_query(inputParameter,conn).iloc[0]

  #get the first value of the row and cast as int
  newBatchTaskId = int(inputParameterResults[0])
  logTaskProgress(cursor,batchTaskId,'Execution of script to generate input parameter for usp_object_to_object_low_level')
except Exception as e:
  errorMessage = "Exception occured while 'Executing of test scenario-1 data preparation: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-1 No objects mapped for task_id
#Data inserted only in task_table and batch_task_table.There are no data inserted into mapping tables.Hence result will return an empty data set.
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check for task_id that is not mapped with any objects'
  #Run stored procedure to test
  spOutput = pd.read_sql_query("exec config.usp_get_object_to_object_low_level {}".format(newBatchTaskId),conn)
  #check the count of result which should be empty
  if spOutput.shape[0] == 0:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 1 stored procedure usp_object_to_object_low_level')  
except Exception as e:
  errorMessage = "Exception occured while testing scenario 1 stored procedure usp_object_to_object_low_level : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-2 Data preparation 
#scripts to populate data into tbl_object and tbl_object_definition and tbl_object_definition_map
try:
  insertData = open('{}GetObjectToObjectLowLevelInsertPrep2.sql'.format(testInputPath), "r").read()
  insertData = insertData.replace('<batch_task_id>', str(newBatchTaskId)) # replace batch_task_id to test for current batch_task
  cursor.execute(insertData) #Execute the script
  while cursor.nextset():
    x = 1  
  logTaskProgress(cursor,batchTaskId,'Execution of script to populate data in corresponding tables for usp_object_to_object_low_level')
except Exception as e:
  errorMessage = "Exception occured while Executing script to populate data for usp_object_to_object_low_level " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-2 For task_id stored procedure returns mapped object
#Data is populated to object_to_object_map,object_definition,object tables.now for batch_task_id some set of data is returned
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test should return source and destination columns for the task_id'
  
  #Run stored procedure to test
  spOutput = pd.read_sql_query("exec config.usp_get_object_to_object_low_level {}".format(newBatchTaskId),conn)
  
  #If the row count of dataframe is 2 then it is retrieves the data
  if spOutput.shape[0] == 2:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 2 for stored procedure usp_object_to_object_low_level')  
except Exception as e:
  errorMessage = "Exception occured while testing on scenario 2 for stored procedure usp_object_to_object_low_level': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-3 Verify columns for source and destination
#verify source and destination columns has been mapped correctly.compare against sqlquery and stored procedure output
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check on source and destination columns are mapped correctly'
  
  #Source columns while preparing data for source (data preparation script-2)
  sourceCols = ['row_id']
  
  #Destination columns while preparing data for destination (data preparation script-2)
  destinationCols = ['ROW_ID','id']
  
  #Run stored procedure to test
  spOutput = spark.createDataFrame(pd.read_sql_query("exec config.usp_get_object_to_object_low_level {}".format(newBatchTaskId),conn).astype(str))
  spSourceCols = spOutput.where(col('destination_object_attribute_computed_type') != 'expression')
  #If shape of query and stored procedure same then all columms are matching including computed columns
  if len(sourceCols) == spSourceCols.count() and len(destinationCols) == spOutput.count():
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'

  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 3 for stored procedure usp_object_to_object_low_level')  
except Exception as e:
  errorMessage = "Exception occured while testing on scenario 3 for stored procedure usp_object_to_object_low_level: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False    
    

# COMMAND ----------

# DBTITLE 1,Scenario-4 Verify only active objects are returned
#populated data with active and inactive object in object table and testing against the result where only active object is returned.
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check for active object are returned'
  
  #Query to get active object_id whle preparing data 'Stg_GetObjectToObjectLowLevel_b' object is active object 
  activeObject = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = 'Stg_GetObjectToObjectLowLevel_b'",conn)).collect()[0][0]
  
  #execute stored procedure to test
  spOutput = spark.createDataFrame(pd.read_sql_query("exec config.usp_get_object_to_object_low_level {}".format(newBatchTaskId),conn).astype(str))
  #Get the object_id
  spDestObjId = spOutput.select(spOutput.destination_object_id).distinct().collect()[0][0]
  # If Query object_id and stored procedure object_id matches then active object are only returned
  if str(activeObject) == spDestObjId:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'

  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 4 for stored procedure usp_object_to_object_low_level')  
except Exception as e:
  errorMessage = "Exception occured while testing on scenario 4 for stored procedure usp_object_to_object_low_level " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False    

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
try:
  #run cleanup script
  cleanupMetadata = open("{}GetObjectToObjectLowLevelCleanUp.sql".format(testInputPath), "r").read()
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