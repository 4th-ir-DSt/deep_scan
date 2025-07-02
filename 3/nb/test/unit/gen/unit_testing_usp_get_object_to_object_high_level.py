# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_get_object_to_object_high_level</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test stored procedure usp_get_object_to_object_high_level</td></tr>
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
# MAGIC     <td>@batchId = 7</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchTaskId</td>
# MAGIC     <td>@batchTaskId to retrieve batchTask details</td>
# MAGIC     <td>@batchTaskId = 10</td>
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
# MAGIC     <td>modified test cases</td>
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
  from datetime import datetime, timedelta
  from pyspark.sql.types import StringType
except Exception as e:
  assert False 

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try :
  #variable for current_time
  currentTs=datetime.now()
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
  date=currentTs

  #PARAMETER FOR LOG_ERROR
  errorLine = ''
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
  testObjectName = 'uspGetObjectToObjectHighLevel'  
  requiredInputParameter = 'batchTaskId'
  sampleOutputLocation = 'NA'
  
  #path for datasets used in stored procedure 
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
except Exception as e:
  errorMessage = "Exception occurred while variable declaration " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try:
  #GET batchTaskId FROM WIDGETS   
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")

  #GET batchId FROM WIDGETS
  dbutils.widgets.text("batchId","")
  batchId = dbutils.widgets.get("batchId")
 
  #GET sourceId FROM WIDGETS   
  dbutils.widgets.text("sourceId","")
  sourceId  = dbutils.widgets.get("sourceId")

  #GET adfPipelineName FROM WIDGETS
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName  = dbutils.widgets.get("adfPipelineName")
  
  #GET notebookName FROM WIDGETS   
  dbutils.widgets.text("notebookName","")
  notebookName  = dbutils.widgets.get("notebookName")

  #GET clusterId FROM WIDGETS
  dbutils.widgets.text("clusterId","")
  clusterId  = dbutils.widgets.get("clusterId")
  
  #call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occurred while fetching parameters and initialising error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish Database Connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = "Exception occurred while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup for previous run
#script for metadata cleanup
try: 
  metadatacleanup = open("{}GetObjectToObjectHighLevelCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Executed cleanup scripts successfully')     
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False
  

# COMMAND ----------

# DBTITLE 1,Scenario 1-Data preparation
#insertion of data into object,task_object_map, object_definition and object_dates_availability tables
try:
  #execute the script and get batch_task_id and task_id
  inputParmeter = open("{}GetBatchTaskToObjectHighLevel.sql".format(testInputPath), "r").read()
  inputParmeter = inputParmeter.replace('<batch_id>', str(batchId))

  inputParameterResults = pd.read_sql_query(inputParmeter,conn).astype(str)
  
  #get the batch_task_id and cast as int
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])
  
  #get the task_id and cast as int
  newTaskId = int(inputParameterResults.at[0,'task_id'])
  #insertion of data into object,task_object_map, object_definition and object_dates_availability tables
  dataPreparation = open("{}GetObjectToObjectHighLevelPrep1.sql".format(testInputPath), "r").read()
  dataPreparation = dataPreparation.replace('<batch_task_id>', str(newBatchTaskId)).replace('<task_id>', str(newTaskId))
  cursor.execute(dataPreparation)
  while cursor.nextset():
    x = 1
  
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate input parameter and data insertion into tables')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter and data insertion': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 1- Get the multiple objects for same source
#to check whether stored procedure has returned the data based on batch_task_id and it has to return multiple targets for the given source
try:
  #Declare variables
  testCaseScenario = 'Check multiple target objects returned for same source'
  testCaseStatus = 'failed'
  executionOutputStatus='Mismatched'
  
  #execute stored procedure to get multiple rows by using batch_task_id 
  spOutput = pd.read_sql_query("exec config.usp_get_object_to_object_high_level {}".format(newBatchTaskId),conn)
  
  #stored procedure should return two rows
  if spOutput.shape[0] == 2:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario 1 test case for stored procedure usp_get_object_to_object_high_level')  
except Exception as e:
  errorMessage = "Exception occurred while executing scenario 1 test case for stored procedure usp_get_object_to_object_high_level: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 1 - Metadata Cleanup
#script for metadata cleanup
try: 
  metadatacleanup = open("{}GetObjectToObjectHighLevelCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed cleanup scripts successfully')     
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 2-Data preparation
#populate data into object,task_object_map, object_definition and object_dates_availability tables
try:
  #execute the script to get batch_task_id and task_id
  inputParmeter = open("{}GetBatchTaskToObjectHighLevel.sql".format(testInputPath), "r").read()
  inputParmeter = inputParmeter.replace('<batch_id>', str(batchId))
  inputParmeterResult = pd.read_sql_query(inputParmeter,conn)
  
  #get the batch_task_id and cast as int
  newBatchTaskId = int(inputParmeterResult.at[0,'batch_task_id'])
  
  #get the task_id and cast as int
  newTaskId = int(inputParmeterResult.at[0,'task_id'])
  
  #insertion of data into object,task_object_map, object_definition and object_dates_availability tables
  dataPreparation = open("{}GetObjectToObjectHighLevelPrep2.sql".format(testInputPath), "r").read()
  dataPreparation = dataPreparation.replace('<batch_task_id>', str(newBatchTaskId)).replace('<task_id>', str(newTaskId))
  cursor.execute(dataPreparation)
  while cursor.nextset():
    x = 1
  
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate input parameter and data insertion into tables')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter and data insertion': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 2- Get the same day data (where clause)
#to check whether stored procedure has returned the data based on batch_task_id 
try:
  #Declare variables
  testCaseScenario = 'object high level data check get the same day data'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  #expected where clause output
  whereClause = "country = 'UK' AND ((date = 20200506 AND time > 132404 AND time <= 162404)) AND ((filecreateddate > CAST('2020-05-06 13:24:04.000' AS TIMESTAMP)) AND (filecreateddate <= CAST('2020-05-06 16:24:04.000' AS TIMESTAMP)))"
  
  #execute stored procedure to get same day object details by using batch_task_id
  spOutput = pd.read_sql_query("exec config.usp_get_object_to_object_high_level {}".format(newBatchTaskId),conn)
  
  #get the source_where_clause
  spOutputWhereClause = spOutput.at[0,'source_where_clause']

  #get the batch_task_where_clause
  batchWhereClause = pd.read_sql_query("select batch_task_where_clause from audit.tbl_batch_task WHERE batch_task_id = {}".format(newBatchTaskId),conn)
  
  #retrieve batchWhereClause
  batchTaskWhereClause = batchWhereClause.at[0,'batch_task_where_clause']
  
  #Read expected output from sql statement as pandas dataframe
  spExpectedOutput = pd.read_sql_query("SELECT DATEDIFF(day,min(output_datetime_from),max(output_datetime_to)) as DATEDIFF FROM audit.tbl_object_dates_availability oda join  config.tbl_task_object_map map on oda.output_object_id = map.object_id WHERE oda.batch_task_id = {} and map.designation = 'source' GROUP BY oda.batch_task_id".format(newBatchTaskId),conn)

  #the stored procedure should returns one row and date difference is 0 then its a same day's data  
  if spOutput.shape[0] == 2 and spExpectedOutput.at[0,'DATEDIFF'] == 0 and spOutputWhereClause == whereClause and batchTaskWhereClause == whereClause:    
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario 2 test case for stored procedure usp_get_object_to_object_high_level') 
except Exception as e:
  errorMessage = "Exception occurred while executing scenario 2 test case for stored procedure usp_get_object_to_object_high_level: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 2-Metadata Cleanup
#script for metadata cleanup
try: 
  metadatacleanup = open("{}GetObjectToObjectHighLevelCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed cleanup scripts successfully')     
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 3-Data Preparation
#populate data into object,task_object_map, object_definition and object_dates_availability tables
try:
  #execute the script and get batch_task_id and task_id
  inputParmeter = open("{}GetBatchTaskToObjectHighLevel.sql".format(testInputPath), "r").read()
  inputParmeter = inputParmeter.replace('<batch_id>', str(batchId))
  inputParmeterResult = pd.read_sql_query(inputParmeter,conn)
  
  #get the batch_task_id and cast as int
  newBatchTaskId = int(inputParmeterResult.at[0,'batch_task_id'])
  
  #get the task_id and cast as int
  newTaskId = int(inputParmeterResult.at[0,'task_id'])
  
  #insertion of data into object,task_object_map, object_definition and object_dates_availability tables
  dataPreparation = open("{}GetObjectToObjectHighLevelPrep3.sql".format(testInputPath), "r").read()
  dataPreparation = dataPreparation.replace('<batch_task_id>', str(newBatchTaskId)).replace('<task_id>', str(newTaskId))
  cursor.execute(dataPreparation)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate input parameter and data insertion into tables')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter and data insertion': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 3 -Get the data for two days (where clause)
#to check whether stored procedure has returned the data based on batch_task_id and where clause should return the data between two days 
try:
  #Declare variables
  testCaseScenario = 'object high level data check get the the data for two days'
  testCaseStatus = 'failed'
  executionOutputStatus =  'Mismatched'
  #expected where clause output
  whereClause = "country = 'UK' AND ((date = 20200506 AND time > 102404) OR (date = 20200507 AND time <= 112404)) AND ((filecreateddate > CAST('2020-05-06 10:24:04.000' AS TIMESTAMP)) AND (filecreateddate <= CAST('2020-05-07 11:24:04.000' AS TIMESTAMP)))"
  
  #execute stored procedure to get two days object details by using batch_task_id
  spOutput = pd.read_sql_query("exec config.usp_get_object_to_object_high_level {}".format(newBatchTaskId),conn)
   
  #get the source_where_clause
  spOutputWhereClause = spOutput.at[0,'source_where_clause']
  #get the batch_task_where_clause
  batchWhereClause = pd.read_sql_query("select batch_task_where_clause from audit.tbl_batch_task WHERE batch_task_id = {}".format(newBatchTaskId),conn)
  batchTaskWhereClause = batchWhereClause.at[0,'batch_task_where_clause']

  #Read expected output from sql statement as pandas dataframe
  spExpectedOutput = pd.read_sql_query("SELECT DATEDIFF(day,min(output_datetime_from),max(output_datetime_to)) as DATEDIFF FROM audit.tbl_object_dates_availability oda join  config.tbl_task_object_map map on oda.output_object_id = map.object_id WHERE batch_task_id = {} and map.designation = 'source' GROUP BY batch_task_id".format(newBatchTaskId),conn)

  #the stored procedure should return one row and date difference is 1 then it is two days data
  if spOutput.shape[0] == 2 and spExpectedOutput.at[0,'DATEDIFF'] == 1 and spOutputWhereClause == whereClause and batchTaskWhereClause == whereClause:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario 3 test case for stored procedure usp_get_object_to_object_high_level')  
except Exception as e:
  errorMessage = "Exception occurred while executing scenario 3 test case for stored procedure usp_get_object_to_object_high_level: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 3-Metadata Cleanup
#script for metadata cleanup
try: 
  metadatacleanup = open("{}GetObjectToObjectHighLevelCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed cleanup scripts successfully')     
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 4-Data preparation
#insertion of data into object,task_object_map, object_definition and object_dates_availability tables
try:
  #execute the script and get batch_task_id and task_id
  inputParmeter = open("{}GetBatchTaskToObjectHighLevel.sql".format(testInputPath), "r").read()
  inputParmeter = inputParmeter.replace('<batch_id>', str(batchId))
  inputParmeterResult = pd.read_sql_query(inputParmeter,conn)
  
  #get the batch_task_id and cast as int
  newBatchTaskId = int(inputParmeterResult.at[0,'batch_task_id'])
  #get the task_id and cast as int
  newTaskId = int(inputParmeterResult.at[0,'task_id'])
  
  #insertion of data into object,task_object_map, object_definition and object_dates_availability tables
  dataPreparation = open("{}GetObjectToObjectHighLevelPrep4.sql".format(testInputPath), "r").read()
  dataPreparation = dataPreparation.replace('<batch_task_id>', str(newBatchTaskId)).replace('<task_id>', str(newTaskId))
  cursor.execute(dataPreparation)
  while cursor.nextset():
    x = 1
  
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate input parameter and data insertion into tables')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter and data insertion': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 4 - Get the data more than two days
#to check whether stored procedure has returned the data based on batch task id and where clause should return the data between more than two days
try:
  #Declare variables
  testCaseScenario = 'object high level data check get the the data more than two days'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  #expected where clause output
  whereClause = "country = 'UK' AND ((date = 20200504 AND time > 102404) OR (date >= 20200505 AND date < 20200507) OR (date = 20200507 AND time <= 102404)) AND ((filecreateddate > CAST('2020-05-04 10:24:04.000' AS TIMESTAMP)) AND (filecreateddate <= CAST('2020-05-07 10:24:04.000' AS TIMESTAMP)))"
  #execute stored procedure to get more than two days object details by using batch_task_id
  spOutput = pd.read_sql_query("exec config.usp_get_object_to_object_high_level {}".format(newBatchTaskId),conn)

  #get the source_where_clause
  spOutputWhereClause = spOutput.at[0,'source_where_clause']
  
  #get the batch_task_where_clause
  batchWhereClause = pd.read_sql_query("select batch_task_where_clause from audit.tbl_batch_task WHERE batch_task_id = {}".format(newBatchTaskId),conn)
  batchTaskWhereClause = batchWhereClause.at[0,'batch_task_where_clause']
  
  #Read expected output from sql statement as pandas dataframe
  spExpectedOutput = pd.read_sql_query("SELECT DATEDIFF(day,min(output_datetime_from),max(output_datetime_to)) as DATEDIFF FROM audit.tbl_object_dates_availability oda join  config.tbl_task_object_map map on oda.output_object_id = map.object_id WHERE batch_task_id = {} and map.designation = 'source' GROUP BY batch_task_id".format(newBatchTaskId),conn)

  #when stored procedure returns one row and date difference is greater than 1 then its  more than two days data
  if spOutput.shape[0] == 2 and spExpectedOutput.at[0,'DATEDIFF'] > 1  and spOutputWhereClause == whereClause and batchTaskWhereClause == whereClause:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario 4 test case for stored procedure usp_get_object_to_object_high_level')  
except Exception as e:
  errorMessage = "Exception occurred while executing scenario 4 test case for stored procedure usp_get_object_to_object_high_level: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 4-Metadata Cleanup
#script for metadata cleanup
try: 
  metadatacleanup = open("{}GetObjectToObjectHighLevelCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed cleanup scripts successfully')     
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 5-Data preparation
#insertion of data into object,task_object_map, object_definition and object_dates_availability tables
try:
  #execute the script and get batch_task_id and task_id
  inputParmeter = open("{}GetBatchTaskToObjectHighLevel.sql".format(testInputPath), "r").read()
  inputParmeter = inputParmeter.replace('<batch_id>', str(batchId))
  inputParmeterResult = pd.read_sql_query(inputParmeter,conn)
  
  #get the batch_task_id and cast as int
  newBatchTaskId = int(inputParmeterResult.at[0,'batch_task_id'])
  #get the task_id and cast as int
  newTaskId = int(inputParmeterResult.at[0,'task_id'])
  
  #insertion of data into object,task_object_map, object_definition and object_dates_availability tables
  dataPreparation = open("{}GetObjectToObjectHighLevelPrep5.sql".format(testInputPath), "r").read()
  dataPreparation = dataPreparation.replace('<batch_task_id>', str(newBatchTaskId)).replace('<task_id>', str(newTaskId))
  cursor.execute(dataPreparation)
  while cursor.nextset():
    x = 1
  
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate input parameter and data insertion into tables')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter and data insertion': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 5- Get the same day data (No time difference)
#to check whether stored procedure has returned the data based on batch_task_id 
try:
  #Declare variables
  testCaseScenario = 'object high level data check get the same day no time difference'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  #expected where clause output
  whereClause = "country = 'UK' AND ((date = 20200506 AND time = 202404)) AND ((filecreateddate = CAST('2020-05-06 20:24:04.000' AS TIMESTAMP)))"
  
  #execute stored procedure to get same day object details by using batch_task_id
  spOutput = pd.read_sql_query("exec config.usp_get_object_to_object_high_level {}".format(newBatchTaskId),conn)
  
  #get the source_where_clause
  spOutputWhereClause = spOutput.at[0,'source_where_clause']
 
  #get the batch_task_where_clause
  batchWhereClause = pd.read_sql_query("select batch_task_where_clause from audit.tbl_batch_task WHERE batch_task_id = {}".format(newBatchTaskId),conn)
  
  batchTaskWhereClause = batchWhereClause.at[0,'batch_task_where_clause']
  
  #Read expected output from sql statement as pandas dataframe
  spExpectedOutput = pd.read_sql_query("SELECT DATEDIFF(day,min(output_datetime_from),max(output_datetime_to)) as DATEDIFF, DATEDIFF(HH,min(output_datetime_from),max(output_datetime_to)) as TIMEDIFF FROM audit.tbl_object_dates_availability oda join  config.tbl_task_object_map map on oda.output_object_id = map.object_id WHERE oda.batch_task_id = {} and map.designation = 'source' GROUP BY oda.batch_task_id".format(newBatchTaskId),conn)

  #the stored procedure should return two rows and date difference is 0 then its a same day's data  
  if spOutput.shape[0] == 2 and spExpectedOutput.at[0,'DATEDIFF'] == 0 and spExpectedOutput.at[0,'TIMEDIFF'] == 0 and spOutputWhereClause == whereClause and batchTaskWhereClause == whereClause:    
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario 5 test case for stored procedure usp_get_object_to_object_high_level') 
except Exception as e:
  errorMessage = "Exception occurred while executing scenario 5 test case for stored procedure usp_get_object_to_object_high_level: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 5-Metadata Cleanup
#script for  metadata cleanup
try: 
  metadatacleanup = open("{}GetObjectToObjectHighLevelCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed cleanup scripts successfully')     
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
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