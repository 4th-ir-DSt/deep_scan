# Databricks notebook source
# MAGIC %md # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_mark_dates_loaded</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test on stored procedure usp_mark_dates_loaded</td></tr>
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
# MAGIC     <td>02/03/2020 </td>
# MAGIC     <td>Framework/td>
# MAGIC     <td>Dev rework changes for camel case function names, date variables and removal of error line
# MAGIC         <br>Unit Test tweak-Added new parameter input and output object_id </td>
# MAGIC   </tr>
# MAGIC 
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
except Exception as e:
  assert False 

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try :
  #VARIBLE FOR CURRENT_TIME
  currentTs=datetime.now()
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
  testObjectName = 'uspMarkDatesLoaded'  
  requiredInputParameter = 'batchTaskId,destinationObjectId,SourceObjectId,outputDatetimeFrom,outputDatetimeTo'
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
  
  #Call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occurred while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish Database Connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Established Database connection successfully')
except Exception as e:
  errorMessage = "Exception occurred while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup for previous run
#run cleanup script
try:
  cleanup = open("{}markDatesLoadedCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for usp_mark_dates_loaded')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for usp_mark_dates_loaded: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False    

# COMMAND ----------

# DBTITLE 1,Generate Input parameter for stored procedure through scripts
#populate data into object_dates_availability table
try:
  inputParameter = open("{}markDatesLoadedPrep1.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>',str(batchId))
  inputParameterResults = pd.read_sql_query(inputParameter,conn)
  
  #input parameter for usp_mark_dates_loaded
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])  
  newInputObjectId = int(inputParameterResults.at[0,'input_object_id'])
  newOutputObjectId = int(inputParameterResults.at[0,'output_object_id'])
  newOutputDatetimeFrom = str(inputParameterResults.at[0,'output_datetime_from'])[0:23]
  newOutputDatetimeTo = str(inputParameterResults.at[0,'output_datetime_to'])[0:23] 
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate input parameters')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameters': " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 1-When is_raw_object = 2 input_datetime is populated in output_datetime column
#execute stored procedure when is_raw_object = 2. Based on the case statement, the column value of input_datetime_from is populated to output_datetime_from and input_datetime_to is populated to output_datetime_to column.

try:
  #Declare variables
  testCaseScenario = 'Check the input_datetime is populated in output_datetime when is_raw_object=2'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  
  #execute stored procedure with newBatchTaskId,newInputObjectId,newOutputObjectId,newOutputDatetimeFrom and newOutputDatetimeTo
  cursor.execute("exec audit.usp_mark_dates_loaded ?,?,?,?,?",newBatchTaskId,newOutputObjectId,newInputObjectId,newOutputDatetimeFrom,newOutputDatetimeTo)
  while cursor.nextset():
      x = 1
  
  #reading stored procedure result as pandas dataframe
  spOutput = pd.read_sql_query("SELECT * FROM audit.tbl_object_dates_availability WHERE batch_task_id = {} AND output_object_id = {} AND input_object_id = {} AND load_successful = 1".format(newBatchTaskId,newOutputObjectId,newInputObjectId),conn)
  
  # compare the column values between input_datetime_from and output_datetime_from and between input_datetime_to and output_datetime_to which should be equal when is_raw_object = 2 
  if spOutput.at[0,'input_datetime_from'] == spOutput.at[0,'output_datetime_from'] and spOutput.at[0,'input_datetime_to'] == spOutput.at[0,'output_datetime_to'] :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to log the test case status in tbl_unit_test
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario 1 test case for stored procedure usp_mark_dates_loaded')
except Exception as e:
  errorMessage = "Exception occurred while executing scenario 1 test case for stored procedure usp_mark_dates_loaded: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario 2 - Data preparation
#update is_raw_object = 1 in object_dates_availability table
try:
  inputParameter = open("{}markDatesLoadedPrep2.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_task_id>',str(newBatchTaskId)).replace('<input_object_id>',str(newInputObjectId))
  inputParameter = inputParameter.replace('<output_object_id>',str(newOutputObjectId))
  cursor.execute(inputParameter)
  while cursor.nextset():
    x = 1 
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to update data in tbl_object_dates_availability')
except Exception as e:
  errorMessage = "Exception occurred while executing script to update data in tbl_object_dates_availability: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 2-Check for the updation of output_datetime_from and output_datetime_to
#stored procedure should update the output_datetime_from,output_datetime_to column with the given input parameter value when is_raw_object = 0
try:
  #Declare variables
  testCaseScenario = 'Count check of updated record with given parameter values for output_datetime_from and output_datetime_to'
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
 
  #executing stored procedure 
  cursor.execute("exec audit.usp_mark_dates_loaded ?,?,?,?,?",newBatchTaskId,newOutputObjectId,newInputObjectId,newOutputDatetimeFrom,newOutputDatetimeTo)
  while cursor.nextset():
      x = 1
  
  #reading stored procedure result as pandas dataframe
  spOutput = pd.read_sql_query("SELECT * FROM audit.tbl_object_dates_availability WHERE batch_task_id = {} AND output_object_id = {} AND input_object_id = {} AND output_datetime_from = '{}' AND output_datetime_to = '{}' AND load_successful = 1".format(newBatchTaskId,newOutputObjectId,newInputObjectId,newOutputDatetimeFrom,newOutputDatetimeTo),conn)
  
  #read count of updated record in tbl_object_dates_availability.
  if spOutput.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
 
  #function call to log the test case status in tbl_unit_test
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario 2 test case for stored procedure usp_mark_dates_loaded')
except Exception as e:
  errorMessage = "Exception occurred while executing scenario 2 test case for stored procedure usp_mark_dates_loaded: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup
try:
  cleanup = open("{}markDatesLoadedCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for usp_mark_dates_loaded')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for usp_mark_dates_loaded: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
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