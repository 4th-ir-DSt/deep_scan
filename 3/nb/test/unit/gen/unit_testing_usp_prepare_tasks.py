# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_prepare_tasks</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test on stored procedure usp_prepare_tasks</td></tr>
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
# MAGIC ## Change Log
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Date</th>
# MAGIC     <th>Changed By</th>
# MAGIC     <th>Change Description</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td> </td>
# MAGIC     <td> </td>
# MAGIC     <td> </td>
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

# DBTITLE 1,Import Module
try:
  import pandas as pd
  import pyodbc
  from datetime import datetime, timedelta
except Exception as e:
  assert False

# COMMAND ----------

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try :
  #VARIBLE FOR CURRENT_TIME
  currentTs = datetime.now()
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
  date = currentTs
  
  #parameter for log_error
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
  testObjectName = 'uspPrepareTasks'  
  requiredInputParameter = 'batch_id,phase_id,source_type_id,task_priority,total_processing_groups'
  testObject = 'storedProcedure'
  sampleOutputLocation = ''
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
except Exception as e:
  errorMessage = "Exception occurred while variable declaration " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try:
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
   
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")
 
  #Call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occurred while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = "Exception occurred while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Perform cleanup from previous runs
#run cleanup script
try:
  cleanup = open("{}uspPrepareTasksCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed cleanup scripts for unit_testing_usp_prepare_tasks')   
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts for unit_testing_usp_prepare_task: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Input parameter for stored procedure
#execute scripts to generate input parameter for stored procedure
try:  
  inputParameter = open("{}uspPrepareTaskDataPrepration.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>',str(batchId))
  
  inputParameterResults = pd.read_sql_query(inputParameter,conn)
  
  #get the parameters required for stored procedure
  newSourceTypeId = int(inputParameterResults.at[0,'source_type_id'])
  newTaskPriority = int(inputParameterResults.at[0,'task_priority'])
  newPhaseId = int(inputParameterResults.at[0,'phase_id'])
  newProcessingGroup = int(inputParameterResults.at[0,'processing_group'])
  
  #parameter for sql scripts
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])

  logTaskProgress(cursor,batchTaskId,'Successfully executed script to generate input parameter for usp_prepare_tasks')  
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter for usp_prepare_tasks " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario 1- Check for invalid input parameter 
#stored procedure should raise error for invalid parameters
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test case for invalid input parameters'
  #Random invalid values as parameters
  jBatchId = ['','l1']
  jSourceTypeId = [234.55,2.2] 
  jTaskPriority = ['s','']
  jPhaseId = ['s','']
  jProcessingGroup = ['9' ,'a']
  
  for i in range (0,2):
    try:
      #run stored procedure for different parameter values which throws error for invaild input
      spOutput = pd.read_sql_query("exec config.usp_prepare_tasks {},{},{},{},{}".format(jBatchId[i],jPhaseId[i],jSourceTypeId[i],jTaskPriority[i],jProcessingGroup[i]),conn)
      
    except Exception as e:
      executionOutputStatus = 'As expected'
      testCaseStatus = 'success'
      
      specificParameterTestScenarion = testCaseScenario + "which are, batch_id:{} and source_type_id:{} and TaskPriority :{} and phaseId :{} and ProcessingGroup : {}".format(str(jBatchId[i]), str(jSourceTypeId[i]),str(jTaskPriority[i]),str(jPhaseId[i]),str(jProcessingGroup[i]))
      
      #function call to log unit test status
      logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                        specificParameterTestScenarion,executionOutputStatus,sampleOutputLocation,
                        datetime.now(),testCaseStatus)
      logTaskProgress(cursor,batchTaskId,'successfully performed scenario 1 unit test on stored procedure usp_prepare_tasks') 
except Exception as e:
  errorMessage = "Exception occurred while testing on scenario 1 for stored procedure usp_prepare_tasks': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False     

# COMMAND ----------

# DBTITLE 1,Scenario - 2 Check the updates in tbl_batch_task table
#check tbl_batch_task table is update with the cluster_id ,databricks_workspace_uri,processing_group
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check for the updation of cluster_id ,databricks_workspace_uri,processing_group in batch_task table'
  
  #execute stored procedure
  cursor.execute("exec config.usp_prepare_tasks  ?,?,?,?,?",batchId,newPhaseId,newSourceTypeId,newTaskPriority,newProcessingGroup)
  while cursor.nextset():
      x = 1
      
  #check the update for cluster_id ,databricks_workspace_uri,processing_group in batch_task table
  spUpdateOutputResult = pd.read_sql_query("SELECT * FROM audit.tbl_batch_task WHERE batch_task_id ={}".format(newBatchTaskId),conn)
 
  #Check if the return value are correct 
  clusterDf = spUpdateOutputResult[spUpdateOutputResult['cluster_id'] == "TestprepareTask"]
  workspaceUrlDf = spUpdateOutputResult[spUpdateOutputResult['databricks_workspace_uri'] == "TestprepareTaskworkspaceURI"] 
  processingGroupDf = spUpdateOutputResult[spUpdateOutputResult['processing_group'] == 1] 
  
  #compare the count of rows returned for test case
  if clusterDf.shape[0] == 1 and workspaceUrlDf.shape[0] == 1 and processingGroupDf.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'  
  
  #function call to log unit test status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'successfully performed scenario 2 unit test on stored procedure usp_prepare_tasks')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 2 stored procedure usp_prepare_tasks : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario - 3 Data Preparation
#run scripts to update object_dates_availability table.
try:  
  updateScript = open("{}uspPrepareTaskUpdate.sql".format(testInputPath), "r").read()
  updateScript = updateScript.replace('<batch_task_id>',str(newBatchTaskId))
  
  cursor.execute(updateScript)
  while cursor.nextset():
    x = 1
  
  logTaskProgress(cursor,batchTaskId,'Successfully executed data prepration scripts for usp_prepare_tasks')  
except Exception as e:
  errorMessage = 'Exception occurred while executing data prepration scripts for usp_prepare_tasks'  + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario - 3 Check updation of status in BatchTask & insertion of message in BatchTaskProgress tables 
#check batch_task_status in batch_task table which should be marked as 'completed' when there are no data to load.Also,the stored procedure inserts message 'batch task not required - no data to load' in batch_task_progress table.

try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check for the insertion of message in batch_task_progress table and updation of status in batch_task table'
  
  #execute stored procedure
  cursor.execute("exec config.usp_prepare_tasks  ?,?,?,?,?",batchId,newPhaseId,newSourceTypeId,newTaskPriority,newProcessingGroup)
  while cursor.nextset():
      x = 1
  
  #check the insert of batch_task_progress message as 'batch task not required' for the batch_task_id
  spInsertOutput = pd.read_sql_query("SELECT * FROM audit.tbl_batch_task_progress WHERE batch_task_progress_message = 'batch task not required - no data to load' AND batch_task_id = {}".format(newBatchTaskId),conn)
  
  #check for the update of batch_task_status as completed for next batch_task_id stored for the batch_task_id
  spUpdateOutput = pd.read_sql_query("SELECT * FROM audit.tbl_batch_task WHERE batch_task_status= 'completed' AND batch_task_id ={}".format(newBatchTaskId),conn)
 
  #compare the count of rows returned for test case
  if spInsertOutput.shape[0] == 1 and spUpdateOutput.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'  
  
  #function call to log unit test status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'successfully performed scenario 3 unit test on stored procedure usp_prepare_tasks')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 stored procedure usp_prepare_tasks : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False


# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}uspPrepareTasksCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed cleanup scripts')   
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Close database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                      batchTaskRowsLoaded,batchTaskRejectRows,batchTaskResult,
                      batchTaskResultLocation,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False