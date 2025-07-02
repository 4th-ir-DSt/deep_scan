# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_get_tasks</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test on stored procedure usp_get_tasks</td></tr>
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
# MAGIC     <td>2019/01/01</td>
# MAGIC     <td>Framework</td>
# MAGIC     <td>Dev rework changes for camel case function names, date variables and removal of error line
# MAGIC         <br>Dev-rework added new parameter phase_id and replaced cluster_name with cluster_id
# MAGIC         <br>Unit test tweak-Addition of input parameter 
# MAGIC         <br>Test case modified as per store procedure update 
# MAGIC     </td>
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
  testObjectName = 'uspGetTasks'  
  
  requiredInputParameter = 'batchId,source_type_id,taskPriotity phase_id,processing_group'
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
  cleanup = open("{}uspGetTasksCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed cleanup scripts for unit_testing_usp_get_tasks ')   
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts for unit_testing_usp_get_tasks: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Input parameter for stored procedure
#execute scripts to generate input parameter for stored procedure
try:  
  inputParameter = open("{}uspGetTask.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>',str(batchId))
  
  inputParameterResults = pd.read_sql_query(inputParameter,conn)

        
  #get the parameters required for stored procedure
  newSourceTypeId = int(inputParameterResults.at[0,'source_type_id'])
  newTaskPriority = int(inputParameterResults.at[0,'task_priority'])
  newPhaseId = int(inputParameterResults.at[0,'phase_id'])
  newProcessingGroup = int(inputParameterResults.at[0,'processing_group'])

  logTaskProgress(cursor,batchTaskId,'Successfully executed script to generate input parameter for usp_get_tasks')  
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter for usp_get_tasks " + str(e)
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
      spOutput = pd.read_sql_query("exec config.usp_get_tasks {},{},{},{},{}".format(jBatchId[i],jSourceTypeId[i],jTaskPriority[i],jPhaseId[i],jProcessingGroup[i]),conn)
    except Exception as e:
      executionOutputStatus = 'As expected'
      testCaseStatus = 'success'
      
      specificParameterTestScenarion = testCaseScenario + "which are, batch_id:{} and source_type_id:{} and TaskPriority :{} and phaseId :{}".format(str(jBatchId[i]), str(jSourceTypeId[i]),str(jTaskPriority[i]),str(jPhaseId[i]))
      
      #function call to log unit test status
      logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                        specificParameterTestScenarion,executionOutputStatus,sampleOutputLocation,
                        datetime.now(),testCaseStatus)
      logTaskProgress(cursor,batchTaskId,'successfully performed scenario 1 unit test on stored procedure usp_get_tasks') 
except Exception as e:
  errorMessage = "Exception occurred while testing on scenario 1 for stored procedure usp_get_tasks': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False     

# COMMAND ----------

# DBTITLE 1,Scenario 2- Check the count of stored procedure output
#the stored procedure should return output based on join conditions.
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check the count of returned rows by stored procedure'
  
  #read stored procedure result as pandas dataframe
  spOutput = pd.read_sql_query("exec config.usp_get_tasks {},{},{},{},{}".format(batchId,newSourceTypeId,newTaskPriority,newPhaseId,newProcessingGroup),conn)
   
  #compare the count of rows returned by stored procedure
  if spOutput.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'  
  
  #function call to log unit test status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'successfully performed scenario 2 unit test on stored procedure usp_get_tasks')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 2 stored procedure usp_get_tasks : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 3 - Compare the given value with returned result
#check the return value of the stored procedure result with the given value.
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check returned value stored procedure'
  
  #Check if the return value are correct 
  clusterDf = spOutput[spOutput['cluster_id'] == "GetTasksTestcluster"]
  notebookDf = spOutput[spOutput['databricks_notebook_name'] == "GetTasksTestdatabricksNotebookName"] 
  
  #compare the count of returned result matches with the given values
  if clusterDf.shape[0] == 1 and notebookDf.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'    
  
  #function call to log unit test status   
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'successfully performed scenario 3 unit test on stored procedure usp_get_tasks')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 stored procedure usp_get_tasks : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}uspGetTasksCleanUp.sql".format(testInputPath), "r").read()
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