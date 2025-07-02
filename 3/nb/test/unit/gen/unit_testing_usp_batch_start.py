# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_batch_start</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test of usp_batch_start store procedure </td></tr>
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
# MAGIC     <td>Dev rework, function names, sql db connection, dates
# MAGIC       <br>modified test cases</td>
# MAGIC   </tr>
# MAGIC   <tr>
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

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Import Module
try:
  import pandas as pd
  import numpy as np
  import pyodbc
  import json
  from datetime import datetime, timedelta
  import collections 
except Exception as e:
  assert False 

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try :
  #varible for current_time
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
  testObjectName = 'uspBatchStart'  
  requiredInputParameter = 'scheduleReference'
  sampleOutputLocation = ''

  #Parameters for test case Scenario
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)

except Exception as e:
  errorMessage = 'Exception occured while variable declaration ' + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try: 
  #GET batchTaskId FROM WIDGETS
  dbutils.widgets.text('batchId','')
  batchId=dbutils.widgets.get('batchId')
  
  #GET batchTaskId FROM WIDGETS
  dbutils.widgets.text('batchTaskId','')
  batchTaskId = dbutils.widgets.get('batchTaskId')
  
  #GET sourceId FROM WIDGETS
  dbutils.widgets.text('sourceId','')
  sourceId = dbutils.widgets.get('sourceId')
  
  #GET adfPipelineName FROM WIDGETS
  dbutils.widgets.text('adfPipelineName','')
  adfPipelineName = dbutils.widgets.get('adfPipelineName')
  
  #GET notebookName FROM WIDGETS
  dbutils.widgets.text('notebookName','')
  notebookName = dbutils.widgets.get('notebookName')
  
  #GET clusterId FROM WIDGETS
  dbutils.widgets.text('clusterId','')
  clusterId = dbutils.widgets.get('clusterId')
  
  #CALL THE GET_LOGGING_PATH FUNCTION TO CREATE A LOG FILE PATH AS A STRING AND STORE IT IN A VARIABLE
  errorLogFileLocation=getLoggingPath(batchId,batchTaskId,date, 'error')

except Exception as e:
  errorMessage = 'Exception occured while getting parameters and initiliasing error log location ' + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn=dbutils.secrets.get(scope = 'data-scope-01', key = 'sql-dbrks-connection-01')
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = 'Exception occured while connecting to database: ' + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup 
#script for  metadata cleanup
try: 
  metadatacleanup = open("{}uspBatchStartCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed cleanup scripts successfully')     
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False
  

# COMMAND ----------

# DBTITLE 1,Get the cluster information
#retrieve current cluster information
notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
currentClusterId = notebook_info["tags"]["clusterId"]
workspaceURI = notebook_info["extraContext"]["api_url"]

# COMMAND ----------

# DBTITLE 1,Input Parameter for Store procedure
try:  
  #Parameter for execution of store procedure 
  #get the scheduleReference for the batch_id which is running
  scriptScheduleReference = open("{}uspBatchStartDataprep.sql".format(testInputPath), "r").read()
  scriptScheduleReference = scriptScheduleReference.replace('<cluster_id>' , str(currentClusterId)).replace('<workspaceURI>' , str(workspaceURI))
  
  scheduleReferenceDf = pd.read_sql_query(scriptScheduleReference,conn).iloc[0]
  scheduleReference = str(scheduleReferenceDf[0])
  #print(scheduleReference)
  logTaskProgress(cursor,batchTaskId,' Input Parameter for Store procedure declared successfully')
except Exception as e:
  errorMessage = 'Exception occured while declaring Input Parameter for Store procedure  : ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 1 check data in tbl_batch table based on schedule reference
#check store procedure is insert the data or not in tbl_batch table
try :
  testCaseScenario = 'check the insertion data in tbl_batch table'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  
  # execute the stored procedure
  spOutputBatchId = pd.read_sql_query("exec audit.usp_batch_start {}".format(scheduleReference),conn)
  #get batch_id
  spOutBatchId = int(spOutputBatchId.at[0,'batch_id'])
  #get batch id based on schedule reference
  spBatchId = pd.read_sql_query("SELECT batch_id FROM audit.tbl_batch ba JOIN config.tbl_schedule sc ON ba.schedule_id = sc.schedule_id WHERE sc.schedule_reference = '{}'".format(scheduleReference),conn)
  if spBatchId.shape[0] == 1:
    testResult = 'success'
    executionOutputStatus = 'As Expected'

 #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)

  logTaskProgress(cursor,batchTaskId,'Successful executed scenario1 test case for stored procedure usp_batch_start ')          
except Exception as e:
    errorMessage = 'Exception occured while executing scenario1 test case for stored procedure usp_batch_start : ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Scenario 2 All the tasks for the schedule are added to tbl_batch_task table
#select the list of task for the schedule id from tbl_task table
#get the list of tasks that were added tbl_batch_task by procedure
#both should be equal

try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check all the tasks for the schedule are added to tbl_batch_task table'
  
  #select the list of task from tbl_task table 
  listOfTask = pd.read_sql_query("SELECT task_id FROM config.tbl_schedule_phase_entity_map map JOIN config.tbl_task ta ON ta.entity_id = map.entity_id AND ta.phase_id  = map.phase_id WHERE map.schedule_id = (SELECT schedule_id FROM config.tbl_schedule WHERE schedule_reference = '{}')".format(scheduleReference),conn)
  #get the list of tasks that were added tbl_batch_task by procedure
  spListOfTask =  pd.read_sql_query("SELECT task_id  FROM audit.tbl_batch_task WHERE batch_id ={}".format(spOutBatchId),conn)
  
  #Compare the two list have the same task values
  listOfTask.sort_values(by='task_id', inplace=True)
  spListOfTask.sort_values(by='task_id', inplace=True)
  
  #compare the values using numpy array equals. This is values only.
  if np.array_equal(listOfTask.values,spListOfTask.values):
    executionOutputStatus='As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario2 test case for stored procedure usp_batch_start') 
except Exception as e:
  errorMessage = 'Exception occured while executing scenario2 test case for stored procedure usp_batch_start: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,scenario 3 Check the cluster_id is assigned for all the Task
# check to see if every task has a cluster_id
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check to see if every task has a cluster_id'
  
  #Get the list of task_id from tbl_batch_task where cluster_id is not null 
  listOfTaskAndCluster = spark.createDataFrame(pd.read_sql_query('SELECT DISTINCT task_id FROM audit.tbl_batch_task WHERE batch_id = {} and cluster_id IS NOT NULL '.format(spOutBatchId),conn))
  
  #compare against the full list of task_id where databricks_notebook_id is not null
  #we are checking on databricks_notebook_id because if there is no notebook assigned then there shouldn't be a cluster
  listOfTask =  spark.createDataFrame(pd.read_sql_query('SELECT DISTINCT task_id as all_task_id FROM config.tbl_task WHERE databricks_notebook_id IS NOT NULL',conn))
  
  #Left join listOfTaskAndCluster with listOfTask to check if task_id is in all_task_id
  joinListofTask = (listOfTaskAndCluster
                    .join(listOfTask 
                        , listOfTaskAndCluster.task_id == listOfTask.all_task_id
                        , how = 'left')
                    .where('all_task_id IS NULL')
                    .count()
                   )
  if joinListofTask == 0:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario3 test case for stored procedure usp_batch_start') 
except Exception as e:
  errorMessage = 'Exception occured while executing scenario3 test case for stored procedure usp_batch_start: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup 
#script for  metadata cleanup
try: 
  metadatacleanup = open("{}uspBatchStartCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed cleanup scripts successfully')     
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False
  

# COMMAND ----------

# DBTITLE 1,Close Database connection
#call taskEndAndCloseConn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows,batchTaskResult,
                            batchTaskResultLocation,
                            adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False