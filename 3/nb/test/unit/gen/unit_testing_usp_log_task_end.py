# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_test_usp_log_task_end</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test the usp_log_task store procedure </td></tr>
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
# MAGIC     <td>NotebookName</td>
# MAGIC     <td>@NotebookName to retrieve notebookname </td>
# MAGIC     <td>@NotebookName = 'testnotebook'</td>
# MAGIC   </tr>
# MAGIC     <tr>
# MAGIC     <td>clusterId</td>
# MAGIC     <td>@clusterId to retrieve cluster details </td>
# MAGIC     <td>@clusterId = '12345'</td>
# MAGIC   </tr>
# MAGIC    <tr>
# MAGIC     <td>sourceID</td>
# MAGIC     <td>@sourceID to retrieve source details </td>
# MAGIC     <td>@sourceID = '1'</td>
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
# MAGIC     <td>14/02/2020</td>
# MAGIC     <td>Framework</td>
# MAGIC     <td>Test case Scenario changed 
# MAGIC         <br>Dev rework changes for camel case function names, date variables and removal of error line</td>
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
  from pyspark.sql.types import StructType,StructField,StringType
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
  testObjectName = 'uspLogTaskEnd'  
  requiredInputParameter = 'batchTaskId,batch_task_status,batch_task_source_rows,batch_task_rows_loaded,batch_task_reject_rows,batch_task_result,    batch_task_result_location'
  testCaseScenario = ''
  executionOutputStatus = ''
  testCaseStatus = ''
  sampleOutputLocation = ''

  #Parameters for test case Scenario
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)

except Exception as e:
  errorMessage="Exception occured while variable declaration " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try: 
  
  #GET batchTaskId FROM WIDGETS
  dbutils.widgets.text("batchId","")
  batchId=dbutils.widgets.get("batchId")
  
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
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occured while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn=dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = 'Exception occured while connecting to database: ' + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Input Parameter for Store procedure
try:  
  #Parameter for execution of store procedure 
  batchTaskRejectRows = '1'
  batchTaskResult = '2'
  batchTaskResultLocation = '/mnt/temp'
  batchTaskRowsLoaded = '10'
  batchTaskSourceRows = '13'
  batchTaskStatus = 'completed'
  logTaskProgress(cursor,batchTaskId,' Input Parameter for Store procedure declared successfully')
except Exception as e:
  errorMessage = 'Exception occured while declaring Input Parameter for Store procedure  : ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Cleanup data from previous TaskEnd unit testing
#update the batch_task_status with pending for the batchTaskId
try:
  cleanup = open("{}uspLogTaskEndCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage = 'Exception occured while execcuting clean up scripts: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Prechecks before stored procedure execution 
# Update the batch_task_status to running in tbl_batch_task table to check if procedure in Scenario1
try:
  initialScripts = open("{}uspLogTaskEndUpdateStatusRunning.sql".format(testInputPath), "r").read()
  initialScripts = initialScripts.replace('<batch_id>' , str(batchId))
  
  #execute the prep1 script and get the first row
  initialScriptsResutls = pd.read_sql_query(initialScripts,conn).iloc[0]
  
  #get the first value of the row and cast as int
  newBatchTaskID = int(initialScriptsResutls[0])
  
  logTaskProgress(cursor,batchTaskId,' Executed initial scripts successfully')     
except Exception as e:
  errorMessage = 'Exception occured while executing initial scripts: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  


# COMMAND ----------

# DBTITLE 1,Execution of procedure
try :
  cursor.execute("exec audit.usp_log_task_end  ?,?,?,?,?,?,?",newBatchTaskID,batchTaskStatus,
                                                              batchTaskSourceRows,batchTaskRowsLoaded,
                                                              batchTaskRejectRows,batchTaskResult,
                                                              batchTaskResultLocation)
  while cursor.nextset():
      x = 1
  logTaskProgress(cursor,batchTaskId,' Successfully executed the stored procedure')

except Exception as e:
  errorMessage = 'Exception execution the procedure usp_log_task_end: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 1-Check for the  batch_task_status = 'completed'
#Test case if stored procedure have updated in batch_task_table table as 'completed' for batch_task_status column
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'check on batch_task_table table if status is completed'
  
#   After successful exegution of Store procedure check if count of record with status completed if 1 is returned then confirms the success
  spUpdateOut = pd.read_sql_query("select * from audit.tbl_batch_task where batch_task_id = {} and batch_task_status = 'completed'".format(newBatchTaskID),conn)
  #compare the count of dataframe if 1 then execute stored procedure as success else with failed status
  if spUpdateOut.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario1 test case for stored procedure usp_log_task_end')  
except Exception as e:
  errorMessage = 'Exception occured while executing scenario1 test case for stored procedure usp_log_task_end: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#update the batch_task_status with pending for the batchTaskId
try:
  cleanup = open("{}uspLogTaskEndCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage = 'Exception occured while execcuting clean up scripts: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Close Database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False