# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_get_last_optimise</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test on stored procedure usp_get_last_optimise</td></tr>
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
  testObjectName = 'uspGetLastOptimise'  
  requiredInputParameter = 'batchTaskId'
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
  cleanup = open("{}uspGetLastOptimiseCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed cleanup scripts')   
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Data preparation for Scenario - 1
#execute scripts to generate input parameter for stored procedure
try:  
  inputParameter = open("{}uspGetLastOptimiseDataPrepration1.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>',str(batchId))
  
  inputParameterResults = pd.read_sql_query(inputParameter,conn)

  #get the parameters required for stored procedure
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])
  newObjectIdA = int(inputParameterResults.at[0,'object_id_a'])
  newObjectIdB = int(inputParameterResults.at[0,'object_id_b'])
  zorderColumnNames = inputParameterResults.at[0,'zorder_column']

  logTaskProgress(cursor,batchTaskId,'Successfully executed script to generate input parameter for usp_get_last_optimise')  
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter for usp_get_last_optimise " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario 1- Check the new entry in audit.tbl_log_optimise table 
#the stored procedure should make an entry in audit.tbl_log_optimise optimise_state as running.
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check the new entry in audit.tbl_log_optimise table '
  
  #read stored procedure result as pandas dataframe
  spOutput = pd.read_sql_query("exec audit.usp_get_last_optimise {}".format(newBatchTaskId),conn)
  
  #reading stored procedure result as pandas dataframe
  insertResult = pd.read_sql_query("SELECT * FROM audit.tbl_log_optimise WHERE object_id = {} AND optimise_state = 'running'".format(newObjectIdA),conn)

  # compare the count of rows returned by stored procedure
  if insertResult.shape[0] == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'  

  #function call to log unit test status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'successfully performed scenario 1 unit test on stored procedure usp_get_last_optimise')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 1 unit test on stored procedure usp_get_last_optimise : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 2- Check if zorder is in proper order in where_clause
#zorder should be in the proper order in the where clause 
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check if zorder is in proper order in where_clause'

  #Concatenating 'zorder by' to zorderColumnNames(zorderColumnNames is concatenation of object_attribute_name columns where zorder_oder column is not null in ascending order )
  zorderOrder = 'ZORDER BY '+zorderColumnNames

  #Get the zorder form the where_clause of the store procedure result 
  zorder = spOutput.at[0,'where_clause']
  zorderIndex = zorder.index('ZORDER BY')
  zorderStoreProcedure = zorder[zorderIndex:]
  
  #Compare the constructed zorder and store procedure zorder are equal 
  if zorderOrder == zorderStoreProcedure:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'  

  #function call to log unit test status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'successfully performed scenario 2 unit test on stored procedure usp_get_last_optimise')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 2 unit test on stored procedure usp_get_last_optimise : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 3 - Check if where condition is not present
# If the partition_date_time_columns is NULL  then where clause should be empty 
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check if where condition is not present'
  
  #Get the where_clause form the store procedure result 
  whereClause = spOutput.at[0,'where_clause']
  whereClauseIndex = whereClause.find('WHERE')
  
  #if there is no where condition then the index will be -1
  if whereClauseIndex == -1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'  
    
  #function call to log unit test status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'successfully performed scenario 3 unit test on stored procedure usp_get_last_optimise') 
  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 unit test on stored procedure usp_get_last_optimise : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Data preparation for Scenario - 4
#execute scripts to update the object_id_b with the existing batch_task_id
try:  
  inputParameterWithWhere = open("{}uspGetLastOptimiseDataPrepration2.sql".format(testInputPath), "r").read()
  inputParameterWithWhere = inputParameterWithWhere.replace('<batch_task_id>',str(newBatchTaskId))
  inputParameterWithWhere = inputParameterWithWhere.replace('<object_id_b>',str(newObjectIdB))

  cursor.execute(inputParameterWithWhere)
  while cursor.nextset():
    x = 1

  logTaskProgress(cursor,batchTaskId,'Successfully executed script to generate input parameter for usp_get_last_optimise')  
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter for usp_get_last_optimise " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario 4 - Check if where condition is present
# If the partition_date_time_columns is NOT NULL  then where clause should be present  
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check the new entry in audit.tbl_log_optimise table '
  
  #read stored procedure result as pandas dataframe
  spOutputWithWhere = pd.read_sql_query("exec audit.usp_get_last_optimise {}".format(newBatchTaskId),conn)
  
  #Get the where_clause form the store procedure result 
  whereClauseWithWhere = spOutputWithWhere.at[0 ,'where_clause']
  
  #Check if WHERE is present in where_clause 
  whereClauseIndexWithWhere = whereClauseWithWhere.find('WHERE')

  
  if whereClauseIndexWithWhere >= 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'  

  #function call to log unit test status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'successfully performed scenario 4 unit test on stored procedure usp_get_last_optimise')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 4 unit test on stored procedure usp_get_last_optimise : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}uspGetLastOptimiseCleanUp.sql".format(testInputPath), "r").read()
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