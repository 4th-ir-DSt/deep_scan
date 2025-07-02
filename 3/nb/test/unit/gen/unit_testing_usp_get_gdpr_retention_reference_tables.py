# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_get_gdpr_retention_reference_tables</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test on stored procedure usp_get_gdpr_retention_reference_tables</tr>
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
  #varible for current_time
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
  
  #parameter for logError
  errorMessage = ''
  adfPipelineName = ''
  clusterId = ''
  notebookName = ''
  batchId = -1
  batchTaskId = -1
  
  #parameter for logTaskEnd
  batchTaskSourceRows = ''
  batchTaskRowsLoaded = ''
  batchTaskRejectRows = '' 
  batchTaskResultLocation = ''
  batchTaskResult = '' 
  
  #Parameter for logging into tbl_unit_test_result  
  testObject = 'stored procedure'
  testObjectName = 'uspGetGdprRetentionReferenceTables'  
  requiredInputParameter = 'sourceId'
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
  #get batchId from widgets
  dbutils.widgets.text("batchId","")
  batchId = dbutils.widgets.get("batchId")
  
  #get adfPipelineName from widgets
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName = dbutils.widgets.get("adfPipelineName")
  
  #get clusterId from widgets
  dbutils.widgets.text("clusterId","")
  clusterId = dbutils.widgets.get("clusterId")
  
  #get notebookName from widgets
  dbutils.widgets.text("notebookName","")
  notebookName = dbutils.widgets.get("notebookName")
  
  #get sourceId from widgets
  dbutils.widgets.text("sourceId","")
  sourceId = dbutils.widgets.get("sourceId")
  
  #get batchTaskId from widgets
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")
 
  #Call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occurred while getting parameters and initialise errorLogFileLocation " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
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
  cleanup = open("{}uspGetGdprRetentionReferenceTablesCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'successfully executed cleanup script for uspGetGdprRetentionReferenceTables')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for uspGetGdprRetentionReferenceTables': " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Data preparation for testing
#script that generate inputs for the stored procedure
try:
  inputParameter = open("{}uspGetGdprRetentionReferenceTables.sql".format(testInputPath), "r").read()
  inputParameterResults = pd.read_sql_query(inputParameter,conn)

  #fetch the input parameter for the stored procedure
  sourceId = inputParameterResults.at[0,'source_id']
   
  #get the values for comparison
  objectName = inputParameterResults.at[0,'object_name']
  whereClauseAdditions = inputParameterResults.at[0,'where_clause_additions']
  retentionDurationDays = str(inputParameterResults.at[0,'retention_duration_days'])
  
  logTaskProgress(cursor,batchTaskId,'Successfully execution of script to generate input parameter for uspGetGdprRetentionReferenceTables')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter for uspGetGdprRetentionReferenceTables: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-1 Compare the result fo stored procedure with the given value
#the usp_get_gdpr_retention_reference_tables gets source_id as input and returns the active object's name and where_clause
#the test checks the output value of stored procedure(active object name and where clause)

try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'compare the stored procedure result with the given value' 
  
  #execute the stored procedure
  spOutput = pd.read_sql_query("exec config.usp_get_gdpr_retention_reference_tables {}".format(sourceId),conn)
  
  #get the stored procedure result as variables to compare
  spOutputObject = spOutput.at[0,'object_name']
  spOutputWhereClause = spOutput.at[0,'where_clause']
  
  #construct the where clause to compare the output of stored procedure
  constructedWhereClause = whereClauseAdditions+retentionDurationDays
  
  #check the count of spOutput,when the stored procedure output is as expected then the shape of the dataframe is equal to 1
  if spOutput.shape[0] == 1 and spOutputObject == objectName and spOutputWhereClause == constructedWhereClause:          
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to log the test_case_status
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)    
  
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario 1 test case for stored procedure uspGetGdprRetentionReferenceTables')
except Exception as e:
  errorMessage = "Exception occurred while executing scenario 1 test case for stored procedure uspGetGdprRetentionReferenceTables: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Data Preparation -Update scripts
#run scripts to update the object as inactive
try:
  update = open("{}uspGetGdprRetentionReferenceUpdate.sql".format(testInputPath), "r").read()
  cursor.execute(update)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'successfully executed update script for uspGetGdprRetentionReferenceTables')
except Exception as e:
  errorMessage = "Exception occurred while executing update script for uspGetGdprRetentionReferenceTables': " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-2 Negative test case-check the stored procedure result for inactive object
#the stored procedure has the filter that returns only the active object.
#the object is updated as inactive. Hence the stored procedure result will be empty.
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'check the stored procedure result as empty for invalid object' 
  
  #execute the stored procedure
  spOutput = pd.read_sql_query("exec config.usp_get_gdpr_retention_reference_tables {}".format(sourceId),conn)
  
  #check the count of rows in the output dataframe,it should return empty rows as we have marked the object as inactive
  if spOutput.shape[0] == 0:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function to log the test_case_status
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                    testCaseScenario,executionOutputStatus,sampleOutputLocation,
                    datetime.now(),testCaseStatus)    
  
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario 2 test case for stored procedure uspGetGdprRetentionReferenceTables')
except Exception as e:
  errorMessage = "Exception occurred while executing scenario 2 test case for stored procedure uspGetGdprRetentionReferenceTables: " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}uspGetGdprRetentionReferenceTablesCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'successfully executed cleanup script for uspGetGdprRetentionReferenceTables')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for uspGetGdprRetentionReferenceTables': " + str(e)
  logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Close database connection
# call taskEndAndCloseConn function to close the database connection and mark end of task in batch_task_table
try:
   taskEndAndCloseConn(cursor,conn,batchTaskId 
                      ,batchTaskSourceRows,batchTaskRowsLoaded
                      ,batchTaskRejectRows,batchTaskResult,batchTaskResultLocation
                      ,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False