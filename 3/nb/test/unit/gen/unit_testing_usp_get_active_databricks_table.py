# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_get_active_databricks_table</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test the usp_get_active_databricks_table store procedure </td></tr>
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
# MAGIC     <td> code changes made for new test case scenarios 
# MAGIC       <br>Dev rework, function names, sql db connection, dates</td>
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
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
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
  CreatedDate = int(currentTs.strftime('%Y%m%d'))
  #get the hour
  CreatedHour = currentTs.hour
  CreatedTimestamp = currentTs
  LastUpdatedTimestamp = currentTs
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
  testObjectName = 'uspGetActiveDatabricksTable'  
  requiredInputParameter = ''
  sampleOutputLocation = ''
  executionDateTime = currentTs
  
  #Parameters for test case Scenario
  testInputPath   = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath  = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
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
  errorLogFileLocation=getLoggingPath(batchId,batchTaskId,date, 'error')

except Exception as e:
  errorMessage="Exception occured while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn=dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage="Exception occured while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Cleanup data from previous TaskEnd unit testing
#run cleanup script
try:
  cleanup = open("{}getActiveDatabricksTableUnitTestCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts for usp_get_active_databricks_table')   
except Exception as e:
  errorMessage="Exception occured while execcuting clean up scripts for usp_get_active_databricks_table: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Input Parameter for Store procedure
try:
  #Parameter for store procedure test case
  objectBaseName = 'getActiveDatabricksTableUnitTest'
  objectAName = objectBaseName + '_a'
  logTaskProgress(cursor,batchTaskId,' Input Parameter for Store procedure declared successfully')
except Exception as e:
  errorMessage="Exception occured while declaring Input Parameter for Store procedure  : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 1 - Data preparation
# scripts to insert an object into the tbl_object and tbl_object_definition where active flag is 0 
try:
  insertScript = open("{}getActiveDatabricksTableUnitTestScenario1.sql".format(testInputPath), "r").read()
  cursor.execute(insertScript)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed insertScript scripts successfully for unit_testing_usp_get_active_databricks_table')     
except Exception as e:
  errorMessage = "Exception occured while executing insertScript scripts for unit_testing_usp_get_active_databricks_table: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 1 - Procedure should not return the object if is_active status is 0
# step 1 insert an object into the tbl_object and tbl_object_definition where active flag is 0 
# step 2 store procedure should not return the object newly inserted
try :
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  testCaseScenario = 'Result should not return the object if is_active status is 0'
  
  #execute the procedure for getting the active table 
  activeTable = pd.read_sql_query("exec [config].[usp_get_active_databricks_tables]", conn).astype(str)
  activeTableDf = activeTable[activeTable['object_name']== objectAName]

  if activeTableDf.shape[0] == 0:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario1 test case for stored procedure sp_get_active_databricks_table')  
except Exception as e:
  errorMessage="Exception occured while executing scenario1 test case for stored procedure usp_get_active_databricks_table: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 2 - only active objects are returned
# step 1 Select the list of object name from tbl_object where is_active =1 and store procedure output
# step 2 the result of store procedure output should be in the list of object name from tbl_object 
try :
  testCaseStatus = 'failed'
  executionOutputStatus = 'Mismatched'
  testCaseScenario = 'only active objects are returned'
  
  #Select the list of object name from tbl_object where is_active =1
  activeObjects=spark.createDataFrame(pd.read_sql_query("SELECT object_id , object_name AS active_object_name from config.tbl_object where is_active = 1",conn))

  #execute the procedure for getting the active table 
  activeTableResult = spark.createDataFrame(pd.read_sql_query("exec [config].[usp_get_active_databricks_tables]", conn).astype(str))

  #join the activeTableResult and left join it with list of active table in tbl_object
  activeTableResultCount = (activeTableResult
                          .join(activeObjects
                              , activeTableResult.object_name == activeObjects.active_object_name
                              , how='left')
                          .where("active_object_name IS NULL")
                          .count()
                         )


  if activeTableResultCount == 0:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successfully executed scenario2 test case for stored procedure usp_get_active_databricks_table')  
except Exception as e:
  errorMessage="Exception occured while executing scenario2 test case for stored procedure usp_get_active_databricks_table  : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}getActiveDatabricksTableUnitTestCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts for usp_get_active_databricks_table')   
except Exception as e:
  errorMessage="Exception occured while execcuting clean up scripts for usp_get_active_databricks_table: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False    

# COMMAND ----------

# DBTITLE 1,Close database connection
# call taskEndAndCloseConn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,
                            adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False