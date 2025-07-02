# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_get_downstream_objects</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test on stored procedure get_downstream_objects</td></tr>
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
# MAGIC     <td>Dev rework, function names, sql db connection, dates</td>
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

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Import Module
try:
  import os
  import pandas as pd
  import pyodbc
  from datetime import datetime, timedelta
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
  from pyspark.sql.functions import col
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
  
  
  #PARAMETER FOR LOG_ERRO
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
  testCaseScenario = ''
  sampleOutputLocation = ''
    
  #Parameter and paths
  testObject = 'stored procedure'
  testObjectName = 'UspGetDownstreamObjects'
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  requiredInputParameter = 'batchTaskId,errorLine,errorMessage,adfPipelineName,clusterID,notebookName'
  errorLogFileLocation=getLoggingPath(batchId,batchTaskId,date,'error')
  executionDateTime = currentTs

except Exception as e:
  errorMessage="Exception occured while variable declaration " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try:
  #GET batchId FROM WIDGETS
  dbutils.widgets.text("batchId","")
  batchId=dbutils.widgets.get("batchId")
  
   #GET sourceId FROM WIDGETS
  dbutils.widgets.text("sourceId","")
  sourceId = dbutils.widgets.get("sourceId")
  
  #GET batchTaskId FROM WIDGETS
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")
  
    #GET adfPipelineName FROM WIDGETS
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName=dbutils.widgets.get("adfPipelineName")
  
   #GET notebookName FROM WIDGETS
  dbutils.widgets.text("notebookName","")
  notebookName = dbutils.widgets.get("notebookName")
  
  #GET clusterId FROM WIDGETS
  dbutils.widgets.text("clusterId","")
  clusterId = dbutils.widgets.get("clusterId")

  #CALL THE GET_LOGGING_PATH FUNCTION TO CREATE A LOG FILE PATH AS A STRING AND STORE IT IN A VARIABLE
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')

except Exception as e:
  errorMessage = "Exception occured while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = "Exception occured while connecting to database: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 1 - data preparation to test no downstream objects that no objects are returned
try:
  #run prep 1 script to prepare data for scenario 1 test
  dataPrepScenario1 = open(testInputPath + "GetDownstreamObjectsPrep1.sql", "r").read()
  cursor.execute(dataPrepScenario1)
  while cursor.nextset():
    x = 1
  #Log into log file   
  logTaskProgress(cursor,batchTaskId,'Successful prepared the data for get_downstream_objects scenario-1')
except Exception as e:
  errorMessage = "Exception occured while preparing the data for get_downstream_objects scenario-1: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  
  

# COMMAND ----------

# DBTITLE 1,Prepare object name variables
try:
  objectBaseName = 'GetDownstreamUnitTestObject'
  objectAName = objectBaseName + '_a'
  objectBName = objectBaseName + '_b'
  objectCName = objectBaseName + '_c'
  objectDName = objectBaseName + '_d'
  objectEName = objectBaseName + '_e'
except Exception as e:
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 1 - object has no downstream objects that no objects are returned
try:
  testCaseScenario ='Test object has no downstream objects that no objects are returned'
  outputLocation = ''
  
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  #execute the procedure for staging table
  downStreamObjectDf = pd.read_sql_query("exec [config].[usp_get_downstream_objects] {}".format(objectAName), conn)

  downStreamObjectCount = downStreamObjectDf.shape[0]
  #expect that the count is 0 as there are no downstream objects
  if downStreamObjectCount == 0:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
   
    #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested object has no downstream objects that no objects are returned scenario')
except Exception as e:
    errorMessage="Exception occured while object has no downstream objects that no objects are returned scenario: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  
  

# COMMAND ----------

# DBTITLE 1,Scenario 2 - data preparation to test two downstream object then both of the objects returned
try:
  #run prep 2 script to prepare data for scenario 2 test
  dataPrepScenario2 = open(testInputPath + "GetDownstreamObjectsPrep2.sql", "r").read()
  cursor.execute(dataPrepScenario2)
  while cursor.nextset():
    x = 1
  #Log into log file   
  logTaskProgress(cursor,batchTaskId,'Successful prepared the data for get_downstream_objects scenario-2')
except Exception as e:
  errorMessage = "Exception occured while preparing the data for get_downstream_objects scenario-2: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 2 - object has two downstream object then both of the objects returned
try:
  testCaseScenario ='Test object has two downstream object then both of the objects returned'
  outputLocation = ''
  
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  #execute the procedure for staging table
  downStreamObjectDf = pd.read_sql_query("exec [config].[usp_get_downstream_objects] {}".format(objectAName), conn)
  downstreamObjectCount = downStreamObjectDf.shape[0]
  #expect that the count is 2 as there are two downstream objects
  if downstreamObjectCount == 2:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
   
    #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested object has two downstream object then both of the objects returned')
except Exception as e:
    errorMessage="Exception occured while object has two downstream object then both of the objects returned: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario 3 - data preparation to test only objects from refined / sensitive / person are returned
try:
  #run prep 3 script to prepare data for scenario 3 test
  dataPrepScenario3 = open(testInputPath + "GetDownstreamObjectsPrep3.sql", "r").read()
  cursor.execute(dataPrepScenario3)
  while cursor.nextset():
    x = 1
  #Log into log file   
  logTaskProgress(cursor,batchTaskId,'Successful prepared the data for get_downstream_objects scenario-3')
except Exception as e:
  errorMessage = "Exception occured while preparing the data for get_downstream_objects scenario-3: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario 3 - only objects from refined / sensitive / person are returned
try:
  testCaseScenario ='Test only objects from refined / sensitive / person are returned'
  outputLocation = ''
  
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  #query the database to return all objects from tbl_object where is_active is true and zone is in (Refined, sensitive or persons)
  scenario3AllActiveObjectsRefSenPer = spark.createDataFrame(pd.read_sql_query("SELECT object_id AS active_object_id from config.tbl_object where is_active = 1 and zone in ('REFINED', 'SENSITIVE', 'PERSONS')",conn))

  #get downstream objects for objectA
  scenario3Df = spark.createDataFrame(pd.read_sql_query("exec [config].[usp_get_downstream_objects] {}".format(objectAName), conn))

  #join the scenario3DF and left join it to all the refined, sensitive, persons active obejcts and see if there are any that shouldn't be returned
  scenario3ResultCount = (scenario3Df
                          .join(scenario3AllActiveObjectsRefSenPer
                                , scenario3Df.object_id == scenario3AllActiveObjectsRefSenPer.active_object_id
                                , how='left')
                          .where("active_object_id IS NULL")
                          .count()
                         )

  if scenario3ResultCount == 0:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
   
    #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested objects from refined / sensitive / person are returned')
except Exception as e:
    errorMessage="Exception occured while only objects from refined / sensitive / person are returned: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario 4 - Data preparation to test only active objects are returned
try:
  #run prep 4 script to prepare data for scenario 4 test
  dataPrepScenario4 = open(testInputPath + "GetDownstreamObjectsPrep4.sql", "r").read()
  cursor.execute(dataPrepScenario4)
  while cursor.nextset():
    x = 1
  #Log into log file   
  logTaskProgress(cursor,batchTaskId,'Successful prepared the data for get_downstream_objects scenario-4')
except Exception as e:
  errorMessage = "Exception occured while preparing the data for get_downstream_objects scenario-4: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario 4 - only active objects are returned
try:
  testCaseScenario ='Test only active objects are returned'
  outputLocation = ''
  
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

  # query the database to return all objects from tbl_object where is_active is true.
  scenario4AllActiveObjects=spark.createDataFrame(pd.read_sql_query("SELECT object_id AS active_object_id, object_name from config.tbl_object where is_active = 1",conn))

  #get downstream objects for objectA
  scenario4Df = spark.createDataFrame(pd.read_sql_query("exec [config].[usp_get_downstream_objects] {}".format(objectAName), conn))

  #join the scenario3DF and left join it to all the refined, sensitive, persons active obejcts and see if there are any that shouldn't be returned
  scenario4ResultCount = (scenario4Df
                          .join(scenario4AllActiveObjects
                                , scenario4Df.object_id == scenario4AllActiveObjects.active_object_id
                                , how='left')
                          .where("active_object_id IS NULL")
                          .count()
                         )

  if scenario4ResultCount == 0:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
   
    #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested only active objects are returned')
except Exception as e:
    errorMessage="Exception occured while only active objects are returned: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
try:
  #run cleanup script
  cleanupMetadata = open(testInputPath + "GetDownstreamObjectsCleanup.sql", "r").read()
  cursor.execute(cleanupMetadata)
  while cursor.nextset():
    x = 1
  #Log into log file   
  logTaskProgress(cursor,batchTaskId,'Successfully performed metadata cleanup')
except Exception as e:
  errorMessage = "Exception occured while performing metadata cleanup: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,close database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId , batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,
                            adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False