# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_load_unstructuredxmlSourcehistory_gen2_sqdb</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test notebook load_unstructuredxmlSourcehistory_gen2_sqdb</td></tr>
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
# MAGIC   
# MAGIC ## Change Log
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Date</th>
# MAGIC     <th>Changed By</th>
# MAGIC     <th>Change Description</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <th>2019/01/01</th>
# MAGIC     <th>Framework</th>
# MAGIC     <th>Changed scenarios 3 and 8 to adapt to historic load, added new scenarios 10 to 12# MAGIC   
# MAGIC     <br>Changed LoadunstructuredxmlSourceHistoryGen2SqdbDataPrep to return the rawLocation. Changed scenarios 3 and 8 to 12 to adapt to historic load changes.</th>
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

# DBTITLE 1,Run logging function notebook
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run Miscellaneous Function notebook
# MAGIC %run ../../../util/spe/misc_functions

# COMMAND ----------

# DBTITLE 1,Import Module
try:
  import os
  import pandas as pd
  import pyodbc
  from datetime import datetime,timedelta
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
  from pyspark.sql.functions import col
  import json
except Exception as e:
  errorMessage = "Exception occurred while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try :
  #variable for current timestamp
  currentTs = datetime.now()
  #get the last 3 positions of the microseconds as we need to remove them
  currentTsMicroseconds = int(str(currentTs.strftime('%f'))[3:6])
  #take away the last three microseconds
  currentTs = currentTs - timedelta(microseconds = currentTsMicroseconds)
  #get the date as an int format
  createdDate = int(currentTs.strftime('%Y%m%d'))
  #get the hour
  createdHour = currentTs.hour
  createdTimestamp = currentTs
  lastUpdatedTimestamp = currentTs
  #use the same time for the date also
  date = currentTs
  

  #PARAMETER FOR logError
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
  testObject = 'notebook'
  testObjectName = 'loadunstructuredxmlSourceHistoryGen2Sqdb'  
  requiredInputParameter = ''
  testCaseScenario = ''
  executionDateTime = datetime.now()
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
  
  #call the getLoggingPath function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occurred while getting parameters and initilising error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish Database Connection
try:
  dbConn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbConn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = "Exception occurred while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Function to insert data into directories to test load_na_gen2_sqdb
#Insert file in required directories for different test conditions    
def testDataLoadPreparation():
  try:
    sampleFile = '/mnt/dataquality/unit_tests/loadunstructuredxmlSourceHistoryGen2Sqdb/input/sampleTestAvroFile.avro'
    dateTestPath = '/mnt/raw/testLoadunstructuredxmlSourceHistoryGen2Sqdb/hist_testLoadunstructuredxmlSourceHistoryGen2Sqdb/input/dateTestFiles/' 
    dateTimeTestPath = '/mnt/raw/testLoadunstructuredxmlSourceHistoryGen2Sqdb/hist_testLoadunstructuredxmlSourceHistoryGen2Sqdb/input/dateTimeTestFiles/' 
    	
    #To test if file is in latest date folder copying data
    dbutils.fs.cp(sampleFile,dateTestPath+'hasFilesInFirstDate/date=20200214')

    #To test if file is in second date directory
    dbutils.fs.cp(sampleFile,dateTestPath+'hasFilesInSecondDate/date=20200213')

    #To test if file is in latest date and latest time
    dbutils.fs.cp(sampleFile,dateTimeTestPath+'hasFilesInFirstDateAndFirstTime/date=20200214/time=181712')

    #To test if file is in latest date and second time
    dbutils.fs.cp(sampleFile,dateTimeTestPath+'hasFilesInFirstDateAndSecondTime/date=20200214/time=131712')

    #To test if file is in second date and latest time
    dbutils.fs.cp(sampleFile,dateTimeTestPath+'hasFilesInSecondDateAndFirstTime/date=20200213/time=181712')

    #To test if file is in second date and second time
    dbutils.fs.cp(sampleFile,dateTimeTestPath+'hasFilesInSecondDateAndSecondTime/date=20200213/time=131712')
    
    #To test if no entry in availability returns the min date from files
    dbutils.fs.cp(sampleFile,dateTimeTestPath+'hasNoDateInAvailability/date=20200213/time=131712')
    dbutils.fs.cp(sampleFile,dateTimeTestPath+'hasNoDateInAvailability/date=20200214/time=131712')
    dbutils.fs.cp(sampleFile,dateTimeTestPath+'hasNoDateInAvailability/date=20200215/time=131712')
    dbutils.fs.cp(sampleFile,dateTimeTestPath+'hasNoDateInAvailability/date=20200216/time=131712')
    
    logTaskProgress(cursor,batchTaskId,'Successfully copied files into directories to test load_unstructuredxmlSourcehistory_gen2_sqdb')

  except Exception as e:
    errorMessage = 'Exception occurred while copying files into directories to test load_unstructuredxmlSourcehistory_gen2_sqdb ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 
    

# COMMAND ----------

# DBTITLE 1,Perform cleanup from previous runs
try:
  #Cleanup the previous created directories
  dbutils.fs.rm('/mnt/raw/testLoadunstructuredxmlSourceHistoryGen2Sqdb',recurse = True)
  #cleanup the previous inserted data
  cleanup = open("{}LoadunstructuredxmlSourceHistoryGen2SqdbCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage = "Exception occurred while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Data preparation for test scenarios
#Preparing parameters for stored procedure test
try:
  #Create date and time level directories for test scenarios
  #Specify paths for date and time level directories
  mainDirTime = ["/mnt/raw/testLoadunstructuredxmlSourceHistoryGen2Sqdb/hist_testLoadunstructuredxmlSourceHistoryGen2Sqdb/input/dateTimeTestFiles"] 
  commonDirTime = ["hasFilesInFirstDateAndFirstTime", "hasFilesInFirstDateAndSecondTime", "hasFilesInSecondDateAndFirstTime", "hasFilesInSecondDateAndSecondTime", "hasNoFiles"] 
  commonDirDateTime = ["date=20200213", "date=20200214"]
  commonDirDateDateTime = ["time=131712", "time=181712"]
  createDateTimeDirectory(mainDirTime,commonDirTime,commonDirDateTime,commonDirDateDateTime)
  
  #Create date and time level directories for history scenarios
  commonDirTime = ["hasNoDateInAvailability"] 
  commonDirDateTime = ["date=20200213", "date=20200214", "date=20200215", "date=20200216"]
  commonDirDateDateTime = ["time=131712"]
  createDateTimeDirectory(mainDirTime,commonDirTime,commonDirDateTime,commonDirDateDateTime)
  
  #Create date level directories for test scenarios
  #Specify paths for date level directories
  mainDirDate = ["/mnt/raw/testLoadunstructuredxmlSourceHistoryGen2Sqdb/hist_testLoadunstructuredxmlSourceHistoryGen2Sqdb/input/dateTestFiles"] 
  commonDirDate = ["hasFilesInFirstDate", "hasFilesInSecondDate", "hasNoFiles"]
  commonDirDateDate = ["date=20200213", "date=20200214"]
  createDateDirectory(mainDirDate,commonDirDate,commonDirDateDate)
  #Copy files in different directories to test different scenarios 
  testDataLoadPreparation()

  insertData = open('{}LoadunstructuredxmlSourceHistoryGen2SqdbDataPrep.sql'.format(testInputPath), "r").read()
  
  rawLocation = pd.read_sql_query(insertData,conn)
    
  logTaskProgress(cursor,batchTaskId,'Successfully prepared data for test scenario')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Transform metadata into spark dataframe
# transfer the rawLocation to spark dataframe
try :
  testScenariosDf = convertPandasToSparkDfLoadNaGen2Sqdb(rawLocation,
                                                            cursor, 
                                                            batchTaskId,
                                                            adfPipelineName,
                                                            clusterId,
                                                            notebookName,
                                                            errorLogFileLocation)
  
except Exception as e:
  errorMessage = 'Exception occurred while Transforming metadata into spark dataframe:' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Run rawObjectLatestDateTime function for all scenarios
#Execute function to mark dates in tbl_object_dates_availability table
try:
  rawObjectHistoricDateTime(testScenariosDf,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Successfully executed function rawObjectHistoricDateTime')
except Exception as e:
  errorMessage = "Exception occurred while executing function rawObjectHistoricDateTime" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-1: Verify if file is in first date folder it has to return first date timestamp
#If data is in first date it has to return first date, here created 2 dates 20200214,20200213 it has to give 20200214  
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify if file is in first date folder it has to return first date timestamp' 
  #Test object while preparing data data for scenario
  testObjectScenario = 'testLoadunstructuredxmlSourceHistoryGen2Sqdb_dateTestFiles_hasFilesInFirstDate'
  #Get object_id for testObject
  testObjectId = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObjectScenario),conn)).collect()[0][0]
  
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select output_datetime_to from audit.tbl_object_dates_availability where output_object_id = '{}'".format(testObjectId),conn)).collect()[0][0]
  if str(dateFromFunction) == '2020-02-14 00:00:00':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-1 notebook load_unstructuredxmlSourcehistory_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-1 notebook load_unstructuredxmlSourcehistory_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-2: Verify if file is in second date folder it has to return second date timestamp
#If data is in second date it has to return first date, here created 2 dates 20200214,20200213 it has to give 20200213  
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify if file is in second date folder it has to return second date timestamp'
  #Test object while preparing data data for scenario
  testObjectScenario = 'testLoadunstructuredxmlSourceHistoryGen2Sqdb_dateTestFiles_hasFilesInSecondDate'
  #Get object_id for testObject
  testObjectId = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObjectScenario),conn)).collect()[0][0]
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select output_datetime_to from audit.tbl_object_dates_availability where output_object_id = '{}'".format(testObjectId),conn)).collect()[0][0]
  if str(dateFromFunction) == '2020-02-13 00:00:00':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-2 notebook load_unstructuredxmlSourcehistory_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-2 notebook load_unstructuredxmlSourcehistory_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-3: Verify minimum default timestamp + treshold is returned when no files are in date directories 
#If there is no files in all date directories it has to return default value i.e., '2000-01-01 00:00:00.000'
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify minimum default timestamp + treshold is returned when no files are in date directories'
  #Test object while preparing data data for scenario
  testObjectScenario = 'testLoadunstructuredxmlSourceHistoryGen2Sqdb_dateTestFiles_hasNoFiles'
  #Get object_id for testObject
  testObjectId = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObjectScenario),conn)).collect()[0][0]
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select output_datetime_to from audit.tbl_object_dates_availability where output_object_id = '{}'".format(testObjectId),conn)).collect()[0][0]

  if str(dateFromFunction) == '2000-01-01 00:00:00':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
   
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-3 notebook load_unstructuredxmlSourcehistory_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-3 notebook load_unstructuredxmlSourcehistory_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-4: Verify return value if files are in first_date and first_time folder
#If file is in first_date's first_time folder it has to return first_time's timestamp of the first_date folder, here created 2 dates 20200214,20200213 and in each date created 2 times 181712,131712 it has to give date 20200214 and time 181712 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify return value if files are in first_date and first_time folder'
  #Test object while preparing data data for scenario
  testObjectScenario = 'testLoadunstructuredxmlSourceHistoryGen2Sqdb_dateTimeTestFiles_hasFilesInFirstDateAndFirstTime'
  #Get object_id for testObject
  testObjectId = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObjectScenario),conn)).collect()[0][0]
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select output_datetime_to from audit.tbl_object_dates_availability where output_object_id = '{}'".format(testObjectId),conn)).collect()[0][0]
  if str(dateFromFunction) == '2020-02-14 18:17:12':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-4 notebook load_unstructuredxmlSourcehistory_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-4 notebook load_unstructuredxmlSourcehistory_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-5: Verify return value if files are in first_date and second_time folder
#If file is in first_date's second_time folder it has to return second_time's timestamp of the first_date folder, here created 2 dates 20200214,20200213 and in each date created 2 times 181712,131712 it has to give date 20200214 and time 131712 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify if files in first date and second time'
  #Test object while preparing data data for scenario
  testObjectScenario = 'testLoadunstructuredxmlSourceHistoryGen2Sqdb_dateTimeTestFiles_hasFilesInFirstDateAndSecondTime'
  #Get object_id for testObject
  testObjectId = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObjectScenario),conn)).collect()[0][0]
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select output_datetime_to from audit.tbl_object_dates_availability where output_object_id = '{}'".format(testObjectId),conn)).collect()[0][0]
  if str(dateFromFunction) == '2020-02-14 13:17:12':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-5 notebook load_unstructuredxmlSourcehistory_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-5 notebook load_unstructuredxmlSourcehistory_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-6: Verify return value if files are in second_date and first_time folder
#If data is in second date first time it has to return second date first time, here created 2 dates 20200214,20200213 and in each date created 2 times 181712,131712 it has to give date 20200213 and time 181712 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify return value if files are in second_date and first_time folder'
  #Test object while preparing data data for scenario
  testObjectScenario = 'testLoadunstructuredxmlSourceHistoryGen2Sqdb_dateTimeTestFiles_hasFilesInSecondDateAndFirstTime'
  #Get object_id for testObject
  testObjectId = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObjectScenario),conn)).collect()[0][0]
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select output_datetime_to from audit.tbl_object_dates_availability where output_object_id = '{}'".format(testObjectId),conn)).collect()[0][0]
  if str(dateFromFunction) == '2020-02-13 18:17:12':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'

  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-6 notebook load_unstructuredxmlSourcehistory_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-6 notebook load_unstructuredxmlSourcehistory_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-7: Verify return value if files are in second_date and second_time folder
#If data is in second date second time it has to return second date first time, here created 2 dates 20200214,20200213 and in each date created 2 times 181712,131712 it has to give date 20200213 and time 131712 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify if files in second date and second time'
  #Test object while preparing data data for scenario
  testObjectScenario = 'testLoadunstructuredxmlSourceHistoryGen2Sqdb_dateTimeTestFiles_hasFilesInSecondDateAndSecondTime'
  #Get object_id for testObject
  testObjectId = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObjectScenario),conn)).collect()[0][0]
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select output_datetime_to from audit.tbl_object_dates_availability where output_object_id = '{}'".format(testObjectId),conn)).collect()[0][0]
  if str(dateFromFunction) == '2020-02-13 13:17:12':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-7 notebook load_unstructuredxmlSourcehistory_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-7 notebook load_unstructuredxmlSourcehistory_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-8: Verify if there is no data in all date and time level directories
#If data is not in all date and time level directories it has to return minimal default value, i.e., '2000-01-01 00:00:00'
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify if there is no data in all date and time level directories'
  #Test object while preparing data data for scenario
  testObjectScenario = 'testLoadunstructuredxmlSourceHistoryGen2Sqdb_dateTimeTestFiles_hasNoFiles'
  #Get object_id for testObject
  testObjectId = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObjectScenario),conn)).collect()[0][0]
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select output_datetime_to from audit.tbl_object_dates_availability where output_object_id = '{}'".format(testObjectId),conn)).collect()[0][0]
 
  if str(dateFromFunction) == '2000-01-01 00:00:00':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-8 notebook load_unstructuredxmlSourcehistory_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-8 notebook load_unstructuredxmlSourcehistory_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-9: Verify if infile_datetime_column has value
#If infile_datetime_column has any value it will return timestamp by reading files and it will take from 'EnqueuedTimeUtc' field min value
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify if infile_datetime_column has value'
  #Test object while preparing data data for scenario
  testObjectScenario = 'testLoadunstructuredxmlSourceHistoryGen2Sqdb_dateTestFiles_EnqueuedTimeUtc'
  infileDatetimeColumn = 'EnqueuedTimeUtc'
  inputFile = '/mnt/dataquality/unit_tests/loadNaGen2Sqdb/input/sampleTestAvroFile.avro'
  #Get object_id for testObject and infileDatetimeColumn
  testcaseInputDf = spark.createDataFrame(pd.read_sql_query("select object_id,location from config.tbl_object where object_name = '{}' and infile_datetime_column = '{}'".format(testObjectScenario,infileDatetimeColumn),conn))

  testObjectId = testcaseInputDf.collect()[0][0]
  testLocation = testcaseInputDf.collect()[0][1]
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select output_datetime_to from audit.tbl_object_dates_availability where output_object_id = '{}'".format(testObjectId),conn)).collect()[0][0]
  
  
  minDatetime = (spark.read.format('com.databricks.spark.avro')
                         .load(testLocation)
                         #get the infile date time column from the variable populated from metadata
                         .selectExpr("to_timestamp({},'MM/dd/yyyy hh:mm:ss aa') AS InfileDateTime".format(infileDatetimeColumn))
                         .agg({"InfileDateTime": "min"})
                         .collect()[0][0]
                        )
  if str(dateFromFunction) == str(minDatetime):
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-9 notebook load_unstructuredxmlSourcehistory_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-9 notebook load_unstructuredxmlSourcehistory_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-10: Verify if no data in the availiability table returns minimal value from the directories + treshold
#If there is no data in the availiability table, return minimal value from the directories, i.e. '2020-02-13 13:17:12' 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify if no data in the availiability table returns minimal value from the directories + treshold'
  #Test object while preparing data data for scenario
  testObjectScenario = 'testLoadunstructuredxmlSourceHistoryGen2Sqdb_dateTimeTestFiles_hasNoDateInAvailability'
  #Get object_id for testObject
  testObjectId = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObjectScenario),conn)).collect()[0][0]
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select output_datetime_to from audit.tbl_object_dates_availability where output_object_id = '{}'".format(testObjectId),conn)).collect()[0][0]

  if str(dateFromFunction) == '2020-02-13 13:17:12':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-10 notebook load_unstructuredxmlSourcehistory_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-10 notebook load_unstructuredxmlSourcehistory_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Run rawObjectLatestDateTime function for the last scenario
#Execute function to mark dates in tbl_object_dates_availability table
try:
  testScenariosDf = testScenariosDf.selectExpr('*').where(testScenariosDf['location'].like('/mnt/raw/testLoadunstructuredxmlSourceHistoryGen2Sqdb/hist_testLoadunstructuredxmlSourceHistoryGen2Sqdb/input/dateTimeTestFiles/hasNoDateInAvailability%'))
  rawObjectHistoricDateTime(testScenariosDf,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Successfully executed function rawObjectHistoricDateTime')
except Exception as e:
  errorMessage = "Exception occurred while executing function rawObjectHistoricDateTime" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-11: Verify if data in the availability table returns max availability table value + treshold
#If there is availiability table, returns max availiability date + treshold. i.e., '2020-02-13 13:17:12' + 2 days
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify if data in the availability table returns max availability table value + treshold'
  #Test object while preparing data data for scenario
  testObjectScenario = 'testLoadunstructuredxmlSourceHistoryGen2Sqdb_dateTimeTestFiles_hasNoDateInAvailability'
  #Get object_id for testObject
  testObjectId = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObjectScenario),conn)).collect()[0][0]
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select max(output_datetime_to) as output_datetime_to from audit.tbl_object_dates_availability where output_object_id = '{}'".format(testObjectId),conn)).collect()[0][0]

  if str(dateFromFunction) == '2020-02-15 13:17:12':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-11 notebook load_unstructuredxmlSourcehistory_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-11 notebook load_unstructuredxmlSourcehistory_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Run rawObjectLatestDateTime function for the last scenario
#Execute function to mark dates in tbl_object_dates_availability table
try:
  rawObjectHistoricDateTime(testScenariosDf,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Successfully executed function rawObjectHistoricDateTime')
except Exception as e:
  errorMessage = "Exception occurred while executing function rawObjectHistoricDateTime" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-12: Verify if data in the availability table returns max value from the files if less than the availability table max + treshold
#If there is availiability table, returns max availiability date + treshold. i.e., '2020-02-15 13:17:12' + 2 days, but this is greater than the max date so it will return the max vaule -> 2020-02-15 13:17:12
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify if data in the availability table returns max value from the files if less than the availability table max + treshold'
  #Test object while preparing data data for scenario
  testObjectScenario = 'testLoadunstructuredxmlSourceHistoryGen2Sqdb_dateTimeTestFiles_hasNoDateInAvailability'
  #Get object_id for testObject
  testObjectId = spark.createDataFrame(pd.read_sql_query("select object_id from config.tbl_object where object_name = '{}'".format(testObjectScenario),conn)).collect()[0][0]
  #Get the datetime from the table tbl_object_dates_availability, which is inserted by function
  dateFromFunction = spark.createDataFrame(pd.read_sql_query("select max(output_datetime_to) as output_datetime_to from audit.tbl_object_dates_availability where output_object_id = '{}'".format(testObjectId),conn)).collect()[0][0]

  if str(dateFromFunction) == '2020-02-15 13:17:12':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario-12 notebook load_unstructuredxmlSourcehistory_gen2_sqdb')
except Exception as e:
  errorMessage = "Exception occurred while prepared data for test scenario-12 notebook load_unstructuredxmlSourcehistory_gen2_sqdb" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
try:
  #Cleanup the created directories
  dbutils.fs.rm('/mnt/raw/testLoadunstructuredxmlSourceHistoryGen2Sqdb',recurse = True)
  #cleanup the inserted data
  cleanup = open("{}LoadunstructuredxmlSourceHistoryGen2SqdbCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage = "Exception occurred while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Close database connection
#call taskEndAndCloseConn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor
                      ,conn
                      ,batchTaskId
                      ,batchTaskSourceRows
                      ,batchTaskRowsLoaded
                      ,batchTaskRejectRows
                      ,batchTaskResult
                      ,batchTaskResultLocation
                      ,adfPipelineName
                      ,clusterId
                      ,notebookName
                      ,errorLogFileLocation)
except:
  assert False