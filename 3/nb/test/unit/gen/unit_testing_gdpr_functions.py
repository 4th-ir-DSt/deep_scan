# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC #Instructions to run the notebook
# MAGIC 
# MAGIC This notebook is responsible for redaction of personal data from a specific table and downstream tables containing that PII
# MAGIC <ul>
# MAGIC <li>Step 1: Run the notebook from cell 3 to cell 12 sequentially.</li>
# MAGIC <li>Step 2: Run cell 13 </li>
# MAGIC    <ul>
# MAGIC      <li>On success : If cell 13 prints "Proceed to run the notebook"  execute cell 14 </li>
# MAGIC      <li>On Failure : If cell 13 prints a message "A batch is already running. Please contact EDS Team" then do not run the next cell stop the execution and contact the EDS Team</li>
# MAGIC    </ul>
# MAGIC    
# MAGIC  <li>Step 3: Run cell 14 </li>
# MAGIC    <ul>
# MAGIC      <li>On success : If cell 14 prints "Proceed to run the notebook"  execute cell 15 </li>
# MAGIC      <li>On Failure : If cell 14 prints a message "A batch is already running. Please contact EDS Team" then do not run the next cell stop the execution and contact the EDS Team</li>
# MAGIC    </ul>
# MAGIC 		
# MAGIC <li>Step 4: Run cell 15 </li>
# MAGIC      <ul>
# MAGIC        <li>On success : If cell 15 prints "Proceed to run the notebook"  execute cell 16 </li>
# MAGIC        <li>On Failure : If cell 15 prints a message "A batch is already running. Please contact EDS Team" then do not run the next cell stop the execution and contact the EDS Team</li>
# MAGIC      </ul>
# MAGIC   
# MAGIC <li>Step 6: Run the cell 16 , 17</li>
# MAGIC      <ul>
# MAGIC        <li>On success : If cell 17 prints "Proceed to run the notebook"  execute cell 18 </li>
# MAGIC        <li>On Failure : If cell 17 prints a message "A batch is already running. Please contact EDS Team" then do not run the next cell stop the execution and contact the EDS Team</li>
# MAGIC      </ul>
# MAGIC <li>Step 7: Run the cell 18,19</li>
# MAGIC <li>Step 8: Run cell 20 to 27 </li>
# MAGIC      <ul>
# MAGIC        <li>On success : If cell success and it prints "Proceed to run the notebook"  execute next cell till cell 27 </li>
# MAGIC        <li>On Failure : If cell and print a message "A batch is already running. Please contact EDS Team" then run cell 28,29,30,31,32,33 to close the connection and complete the batch process and contact the EDS Team for more details </li>
# MAGIC      </ul>
# MAGIC  <li>Step 9: On success of cell 27 run cell 28,29,30,31,32,33 to clean up the data </li>
# MAGIC      

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_gdpr_na_na_dbdt_gdprredactionkeys</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td> unit testing of gdpr_na_na_dbdt_gdprredactionkeys notebook</td></tr>
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
# MAGIC     <td></td>
# MAGIC     <td> </td>
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

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  import collections
  from datetime import datetime,timedelta
  from pyspark.sql.functions import lit,col
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
  from pyspark.sql.functions import broadcast

except Exception as e:
  errorMessage = "Exception occurred while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Initialise variables
try:
  
  #variable current_time
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
  LastUpdatedTimestamp = currentTs
  #use the same time for the date also
  date = currentTs
  
 #parameter for logError
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
  
  #Parameter and paths
  testObject = 'Notebook'
  testObjectName = 'gdprFunction'
  outputLocation = ''
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  requiredInputParameter = 'batchTaskId'
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')
  executionDateTime = currentTs
  
  #Call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')
  
except Exception as e:
  errorMessage = "Exception occurred while variable declaration " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn = pyodbc.connect(dbconn, autocommit = True)
  cursor = conn.cursor()
except Exception as e:
  errorMessage = 'unable to establish SQL Database connection'
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Cleanup Script
try:
  #run the cleanup script for the new batch insert 
  cleanupMetadataBatchId = open(testInputPath + "gdprFunctionBatchIdInsertCleanup.sql", "r").read()
  cursor.execute(cleanupMetadataBatchId)
  #run cleanup script for the object and the downstream object
  cleanupMetadata = open(testInputPath + "gdprFunctionObjectCleanup.sql", "r").read()
  cursor.execute(cleanupMetadata)
  
  while cursor.nextset():
    x = 1
  print("Proceed to run the notebook")
except Exception as e:
  errorMessage = "Exception occurred while performing metadata cleanup: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False 

# COMMAND ----------

# DBTITLE 1,Delete the table if exists
# MAGIC %sql
# MAGIC -- clean up the existing deltatable 
# MAGIC DROP TABLE IF EXISTS retentionKeysTable;
# MAGIC DROP TABLE IF EXISTS gdprRedactionKeysUnitTestObject_a ;
# MAGIC DROP TABLE IF EXISTS gdprRedactionKeysUnitTestObject_b ;
# MAGIC DROP TABLE IF EXISTS gdprRedactionKeysUnitTestObject_c ;
# MAGIC DROP TABLE IF EXISTS gdprRedactionKeysUnitTestObject_d ;

# COMMAND ----------

# DBTITLE 1,Remove the folders if it exists
try :
  #clean up the delta table folders for testing
  dbutils.fs.rm("/mnt/dataquality/unit_tests/gdprFunction/tables" , True);
  logToFile(errorLogFileLocation,errorMessage)
  print("Proceed to run the notebook")
except Exception as e:
  errorMessage = 'Exception occurred while deleting the delta table folders: ' + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Prepare object name variables
# intialized variables that needs to be passed as widgets to gdpr_na_na_dbdt_gdprredactionkeys notebook 
try:
  objectBaseName = 'gdprRedactionKeysUnitTestObject'
  stagingTableName = objectBaseName + '_a'
  keyColumnName = 'UUID'
  keyTableName = 'retentionKeysTable'
  refinedTableName1 = objectBaseName + '_b'
  refinedTableName2 = objectBaseName + '_c'
  personTableName = objectBaseName + '_d'
  scheduleReference = objectBaseName+'schedule'
  batchTaskRejectRows = ''
  batchTaskResult = ''
  batchTaskResultLocation = ''
  batchTaskRowsLoaded = ''
  batchTaskSourceRows = ''
  batchTaskStatus = 'completed'
  print('you can proceed with the next cell')
except Exception as e:
  errorMessage = 'Exception occurred while getting parameters and initialising error log location ' + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Run notebook gdpr_na_na_dbdt_gdprredactionkeys
# MAGIC %run /dtp/nb/util/gen/gdpr_functions

# COMMAND ----------

# DBTITLE 1,Check the status of existing batch
#call the function to check if any batch is running.based on the output proceed with the execution of other cells.
try:
  batchStatusCheck()
except Exception as e:
  errorMessage = 'batchStatusCheck function call failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Data Preparation for unit test
#execute scripts to generate input parameter for adf pipeline
try:
  batchStatus = batchStatusCheck()
  if batchStatus == True:
    
     #metadata inset for new batch_id
    #inputBatchIdParameter = open("{}gdprFunctionBatchIdInsert.sql".format(testInputPath), "r").read()
    #cursor.execute(inputBatchIdParameter)
    
    #metadata inset for new object and downstream object
    inputParameter = open("{}gdprFunctionDataPrep1.sql".format(testInputPath), "r").read()
    cursor.execute(inputParameter)
  
    while cursor.nextset():
      x = 1

except Exception as e:
  errorMessage = 'initialBatchStatusCheck function call failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False


# COMMAND ----------

# DBTITLE 1,Start a new batch by calling usp_batch_start
#execute the usp_batch_start store procedure to start a new batch 
try:
  cursor.execute('exec audit.usp_batch_start ?',scheduleReference)
  while cursor.nextset():
      x = 1
  print('you can proceed with the next cell')
except Exception as e:
  print("A batch is already running.please contact EDS Team")
  errorMessage = 'batchStatusCheck function call failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Get the batch_id and batch_task_id
#select the new batch_id and batch_task_id inserted 
try :
  currentBatch = pd.read_sql_query("SELECT ba.batch_id ,tbt.batch_task_id FROM audit.tbl_batch  ba JOIN config.tbl_schedule sc ON ba.schedule_id = sc.schedule_id JOIN audit.tbl_batch_task tbt ON ba.batch_id = tbt.batch_id WHERE sc.schedule_reference = '{}' and ba.status = 'running'".format(scheduleReference), conn)
  currentBatchDF = spark.createDataFrame(currentBatch)
  currentBatchDic = currentBatchDF.collect()[0].asDict() 
  batchId = currentBatchDic['batch_id']
  batchTaskId = currentBatchDic['batch_task_id'] 
except Exception as e:
  errorMessage = 'batchStatusCheck function call failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Check if the batch_id is running is for this process and update status as running
#checkCurrentBatch checks if batch is running for the batch id provided
try:
  checkCurrentBatch = currentBatchStatusCheck(batchId)
  if checkCurrentBatch == True:
    #call usp_log_task_start store procedure to update the batchTaskId to running
    cursor.execute("exec audit.usp_log_task_start ?",batchTaskId)
    print("Proceed to run the notebook")
  else:
    print("Don't run the batch now.Please contact EDS team")
except Exception as e:
  errorMessage = 'currentBatchStatusCheck function call failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Create staging table and the downstream table 
# MAGIC %sql
# MAGIC create table retentionKeysTable (
# MAGIC key_value int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprFunction/tables/retentionKeysTable";
# MAGIC 
# MAGIC create table gdprRedactionKeysUnitTestObject_a (
# MAGIC UUID int,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC PiiHashVersion string,
# MAGIC Gender string,
# MAGIC Age int,
# MAGIC Date_of_birth timestamp,
# MAGIC Premium_amount decimal(23,3),
# MAGIC Amount_coverage bigint,
# MAGIC StartDate Date)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprFunction/tables/gdprRedactionKeysUnitTestObject_a";
# MAGIC 
# MAGIC create table gdprRedactionKeysUnitTestObject_b (
# MAGIC UUID string,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC PiiHashVersion int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprFunction/tables/gdprRedactionKeysUnitTestObject_b";
# MAGIC 
# MAGIC create table gdprRedactionKeysUnitTestObject_c (
# MAGIC UUID string,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC PiiHashVersion int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprFunction/tables/gdprRedactionKeysUnitTestObject_c";
# MAGIC 
# MAGIC 
# MAGIC create table gdprRedactionKeysUnitTestObject_d (
# MAGIC UUID string,
# MAGIC PiiHash string,
# MAGIC PiiTraceabilityHash string,
# MAGIC PiiHashVersion int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/gdprFunction/tables/gdprRedactionKeysUnitTestObject_d";

# COMMAND ----------

# DBTITLE 1,Insert data into staging table and the downstream table 
# MAGIC %sql
# MAGIC insert into gdprRedactionKeysUnitTestObject_a  values(102,'aqlptreqw','claim','1','Male','32','1987-02-09','96532245.89','456788990','2008-01-15');
# MAGIC insert into gdprRedactionKeysUnitTestObject_a  values(103,'aqlfdfptre','LicenseDate','1','Male','25','1995-09-21','452245.89','906788990','2007-01-25');
# MAGIC insert into gdprRedactionKeysUnitTestObject_a  values(104,'qwqaqlptre','ForenameInitial1','1','Female','30','1990-09-11','15632245.89','2116788990','2001-01-05');
# MAGIC insert into gdprRedactionKeysUnitTestObject_a  values(105,'ioqaqlptre','abc','1','Female','30','1990-09-11','782245.89','12116788990','2002-01-12');
# MAGIC insert into gdprRedactionKeysUnitTestObject_a  values(106,'mkjlptre','ForenameInitial1','1','Female','30','1990-09-11','4312245.89','2346788990','2003-09-15');
# MAGIC insert into gdprRedactionKeysUnitTestObject_a  values(107,'aqlfdfptre','LicenseDate','1','Male','32','1987-02-09','7812245.89','678788990','2003-03-05');
# MAGIC insert into gdprRedactionKeysUnitTestObject_a  values(108,'pqsjlptre','ForenameInitial1','1','Female','25','1995-09-21','7812245.89','1126788990','2003-01-01');
# MAGIC insert into gdprRedactionKeysUnitTestObject_a  values(109,'qwpsjlptre','LicenseDate','1','Female','30','1990-09-11','7812245.89','0996788990','2003-01-21');
# MAGIC insert into gdprRedactionKeysUnitTestObject_a  values(110,'qwqaqlptre','jkl','1','Female','30','1990-09-11','7812245.89','4566788990','2003-01-20');
# MAGIC 
# MAGIC insert into retentionKeysTable  values(101);
# MAGIC insert into retentionKeysTable  values(102);
# MAGIC insert into retentionKeysTable  values(103);
# MAGIC insert into retentionKeysTable  values(104);
# MAGIC insert into retentionKeysTable  values(105);
# MAGIC 
# MAGIC insert into gdprRedactionKeysUnitTestObject_b  values(101,'aqlfdfptre','ForenameInitial6',1);
# MAGIC insert into gdprRedactionKeysUnitTestObject_b  values(121,'opjlptre','LicenseDate',2);
# MAGIC insert into gdprRedactionKeysUnitTestObject_b  values(132,'aqlfdfptre','ForenameInitial1',2);
# MAGIC insert into gdprRedactionKeysUnitTestObject_b  values(102,'qwpsjlptre','claim',1);
# MAGIC insert into gdprRedactionKeysUnitTestObject_b  values(104,'mkjlptre','ForenameInitial1',1);
# MAGIC insert into gdprRedactionKeysUnitTestObject_b  values(105,'ioqaqlptre','abc','1');
# MAGIC 
# MAGIC insert into gdprRedactionKeysUnitTestObject_c  values(101,'aqlfdfptre','ForenameInitial6',1);
# MAGIC insert into gdprRedactionKeysUnitTestObject_c  values(121,'opjlptre','LicenseDate',2);
# MAGIC insert into gdprRedactionKeysUnitTestObject_c  values(102,'qwpsjlptre','claim',1);
# MAGIC insert into gdprRedactionKeysUnitTestObject_c  values(104,'mkjlptre','ForenameInitial1',1);
# MAGIC insert into gdprRedactionKeysUnitTestObject_c  values(105,'ioqaqlptre','abc','1');
# MAGIC 
# MAGIC insert into gdprRedactionKeysUnitTestObject_d  values(101,'aqlfdfptre','ForenameInitial6',1);
# MAGIC insert into gdprRedactionKeysUnitTestObject_d  values(121,'opjlptre','LicenseDate',2);
# MAGIC insert into gdprRedactionKeysUnitTestObject_d  values(102,'qwpsjlptre','claim',1);
# MAGIC insert into gdprRedactionKeysUnitTestObject_d  values(104,'mkjlptre','ForenameInitial1',1);
# MAGIC insert into gdprRedactionKeysUnitTestObject_d  values(105,'ioqaqlptre','abc','1');

# COMMAND ----------

# DBTITLE 1,scenario - 1 Verify if the key table are having values
#test the readKeyValuesAndStagingTable function if it is reading the keylist table and the staging table properly and the returning it as a datframe 
try:
  #check the status of current batch only when it is in running status the execute the below scripts
  checkCurrentBatch = currentBatchStatusCheck(batchId)
  if checkCurrentBatch == True:
    
    testCaseScenario ='Verify if the key table given by user is read properly'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

    #Call the readKeyValuesAndStagingTable which returns two dataframe 
    tempKeysTable,stagingTable = readKeyValuesAndStagingTable(keyTableName,keyColumnName,stagingTableName,cursor,batchTaskId,adfPipelineName,clusterId,
                                                               notebookName,errorLogFileLocation)

    #Check if the count of rows in each dataframe is greater than zero
    if tempKeysTable.count() > 0 and stagingTable.count() > 0 :
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    #Log the test result in database
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully Tested readKeyValuesAndStagingTable function')
    print("Proceed to run the notebook")
    
  else:
    print("Don't run the next cell.Please contact EDS team")
except Exception as e:
    errorMessage='Exception occurred while testing readKeyValuesAndStagingTable function: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 2 Test getMatchingKeyFromStaging function 
#test getMatchingKeyFromStaging function is performing join properly 
try:
  #check the status of current batch only when it is in running status the execute the below scripts
  checkCurrentBatch = currentBatchStatusCheck(batchId)
  if checkCurrentBatch == True:
    
    testCaseScenario ='Test getMatchingKeyFromStaging function join result'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

    tempPIIHA = getMatchingKeyFromStaging(keyColumnName,tempKeysTable,stagingTable,cursor,batchTaskId,adfPipelineName,
                                                    clusterId,notebookName,errorLogFileLocation)

    #tempKeysTable will have the list of key to be reducted and tempKeysTableList will have list of keys matched in the staging table
    tempPIIHAList = tempPIIHA.select(keyColumnName).collect()
    tempKeysTableList = tempKeysTable.select('key_value').collect()

    #tempKeysTableList will contain the list of keys given by user and tempPIIHAList will key matched with the staging table 
    #check if all tempPIIHAList values are present in tempKeysTableList
    if(all(x in tempKeysTableList for x in tempPIIHAList)): 
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    #Log test status into unit test table 
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully Tested getMatchingKeyFromStaging  function')
    print("Proceed to run the notebook")
  else:
    print("Don't run the next cell.Please contact EDS team")
except Exception as e:
    errorMessage='Exception occurred while testing getMatchingKeyFromStaging  function: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 3 Test getMatchingPiiHash function
#getMatchingPiiHash function join stagingTables and matchedKey with the pii_traceability_hash to get list of keys and PII hash attributes 
try:
   #check the status of current batch only when it is in running status the execute the below scripts
  checkCurrentBatch = currentBatchStatusCheck(batchId)
  if checkCurrentBatch == True:
    
    testCaseScenario ='test function getMatchingPiiHash joining result'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

    supersetTempKeysPIIHA = getMatchingPiiHash(stagingTable,tempPIIHA,keyColumnName,cursor,batchTaskId,adfPipelineName,
                                            clusterId,notebookName,errorLogFileLocation)

    #converting dataframe to list
    supersetTempKeysPIIHAList = supersetTempKeysPIIHA.select('PiiTraceabilityHash').collect()
    tempPIIHAList = tempPIIHA.select('PiiTraceabilityHash').collect()

    #supersetTempKeysPIIHAList should have only values from tempPIIHAList 
    if(all(x in tempPIIHAList for x in supersetTempKeysPIIHAList)): 
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    #Log test status into unit test table 
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully Tested getMatchingPiiHash  function')
    print("Proceed to run the notebook")
  else:
    print("Don't run the next cell.Please contact EDS team")
except Exception as e:
    errorMessage='Exception occurred while testing getMatchingPiiHash  function: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 4 Test getInUsePiiHashList function
#getInUsePiiHashList function join supersetTempKeysPIIHA with tempKeysTable to get inUsePIIHA keys which are blocked as they are used for other keys
try:
  #check the status of current batch only when it is in running status the execute the below scripts
  checkCurrentBatch = currentBatchStatusCheck(batchId)
  if checkCurrentBatch == True:
    
    testCaseScenario ='test function getInUsePiiHashList joining result'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

    inUsePIIHA = getInUsePiiHashList(keyColumnName,supersetTempKeysPIIHA,tempKeysTable ,cursor,batchTaskId,adfPipelineName,
                                        clusterId,notebookName,errorLogFileLocation)

    #supersetTempKeysPIIHA should have the count greater than or equal to tempKeysTableList
    if supersetTempKeysPIIHA.count() >= inUsePIIHA.count():
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    #Log overall test status into unit test table 
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully Tested getInUsePiiHashList  function')
    print("Proceed to run the notebook")
  else:
    print("Don't run the next cell.Please contact EDS team")
except Exception as e:
    errorMessage='Exception occurred while testing getInUsePiiHashList  function: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 5 test keyListToRemove function
#keyListToRemove function join tempPIIHA with inUsePIIHA to get PIIHAToRemove and KeyToRemove 
# Remove these PII hash attributes inUsePIIHA from the set to be deleted tempPIIHA/
try:
  #check the status of current batch only when it is in running status the execute the below scripts
  checkCurrentBatch = currentBatchStatusCheck(batchId)
  if checkCurrentBatch == True:
    
    testCaseScenario ='test function keyListToRemove joining result'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

    PIIHAToRemoveStr,keyToRemoveStr = keyListToRemove(tempPIIHA,inUsePIIHA,keyColumnName,cursor,batchTaskId,adfPipelineName,
                                                            clusterId,notebookName,errorLogFileLocation)

  #  convert the return string to list
    PIIHAToRemoveStrList = PIIHAToRemoveStr.split(",")
    #convert datframe to list 
    inUsePIIHAList = inUsePIIHA.collect()
  # values from inUsePIIHAList should not be present in the PIIHAToRemoveStrList 
  #collections.Counter will compare 2 list and check if it is having any common values between them 
    if (collections.Counter(inUsePIIHAList) != collections.Counter(PIIHAToRemoveStrList)):
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    #Log test status into unit test table 
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully Tested getInUsePiiHashList  function')
    print("Proceed to run the notebook")
  else:
    print("Don't run the next cell.Please contact EDS team")
except Exception as e:
    errorMessage='Exception occurred while testing getInUsePiiHashList  function: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Scenario - 6 check getDownstreamObjects has two downstream object returned 
#getDownstreamObjects function executes the usp_get_downstream_object to get the downstream object for the given staging table 
try:
  #check the status of current batch only when it is in running status the execute the below scripts
  checkCurrentBatch = currentBatchStatusCheck(batchId)
  if checkCurrentBatch == True:
    
    testCaseScenario ='Test getDownstreamObjects function return two downstream object'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

    #execute the procedure for staging table
    downstreamTableName =  getDownstreamObjects(stagingTableName,conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

    #check if the staging table returns 3 downstream object
    if downstreamTableName.count() == 3:
      testResult = 'success'
      executionOutputStatus = 'As Expected'

    #Log test status into unit test table 
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully tested getDownstreamObjects function')
    print("Proceed to run the notebook")
  else:
    print("Don't run the next cell.Please contact EDS team")
except Exception as e:
    errorMessage='Exception occurred while testing getDownstreamObjects function : ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,scenario - 7 test removePiiInDownstreamAndStaging function 
#data prepration script have made a entry for one staging table having 2 refined and 1 person as downstream on runnning this test case it should remove the key from staging and person table and mark it to REDACTED in the Refined zone 
try :
  #check the status of current batch only when it is in running status the execute the below scripts
  checkCurrentBatch = currentBatchStatusCheck(batchId)
  if checkCurrentBatch == True:

    testCaseScenario ='Test removePiiInDownstreamAndStaging  function update and delete of the key columns are working as expected'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

    #Call removePiiInDownstreamAndStaging to remove the matching key in staging and mark as REDACTED in downstream
    removePiiInDownstreamAndStaging(downstreamTableName,keyColumnName,keyToRemoveStr,PIIHAToRemoveStr,
                                     cursor,batchTaskId,adfPipelineName,clusterId,
                                     notebookName,errorLogFileLocation)


    #Check if the person table is having the key value which have to be removed 
    personTableDeleteCheck = sqlContext.sql("select * from {} where {} in ({})".format(personTableName,keyColumnName,PIIHAToRemoveStr))

    #check if Refined table1 is updated to REDACTED
    refinedTableUpdateCheck1 = sqlContext.sql("select PiiHash,PiiTraceabilityHash,PiiHashVersion from {} where {} in ({})".format(refinedTableName1,keyColumnName,keyToRemoveStr))

      #check if Refined table1 is updated to REDACTED
    refinedTableUpdateCheck2 = sqlContext.sql("select PiiHash,PiiTraceabilityHash,PiiHashVersion from {} where {} in ({})".format(refinedTableName2,keyColumnName,keyToRemoveStr))

    #Get only the distinct values from dataframe if should not contain other than REDACTED values if it is having then the the table is not updated properly
    refinedCheckForUpdate = refinedTableUpdateCheck1.select("PiiHash","PiiTraceabilityHash","PiiHashVersion").distinct()

    #check if the data is updated to REDACTED in downstream(Refined zone)
    if (refinedCheckForUpdate.count() == 1): 
      refinedCheckForUpdateDic = refinedCheckForUpdate.collect()[0].asDict() 
      if refinedCheckForUpdateDic['PiiHash'] == 'REDACTED' and refinedCheckForUpdateDic['PiiTraceabilityHash'] == 'REDACTED'and refinedCheckForUpdateDic['PiiHashVersion'] == -1 :
        testResult = 'success'
        executionOutputStatus = 'As Expected'

    #Log test status into unit test table 
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully tested getDownstreamObjects function')
    print("Proceed to run the notebook")
  else:
    print("Don't run the next cell.Please contact EDS team") 
except Exception as e:
    errorMessage='Exception occurred while testing getDownstreamObjects function : ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,scenario - 8 test Staging table values are redacted
try :
   #check the status of current batch only when it is in running status the execute the below scripts
  checkCurrentBatch = currentBatchStatusCheck(batchId)
  if checkCurrentBatch == True:

    testCaseScenario ='Test staging table columns are updated to REDACTED'
    testResult = 'failed' #initialise the test result
    executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

    #Check if the staging table is having the key value are updated 
    stagingTableCheck = sqlContext.sql("select PiiHash,PiiTraceabilityHash,PiiHashVersion from {} where {} in ({})".format(stagingTableName,keyColumnName,keyToRemoveStr))
    stagingTableCheckDic = stagingTableCheck.collect()[0].asDict() 

    if stagingTableCheckDic['PiiHash'] == 'REDACTED' and stagingTableCheckDic['PiiTraceabilityHash'] == 'REDACTED'and stagingTableCheckDic['PiiHashVersion'] == -1 :
      testResult = 'success'
      executionOutputStatus = 'As Expected'

          #Log test status into unit test table 
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successfully tested getDownstreamObjects function')
    print("Proceed to run the notebook")
  else:
    print("Don't run the next cell.Please contact EDS team") 
except Exception as e:
    errorMessage='Exception occurred while testing getDownstreamObjects function : ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 
  
  

# COMMAND ----------

# DBTITLE 1,Drop the delta table created for testing
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS retentionKeysTable;
# MAGIC DROP TABLE IF EXISTS gdprRedactionKeysUnitTestObject_a ;
# MAGIC DROP TABLE IF EXISTS gdprRedactionKeysUnitTestObject_b ;
# MAGIC DROP TABLE IF EXISTS gdprRedactionKeysUnitTestObject_c ;
# MAGIC DROP TABLE IF EXISTS gdprRedactionKeysUnitTestObject_d ;

# COMMAND ----------

# DBTITLE 1,Remove test folders
try :
  #clean up the delta table folders for testing
  dbutils.fs.rm("/mnt/dataquality/unit_tests/gdprFunction/tables" , True);
  print("Proceed to run the notebook")
except Exception as e:
  errorMessage = 'Exception occurred while deleting the delta table folders: ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Cleanup script
try:
  #run the cleanup script for the new batch insert 
  cleanupMetadataBatchId = open(testInputPath + "gdprFunctionBatchIdInsertCleanup.sql", "r").read()
  cursor.execute(cleanupMetadataBatchId)
  #run cleanup script for the object and the downstream object
  cleanupMetadata = open(testInputPath + "gdprFunctionObjectCleanup.sql", "r").read()
  cursor.execute(cleanupMetadata)
  
  while cursor.nextset():
    x = 1
  print("Proceed to run the notebook")
except Exception as e:
  errorMessage = "Exception occurred while performing metadata cleanup: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False 

# COMMAND ----------

# DBTITLE 1,Update the task to completed
#call usp_batch_complete store procedure to complete the batch
try:
  cursor.execute("exec audit.usp_log_task_end  ?,?,?,?,?,?,?",batchTaskId,batchTaskStatus,
                                                              batchTaskSourceRows,batchTaskRowsLoaded,
                                                              batchTaskRejectRows,batchTaskResult,
                                                              batchTaskResultLocation)
  while cursor.nextset():
      x = 1
except Exception as e:
  errorMessage = 'Exception occurred while executing usp_batch_complete store procedure : ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Update the batch to completed
#call usp_batch_complete store procedure to complete the batch
try:
  cursor.execute('exec audit.usp_batch_complete ?',batchId)
  while cursor.nextset():
      x = 1
except Exception as e:
  errorMessage = 'Exception occurred while executing usp_batch_complete store procedure : ' + str(e)
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