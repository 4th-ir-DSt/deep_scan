# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_tableStructureCheck_table</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test notebook for testing function tableStructureCheck from misc_functions</td></tr>
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
# MAGIC     <td>  </td>
# MAGIC     <td>  </td>
# MAGIC     <td>  </td>
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
  import json
  from datetime import datetime,timedelta
  import numpy as np
  import pyspark.sql.functions as F
  from pyspark.sql import Window
  from pyspark.sql.functions import asc,udf,col, concat, concat_ws, when, upper, lit, col, collect_list, max,regexp_replace,struct,split,posexplode,coalesce
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DecimalType,DateType
  from pyspark import SparkContext,SparkConf
  import xml.etree.ElementTree as ET
  from  itertools import chain
  
except Exception as e:
  errorMessage = "Exception occured while import modules " + str(e)
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
  testObjectName = 'TestMainNaSqldDbdt'  
  requiredInputParameter = 'batchTaskId'
  testCaseScenario = ''
  executionDateTime = datetime.now()
  sampleOutputLocation = 'NA'
  
  #path for datasets used in stored procedure 
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = 'dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  testDirPath = testInputPath.replace('/dbfs','')
except Exception as e:
  errorMessage = "Exception occured while variable declaration " + str(e)
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
  errorMessage = "Exception occured while getting parameters and initilising error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish Database Connection
try:
  dbConn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbConn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage = "Exception occured while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Perform cleanup from previous runs 
#run cleanup script
try:

  #Cleanup the previous inserted metadata
  cleanup1 = open("{}MainNaSqldDbdtCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup1)
  
  while cursor.nextset():
    x = 1  
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage = "Exception occured while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Cleanup - drop the table if exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_create_a;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_create_b;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_create_c;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_create_d;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_a;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_b;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_c;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_d;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_e;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_f;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_g;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_h;
# MAGIC DROP TABLE IF EXISTS Raw_TestMainNaSqldDbdt;

# COMMAND ----------

# DBTITLE 1,Cleanup - remove the directory 
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_create_a',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_create_b',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_create_c',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_create_d',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_a',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_b',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_c',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_d',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_e',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_f',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_g',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_h',recurse=True)
dbutils.fs.rm(testDirPath+'TestunstructuredxmlSource/TestunstructuredxmlSourceShema_Def',recurse=True)
dbutils.fs.rm(testDirPath+'Raw_TestMainNaSqldDbdt',recurse=True)


# COMMAND ----------

# DBTITLE 1,Data preparation  
#execute scripts to insert data into tbl_object and tbl_object_definition
try:
  #inserting data into tbl_object and tbl_object_definition to create the table 
  inputParameter1 = open('{}MainNaSqldDbdtDataPrep1.sql'.format(testInputPath), "r").read()
  cursor.execute(inputParameter1)
  
   #inserting data into tbl_object and tbl_object_definition to alter the existing table 
  inputParameter2 = open('{}MainNaSqldDbdtDataPrep2.sql'.format(testInputPath), "r").read()
  #execute the prep2 script 
  cursor.execute(inputParameter2)
  
  while cursor.nextset():
    x = 1
  
  logTaskProgress(cursor,batchTaskId,'Execution of script to insert records into tbl_object')
except Exception as e:
  errorMessage = "Exception occurred while executing data preparation script: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Create the table manually to alter it by tableStructureCheck function
# MAGIC %sql
# MAGIC 
# MAGIC -- These table are created to test tableStructureCheck function on various test cases like adding, removing column ,properties and partition 
# MAGIC create table TestMainNaSqldDbdt_a(
# MAGIC   TestMainNaSqldDbdt_name string,
# MAGIC   TestMainNaSqldDbdt_id int,
# MAGIC   TestMainNaSqldDbdt_address string,
# MAGIC   TestMainNaSqldDbdt_city string,
# MAGIC   TestMainNaSqldDbdt_area string) 
# MAGIC using DELTA
# MAGIC TBLPROPERTIES(delta.deletedFileRetentionDuration="interval 31 days")
# MAGIC Location "/mnt/dataquality/unit_tests/TestMainNaSqldDbdt/input/TestMainNaSqldDbdt_a";;
# MAGIC 
# MAGIC create table TestMainNaSqldDbdt_b (
# MAGIC   TestMainNaSqldDbdt_name string,
# MAGIC   TestMainNaSqldDbdt_value int,
# MAGIC   TestMainNaSqldDbdt_city string,
# MAGIC   TestMainNaSqldDbdt_area string) 
# MAGIC using delta 
# MAGIC location '/mnt/dataquality/unit_tests/TestMainNaSqldDbdt/input/TestMainNaSqldDbdt_b' ;
# MAGIC 
# MAGIC create table TestMainNaSqldDbdt_c (
# MAGIC   TestMainNaSqldDbdt_value string,
# MAGIC   TestMainNaSqldDbdt_city string,
# MAGIC   TestMainNaSqldDbdt_area string
# MAGIC   ) 
# MAGIC using delta
# MAGIC partitioned by (TestMainNaSqldDbdt_area)
# MAGIC TBLPROPERTIES(delta.deletedFileRetentionDuration="interval 31 days") 
# MAGIC location '/mnt/dataquality/unit_tests/TestMainNaSqldDbdt/input/TestMainNaSqldDbdt_c'; 
# MAGIC 
# MAGIC create table TestMainNaSqldDbdt_d (
# MAGIC   TestMainNaSqldDbdt_name string,
# MAGIC   TestMainNaSqldDbdt_id int,
# MAGIC   TestMainNaSqldDbdt_address string,
# MAGIC   TestMainNaSqldDbdt_value int,
# MAGIC   TestMainNaSqldDbdt_city string,
# MAGIC   TestMainNaSqldDbdt_area string) 
# MAGIC using delta
# MAGIC TBLPROPERTIES(delta.deletedFileRetentionDuration="interval 31 days") 
# MAGIC location '/mnt/dataquality/unit_tests/TestMainNaSqldDbdt/input/TestMainNaSqldDbdt_d' ;
# MAGIC 
# MAGIC create table TestMainNaSqldDbdt_e (
# MAGIC   TestMainNaSqldDbdt_name string,
# MAGIC   TestMainNaSqldDbdt_id int,
# MAGIC   TestMainNaSqldDbdt_address string,
# MAGIC   TestMainNaSqldDbdt_city string,
# MAGIC   TestMainNaSqldDbdt_area string) 
# MAGIC using delta
# MAGIC TBLPROPERTIES(delta.deletedFileRetentionDuration="interval 31 days") 
# MAGIC location '/mnt/dataquality/unit_tests/TestMainNaSqldDbdt/input/TestMainNaSqldDbdt_e' ;
# MAGIC 
# MAGIC create table TestMainNaSqldDbdt_f (
# MAGIC   TestMainNaSqldDbdt_value int,
# MAGIC   TestMainNaSqldDbdt_city string,
# MAGIC   TestMainNaSqldDbdt_area string
# MAGIC   ) 
# MAGIC using delta
# MAGIC partitioned by (TestMainNaSqldDbdt_value) 
# MAGIC location '/mnt/dataquality/unit_tests/TestMainNaSqldDbdt/input/TestMainNaSqldDbdt_f'; 
# MAGIC 
# MAGIC 
# MAGIC create table TestMainNaSqldDbdt_g (
# MAGIC   TestMainNaSqldDbdt_value int,
# MAGIC   TestMainNaSqldDbdt_city string,
# MAGIC   TestMainNaSqldDbdt_area string
# MAGIC   ) 
# MAGIC using delta
# MAGIC partitioned by (TestMainNaSqldDbdt_area)
# MAGIC TBLPROPERTIES(delta.deletedFileRetentionDuration="interval 31 days",delta.logRetentionDuration="interval 30 days",delta.autoOptimize.optimizeWrite="true") 
# MAGIC location '/mnt/dataquality/unit_tests/TestMainNaSqldDbdt/input/TestMainNaSqldDbdt_g'; 
# MAGIC 
# MAGIC create table TestMainNaSqldDbdt_h (
# MAGIC   TestMainNaSqldDbdt_name string,
# MAGIC   TestMainNaSqldDbdt_id int,
# MAGIC   TestMainNaSqldDbdt_value int,
# MAGIC   TestMainNaSqldDbdt_address string,
# MAGIC   TestMainNaSqldDbdt_city string,
# MAGIC   TestMainNaSqldDbdt_area string)
# MAGIC   
# MAGIC using delta
# MAGIC partitioned by (TestMainNaSqldDbdt_area)
# MAGIC TBLPROPERTIES(delta.deletedFileRetentionDuration="interval 31 days",delta.logRetentionDuration="interval 30 days",delta.autoOptimize.optimizeWrite="true") 
# MAGIC location '/mnt/dataquality/unit_tests/TestMainNaSqldDbdt/input/TestMainNaSqldDbdt_h'; 

# COMMAND ----------

# DBTITLE 1,Insert data into test tables 
# MAGIC %sql
# MAGIC INSERT INTO TestMainNaSqldDbdt_a  VALUES('Anu','1010','ABCD','Chennai','South');
# MAGIC INSERT INTO TestMainNaSqldDbdt_b  VALUES('Anu' , 20000,'Chennai','South');
# MAGIC INSERT INTO TestMainNaSqldDbdt_c  VALUES(20000,'Chennai','South');
# MAGIC INSERT INTO TestMainNaSqldDbdt_d  VALUES('Anu','1010','ABCD',20000, 'Chennai','South');
# MAGIC INSERT INTO TestMainNaSqldDbdt_e  VALUES('Anu','1010','ABCD', 'Chennai','South');
# MAGIC INSERT INTO TestMainNaSqldDbdt_f  VALUES(20000,'Chennai','South');
# MAGIC INSERT INTO TestMainNaSqldDbdt_f  VALUES(20000,'Chennai','South');
# MAGIC INSERT INTO TestMainNaSqldDbdt_h  VALUES('Anu','1010',20000 ,'ABCD','Chennai','South');

# COMMAND ----------

# DBTITLE 1,Copy the struct file of unstructuredxmlSource to test location
#Copy the struct file of unstructuredxmlSource to test location to create a test raw table
try :
  
  structPath = '/mnt/raw/reference/unstructuredxmlSource/unstructuredxmlSourceShema_Def'
  structPathTest =testDirPath+'TestunstructuredxmlSource/TestunstructuredxmlSourceShema_Def'
  df = spark.read.format('csv').option("header","true").load(structPath)
  
  df.write.format("csv")\
           .mode("overwrite") \
           .option("header", "true")\
           .save(structPathTest)
  logTaskProgress(cursor,batchTaskId,'Successfuly copied the unstructuredxmlSourceShema_Def file to test location') 

except Exception as e:
  errorMessage = "Exception occured while copying the struct file  : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Execute getActiveDatabricksTablesSpExec and getTableHighAndLowLevelDF function
#Execute getActiveDatabricksTablesSpExec and getTableHighAndLowLevelDF function to get the list of object to test 
try:
  #List of table to be tested 
  testTableList = ['Raw_TestMainNaSqldDbdt','TestMainNaSqldDbdt_create_a','TestMainNaSqldDbdt_create_b' ,'TestMainNaSqldDbdt_create_c','TestMainNaSqldDbdt_create_d','TestMainNaSqldDbdt_a','TestMainNaSqldDbdt_b','TestMainNaSqldDbdt_c','TestMainNaSqldDbdt_d','TestMainNaSqldDbdt_e','TestMainNaSqldDbdt_f' ,'TestMainNaSqldDbdt_g' ,'TestMainNaSqldDbdt_h']
  
  #Get all the active object from  tbl_object using the function getActiveDatabricksTablesSpExec
  funcOutput = getActiveDatabricksTablesSpExec( conn
                                             ,cursor
                                             ,batchTaskId
                                             ,errorMessage
                                             ,adfPipelineName
                                             ,clusterId
                                             ,notebookName
                                             ,errorLogFileLocation)
  
  #Get all the active object high and low level information using the function getTableHighAndLowLevelDF
  tableHighDetailsDf,tableLowDetailsDf = getTableHighAndLowLevelDF( funcOutput
                                                     ,cursor
                                                     ,batchTaskId
                                                     ,errorMessage
                                                     ,adfPipelineName
                                                     ,clusterId
                                                     ,notebookName
                                                     ,errorLogFileLocation)

  #Filter the inserted test objects 
  tableHighDetailsDf = tableHighDetailsDf.selectExpr("*").where(col('object_name').isin(testTableList))
  tableLowDetailsDf = tableLowDetailsDf.selectExpr("*").where(col('object_name').isin(testTableList)) 
    
  logTaskProgress(cursor,batchTaskId,'Successfully executed the function getActiveDatabricksTablesSpExec and getTableHighAndLowLevelDF') 
except Exception as e:
  errorMessage = "Exception occured while executing functions getActiveDatabricksTablesSpExec and getTableHighAndLowLevelDF : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Run tableStructureCheck to create alter all test table
#run the tableStructureCheck function to create/alter all test table
try:
  
  tableStructureCheck(tableHighDetailsDf
                      ,tableLowDetailsDf,
                        cursor,
                        batchTaskId,
                        errorMessage,
                        adfPipelineName,
                        clusterId,
                        notebookName,
                        errorLogFileLocation)
 
  logTaskProgress(cursor,batchTaskId,'Successfully executed the functions tableStructureCheck')  
    
except Exception as e:
  errorMessage = "Exception occured while testing functions tableStructureCheck : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 1 - Check creation of simple table without partition without properties
#Check a table is created without partition without properties
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck to check a simple table creation' 
  testTable = 'TestMainNaSqldDbdt_create_a' 
  
  #Expected table columns and datatype 
  expectedColumnOrder = ['TestMainNaSqldDbdt_name', 'TestMainNaSqldDbdt_id', 'TestMainNaSqldDbdt_address' , 'TestMainNaSqldDbdt_value', 'TestMainNaSqldDbdt_city', 'TestMainNaSqldDbdt_area']
  expectedDataType = ['string','int','string','int','string','string']
 
  #Describe the table to get table details
  tableColumnCheck = spark.sql("DESCRIBE  {}".format(testTable) ) 
  tableVersionCheck = spark.sql("DESCRIBE HISTORY {}".format(testTable) ) 

  #Get of columns names from the table created 
  tablecolumnsOrder = tableColumnCheck.select("col_name").rdd.flatMap(lambda x: x).collect()
  #Get the datatype form the table created 
  tableDataType = tableColumnCheck.select("data_type").rdd.flatMap(lambda x: x).collect()
  
  #compare the expected result with the table result 
  if expectedColumnOrder == tablecolumnsOrder and expectedDataType == tableDataType :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
     
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 1 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 1: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 2 - Check creation of table without properties and with partition
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck to check table creation without properties and with partition ' 
  testTable = 'TestMainNaSqldDbdt_create_b' 
  
  #Expected table columns, datatype ,partition,properties
  expectedColumnOrder = ['TestMainNaSqldDbdt_name', 'TestMainNaSqldDbdt_id', 'TestMainNaSqldDbdt_address' , 'TestMainNaSqldDbdt_value', 'TestMainNaSqldDbdt_city', 'TestMainNaSqldDbdt_area']
  expectedDataType = ['string','int','string','int','string','string']
  expectedPartition = [['TestMainNaSqldDbdt_value']]
  expectedProperties = [{}]

  #Describe the table to get table details
  tableDescribe = spark.sql("DESCRIBE  {}".format(testTable) ) 
  tableDescribeDetail = spark.sql("DESCRIBE  DETAIL {}".format(testTable) ) 

  #Get of columns names form the table created 
  tablecolumnsOrder = tableColumnCheck.select("col_name").rdd.flatMap(lambda x: x).collect()
  #Get the datatype form the table created 
  tableDataType = tableColumnCheck.select("data_type").rdd.flatMap(lambda x: x).collect()
  #Get Properties and partition
  tablePartition = tableDescribeDetail.select("partitionColumns").rdd.flatMap(lambda x: x).collect()
  tableProperties = tableDescribeDetail.select("properties").rdd.flatMap(lambda x: x).collect()
  
  #compare the expected result with the table result 
  if expectedColumnOrder == tablecolumnsOrder and expectedDataType == tableDataType and expectedPartition == tablePartition  and expectedProperties == tableProperties  :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
 
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 2 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 2: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 3 - Check table creation with properties and without partition
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructure check table creation with properties and without partition' 
  testTable = 'TestMainNaSqldDbdt_create_c' 
  
  #Expected table columns, datatype ,partition,properties
  expectedColumnOrder = ['TestMainNaSqldDbdt_name', 'TestMainNaSqldDbdt_id', 'TestMainNaSqldDbdt_address' , 'TestMainNaSqldDbdt_value', 'TestMainNaSqldDbdt_city', 'TestMainNaSqldDbdt_area']
  expectedDataType = ['string','int','string','int','string','string']
  expectedPartition = [[]]
  expectedProperties = [{'delta.deletedFileRetentionDuration': 'interval 31 days', 'delta.autoOptimize.optimizeWrite': 'true', 'delta.logRetentionDuration': 'interval 30 days'}]
  
  #Describe the table to get table details
  tableDescribe = spark.sql("DESCRIBE  {}".format(testTable) ) 
  tableDescribeDetail = spark.sql("DESCRIBE  DETAIL {}".format(testTable) ) 

  #Get of columns names from the table created 
  tablecolumnsOrder = tableColumnCheck.select("col_name").rdd.flatMap(lambda x: x).collect()
  #Get the datatype from the table created 
  tableDataType = tableColumnCheck.select("data_type").rdd.flatMap(lambda x: x).collect()
  
  #Get Properties and partition
  tablePartition = tableDescribeDetail.select("partitionColumns").rdd.flatMap(lambda x: x).collect()
  tableProperties = tableDescribeDetail.select("properties").rdd.flatMap(lambda x: x).collect()
 
  #compare the expected result with the table result 
  if expectedColumnOrder == tablecolumnsOrder and expectedDataType == tableDataType and expectedPartition == tablePartition and expectedProperties == tableProperties :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 3 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 3: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 4 - Check table creation with properties and with partition
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck to check table creation with properties and with partition ' 
  testTable = 'TestMainNaSqldDbdt_create_d' 
  
  #Expected table columns, datatype ,partition,properties
  expectedColumnOrder =  ['TestMainNaSqldDbdt_name', 'TestMainNaSqldDbdt_id', 'TestMainNaSqldDbdt_address', 'TestMainNaSqldDbdt_value', 'TestMainNaSqldDbdt_city', 'TestMainNaSqldDbdt_area']
  
  expectedDataType = ['string', 'int', 'string', 'int', 'string', 'string']
  
  expectedPartition = [['TestMainNaSqldDbdt_value']]
  
  expectedProperties = [{'delta.deletedFileRetentionDuration': 'interval 31 days', 'delta.autoOptimize.optimizeWrite': 'true', 'delta.logRetentionDuration': 'interval 30 days'}]
  
  #Describe the table to get table details
  tableDescribe = spark.sql("DESCRIBE  {}".format(testTable) ) 
  tableDescribeDetail = spark.sql("DESCRIBE  DETAIL {}".format(testTable) ) 

  #Get of columns names from the table created 
  tablecolumnsOrder = tableColumnCheck.select("col_name").rdd.flatMap(lambda x: x).collect()
  
  #Get the datatype from the table created 
  tableDataType = tableColumnCheck.select("data_type").rdd.flatMap(lambda x: x).collect()
  
  #Get partition from the table created 
  tablePartition = tableDescribeDetail.select("partitionColumns").rdd.flatMap(lambda x: x).collect()
  
  #Get properties from the table created 
  tableProperties = tableDescribeDetail.select("properties").rdd.flatMap(lambda x: x).collect()
 
   #compare the expected result with the table result 
  if expectedColumnOrder == tablecolumnsOrder and expectedDataType == tableDataType and expectedPartition == tablePartition and expectedProperties == tableProperties :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 4 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 4: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 5 - Check raw table creation 
# Raw table column 'unstructuredxmlSource_XML_OBJECT' data type will be struct , which is retrieved from a schema file " testDirPath+'TestunstructuredxmlSource/TestunstructuredxmlSourceShema_Def' " and the column is 'xmlSchemaStruct'
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck to check raw table creation ' 
  testTable = 'Raw_TestMainNaSqldDbdt' 
  structColumn = 'unstructuredxmlSource_XML_OBJECT'
  structColumnName = 'xmlSchemaStruct'
  structSchemaLocation = testDirPath+'TestunstructuredxmlSource/TestunstructuredxmlSourceShema_Def'
  
  #Expected table columns, datatype ,partition,properties
  expectedColumnOrder = ['date','time', 'formattedMessage','EnqueuedTimestampUTC','CreatedTimestamp','LastUpdatedTimestamp','CreatedBatchID', 'LastUpdatedBatchID','SourceID' ,'unstructuredxmlSource_XML_OBJECT', 'body','SequenceNumber' ]
  
  expectedPartition = [['date','time','formattedMessage']]
  
  expectedProperties = [{'delta.deletedFileRetentionDuration': 'interval 31 days', 'delta.autoOptimize.optimizeWrite': 'true', 'delta.logRetentionDuration': 'interval 30 days'}]
  
  #describe the table created 
  tableDescribe = spark.sql("DESCRIBE  {}".format(testTable) ) 
  tableDescribeDetail = spark.sql("DESCRIBE  DETAIL {}".format(testTable) ) 

  #Get of columns names form the table created and remove the partation columns
  tablecolumnsOrder = tableDescribe.select("col_name").rdd.flatMap(lambda x: x).collect() 
  indexPart = tablecolumnsOrder.index('# Partition Information')
  
  #remove the partition column and get only the list of columns 
  tablecolumnsOrder = tablecolumnsOrder[0:indexPart]

  #Get Properties and partition
  tablePartition = tableDescribeDetail.select("partitionColumns").rdd.flatMap(lambda x: x).collect()
  tableProperties = tableDescribeDetail.select("properties").rdd.flatMap(lambda x: x).collect()
  
  #Read the schema from the struct file 
  schemaFromFile = spark.read.format("csv").option("header","true").load(structSchemaLocation).select(structColumnName).collect()[0][0] 
  
  #Read the schema of the column unstructuredxmlSource_XML_OBJECT from the raw  table 
  schemaFromTable = spark.read.format("delta").table(testTable).select(structColumn).schema.simpleString()
  schemaFromFile = "struct<{}:".format(structColumn) + schemaFromFile + ">"
  
  #Check if the schema of the table and file are same 
  schemaCheck = True if schemaFromTable == schemaFromFile else False
  
  #Compare the expected result with the table result 
  if expectedColumnOrder == tablecolumnsOrder and expectedPartition == tablePartition  and expectedProperties == tableProperties and schemaCheck  :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #Function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 5 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 5: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 6 - Alter a table by adding a column, properties and change a existing properties 
#Alter the table TestMainNaSqldDbdt_a by adding TestMainNaSqldDbdt_value column
#Changing the properties of deletedFileRetentionDuration from 31 days to 90 days and 
#Adding a new properties 'logRetentionDuration'
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck alter a table by adding a column, properties and change a existing properties' 
  testTable = 'TestMainNaSqldDbdt_a' 
  tableOverwrite = False
 
  #Expected table columns,properties
  expectedColumnOrder = ['TestMainNaSqldDbdt_name', 'TestMainNaSqldDbdt_id', 'TestMainNaSqldDbdt_address' , 'TestMainNaSqldDbdt_value', 'TestMainNaSqldDbdt_city', 'TestMainNaSqldDbdt_area']
  expectedProperties = [{'delta.deletedFileRetentionDuration': 'interval 90 days', 'delta.logRetentionDuration': 'interval 30 days'}]
  
  #describe the table created 
  tableColumnCheck = spark.sql("DESCRIBE  {}".format(testTable) )
  tablePropertiesCheck = spark.sql("DESCRIBE DETAIL {}".format(testTable))

  #Get columns from the table 
  tablecolumnsOrder = tableColumnCheck.select("col_name").rdd.flatMap(lambda x: x).collect()
  #Get columns of the table 
  tableProperties = tablePropertiesCheck.select("properties").rdd.flatMap(lambda x: x).collect()
 
  #Check if the table is in append mode 
  #adding a column, properties and changing a existing properties will not overwrite the table so it should be in append mode 
  tableOverwriteCheck = tableVersionCheck.select("operationParameters.mode").filter(tableVersionCheck.operation == 'WRITE').rdd.flatMap(lambda x: x).collect()

# Set value for table overwrite
  tableOverwrite = True if 'Append' in tableOverwriteCheck else False

  #Compare the expected result with the table result 
  if expectedProperties == tableProperties and expectedColumnOrder == tablecolumnsOrder and tableOverwrite == False:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 6 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 6: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False



# COMMAND ----------

# DBTITLE 1,Scenario - 7 Remove a column
#TestMainNaSqldDbdt_name column should be removed
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck to remove a column ' 
  testTable = 'TestMainNaSqldDbdt_b' 

  #Expected table columns
  expectedColumnOrder = ['TestMainNaSqldDbdt_value', 'TestMainNaSqldDbdt_city', 'TestMainNaSqldDbdt_area']
   
  #describe the table created 
  tableColumnCheck = spark.sql("DESCRIBE  {}".format(testTable) )
  tableVersionCheck = spark.sql("DESCRIBE HISTORY  {}".format(testTable) )
  
  #Get number of columns
  tablecolumnsOrder = tableColumnCheck.select("col_name").rdd.flatMap(lambda x: x).collect()
  print(tablecolumnsOrder)
  #check if the table is in append mode 
  #Removing a column should overwrite the table check if it overwritten after the table creation so filter the version > 1
  tableOverwriteCheck = tableVersionCheck.select("operationParameters.mode").filter(tableVersionCheck.operation == 'WRITE').filter(tableVersionCheck.version > 1 ).rdd.flatMap(lambda x: x).collect()
  
  # Set value for tableoverwrite
  tableOverwrite = True if 'Overwrite' in tableOverwriteCheck else False
  
  print(tableOverwrite)
  #Compare the expected result with the table result 
  if tablecolumnsOrder == expectedColumnOrder and   tableOverwrite :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
 
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 7')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 7: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False


# COMMAND ----------

# DBTITLE 1,Scenario - 8 change data type of a column
#TestMainNaSqldDbdt_value column is 'string' after altering it should be 'int'
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck change data type of a column ' 
  testTable = 'TestMainNaSqldDbdt_c'  
  
  #Expected table columns and datatype
  expectedColumnOrder = ['TestMainNaSqldDbdt_value', 'TestMainNaSqldDbdt_city', 'TestMainNaSqldDbdt_area']
  expectedDataType = ['int','string','string']
  
  #Describe the table and get the latest details
  tableColumnCheck = spark.sql("DESCRIBE  {}".format(testTable) )
  tableHisCheck = spark.sql("DESCRIBE history {}".format(testTable))
 
  #Get of columns names from the table created and remove the partition columns
  tablecolumnsOrder = tableColumnCheck.select("col_name").rdd.flatMap(lambda x: x).collect() 
  indexPart = tablecolumnsOrder.index('# Partition Information')
  
  #remove the partition column and get only the list of columns 
  tablecolumnsOrder = tablecolumnsOrder[0:indexPart]
  
  #Get datatype of columns from the table created and remove the partition columns
  tableNewDataType = tableColumnCheck.select("data_type").rdd.flatMap(lambda x: x).collect() 
  lastIndex = tableNewDataType.index('')
  
  #remove the partition column and get only the list of datatype 
  tableNewDataType = tableNewDataType[0:lastIndex]
 
  #Removing a column should overwrite the table check if it overwritten  after the table creation so filter the version > 1
  tableOverwriteCheck = tableHisCheck.select("operationParameters.mode").filter(tableHisCheck.operation == 'WRITE').filter(tableHisCheck.version > 1 ).rdd.flatMap(lambda x: x).collect()

  #set the value for tableOverwrite
  tableOverwrite = True if 'Overwrite' in tableOverwriteCheck else False

  #Compare the expected result with the table result 
  if tablecolumnsOrder == expectedColumnOrder  and  tableNewDataType == expectedDataType and tableOverwrite :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'

  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 8')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 8: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario - 9 Add partitioning to the existing table 
#new partition should be added with the existing column TestMainNaSqldDbdt_value
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck for add partitioning  ' 
  testTable = 'TestMainNaSqldDbdt_d' 

  #Expected table partition
  expectedPartition = [['TestMainNaSqldDbdt_value']]
   
  #Describe the table to get the details
  tablePartitionCheck = spark.sql("DESCRIBE DETAIL {}".format(testTable) )
  tableOverwriteCheck = spark.sql("DESCRIBE HISTORY  {}".format(testTable) )
   
  #Get partition Columns
  tablePartition = tablePartitionCheck.select("partitionColumns").rdd.flatMap(lambda x:x).collect()
  
  #adding partitionshould overwrite the table
  #check if it overwrite after the table creation so filter the version > 1
  tableOverwriteCheck = tableOverwriteCheck.select("operationParameters.mode").filter(tableOverwriteCheck.operation == 'WRITE').filter(tableOverwriteCheck.version > 1 ).rdd.flatMap(lambda x: x).collect()

  #set tableOverwrite value
  tableOverwrite = True if 'Overwrite' in tableOverwriteCheck else False
   
 #compare the expected result with table result
  if expectedPartition == tablePartition and tableOverwrite:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
 
 # function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 9 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 9: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario - 10 Add column and add partitioning
# Add a new column TestMainNaSqldDbdt_value and partition the same column
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck add column and add partitioning' 
  testTable = 'TestMainNaSqldDbdt_e' 
  
  #Expected table partition ,column
  expectedPartition = [['TestMainNaSqldDbdt_value']]
  expectedColumnOrder = ['TestMainNaSqldDbdt_name', 'TestMainNaSqldDbdt_id', 'TestMainNaSqldDbdt_address' , 'TestMainNaSqldDbdt_value', 'TestMainNaSqldDbdt_city', 'TestMainNaSqldDbdt_area']

  #describe the table and get the latest details 
  tableColumnCheck = spark.sql("DESCRIBE  {}".format(testTable) )
  tableVersion = spark.sql("DESCRIBE HISTORY {}".format(testTable) )
 
  #Get of columns names form the table created and remove the partition columns
  tableColumnsOrder = tableColumnCheck.select("col_name").rdd.flatMap(lambda x: x).collect() 
  indexPart = tableColumnsOrder.index('# Partition Information')
  
  #remove the partition column and get only the list of columns 
  tableColumnsOrder = tableColumnsOrder[0:indexPart]
  
  #add column and partitioning should overwrite the table
  #check if it overwritten  after the table creation so filter the version > 1
  tableOverwriteCheck = tableVersion.select("operationParameters.mode").filter(tableVersion.operation == 'WRITE').filter(tableVersion.version > 1 ).rdd.flatMap(lambda x: x).collect()

  #set values to tableOverwrite
  tableOverwrite = True if 'Overwrite' in tableOverwriteCheck else False
     
 #compare the expected result with table result
  if tablePartition == expectedPartition and tableOverwrite and tableColumnsOrder == expectedColumnOrder :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
 
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 10 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 10: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,scenario - 11 Remove partitioning
# table TestMainNaSqldDbdt_f has partition column TestMainNaSqldDbdt_value ,it should be removed
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck to remove partitioning' 
  testTable = 'TestMainNaSqldDbdt_f' 
 
 #Expected table partition 
  expectedPartition = [[]]
  
  #describe the table and get the latest details   
  tablePartitionCheck = spark.sql("DESCRIBE DETAIL {}".format(testTable) )
  tableCheck = spark.sql("DESCRIBE HISTORY {}".format(testTable) )
  
  #Get partition details 
  tablePartition = tablePartitionCheck.select("partitionColumns").rdd.flatMap(lambda x:x).collect()
  
  #Removing partition should overwrite the table
  #check if it overwritten  after the table creation so filter the version > 1
  tableOverwriteCheck = tableCheck.select("operationParameters.mode").filter(tableCheck.operation == 'WRITE').filter(tableCheck.version > 1 ).rdd.flatMap(lambda x: x).collect()

  #set value for tableOverwrite
  tableOverwrite = True if 'Overwrite' in tableOverwriteCheck else False
  
  #compare the expected result with table result
  if expectedPartition == tablePartition and tableOverwrite :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 11 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 11: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario - 12 Change table partitioning
#TestMainNaSqldDbdt_area is the existing partition it should be changed to TestMainNaSqldDbdt_city
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck to change partitioning' 
  testTable = 'TestMainNaSqldDbdt_g' 
  
  #Expected table partition 
  expectedPartition = [['TestMainNaSqldDbdt_city']]
   
  #describe the table and get the latest details   
  tablePartitionCheck = spark.sql("DESCRIBE DETAIL {}".format(testTable))
  tableCheckVersion = spark.sql("DESCRIBE HISTORY  {}".format(testTable) )
  
  #Get number of columns
  tablePartition = tablePartitionCheck.select("partitionColumns").rdd.flatMap(lambda x:x).collect()
  
  #Change partitioning should overwrite the table

  tableOverwriteCheck = tableCheckVersion.select("operationParameters.mode").filter(tableCheckVersion.operation == 'WRITE').rdd.flatMap(lambda x: x).collect()
  
 
  #set value for tableOverwrite
  tableOverwrite = True if 'Overwrite' in tableOverwriteCheck else False
   
  #compare the expected result with table result
  if expectedPartition == tablePartition and tableOverwrite :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'

  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
#   logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 12 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 12: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario - 13 Reorder columns in table
# existing column order is [TestMainNaSqldDbdt_name,TestMainNaSqldDbdt_id,TestMainNaSqldDbdt_value,TestMainNaSqldDbdt_address,TestMainNaSqldDbdt_city,TestMainNaSqldDbdt_area]
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck add column and add partitioning' 
  testTable = 'TestMainNaSqldDbdt_h' 
  
  #expected column order
  expectedColumnOrder = ['TestMainNaSqldDbdt_name','TestMainNaSqldDbdt_id','TestMainNaSqldDbdt_address','TestMainNaSqldDbdt_value', 'TestMainNaSqldDbdt_city', 'TestMainNaSqldDbdt_area']
  
  #describe table to get the details
  tableColumnCheck = spark.sql("DESCRIBE  {}".format(testTable)) 
  tableCheckHistory = spark.sql("DESCRIBE HISTORY  {}".format(testTable) )
 
  #Get of columns names form the table created and remove the partation columns
  tablecolumnsOrder = tableColumnCheck.select("col_name").rdd.flatMap(lambda x: x).collect() 
  indexPart = tablecolumnsOrder.index('# Partition Information')
  #remove the partition column and get only the list of columns 
  tablecolumnsOrder = tablecolumnsOrder[0:indexPart]
  
  #check if the table is table is in Overwrite mode 
  #Reordering a column should overwrite the table check if it overwritten after the table creation so filter the version > 1
  tableOverwriteCheck = tableCheckHistory.select("operationParameters.mode").filter(tableCheckHistory.operation == 'WRITE').filter(tableCheckHistory.version > 1 ).rdd.flatMap(lambda x: x).collect()

  #set value for tableOverwrite
  tableOverwrite = True if 'Overwrite' in tableOverwriteCheck else False
 
 #compare the expected result with table result
  if tablecolumnsOrder == expectedColumnOrder and  tableOverwrite   :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
   
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 13 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 13: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Alter the struct file to add a new column
#copy the unstructuredxmlSourceShema struct  file and add a new column 'Raw_TestColumn' 
try :
  
  #struct test location
  structPathTest =testDirPath+'TestunstructuredxmlSource/TestunstructuredxmlSourceShema_Def'
  #struct unstructuredxmlSource location
  structCDILunstructuredxmlSource = '/mnt/raw/reference/unstructuredxmlSource/unstructuredxmlSourceShema_Def'
  
#Read the struct from the unstructuredxmlSource location and add a new "Raw_TestColumn" column to the struct file and write it into test location 
  df = spark.read.format('csv').option("header","true").load(structCDILunstructuredxmlSource)
  xmlSchemaStruct = df.select("xmlSchemaStruct").collect()[0][0]
  xmlSchemaStruct = xmlSchemaStruct.replace("MessageType:string", "MessageType:string,Raw_TestColumn:string")

  dfOut = df.withColumn("xmlSchemaStruct", lit(xmlSchemaStruct))
  xmlSchemaStructAfterAlter = dfOut.select("xmlSchemaStruct").collect()[0][0]
  
#write the struct file in test location 
  dfOut.write.format("csv")\
             .mode("overwrite") \
             .option("header", "true")\
             .save(structPathTest)
  
  logTaskProgress(cursor,batchTaskId,'Successfully copied the struct file to the test location')  
    
except Exception as e:
  errorMessage = "Exception occured while testing functions copying struct file : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Execute tableStructureCheck function to alter the Raw table 
try:
  testTableList = ['Raw_TestMainNaSqldDbdt']  
  
  #Get all the active object from  tbl_object using the function getActiveDatabricksTablesSpExec
  funcOutput = getActiveDatabricksTablesSpExec( conn
                                             ,cursor
                                             ,batchTaskId
                                             ,errorMessage
                                             ,adfPipelineName
                                             ,clusterId
                                             ,notebookName
                                             ,errorLogFileLocation)
  
  #Get all the active object high and low level information using the function getTableHighAndLowLevelDF
  tableHighDetailsDf,tableLowDetailsDf = getTableHighAndLowLevelDF( funcOutput
                                                     ,cursor
                                                     ,batchTaskId
                                                     ,errorMessage
                                                     ,adfPipelineName
                                                     ,clusterId
                                                     ,notebookName
                                                     ,errorLogFileLocation)

   #Filter testTableList from the inserted test objects  
  tableHighDetailsDf = tableHighDetailsDf.selectExpr("*").where(col('object_name').isin(testTableList))
  tableLowDetailsDf = tableLowDetailsDf.selectExpr("*").where(col('object_name').isin(testTableList)) 
  
  #Execute TbleStructureCheck Function
  tableStructureCheck(tableHighDetailsDf
                      ,tableLowDetailsDf,
                        cursor,
                        batchTaskId,
                        errorMessage,
                        adfPipelineName,
                        clusterId,
                        notebookName,
                        errorLogFileLocation)
 
  logTaskProgress(cursor,batchTaskId,'Unit test performed to test functions tableStructureCheck')  
    
except Exception as e:
  errorMessage = "Exception occured while testing functions tableStructureCheck : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario - 14 Check if the raw table is altered 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test function tableStructureCheck if the raw table is altered ' 
  testTable = 'Raw_TestMainNaSqldDbdt' 

  #Read the raw delta table after alter
  schemaFromTable = spark.read.format("delta").table('Raw_TestMainNaSqldDbdt').select('unstructuredxmlSource_XML_OBJECT').schema.simpleString()
  
  #check the new column added is present in the table struct
  schemaNewColumn = True if "Raw_TestColumn:string" in schemaFromTable else False 
  
  #check if the newly column is added column is present
  if schemaNewColumn :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
 
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for create/alter table scenario 13 ')  
except Exception as e:
  errorMessage = "Exception occurred while testing  create/alter table scenario 13: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Drop the input Delta table if it exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_create_a;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_create_b;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_create_c;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_create_d;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_a;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_b;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_c;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_d;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_e;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_f;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_g;
# MAGIC DROP TABLE IF EXISTS TestMainNaSqldDbdt_h;
# MAGIC DROP TABLE IF EXISTS Raw_TestMainNaSqldDbdt;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_create_a',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_create_b',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_create_c',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_create_d',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_a',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_b',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_c',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_d',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_e',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_f',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_g',recurse=True)
dbutils.fs.rm(testDirPath+'TestMainNaSqldDbdt_h',recurse=True)
dbutils.fs.rm(testDirPath+'Raw_TestMainNaSqldDbdt',recurse=True)
dbutils.fs.rm(testDirPath+'TestunstructuredxmlSource',recurse=True)

# COMMAND ----------

# DBTITLE 1,Cleanup metadata
#run cleanup scripts
try:
  cleanup1 = open("{}MainNaSqldDbdtCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup1)
  
  while cursor.nextset():
    x = 1  
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage = "Exception occured while executing clean up scripts: " + str(e)
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