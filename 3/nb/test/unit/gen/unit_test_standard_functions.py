# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>Unit test for generic_notebook_function</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Unit test for Standard Function for all notebook </td></tr>
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
# MAGIC </table> 
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
# MAGIC     <td>Added unstructuredxmlSource helper functions unit test cases 
# MAGIC       <br>Changed input, output and results paths for all functions
# MAGIC       <br>Added unit test cases for new functions convertSinglePandasToSparkDf,convertPandasToSparkDf,markDatesLoaded,performStringreplaceCastingAndNaming,cleanPiiHashLiteralAttributes,concatenateFunc,computeHashValue,piiGetHashVariables,spExecHighAndLowLevel,getStructForHighAndLowLevelDf,getSelectAndWithColumnDetails,spExecunstructuredxmlSourceGetObjectDetails  ,generateMappigExpression,generateDict
# MAGIC       <br>Modified unit tests for getCompiledWithExpressionForUdf, performStringreplaceCastingAndNaming, getWithExpressionsAndSelectList to test for udf type of hash
# MAGIC       <br>Modified unit test for selectScrFromDest to include preSelect and postSelect
# MAGIC       <br>Modified unit tests for getWithExpressionsAndSelectList to test changes for casting columns. Types BINARY, DATE, STRING, TIMESTAMP will not be cast explicitly
# MAGIC       <br>Added sourceObjectId for markDatesLoaded unit tests
# MAGIC       <br>Corrections to expected output for getWithExpressionsAndSelectList tets
# MAGIC       <br>Added additional tests for ignore and unpivot computed types in "Test getWithExpressionsAndSelectList"<br>correct name of function in Test generateMappingExpression<br>Added join condition tests in Test getWithExpressionsAndSelectList
# MAGIC       <br>Added test for the new function
# MAGIC       writeToTargetWithOverwrite, 
# MAGIC       writeToTargetTableWithOverwrite,
# MAGIC       identifyIfNewDataInDependentTables,
# MAGIC       createOrReplaceTemporaryView,  
# MAGIC       selectDestFromJoinSrc,
# MAGIC       selectDestFromSrcTable
# MAGIC       <br>Added getBatchLevelObjectRowCount function test case
# MAGIC       <br>Updated markDatesLoaded function call and added create table for writeToTargetWithOverwrite
# MAGIC       <br>Amended Pandas DF to allow upgrade of pandas / spark 3 cluster runtimes
# MAGIC       <br>Added object reference details function test case scenario
# MAGIC       <br>Updated Test cases for GetWithExpressionsAndSelectList with strincasting function changes
# MAGIC   </tr>
# MAGIC     <tr>
# MAGIC     <td>2021/12/03</td>
# MAGIC     <td>Akhilesh Kothari</td>
# MAGIC     <td>Added unit test for function readExcelFileIntoSparkDf to read excel file into spark dataframe</td>
# MAGIC   </tr>  
# MAGIC   </tr>
# MAGIC     <tr>
# MAGIC     <td>2021/12/08</td>
# MAGIC     <td>Akhilesh Kothari</td>
# MAGIC     <td>Added unit tests for functions populateObjectViewDateAvailability and getSourceObjectName</td>
# MAGIC   </tr>  
# MAGIC   </tr>
# MAGIC     <tr>
# MAGIC     <td>2021/12/14</td>
# MAGIC     <td>Akhilesh Kothari</td>
# MAGIC     <td>Added unit test for function getViewDefinition which replaces function getSourceObjectName</td>
# MAGIC   </tr>  
# MAGIC   </tr>
# MAGIC     <tr>
# MAGIC     <td>2022/01/04</td>
# MAGIC     <td>Chitransh Srivastava</td>
# MAGIC     <td>Added unit testing for new functions getAllViewDefinitions and createAlterViews</td>
# MAGIC   </tr>
# MAGIC    </tr>
# MAGIC     <tr>
# MAGIC     <td>2021/03/01</td>
# MAGIC     <td>Akhilesh Kothari</td>
# MAGIC     <td>Added unit test for function spExecGetPKsType2Fields, which gets object details
# MAGIC     <br>Added unit tst for function  addAuditAndHBKColumnsOnDF, which adds audit & HBK columns to dataframe</td>
# MAGIC   </tr> 
# MAGIC   <tr>
# MAGIC     <td>2022/01/25</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>Added unit test for sqdw connection and populate date availablity for master view</td>
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
  import json
  from itertools import chain
  from datetime import datetime, timedelta
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,DecimalType,ShortType,LongType
  from pyspark.sql.functions import lit,explode,col,concat_ws,create_map,array,concat,coalesce,collect_list
  from pyspark import SparkContext,SparkConf
  from functools import reduce
except Exception as e:
  errorMessage = "Exception occured while import modules " + str(e)
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
  createdDate = int(currentTs.strftime('%Y%m%d'))
  #get the hour
  createdHour = currentTs.hour
  createdTimestamp = currentTs
  lastUpdatedTimestamp = currentTs
  #use the same time for the date also
  date=currentTs

  #PARAMETER FOR LOG_ERROR
  errorLine = ''
  errorMessage = ''
  adfPipelineName = ''
  clusterId = ''
  notebookName = ''
  batchId = 1
  batchTaskId = 1
  
  #parameter for log_task_end
  batchTaskStatus = ''
  batchTaskSourceRows = 0
  batchTaskRowsLoaded = 0
  batchTaskRejectRows = 0
  batchTaskResult = ''
  batchTaskResultLocation = ''
  batchTaskProgressMessage = ''  
    
  #Parameter for logging into tbl_unit_test_result  
  testCaseScenario = ''
  outputLocation = ''
  testObject = 'notebook'
  testObjectName = 'standardFunction'
  requiredInputParameter = 'batchTaskId,errorMessage,adfPipelineName,clusterID,notebookName'
  #path for datasets used in Standard functions
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  #Paths to create table
  tableInputPath = testInputPath.replace('/dbfs','')
  tableOutputPath = testOutputPath.replace('/dbfs','')
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')
  
except Exception as e:
  errorMessage = "Exception occured while variable declaration " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try:   
  # Get input parameters for stored procedure
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
   
  #GET batchTaskId FROM WIDGETS
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")
 
  #Call the getLoggingPath function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occured while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Test sqlDbConn
try :
  #Parameters for executing sql_db_conn
  testCaseScenario ='Database connection check'
  dbconn=dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  
  #Call sql_db_conn to connect to a database
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  #Execute a simple select query to check if connection is successfull 
  connCheckDF = pd.read_sql_query("select GETDATE() as the_date",conn)
  #Converting panda dataframe to spark dataframe 
  connCheckDFSpark = spark.createDataFrame(connCheckDF)
  
  if connCheckDFSpark.count() > 0:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
    
  #Log overall test status into unit test table 
  #logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
   #                    testCaseScenario,executionOutputStatus,outputLocation,
    #                   datetime.now(),testResult)    
  logTaskProgress(cursor,batchTaskId,'Successful Tested sqlDbConn function')
except Exception as e:
    errorMessage = "Exception occured while connecting to database: " + str(e)
    logToFile(errorLogFileLocation,errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Drop the table path to clean up the data
try:
    dbutils.fs.rm(tableInputPath + 'inputSrcTable', recurse = True)
    dbutils.fs.rm(tableOutputPath + 'outputDeltaTable',recurse = True)
    dbutils.fs.rm(tableOutputPath + 'testDependentFirst',recurse = True)
    dbutils.fs.rm(tableOutputPath + 'testDependentSecond',recurse = True)
    dbutils.fs.rm(tableOutputPath + 'writeToTargetWithOverwrite',recurse = True)
    
    logTaskProgress(cursor,batchTaskId,'Successful deleted the input source table path')
except Exception as e:
    errorMessage = "Exception occured while deleting the input source table path: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Drop the Delta table if it exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS inputSrcTable;
# MAGIC DROP TABLE IF EXISTS outputDeltaTable;
# MAGIC DROP TABLE IF EXISTS testDependentFirst;
# MAGIC DROP TABLE IF EXISTS testDependentSecond;
# MAGIC DROP TABLE IF EXISTS writeToTargetWithOverwrite;
# MAGIC DROP VIEW IF EXISTS TestCreateOrReplaceTemporaryView;

# COMMAND ----------

# DBTITLE 1,Create Delta table for Testing
# MAGIC %sql
# MAGIC create table inputSrcTable(
# MAGIC RECORD_ID int,
# MAGIC PRODUCT_NAME string,
# MAGIC PERSON_NAME string,
# MAGIC PERSON_ID int,
# MAGIC PERSON_BALANCE float,
# MAGIC PURCHASE_PRICE float,
# MAGIC CHARITY_DONATION float,
# MAGIC CATEGORY string,
# MAGIC SUB_CATEGORY string)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/standardFunction/input/inputSrcTable";
# MAGIC 
# MAGIC create table outputDeltaTable(
# MAGIC RECORD_ID int,
# MAGIC PRODUCT_NAME string,
# MAGIC PERSON_NAME string,
# MAGIC PERSON_ID int,
# MAGIC PERSON_BALANCE float,
# MAGIC PURCHASE_PRICE float,
# MAGIC CHARITY_DONATION float,
# MAGIC CATEGORY string,
# MAGIC SUB_CATEGORY string)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/standardFunction/output/outputDeltaTable" ;
# MAGIC 
# MAGIC create table testDependentFirst(
# MAGIC RECORD_ID int,
# MAGIC PRODUCT_NAME string,
# MAGIC PERSON_NAME string,
# MAGIC PERSON_ID int,
# MAGIC PERSON_BALANCE float,
# MAGIC PURCHASE_PRICE float,
# MAGIC CHARITY_DONATION float,
# MAGIC CATEGORY string,
# MAGIC SUB_CATEGORY string)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/standardFunction/output/testDependentFirst" ;
# MAGIC 
# MAGIC create table testDependentSecond(
# MAGIC RECORD_ID int,
# MAGIC PRODUCT_NAME string,
# MAGIC PERSON_NAME string,
# MAGIC PERSON_ID int,
# MAGIC PERSON_BALANCE float,
# MAGIC PURCHASE_PRICE float,
# MAGIC CHARITY_DONATION float,
# MAGIC CATEGORY string,
# MAGIC SUB_CATEGORY string)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/standardFunction/output/testDependentSecond" ;
# MAGIC 
# MAGIC create table writeToTargetWithOverwrite(
# MAGIC RECORD_ID int,
# MAGIC PRODUCT_NAME string,
# MAGIC PERSON_NAME string,
# MAGIC PERSON_ID int,
# MAGIC PERSON_BALANCE float,
# MAGIC PURCHASE_PRICE float,
# MAGIC CHARITY_DONATION float,
# MAGIC CATEGORY string,
# MAGIC SUB_CATEGORY string)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/standardFunction/output/writeToTargetWithOverwrite" ;

# COMMAND ----------

# DBTITLE 1,Insert data into source table
try:
  fileName = "inputSrcFile.csv"
  inputFormat = "delta"
  srcFile = tableInputPath + fileName
  tgtTableLocation = tableInputPath + 'inputSrcTable'
  schema = StructType(
                         [StructField("RECORD_ID"            ,    IntegerType()  ,    True),
                         StructField("PRODUCT_NAME"          ,    StringType()   ,    True),
                         StructField("PERSON_NAME"           ,    StringType()   ,    True),
                         StructField("PERSON_ID"             ,    IntegerType()  ,    True),
                         StructField("PERSON_BALANCE"        ,    FloatType()    ,    True),
                         StructField("PURCHASE_PRICE"        ,    FloatType()    ,    True),  
                         StructField("CHARITY_DONATION"      ,    FloatType()    ,    True),
                         StructField("CATEGORY"              ,    StringType()   ,    True),
                         StructField("SUB_CATEGORY"          ,    StringType()   ,    True)]
                     )
  #Create dataframe from csv file
  inputCsvDF = spark.read.format("csv").option("header","true").schema(schema).load(srcFile)
  #Load into delta table
  inputCsvDF.write.format(inputFormat).mode("overwrite").save(tgtTableLocation)
except Exception as e:
  assert False

# COMMAND ----------

# DBTITLE 1,Test convertSinglePandasToSparkDf
try:
  #Parameters to run convertSinglePandasToSparkDf function
  testCaseScenario ='Convert pandas dataframe to spark dataframe'
  requiredInputParameter = 'sourcePandasDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  fileName = "inputSrcFile.csv"
  sourceFile = testInputPath + fileName
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  # read file as pandas dataframe
  sourceAsPandaDataframe = pd.read_csv(sourceFile)
  convertToSparkDataFrame = convertSinglePandasToSparkDf(sourceAsPandaDataframe,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  if convertToSparkDataFrame.count() == 3:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested convertSinglePandasToSparkDf function')
except Exception as e:
  errorMessage = "Exception occured while testing convertSinglePandasToSparkDf function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Test writeToTarget
try :
  #Variable need to run write_to_target function 
  testCaseScenario ='writeToTarget function test if the given dataframe is written to the given location'
  requiredInputParameter = 'DestinationDF,targetTableLocation,targetObjectName,targetFormat,cursor,batchTaskId,adfPipelineName,clusterName,notebookName,errorLogFileLocation' 
  sourceTableName = 'inputSrcTable'
  testTableLocation = tableInputPath + sourceTableName
  SourceDF = spark.read.format('delta').option("header",True).load(tableInputPath + sourceTableName) 
  targetObjectName = 'writeToTargetFunction'
  targetFormat = 'delta'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  targetLocation = tableOutputPath + 'writeToTarget' 
  #Run write_to_target function passing the dataframe and the location the output file is to be created
  desDF = writeToTarget(SourceDF,tableOutputPath,targetObjectName,targetFormat,
                            cursor,batchTaskId,adfPipelineName,clusterId,
                            notebookName,errorLogFileLocation) 
  
  #After function execution read the target location and check if the file is created 
  fileCheck = bool(dbutils.fs.ls(tableOutputPath))

  if fileCheck == True:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested writeToTarget')
except Exception as e:
    errorMessage="Exception occured while testing writeToTarget function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False


# COMMAND ----------

# DBTITLE 1,Test writeToTargetWithOverwrite
try :
  #Variable need to run writeToTargetWithOverwrite function 
  testCaseScenario ='writeToTargetWithOverwrite function test if the given dataframe is over written in the target location'
  requiredInputParameter = 'DestinationDF,targetTableLocation,targetObjectName,targetFormat,cursor,batchTaskId,adfPipelineName,clusterName,notebookName,errorLogFileLocation' 
  targetObjectName = ''
  targetFormat = 'delta'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  targetLocation = tableOutputPath + 'writeToTargetWithOverwrite'
  #populate the data in target before overwriting 
  sourceFileName = 'inputSrcFile.csv'
  sourceFileNameDf = spark.read.format("csv").option("header","true").schema(schema).load(tableInputPath+sourceFileName)

  sourceFileNameDf.write.format('delta').mode('append').save(targetLocation)
  
  #Read the data to overwrite the target location
  sourceFileToOverwrite = 'srcFileOverwrite.csv'
  SourceDFWithOverwrite = spark.read.format("csv").option("header","true").schema(schema).load(tableInputPath+sourceFileToOverwrite)
 
  #Read the value before overwriting 
  outputCountBeforeFunctionCall = spark.read.format("delta").load(targetLocation).select('CATEGORY').where('RECORD_ID = 100023').collect()

  #Function call to overwrite the dataframe in target location
  writeToTargetWithOverwrite(SourceDFWithOverwrite,targetLocation,targetObjectName,targetFormat
                               ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  #Read the value after overwriting
  outputCountAfterFunctionCall = spark.read.format("delta").load(targetLocation).select('CATEGORY').where('RECORD_ID = 100023').collect()

  #compare the values before and after overwriting , when value are not equal then the file has overwritten.
  if outputCountBeforeFunctionCall !=  outputCountAfterFunctionCall :
    testResult = 'success'
    executionOutputStatus = 'As Expected'
   
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully Tested writeToTargetWithOverwrite')
except Exception as e:
    errorMessage="Exception occured while testing writeToTargetWithOverwrite function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test writeToTargetTableWithOverwrite
try :
  #Variable need to run writeToTargetTableWithOverwrite function 
  testCaseScenario ='writeToTargetTableWithOverwrite function test if the given dataframe is written to the given location in overwrite mode'
  requiredInputParameter = 'DestinationDF,targetTableLocation,targetObjectName,targetFormat,cursor,batchTaskId,adfPipelineName,clusterName,notebookName,errorLogFileLocation' 
  sourceFileName = 'inputSrcFile.csv'
  targetTableLocation = tableOutputPath + 'outputDeltaTable'
  targetObjectName = 'outputDeltaTable'
  targetFormat = 'delta'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
 
 #Load a new source file with 3 row to the targetObjectName
  SourceDF = spark.read.format("csv").option("header","true").schema(schema).load(tableInputPath + sourceFileName)
  

  #Load a new source file with 5 row to overwrite the existing data in targetObjectName 
  sourceFileToOverwrite = 'srcFileOverwrite.csv'
  SourceDFWithOverwrite = spark.read.format("csv").option("header","true").schema(schema).load(tableInputPath + sourceFileToOverwrite)
 
  #Run write_to_target function passing the dataframe and the location the target table 
  writeToTarget(SourceDF,targetTableLocation,targetObjectName,targetFormat,
                            cursor,batchTaskId,adfPipelineName,clusterId,
                            notebookName,errorLogFileLocation) 


  #Check the row count in the output table before loading 
  outputCountBeforeFunctionCall = spark.sql("SELECT * FROM {}".format(targetObjectName)).count()
  
  #Function call to overwrite the target location
  
  writeToTargetTableWithOverwrite(SourceDFWithOverwrite,targetObjectName,targetFormat
                                    ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
 #Check the row count in the output location after overwriting with existing data
  outputCountAfterFunctionCall = spark.sql("SELECT * FROM {}".format(targetObjectName)).count()
  
  
  if outputCountBeforeFunctionCall == 3 and outputCountAfterFunctionCall == 5:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully Tested writeToTargetTableWithOverwrite')
except Exception as e:
    errorMessage="Exception occured while testing writeToTargetTableWithOverwrite function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test Connection is closed
try :
  testCaseScenario ='Test if the database connection is closed'
  requiredInputParameter = 'cursor,conn,batchTaskId, batchTaskSourceRows,batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,                            batchTaskResultLocation,adfPipelineName,clusterName,notebookName,errorLogFileLocation'
  outputLocation = ''
  
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  
  #Run taskEndAndCloseConn function to close the connection 
  taskEndAndCloseConn(cursor,conn,batchTaskId, batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  try:
    #Connect to databse after connection close
    connCheckDF = pd.read_sql_query("select getdate() as my_date",conn)
  except Exception as ex:
    if 'Attempt to use a closed connection' in str(ex):
      testResult = 'success'
      executionOutputStatus = 'As Expected'
      
      #reconnect to the database    
      #Log into a file that connection is closed so not able to connect to database
      dbconn = dbutils.secrets.get(scope="KeyVault", key="sql-dbrks-connection-01")
      #Call sql_db_conn to connect to a database
      conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
      
  logTaskProgress(cursor,batchTaskId,'Successful Tested taskEndAndCloseConn ')
  
except Exception as e:
    errorMessage="Exception occured while testing taskEndAndCloseConn function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test GetSelectPath
try :
  #Variable need to run write_to_target function 
  testScenarioReference = 'GetSelectPath'
  testCaseScenario ='Verify getting correct path from the given path'
  requiredInputParameter = 'DestinationDF,targetTableLocation,targetObjectName,targetFormat,cursor,batchTaskId,errorLine,adfPipelineName,clusterName,notebookName,errorLogFileLocation' 
  testResultLocation = testResultsPath + testScenarioReference + '/batch_task_id=' + str(batchTaskId) + '/' 
  
    #test number,full string, current attribute, expected output
  testData =  [ (1,'pm.[x].[y]'  , 'pm'   , 'pm')
               ,(2,'pm.[x].[y]'  , '[x]'  , 'pm.x')
               ,(3,'pm.[x].[y]'  , '[y]'  , 'x.y')
               ,(4,'pm.x.[y]'    , '[y]'  , 'pm.x.y')
               ,(5,'pm.[x].e.[y]', 'e'    , 'x.e')
               ,(6,'pm.[x].e.[y]', '[y]'  , 'x.e.y')
               ,(7,'hello'       , 'hello', 'hello') ]
  overallTestResult = 'success'
  allTestResults = []
  
  #Loop over for each test case 
  for testCase in testData:
    testNumber = 'Test-'+str(testCase[0])
    inputString = testCase[1]
    currentAttribute = testCase[2]
    expectedOutput = testCase[3]
    #Call the get_select_path function
    outputString = getSelectPath(inputString,currentAttribute,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

    #Now check the output equals the expected result

    if outputString == expectedOutput: #If test result is true, make test result is success
      testResult = 'success'
      executionOutputStatus = 'As Expected'
      
      
    else:
      testResult = 'failed' #initialise the test result
      executionOutputStatus = 'Mismatched'
      overallTestResult = 'failed'
      
    #testid, inputString, acurrentAttribute, expectedOutput, outputString and testResult
    testDataSet = (testNumber, inputString, currentAttribute, expectedOutput, outputString, testResult)
    allTestResults.append(testDataSet)
  
  if overallTestResult == 'failed':
    if not os.path.exists(testResultLocation):
      os.makedirs(testResultLocation)
    #Write output to file in the ouptut location when ther is a failed scenario    
    file = open(testResultLocation + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '_outputResultFile','w')
    file.write(str(allTestResults))
    file.close()
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),overallTestResult)

  logTaskProgress(cursor,batchTaskId,'Successful tested GetSelectPath')
except Exception as e:
    errorMessage = "Exception occured while testing GetSelectPath function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test stripLastAttributeFromColumnSelect
try :
  #Variable need to run stripLastAttributeFromColumnSelect function 
  testScenarioReference = 'StripLastAttributeFromColumnSelect'
  testCaseScenario ='Verify stripLastAttributeFromColumnSelect function is giving correct path from the given path'
  requiredInputParameter = 'DestinationDF,targetTableLocation,targetObjectName,targetFormat,cursor,batchTaskId,adfPipelineName,clusterName,notebookName,errorLogFileLocation' 
  testResultLocation = testResultsPath + testScenarioReference + '/batch_task_id=' + str(batchTaskId) + '/'
    
    #full string, current attribute, expected output
  #test number, inputValue, expected result
  testData = [ (1,'[x]'          , '[x]')
              ,(2,'[x].[y]'      , '[x].[y]')
              ,(3,'[x].[y].z'    , '[x].[y]')
              ,(4,'[x].p.[q].r'  , '[x].p.[q]')
              ,(5,'p.q.r'        , None)
              ,(6,'p.q.r.[x].f.g', 'p.q.r.[x]')
              ,(7,'hello there'  , None)]
  
  overallTestResult = 'success'
  allTestResults = []
  
  #Loop over for each test case 
  for testCase in testData:
    testNumber = 'Test-'+str(testCase[0])
    inputString = testCase[1]
    expectedOutput = testCase[2]
    
    #Call the stripLastAttributeFromColumnSelect function
    outputString = stripLastAttributeFromColumnSelect(inputString,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

    #Now check the output equals the expected result

    if outputString == expectedOutput: #If test result is true, make test result is success
      testResult = 'success'
      executionOutputStatus = 'As Expected'
      
    else:
      testResult = 'failed' #initialise the test result
      executionOutputStatus = 'Mismatched'
      overallTestResult = 'failed'
#   testid, testresult, actual result, expected result
    testDataSet = (testNumber, inputString, expectedOutput, outputString, testResult)
    allTestResults.append(testDataSet)
  
  if overallTestResult == 'failed':
    if not os.path.exists(testResultLocation):
      os.makedirs(testResultLocation)
    #Write output to file in the ouptut location when ther is a failed scenario    
    file = open(testResultLocation + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '_outputResultFile','w')
    file.write(str(allTestResults))
    file.close()
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),overallTestResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested StripLastAttributeFromColumnSelect')
except Exception as e:
    errorMessage = "Exception occured while testing StripLastAttributeFromColumnSelect function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test getWithColumnExpression
try :
  #Variable need to run GetWithColumnExpression function 
  testScenarioReference = 'GetWithColumnExpression'
  testCaseScenario ='Verify to get withcolumn expression from the given path'
  requiredInputParameter = 'DestinationDF,targetTableLocation,targetObjectName,targetFormat,cursor,batchTaskId,adfPipelineName,clusterName,notebookName,errorLogFileLocation' 
  testResultLocation = testResultsPath + testScenarioReference + '/batch_task_id=' + str(batchTaskId) + '/'  
  
    #full string, current attribute, expected output
  testData = [ (1,'[x]'          , '[x]', compile('explode(col("x"))',"",'eval'))
              ,(2,'[x].[y]'      , '[x]', compile('explode(col("x"))',"",'eval'))
              ,(3,'[x].[y]'      , '[y]', compile('explode(col("x.y"))',"",'eval'))
              ,(4,'[x].p.[q].r'  , '[x]', compile('explode(col("x"))',"",'eval'))
              ,(5,'[x].p.[q].r'  , '[q]', compile('explode(col("x.p.q"))',"",'eval'))                 
              ,(6,'p.q.r.[x].f.g', '[x]', compile('explode(col("p.q.r.x"))',"",'eval'))
              ,(7,'p.[q].r.[x].f.g', '[x]', compile('explode(col("q.r.x"))',"",'eval')) ]
  
  overallTestResult = 'success'
  allTestResults = []
  
  #Loop over for each test case 
  for testCase in testData:
    testNumber = 'Test-'+str(testCase[0])
    inputString = testCase[1]
    currentAttribute = testCase[2]
    expectedOutput = testCase[3]
    
    #Call the GetWithColumnExpression function
    outputString = getWithColumnExpression(inputString, currentAttribute,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

    #Now check the output equals the expected result
    print(outputString)
    if outputString[0] == expectedOutput: #If test result is true, make test result is success
      testResult = 'success'
      executionOutputStatus = 'As Expected'
      
    else:
      testResult = 'failed' #initialise the test result
      executionOutputStatus = 'Mismatched'
      overallTestResult = 'failed'
      
    #testid, inputString, acurrentAttribute, expectedOutput, outputString and testResult
    testDataSet = (testNumber, inputString, currentAttribute, expectedOutput, outputString, testResult)
    allTestResults.append(testDataSet)
  
  if overallTestResult == 'failed':
    if not os.path.exists(testResultLocation):
      os.makedirs(testResultLocation)
    #Write output to file in the ouptut location when ther is a failed scenario    
    file = open(testResultLocation + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '_outputResultFile','w')
    file.write(str(allTestResults))
    file.close()
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),overallTestResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested getWithColumnExpression')
except Exception as e:
    errorMessage = "Exception occured while testing getWithColumnExpression function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test getFinalXmlAttribute
try :
  #Variable need to run getFinalXmlAttribute function 
  testScenarioReference = 'GetFinalXmlAttribute'
  testCaseScenario ='Get the final xml attribute from the given string'
  requiredInputParameter = 'DestinationDF,targetTableLocation,targetObjectName,targetFormat,cursor,batchTaskId,adfPipelineName,clusterName,notebookName,errorLogFileLocation' 
  testResultLocation = testResultsPath + testScenarioReference + '/batch_task_id=' + str(batchTaskId) + '/'

  #full string, current attribute, expected output
  testData = [  (1,'[x]'          , '[x]')
               ,(2,'[x].[y]'      , '[y]')
               ,(3,'[x].[y]'      , '[y]')
               ,(4,'[x].p.[q].r'  , 'r')
               ,(5,'[x].p.[q].r'  , 'r')
               ,(6,'p.q.r.[x].f.g', 'g')
               ,(7,'hello there'  , 'hello there') ]
  overallTestResult = 'success'
  allTestResults = []
  
  #Loop over for each test case 
  for testCase in testData:
    testNumber = 'Test-'+str(testCase[0])
    inputString = testCase[1]
    expectedOutput = testCase[2]
    
    #Call the getFinalXmlAttribute function
    outputString = getFinalXmlAttribute(inputString,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

    #Now check the output equals the expected result

    if outputString == expectedOutput: #If test result is true, make test result is success
      testResult = 'success'
      executionOutputStatus = 'As Expected'
      
    else:
      testResult = 'failed' #initialise the test result
      executionOutputStatus = 'Mismatched'
      overallTestResult = 'failed'
      
    #testid, inputString, acurrentAttribute, expectedOutput, outputString and testResult
    testDataSet = (testNumber, inputString, expectedOutput, outputString, testResult)
    allTestResults.append(testDataSet)
  
  if overallTestResult == 'failed':
    if not os.path.exists(testResultLocation):
      os.makedirs(testResultLocation)
    #Write output to file in the ouptut location when ther is a failed scenario    
    file = open(testResultLocation + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '_outputResultFile','w')
    file.write(str(allTestResults))
    file.close()
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),overallTestResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested getFinalXmlAttribute')
except Exception as e:
    errorMessage = "Exception occured while testing getFinalXmlAttribute function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False


# COMMAND ----------

# DBTITLE 1,Test calculateExplodeWithExpressions
try :
  #Variable need to run calculateExplodeWithExpressions function
  testScenarioReference = 'CalculateExplodeWithExpressions'
  testCaseScenario ='Get the explode expression from the given path'
  requiredInputParameter = 'DestinationDF,targetTableLocation,targetObjectName,targetFormat,cursor,batchTaskId,adfPipelineName,clusterName,notebookName,errorLogFileLocation' 
  testResultLocation = testResultsPath + testScenarioReference + '/batch_task_id=' + str(batchTaskId) + '/'
  
    #full string, current attribute, expected output
  inputColumnSelectList = [ ('pm.[x].[y].m')
                           ,('pm.[x].[y].r.p')
                           ,('pm.[x].f')
                           ,('pm.p') ]
  
  expectedResults = [ (1,'x', 'explode(col("pm.x"))')
                    , (2,'y', 'explode(col("x.y"))') ]
  
  overallTestResult = 'success'
  allTestResults = []
    #Call the calculateExplodeWithExpressions function
  withColumnList = calculateExplodeWithExpressions(inputColumnSelectList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  withColumnList.sort()
  expectedResults.sort(key = lambda expectedResults: expectedResults[0]) 

  for expectResult in expectedResults:
    testNumber = 'Test-'+str(expectResult[0])
    actualResultLookup = [withColumnElement[2] for withColumnElement in withColumnList if withColumnElement[0] == expectResult[1]]
    expectedOutput = expectResult[2]
    
    #Now check the output equals the expected result
    if len(actualResultLookup) == 1 and expectedOutput == actualResultLookup[0]: #If test result is true, make test result is success
      testResult = 'success'
      executionOutputStatus = 'As Expected'
      
    else:
      testResult = 'failed' #initialise the test result
      executionOutputStatus = 'Mismatched'
      overallTestResult = 'failed'

    #testid, inputString, acurrentAttribute, expectedOutput, outputString and testResult
    testDataSet = (testNumber, expectedOutput, actualResultLookup[0], testResult)  
    allTestResults.append(testDataSet)
  
  if overallTestResult == 'failed':
    if not os.path.exists(testResultLocation):
      os.makedirs(testResultLocation)
    #Write output to file in the ouptut location when ther is a failed scenario    
    file = open(testResultLocation + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '_outputResultFile','w')
    file.write(str(allTestResults))
    file.close()
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),overallTestResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested calculateExplodeWithExpressions')
except Exception as e:
    errorMessage="Exception occured while testing calculateExplodeWithExpressions function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test selectDestFromSrc
try :
  
  #Parameters to run selectDestFromSrc function
  testCaseScenario ='Select the destination columns from the source dataframe'
  requiredInputParameter = 'sourceFormat,sourceHeader,sourceLocation,sourceTableName,sourceColumns,whereExpression,cursor,batchTaskId,adfPipelineName,clusterName,notebookName,errorLogFileLocation' 
  sourceColumns = ['RECORD_ID AS record_id','PRODUCT_NAME AS product_name','PERSON_NAME AS person_name','PERSON_ID AS person_id','PERSON_BALANCE AS balance','PURCHASE_PRICE AS purchase_price','CHARITY_DONATION AS charity_donation','CATEGORY AS categroy','SUB_CATEGORY AS sub_category','colList1 as colList1','colList2 as colList2']
  sourceTableName = 'inputSrcTable'
  sourceFormat = 'delta'
  sourceHeader = ''
  whereExpression = 'PURCHASE_PRICE > 5000'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  columnList = [('colList1', 'lit("country")','Test','preSelect'), ('colList2', 'lit("UK")','Test','preSelect'),('colList3','concat(col("colList1"),lit("-"),col("colList2"))','','postSelect')]

  inputpath = tableInputPath + 'inputSrcTable'
   
  #Run selectDestFromSrc function to read the source file and then rename them to the destination column names
  sourceDf = selectDestFromSrc(sourceFormat,sourceHeader,inputpath,sourceTableName,columnList,
                                   sourceColumns,whereExpression,cursor,batchTaskId,
                                   adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  if sourceDf.count() > 0:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested selectDestFromSrc function')
except Exception as e:
    errorMessage = "Exception occured while testing selectDestFromSrc function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test selectDestFromSrcTable
try :
  
  #Parameters to run selectDestFromSrcTable function
  testCaseScenario ='Select the destination columns from the source table'
  requiredInputParameter = 'sourceTableName,withColumnList,sourceColumns,whereExpression,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation' 
  sourceTableName = 'inputSrcTable'
  
  withColumnList = [('colList1', 'lit("country")','Test','preSelect'), ('colList2', 'lit("UK")','Test','preSelect'),('colList3','concat(col("colList1"),lit("-"),col("colList2"))','','postSelect')]
  
  sourceColumns = ['RECORD_ID AS record_id','PRODUCT_NAME AS product_name','PERSON_NAME AS person_name','PERSON_ID AS person_id','PERSON_BALANCE AS balance','PURCHASE_PRICE AS purchase_price','CHARITY_DONATION AS charity_donation','CATEGORY AS categroy','SUB_CATEGORY AS sub_category','colList1 as colList1','colList2 as colList2']
  
  whereExpression = 'PURCHASE_PRICE > 5000'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
    
   #Run selectDestFromSrcTable function to read the source table and then rename them to the destination column names  
  sourceAsDesTableDf = selectDestFromSrcTable(sourceTableName,withColumnList,sourceColumns,whereExpression,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
   
  
  #After filtering the source data with were condition sourceAsDesTableDf must have 2 rows    
  if sourceAsDesTableDf.count() == 2:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested selectDestFromSrcTable function')
except Exception as e:
    errorMessage = "Exception occured while testing selectDestFromSrcTable function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test selectDestFromJoinSrc
try :
  
  #Parameters to run selectDestFromJoinSrc function
  testCaseScenario ='Join the destination columns to source dataframe'
  requiredInputParameter = 'joinSourceDf,withColumnList,sourceColumns,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation' 
  sourceFormat = 'delta'
  sourceHeader = ''
  sourceLocation = tableInputPath + 'inputSrcTable'
  joinSourceDf = spark.read.format(sourceFormat).option("header",sourceHeader).load(sourceLocation)
  
  withColumnList = [('colList1', 'lit("country")','Test','preSelect'), ('colList2', 'lit("UK")','Test','preSelect'),('colList3','concat(col("colList1"),lit("-"),col("colList2"))','','postSelect')]
  
  sourceColumns = ['RECORD_ID AS record_id','PRODUCT_NAME AS product_name','PERSON_NAME AS person_name','PERSON_ID AS person_id','PERSON_BALANCE AS balance','PURCHASE_PRICE AS purchase_price','CHARITY_DONATION AS charity_donation','CATEGORY AS categroy','SUB_CATEGORY AS sub_category','colList1 as colList1','colList2 as colList2']
  
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  
    
   #Run selectDestFromJoinSrc function to read the source table and then rename them to the destination column names  
  joinSourceDestDf = selectDestFromJoinSrc(joinSourceDf,withColumnList,sourceColumns,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation) 
  
  #joinSourceDestDf should be same as the source count
  if joinSourceDestDf.count() == 3:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully Tested selectDestFromJoinSrc function')
except Exception as e:
    errorMessage = "Exception occured while testing selectDestFromJoinSrc function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test getCompiledWithExpressionForUdf
try :
  #Variable need to run getCompiledWithExpressionForUdf function 
  testScenarioReference = 'GetCompiledWithExpressionForUDF'
  testCaseScenario ='Get the udf in compile format for given input udf'
  requiredInputParameter = 'DestinationDF,targetTableLocation,targetObjectName,targetFormat,cursor,batchTaskId,adfPipelineName,clusterName,notebookName,errorLogFileLocation' 
  testResultLocation = testResultsPath + testScenarioReference + '/batch_task_id=' + str(batchTaskId) + '/'
  
    #full string, current attribute, expected output
  testData = [ (1,'x', 'parseXML(col("¦Hello.[x].y¬"))', 'udf', compile('parseXML(col("¦Hello.[x].y¬"))',"",'eval'))
              ,(2,'z', 'parseXML(col("¦[x].y¬"))'        , 'udf', compile('parseXML(col("¦[x].y¬"))',"",'eval'))
              ,(3,'q', 'calculatePiiHash(col("¦[x].y¬"))', 'hash', compile('calculatePiiHash(col("¦[x].y¬"))',"",'eval'))]
  
  overallTestResult = 'success'
  allTestResults = []
  #Loop over for each test case 
  for testCase in testData:
    testNumber = 'Test-'+str(testCase[0])
    inputColumn = testCase[1]
    inputUDF = testCase[2]
    inputUDFType = testCase[3]
    expectedOutput = testCase[4]
    
    #Call the getCompiledWithExpressionForUdf function
    outputTuple = getCompiledWithExpressionForUdf([(inputColumn,inputUDF,inputUDFType)],cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

    #Now check the output equals the expected result

    if len(outputTuple) == 1 and expectedOutput == outputTuple[0][1]: #If test result is true, make test result is success
      testResult = 'success'
      executionOutputStatus = 'As Expected'
      
    else:
      testResult = 'failed' #initialise the test result
      executionOutputStatus = 'Mismatched'
      overallTestResult = 'failed'

    #testid, inputString, acurrentAttribute, expectedOutput, outputString and testResult
    testDataSet = (testNumber, inputColumn, inputUDF, expectedOutput, outputTuple[0][1], testResult)      
    allTestResults.append(testDataSet)
  
  if overallTestResult == 'failed':
    if not os.path.exists(testResultLocation):
      os.makedirs(testResultLocation)
    #Write output to file in the ouptut location when ther is a failed scenario    
    file = open(testResultLocation + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '_outputResultFile','w')
    file.write(str(allTestResults))
    file.close()
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),overallTestResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested getCompiledWithExpressionForUdf')
except Exception as e:
    errorMessage = "Exception occured while testing getCompiledWithExpressionForUdf function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test getWithExpressionsAndSelectList
try :
  #Variable need to run getWithExpressionsAndSelectList function 
  testScenarioReference = 'GetWithExpressionsAndSelectList'
  testCaseScenario ='Get the column with expression'
  requiredInputParameter = 'DestinationDF,targetTableLocation,targetObjectName,targetFormat,cursor,batchTaskId,adfPipelineName,clusterName,notebookName,errorLogFileLocation' 
  testResultLocation = testResultsPath + testScenarioReference + '/batch_task_id=' + str(batchTaskId) + '/'
  
  myVariableTest = 'hello'
  CreatedBatchID = 3
  
  #'target_column_name', 'expression', 'computed_type', 'source_column', 'target_column_type', 'source_column_type'
  columnList = [('columnA','¦Hello¬', 'expression', 'unstructuredxmlSourceCOLUMN', 'STRING', 'STRING')
                ,('columnB','¦Hello.[x].y¬', 'expression', 'unstructuredxmlSourceCOLUMN', 'STRING', 'STRING')
                ,('columnC','¦Hello.[x].y¬ + "|" + ¦Hello.[p].q¬', 'expression', 'unstructuredxmlSourceCOLUMN', 'STRING', None)
                ,('columnD','¦Hello.[x].y¬ + "|" + ¦Hello.[p].q.[r].s¬', 'expression', 'unstructuredxmlSourceCOLUMN', 'STRING', None)
                ,('columnE','\'unstructuredxmlSource\'', 'expression', 'unstructuredxmlSourceCOLUMN', 'STRING', 'STRING')
                ,('columnF','parseXML(¦[something]¬)', 'udf', 'unstructuredxmlSourceCOLUMN', 'STRING', None)
                ,('columnG', None,'none','MySourceColumn', 'STRING', 'INT')
                ,('columnH','parseXML(something)', 'udf', 'unstructuredxmlSourceCOLUMN', 'STRING', 'STRING')
                ,('columnI','parseXML(col("¦Hello.[x].y¬"))', 'udf', 'unstructuredxmlSourceCOLUMN', 'STRING', 'STRING')
                ,('columnJ','lit(\'3\')', 'expression', None, 'INT', None)
                ,('columnK', None, 'none', 'mySourceColumn','STRING', 'STRING')
                ,('columnL', 'myVariableTest', 'variable', None, 'STRING', 'STRING')
                ,('columnM','calculatePiiHash(¦[something]¬)', 'hash', 'unstructuredxmlSourceCOLUMN', 'STRING', 'STRING')
                ,('columnN','lit(\'3.1\')', 'expression', None, 'DECIMAL(18,1)', None)
                ,('CreatedBatchID', 'CreatedBatchID', 'variable', None, 'BIGINT', None)
                ,('AlrCode', '', 'none', 'AlrCode', 'STRING', None)
                ,('columnO'
                  ,'stack(3, \'Driver1\',¦message.data.Driver1¬, \'Driver2\', ¦message.data.Driver2¬, \'Driver3\', ¦message.data.Driver3¬) AS (Driver, DriverName)'
                  ,'unpivot'
                  ,'unstructuredxmlSourceCOLUMN'
                  ,'STRING'
                  ,None)
                ,('columnP','stack(3, \'Driver1\',COALESCE(¦message.data.Driver1¬, \'\')||\'|\'||¦message.data.Age1¬, \'Driver2\', COALESCE(¦message.data.Driver2¬, \'\')||\'|\'||¦message.data.Age2¬, \'Driver3\', COALESCE(¦message.data.Driver3¬, \'\')||\'|\'||¦message.data.Age3¬) AS (Driver, DriverName)'
                  ,'unpivot'
                  ,'unstructuredxmlSourceCOLUMN'
                  ,'STRING'
                  ,None)
                ,('ColumnQ', None, 'ignore', None, None, None)
                ,('columnR',"InnerJoin(EmployeeSP,FirstName)[Grade]", 'join', None, 'STRING', None)
                ,('columnS',"LeftJoin(EmployeeLC,FirstName||'-'||LastName)[Country]", 'join', None, 'STRING', None)
                ,('columnT',"RightJoin(EmployeeLC,FirstName||'-'||LastName)[Address]", 'join', None, 'STRING', None)
               ]
 
  expectedResults = [(1,'columnA', 'unstructuredxmlSource_COLUMN.OuterXML.Hello AS columnA')
                    ,(2,'columnB', 'x.y AS columnB')
                    ,(3,'columnC', 'x.y + "|" + p.q AS columnC')
                    ,(4,'columnD', 'x.y + "|" + r.s AS columnD')
                    ,(5,'columnE', "'unstructuredxmlSource' AS columnE")
                    ,(6,'columnF', 'columnF')
                    ,(7,'columnG', 'CAST(MySourceColumn AS STRING) AS columnG')
                    ,(8,'columnH', 'columnH')
                    ,(9,'columnI', 'columnI')
                    ,(10,'columnJ', "CAST(lit('3') AS INT) AS columnJ")
                    ,(11,'columnK', 'mySourceColumn AS columnK')
                    ,(12,'columnL', '\'hello\' AS columnL')
                    ,(13,'columnN', "CAST(lit('3.1') AS DECIMAL(18,1)) AS columnN")
                    ,(14,'CreatedBatchID',"CAST('3' AS BIGINT) AS CreatedBatchID")
                    ,(15,'AlrCode','AlrCode AS AlrCode')
                    ,(16
                      ,'columnO'
                      ,'stack(3, \'Driver1\',unstructuredxmlSource_COLUMN.OuterXML.message.data.Driver1, \'Driver2\', unstructuredxmlSource_COLUMN.OuterXML.message.data.Driver2, \'Driver3\', unstructuredxmlSource_COLUMN.OuterXML.message.data.Driver3) AS (Driver, DriverName)')
                    ,(17
                      ,'columnP','stack(3, \'Driver1\',COALESCE(unstructuredxmlSource_COLUMN.OuterXML.message.data.Driver1, \'\')||\'|\'||unstructuredxmlSource_COLUMN.OuterXML.message.data.Age1, \'Driver2\', COALESCE(unstructuredxmlSource_COLUMN.OuterXML.message.data.Driver2, \'\')||\'|\'||unstructuredxmlSource_COLUMN.OuterXML.message.data.Age2, \'Driver3\', COALESCE(unstructuredxmlSource_COLUMN.OuterXML.message.data.Driver3, \'\')||\'|\'||unstructuredxmlSource_COLUMN.OuterXML.message.data.Age3) AS (Driver, DriverName)')
                    ,(18,'columnR','EmployeeSP_Grade AS columnR' )
                    ,(19,'columnS','coalesce(EmployeeLC_Country,"Unknown") AS columnS' )
                    ,(20,'columnT','EmployeeLC_Address AS columnT' )
                    ]
  
  expectedResultsWithExpression = [ (1, 'p'        , 'explode(col("unstructuredxmlSource_COLUMN.OuterXML.Hello.p"))'    , 'preSelect')
                                   ,(2, 'r'        , 'explode(col("p.q.r"))'                          , 'preSelect')
                                   ,(3, 'x'        , 'explode(col("unstructuredxmlSource_COLUMN.OuterXML.Hello.x"))'    , 'preSelect')
                                   ,(4, 'something', 'explode(col("unstructuredxmlSource_COLUMN.OuterXML.something"))'  , 'preSelect')
                                   ,(5, 'columnF'  , 'parseXML(unstructuredxmlSource_COLUMN.OuterXML.something)'        , 'preSelect')
                                   ,(6, 'columnI'  , 'parseXML(col("x.y"))'                           , 'preSelect')
                                   ,(7, 'columnM'  , 'calculatePiiHash(unstructuredxmlSource_COLUMN.OuterXML.something)', 'postSelect')
                                   ,(8, 'columnH'  , 'parseXML(something)'                            , 'preSelect')
                                  ]

  xmlStartString = '¦unstructuredxmlSource_COLUMN.OuterXML.'
  
  overallTestResult = 'success'
  allTestResults = []
  
   
  withExpressionListFinal, ColumnListFinal = getWithExpressionsAndSelectList(columnList, xmlStartString,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  ColumnListFinal.sort()
  expectedResults.sort
  
  #Loop over for each test case 
  for testCase in expectedResults:
    testNumber = 'Test-'+str(testCase[0])
    testColumn = testCase[1]
    expectedOutput = testCase[2]
    
    actualOutput = [col[1] for col in ColumnListFinal if col[0] == testColumn][0]

    #Now check the output equals the expected result
    if len(actualOutput) >= 1 and actualOutput == expectedOutput: #If test result is true, make test result is success
      testResult = 'success'
      executionOutputStatus = 'As Expected'
      
    else:
      testResult = 'failed' #initialise the test result
      executionOutputStatus = 'Mismatched'
      overallTestResult = 'failed'

    #testid, inputString, acurrentAttribute, expectedOutput, outputString and testResult
    testDataSet = (testNumber, testColumn, expectedOutput, actualOutput, testResult)      
    allTestResults.append(testDataSet)
    
  #additional check for ignore
  ignoreListFound = [c[0] for c in ColumnListFinal if c[0] == 'ColumnQ']
  ignoreTestResult = 'failed' if len(ignoreListFound) > 0 else 'success'
  ignoreActualOutput = 'ColumnQ was returned' if ignoreTestResult == 'failed' else 'ColumnQ was not returned'
  if ignoreTestResult == 'failed': overallTestResult = 'failed'

  ignoreTestDataSet = ('Test-99', 'ColumnQ', 'Nothing', ignoreActualOutput,ignoreTestResult)
  allTestResults.append(ignoreTestDataSet)
  
  #loop over all withExpression test results
  for expectedWithResult in expectedResultsWithExpression:
    testNumber = 'Test-'+str(expectedWithResult[0])
    testColumn = expectedWithResult[1]
    expectedOutput = expectedWithResult[2]
    expectedOrder = expectedWithResult[3]
    
    actualOutput = [(col[2], col[3]) for col in withExpressionListFinal if col[0] == testColumn][0]
    
    if len(actualOutput) == 2:
      actualOutputColumn = actualOutput[0]
      actualOutputOrder = actualOutput[1]
      
      if (expectedOutput == actualOutputColumn) and (expectedOrder == actualOutputOrder):
        testResult = 'success'
        executionOutputStatus = 'As Expected'
      else:
        testResult = 'failed' #initialise the test result
        executionOutputStatus = 'Mismatched'
        overallTestResult = 'failed'
    
    #testid, inputString, acurrentAttribute, expectedOutput, outputString and testResult
    testDataSet = (testNumber, testColumn, (expectedOutput, expectedOrder), actualOutput, testResult)      
    allTestResults.append(testDataSet)  
#     print(allTestResults)
  if overallTestResult == 'failed':
    if not os.path.exists(testResultLocation):
      os.makedirs(testResultLocation)
    #Write output to file in the ouptut location when ther is a failed scenario    
    file = open(testResultLocation + datetime.now().strftime("%Y%m%d%H%M%S") + '_outputResultFile','w')
    file.write(str(allTestResults))
    file.close()
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),overallTestResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested getWithExpressionsAndSelectList')

except Exception as e:
#   print(e)
  errorMessage="Exception occured while testing getWithExpressionsAndSelectList function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test generateDict
try: 
  #Parameters to run generateDict function
  testCaseScenario ='dict check Generate dictionary from dataframe'
  requiredInputParameter = 'dataframe,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  fileName = "inputSrcFile.csv"
  sourceFile = testInputPath + fileName
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #input data and expected output
  inpuDataToDf = [["[(100022,''),(100022,'adasd')]","{100022: 'adasd', '¦default¬': 'Unknown'}"]
                  ,["[(100021,''),(100023,'adasd')]","{100021: '', 100023: 'adasd', '¦default¬': 'Unknown'}"]]
  
  overallTestResult = 'success'
  allTestResults = []
  #Loop over for each test case
  for idata in inpuDataToDf:
    # create spark dataframe
    inputData = eval(idata[0])
    expectedOutput = eval(idata[1])
    
    sourceAsSparkDataFrame = spark.createDataFrame(inputData,['RECORD_ID','PRODUCT_NAME'])
    
    #print(sourceAsSparkDataFrame)
    srcdict = generateDict(sourceAsSparkDataFrame,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    if srcdict ==  expectedOutput:
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    else:
      testResult = 'failed' #initialise the test result
      executionOutputStatus = 'Mismatched'
      overallTestResult = 'failed'
    #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                     testCaseScenario,executionOutputStatus,outputLocation,
                     datetime.now(),overallTestResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested generateDict')    
except Exception as e:
    errorMessage="Exception occured while testing generateDict function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup for markDatesLoaded previous run
#run cleanup script
try:
  cleanup = open("{}standardFunctionMarkDatesLoadedCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for usp_mark_dates_loaded')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for usp_mark_dates_loaded: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False    

# COMMAND ----------

# DBTITLE 1,Generate Input parameter for markDatesLoaded
#populate data into object_dates_availability table
try:
  inputParameter = open("{}standardFunctionMarkDatesLoaded.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>',str(batchId))
  inputParameterResults = pd.read_sql_query(inputParameter,conn)
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])  
  sourceObjectId = int(inputParameterResults.at[0,'src_object_id'])
  destinationObjectId = int(inputParameterResults.at[0,'dest_object_id'])
  newDatetimeFrom = str(inputParameterResults.at[0,'datetime_from'])[0:23]
  newDatetimeTo = str(inputParameterResults.at[0,'datetime_to'])[0:23] 
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate input parameters')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameters': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Test markDatesLoaded
#update output_datetime_from and output_datetime_to columns in object_dates_availability table
try:
  #Parameters to run markDatesLoaded function
  testCaseScenario ='log markDatesLoaded to complete the load from source object'
  requiredInputParameter = 'destinationObjectId, sourceObjectId, datetimeFrom, datetimeTo, cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  fileName = "inputSrcFile.csv"
  sourceFile = testInputPath + fileName
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #call markDatesLoaded function
  conn, cursor = markDatesLoaded(destinationObjectId, sourceObjectId, newDatetimeFrom, newDatetimeTo,dbconn, conn, cursor,newBatchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
  #reading markDatesLoaded result as pandas dataframe
  spOutput = pd.read_sql_query("SELECT * FROM audit.tbl_object_dates_availability WHERE batch_task_id = {} AND input_object_id = {} AND output_object_id = {} AND output_datetime_from = '{}' AND output_datetime_to = '{}' AND load_successful = 1".format(newBatchTaskId,sourceObjectId,destinationObjectId,newDatetimeFrom,newDatetimeTo),conn)
  if spOutput.shape[0] == 1:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested markDatesLoaded')    
except Exception as e:
    errorMessage="Exception occured while testing markDatesLoaded function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup for markDatesLoaded
#run cleanup script
try:
  cleanup = open("{}standardFunctionMarkDatesLoadedCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for usp_mark_dates_loaded')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for usp_mark_dates_loaded: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False    

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup for spExecHighAndLowLevel previous run
try:
  #run cleanup script
  cleanupMetadata = open("{}standardFunctionObjectHighAndLowLevelCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanupMetadata)
  while cursor.nextset():
    x = 1 
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')
except Exception as e:
  errorMessage = "Exception occured while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Data preparation for spExecHighAndLowLevel
#insertion of data into object,task_object_map, object_definition and object_dates_availability tables
try:
  #execute the script and get batch_task_id and task_id
  inputParmeter = open("{}standardFunctionObjectHighAndLowLevelPrep1.sql".format(testInputPath), "r").read()
  inputParmeter = inputParmeter.replace('<batch_id>', str(batchId))

  inputParameterResults = pd.read_sql_query(inputParmeter,conn).astype(str)
  
  #get the batch_task_id and cast as int
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])
  #insertion of data into object,task_object_map, object_definition and object_dates_availability tables
  dataPreparation = open("{}standardFunctionObjectHighAndLowLevelPrep2.sql".format(testInputPath), "r").read()
  dataPreparation = dataPreparation.replace('<batch_task_id>', str(newBatchTaskId))
  cursor.execute(dataPreparation)
  while cursor.nextset():
    x = 1
  
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate input parameter and data insertion into tables')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter and data insertion': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Test spExecHighAndLowLevel
#get the source and destination object details and field details
try:
  #Parameters to run markDatesLoaded function
  testCaseScenario ='get the object and attribute details'
  requiredInputParameter = 'conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  sourceDestinationTableDetails,sourceDestinationFieldsDetails = spExecHighAndLowLevel(conn,cursor,newBatchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  # sourceDestinationTableDetails,sourceDestinationFieldsDetails
  spObjectName = sourceDestinationTableDetails.at[0,'source_object_name']
  spAttributeName = sourceDestinationFieldsDetails.at[0,'source_object_attribute_name']
  objectName = 'spExecHighAndLowLevel_a'
  attributeName = 'row_id'
  
  if spObjectName == objectName and spAttributeName == attributeName:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested spExecHighAndLowLevel function')    
except Exception as e:
    errorMessage="Exception occured while testing spExecHighAndLowLevel function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup for spExecGetPKsType2Fields for previous run
#run cleanup script
try:
  cleanup = open("{}standardFunctionspPKsType2FieldsCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for PKsType2Fields for previous run')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for PKsType2Fields for previous run: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Data preparation for spExecGetPKsType2Fields
#insertion of data into object, task_object_map and object_definition tables
try:
  #execute the script and get batch_task_id and task_id
  inputParmeter = open("{}standardFunctionspPKsType2FieldsDataPrep.sql".format(testInputPath), "r").read()
  inputParmeter = inputParmeter.replace('<batch_id>', str(batchId))

  inputParameterResults = pd.read_sql_query(inputParmeter,conn).astype(str)
  
  #get the batch_task_id and cast as int
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])
  
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate test data for PKsType2Fields')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate test data for PKsType2Fields': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test spExecGetPKsType2Fields
#get the primary keys and type2 fields
try:
  #Parameters to run markDatesLoaded function
  testCaseScenario ='get the pks and type2 fields list'
  requiredInputParameter = 'conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''  

  #exec function
  pksType2FieldsDetails = spExecGetPKsType2Fields(conn, cursor, newBatchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
  
  #convert to pyspark
  fieldsSchema = getStructForPKsType2Df()
  pksType2FieldsDF = convertPandasToSparkDfWithSchema(pksType2FieldsDetails,fieldsSchema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation).na.fill('')

  #get values in list object
  pksFieldsList = pksType2FieldsDF.where("primary_key_order is not null").orderBy('primary_key_order').agg(collect_list(pksType2FieldsDF.object_attribute_name)).collect()[0][0]
  type2FieldsList = pksType2FieldsDF.where("track_type_2_changes==True").orderBy('object_attribute_name').agg(collect_list(pksType2FieldsDF.object_attribute_name)).collect()[0][0]
  
  #expected value in a list object
  expectedPksFieldsList = ['PKField1','PKField2']
  expectedType2FieldsList = ['TypeField1','TypeField1']
  
  if set(pksFieldsList) & set(expectedPksFieldsList) and set(type2FieldsList) & set(expectedType2FieldsList):
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested spExecGetPKsType2Fields function')    
except Exception as e:
    errorMessage="Exception occured while testing spExecGetPKsType2Fields function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup for spExecGetPKsType2Fields
#run cleanup script
try:
  cleanup = open("{}standardFunctionspPKsType2FieldsCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for PKsType2Fields')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for PKsType2Fields: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Test addAuditAndHBKColumnsOnDF
try:
  #Parameters to run addAuditAndHBKColumnsOnDF function 
  testCaseScenario ='Add audit and HBK columns to spark dataframe'
  requiredInputParameter = 'sourceDF,pksFieldsList,type2FieldsList,currentTs,lakeFromDatetime,hasHPK,sourceId,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  sourceFile = "/mnt/dataquality/unit_tests/standardFunction/input/inputSrcFile.csv"
  #sourceFile = testInputPath + fileName
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

  #read file as spark dataframe
  sourceDF = spark.read.format("csv").option("header","true").schema(schema).load(sourceFile)
  
  #call function
  pksFieldsList = ['RECORD_ID','PERSON_ID']
  type2FieldsList = ['PRODUCT_NAME','PERSON_NAME']
  currentTs=datetime.now()
  hasHPK=True
  #register udf function
  computeHashValueUdfRegistration()
  #call function
  sourceDF = addAuditAndHBKColumnsOnDF(sourceDF,pksFieldsList,type2FieldsList,currentTs,currentTs,hasHPK,sourceId,batchId,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  allColumns = sourceDF.columns
  
  #check output
  columnNameToCheck=['SourceID', 'lakeCreatedDate', 'lakeCreatedTimestamp', 'lakeCreatedBatchID', 'lakeLastUpdateDate', 'lakeLastUpdateTimestamp', 'lakeLastUpdatedBatchID', 'HashedBusinessKey', 'HashValue', 'HashedPartitionKey']
  for columnName in columnNameToCheck:
    if (columnName in allColumns):
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    else:
      testResult = 'failed'
      executionOutputStatus = 'Mismatched'
      break
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested addAuditAndHBKColumnsOnDF function')
except Exception as e:
  errorMessage = "Exception occured while testing addAuditAndHBKColumnsOnDF function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test convertPandasToSparkDf
#convert pandas dataframe to spark dataframe
try:  
  #Parameters to run convertPandasToSparkDf function
  testCaseScenario ='convert pandas to spark dataframe'
  requiredInputParameter = 'sourceDestinationTableDetails,sourceDestinationFieldsDetails,sourceDestinationFieldsDetailsSchema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #Source files read as pandas dataframe
  sourceTable = "sourceDestinationTableDetails.csv"
  srcFile = testInputPath + sourceTable
  sourceTableDetails = pd.read_csv(srcFile)
  
  sourceField = "sourceDestinationFieldsDetails.csv"
  sourceFile = testInputPath + sourceField
  sourceFieldDetails = pd.read_csv(sourceFile)

  #call schema
  fieldSchema = getStructForHighAndLowLevelDf()
  srcAndDesTableDF,srcAndDesFieldsDF = convertPandasToSparkDf(sourceTableDetails,sourceFieldDetails,fieldSchema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  if srcAndDesTableDF.count() == 1 and srcAndDesFieldsDF.count() == 1:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested convertPandasToSparkDf function')
except Exception as e:
    errorMessage="Exception occured while testing convertPandasToSparkDf function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False


# COMMAND ----------

# DBTITLE 1,Test getSelectAndWithColumnDetails
#get the column list 
try:
  #Parameters to run getSelectAndWithColumnDetails function
  testCaseScenario ='test getSelectAndWithColumnDetails function to get the column list'
  requiredInputParameter = 'srcAndDesFieldsDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #read input file
  sourceField = "sourceDestinationFieldsDetails.csv"
  sourceFile = tableInputPath + sourceField
  srcfile = spark.read.format("csv").option("header","true").load(sourceFile)
  columnList = getSelectAndWithColumnDetails(srcfile,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  for colList in columnList:
    # validate function input data and count
    if 'ROW_ID' in colList and len(columnList) == 1 :
      testResult = 'success'
      executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,
                    datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested getSelectAndWithColumnDetails function')
except Exception as e:
    errorMessage="Exception occured while testing getSelectAndWithColumnDetails function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False
    

# COMMAND ----------

# DBTITLE 1,Test computeHashValue
#generate hash value
try:
  #Parameters to run computeHashValue function
  testCaseScenario ='test computeHashValue function to generate hash value'
  requiredInputParameter = 'hashString'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  hashString = 'dashggfhgsdjfgjas21654673215'
  #call the function computeHashValue
  shaValue = computeHashValue(hashString)
  #validate datatype of output value 
  if str(shaValue).isdigit():
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,
                      datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested computeHashValue function')
except Exception as e:
    errorMessage="Exception occured while testing computeHashValue function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test concatenateFunc
#this function evalutes the string expresesion in the list and concatenate with '|' seperator
try:
  #Parameters to run concatenateFunc function
  testCaseScenario ='test concatenateFunc function to concatenate values in list'
  requiredInputParameter = 'List,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  colList = ['"id"','"name"','"country"']
  expectedOutput = "Column<b'concat_ws(|, coalesce(id, ), coalesce(name, ), coalesce(country, ))'>"
  concateList = concatenateFunc(colList)
  if str(concateList) == expectedOutput:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,
                        datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested concatenateFunc function')
except Exception as e:
    errorMessage="Exception occured while testing concatenateFunc function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test performStringreplaceCastingAndNaming
#replace the values with new value in a list 
try:
  #Parameters to run performStringreplaceCastingAndNaming function
  testCaseScenario ='perform string replacement, variable replacement, casting and naming'
  requiredInputParameter = 'columnList,stringReplacementList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #'target_column_name', 'expression', 'computed_type', 'source_column', 'target_column_type', 'source_column_type'
  columnList = [('columnA','¦Hello¬', 'expression', 'unstructuredxmlSourceCOLUMN', 'STRING', 'STRING')
                ,('columnB','¦Hello.[x].y¬', 'udf', 'unstructuredxmlSourceCOLUMN', 'STRING', 'STRING')
                ,('columnC','¦Hello.[x].y¬ + "|" + ¦Hello.[p].q¬', 'expression', 'unstructuredxmlSourceCOLUMN', 'STRING', None)
                ,('columnD','¦Hello.[x].y¬ + "|" + ¦Hello.[p].q.[r].s¬', 'expression', 'unstructuredxmlSourceCOLUMN', 'STRING', None)
               ]
  
  replacementstring  = [("Hello", "Replaced")]

  expectedResults = [('columnA', '¦Replaced¬ AS columnA', 'expression', 'unstructuredxmlSourceCOLUMN', 'STRING', 'STRING')                                                    ,('columnB', 'columnB', 'udf', 'unstructuredxmlSourceCOLUMN', 'STRING', 'STRING')                                                                          ,('columnC', '¦Replaced.[x].y¬ + "|" + ¦Replaced.[p].q¬ AS columnC', 'expression', 'unstructuredxmlSourceCOLUMN', 'STRING', None)                          ,('columnD', '¦Replaced.[x].y¬ + "|" + ¦Replaced.[p].q.[r].s¬ AS columnD', 'expression', 'unstructuredxmlSourceCOLUMN', 'STRING', None)
                    ]
  
  stringReplace = performStringreplaceCastingAndNaming(columnList,replacementstring,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  print(stringReplace)
  if stringReplace == expectedResults:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
    #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,
                        datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested performStringreplaceCastingAndNaming function')
except Exception as e:
    errorMessage="Exception occured while testing performStringreplaceCastingAndNaming function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test cleanPiiHashLiteralAttributes
#this function takes the atrribute list with lit() and col() and concatenate the consecutive lit items to single lit with |
try:  
  #Parameters to run cleanPiiHashLiteralAttributes function
  testCaseScenario ='Clean Literal string value attributes'
  requiredInputParameter = 'attributeFunctionList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #input parameter
  attributeFunctionList = ['lit("Product")','lit("Purpose")', 'col("Category")']
  #expected output
  expectedOutput = ['lit("Product|Purpose")', 'col("Category")']
  #call function
  cleanPiiHash = cleanPiiHashLiteralAttributes(attributeFunctionList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  #validate actual output and expected output
  if cleanPiiHash == expectedOutput:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
    #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,
                        datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested performStringreplaceCastingAndNaming function')
except Exception as e:
    errorMessage="Exception occured while testing performStringreplaceCastingAndNaming function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False
  

# COMMAND ----------

# DBTITLE 1,Test piiGetHashVariables
# this functon gets the expression and clean the expressions using cleanPiiHashLiteralAttributes function and return values
try:
  #Parameters to run piiGetHashVariables function
  testCaseScenario ='get pii hash variables'
  requiredInputParameter = 'objectPiiHashDetailsdf,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #input parameter
  srcSchema = StructType(
                           [StructField("hash_version"                        ,    IntegerType()  ,    True),
                           StructField("piiExpression"                        ,    StringType()   ,    True),
                           StructField("piiTreaceabilityExpression"           ,    StringType()   ,    True),
                           ]
                       )

  objectHashDetailsdf = convertPandasToSparkDfWithSchema([[100,'lit("Test"),lit("Function"),col("FirstName")','lit("Test"),lit("Function"),col("FirstName"),col("SomePiiTraceabilityColumn")']],srcSchema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  #convert to pandas datframe
  objectPiiHashPandadf = objectHashDetailsdf.toPandas()
  piiVersion,piiColumnFunctionList,piiTraceabilityColumnFunctionList = piiGetHashVariables(objectPiiHashPandadf,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  expectedPiiColumnfunList = ['lit("Test|Function")', 'col("FirstName")']
  expectedPiiTraceability = ['lit("Test|Function")','col("FirstName")','col("SomePiiTraceabilityColumn")']
  if piiVersion == 100 and piiColumnFunctionList == expectedPiiColumnfunList and piiTraceabilityColumnFunctionList == expectedPiiTraceability:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
      #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,
                          datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested piiGetHashVariables function')
except Exception as e:
    errorMessage="Exception occured while testing piiGetHashVariables function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False
  

# COMMAND ----------

# DBTITLE 1,Test generateMappingExpression
#this function give the mapping expression
try:
  #Parameters to run generateMappigExpression function
  testCaseScenario ='getting mapping expression for use in lookups'
  requiredInputParameter = 'mappingDataFrame,columnList,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #input parameter
  srcSchema = StructType(
                             [StructField("Id"               ,    IntegerType()  ,    True),
                             StructField("Name"              ,    StringType()   ,    True),
                             StructField("Product"           ,    StringType()   ,    True),
                             StructField("Country"           ,    StringType()   ,    True),
                             ]
                         )
  #create spark dataframe
  objectHashDetailsspdf = convertPandasToSparkDfWithSchema([[100,'joe','chocolates','swiss']],srcSchema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation) 
  colList = ['Country','Name',]
  expectedOutput = "{'swiss': 'joe'}"
  #call generateMappigExpression function
  mapExpr = generateMappingExpression(objectHashDetailsspdf,colList,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  mapExprOutput = objectHashDetailsspdf.select(mapExpr).collect()[0][0]
  # get the resut of mapping expression if len(columnList)>2
  columnList = ['Country','Name','product']
  mapExpOutput = "{'swiss': ['joe', 'chocolates']}"
  #call generateMappigExpression function
  mapExpression = generateMappingExpression(objectHashDetailsspdf,columnList,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  mapExpressionOutput = objectHashDetailsspdf.select(mapExpression).collect()[0][0]
  # print(mapExpressionOutput)

  if str(mapExprOutput) == expectedOutput and str(mapExpressionOutput) == mapExpOutput:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
      #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,
                          datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested generateMappigExpression function')
except Exception as e:
    errorMessage="Exception occured while testing generateMappigExpression function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test spExecGetunstructuredxmlSourceObjectDetails function
#this function get the path, object_attribute_type and element values
try:
  #Parameters to run spExecGetunstructuredxmlSourceObjectDetails function
  testCaseScenario ='getting unstructuredxmlSource object details'
  requiredInputParameter = 'batchTaskId,conn,cursor,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #input parameter
  testPath = 'PolMessage.PolData.[CalculatedResult].CalculatedResult_CompulsoryXs'
  testElement = 'CalculatedResult_CompulsoryXs'
  unstructuredxmlSourceObjectDetails = spExecunstructuredxmlSourceGetObjectDetails(batchTaskId,conn,cursor,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

  spPathOutput = unstructuredxmlSourceObjectDetails[unstructuredxmlSourceObjectDetails['Path'] == testPath]
  spElementOutput = unstructuredxmlSourceObjectDetails[unstructuredxmlSourceObjectDetails['Element'] == testElement]
  if spPathOutput.shape[0] == 1 and spElementOutput.shape[0] == 1:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
        #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,
                            datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested scenario 1 test case for function spExecunstructuredxmlSourceGetObjectDetails')
except Exception as e:
    errorMessage="Exception occured while testing scenario 1 test case for function spExecunstructuredxmlSourceGetObjectDetails: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test createOrReplaceTemporaryView
#this function createOrReplaceTemporaryView prepare the sql to create the view
try:
  #Parameters to run createOrReplaceTemporaryView function
  testCaseScenario ='Test createOrReplaceTemporaryView function '
  requiredInputParameter = 'viewName,viewDefinition,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  
  #input parameter
  viewName = 'TestCreateOrReplaceTemporaryView'
  viewDefinition = 'SELECT RECORD_ID,PRODUCT_NAME  FROM inputSrcTable'
  
  #Function call to create a view
  createOrReplaceTemporaryView (viewName,viewDefinition,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  #check if the View exists 
  viewResult = spark.sql("SELECT * FROM {}".format(viewName)).count()
  
  if viewResult >= 1:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
        #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,
                            datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested function createOrReplaceTemporaryView')
except Exception as e:
    errorMessage="Exception occured while testing function createOrReplaceTemporaryView: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False   
    

# COMMAND ----------

# DBTITLE 1,Test identifyIfNewDataInDependentTables
#this function identifyIfNewDataInDependentTables identify new data has been loaded into dependent objects after the target was last loaded
try:
  #Parameters to run identifyIfNewDataInDependentTables function
  testCaseScenario ='Test identifyIfNewDataInDependentTables function '
  requiredInputParameter = 'targetTableName, dependentTableNamesList, cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  
  #input parameter
  sourceFile = 'srcFileOverwrite.csv'
  targetTableName = 'outputDeltaTable'
  dependentTableNamesList = ['testDependentFirst','testDependentSecond']
  dependentTableNameFirst = tableOutputPath +'testDependentFirst'
  dependentTableNameSecond = tableOutputPath +'testDependentSecond'
  
  #Read the data to populate into dependent table 
  dependentTableFirstDf = spark.read.format("csv").option("header","true").schema(schema).load(tableInputPath + sourceFile)
  dependentTableSecondDf = spark.read.format("csv").option("header","true").schema(schema).load(tableInputPath + sourceFile)
  
  #Run write_to_target function to load into testDependentFirst table 
  writeToTarget(dependentTableFirstDf,dependentTableNameFirst,targetObjectName,targetFormat,
                            cursor,batchTaskId,adfPipelineName,clusterId,
                            notebookName,errorLogFileLocation) 
  
  
  #Run write_to_target function to load into testDependentSecond table
  writeToTarget(dependentTableSecondDf,dependentTableNameSecond,targetObjectName,targetFormat,
                            cursor,batchTaskId,adfPipelineName,clusterId,
                            notebookName,errorLogFileLocation) 

  #Function call to check the dependancy 
  dependancyCheck = identifyIfNewDataInDependentTables(targetTableName,dependentTableNamesList,cursor,
                                                           batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

  #Function returns true when the dependent table are loaded 
  if dependancyCheck == True :
    testResult = 'success'
    executionOutputStatus = 'As Expected'
 
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,
                            datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested function identifyIfNewDataInDependentTables')
except Exception as e:
    errorMessage="Exception occured while testing function identifyIfNewDataInDependentTables: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False   


# COMMAND ----------

# DBTITLE 1,Metadata Cleanup for spExecHighAndLowLevel
try:
  #run cleanup script
  cleanupMetadata = open("{}standardFunctionObjectHighAndLowLevelCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanupMetadata)
  while cursor.nextset():
    x = 1 
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')
except Exception as e:
  errorMessage = "Exception occured while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Drop the input source table to clean up the data
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS inputSrcTable;
# MAGIC DROP TABLE IF EXISTS outputDeltaTable;
# MAGIC DROP TABLE IF EXISTS testDependentFirst;
# MAGIC DROP TABLE IF EXISTS testDependentSecond;
# MAGIC DROP TABLE IF EXISTS writeToTargetWithOverwrite;
# MAGIC DROP VIEW IF EXISTS TestCreateOrReplaceTemporaryView;

# COMMAND ----------

# DBTITLE 1,Drop the table path to clean up the data
try:
    dbutils.fs.rm(tableInputPath + 'inputSrcTable', recurse = True)
    dbutils.fs.rm(tableOutputPath + 'outputDeltaTable',recurse = True)
    dbutils.fs.rm(tableOutputPath + 'testDependentFirst',recurse = True)
    dbutils.fs.rm(tableOutputPath + 'testDependentSecond',recurse = True)
    logTaskProgress(cursor,batchTaskId,'Successful deleted the input source table path')
except Exception as e:
    errorMessage = "Exception occured while deleting the input source table path: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test getBatchLevelObjectRowCount 
# While preparing data created table and inserted 3 records.So,For current jobId for given object row count =3.Function has to return same for successful run
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify row count of a given object for a batch' 
  #Test object name 
  testObjectName = 'standardFunctions_getBatchLevelObjectRowCount'
  # Run the notebbok to get output
  outputRowCount = dbutils.notebook.run("/dtp/nb/test/unit/gen/unit_testing_getBatchLevelObjectRowCount_statutory", 60)
  #If query output row count = 3 then make test case as success
  if int(outputRowCount) == 3:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Successful Tested function getBatchLevelObjectRowCount')  
except Exception as e:
  errorMessage = "Exception occured while testing function getBatchLevelObjectRowCount" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Data Preparation
# scripts to insert an object and reference details into the tbl_object and tbl_object_reference 
try: 
  dataPreparation = open("{}standardFunctionObjectReferencePrep1.sql".format(testInputPath), "r").read()
  dataPreparation = dataPreparation.replace('<batch_id>', str(batchId))
  dataPreparationResults = pd.read_sql_query(dataPreparation,conn).astype(str)
  
  #get the object name
  testInputObjectName = dataPreparationResults.at[0,'object_name']
  
  logTaskProgress(cursor,batchTaskId,' Executed initial scripts successfully')     
except Exception as e:
  errorMessage = "Exception occured while executing initial scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test getObjectReferenceDetails function
try:
  #get the table value from json message
  #while preparing metadata repartition_count =16 value is given
  testCaseScenario ='getObjectReferenceDetails function'
  #Test object name 
  testObjectName = 'standardFunctions_getObjectReferenceDetails'
  requiredInputParameter = 'jsonMessage'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #execute getObjectReferenceDetails function
  objectReferenceDetails = getObjectReferenceDetails(testInputObjectName
                                                     ,cursor
                                                     ,batchTaskId
                                                     ,adfPipelineName
                                                     ,clusterId
                                                     ,notebookName
                                                     ,errorLogFileLocation)
  #validate the function output
  if objectReferenceDetails.at[0,'repartition_count'] == '16':
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                             testCaseScenario,executionOutputStatus,outputLocation,
                             datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested getObjectReferenceDetails function')
except Exception as e:
  errorMessage = "Exception occured while testing getObjectReferenceDetails function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False
  

# COMMAND ----------

# DBTITLE 1,Test readExcelFileIntoSparkDf function
try:
  #Parameters to run readExcelFileIntoSparkDf function
  testCaseScenario ='convert excel file to spark dataframe'
  requiredInputParameter = 'sourceLocation,sheet_name,usecols,header,skiprows,column_check,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #set source file variable
  sourceFile = "inputExcelFile.xlsx"
  srcExcelFile = testInputPath + sourceFile
  #read excel file into pandas dataframe and then convert to spark dataframe
  pd_df = pd.read_excel(srcExcelFile, sheet_name='Breakfast', usecols='A:G', header=1, skiprows=0, engine='openpyxl')
  pd_df=pd_df.applymap(str)
  df = spark.createDataFrame(pd_df)
  #call the sandard function to read excel file into spark dataframe
  sourceDF = readExcelFileIntoSparkDf(srcExcelFile,'Breakfast','A:G',1,0,None,
                                    cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  #check the outputs of both dataframe
  if df.count() == 7 and sourceDF.count() == 7:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #print(testResult,executionOutputStatus)
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested readExcelFileIntoSparkDf function')  
except Exception as e:
    errorMessage = "Exception occured while testing readExcelFileIntoSparkDf: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup for populateObjectDateAvailability previous run
#run cleanup script
try:
  cleanup = open("{}standardFunctionPopulateObjectDateAvailabilityCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for populateObjectDateAvailability previous run')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for populateObjectDateAvailability previous run: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False    


# COMMAND ----------

# DBTITLE 1,Generate Input parameter for populateObjectDateAvailability
#populate data into object_dates_availability table
try:
  inputParameter = open("{}standardFunctionPopulateObjectDateAvailabilityDataPrep.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>',str(batchId))
  inputParameterResults = pd.read_sql_query(inputParameter,conn)
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])  
  src_tbl_object_name_1 = str(inputParameterResults.at[0,'src_tbl_object_name_1'])
  src_tbl_object_name_2 = str(inputParameterResults.at[0,'src_tbl_object_name_2'])
  src_vw_object_name = str(inputParameterResults.at[0,'src_vw_object_name'])

  #print(newBatchTaskId,src_tbl_object_name_1, src_tbl_object_name_2, src_vw_object_name)
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate input parameters for populateObjectDateAvailability')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameters for populateObjectDateAvailability': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Test populateObjectDateAvailability function
#test function and don't cleanup data as next function getSourceObjectName will use same data
try:
  testCaseScenario ='populate object name by task batch id'
  requiredInputParameter = 'batchTaskId,viewName,viewTables,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''

  retVal = False
  viewName = src_vw_object_name
  viewTables = src_tbl_object_name_1 + ',' + src_tbl_object_name_2
  #print(viewName, viewTables)
  retVal = populateObjectDateAvailability(viewName,viewTables,cursor,newBatchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

  #check the output
  outputParam = open("{}standardFunctionPopulateObjectDateAvailabilityCheckOutput.sql".format(testInputPath), "r").read()
  results = pd.read_sql(outputParam,conn)

  #view has 2 tables so expecting rows count as 4 (2 rows added by data prep and 2 rows by proc)
  if (results.at[0,'TotalRows'] == 4):
    testResult = 'success'
    executionOutputStatus = 'As Expected'
      
  #print(results.at[0,'TotalRows'],testResult,executionOutputStatus)
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)  
  
  logTaskProgress(cursor,batchTaskId,'Successful Tested populateObjectDateAvailability function') 
except Exception as e:
  errorMessage = "Exception occured while testing populateObjectDateAvailability:" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test getViewDefinition function
#this code uses cleanup and preparation from populateObjectDateAvailability
try:
  testCaseScenario ='get view definition by task batch id (newBatchTaskId will be used as prepared by above query)'
  requiredInputParameter = 'conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''

  #get viewDef for newbatchtaskid (data prep done by as part of object availability script)
  viewDefDf = getViewDefinition(conn,cursor,newBatchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  viewDefDict = viewDefDf.collect()[0].asDict()

  #check the output (check view name is created as part of data prep query)
  if viewDefDict['ViewName'] == "unitTestStandardFunctionPopulateObjectDateAvailability_SrcView":
    testResult = 'success'
    executionOutputStatus = 'As Expected'

  #print(viewDefDict['ViewName'],testResult,executionOutputStatus)
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)  
  
  logTaskProgress(cursor,batchTaskId,'Successful Tested getSourceObjectName function') 
except Exception as e:
  errorMessage = "Exception occured while testing getSourceObjectName:" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup for populateObjectDateAvailability
#run cleanup script
try:
  cleanup = open("{}standardFunctionPopulateObjectDateAvailabilityCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for populateObjectDateAvailability')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for populateObjectDateAvailability: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Data Preperation for getAllViewDefinitions
try:
    spark.sql("""CREATE OR REPLACE TABLE standardised.TblTestx3 ( Id integer,Country string )""")
    spark.sql("""Delete from standardised.TblTestx3""")
    spark.sql("""Insert into standardised.TblTestx3 values(1,'India'),(2,'India'),(3,'UK'),(4,'France'),(5,'Germany')""")
    inputParameter = open(testInputPath + "standardFunctionCreateAlterViewDataPrep.sql", "r").read()
    cursor.execute(inputParameter)
    while cursor.nextset():
        x = 1
    #Log into log file   
    logTaskProgress(cursor,batchTaskId,'Successfully performed data prep for getAllViewDefinitions')
except Exception as e:
    errorMessage = "Exception occurred while performing data preop for createConformedMasterView: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Test function getAllViewDefinitions
try:
    executionOutputStatus = 'Mismatched'
    testCaseStatus = 'failed'
    testCaseScenario = 'Test function getAllViewDefinitions'
    tmp_viewDefintionsPandasDF = getAllViewDefinitions(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    viewDefintionsPandasDF = tmp_viewDefintionsPandasDF[tmp_viewDefintionsPandasDF["ViewName"] == "conformed.VwTestx3"]
    if len(viewDefintionsPandasDF) >= 1:
            executionOutputStatus = 'As expected'
            testCaseStatus = 'success'
        
     #Log overall test status into unit test table 
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                     testCaseScenario,executionOutputStatus,outputLocation,
                     datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successful Tested getAllViewDefinitions function')  

except Exception as e:
    errorMessage="Exception occurred calling function GetAllViewDefinitions : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  

# COMMAND ----------

# DBTITLE 1,Test function createAlterViews
try:
    createAlterViews(viewDefintionsPandasDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    lst = 'Conformed.VwTestx3'        
    cnt = spark.sql("select count(1) from {}".format(lst))
    if cnt.count() >= 1:
        executionOutputStatus = 'As expected'
        testCaseStatus = 'success'
    
    #Log overall test status into unit test table 
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                     testCaseScenario,executionOutputStatus,outputLocation,
                     datetime.now(),testResult)
    logTaskProgress(cursor,batchTaskId,'Successful Tested createAlterViews function')  

except Exception as e:
    errorMessage="Exception occurred calling function createAlterViews : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Metadata CleanUp for getAllViewDefinitions
#run cleanup script
try:
  cleanup = open("{}standardFunctionCreateAlterViewDataCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for usp_mark_dates_loaded')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for usp_mark_dates_loaded: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False    

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup - ObjectReference
#script for metadata cleanup
try: 
  metadatacleanup = open("{}standardFunctionObjectReferenceCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Executed cleanup scripts successfully')     
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test sqdw connection
try :
  #Parameters for executing sql_db_conn
  testObjectName = 'StandardFunctions_sqlDWConn'
  requiredInputParameter = 'sqlDWConnectionString'
  testCaseScenario ='Synapse database connection check'
  sqlDWConnectionString = dbutils.secrets.get(scope="lza-da-kv-001-d", key="lza-dp-sqlacct-001-databricks-syn-connection")
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  
  #Call sql_db_conn to connect to a database
  sqlDWConn,sqlDWCursor = sqlDWConn(sqlDWConnectionString,
                          batchTaskId,                          
                          adfPipelineName,
                          clusterId,
                          notebookName,
                          errorLogFileLocation,
                          conn,
                          cursor)
  
  #Execute a simple select query to check if connection is successfull 
  connCheckDF = pd.read_sql_query("select GETDATE() as the_date",sqlDWConn)
  #Converting panda dataframe to spark dataframe 
  connCheckDFSpark = spark.createDataFrame(connCheckDF)
  
  if connCheckDFSpark.count() > 0:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  
  sqlDWCloseConn(sqlDWConn,sqlDWCursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation,conn,cursor)
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,datetime.now(),testResult)    
  logTaskProgress(cursor,batchTaskId,'Successful tested sqlDWConn function')
except Exception as e:
    errorMessage = "Exception occured while connecting to database: " + str(e)
    logToFile(errorLogFileLocation,errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Generate batch_task_id for usp_populate_master_view_date_availablity
#populate data into object_dates_availability table
try:
  inputParameter = open("{}standardFunctionPopulateConformedMasterViewDateAvailablityDataPrep.sql".format(testInputPath), "r").read()
  inputParameter = inputParameter.replace('<batch_id>',str(batchId))
  inputParameterResults = pd.read_sql_query(inputParameter,conn)
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])  
  cnfViewObjId = int(inputParameterResults.at[0,'cnf_vw_object_id'])
  
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate input parameters for PopulateConformedMasterViewDateAvailablity')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameters for PopulateConformedMasterViewDateAvailablity': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Test populateConformedMasterViewDateAvailablity
try:
  testCaseScenario ='Check populateConformedMasterViewDateAvailablity function'
  requiredInputParameter = 'batchTaskId'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  
  populateConformedMasterViewDateAvailablity(cursor,newBatchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  results = pd.read_sql('select COUNT(*) as TotalRows from audit.tbl_object_dates_availability where output_object_id = {}'.format(cnfViewObjId),conn)

  #view has 2 tables so expecting rows count as 4 (2 rows added by data prep and 2 rows by proc)
  if (results.at[0,'TotalRows'] == 2):
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)  
  
  logTaskProgress(cursor,batchTaskId,'Successful Tested populateConformedMasterViewDateAvailablity function') 
except Exception as e:
  errorMessage = "Exception occured while testing populateConformedMasterViewDateAvailablity:" + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Metadata cleanup for populateConformedMasterViewDateAvailablity
#run cleanup script
try:
  cleanup = open("{}standardFunctionPopulateConformedMasterViewDateAvailablityCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Execution of cleanup script for populateConformedMasterViewDateAvailablity')
except Exception as e:
  errorMessage = "Exception occurred while executing of cleanup script for populateConformedMasterViewDateAvailablity: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,close database connection
#call taskEndAndCloseConn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False