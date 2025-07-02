# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_json_unstructuredSource_gen2_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Unit test for json_unstructuredSource_gen2_dbdt </td></tr>
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
# MAGIC   <td>2019/01/01</td>
# MAGIC   <td>Framework</td>
# MAGIC   <td>updated call to mark dates loaded with new parameters</td>
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

# DBTITLE 1,Run log function notebook
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run standard function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run unstructuredSource function notebook
# MAGIC %run ../../../util/spe/unstructuredSource_functions

# COMMAND ----------

# DBTITLE 1,Import Module
try:
  import pandas as pd
  import pyodbc
  import json
  from datetime import datetime, timedelta
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,BooleanType
  from pyspark.sql.functions import lit,col,collect_list,explode,concat
  from pyspark import SparkContext,SparkConf
  from collections import defaultdict
  from functools import reduce
except Exception as e:
  errorMessage = "Exception occurred while import modules " + str(e)
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
  date = currentTs

  #PARAMETER FOR LOG_ERROR
  errorLine = ''
  errorMessage = ''
  adfPipelineName = ''
  clusterId = ''
  notebookName = ''
  batchId = -1
  batchTaskId = -1
  
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
  testObjectName = 'jsonunstructuredSourceGen2Dbdt'
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
  errorMessage = "Exception occurred while variable declaration " + str(e)
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

# DBTITLE 1,Metadata Cleanup for spExecHighAndLowLevel previous run
#script for  metadata cleanup
try: 
  metadatacleanup = open("{}jsonunstructuredSourceGen2DbdtCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed cleanup scripts successfully')     
except Exception as e:
  errorMessage = "Exception occurred while executing cleanup scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False
  

# COMMAND ----------

# DBTITLE 1,Data preparation
#data preparation for high and low level stored procedures
try:
  #execute the script and get batch_task_id and task_id
  inputParmeter = open("{}jsonunstructuredSourceGen2DbdtPrep1.sql".format(testInputPath), "r").read()
  inputParmeter = inputParmeter.replace('<batch_id>', str(batchId))
  
  inputParameterResults = pd.read_sql_query(inputParmeter,conn).astype(str)
  
  #get the batch_task_id and cast as int
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])
  
  #get the task_id and cast as int
  newTaskId = int(inputParameterResults.at[0,'task_id'])
  
  #insertion of data into object,task_object_map, object_definition and object_dates_availability tables
  dataPreparation = open("{}jsonunstructuredSourceGen2DbdtPrep2.sql".format(testInputPath), "r").read()
  dataPreparation = dataPreparation.replace('<batch_task_id>', str(newBatchTaskId)).replace('<task_id>', str(newTaskId))
  dataPreparationResults = pd.read_sql_query(dataPreparation,conn).astype(str)
  newSourceObjectId = int(dataPreparationResults.at[0,'input_object_id'])
  newDestinationObjectId = int(dataPreparationResults.at[0,'output_object_id'])
  newOutputDateTime = str(dataPreparationResults.at[0,'output_date'])[0:23]
  
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate input parameter and data insertion into tables')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter and data insertion': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Drop the input Delta table if already exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS unitTestjsonunstructuredSourceGen2Dbdt_a

# COMMAND ----------

# DBTITLE 1,Create input Delta table
# MAGIC %sql
# MAGIC create table unitTestjsonunstructuredSourceGen2Dbdt_a(
# MAGIC firstName string,
# MAGIC lastName string,
# MAGIC gender string,
# MAGIC age int,
# MAGIC streetAddress int,
# MAGIC city string,
# MAGIC state string,
# MAGIC postalCode int,
# MAGIC phoneType string,
# MAGIC phoneNumber bigint)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/jsonunstructuredSourceGen2Dbdt/input/unitTestjsonunstructuredSourceGen2Dbdt_a"

# COMMAND ----------

# DBTITLE 1,Insert data into source table
try:
  fileName = "jsonunstructuredSourceGen2Dbdt.json"
  inputFormat = "delta"
  srcFile = tableInputPath + fileName
  tgtTableLocation = tableInputPath + 'unitTestjsonunstructuredSourceGen2Dbdt_a' 
  fileName = "jsonunstructuredSourceGen2Dbdt.json"
  inputFormat = "delta"
  srcFile = tableInputPath + fileName
  
  #Create dataframe from json file
  inputJsonDF = spark.read.json(srcFile, multiLine= True)
  
  #explode the array
  inputJsonExplodeDF = inputJsonDF.select("firstName", "lastName", "gender", "age", col("address.streetAddress").alias("streetAddress"), col("address.city").alias("city"), col("address.state").alias("state"), col("address.postalCode").alias("postalCode"), explode("phoneNumbers").alias("phoneNumbers"))
  
  #convert data types
  inputJsonExplDF = inputJsonExplodeDF.select("firstName", "lastName", "gender", col("age").alias("age").cast("int"), col("streetAddress").alias("streetAddress").cast("int"), "city", "state", col("postalCode").alias("postalCode").cast("int"), col("phoneNumbers.type").alias("phoneType"), col("phoneNumbers.number").alias("phoneNumber").cast("bigint"))

  #Load into delta table
  inputJsonExplDF.write.format(inputFormat).mode("overwrite").save(tgtTableLocation)
except Exception as e:
  assert False

# COMMAND ----------

# DBTITLE 1,Test spExecHighAndLowLevel function
try:
  #Parameters to run spExecHighAndLowLevel function
  testCaseScenario ='get the object and attribute details'
  requiredInputParameter = 'conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

  sourceDestinationTableDetails,sourceDestinationFieldsDetails = spExecHighAndLowLevel(conn,cursor,newBatchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  # sourceDestinationTableDetails,sourceDestinationFieldsDetails
  spObjectName = sourceDestinationTableDetails.at[0,'source_object_name']
  spAttributeName = sourceDestinationFieldsDetails.at[0,'source_object_attribute_name']
  objectName = 'unitTestjsonunstructuredSourceGen2Dbdt_a'
  attributeName = 'firstName'
  
  #check the object and attribute with the actual output
  if spObjectName == objectName and spAttributeName == attributeName:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  
  #Log the test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully tested spExecHighAndLowLevel function')    
except Exception as e:
    errorMessage = "Exception occurred while testing spExecHighAndLowLevel function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False
  

# COMMAND ----------

# DBTITLE 1,Test convertPandasToSparkDf function
try:  
  testCaseScenario = 'convert pandas to spark dataframe'
  requiredInputParameter = 'sourceDestinationTableDetails,sourceDestinationFieldsDetails,sourceDestinationFieldsDetailsSchema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  
  #call schema
  sourceDestinationFieldsDetailsSchema = getStructForHighAndLowLevelDf()
  
  #transform pandas dataframe to spark dataframe
  srcAndDesTableDF,srcAndDesFieldsDF = convertPandasToSparkDf(sourceDestinationTableDetails,sourceDestinationFieldsDetails,sourceDestinationFieldsDetailsSchema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  # check count of objects and and attributes
  if srcAndDesTableDF.count() == 1 and srcAndDesFieldsDF.count() == 4:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  
  #Log the test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                         testCaseScenario,executionOutputStatus,outputLocation,
                         datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully tested convertPandasToSparkDf function')
except Exception as e:
    errorMessage = "Exception occured while testing convertPandasToSparkDf function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False


# COMMAND ----------

# DBTITLE 1,Get dataframe values into variables
try:
  #collect dataframe row as a dictionary
  srcDesDict = srcAndDesTableDF.collect()[0].asDict()
  
  #the list of source columns that are required for destination e.g. ["name as firstname", "dob as dateofbirth"]
  sourceColumns = srcAndDesFieldsDF.agg(collect_list(srcAndDesFieldsDF.source_as_destination)).collect()[0][0]
  
  #the name of the source taken from the procedure output
  sourceTableName = srcDesDict['source_object_name']
  
  #the location of the source taken from the procedure output
  sourceLocation = srcDesDict['source_location']
 
  #the format of the source taken from the procedure output
  sourceFormat = srcDesDict['source_format']
  
  #the header of the source taken from the procedure output
  sourceHeader = srcDesDict['source_header']
  
  #the calculated where expression to be applied in source dataframe
  whereExpression = srcDesDict['source_where_clause']
  
  #the xml start string to be applied when calculating xml fields
  jsonStartString = srcDesDict['source_XML_string_prefix']
  
  #the id of the target object taken from the procedure ouptut
  targetObjectId = srcDesDict['destination_object_id']
  
  #the name of the target table taken from the procedure output
  targetObjectName = srcDesDict['destination_object_name']
  
  #the name of the target location taken from the procedure output
  targetLocation = srcDesDict['destination_location']
  
  #the format of the target taken from the procedure output
  targetFormat = srcDesDict['destination_format']

  #the properties of the target taken from the procedure output
  targetProperties = srcDesDict['destination_extended_properties']
  
  logTaskProgress(cursor,batchTaskId,"Got dataframe values into variables")
except Exception as e:
  errorMessage = "Unable to retrieve the dataframe values as variables: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test getSelectAndWithColumnDetails function
try:
  testCaseScenario ='getSelectAndWithColumnDetailsfunction test to get the column list'
  #Parameters to run getSelectAndWithColumnDetails function
  requiredInputParameter = 'srcAndDesFieldsDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

  #execute getSelectAndWithColumnDetails to get the column list
  columnList = getSelectAndWithColumnDetails(srcAndDesFieldsDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  for colList in columnList:
    # validate function input data and count
    if 'First_Name' in colList and len(columnList) == 4 :
      testResult = 'success'
      executionOutputStatus = 'As Expected'
  
  #Log the test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,
                    datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested getSelectAndWithColumnDetails function')
except Exception as e:
    errorMessage="Exception occured while testing getSelectAndWithColumnDetails function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False


# COMMAND ----------

# DBTITLE 1,Test getWithExpressionsAndSelectList function
try :
  testScenarioReference = 'GetWithExpressionsAndSelectList'
  testCaseScenario ='Get the column with expression'
  #Variable need to run getWithExpressionsAndSelectList function 
  requiredInputParameter = 'columnList, xmlStartString,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation' 
   
  #construct expectedResults for comparison   
  expectedResults = [('First_Name', 'firstName AS First_Name', 'none', 'firstName', 'STRING', 'STRING'), ('Age', 'age AS Age', 'none', 'age', 'int', 'int'), ('City', 'city AS City', 'none', 'city', 'STRING', 'STRING'), ('phone_Number', 'phoneNumber AS phone_Number', 'none', 'phoneNumber', 'bigint', 'bigint')]
  
  #function call
  withExpressionListFinal, ColumnListFinal = getWithExpressionsAndSelectList(columnList, jsonStartString,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  #Now check the length of ColumnListFinal, and compare ColumnListFinal and expected result
  if len(ColumnListFinal) == 4 and ColumnListFinal == expectedResults: #If test result is true, make test result is success
    testResult = 'success'
    executionOutputStatus = 'As Expected'

  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully tested getWithExpressionsAndSelectList')
    
except Exception as e:
    errorMessage = "Exception occured while testing getWithExpressionsAndSelectList function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Register unstructuredSource functions
try:
  #input parameter
  targertObjectProperties = json.loads(targetProperties)
  schemaLocation = targertObjectProperties['schema']
  
  #Call the function to register JSON functions 
  unstructuredSourceFunctions.getunstructuredSourceSchemaAndRegisterFunction(schemaLocation)
  logTaskProgress(cursor,batchTaskId,"unstructuredSource schema and udf are registered")
except Exception as e:
  errorMessage = "Unable to retrieve with and select Expressions: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Test selectDestFromSrc
#test the function that select destination column (aliased) from the source 
try :
  testCaseScenario ='Select the destination columns from the source dataframe'
  #Parameters to run selectDestFromSrc function
  requiredInputParameter = 'sourceFormat,sourceHeader,sourceLocation,sourceTableName,columnList, sourceColumns,whereExpression,cursor,batchTaskId, adfPipelineName,clusterId,notebookName,errorLogFileLocation' 
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
 
     
  #Run selectDestFromSrc function to read the source file and then rename them to the destination column names
  sourceSelectWhereDF = selectDestFromSrc(sourceFormat,sourceHeader,sourceLocation,sourceTableName,columnList,
                                    sourceColumns,whereExpression,cursor,batchTaskId,
                                    adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  
  #compare the return count of the function
  if sourceSelectWhereDF.count() == 1:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log the test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully tested selectDestFromSrc function')
except Exception as e:
    errorMessage = "Exception occured while testing selectDestFromSrc function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test writeToTarget
#test the function that writes the dataframe in target location
try :
  testCaseScenario ='writeToTarget function test if the given dataframe is written to the given location'
  #Variable need to run write_to_target function 
  requiredInputParameter = 'sourceDF,targetTableLocation,targetObjectName,targetFormat, cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'  
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 

  #clean destination if it exists
  dbutils.fs.rm(tableOutputPath,True)
  
  #Run write_to_target function passing the dataframe and the location the output file is to be created
  desDF = writeToTarget(sourceSelectWhereDF,targetLocation,targetObjectName,targetFormat,
                            cursor,batchTaskId,adfPipelineName,clusterId,
                            notebookName,errorLogFileLocation) 
  
  #After function execution read the target location and check if the file is created 
  fileCheck = bool(dbutils.fs.ls(tableOutputPath))
  
  #check if the file exists in target
  if fileCheck == True:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully tested writeToTarget')
except Exception as e:
    errorMessage = "Exception occured while testing writeToTarget function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False


# COMMAND ----------

# DBTITLE 1,Test markDatesLoaded
#update output_datetime_from and output_datetime_to columns in object_dates_availability table
try: 
  testCaseScenario ='log markDatesLoaded to complete the load from source object'
  #Parameters to run markDatesLoaded function
  requiredInputParameter = 'destinationObjectId, sourceObjectId, datetimeFrom, datetimeTo, cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  
  #assign datetime value to the variables
  newDatetimeFrom = newOutputDateTime
  newDatetimeTo = newOutputDateTime
 
  #call markDatesLoaded function
  conn, cursor = markDatesLoaded(newDestinationObjectId, newSourceObjectId, newDatetimeFrom, newDatetimeTo, dbconn, conn, cursor, newBatchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
  
  #reading markDatesLoaded result as pandas dataframe
  spOutput = pd.read_sql_query("SELECT * FROM audit.tbl_object_dates_availability WHERE batch_task_id = {} AND input_object_id = {} AND output_object_id = {} AND output_datetime_from = '{}' AND output_datetime_to = '{}' AND load_successful = 1".format(newBatchTaskId,newSourceObjectId,newDestinationObjectId,newDatetimeFrom,newDatetimeTo),conn)
  
  #check the count of dataframe
  if spOutput.shape[0] == 1:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  
  #Log the test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successfully tested markDatesLoaded')    
except Exception as e:
    errorMessage = "Exception occured while testing markDatesLoaded function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False
  

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup
#script for  metadata cleanup
try: 
  metadatacleanup = open("{}jsonunstructuredSourceGen2DbdtCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(metadatacleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,' Executed cleanup scripts successfully')     
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