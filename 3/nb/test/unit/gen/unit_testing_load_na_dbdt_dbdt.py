# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>Unit test for unit_testing_load_na_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Unit test for load_na_dbdt_dbdt notebook </td></tr>
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
# MAGIC     <td>updated call to mark dates loaded with new parameters</td>
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
  from itertools import chain
  from datetime import datetime, timedelta
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
  from pyspark.sql.functions import lit,explode,col,concat_ws,create_map,array,concat,collect_list
  from pyspark import SparkContext,SparkConf
  from functools import reduce
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
  testCaseScenario = ''
  outputLocation = ''
  testObject = 'notebook'
  testObjectName = 'loadNaDbdtDbdt'
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

# DBTITLE 1,Drop the delta table if it exists
# MAGIC %sql
# MAGIC drop table if exists testLoadNaDbdtDbdt_a;
# MAGIC drop table if exists Stg_testLoadNaDbdtDbdt_b;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(tableInputPath + '/testLoadNaDbdtDbdt_a',recurse=True)
dbutils.fs.rm(tableInputPath + '/Stg_testLoadNaDbdtDbdt_b',recurse=True)

# COMMAND ----------

# DBTITLE 1,Create source delta table
# MAGIC %sql
# MAGIC create table testLoadNaDbdtDbdt_a(
# MAGIC row_id string,
# MAGIC id     string)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/loadNaDbdtDbdt/input/testLoadNaDbdtDbdt_a";
# MAGIC 
# MAGIC -- insert data into table
# MAGIC insert into testLoadNaDbdtDbdt_a  values('aaa','a');
# MAGIC insert into testLoadNaDbdtDbdt_a  values('bbb','b');
# MAGIC insert into testLoadNaDbdtDbdt_a  values('ccc','c');
# MAGIC insert into testLoadNaDbdtDbdt_a  values('ddd','d');

# COMMAND ----------

# DBTITLE 1,Metadata Cleanup
try:
  #run cleanup script
  cleanupMetadata = open("{}LoadNaDbdtDbdtCleanUp.sql".format(testInputPath), "r").read()
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
  inputParmeter = open("{}LoadNaDbdtDbdtDataPrep1.sql".format(testInputPath), "r").read()
  inputParmeter = inputParmeter.replace('<batch_id>', str(batchId))
  
  inputParameterResults = pd.read_sql_query(inputParmeter,conn).astype(str)
  
  #get the batch_task_id and cast as int
  newBatchTaskId = int(inputParameterResults.at[0,'batch_task_id'])
  #insertion of data into object,task_object_map, object_definition and object_dates_availability tables
  dataPreparation = open("{}LoadNaDbdtDbdtDataPrep2.sql".format(testInputPath), "r").read()
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

# DBTITLE 1,Get metadata - Execute high and low level stored procedures
#call spExecHighAndLowLevel function to retrieve values from store procedure and store in pandas dataframe                                         
try:        
  sourceDestinationTableDetails,sourceDestinationFieldsDetails = spExecHighAndLowLevel(conn
                                                                                       ,cursor
                                                                                       ,newBatchTaskId
                                                                                       ,adfPipelineName
                                                                                       ,clusterId
                                                                                       ,notebookName
                                                                                       ,errorLogFileLocation)

  #Replace the nulls with 'none' as this sp always returns single record no need to specify schema
  sourceDestinationTableDetails=sourceDestinationTableDetails.fillna('none')
  logTaskProgress(cursor,batchTaskId,"Executed high and low level store procedures")
except Exception as e:
  errorMessage="Exception occured while execution of object high and low level stored procedures: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test convertPandasToSparkDf
#call Transform_pd_df_to_Pyspark_df function to convert pandas to pyspark dataframe and store return values in variables
try:
  sourceDestinationFieldsDetailsSchema = getStructForHighAndLowLevelDf()
  srcAndDesTableDF,srcAndDesFieldsDF = convertPandasToSparkDf(sourceDestinationTableDetails,
                                                                  sourceDestinationFieldsDetails,
                                                                  sourceDestinationFieldsDetailsSchema,
                                                                  cursor,
                                                                  batchTaskId,
                                                                  adfPipelineName,
                                                                  clusterId,
                                                                  notebookName,
                                                                  errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Converted sourceDestinationTableDetails and sourceDestinationFieldsDetails pandas to spark dataframes")
except Exception as e:
  errorMessage="Unable to convert pandas to spark dataframe: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Get dataframe values into variables
try:
  #collect dataframe row as a dictionary
  srcDesDict = srcAndDesTableDF.collect()[0].asDict()
  #the list of source columns that are required for destination e.g. ["name as firstname", "dob as dateofbirth"]
  sourceColumns = srcAndDesFieldsDF.agg(collect_list(srcAndDesFieldsDF.source_as_destination)).collect()[0][0]
  #the id of the source taken from the procedure output
  sourceObjectId = srcDesDict['source_object_id']
  #the name of the source taken from the procedure output
  sourceTableName = srcDesDict['source_object_name']
  #the location of the source taken from the procedure output
  sourceLocation = srcDesDict['source_location']
  #the format of the source taken from the procedure output
  sourceFormat = srcDesDict['source_format']
  #the header of the source taken from the procedure output
  sourceHeader=srcDesDict['source_header']
  #the calculated where expression to be applied in source dataframe
  whereExpression = srcDesDict['source_where_clause']
  #the xml start string to be applied when calculating xml fields
  xmlStartString = srcDesDict['source_XML_string_prefix']
  #the id of the target object taken from the procedure ouptut
  targetObjectId=srcDesDict['destination_object_id']
  #the name of the target table taken from the procedure output
  targetObjectName=srcDesDict['destination_object_name']
  #the name of the target location taken from the procedure output
  targetLocation=srcDesDict['destination_location']
  #the format of the target taken from the procedure output
  targetFormat=srcDesDict['destination_format']
  #Check Target object has Pii attributes
  calculatePiiHash=srcDesDict['calculate_pii_hash']

  
  logTaskProgress(cursor,batchTaskId,"Got dataframe values into variables")
except Exception as e:
  errorMessage="Unable to retrieve the dataframe values as variables: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Get Column List Details
#call getSelectAndWithColumnDetails to get Columnlist tuple
try:
  columnList = getSelectAndWithColumnDetails(srcAndDesFieldsDF
                                             ,cursor 
                                             ,batchTaskId
                                             ,adfPipelineName
                                             ,clusterId
                                             ,notebookName
                                             ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"column list tuples are created")
except Exception as e:
  errorMessage="Unable to retrieve he column Details for With Expression and select: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Get With And Select Column Expressions
try:
  #Get With Column Expression and Select Expression Details by calling the function
  withExpressionListFinal,columnListFinal=getWithExpressionsAndSelectList(columnList
                                                                          ,xmlStartString
                                                                          ,cursor
                                                                          ,batchTaskId
                                                                          ,adfPipelineName
                                                                          ,clusterId
                                                                          ,notebookName
                                                                          ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"with column and select column expressions are created")
  #Prepare select expression
  selectExpression=[item[1] for item in columnListFinal]  
  
except Exception as e:
  errorMessage="Unable to retrieve with and selectedExpressions: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Selection of destination columns from source
# call selectDestFromSrc function to select destination column from the source and store return values in a variable.
try:
  sourceSelectWhereDF = selectDestFromSrc(sourceFormat
                                          ,sourceHeader
                                          ,sourceLocation
                                          ,sourceTableName
                                          ,withExpressionListFinal
                                          ,selectExpression
                                          ,whereExpression
                                          ,cursor
                                          ,batchTaskId
                                          ,adfPipelineName
                                          ,clusterId
                                          ,notebookName
                                          ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Destination dataframes are created")
except Exception as e:
  errorMessage="Exception occured while reading destination columns from the source: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
  assert False

# COMMAND ----------

# DBTITLE 1,Create destination delta table
# MAGIC %sql
# MAGIC create table Stg_testLoadNaDbdtDbdt_b(
# MAGIC row_id string,
# MAGIC id     string)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/loadNaDbdtDbdt/input/Stg_testLoadNaDbdtDbdt_b";

# COMMAND ----------

# DBTITLE 1,write to target location
#call write_to_target function to write dataframe into destination location
try:
  logTaskProgress(cursor,batchTaskId,"Started writing the destination dataframes to target location")
  writeToTarget(sourceSelectWhereDF
                  ,targetLocation
                  ,targetObjectName
                  ,targetFormat
                  ,cursor
                  ,batchTaskId
                  ,adfPipelineName
                  ,clusterId
                  ,notebookName
                  ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Finished writing the destination dataframes to target location")
except Exception as e:
  errorMessage="Exception occured while loading into target location: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario-1 Verify data loaded into destination table
#While creating table manually there is no data in destination table. after function run the data will be insert from source to destination
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify data loaded into destination table' 
  destinationTable = 'Stg_testLoadNaDbdtDbdt_b' 
  #If count of table > 0 then data is loaded from source to destination
  funcOutput = spark.sql("select * from {}".format(destinationTable))
  if funcOutput.count() > 0 :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 3 function tableStructureCheck')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 function tableStructureCheck : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-2 Verify where condition while loading from delta to delta
#While preparing metadata gave where condition row_id = 'aaa',So verify if count is 1 and row_id value is 'aaa' then make test case is success
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Verify table partition update' 
  destinationTable = 'Stg_testLoadNaDbdtDbdt_b' 
  funcOutput = spark.sql("select distinct(row_id) from {}".format(destinationTable))
  if funcOutput.count() == 1 and  funcOutput.collect()[0][0] == 'aaa':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 3 function tableStructureCheck')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 function tableStructureCheck : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Data preparation for markDatesLoaded
#insertion of data into object,task_object_map, object_definition and object_dates_availability tables
try:
  #execute the script and get batch_task_id and task_id
  inputParmeter = open("{}LoadNaDbdtDbdtDataPrep3.sql".format(testInputPath), "r").read()
  inputParmeter = inputParmeter.replace('<targetObjectId>', str(targetObjectId)).replace('<newBatchTaskId>',str(newBatchTaskId)).replace('<sourceObjectId>', str(sourceObjectId))
  cursor.execute(inputParmeter)
  while cursor.nextset():
    x = 1
  
  logTaskProgress(cursor,batchTaskId,'Successful execution of script to generate input parameter and data insertion into tables')
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter and data insertion': " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Mark the dates as loaded into the destination
try:
    conn, cursor = markDatesLoaded(targetObjectId, sourceObjectId, currentTs,currentTs,dbConn,conn,cursor,newBatchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    logTaskProgress(cursor,batchTaskId,"Loaded dates are marked for destination object")
except Exception as e:
  errorMessage="Exception occured while marking the dates as loaded: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario-3 Verify function markDatesLoaded is updating
#update output_datetime_from and output_datetime_to columns in object_dates_availability table
try:
  #Parameters to run markDatesLoaded function
  testCaseScenario ='Verify function markDatesLoaded is updating'
  requiredInputParameter = 'destinationObjectId, sourceObjectId, datetimeFrom, datetimeTo, cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #reading markDatesLoaded result as pandas dataframe
  spOutput = pd.read_sql_query("SELECT * FROM audit.vw_object_dates_availability WHERE batch_task_id = {} AND output_object_id = {} AND load_successful = 1".format(newBatchTaskId,targetObjectId),conn)
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

# DBTITLE 1,Metadata Cleanup
try:
  #run cleanup script
  cleanupMetadata = open("{}LoadNaDbdtDbdtCleanUp.sql".format(testInputPath), "r").read()
  cursor.execute(cleanupMetadata)
  while cursor.nextset():
    x = 1 
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')
except Exception as e:
  errorMessage = "Exception occured while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Drop the delta table if it exists
# MAGIC %sql
# MAGIC drop table if exists testLoadNaDbdtDbdt_a;
# MAGIC drop table if exists Stg_testLoadNaDbdtDbdt_b;

# COMMAND ----------

# DBTITLE 1,Remove the directory for previous runs
dbutils.fs.rm(tableInputPath + '/testLoadNaDbdtDbdt_a',recurse=True)
dbutils.fs.rm(tableInputPath + '/Stg_testLoadNaDbdtDbdt_b',recurse=True)

# COMMAND ----------

# DBTITLE 1,close database connection
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