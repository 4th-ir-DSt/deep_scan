# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_struct_functions</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Unit Test for struct_functions </td></tr>
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
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC     <td></td>
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
# MAGIC     <td>Amended Pandas DF to allow upgrade of pandas / spark 3 cluster runtimes</td>
# MAGIC   </tr>  
# MAGIC </table>  
# MAGIC   
# MAGIC ## General Standards applying to each notebook
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
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DecimalType,DateType
  from datetime import datetime, timedelta
  from pyspark.sql.functions import lit,udf, struct, col, split, posexplode,max,regexp_replace,coalesce
  from pyspark import SparkContext,SparkConf
  from itertools import chain
except Exception as e:
  errorMessage = "Exception occurred while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Run log function notebook
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run standard function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run unstructuredxmlSource functions notebook
# MAGIC %run ../../../util/spe/struct_functions

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
  testObjectName = 'structFunctions'  
  requiredInputParameter = ''
  testCaseScenario = ''
  executionDateTime = datetime.now()
  sampleOutputLocation = 'NA'
  
   #initialise Notebook variables
  objectSchemaStruct = StructType()
  objectSchemaStructDic = {}
  structWrapperType = 'JSON'
  superSource = 'unStructuredSource_test'
  location ="/mnt/raw/reference/"+superSource
  
  #path for datasets used in stored procedure 
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = 'dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  testDirPath = testInputPath.replace('/dbfs','')
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

# DBTITLE 1,Remove the folders if it exists
try :
  #clean up the test reference folder
  dbutils.fs.rm(location, True);
  logTaskProgress(cursor,batchTaskId,'Deleted the test refrence folders successfully')
except Exception as e:
  errorMessage = 'Exception occurred while deleting the test reference : ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test function findElementType 
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test functions findElementType'
  
  # If an element has a child findElementType should return StructType 
  element = 'A1ADCH'
  pathList = ['message.data.A1ADCH.A1BDXF']
  returnElementType = structFunctions.findElementType(element
                                                     , pathList
                                                     , cursor
                                                     , batchTaskId
                                                     ,  adfPipelineName
                                                     , clusterId
                                                     , notebookName
                                                     , errorLogFileLocation)

  # If an element doesn’t has a child findElementType should return StringType 
  elementMissingElement = 'A1ADCH'
  pathListMissingElement = ['message.data']
  returnElementTypeMissingElement = structFunctions.findElementType(elementMissingElement
                                                                  , pathListMissingElement
                                                                  , cursor
                                                                  , batchTaskId
                                                                  , adfPipelineName
                                                                  , clusterId
                                                                  , notebookName
                                                                  , errorLogFileLocation)

 # If an element doesn’t has a child findElementType should return StringType 
  elementMultiplePathList = 'A1ADCH'
  pathListMultiplePathList = ['' ,'message.data' ,'A1ADCH']
  returnElementTypeMultiplePathList = structFunctions.findElementType(elementMultiplePathList
                                                                   , pathListMultiplePathList
                                                                   , cursor
                                                                   , batchTaskId
                                                                   , adfPipelineName
                                                                   , clusterId
                                                                   , notebookName
                                                                   , errorLogFileLocation)


  
  if returnElementType =='StructType()' and returnElementTypeMissingElement =='StringType()' and returnElementTypeMultiplePathList =='StringType()':
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for functions findElementType ')  
except Exception as e:
  errorMessage = "Exception occurred while testing functions findElementType  : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test function findElementArrayStatus
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test functions findElementType'
  
  # If the element is array type it should return True 
  elementArray = 'A1ADCH'
  pathListArray = ['message.data','[A1ADCH]']
  returnStatusArray = structFunctions.findElementArrayStatus(elementArray
                                                           , pathListArray
                                                           , cursor
                                                           , batchTaskId
                                                           , adfPipelineName
                                                           , clusterId
                                                           , notebookName
                                                           , errorLogFileLocation)

  #  If an element is not a array type it should return False  
  element = 'A1ADCH'
  pathList = ['message.data','A1ADCH']
  returnStatus = structFunctions.findElementArrayStatus(element
                                                      , pathList
                                                      , cursor
                                                      , batchTaskId
                                                      , adfPipelineName
                                                      , clusterId
                                                      , notebookName
                                                      , errorLogFileLocation)
 #compare the function return value 
  if returnStatusArray == True and returnStatus == False:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
     
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for functions findElementArrayStatus ')  
except Exception as e:
  errorMessage = "Exception occurred while testing functions findElementArrayStatus  : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test function generateElementDetailsDictironary
try:
  #Check for element without array
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test functions generateElementDetailsDictironary'
  
  elementList = ['message','P1ADCH']
  pathList = ['message.P1ADCH']

  expectedSchemaDic = {'message'  : [False, 'StructType()'],
                       'P1ADCH'   : [False, 'StringType()'], 
                       'OuterJSON': [False, 'StructType()']
                      }

  #Check for an array element 
  elementListArray = ['message','P1ADCH']
  pathListArray = ['message.[P1ADCH]']

  expectedSchemaDicArray = {'message'  : [False, 'StructType()'], 
                            'P1ADCH'   : [True, 'StringType()'], #findElementArrayStatus returns True as it is and array element
                            'OuterJSON': [False, 'StructType()']
                           }

  objectSchemaDic = structFunctions.generateElementDetailsDictironary(elementList
                                                                    , pathList
                                                                    , structWrapperType
                                                                    , cursor,batchTaskId
                                                                    , adfPipelineName
                                                                    , clusterId,notebookName
                                                                    , errorLogFileLocation)
  #Check for an array element 
  objectSchemaDicArray = structFunctions.generateElementDetailsDictironary(elementListArray
                                                                         , pathListArray
                                                                         , structWrapperType
                                                                         , cursor,batchTaskId
                                                                         , adfPipelineName
                                                                         , clusterId,notebookName
                                                                         , errorLogFileLocation)
 
  
  if objectSchemaDic == expectedSchemaDic and objectSchemaDicArray == expectedSchemaDicArray :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'


  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for functions generateElementDetailsDictionary ')  
except Exception as e:
  errorMessage = "Exception occurred while testing functions generateElementDetailsDictionary  : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test function getInitialListsAndDictionaries
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test functions getInitialListsAndDictionaries'

  
  #data list
  dataList = [["Raw_unStructuredSourceAyLib_Delta","message.data.P1ADCH","int","P1ADCH"]] 
  #schema 
  schema = StructType([StructField('object_name'           , StringType(),True)
                      ,StructField('Path'                  , StringType(),True)
                      ,StructField('object_attribute_type' , StringType(),True)
                      ,StructField('Element'               , StringType(),True)
                      ])
  
  #creating to spark dataframe 
  currentMetadataDetailsDF = convertPandasToSparkDfWithSchema(dataList,schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation) 
  #expected output
  expectedPathList = ['message.data.P1ADCH']
  expectedElementList = ['message', 'data', 'P1ADCH']
 
  
  #datalist with array
  dataListArray =  [["Raw_unStructuredSourceAyLib_Delta","PolMessage.PolData.[Vehicle].Ncd.[NcdStepbackData].AllowedNcdYears","int","Ncd"]] 
  #creating spark dataframe 
  metadataDetailsDFArray = convertPandasToSparkDfWithSchema(dataListArray,schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation) 
  #expected output 
  expectedElementListArray = ['PolMessage', 'PolData', 'Vehicle', 'Ncd', 'NcdStepbackData', 'AllowedNcdYears']
  expectedPathListArray = ['PolMessage.PolData.[Vehicle].Ncd.[NcdStepbackData].AllowedNcdYears']
  
  
  pathList,elementList,objectSchemaDic = structFunctions.getInitialListsAndDictionaries( currentMetadataDetailsDF
                                                                                       , structWrapperType
                                                                                       , cursor
                                                                                       , batchTaskId
                                                                                       , adfPipelineName
                                                                                       , clusterId
                                                                                       , notebookName
                                                                                       , errorLogFileLocation)
  
  pathListArray,elementListArray,objectSchemaDicArray = structFunctions.getInitialListsAndDictionaries( metadataDetailsDFArray
                                                                                                      , structWrapperType
                                                                                                      , cursor
                                                                                                      , batchTaskId
                                                                                                      , adfPipelineName
                                                                                                      , clusterId
                                                                                                      , notebookName
                                                                                                      , errorLogFileLocation)
 

  
  if expectedElementList == elementList  and expectedPathList == pathList   and expectedElementListArray == elementListArray   and expectedPathListArray == pathListArray:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'

  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for functions getInitialListsAndDictionaries ')  
except Exception as e:
  errorMessage = "Exception occurred while testing functions getInitialListsAndDictionaries  : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False                                                                 

# COMMAND ----------

# DBTITLE 1,Test function getNoElementsAndMetadataDict
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test functions getNoElementsAndMetadataDict'
  
  #creating a dataFrame
  dataList = [["Raw_unStructuredSourceAyLib_Delta","headers.[Timestamp]","TIMESTAMP","Timestamp"]]
  currentMetadataDF = convertPandasToSparkDfWithSchema(dataList,schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation) 

  #expected output
  expectedsplitMedataDic = {(0, 'headers'): {'Parent': ['OuterJSON']}
                          , (1, 'Timestamp'): {'Parent': ['headers']}
                           }

  expectedNumberOfElements = 1
  
  numberOfElements, splitMedataDic  = structFunctions.getNoElementsAndMetadataDict( currentMetadataDF
                                                                                      , structWrapperType # 'JSON'
                                                                                      , cursor
                                                                                      , batchTaskId
                                                                                      , adfPipelineName
                                                                                      , clusterId
                                                                                      , notebookName
                                                                                      , errorLogFileLocation)

  
  #compare the return value of the function with expected output 
  if expectedNumberOfElements == numberOfElements and expectedsplitMedataDic == splitMedataDic :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for functions getNoElementsAndMetadataDict ')  
except Exception as e:
  errorMessage = "Exception occurred while testing functions getNoElementsAndMetadataDict  : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test function getSchemaStruct
try:
  #Declare variables
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test functions getSchemaStruct'
  
  #variables for testing
  splitMedataDic =   { (0, 'message'): {'Parent': ['OuterJSON']} ,
                       (1, 'data')   : {'Parent': ['message']},
                       (2, 'P1ADCH') : {'Parent': ['data']}
                    }
  numberOfElements = 2
  pathList = ['message.data.P1ADCH']
  objectSchemaStruct = StructType()
  objectSchemaStructDic = {}
  
  #expected output
  expectedSchemaStruct  = "StructType(List(StructField(OuterJSON,StructType(List(StructField(message,StructType(List(StructField(data,StructType(List(StructField(P1ADCH,StringType,true))),true))),true))),true)))"

  
  
  #Testing for no child element
  splitMedataDicNoChild  = {(0, 'headers')  : {'Parent': ['OuterJSON']},
                             (1, 'Timestamp'): {'Parent': ['headers']} 
                            }
  numberOfElementsNoChild = 1
  pathListNoChild = ['headers.Timestamp']
  objectSchemaDicNoChild  =     {'headers'  : [False, 'StructType()'],
                                 'Timestamp': [False, 'StringType()'], 
                                 'OuterJSON': [False, 'StructType()']
                                 }
  expectedSchemaStructNoChild = "StructType(List(StructField(OuterJSON,StructType(List(StructField(headers,StructType(List(StructField(Timestamp,StringType,true))),true))),true)))"


  objectSchemaStruct = structFunctions.getSchemaStruct( splitMedataDic
                                                      , objectSchemaStruct
                                                      , numberOfElements
                                                      , pathList
                                                      , objectSchemaStructDic
                                                      , structWrapperType #'XML or JSON'
                                                      , objectSchemaDic 
                                                      , cursor
                                                      , batchTaskId
                                                      , adfPipelineName
                                                      , clusterId
                                                      , notebookName
                                                      , errorLogFileLocation)
  
  #run the function for No Child elements
  objectSchemaStructNoChild = structFunctions.getSchemaStruct( splitMedataDicNoChild
                                                                   , objectSchemaStruct
                                                                   , numberOfElementsNoChild
                                                                   , pathListNoChild
                                                                   , objectSchemaStructDic
                                                                   , structWrapperType #'XML or JSON'
                                                                   , objectSchemaDicNoChild
                                                                   , cursor
                                                                   , batchTaskId
                                                                   , adfPipelineName
                                                                   , clusterId
                                                                   , notebookName
                                                                   , errorLogFileLocation)
 

  #compare the return value of the function with expected output 
  if str(objectSchemaStruct) == expectedSchemaStruct  and  str(objectSchemaStructNoChild) == expectedSchemaStructNoChild :
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'
    
  #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for functions getSchemaStruct ')  
except Exception as e:
  errorMessage = "Exception occurred while testing functions getSchemaStruct  : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test function spExecGetObjectDetails
#the usp_get_raw_object_details stored procedure should return the unstructuredxmlSource object details  .
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test functions spExecGetObjectDetails'
  source = "unStructuredSource"
  
  metadataDetails = structFunctions.spExecGetObjectDetails( batchTaskId
                                                          , source
                                                          , conn
                                                          , cursor
                                                          , adfPipelineName
                                                          , clusterId
                                                          , notebookName
                                                          , errorLogFileLocation)
               
   # compare the count of rows returned by function
  if metadataDetails.shape[0] >= 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'  

   #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for functions spExecGetObjectDetails ')  
except Exception as e:
  errorMessage = "Exception occurred while testing functions spExecGetObjectDetails  : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test function convertStructDictToDataFrame
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test functions convertStructDictToDataFrame'

  #variables for testing
  splitMedataDic =   {(0, 'message'): {'Parent': ['OuterJSON']}, 
                      (1, 'data'): {'Parent': ['message']}, 
                      (2, 'P1ADCH'): {'Parent': ['data']}
                     }
  numberOfElements = 2
  pathList = ['message.data.P1ADCH']
  objectSchemaDic = {'message'  : [False, 'StructType()'],
                     'data'     : [False, 'StructType()'], 
                     'P1ADCH'   : [False, 'StringType()'], 
                     'OuterJSON': [False, 'StructType()']
                                 }
  objectSchemaStruct = StructType()
  objectSchemaStructDic = {}

  #get the objectSchemaStruct from getSchemaStruct
  objectSchemaStruct = structFunctions.getSchemaStruct( splitMedataDic
                                                      , objectSchemaStruct
                                                      , numberOfElements
                                                      , pathList
                                                      , objectSchemaStructDic
                                                      , structWrapperType #'XML or JSON'
                                                      , objectSchemaDic 
                                                      , cursor
                                                      , batchTaskId
                                                      , adfPipelineName
                                                      , clusterId
                                                      , notebookName
                                                      , errorLogFileLocation)

  objectSchemaDF = convertStructDictToDataFrame( objectSchemaDic
                                               , objectSchemaStruct
                                               , batchId
                                               , createdTimestamp
                                               , lastUpdatedTimestamp
                                               , conn
                                               , cursor
                                               , adfPipelineName
                                               , clusterId
                                               , notebookName
                                               , errorLogFileLocation )

  # compare the count of rows returned by function
  if objectSchemaDF.count() >= 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'  

   #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for functions convertStructDictToDataFrame ')  
except Exception as e:
  errorMessage = "Exception occurred while testing functions convertStructDictToDataFrame  : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test function writeSchemaDictToStorage
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Test functions writeSchemaDictToStorage'
  superSource = 'unStructuredSource_test'
  objectName = 'P1ADCH'
  outputLocation = location+ "/"+objectName+"Schema_Def"

  writeSchemaDictToStorage( superSource
                           , objectName
                           , objectSchemaDF
                           , conn
                           , cursor
                           , adfPipelineName
                           , clusterId
                           , notebookName
                           , errorLogFileLocation )

  #Check if the file is present in destination location

  outputDf = spark.read.format("csv").options(header ='true').load(outputLocation)
 
  
  # compare the count of rows returned by function
  if outputDf.count() == 1:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'  
    
   #function call to insert a row into tbl_unit_test on the test case status  
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed for functions writeSchemaDictToStorage ')  
except Exception as e:
  errorMessage = "Exception occurred while testing functions writeSchemaDictToStorage  : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Remove the file if it exists
try :
  #clean up the test reference folder
  dbutils.fs.rm(location, True);
  logTaskProgress(cursor,batchTaskId,'Deleted the test refrence folders successfully')
except Exception as e:
  errorMessage = 'Exception occurred while deleting the test reference : ' + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Complete task and close connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,
                          conn,
                          batchTaskId,
                          batchTaskSourceRows,
                          batchTaskRowsLoaded,
                          batchTaskRejectRows,
                          batchTaskResult,
                          batchTaskResultLocation,
                          adfPipelineName,
                          clusterId,
                          notebookName,
                          errorLogFileLocation)
except:
  assert False