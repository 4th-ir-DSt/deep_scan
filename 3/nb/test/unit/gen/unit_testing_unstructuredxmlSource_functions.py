# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_unstructuredxmlSource_functions</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Unit test for unstructuredxmlSource functions </td></tr>
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
# MAGIC   <td>Added test case to filter polData request type in single quotes
# MAGIC     <br>Amended Pandas DF to allow upgrade of pandas / spark 3 cluster runtimes</td>
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

# DBTITLE 1,Run unstructuredxmlSource specific functions
# MAGIC %run ../../../util/spe/unstructuredxmlSource_functions

# COMMAND ----------

# DBTITLE 1,Import Module
try:
  import pandas as pd
  import pyodbc
  import os
  import re
  from datetime import datetime, timedelta
  import xml.etree.ElementTree as ET
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
  from pyspark.sql.functions import lit,col,collect_list,explode,regexp_replace,split,posexplode,coalesce,max
  from pyspark import SparkContext,SparkConf
  from collections import defaultdict
  from functools import reduce
  from itertools import chain
except Exception as e:
  errorMessage = "Exception occurred while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try :
  #VARIBLE FOR CURRENT_TIME
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
  testObjectName = 'unstructuredxmlSourceFunctions'
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
  errorMessage = "Exception occurred while getting parameters and initiliasing error log location " + str(e)
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

# DBTITLE 1,Register unstructuredxmlSource functions
try:
  #Call the function to register unstructuredxmlSource functions 
  getunstructuredxmlSourceSchemaAndRegisterFunction()
  logTaskProgress(cursor,batchTaskId,"unstructuredxmlSourceschema and udf are registered")
except Exception as e:
  errorMessage="Unable to retrieve with and selectedExpressions: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Test getDataString function
try:
  #Parameters to run getDataString function
  testCaseScenario ='getDataString function'
  requiredInputParameter = 'xmlMessage,searchString,startTag,closeTag'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #input parameters for getDataString function
  inputxmlMessage = '<electricaldevice><laptop><name>surfacebook</name><price>$1500</price></laptop><laptop><name>thinkpad</name><price>$950</price></laptop></electricaldevice><entertainment><streaming_media><name>Netflix</name><price>$10</price></streaming_media><streaming_media><name>Amazon prime</name><price>$7</price></streaming_media></entertainment>'
  expectedOutput = 'electricaldevice><laptop><name>surfacebook</name><price>$1500</price></electricaldevice'
  searchString = 'laptop'
  startTag = 'electricaldevice'
  closeTag = 'electricaldevice'
  xmlString = getDataString(inputxmlMessage,searchString,startTag,closeTag)
  #verifying actual and expected outputs
  if xmlString == expectedOutput:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                           testCaseScenario,executionOutputStatus,outputLocation,
                           datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested getDataString function')
except Exception as e:
  errorMessage = "Exception occured while testing getDataString function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Test extractXML function with Single PolData element
try:
  #extract and construct xml message with all attributes from string message
  #Parameters to run getDataString function
  testCaseScenario ='Single PolData element extractXML function'
  requiredInputParameter = 'xmlMessage,searchString,startTag,closeTag'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #Input message read from file
  myDictTest = [
                [testInputPath + "xmlMessage1a.txt"]
               ,[testInputPath + "xmlMessage1c.txt"]
               ]

  for inputFile in myDictTest:
    fileLocation = inputFile[0]

    #read input messages
    messageFromFile = open(fileLocation, 'r').read()
    #execute
    outputXML = extractXML(messageFromFile)
    #uuid and date patterns
    uuidPattern  = r"[A-Z]{2}\-[a-z0-9]{8}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{12}"
    outputXMLUUID = outputXML.split('<UUID Val="')[1].split('<')[0].split('"/>')[0]
    outputUUID = bool(re.match(uuidPattern,outputXMLUUID))

    datepattern   = r"[0-9]{4}\-[0-9]{2}\-[0-9T0-9]{5}\:[0-9]{2}\:[0-9]{2}\,[0-9]{3}"
    outputXMLDate = outputXML.split('<Date Val="')[1].split('<')[0].split('"/>')[0]
    outputDate = bool(re.match(datepattern,outputXMLDate))

    #validate extracted xml message
    if outputUUID == True and outputDate == True and "<OuterXML>" and 'Type="Input"' in outputXML and "&lt" and "&gt" and "![CDATA[" and 'Tyep="Request"' not in outputXML:

      testResult = 'success'
      executionOutputStatus = 'As Expected'
      #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                           testCaseScenario,executionOutputStatus,outputLocation,
                           datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested extractXML function')
except Exception as e:
  errorMessage = "Exception occured while testing extractXML function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False


# COMMAND ----------

# DBTITLE 1,Test extractXML function if request has 2nd empty polData element
try: 
  #extract and construct xml message with all attributes from string message and if Request has 2nd empty PolData element
  #Parameters to run getDataString function
  testCaseScenario ='Request has 2nd empty PolData element extractXML function'
  requiredInputParameter = 'xmlMessage,searchString,startTag,closeTag'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #Input message read from file
  myDictTest = [
                [testInputPath + "xmlMessage2a.txt"]
               ,[testInputPath + "xmlMessage2b.txt"]
               ]

  for inputFile in myDictTest:
    fileLocation = inputFile[0]

    #read input messages
    messageFromFile = open(fileLocation, 'r').read()
    #execute extractXML function
    outputXML = extractXML(messageFromFile)
    #uuid and date patterns
    uuidPattern  = r"[A-Z]{2}\-[a-z0-9]{8}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{12}"
    outputXMLUUID = outputXML.split('<UUID Val="')[1].split('<')[0].split('"/>')[0]
    outputUUID = bool(re.match(uuidPattern,outputXMLUUID))

    datepattern   = r"[0-9]{4}\-[0-9]{2}\-[0-9T0-9]{5}\:[0-9]{2}\:[0-9]{2}\,[0-9]{3}"
    outputXMLDate = outputXML.split('<Date Val="')[1].split('<')[0].split('"/>')[0]
    outputDate = bool(re.match(datepattern,outputXMLDate))

    #validate extracted xml message
    if outputUUID == True and outputDate == True and "<OuterXML>" and 'PolData Type="Input"' in outputXML and "&lt" and "&gt" and "![CDATA[" and 'Tyep="Request"' and 'PolData Type="Request"' not in outputXML:
      
      testResult = 'success'
      executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                           testCaseScenario,executionOutputStatus,outputLocation,
                           datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested extractXML function')
except Exception as e:
  errorMessage = "Exception occured while testing extractXML function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Test extractXML function if request has 2 populated PolData elements
try:
  #extract and construct xml message with all attributes from string message and if message has 2 populated PolData elements
  #Parameters to run getDataString function
  testCaseScenario ='Request has 2 populated PolData elements extractXML function'
  requiredInputParameter = 'xmlMessage,searchString,startTag,closeTag'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #Input message read from file
  myDictTest = [
                [testInputPath + "xmlMessage3a.txt"]
               ,[testInputPath + "xmlMessage3b.txt"]
               ]

  for inputFile in myDictTest:
    fileLocation = inputFile[0]

    #read input messages
    messageFromFile = open(fileLocation, 'r').read()
    #execute extractXML function
    outputXML = extractXML(messageFromFile)
  #uuid and date patterns
    uuidPattern  = r"[A-Z]{2}\-[a-z0-9]{8}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{12}"
    outputXMLUUID = outputXML.split('<UUID Val="')[1].split('<')[0].split('"/>')[0]
    outputUUID = bool(re.match(uuidPattern,outputXMLUUID))

    datepattern   = r"[0-9]{4}\-[0-9]{2}\-[0-9T0-9]{5}\:[0-9]{2}\:[0-9]{2}\,[0-9]{3}"
    outputXMLDate = outputXML.split('<Date Val="')[1].split('<')[0].split('"/>')[0]
    outputDate = bool(re.match(datepattern,outputXMLDate))

    #polData count
    outputPolData = outputXML.split('PolData Type="')[1].split('<')[0].split('">')[0]
    polDataCount = re.findall(r'PolData Type="Input"', outputXML)

  #validate extracted xml message
    if len(polDataCount) == 1 and outputUUID == True and outputDate == True and "<OuterXML>" and 'PolData Type="Input"' in outputXML and "&lt" and "&gt" and "![CDATA[" and 'Tyep="Request"' and 'PolData Type="Request"' and '<ProcessingIndicators_ProcessType Val="04"/>' and '<ProcessingIndicators_ProcessType Val="37"/>' not in outputXML:
      
      testResult = 'success'
      executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                           testCaseScenario,executionOutputStatus,outputLocation,
                           datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested extractXML function')
except Exception as e:
  errorMessage = "Exception occured while testing extractXML function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False


# COMMAND ----------

# DBTITLE 1,Test extractXML function for Response PolData message
try:
  #extract and construct xml message with all attributes from Response PolData message
  #Parameters to run getDataString function
  testCaseScenario ='Response PolData extractXML function'
  requiredInputParameter = 'xmlMessage,searchString,startTag,closeTag'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #Input message read from file
  myDictTest = [
            [testInputPath + "xmlMessage5a.txt"]
           ,[testInputPath + "xmlMessage5b.txt"]
           ]

  for inputFile in myDictTest:
    fileLocation = inputFile[0]

    #read input messages
    messageFromFile = open(fileLocation, 'r').read()
    #execute extractXML function
    outputXML = extractXML(messageFromFile)
    #   print(outputXML)
    #uuid and date patterns
    uuidPattern  = r"[A-Z]{2}\-[a-z0-9]{8}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{12}"
    outputXMLUUID = outputXML.split('<UUID Val="')[1].split('<')[0].split('"/>')[0]
    outputUUID = bool(re.match(uuidPattern,outputXMLUUID))

    datepattern   = r"[0-9]{4}\-[0-9]{2}\-[0-9T0-9]{5}\:[0-9]{2}\:[0-9]{2}\,[0-9]{3}"
    outputXMLDate = outputXML.split('<Date Val="')[1].split('<')[0].split('"/>')[0]
    outputDate = bool(re.match(datepattern,outputXMLDate))

    #   #validate extracted xml message
    if outputUUID == True and outputDate == True and "<OuterXML>" and '<PolData Type="Output">' and '<PolMessage' and '<SchemeResult' in outputXML and "&lt" and "&gt" not in outputXML:
      
         
      testResult = 'success'
      executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                           testCaseScenario,executionOutputStatus,outputLocation,
                           datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested extractXML function')
except Exception as e:
  errorMessage = "Exception occured while testing extractXML function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False


# COMMAND ----------

# DBTITLE 1,Test extractXML function for Response PolData message has error result
try:
  #extract and construct xml message with all attributes from Response PolData message if message has ErrorDetails
  #Parameters to run getDataString function
  testCaseScenario ='Response PolData has ErrorDetail extractXML function'
  requiredInputParameter = 'xmlMessage,searchString,startTag,closeTag'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #Input message read from file
  myDictTest = [
            [testInputPath + "xmlMessage6a.txt"]
           ]

  for inputFile in myDictTest:
    fileLocation = inputFile[0]

    #read input messages
    messageFromFile = open(fileLocation, 'r').read()
    #execute extractXML function
    outputXML = extractXML(messageFromFile)
  #   print(outputXML)
    #uuid and date patterns
    uuidPattern  = r"[A-Z]{2}\-[a-z0-9]{8}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{12}"
    outputXMLUUID = outputXML.split('<UUID Val="')[1].split('<')[0].split('"/>')[0]
    outputUUID = bool(re.match(uuidPattern,outputXMLUUID))

    datepattern   = r"[0-9]{4}\-[0-9]{2}\-[0-9T0-9]{5}\:[0-9]{2}\:[0-9]{2}\,[0-9]{3}"
    outputXMLDate = outputXML.split('<Date Val="')[1].split('<')[0].split('"/>')[0]
    outputDate = bool(re.match(datepattern,outputXMLDate))

    #   #validate extracted xml message
    if outputUUID == True and outputDate == True and "<OuterXML>" and '<PolData Type="Output">' and '<PolMessage' and '<SchemeResult' and '<ErrorDetail' in outputXML and "&lt" and "&gt" not in outputXML:
      
     
      testResult = 'success'
      executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                           testCaseScenario,executionOutputStatus,outputLocation,
                           datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested extractXML function')
except Exception as e:
  errorMessage = "Exception occured while testing extractXML function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False


# COMMAND ----------

# DBTITLE 1,Test extractXML function for Missing Root Elements 
try:
  #test extractXML function for Missing Root Elements  
  #Parameters to run getDataString function
  testCaseScenario ='Missing Root Elements extractXML function'
  requiredInputParameter = 'xmlMessage,searchString,startTag,closeTag'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #Input message read from file
  myDictTest = [
            [testInputPath + "xmlMessage7a.txt"]
           ,[testInputPath + "xmlMessage7b.txt"]
           ,[testInputPath + "xmlMessage7c.txt"]
           ,[testInputPath + "xmlMessage7d.txt"]
           ,[testInputPath + "xmlMessage8a.txt"]
           ,[testInputPath + "xmlMessage8b.txt"]
           ,[testInputPath + "xmlMessage8c.txt"]
           ]

  for inputFile in myDictTest:
    fileLocation = inputFile[0]

    #read input messages
    messageFromFile = open(fileLocation, 'r').read()
    #execute extractXML function
    outputXML = extractXML(messageFromFile)
      #   #validate extracted xml message
    if 'Exception Occured' in outputXML:
      
            
      testResult = 'success'
      executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                           testCaseScenario,executionOutputStatus,outputLocation,
                           datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested extractXML function')
except Exception as e:
  errorMessage = "Exception occured while testing extractXML function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False


# COMMAND ----------

# DBTITLE 1,Test extractXML function if polData Request type is in single quotes
try:
  #test extractXML function for Missing Root Elements  
  #Parameters to run getDataString function
  testCaseScenario ='Test extractXML function if polData Request type is in single quotes'
  requiredInputParameter = 'xmlMessage'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #Input message read from file
  myDictTest = [
            [testInputPath + "xmlMessage9a.txt"]

           ]

  for inputFile in myDictTest:
    fileLocation = inputFile[0]

    #read input messages
    messageFromFile = open(fileLocation, 'r').read()
    #execute extractXML function
    outputXML = extractXML(messageFromFile)
      #   #validate extracted xml message
    if "<PolData Type='Request'" not in outputXML and '<PolData Type="Request"' not in outputXML:
      testResult = 'success'
      executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                           testCaseScenario,executionOutputStatus,outputLocation,
                           datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested extractXML function')
except Exception as e:
  errorMessage = "Exception occured while testing extractXML function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False


# COMMAND ----------

# DBTITLE 1,Test isArray function
#Tesing of isArray function 
try:
  testScenarioReference = 'IsArray'
  testCaseScenario ='Verify the structure is array in xml'
  requiredInputParameter = 'attribute,xmlDicSchema'
  outputLocation = '/dbfs'+ tableOutputPath + testScenarioReference + '/batch_task_id=' + str(batchTaskId) + '/' 
  
    #current attribute,xml attribute and expected output
  testData = [ 
               (1,"a" , "{'a': [False, 'StructType()']}" , False)
              ,(2,"b" , "{'b': [True, 'StructType()']}"  , True)
             ]
  overallTestResult = 'success'
  allTestResults = []
  for testCase in testData:
    testNumber = 'Test-'+str(testCase[0])
    inputAttribute = testCase[1] #xml attribute
    xmlSchemaDic = eval(testCase[2]) #Input xml schema dictionary
    expectedOutput = testCase[3] #Expected output
    
    #Execute xml parser function
    outputStatus = isArray(inputAttribute,xmlSchemaDic)

    if outputStatus == expectedOutput:
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    
    else:
      testResult = 'failed' #initialise the test result
      executionOutputStatus = 'Mismatched'
      overallTestResult = 'failed'
      
    #testid, inputString, acurrentAttribute, expectedOutput, outputString and testResult
    testDataSet = (testNumber, inputAttribute, xmlSchemaDic, expectedOutput, outputStatus, testResult)
    allTestResults.append(testDataSet)
  
  if overallTestResult == 'failed':
    if not os.path.exists(outputLocation):
      os.makedirs(outputLocation)
    #Write output to file in the ouptut location when ther is a failed scenario    
    file = open(outputLocation + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '_outputResultFile','w')
    file.write(str(allTestResults))
    file.close()
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),overallTestResult)

  logTaskProgress(cursor, batchTaskId, "Testing isArray function completed")
    
except Exception as e:
    errorMessage = "Exception occured while testing isArray  function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  

# COMMAND ----------

# DBTITLE 1,Test getType function
#Tesing of getType function
try:
  testScenarioReference = 'GetType'
  testCaseScenario ='Verify getting correct datatype from schema'
  requiredInputParameter = 'attribute,xmlDicSchema' 
  outputLocation = testOutputPath + testScenarioReference + '/batch_task_id=' + str(batchTaskId) + '/' 
    #Current attribute,xml schema dictionary and expected output
    #Make sure when searching for any attribute it has to be there in dictionary,if that is not there in dictionary it will take as string it will return wrong output
  testData = [ 
               (1,"a" , "{'a': [True, 'StructType()']}" , "StructType()")
              ,(2,"b" , "{'b': [False, 'StringType()']}" , "StringType()")
              ,(3,"c" , "{'c': [False, 'IntegerType()']}" , "IntegerType()")
             ]
  overallTestResult = 'success'
  allTestResults = []
  for testCase in testData:
    testNumber = 'Test-'+str(testCase[0])
    inputAttribute = testCase[1] #xml attribute
    xmlSchemaDic = eval(testCase[2]) #Input xml schema dictionary
    expectedOutput = testCase[3] #Expected output
    
    #Execute xml parser function
    outputResult = getType(inputAttribute,xmlSchemaDic)
    if outputResult == expectedOutput:
      testResult = 'success'
      executionOutputStatus = 'As Expected'

    else:
      testResult = 'failed' #initialise the test result
      executionOutputStatus = 'Mismatched'
      overallTestResult = 'failed'
      
    #testid, inputString, acurrentAttribute, expectedOutput, outputString and testResult
    testDataSet = (testNumber, inputAttribute, xmlSchemaDic, expectedOutput, outputResult, testResult)
    allTestResults.append(testDataSet)
  
  if overallTestResult == 'failed':
    if not os.path.exists(outputLocation):
      os.makedirs(outputLocation)
    #Write output to file in the ouptut location when ther is a failed scenario    
    file = open(outputLocation + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '_outputResultFile','w')
    file.write(str(allTestResults))
    file.close()
    
   #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),overallTestResult)

  logTaskProgress(cursor, batchTaskId, "Testing getType function completed")
    
except Exception as e:
    errorMessage = "Exception occured while testing getType  function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  

# COMMAND ----------

# DBTITLE 1,Test getAttributes function
#Tesing of getAttributes function
try:
  testScenarioReference = 'GetAttributes'
  testCaseScenario ='Get attirbutes from xml by using xml schema'
  requiredInputParameter = 'attribute,xmlDicSchema'
  outputLocation = testOutputPath + testScenarioReference + '/batch_task_id=' + str(batchTaskId) + '/'
  #Xml record and expected output    
  testData = [ 
               (1,"<a><b c='c1'/></a>"                  , "{}")
              ,(2,"<a><b c='c1'></b><b c='c2'></b></a>" , "{}")
              ,(3,"<b c='c1'/>"                         , "{'c': 'c1'}")
             ]
  overallTestResult = 'success'
  allTestResults = []
  for testCase in testData:
    tempDict = {}
    testNumber = 'Test-'+str(testCase[0])
    inputAttribute = testCase[1] #xml attribute
    expectedOutput = eval(testCase[2]) #Expected output
    rootXml = ET.fromstring(inputAttribute)
    
    #Execute xml parser function
    outputResult = getAttributes(tempDict,rootXml)
    
    if outputResult == expectedOutput:
      testResult = 'success'
      executionOutputStatus = 'As Expected'

    else:
      testResult = 'failed' #initialise the test result
      executionOutputStatus = 'Mismatched'
      overallTestResult = 'failed'
      
    #testid, inputString, acurrentAttribute, expectedOutput, outputString and testResult
    testDataSet = (testNumber, inputAttribute, expectedOutput, outputResult, testResult)
    allTestResults.append(testDataSet)
  
  if overallTestResult == 'failed':
    if not os.path.exists(outputLocation):
      os.makedirs(outputLocation)
    #Write output to file in the ouptut location when ther is a failed scenario    
    file = open(outputLocation + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '_outputResultFile','w')
    file.write(str(allTestResults))
    file.close()
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),overallTestResult)

  logTaskProgress(cursor, batchTaskId, "Testing GetAttributes function completed")
    
except Exception as e:
    errorMessage = "Exception occured while testing GetAttributes  function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  

# COMMAND ----------

# DBTITLE 1,Test typeCast function
try:
  #Parameters to run getDataString function
  testCaseScenario ='typeCast function'
  requiredInputParameter = 'xmlMessage,searchString,startTag,closeTag'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  #input parameters to typeCast
  myDictTest = [
                  ["Product_Name", "StringType", "Product_Name"]
                 ,[24335, "IntegerType", 24335]
                 ,["Category", "StringType", "Category"]
                 ,["Description", "StringType", "Description"]
                 ,[97.835, "FloatType", 97.835]
               ]

  for inputFile in myDictTest:
    item = inputFile[0]
    targetType = inputFile[1]
    CastItem = inputFile[2]
    #execute typeCast function
    typeCastItem = typeCast(item,targetType)
    if typeCastItem == CastItem:
      testResult = 'success'
      executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                           testCaseScenario,executionOutputStatus,outputLocation,
                           datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested typeCast function')
except Exception as e:
  errorMessage = "Exception occured while testing typeCast function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Test isFormatedMessage function
#Tesing of xml parser function
try:
  testScenarioReference = 'isFormatedMessage'
  testCaseScenario ='Verify message formated properly or not'
  requiredInputParameter = 'DestinationDF,targetTableLocation,targetObjectName,targetFormat,cursor,batchTaskId,errorLine,adfPipelineName,clusterName,notebookName,errorLogFileLocation' 
  outputLocation = testOutputPath + testScenarioReference + '/batch_task_id=' + str(batchTaskId) + '/'
  
  #full string, current attribute, expected output
  testData = [ 
              (1,"<a><b c='c1'></b><b c='c2'></b></a>", "{'a': [False, 'StructType()'], 'b': [True, 'StructType()'], 'c': [False, 'StringType()']}" 
               ,"True") ,
              (2,"<a><b c='c1'/></a>",  "{'a': [False, 'StructType()'], 'b': [False, 'StringType()'], 'c': [False, 'StringType()']}" 
               ,"True") ,
              (3,"<a><b Val='b1'/></a>", "{'a': [False, 'StructType()'], 'b': [False, 'StringType()']}" 
               ,"True")
             ]
  overallTestResult = 'success'
  allTestResults = []
  for testCase in testData:
  
    testNumber = 'Test-'+str(testCase[0]) 
    inputXmlMessage = testCase[1] #Input xml message
    xmlSchemaDic = eval(testCase[2]) #Input xml schema dictionary
    expectedOutput = eval(testCase[3]) #Expected output

    #Parameters to run parseXML function
    requiredInputParameter = 'xmlFile,xmlDicSchema'
    #Execute xml parser function
    outputDict = isFormatedMessage(inputXmlMessage)

    if outputDict == expectedOutput:
      testResult = 'success'
      executionOutputStatus = 'As Expected'
    else:
      testResult = 'failed' #initialise the test result
      executionOutputStatus = 'Mismatched'
      overallTestResult = 'failed'
      
    #testid, inputString, acurrentAttribute, expectedOutput, outputString and testResult
    testDataSet = (testNumber, inputXmlMessage, xmlSchemaDic, expectedOutput, outputDict, testResult)
    allTestResults.append(testDataSet)
  
  if overallTestResult == 'failed':
    if not os.path.exists(outputLocation):
      os.makedirs(outputLocation)
    #Write output to file in the ouptut location when ther is a failed scenario    
    file = open(outputLocation + datetime.now().strftime("%Y%m%d%H%M%S") + '_outputResultFile','w')
    file.write(str(allTestResults))
    file.close()
    
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,outputLocation,
                       datetime.now(),overallTestResult)

  logTaskProgress(cursor, batchTaskId, "Testing isFormatedMessage function completed")
    
except Exception as e:
    errorMessage = "Exception occured while testing isFormatedMessage function: " + str(e)
    logError(cursor,batchTaskId,errorLine,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  

# COMMAND ----------

# DBTITLE 1,Test nestedXMLParser function
try:
  #extract and construct xml message with all attributes from string message
  #Parameters to run getDataString function
  testCaseScenario ='nestedXMLParser function'
  requiredInputParameter = 'xmlMessage,searchString,startTag,closeTag'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #Input message read from file
  myDictTest = [
                [testInputPath + "xmlMessage1a.txt", "QuoteDetail", "Input"]
               ,[testInputPath + "xmlMessage1c.txt", "QuoteDetail", "Input"]
               ,[testInputPath + "xmlMessage2a.txt", "QuoteDetail", "Input"]
               ,[testInputPath + "xmlMessage3a.txt", "QuoteDetail", "Input"]
               ,[testInputPath + "xmlMessage4a.txt", "QuoteDetail", "Input"]
             ]

  for inputFile in myDictTest:
    fileLocation = inputFile[0]
    tranName = inputFile[1]
    polDataType = inputFile[2]
    #read input messages
    messageFromFile = open(fileLocation, 'r').read()
    #execute extractXML function
    xmlMessage = extractXML(messageFromFile)
    #get the root xml string from xml message
    rootXML = ET.fromstring(xmlMessage)
    #execute nestedXMLParser function
    dicXML = nestedXMLParser(rootXML)
    
    #get the poldata type
    dicXMLPolDataType = dicXML["OuterXML"]["PolMessage"]["PolData"]["Type"]
    #get the tran name
    dicXMLTranName = dicXML["OuterXML"]["PolMessage"]["Transaction"]["TranName"]
    #validate tran name and poldata type
    if tranName == dicXMLTranName and polDataType == dicXMLPolDataType:
      testResult = 'success'
      executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                           testCaseScenario,executionOutputStatus,outputLocation,
                           datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested nestedXMLParser function')
except Exception as e:
  errorMessage = "Exception occured while testing nestedXMLParser function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False
    

# COMMAND ----------

# DBTITLE 1,Test parseXML function
try:
  #extract and construct xml message with all attributes from string message
  #Parameters to run getDataString function
  testCaseScenario ='parseXML function'
  requiredInputParameter = 'xmlMessage,searchString,startTag,closeTag'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status 
  outputLocation = ''
  #Input message read from file
  myDictTest = [
                [testInputPath + "xmlMessage1a.txt", "QuoteDetail", "Input"]
               ,[testInputPath + "xmlMessage1c.txt", "QuoteDetail", "Input"]
               ,[testInputPath + "xmlMessage2a.txt", "QuoteDetail", "Input"]
               ,[testInputPath + "xmlMessage3a.txt", "QuoteDetail", "Input"]
               ,[testInputPath + "xmlMessage4a.txt", "QuoteDetail", "Input"]
             ]

  for inputFile in myDictTest:
    fileLocation = inputFile[0]
    tranName = inputFile[1]
    polDataType = inputFile[2]
    #read input messages
    messageFromFile = open(fileLocation, 'r').read()
    #execute extractXML function
    xmlMessage = extractXML(messageFromFile)
    #get the root xml string from xml message
    parsedXML = parseXML(xmlMessage)
    #get the poldata type
    parsedPolDataType = parsedXML["OuterXML"]["PolMessage"]["PolData"]["Type"]
    #get the tran name
    parsedTranName = parsedXML["OuterXML"]["PolMessage"]["Transaction"]["TranName"]
    #validate tran name and poldata type
    if tranName == parsedTranName and polDataType == parsedPolDataType:
      testResult = 'success'
      executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                             testCaseScenario,executionOutputStatus,outputLocation,
                             datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested parseXML function')
except Exception as e:
  errorMessage = "Exception occured while testing parseXML function: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False
    

# COMMAND ----------

# DBTITLE 1,Test findElementType function
try:
  #Parameters to run findElementType function
  testCaseScenario ='find element structtype'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status
  requiredInputParameter = 'element,pathList,metaDataDic,dataTypeDic,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  #input parameters to findElementType
  DataType = {"STRING":"StringType()","INT":"IntegerType()"}
  Path = 'PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs'
  MetaData = {'CalculatedResult_CompulsoryXs': ['PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs', 'STRING']}
  Element =  'PolMessage'
  expectedOutput = 'StringType()'
  #execute findElementType function
  elementType = findElementType( Element,Path,MetaData,DataType,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  if expectedOutput == elementType:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested findElementType function')
except Exception as e:
    errorMessage = "Exception occurred while testing findElementType function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False
    
  

# COMMAND ----------

# DBTITLE 1,Test findElementArrayStatus
try:
  #Parameters to run findElementArrayStatus function
  testCaseScenario ='find an element is an array or not'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status
  requiredInputParameter = 'element,pathList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  #input parameters to findElementArrayStatus
  pathList = ['PolMessage','PolData','[CalculatedSummaryResult]','CalculatedResult_CompulsoryXs']
  element =  'CalculatedSummaryResult'
  #excute findElementArrayStatus function
  arrayStatus = findElementArrayStatus(element,pathList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  # check array status
  if arrayStatus == True:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested findElementArrayStatus function')
except Exception as e:
    errorMessage = "Exception occurred while testing findElementArrayStatus function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False
  

# COMMAND ----------

# DBTITLE 1,Test generateElementDetailsDictironary function
try:
  #Parameters to run generateElementDetailsDictironary function
  testCaseScenario ='generate element details as dictionary'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status
  requiredInputParameter = 'element,pathList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  #input parameters to generateElementDetailsDictironary
  dataTypeDic = {"STRING":"StringType()","INT":"IntegerType()"}
  pathList = 'PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs'
  metaDataDic = {'CalculatedResult_CompulsoryXs': ['PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs', 'STRING']}
  elementList =  ['PolMessage','PolData']
  
  expectedOutput = {'PolMessage': [False, 'StringType()'], 'PolData': [False, 'StringType()'], 'OuterXML': [False, 'StructType()']}
  #execute generateElementDetailsDictironary function
  elementDetailsDic = generateElementDetailsDictironary(elementList,pathList,metaDataDic,dataTypeDic,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  #print(gendic)
  if expectedOutput == elementDetailsDic:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful tested generateElementDetailsDictironary function')
except Exception as e:
    errorMessage = "Exception occurred while testing generateElementDetailsDictironary function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test getInitialListsAndDictionaries function
try:
  #Parameters to run getInitialListsAndDictionaries function
  testCaseScenario ='getInitialListsAndDictionaries'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status
  requiredInputParameter = 'metadataDetailsDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  
  #input parameters to getInitialListsAndDictionaries
  srcfile = 'initialListAndDictionary.csv'
  inputpath = tableInputPath + srcfile
  metadataDetailsDF = spark.read.csv(inputpath, header = True)
  
  #expected output
  testData = [(['PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs'],['PolMessage', 'PolData', 'CalculatedResult', 'CalculatedResult_CompulsoryXs'],{'PolMessage': ['{STRING:StringType(),INT:IntegerType()}', 'PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs', '{CalculatedResult_CompulsoryXs: [PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs, STRING]}']},{'STRING': 'StringType()'},{'PolMessage': [False, 'StructType()'], 'PolData': [False, 'StructType()'], 'CalculatedResult': [False, 'StructType()'], 'CalculatedResult_CompulsoryXs': [False, 'StringType()'], 'OuterXML': [False, 'StructType()']})]
  
  for expecteOutput in testData:
    inputPathList = expecteOutput[0]
    inputElementList = expecteOutput[1]
    inputMetaDataDic = expecteOutput[2]
    inputDataTypeDic = expecteOutput[3]
    inputXmlSchemaDic = expecteOutput[4]
    
    #execute getInitialListsAndDictionaries
    pathList, elementList, metaDataDic, dataTypeDic, xmlSchemaDic = getInitialListsAndDictionaries(metadataDetailsDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
    #check actual output and expected output is matched or not
    if inputPathList == pathList and inputElementList == elementList and inputMetaDataDic == metaDataDic and inputDataTypeDic == dataTypeDic and inputXmlSchemaDic == xmlSchemaDic:
      
      testResult = 'success'
      executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested getInitialListsAndDictionaries function')
except Exception as e:
    errorMessage = "Exception occurred while testing getInitialListsAndDictionaries function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test getNoElementsAndMetadataDict function
try:
  #Parameters to run getInitialListsAndDictionaries function
  testCaseScenario ='getNoElementsAndMetadataDict'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status
  requiredInputParameter = 'metadataDetailsDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'

  #input parameters to getNoElementsAndMetadataDict
  schema = StructType([StructField("Path",StringType(),True)])
  metadataDetailsDF = convertPandasToSparkDfWithSchema([["PolMessage.PolData.[CalculatedResult].CalculatedResult_CompulsoryXs"]],schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

  expectedOutput = {(0, 'PolMessage'): {'Parent': ['OuterXML']}, (1, 'PolData'): {'Parent': ['PolMessage']}, (2, 'CalculatedResult'): {'Parent': ['PolData']}, (3, 'CalculatedResult_CompulsoryXs'): {'Parent': ['CalculatedResult']}}

  numberOfElements, splitMedataDic = getNoElementsAndMetadataDict(metadataDetailsDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

  if numberOfElements == 3 and expectedOutput == splitMedataDic:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
    #Log test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested getNoElementsAndMetadataDict function')
except Exception as e:
    errorMessage = "Exception occurred while testing getNoElementsAndMetadataDict function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False
  

# COMMAND ----------

# DBTITLE 1,Test getXMLSchemaStruct function
try:
  #Parameters to run getXMLSchemaStruct function
  testCaseScenario ='getXMLSchemaStruct'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status
  requiredInputParameter = 'metadataDetailsDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation'

  #input parameters to getXMLSchemaStruct
  splitMedataDic = {(0, 'PolMessage'): {'Parent': ['OuterXML']}, (1, 'PolData'): {'Parent': ['PolMessage']}, (2, 'CalculatedResult'): {'Parent': ['PolData']}, (3, 'CalculatedResult_CompulsoryXs'): {'Parent': ['CalculatedResult']}}
  xmlSchemaStruct = 'String'
  numberOfElements = 3
  pathList = ['PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs']
  metaDataDic = {'PolMessage': ['{STRING:StringType(),INT:IntegerType()}', 'PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs', '{CalculatedResult_CompulsoryXs: [PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs, STRING]}']}
  dataTypeDic = {'STRING': 'StringType()'}
  xmlSchemaStructDic = {'PolMessage': [False, 'StructType()'], 'PolData': [False, 'StructType()'], 'CalculatedResult': [False, 'StructType()'], 'CalculatedResult_CompulsoryXs': [False, 'StringType()'], 'OuterXML': [False, 'StructType()']}
  #expected output
  expectedOutput = 'StructType(List(StructField(OuterXML,StructType(List(StructField(PolMessage,StructType(List(StructField(PolData,StructType(List(StructField(CalculatedResult,StructType(List(StructField(CalculatedResult_CompulsoryXs,StringType,true))),true))),true))),true))),true)))'

  #execute getXMLSchemaStruct function
  xmlSchemaStruct = getXMLSchemaStruct(splitMedataDic,xmlSchemaStruct,numberOfElements,pathList,metaDataDic,dataTypeDic,xmlSchemaStructDic,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  #validate xmlSchemaStruct
  if expectedOutput == str(xmlSchemaStruct):
    testResult = 'success'
    executionOutputStatus = 'As Expected'
    #Log test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested getXMLSchemaStruct function')
except Exception as e:
    errorMessage = "Exception occurred while testing getXMLSchemaStruct function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Test createReferenceDataFrames function
try:
#Parameters to run createReferenceDataFrames function
  testCaseScenario ='createReferenceDataFrames function'
  testResult = 'failed' #initialise the test result
  executionOutputStatus = 'Mismatched'#initialise the execution_output_status
  requiredInputParameter = 'cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation'
  #input parameters to createReferenceDataFrames
  batchTaskId = batchTaskId
  errorMessage = ''
  adfPipelineName = ''
  clusterId = ''
  notebookName = 'unit_testing_unstructuredxmlSource_functions'
  errorLogFileLocation = errorLogFileLocation
  #execute createReferenceDataFrames function
  unstructuredxmlSourceSchemRefDF,unstructuredxmlSourceIntermediayDF = createReferenceDataFrames(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  if unstructuredxmlSourceSchemRefDF.count() > 0 and unstructuredxmlSourceIntermediayDF.count() > 0:
    testResult = 'success'
    executionOutputStatus = 'As Expected'
  #Log overall test status into unit test table 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,outputLocation,datetime.now(),testResult)
  logTaskProgress(cursor,batchTaskId,'Successful Tested createReferenceDataFrames function')
except Exception as e:
    errorMessage = "Exception occurred while testing createReferenceDataFrames function: " + str(e)
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