# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>jsxm_unstructuredxmlSource_gen2_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data from unstructuredxmlSource Raw to Raw Delta</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2020/02/03</td></tr>
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
# MAGIC     <td>batch_task_id</td>
# MAGIC     <td>batch_task_id is used to retrieve object details</td>
# MAGIC     <td>@batch_task_id = 4</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batch_id</td>
# MAGIC     <td>batch_id is used to retrieve batch details</td>
# MAGIC     <td>@batch_id = 1</td>
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
# MAGIC     <td> 2019/01/01 </td>
# MAGIC     <td> Framework </td>
# MAGIC     <td> Added Specific unstructuredxmlSource source select Function 
# MAGIC          <br>Moved retrieve column list tuple to seperate standard function
# MAGIC          <br>Dev rework for functions and varibale names cleanup
# MAGIC          <br>Added Function call to register functions
# MAGIC          <br>Updated select source from Destination to use it from the standard functions instead of unstructuredxmlSource sepcific as part of message quality udf changes
# MAGIC          <br>Fixed indentation for logTaskProgress after mark dates loaded
# MAGIC          <br>Added source object id to mark dates loaded
# MAGIC          <br>Added import re
# MAGIC          <br>Updated write to target location to use writeToTargetTableWithAppend instead of location
# MAGIC          <br>Added call to getBatchLevelObjectRowCount to get rows loaded into target
# MAGIC          <br>import json
# MAGIC          <br>Changed call for logProgress to with retry after write to delta table. Also passed additional parameters to mark dates loaded to enable retry</td>
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

# DBTITLE 1,Run log function notebook
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run standard function notebook
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run unstructuredxmlSource specific functions
# MAGIC %run ../../util/spe/unstructuredxmlSource_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  import json
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,BooleanType
  from datetime import datetime, timedelta
#   import pyspark.sql.functions as F
  from pyspark.sql.functions import lit,col,collect_list
  from pyspark import SparkContext,SparkConf
  from functools import reduce
  from collections import defaultdict
  import xml.etree.ElementTree as ET
  import re
except Exception as e:
  error_message="Exception occured while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise variables
try: 
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
  
except Exception as e:
  error_message = "Exception occured while variable initialisation :" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables and initialise error log location
try:
  #GET batchTaskId FROM WIDGETS   
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")

  #GET batchId FROM WIDGETS
  dbutils.widgets.text("batchId","")
  batchId = dbutils.widgets.get("batchId")
  
  #GET sourceId  FROM WIDGETS   
  dbutils.widgets.text("sourceId","")
  sourceId  = dbutils.widgets.get("sourceId")

  #GET adfPipelineName  FROM WIDGETS
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName  = dbutils.widgets.get("adfPipelineName")
  
  #GET notebookName  FROM WIDGETS   
  dbutils.widgets.text("notebookName","")
  notebookName  = dbutils.widgets.get("notebookName")

  #GET clusterId  FROM WIDGETS
  dbutils.widgets.text("clusterId","")
  clusterId  = dbutils.widgets.get("clusterId")
  
  #assigned the source and batch to other variables that are referenced from the metadata
  SourceID=sourceId
  CreatedBatchID=batchId
  LastUpdatedBatchID=batchId
  
  #call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')  
except Exception as e:
  errorMessage = "Exception occured while getting parameters and initiliasing error log location: " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn=dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")

#call function sql_db_conn to establish Database connection with given scope and key values
try:
  conn,cursor = sqlDbConn(dbconn
                          ,batchTaskId
                          ,adfPipelineName
                          ,clusterId
                          ,notebookName
                          ,errorLogFileLocation)
except Exception as e:
  errorMessage="unable to establish DB connection: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Get metadata - Execute high and low level stored procedures
#call sp_exec_high_and_low_level function to retrieve values from store procedure and store in pandas dataframe                                         
try:        
  sourceDestinationTableDetails,sourceDestinationFieldsDetails = spExecHighAndLowLevel(conn
                                                                                       ,cursor
                                                                                       ,batchTaskId
                                                                                       ,adfPipelineName
                                                                                       ,clusterId
                                                                                       ,notebookName
                                                                                       ,errorLogFileLocation)
  #Replace the nulls with 'none' as this sp always returns single record no need to specify schema
  sourceDestinationTableDetails=sourceDestinationTableDetails.fillna('none')
 # sourceDestinationFieldsDetails =sourceDestinationFieldsDetails.fillna('-1')
except Exception as e:
  errorMessage="Exception occured while execution of object high and low level stored procedures: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Specify schema For High Level and Low Level store proc by calling funciton
try:
  #Schema for High level stor proc as pandas stores different null values for different types having an issue while conveting to spark df for infering types for null columns.
  sourceDestinationFieldsDetailsSchema = getStructForHighAndLowLevelDf()
except Exception as e:
  errorMessage="Unable to get the schema for source and destination Details: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Transform metadata into spark dataframe

#call Transform_pd_df_to_Pyspark_df function to convert pandas to pyspark dataframe and store return values in variables
try:
  srcAndDesTableDF,srcAndDesFieldsDF = convertPandasToSparkDf(sourceDestinationTableDetails
                                                              ,sourceDestinationFieldsDetails
                                                              ,sourceDestinationFieldsDetailsSchema
                                                              ,cursor
                                                              ,batchTaskId
                                                              ,adfPipelineName
                                                              ,clusterId
                                                              ,notebookName
                                                              ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"source and destination table and fields dataframes created")
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
  #the id of the source object taken from teh procedure output
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
  

  
  logTaskProgress(cursor,batchTaskId,"Got dataframe values into variables")
except Exception as e:
  errorMessage="Unable to retrieve the dataframe values as variables: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Get Column List Details
#call Get_Select_And_With_Column_Details to get Columnlist tuple
try:
  columnList = getSelectAndWithColumnDetails(srcAndDesFieldsDF
                                             ,cursor
                                             ,batchTaskId
                                             ,adfPipelineName
                                             ,clusterId
                                             ,notebookName
                                             ,errorLogFileLocation)
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
  #Prepare select expression
  selectExpression=[item[1] for item in columnListFinal]
  logTaskProgress(cursor,batchTaskId,"Wiht Expression and select expressions are created")
except Exception as e:
  errorMessage="Unable to retrieve with and selectedExpressions: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Register unstructuredxmlSource functions
try:
  #Call the function to register unstructuredxmlSource functions 
  getunstructuredxmlSourceSchemaAndRegisterFunction()
  logTaskProgress(cursor,batchTaskId,"unstructuredxmlSourceshcema and udf are registered")
except Exception as e:
  errorMessage="Unable to retrieve with and selectedExpressions: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Selection of destination columns from source
# call select_dest_from_src function to select destination column from the source and store return values in a variable.
try:
  #remove the opening character from the string
  colXmlStartString = xmlStartString[1:]
  
  #The messages which are badly formated need to be stored in error location, We filter the messages based on below conditon.
#   unFormatedFilterCondition=compile("col('{}.MessageType').isin ('String Parser Exception','Not Well Formed XML Exception')".format(colXmlStartString),"",'eval')
  destinationDf = selectDestFromSrc(sourceFormat
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
  logTaskProgress(cursor,batchTaskId,"selectDestFromSrc function executed")

except Exception as e:
  errorMessage="Exception occured while reading destination columns from the source: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
  assert False

# COMMAND ----------

# DBTITLE 1,write to target location
#call writeToTargetTableWithAppend function to write dataframe into destination table with append
try: 
  writeToTargetTableWithAppend(destinationDf
                              ,targetObjectName
                              ,targetFormat
                              ,cursor
                              ,batchTaskId
                              ,adfPipelineName
                              ,clusterId
                              ,notebookName
                              ,errorLogFileLocation) 
  conn, cursor = logTaskProgressWithRetry(dbconn,conn,cursor,batchTaskId,"writeToTargetTableWithAppend function executed")
except Exception as e:
  errorMessage="Exception occured while loading into target Table with appen: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Capture rows loaded into target object
#this will only work when this notebook is executed from ADF or as part of a databricks job. Otherwise a row count of -1 will be returned
batchTaskRowsLoaded = getBatchLevelObjectRowCount(targetObjectName)

# COMMAND ----------

# DBTITLE 1,Mark the dates as loaded into the destination
try:
  conn, cursor = markDatesLoaded(targetObjectId
                                ,sourceObjectId
                                ,currentTs
                                ,currentTs
                                ,dbconn
                                ,conn                  
                                ,cursor
                                ,batchTaskId
                                ,adfPipelineName
                                ,clusterId
                                ,notebookName
                                ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"markesDatesLoaded function executed")
except Exception as e:
  errorMessage="Exception occured while marking the dates as loaded: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,complete task and close connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
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