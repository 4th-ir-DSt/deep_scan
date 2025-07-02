# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>generic_notebook_function</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Standard Function for all notebook </td></tr>
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
# MAGIC     <td>Added function lark_dates_loaded.
# MAGIC          <br>Modified write_to_target and add_default_column function.
# MAGIC          <br>Added functions for preparing selects for columns and withExpressions: getSelectPath,stripLastAttributeFromColumnSelect,getWithColumnExpression,getFinalXmlAttribute,calculateExplodeWithExpressions,getCompiledWithExpressionForUdf,performStringreplaceCastingAndNaming, getWithExpressionsAndSelectList.
# MAGIC          <br>Added the pii hash functions.
# MAGIC          <br>updated source and destination function and removed default attributes function.
# MAGIC          <br>Corrected parameters for all functions. Also added new helper functions for getting the struct definition.
# MAGIC          <br>Added Mapping Expression and Generate dictionary functions and cleanup variables.
# MAGIC          <br>Cleared the variable and function names for devrework and removed errorLine from all the notebook.
# MAGIC          <br>Updated variable names to mappingRefDic, targetObjectName.
# MAGIC          <br>Updated write to target function to remove object name from path.
# MAGIC          <br>Modified functions getCompiledWithExpressionForUdf, performStringreplaceCastingAndNaming, getWithExpressionsAndSelectList to allow for udf type hash.
# MAGIC          <br>Updated selectSourceFromDesc to use pre and post select for withcoumns.
# MAGIC          <br>Modified function performStringreplaceCastingAndNaming for casting columns. Types BINARY, DATE, STRING, TIMESTAMP will not be cast explicitly. Additionally filtered hash types from teh column list as they are only calculated after the select.
# MAGIC          <br>Added source object id to mark dates loaded and removed variable type from casting, also initial with extression list is only for preSelect.
# MAGIC          <br>Updated default value for generateDict function to Unknown instead of Null.
# MAGIC          <br>added writeToTargetWithOverwrite in addition to weiteToTarget that is currently append only<br>added selectDestFromSrcTable that selects from a source table rather than a location<br>corrected name of generateMappngExpression to generateMappingExpression<br>Added function identifyIfNewDataInDependentTables to identify if there are changes in dependent tables after the destination table has been loaded<br>Added function createOrReplaceTemporaryView to create or replace a temporary view<br>created additional function writeToTargetTableWithOverwrite.
# MAGIC          <br>updated performStringreplaceCastingAndNaming for join type attributes <br>added selectDestFromJoinSrc function to get destination dataframe from joined source dataframe.
# MAGIC          <br>updated performStringreplaceCastingAndNaming for unpivot and ignore computed type exclusion in getWithExpressionsAndSelectList.
# MAGIC          <br>updated performStringreplaceCastingAndNaming for join computed type attirbute names.
# MAGIC          <br>Added function cacheDatabricksTable.
# MAGIC          <br>>updated write target table with overwrite function to include format.
# MAGIC          <br>updated performStringreplaceCastingAndNaming to exclude hash type attributes.
# MAGIC          <br>Added function createOrReplaceView.
# MAGIC          <br>Added functions getNotebookJobId and getBatchLevelObjectRowCount.
# MAGIC          <br>Added funciton writeToTargetTableWithAppend.
# MAGIC          <br>Modified concatenateFunc to coalesce the column value with '' so that a value is always returned.
# MAGIC          <br>changed markDatesLoaded to attempt retries with exponential back off. Also removed unecessary logging from writeTo functions as it is also being logged in calling notebooks right after the writeTo function.
# MAGIC          <br>Added new Pandas DF to Spark DF function to allow upgrade of pandas / spark 3 cluster runtimes.
# MAGIC          <br>Updated the variable to camel case in convertPandasToSparkDfWithSchema function.
# MAGIC          <br>Added import reduce function.
# MAGIC          <br>Added getObjectReferenceDetails function.
# MAGIC          <br>Added function removeDuplicateColumnsFromString.
# MAGIC          <br>Added function dataframeJsonWrapper.
# MAGIC          <br>Updated performStringreplaceCastingAndNaming function for the string columns which doesnot match with source and destination types.
# MAGIC          <br>>Removed commented code from performStringreplaceCastingAndNaming function </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2021/11/09</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Added Compute Hash Value functions for bigint and decimal 28.
# MAGIC          <br>Modified object to object lowlevel function struct to include primary_key_order and  track_type_2_changes fields.
# MAGIC          <br>Added apply scdType2 Function
# MAGIC        </td>
# MAGIC   </tr>
# MAGIC    <tr>
# MAGIC     <td>2021/11/17</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Added computeObscure Udf 
# MAGIC        </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2021/12/03</td>
# MAGIC     <td>Akhilesh Kothari</td>
# MAGIC     <td>Added a new function readExcelFileIntoSparkDf to read excel file into spark dataframe</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2021/12/08</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>Added function sqlDWConn for connection to synapse sql datawarehouse
# MAGIC     <br>Added function getAllViewDefinitions function gets all conformed view defnition from metadata database
# MAGIC     <br>Added function createAlterViews 
# MAGIC     <br>Added function getSourceToDestinationHighLevelDetails to only get high level details, existing function brings both high and low level details
# MAGIC     <br>Added executeSynapseSQL function to execute synapse SQL commands
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2021/12/08</td>
# MAGIC     <td>Akhilesh Kothari</td>
# MAGIC     <td>Added 2 new functions - populateObjectViewDateAvailability to populate data by executing sp, getSourceObjectName to get object name by query</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2021/12/14</td>
# MAGIC     <td>Akhilesh Kothari</td>
# MAGIC     <td>Replace function getSourceObjectName with new function getViewDefinition, the new function returns view name and definition in a spark dataframe</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/01/19</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>Added function populateConformedMasterViewDateAvailablity</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/01/06</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Added applyScdType2onDestForIncrLoad Function</td>
# MAGIC   </tr>
# MAGIC     <tr>
# MAGIC     <td>2022/01/10</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Updated cleanPiiHashLiteralAttributes Function for finding the lastitem in the list for piihash traceabliliy function</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/01/28</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Updated concatenateFuct to hanlde with column data types(Eg:lit(2) in concatenatelist)</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/03/01</td>
# MAGIC     <td>Akhilesh Kothari</td>
# MAGIC     <td>Adding new function spExecGetPKsType2Fields, new function gets primary keys and type 2 fields from object definition table
# MAGIC     <br>Adding new function getStructForPKsType2Df, new function to return struct for spark dataframe
# MAGIC     <br>Adding new function addAuditAndHBKColumnsOnDF, new function to add audit and HBK columns</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/03/31</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Updated type2 scd function to include hashpartition key in the join and added batchid and currentTs parameters
# MAGIC     <br>Bug fix for hash tinyint function
# MAGIC     <br>Update identify data to load function to have a check in place if the truncate table happend after the data loaded
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC     <tr>
# MAGIC     <td>2022/03/31</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Bug Fix for incrimental scdtype2 function and included batchId 
# MAGIC </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/04/06</td>
# MAGIC     <td>Akhilesh</td>
# MAGIC     <td>Added a new function applySCDType2OnDestForBatchLoad for new batch effective date design
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/04/28</td>
# MAGIC     <td>Akhilesh</td>
# MAGIC     <td>Added a new function applySCDType2OnDestWithDelete for scd type2, which handle deletes
# MAGIC     <br>Altered function applySCDType2OnDestForBatchLoad to handle deletes from standardised
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/05/25</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>Applied fix to applySCDType2OnDestForBatchLoad function
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/06/13</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>Added populateObjectDateAvailabilityWithDataCheck
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/06/22</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>Removed parition key from join as it is not used for partition for any Phase A conformed (partition_scheme) and not available in all tables, added computeHashValueUdfRegistration& calculatelakeDeletedTimestampUdfRegistration
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/09/14</td>
# MAGIC     <td>Manmohan</td>
# MAGIC     <td>Added progress logging step for SCD2 functions</td>
# MAGIC   </tr>   
# MAGIC     <tr>
# MAGIC     <td>2022/10/31</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Added checkDirectory and selectDestFromSrcWithDelimiter Functions,applySCDType2onDestForIncrSourceLoad</td>
# MAGIC   </tr> 
# MAGIC   <tr>
# MAGIC     <td>18/11/2022</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Added function usp_log_file_object_availability_dates</td>
# MAGIC   </tr> 
# MAGIC   <tr>
# MAGIC     <td>18/01/2023</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Added Get Previous Bed Filter and createStandardisedFilterView functions to handle the updates from standardiset to go as deletes in conformed</td>
# MAGIC   </tr> 
# MAGIC     <tr>
# MAGIC     <td>06/02/2023</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Updated  createStandardisedFilterView function with new design and added createStandardisedGrainView</td>
# MAGIC   </tr> 
# MAGIC   <tr>
# MAGIC     <td>29/03/2023</td>
# MAGIC     <td>Manmohan S</td>
# MAGIC     <td>Updated function applySCDType2onDestForBatchLoad's Merge command to filter for Live records from Target bug fix 51744</td>
# MAGIC   </tr>  
# MAGIC   <tr>
# MAGIC     <td>17/04/2023</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>added applyScdType2onDestWithDelete_conf for full data sets in conformed</td>
# MAGIC   </tr> 
# MAGIC    
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
#  import hashlib
  from pyspark.sql.types import LongType,StringType,IntegerType,BooleanType,ByteType,TimestampType
  from  hashlib import sha256
  import time
  from functools import reduce
  import pandas
  import openpyxl
  from pyspark.sql.functions import *
except Exception as e:
  errorMessage="Exception occured while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Function To Establish SQL Database connection
# Function to Establish SQL Database connection 
#dbconn will contain the scope and the key 
def sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):  
  try:
    conn = pyodbc.connect(dbconn, autocommit = True)
    cursor = conn.cursor()
    logTaskProgress(cursor, batchTaskId, "Established Database connection in the notebook")
    return conn,cursor
  except Exception as e:
    errorMessage="unable to establish DB connection: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False
  

# COMMAND ----------

# DBTITLE 1,Function to convert single source pandas to spark dataframe
#Function definition to convert pandas dataframe to spark
def convertSinglePandasToSparkDf(sourcePandasDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation): 
  try:
    #creating a spark dataframe out of pandas dataframe                                            
    sparkDataframe=spark.createDataFrame(sourcePandasDF)    
    logTaskProgress(cursor,batchTaskId,"Converted pandas dataframe to spark dataframe")
    return sparkDataframe
  except Exception as e:
    errorMessage="Unable to convert pandas to spark dataframe: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function To convert Source Pandas to Spark dataframe
#Function definition to convert pandas dataframe to spark
def convertPandasToSparkDf(sourceDestinationTableDetails,sourceDestinationFieldsDetails,sourceDestinationFieldsDetailsSchema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation): 
  try:
    #creating a spark dataframe out of pandas dataframe                                            
    srcAndDesTableDF=spark.createDataFrame(sourceDestinationTableDetails)
    srcAndDesFieldsDF=convertPandasToSparkDfWithSchema(sourceDestinationFieldsDetails,sourceDestinationFieldsDetailsSchema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)   
    logTaskProgress(cursor,batchTaskId,"Converted pandas dataframe to spark dataframe")
    return srcAndDesTableDF,srcAndDesFieldsDF
  except Exception as e:
    errorMessage="Unable to convert pandas to spark dataframe: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False
  

# COMMAND ----------

# DBTITLE 1,Function to write to target location
#Function to write a dataframe into a given loction and given format
def writeToTarget(sourceDF,targetTableLocation,targetObjectName,targetFormat,
                    cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try: 
    # Loads into target table 
    sourceDF.write.format(targetFormat)\
                       .mode("append") \
                       .save(targetTableLocation)

  except Exception as e:
    errorMessage="Exception occured while loading into target location: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to write to target location with overwrite
#Function to write a dataframe into a given loction and given location
def writeToTargetWithOverwrite(sourceDF,targetTableLocation,targetObjectName,targetFormat
                               ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try: 
    # Loads into target location with overwrite
    (sourceDF
     .write
     .format(targetFormat)
     .mode("overwrite")
     .save(targetTableLocation)
    )

  except Exception as e:
    errorMessage="Exception occured while loading into target location: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to write Target Table With Overwrite
#Function to write a dataframe into a table with overwrite
def writeToTargetTableWithOverwrite(sourceDF,targetObjectName,targetFormat
                                    ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try: 
    # Loads into target table with overwrite
    (sourceDF
     .write
     .mode("overwrite")
     .format(targetFormat)
     .saveAsTable(targetObjectName)
    )
    
  except Exception as e:
    errorMessage="Exception occured while loading into target table with overwrite: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to write Target Table With Append
#Function to write a dataframe into a table with overwrite
def writeToTargetTableWithAppend(sourceDF,targetObjectName,targetFormat
                                    ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try: 
    # Loads into target table with append
    (sourceDF
     .write
     .mode("append")
     .format(targetFormat)
     .saveAsTable(targetObjectName)
    )

  except Exception as e:
    errorMessage="Exception occured while loading into target tabel with append: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to close database connection and update completion of task
#Close the Database connection and updates batch task status as completed in batch_task_table and close Database connection
def taskEndAndCloseConn(cursor
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
                        ,errorLogFileLocation):
  try: 
    # function call to log task end to mark the notebook as complete
    logTaskEnd(cursor,batchTaskId, 'completed', batchTaskSourceRows, batchTaskRowsLoaded, batchTaskRejectRows, batchTaskResult,batchTaskResultLocation)    
    #close the connection to the database
    conn.close()
  except Exception as e:
    errorMessage="Exception occured completing the task : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to mark dates as loaded
def markDatesLoaded(destinationObjectId, sourceObjectId, datetimeFrom, datetimeTo, dbconn, conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  try:
    #initialise variables for retry
    retryCount = 0
    maxRetries = 3
    backoffWaitTime = 10
    backoffFactor = 2 #we will multiply the value of backoffWaitTime by this value to wait a longer period of time
    haveDbConnection = True #initialise to True assuming that we have a connection
    getStructForHighAndLowLevelDf
    while ( retryCount <= maxRetries ):
      try:
        if (retryCount > 0):
          time.sleep(backoffWaitTime) #wait for some seconds
          #increment the back off time for next time
          backoffWaitTime = backoffWaitTime * backoffFactor

        #if there isn't a db connection, then we need to recreate it
        if haveDbConnection == False:
          #call function sqlDbConn to establish Database connection with given scope and key values
          conn,cursor = sqlDbConn(dbconn, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
          haveDbConnection = True

        # function call to mark dates loaded to complete the load from source object for the dates that have been loaded into the destination
        logMarkDatesLoaded(cursor, batchTaskId, destinationObjectId, sourceObjectId, datetimeFrom, datetimeTo)   
        #we can return from here as we now have completed the call to logMarkDatesLoaded
        return conn,cursor
      
      except Exception as e:
        haveDbConnection = False
        if ( retryCount == maxRetries ):
          #if it still hasn't succeded at this point and we have reached maximum retries then we just throw the exception
          raise e
        retryCount = retryCount + 1 #increment the retries  
  
  except Exception as e:
    errorMessage="Exception occured while marking the dates as loaded : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function Columns: get select path
def getSelectPath(fullpath,current,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  #this function takes in an xml path with encoded [] attributes that identify an array type.
  #the function will return the path for the attribute that is based on the point from teh start of the string or the previous array that has been exploded to the current position
  try:
    #we are trying to get the explode to that point in the path.. i.e. in this case we need to return pm.x
    #find teh current position of the target attribute in the string  
    currentPosition = fullpath.index(current)

    #check to see if the string previous to the current attribute contains a ]
    #if true then we will need to go from that position
    if '[' in fullpath[0:currentPosition]:
      #find the position in the string of the previous [
      previousSbPosition = fullpath[0:currentPosition-1].rfind('[')
      #now return the string from teh previous [ position to the current position, then add on the current attribute, finally remove all the square brackets
      string = (fullpath[previousSbPosition:currentPosition] + current).replace('[', '').replace(']','')
    else:
      #since there are no previous [ in the string, just go from teh start to the current position, then add on the current attribute, and remove the square brackets
      string = (fullpath[0:currentPosition] + current).replace('[', '').replace(']','')
    return string
  except Exception as e:
    errorMessage="Exception occured while executing getSelectPath : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function Columns: strip last attribute from column select
def stripLastAttributeFromColumnSelect(columnsString,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  #This function takes in a string and returns only the last attribute of the string.
  #It is intended to be used to retrun the attribute from the xml
  #Example pass in '[x].y.[p].z' then it will return 'z'
  try:
    if ']' in columnsString:
      return columnsString[0:columnsString.rfind(']')+1]
  except Exception as e:
    errorMessage="Exception occured while executing stripLastAttributeFromColumnSelect : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function Columns: get with column expression
def getWithColumnExpression(strippedSelectItem,column,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  #this function is intended to generate a withColumn expression to explode a particular column in an xml path
  #example if the select string is '[x].[y]' and the current position is '[x]' then it will returned the compiled version of teh explode statement 'compile('explode(col("x"))',"",'eval')'
  #and also the uncompiled version so it is easier to read(debug)
  try:
    if '[' in column:
      #if there is an array in p then we need to explode it
      print("array evaluation")
      returnString = getSelectPath(strippedSelectItem,column,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      cleanColumn = column.replace('[', '').replace(']','')
      explodeExpression = 'explode(col("{1}"))'.format(cleanColumn, returnString)
      #add the two columns: compiled and uncompiled to a tupe and return
      withColExpressionTuple = (compile(explodeExpression,"",'eval'), explodeExpression)
      #return the compiled and uncompiled expression
      return withColExpressionTuple
  except Exception as e:
    errorMessage="Exception occured while executing getWithColumnExpression : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function Columns: get final xml attribute
def getFinalXmlAttribute(columnSelect,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  #this function receives an xml path reference to an attribute and returns the final attribute
  #e.g. takes in 'Hello.[p].[x].y' and returns 'y'
  try:
    if '.' in columnSelect:
      lastAttribute = columnSelect[columnSelect.rfind('.') + 1 : len(columnSelect)]
    else:
      lastAttribute = columnSelect
    return lastAttribute
  except Exception as e:
    errorMessage="Exception occured while executing getFinalXmlAttribute : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function Columns: calculate explode with expressions
def calculateExplodeWithExpressions(columnSelectList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  #This function is designed to take the full list of select statements, and for those that contain an array in the path then it will return the contents the new column name to be created, along with the with expressions to explode the column
  #examples
  #  if you pass in [('pm.[x].[y].m')
  #                 ,('pm.[x].[y].r.p')
  #                 ,('pm.[x].f')
  #                 ,('pm.p')]
  #  it will return explodes for just x and y
  #    [(1, 'x', 'explode(col("pm.x"))')
  #    ,(2, 'y', 'explode(col("x.y"))')]
  
  #first remove the last element as long as it doesn't have ]
  #substring from 0 to rfind ]

  try:
    #initialise empty lists
    strippedSelectList = []
    withColumnList = []

    #check there are actual items in the select list. i.e. is there work to do..
    if len(columnSelectList) >= 0:
      for columnsString in columnSelectList:
        #check if there is work to do on an array type
        if '[' in columnsString:
          #make the call to teh function to remove teh last attributes that aren't an array type i.e. []
          stripped = stripLastAttributeFromColumnSelect(columnsString,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
          if stripped is not None:
            #add the item to the list if it isn't there already. i.e. don't create any duplicates
            strippedSelectList.append(stripped) if stripped not in strippedSelectList else strippedSelectList

      #sort the list so that we have items in teh correct order.
      strippedSelectList.sort()
      #at this point the list should have the trailing attributes removed and should only contain selects for where there is an array

      #for each string in the list we need to explode each of the [] items and add the explode to the output list..
      for strippedSelectItem in strippedSelectList:
        #for each of the selects we first need to split on the . to get the columns into a new list
        strippedSelectItemList = strippedSelectItem.split('.')
        #for each item (node/column) in the list we then have to translate it into a withcolumn with explode and add it to a list if it doesn't already exist
        for column in strippedSelectItemList:
          #pass the entire select string and the node/column that is currently being looked at into the function
          withColExpression = getWithColumnExpression(strippedSelectItem, column,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
          #check to see if the expression contains anything and if it does, try and add it to the list if it doesn't exist
          if withColExpression is not None:
            newColWithExpression = (column.replace('[','').replace(']',''), withColExpression[0], withColExpression[1])
            withColumnList.append(newColWithExpression) if newColWithExpression not in withColumnList else withColumnList

    return withColumnList
  except Exception as e:
    errorMessage="Exception occured while executing calculateExplodeWithExpressions : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function Columns: get compiled with expression for udf
def getCompiledWithExpressionForUdf(columnList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  #this receives a list contains tuples of column_name and a udf expression
  #[('a','parseXML(col("¦Hello.[x].y¬"))')]
  #it will then return a list with the coumn_name, a compiled reference to the code and then the plain text version of the udf 'compile('parseXML(col("¦Hello.[x].y¬"))',"",'eval'))'
  try:
    withExpressionList = []
    #print(columnList)
    for column in columnList:
      #calculate if the udf should be executed pre of post the select statement
      relativeToSelectPostition = 'unknown'
      if column[2] == 'udf':
        relativeToSelectPostition = 'preSelect'
      elif column[2] == 'hash':
        relativeToSelectPostition = 'postSelect'
          
      #we know that the column type is udf, so we have to return the new column name and then the compiled expression for the contents of the udf call
      columnWithExpression = (column[0], compile(column[1],"",'eval'), column[1], relativeToSelectPostition)
      withExpressionList.append(columnWithExpression) if columnWithExpression not in withExpressionList else withExpressionList

    return withExpressionList  
  except Exception as e:
    errorMessage="Exception occured while executing getCompiledWithExpressionForUdf : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  

# COMMAND ----------

# DBTITLE 1,Function Columns: perform stringreplace, variable replacement, casting and naming
def performStringreplaceCastingAndNaming(columnList,stringReplacementList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  #this function performs several operations on teh select columns.  This includes replacing the xml paths fromt he point of explode (including replacement of the special ¦ and ¬ characters), the evaluation of variables as a select, adds in casting if there datatypes are difference for compute types that are not udf or expression.
  try:
    #firstly, loop through each of the string replacemetns to take place
    for replacement in stringReplacementList:
      #for each string replacmetn loop through the column list and perform the replacment. We are only replacing column 1 that is the select itself
      columnList = [(c[0]
                   , c[1].replace(replacement[0], replacement[1])
                   , c[2]
                   , c[3]
                   , c[4]
                   , c[5]) for c in columnList if c[2] != 'hash']

    #loop through each of the selects and evaluate the variable expression into the select if the compute type is variable.
    #the formatting is quite tricky, but we are trying to pass in a quoted variable statement into the eval.
    #i.e. we pass in "\'{}\'".format(variable_name) into the eval. The eval then replaces the {} with the actual variable and removes the escape symbols from the quotes ', but maintains the double-quotes around the entire select. This eventually leaves the variable in single quotes.
    columnList = [(c[0]
                 , eval('{}'.format('"\'{}\'".format(' + c[1] + ')')) if c[2] == 'variable' else c[1]
                 , c[2]
                 , c[3]
                 , c[4]
                 , c[5]) for c in columnList if c[2] != 'hash']

    #for compute types that udf or expressio or where the source and desination types are the same don't perform any cast, otherwise perform a simple cast as the destination type 
    #explicityly for expressions, the type will be expliclity cast in the expression itself in the database (i.e. in the metadata)
    columnList = [(c[0]
                  ,(c[1] if ((c[4] == c[5]
                             or c[2] in ['udf','unpivot'] 
                             or c[4].upper() in ['BINARY', 'DATE', 'TIMESTAMP'] 
                             or(c[4].upper() in ['STRING'] 
                                and c[2] in ('expression','none','join') 
                                and c[5]==None)) 
                            and (c[2] != 'variable'  ))
                    #If the data types not match or udf nad pivot types no need for casting, and for binary date and timestamp columns no casting required. for expressions and join columns which don't have source colun(c[2] is none) don't need casting.
#                    else 'CAST({} AS {})'.format(c[1], c[4]) if  c[4] != c[5]  and c[2]== 'none' and c[4] in [ 'STRING']  and c[5]!=None
                       else c[1] if c[2]=='variable' and c[4].upper() in['STRING'] #If it is variable and string type don't need to do casting.
                       else 'CAST({} AS {})'.format(c[1], c[4])
                   )
                 , c[2]
                 , c[3]
                 , c[4]
                 , c[5]) for c in columnList]

    #as the as column to the column.
   # columnList = [(c[0],c[1] + ' AS {0}'.format(c[0]), c[2], c[3], c[4], c[5]) for c in columnList]
      #as the as column to the column or take c[0] i.e. the target column name for udf/hash as it has already been created in a withColumn
    #Verify if c[2] is in 'join' append the join table name to column name with '_' and coalesce with 'Unknown' if it is lef join.
    columnList = [(c[0]
                 , c[0] if c[2] in ['udf'] 
                        else 'coalesce('+c[1].split(',')[0].split('(')[1] + '_{0},"Unknown") AS {1}'.format(c[1].split('[')[1][:-1],c[0]) 
                              if c[2] in ['join'] and c[1].split('(')[0].replace('Join','')=='Left' #If attribute is join type and left join coalesce with Unknown
                        else c[1].split(',')[0].split('(')[1] + '_{0} AS {1}'.format(c[1].split('[')[1][:-1],c[0]) 
                              if c[2] in ['join'] and c[1].split('(')[0].replace('Join','')!='Left' #If attribute is join type and not left join 
                        else c[1] if c[2] in ['unpivot'] 
                        else c[1] + ' AS {0}'.format(c[0])
                 , c[2]
                 , c[3]
                 , c[4]
                 , c[5]) for c in columnList]
  
    return columnList
  except Exception as e:
    errorMessage="Exception occured while executing performStringreplaceCastingAndNaming : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False   

# COMMAND ----------

# DBTITLE 1,Function Columns: get with expressions and select list
def getWithExpressionsAndSelectList(columnList,xmlStartString,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  #function that recieves a list in the form of
  #'target_column_name', 'expression', 'computed_type', 'source_column', 'target_column_type', 'source_column_type'
  #and returns two lists
  #1. 'target_column_name', 'compiled contents for with expression', 'code being used in the compiled expression'
  #note that the target column name and the compuiled contents are to be used when adding the with expressions.
  #they should be used in order also. as some with expressions need to be run before others
  #2. 'target_column_name', 'select', 'computed_type', 'source_column', 'target_column_type', 'source_column_type'
  #note that the 'select' column is the one to use in the select expression. The other columns are just there for context and debugging.
  try:
    openingCharacter = '¦'
    closingCharacter = '¬'
    firstPassColumnList = []
    allColumns = []
    stringReplacementList = []
    
    #this function will take in the list of columns to be selected
    #it will loop through the columns and append the path prefix that is not already stored in XML
    #it will then loop through each of the selects, then for each expression that appears it will calculate the 


    #add the column and initial XML reference to each xml column reference by replacing the opening character
    for column in columnList:
      if column[2] != 'ignore':
        #we need to access the expression
        simpleStringFirstPass = ( column[0]
                                , column[1].replace(openingCharacter, xmlStartString) if 'none' not in column[2] else column[3]
                                , column[2]
                                , column[3]
                                , column[4]
                                , column[5])
        firstPassColumnList.append(simpleStringFirstPass) if simpleStringFirstPass not in firstPassColumnList else firstPassColumnList

    #print(firstPassColumnList)

    #at this point teh columns should all have the initial root of the column and XML in place
    #now try and split each column into a new item in a list using the ~ and ¬ as delimiters

    for fpColumn in firstPassColumnList:
      #print(fpColumn)
      #EXAMPLE: ('columnD', '¦unstructuredxmlSource_COLUMN.OuterXML.Hello.[x].y¬ + "|" + ¦unstructuredxmlSource_COLUMN.OuterXML.Hello.[p].q.[r].s¬', 'expression', 'unstructuredxmlSourceCOLUMN')

      #only do work if there is something to do.. i.e. there is xml parsing required
      if openingCharacter in fpColumn[1]:
        #split on each of the columns in the select list
        #remember the leading ~ will be removed, leaving only the trainling ¬
        flColumns = fpColumn[1].split(openingCharacter)
        #print(flColumns)
        #EXAMPLE: ['', 'unstructuredxmlSource_COLUMN.OuterXML.Hello.[x].y¬ + "|" + ', 'unstructuredxmlSource_COLUMN.OuterXML.Hello.[p].q.[r].s¬']
        for eflColumn in flColumns:
          #check there is something there
          if len(eflColumn) > 0 and closingCharacter in eflColumn:
            #EXAMPLE: unstructuredxmlSource_COLUMN.OuterXML.Hello.[x].y¬ + "|" +
    #         print(eflColumn)
            #now find the end of each column and add it to a distinct column list
            actualColumn = eflColumn[0:eflColumn.find(closingCharacter)]
            #EXAMPLE: unstructuredxmlSource_COLUMN.OuterXML.Hello.[x].y
            #print(actualColumn)

            #if the column isn't in the list then add it
            #add it do the allColumns list that contains the distinct list of columns that have some kind of xml component to them.
            #The target will be to use this for string replacemtn further below
            allColumns.append(actualColumn) if actualColumn not in allColumns else allColumns

            #also calculate the string replacement list
            #find the current column as the last item in the select and use the get_path function to get the path for replacement
            finalAttribute = getFinalXmlAttribute(actualColumn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
            #EXAMPLE: y
            #print(final_attribute)
            #since some of the xml nodes are arrays and need exploding, this will result in a possibly different path, so calculate that.
            attributePath = getSelectPath(actualColumn,finalAttribute,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
            #EXAMPLE: x.y
#            print(attribute_path)        

            #we need to calculate the string that was in the original selects and the replacement string, and store it in a list.  This will be used to do the string replacemetns on the original list.
    #         stringToReplace = (openingCharacter + eflColumn, attribute_path)
            stringToReplace = (openingCharacter + actualColumn + closingCharacter, attributePath)
            #EXAMPLE: '¦unstructuredxmlSource_COLUMN.OuterXML.Hello.[x].y¬ + "|" + ', 'x.y'
            #print(stringToReplace)
            #add the string replacement tuple if it isn't there
            stringReplacementList.append(stringToReplace) if stringToReplace not in stringReplacementList else stringReplacementList

            #at this point we have the string replacements that we need to carry out, and the columns to be passed in to create the with columns for exploding columns

    #find the columns with compute type 'udf' and contain and array
    arrayUdfColumns = [c for c in firstPassColumnList if '[' in c[1] and c[2] in ['udf', 'hash']]

    for replacement in stringReplacementList:
      arrayUdfColumns = [(c[0], c[1].replace(replacement[0], replacement[1]), c[2]) for c in arrayUdfColumns]

    withExpressionListArrayUdf = getCompiledWithExpressionForUdf(arrayUdfColumns,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    #print(withExpressionListArrayUdf)

    #using the list of all columns, calculate the with column expressions using the function.
    withExpressionList = calculateExplodeWithExpressions(allColumns,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    #add additional column onto the list so that it aligns with teh udfColumns lists
    withExpressionList = [[c[0],c[1],c[2],"preSelect"] for c in withExpressionList]
    # print(withExpressionList)

    noArrayUdfColumns = [c for c in firstPassColumnList if '[' not in c[1] and c[2] in ['udf', 'hash']]

    for replacement in stringReplacementList:
      noArrayUdfColumns = [(c[0], c[1].replace(replacement[0], replacement[1]), c[2]) for c in noArrayUdfColumns]

    withExpressionListNoArrayUdf = getCompiledWithExpressionForUdf(noArrayUdfColumns,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

    #print(withExpressionListNoArrayUdf)

    withExpressionListFinal = withExpressionList+withExpressionListArrayUdf+withExpressionListNoArrayUdf
#     withExpressionListFinal.append(withExpressionListArrayUdf)
#     withExpressionListFinal.append(withExpressionList)
#     withExpressionListFinal.append(withExpressionListNoArrayUdf)

    #make call to function to correct and enhance column selects
    columnListFinal = performStringreplaceCastingAndNaming(firstPassColumnList, stringReplacementList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

    return withExpressionListFinal, columnListFinal
  except Exception as e:
    errorMessage="Exception occured while executing getWithExpressionsAndSelectList : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  

# COMMAND ----------

# DBTITLE 1,Function pii_Hash : Clean Literal string value attributes 
def cleanPiiHashLiteralAttributes(attributeFunctionList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    #This function takes the atrribute list with lit() and col() and concatenate the consecutive lit items to single lit with |
    # [lit('Test'),lit('Function'), col('a')]------>[lit(Test|Function),col('a')]
    try:
      #Apend the symbol as end of the list . Later this will be checked in list
#       attributeFunctionList.append('¬')
      #Define a list to retrun
      attributeFunctionCleanList=[]
      #Initiate concatenation variable
      concatString=''
      
      for attributeFunctionListItem in attributeFunctionList:
        #Check whether attribute is column function or lit.if its lit concatenate to the variable with delimeiter
        if attributeFunctionListItem.startswith('lit('):
          if attributeFunctionListItem==attributeFunctionList[-1]: #check for end of the list element and append the concatenate string to list
            if concatString!='':
              attributeFunctionCleanList.append('lit("'+concatString[1:]+'")')#Add it to output list by remiving the first character ''
              
          concatString=concatString+'|'+attributeFunctionListItem[5:-2] # take the string between lit('')

        else:#if it is column attribute add col and concatenated string with lit to return list
          if concatString!='':
            attributeFunctionCleanList.append('lit("'+concatString[1:]+'")')
            
          attributeFunctionCleanList.append(attributeFunctionListItem)

          concatString='' # Initiate the concatenate varibale for next iteration
      return attributeFunctionCleanList
      
    except Exception as e:
      errorMessage="unable to apply hash_literal cleaning: " + str(e)
      logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      assert False

# COMMAND ----------

# DBTITLE 1,Function pii_Hash : Concatenate with String Evalutation
def concatenateFunc(List):

  #this function evalutes the string expresesion in the list and concatenate with '|' seperator
  #It map the the function eval to every element and create a new list and return the concatenated string with '|' 

  #Added a coalesce to replace null values with an empty string so they are specifically injected with a '|' 
# If it is a column type means lit, we don't need to evaluate

  return concat_ws('|',*list(map(lambda x: eval("coalesce(col('" + x +"').cast(StringType()), lit(''))") if not (str(x).startswith('Column') or (str(x).startswith('lit')))  else eval(x) if str(x).startswith('lit') else x,List)))



# COMMAND ----------

# DBTITLE 1,Function pii_Hash : Compute Hash UDF
#specifically no try except as this is being called while computing for a dataframe.  The caller will handle the error.
def computeHashValue(hashString):
  #use the sha256 to generate binary hash and convert to long int for performance gain on joins
  shaValue = int(sha256(hashString.upper().encode()).hexdigest(), 32) % (10 ** 18)

  return shaValue
      
#Register as UDF
#computeHashValueUdf=udf(computeHashValue, LongType())

# COMMAND ----------

# DBTITLE 1,Function Hash:Compute Hash Value To Decimal 28
def computeHashValueDecimal28(hashString):
  # truncate hash to first 12 bytes then cast to a python int, and return it as a String.
  # 12 bytes allows up to 7.922816E+28 - the biggest number of whole bytes you can get into a decimal(28,0)
  shaValue = Decimal(int.from_bytes(sha256(hashString.upper().encode()).digest()[0:11], byteorder=sys.byteorder, signed=True))
  return shaValue

# COMMAND ----------

# DBTITLE 1,Function Hash:Comput Hash Value BigInt
def computeHashValueBigInt(hashString):
  # truncate hash to first 8 bytes then cast to a phyton int, and return it as a Long.
  shaValue = int.from_bytes(sha256(hashString.upper().encode()).digest()[0:7], byteorder=sys.byteorder, signed=True)
  return shaValue

# COMMAND ----------

# DBTITLE 1,Function Hash:Compute Obscure value
def computeObscure(attribute):
  # Convert to string and apply hash and get first 12 characters of the string

  obscureValue = sha256(str(attribute).encode()).hexdigest()[0:11]
  return obscureValue

# COMMAND ----------

# DBTITLE 1,Register computeHashValueUdf
def computeHashValueUdfRegistration():
  
  #Declare the function as global so that it can be registered and accessed outside of the funciton
  global computeHashValueBigIntUdf
  global computeHashValueDecimal28Udf
  global ComputeObscureUdf 
  global computeHashValueTinyIntUdf
  computeHashValueBigIntUdf=udf(computeHashValueBigInt, LongType())
  computeHashValueDecimal28Udf=udf(computeHashValueDecimal28, DecimalType(28,0)) 
  ComputeObscureUdf=udf(computeObscure,StringType())
  computeHashValueTinyIntUdf=udf(computeHashValueBigInt,ByteType())

# COMMAND ----------

# DBTITLE 1,Function pii_Hash : Get Pii Hash Variables
def piiGetHashVariables(objectPiiHashDetailsdf,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    # This functon gets the expression from store proc and clean the expressions using cleanPiiHashLiteralAttributes function and return values
    #Get the pii details dataframe from store proc
    
    #Get the pii version and expression variabiles for pii and traceabilipty hash
    piiHashVersion=objectPiiHashDetailsdf['hash_version'][0]
    
    piiHashColumnFunctionList=objectPiiHashDetailsdf['piiExpression'][0].split(',')
    
    piiHashTraceabilityColumnFunctionList=objectPiiHashDetailsdf['piiTreaceabilityExpression'][0].split(',')
    
    # Clean the expression using cleanPiiHashLiteralAttributes
    piiHashColumnFunctionList=cleanPiiHashLiteralAttributes(piiHashColumnFunctionList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
    piiHashTraceabilityColumnFunctionList=cleanPiiHashLiteralAttributes(piiHashTraceabilityColumnFunctionList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
    return piiHashVersion,piiHashColumnFunctionList,piiHashTraceabilityColumnFunctionList
  except Exception as e:
      errorMessage="unable to apply hash_literal cleaning: " + str(e)
      logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      assert False

# COMMAND ----------

# DBTITLE 1,Function:Exec High And Low Level Object HVR specific change, To be removed after UnitTests in DEV
#function definition of spExecHighAndLowLevel that executes stored procedure and save the result as pandas dataframe
def spExecHighAndLowLevel_HVR(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #execution of object high and low level stored procedures
    sourceDestinationTableDetails=pd.read_sql_query("exec [config].[usp_get_object_to_object_high_level_HVR] {}".format(batchTaskId),conn)
    sourceDestinationFieldsDetails=pd.read_sql_query("exec [config].[usp_get_object_to_object_low_level] {}".format(batchTaskId),conn)

    logTaskProgress(cursor,batchTaskId,"executed object to object high and low level stored procedures")
    return sourceDestinationTableDetails,sourceDestinationFieldsDetails
  except Exception as e:
    errorMessage="Exception occured while execution of object high and low level stored procedures: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function:Exec High And Low Level Object 
#function definition of spExecHighAndLowLevel that executes stored procedure and save the result as pandas dataframe
def spExecHighAndLowLevel(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #execution of object high and low level stored procedures
    sourceDestinationTableDetails=pd.read_sql_query("exec [config].[usp_get_object_to_object_high_level] {}".format(batchTaskId),conn)
    sourceDestinationFieldsDetails=pd.read_sql_query("exec [config].[usp_get_object_to_object_low_level] {}".format(batchTaskId),conn)

    logTaskProgress(cursor,batchTaskId,"executed object to object high and low level stored procedures")
    return sourceDestinationTableDetails,sourceDestinationFieldsDetails
  except Exception as e:
    errorMessage="Exception occured while execution of object high and low level stored procedures: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: Get the struct for high and low level procedure call
def getStructForHighAndLowLevelDf():
  #Schema for High level stor proc as pandas stores different null values for different types having an issue while conveting to spark df for infering types for null columns.
  sourceDestinationFieldsDetailsSchema=StructType([
                                                   StructField('source_object_id'                          ,IntegerType(),True)
                                                  ,StructField('source_object_definition_id'	           ,IntegerType(),True)
                                                  ,StructField('source_object_attribute_name'	           ,StringType(),True)
                                                  ,StructField('source_object_attribute_type'	           ,StringType(),True)
                                                  ,StructField('destination_object_id'	                   ,IntegerType(),True)
                                                  ,StructField('destination_object_definition_id'	       ,IntegerType(),True)
                                                  ,StructField('destination_object_attribute_name'	       ,StringType(),True)
                                                  ,StructField('destination_object_attribute_type'	       ,StringType(),True)
                                                  ,StructField('destination_object_attribute_order'	       ,StringType(),True)
                                                  ,StructField('destination_is_pii'	                       ,IntegerType(),True)
                                                  ,StructField('destination_hash_attribute_type_code_id'   ,StringType(),True)
                                                  ,StructField('destination_object_attribute_is_computed'  ,StringType(),True)
                                                  ,StructField('destination_object_attribute_computed_type',StringType(),True)
                                                  ,StructField('destination_object_attribute_default_value',StringType(),True)
                                                  ,StructField('source_as_destination'                     ,StringType(),True)
                                                  ,StructField('primary_key_order'                         ,IntegerType(),True)
                                                  ,StructField('track_type_2_changes'                     ,IntegerType(),True)
                                                  ])
  return sourceDestinationFieldsDetailsSchema

# COMMAND ----------

# DBTITLE 1,Function: Get the struct for PKs and Type2 fields procedure call
def getStructForPKsType2Df():
  #Schema as pandas stores different null values for different types having an issue while conveting to spark df for infering types for null columns.
  fieldsSchema = StructType([
                          StructField('object_attribute_name'	           ,StringType(),True)
                          ,StructField('primary_key_order'                 ,IntegerType(),True)
                          ,StructField('track_type_2_changes'              ,BooleanType(),True)
                          ])
  return fieldsSchema

# COMMAND ----------

# DBTITLE 1,Function: Exec proc to get primary keys and type2 fields
#Execute a proc to get results into a pandas dataframe
def spExecGetPKsType2Fields(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    objectDetails = pd.read_sql_query("exec [config].[usp_get_object_pk_type2_fields] {}".format(batchTaskId),conn)
    
    logTaskProgress(cursor,batchTaskId,"executed get pks and type2 fields stored procedure")
    return objectDetails
  except Exception as e:
    errorMessage="Exception occured while execution of get pks and type2 fields stored procedure: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False        

# COMMAND ----------

# DBTITLE 1,Function: Add audit columns and HBK column to data frame
def addAuditAndHBKColumnsOnDF(dfCnf,pksFieldsList,type2FieldsList,currentTs,lakeFromDatetime,hasHPK,sourceId,batchId,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
#add audit and hbk columns
  try:
    #get the date as an int format
    createdDate = int(currentTs.strftime('%Y%m%d'))
    
    dfCnf = (dfCnf.withColumn("SourceID",lit(sourceId))
                  .withColumn("lakeCreatedDate",lit(createdDate))
                  .withColumn("lakeCreatedTimestamp",lit(currentTs))
                  .withColumn("lakeCreatedBatchID",lit(batchId))
                  .withColumn("lakeLastUpdateDate",lit(createdDate))
                  .withColumn("lakeLastUpdateTimestamp",lit(currentTs))
                  .withColumn("lakeLastUpdatedBatchID",lit(batchId))
                  .withColumn("LakeValidFromTimestamp",lit(lakeFromDatetime))
                  .withColumn("LakeValidToTimestamp",lit(None))
                  .withColumn("LakeIsActive",lit(True))
                  #hbk columns
                  .withColumn("HashedBusinessKey", computeHashValueBigIntUdf(concatenateFunc(pksFieldsList)))
                  .withColumn("HashValue", computeHashValueBigIntUdf(concatenateFunc(type2FieldsList)))
           )
    if (hasHPK):
        dfCnf = dfCnf.withColumn("HashedPartitionKey", computeHashValueTinyIntUdf(concatenateFunc(pksFieldsList)))

    logTaskProgress(cursor,batchTaskId,"HBK and audit columns added to DF")
    return dfCnf
  except Exception as e:
    errorMessage="Exception occured while adding columns to DF: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
    assert False 

# COMMAND ----------

# DBTITLE 1,Function: Get the select and with column details
#Prepare the tuple to pass on for get with expression and select list function
def getSelectAndWithColumnDetails(srcAndDesFieldsDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    columnList = srcAndDesFieldsDF.select("destination_object_attribute_name"
                                          ,"destination_object_attribute_default_value"
                                          ,"destination_object_attribute_computed_type"
                                          ,"source_object_attribute_name"
                                          ,"destination_object_attribute_type"
                                          ,"source_object_attribute_type").rdd.map(tuple).collect()
    logTaskProgress(cursor,batchTaskId,"Got the column Details for With Expression and select")
    return columnList
  except Exception as e:
    errorMessage="Unable to retrieve he column Details for With Expression and select: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to prepare destination datataframe
def selectDestFromSrc(sourceFormat,sourceHeader,sourceLocation,sourceTableName,withColumnList,
                         sourceColumns,whereExpression,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #It uses the reduce function to apply the with column recursively and lamda function to add with columns
    #Filter the preselect and postselect withcolumns and use the reduce to create dataframe.
    preSelectWithColumnList=[withColumnTuple for withColumnTuple in withColumnList if withColumnTuple[3]=='preSelect']
    postSelectWithColumnList=[withColumnTuple for withColumnTuple in withColumnList if withColumnTuple[3]=='postSelect']
    #reduce function takes 3 parameters(functiontto iterate,list of iterables,Initialiser.here intiialirser will be the dataframe
      #apply the reduce with preselectwithColumnlist first
      #select the source columns
      #apply the reduce with postSelectWithColumnList using abve selected dataframe as initializer.
    sourceSelectWhereDF=reduce(lambda dataframe,columnList:dataframe.withColumn(columnList[0],eval(columnList[1])),postSelectWithColumnList,
                                reduce(lambda dataframe,columnList:dataframe.withColumn(columnList[0],eval(columnList[1])),preSelectWithColumnList,  
                                                               (spark.read.format(sourceFormat)
                                                               .option("header",sourceHeader)
                                                               .load(sourceLocation)
                                                               .where(whereExpression))).selectExpr(sourceColumns))  
    logTaskProgress(cursor,batchTaskId,"Read from the source based on select expression and where condition")
    return sourceSelectWhereDF  
  except Exception as e:
    errorMessage="Exception occured while reading destination columns from the source: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
    assert False 

# COMMAND ----------

# DBTITLE 1,Function to prepare destination datataframe using source table
def selectDestFromSrcTable(sourceTableName,withColumnList,sourceColumns,whereExpression,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #It uses the reduce function to apply the with column recursively and lamda function to add with columns
    #Filter the preselect and postselect withcolumns and use the reduce to create dataframe.
    preSelectWithColumnList=[withColumnTuple for withColumnTuple in withColumnList if withColumnTuple[3]=='preSelect']
    postSelectWithColumnList=[withColumnTuple for withColumnTuple in withColumnList if withColumnTuple[3]=='postSelect']
    #reduce function takes 3 parameters(functiontto iterate,list of iterables,Initialiser.here intiialirser will be the dataframe
      #apply the reduce with preselectwithColumnlist first
      #select the source columns
      #apply the reduce with postSelectWithColumnList using abve selected dataframe as initializer.
    sourceSelectWhereDF=reduce(lambda dataframe,columnList:dataframe.withColumn(columnList[0],eval(columnList[1])),postSelectWithColumnList,
                                reduce(lambda dataframe,columnList:dataframe.withColumn(columnList[0],eval(columnList[1])),preSelectWithColumnList,  
                                                               (spark.read.table(sourceTableName)
                                                               .where(whereExpression))).selectExpr(sourceColumns))  
    logTaskProgress(cursor,batchTaskId,"Read from the source based on select expression and where condition")
    return sourceSelectWhereDF  
  except Exception as e:
    errorMessage="Exception occured while reading destination columns from the source: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
    assert False 

# COMMAND ----------

# DBTITLE 1,Function to get unstructuredxmlSource object details metadata
#function definition of [config].[usp_unstructuredxmlSource_get_object_details] that executes stored procedure and save the result as pandas dataframe
def spExecunstructuredxmlSourceGetObjectDetails(batchTaskId,conn,cursor,adfPipelineName,
                               clusterId,notebookName,errorLogFileLocation):
  try:
    #execution of unstructuredxmlSource object details stored procedure
    unstructuredxmlSourceMetadataDetails=pd.read_sql_query("exec [config].[usp_unstructuredxmlSource_get_object_details]",conn)
    

    logTaskProgress(cursor,batchTaskId,"executed unstructuredxmlSource_get_object_details stored procedure")
    return unstructuredxmlSourceMetadataDetails
  except Exception as e:
    errorMessage="Exception occured while execution of unstructuredxmlSource_get_object_details stored procedures: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function To Generate Mapping Expression
def generateMappingExpression(mappingDataFrame,columnList,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  #This function strictly for smaller Datasets under 500 rows
  #This Function Will give the mapping expression to use in the lookups, As this mapping expression can be used as alternate for joins(The data size should be minimal)
  try:
    if(len(columnList)>2):# Check how many columns are tehre in the column list if it is more than 2 we need to create them as arrays
      mappingRefDic =mappingDataFrame.select(columnList).toPandas().set_index(columnList[0]).T.to_dict('list') #Prepare the data dictionary with the First column as key and other columns as list of values
      # Create map expects key and value sequences we extract from the dictironary and check if it is a list or not if it is alist we wrap in array if not normal lit 
      mappingExpression=create_map([array([lit(y) for y in x]) if isinstance(x, list) else lit(x) for x in chain(*mappingRefDic.items())]) 
    else:
      #If the columns are only 2 we dont need to create dictionary and lit .We can get directly from the columns of the dataframe

      mappingRefDic=mappingDataFrame.select(columnList).rdd.collectAsMap()
      mappingExpression=create_map([lit(x) for x in chain(*mappingRefDic.items())]) 
      #Return mapping Expression first column as key and second column as values

    return mappingExpression
  except Exception as e:
    errorMessage="Exception occured while execution of unstructuredxmlSource_get_object_details stored procedures: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function To Generate Dictionary From A DataFrame
def generateDict(dataframe,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #This function takes the dataframe and return the dictionary with first column as key  with default value with Key '¦default¬' set to it.
    columnList=dataframe.columns #If the df has 2 columns the default value is stright farward
    if len(dataframe.columns)<=2:
      dfDictionary= dataframe.rdd.collectAsMap()#Convert to dictionary
      dfDictionary.update({'¦default¬':'Unknown'})
    else:
      dfDictionary= dataframe.toPandas().set_index(columnList[0]).T.to_dict()#Convert to dictionary
      defaultDic={}
      for cname in columnList[1:]: #Loop throgh from 2nd column and create the dictionary with None
        defaultDic.update({cname:'Unknown'})
      dfDictionary.update({'¦default¬':defaultDic})
    return dfDictionary 
  except Exception as e:
    errorMessage="Exception occured while executing function generateDict: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to identify new data has been loaded into dependent objects after the target was last loaded
#example input of targetTableName 'delta107m' and dependentTableNamesList ['testBroadcastTableFirst', 'testBroadcastTableSecond']

def identifyIfNewDataInDependentTables (targetTableName, dependentTableNamesList, cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #get the latest timestamp the reference table has rows loaded into it
    destinationTableLastUpdatedDF = (spark.sql("DESCRIBE HISTORY {}".format(targetTableName))
                                   .select("timestamp", "operationMetrics.numOutputRows")
                                 
                                   )
    destinationTableLastUpdated=  (destinationTableLastUpdatedDF.where("operationMetrics.numOutputRows > 0")
                                   .agg({'timestamp':'max'})
                                   .toDF("latestUpdate")
                                   .fillna({'latestUpdate':0}).collect()[0][0])
    destinationTableLastTruncated = (destinationTableLastUpdatedDF.where("operation == 'TRUNCATE'")
                                   .agg({'timestamp':'max'})
                                   .toDF("latestUpdate")
                                   .fillna({'latestUpdate':0}).collect()[0][0]
                                  )
    
    #set to False initial, so that unless we change it to True then it will remain False
    dependentTableIsUpdated = True if destinationTableLastTruncated>destinationTableLastUpdated else False
    #for each of the dependent tables
    for dependentTable in dependentTableNamesList:
      #check if we need to loop again. i.e. if it is already True there is no point checking other tables
      if dependentTableIsUpdated == False:
        #get the latest timestamp the dependent table has rows loaded into it
        currentDependentTableLastUpdated = (spark.sql("DESCRIBE HISTORY {}".format(dependentTable))
                                            .select("timestamp", "operationMetrics.numOutputRows")
                                            .where("operationMetrics.numOutputRows > 0")
                                            .agg({'timestamp':'max'})
                                            .toDF("latestUpdate")
                                            .fillna({'latestUpdate':0}).collect()[0][0]
                                           )

        #has the dependent table had rows loaded later than the source object
        if currentDependentTableLastUpdated >= destinationTableLastUpdated:
          dependentTableIsUpdated = True
  
    return dependentTableIsUpdated 
  except Exception as e:
    errorMessage="Exception occured while executing function identifyIfNewDataInDependentTables: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  

# COMMAND ----------

# DBTITLE 1,Function to create or replace a temporary view
def createOrReplaceTemporaryView (viewName,viewDefinition,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #prepare the sql to create the view
    viewSQL = 'CREATE OR REPLACE TEMPORARY VIEW {} AS \n{}'.format(viewName, viewDefinition)
    #execute the sql to create the view
    spark.sql(viewSQL)
  except Exception as e:
    errorMessage="Exception occured while executing function createOrReplaceTemporaryView to create view {}: ".format(viewName) + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Function to create or replace a view
def createOrReplaceView (viewName,viewDefinition,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #prepare the sql to create the view
    viewSQL = 'CREATE OR REPLACE VIEW {} AS \n{}'.format(viewName, viewDefinition)
    #execute the sql to create the view
    spark.sql(viewSQL)
  except Exception as e:
    errorMessage="Exception occured while executing function createOrReplaceView to create view {}: ".format(viewName) + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Function To get SparkTables
def getSparkCatalogTableDetails(conn
                      ,cursor
                      ,batchTaskId
                      ,errorMessage
                      ,adfPipelineName
                      ,clusterId
                      ,notebookName
                      ,errorLogFileLocation):
  try:
    #Execution of get active location stored procedure
        # databaseList[0].name
    databaseList=spark.catalog.listDatabases()
    databaseNameList=list(map(lambda x: x.name,databaseList))
    TableDetails=list(map(lambda x:spark.catalog.listTables(x),databaseNameList))
    FinalTableList = [x for l in TableDetails for x in l]
    PermanentTableList=[x for x in FinalTableList if x.isTemporary==False]
    TablesName=list(map(lambda x:(x.database+'.'+x.name,x.tableType),PermanentTableList))
    logTaskProgress(cursor,batchTaskId,"executed getSparkCatalogTableDetails Fuction")
    return spark.createDataFrame(TablesName,['tableName','Type'])    
  except Exception as e:
    errorMessage = "Exception occured while execution of getSparkCatalogTableDetails: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to get the high and low level detail dataframes from input pandas dataframe
#function to convert the panda dataframe to spark dataframe
def getTableHighAndLowLevelDF(activeDatabricksTablesPandasDF,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:

    #schema structre of the procudure result 
    schema = StructType([ StructField("object_name"                           , StringType()  , True)
                         ,StructField("location"                              , StringType()  , True)
                         ,StructField("partition_scheme"                      , StringType()  , True)
                         ,StructField("object_attribute_name"                 , StringType()  , True)
                         ,StructField("object_attribute_type"                 , StringType()  , True)
                         ,StructField("object_attribute_order"                , IntegerType() , True)
                         ,StructField("extended_properties"                   , StringType()  , True)
                         ,StructField("schema_string"                         , StringType()  , True) 
                         ,StructField("XML_string_prefix"                     , StringType()  , True) ])
    #converting the panda dataframe to spark dataframe
    #currently filtering for Raw_unstructuredxmlSourceQuoteHistoricDelta table so that we don't try and create/change it
    activeTableDf = convertPandasToSparkDfWithSchema(activeDatabricksTablesPandasDF,schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation).na.fill('')
    
    # Getting the unique table attributes into a new dataframe
    tableHighDetailsDf = (activeTableDf
                          .select('object_name', 'location', 'partition_scheme', 'extended_properties', 'schema_string', 'XML_string_prefix')
                          .distinct()
                         )
    
    #get all the tables so we can check to see if the table exists
    allTablesDF = getSparkCatalogTableDetails(conn
                      ,cursor
                      ,batchTaskId
                      ,errorMessage
                      ,adfPipelineName
                      ,clusterId
                      ,notebookName
                      ,errorLogFileLocation)
    allTablesDF = allTablesDF.select("tableName")

    #perform a left join to the existing tables to see if the table already exists or not.
    tableHighDetailsDf = (tableHighDetailsDf
                           #left join and convert the object name and table name to upper to match case
                          .join(allTablesDF, upper(tableHighDetailsDf.object_name) == upper(allTablesDF.tableName), how='left')
                          #when table name is null then table exists is False, otherwise True
                          .withColumn("table_exists", when(col("tableName").isNull(), False).otherwise(True))
                          #drop the tableName as it is no longer required
                          .drop("tableName")
                         )
    # select only the required columns from activeTableDF
    tableLowDetailsDf = activeTableDf.select('object_name','object_attribute_name','object_attribute_type','object_attribute_order')
    logTaskProgress(cursor,batchTaskId,"got high and low level table details into dataframe")
    
    return tableHighDetailsDf,tableLowDetailsDf
  except Exception as e:
    errorMessage="Unable to get high and low level table details into dataframe: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to prepare destination datataframe using source DataFrame
def selectDestFromJoinSrc(joinSourceDf,withColumnList,sourceColumns,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #It uses the reduce function to apply the with column recursively and lamda function to add with columns
    #Filter the preselect and postselect withcolumns and use the reduce to create dataframe.
    preSelectWithColumnList=[withColumnTuple for withColumnTuple in withColumnList if withColumnTuple[3]=='preSelect']
    postSelectWithColumnList=[withColumnTuple for withColumnTuple in withColumnList if withColumnTuple[3]=='postSelect']
    #reduce function takes 3 parameters(functiontto iterate,list of iterables,Initialiser.here intiialirser will be the dataframe
      #apply the reduce with preselectwithColumnlist first
      #select the source columns
      #apply the reduce with postSelectWithColumnList using abve selected dataframe as initializer.
    sourceSelectWhereDF=reduce(lambda dataframe,columnList:dataframe.withColumn(columnList[0],eval(columnList[1])),postSelectWithColumnList,
                                reduce(lambda dataframe,columnList:dataframe.withColumn(columnList[0],eval(columnList[1])),preSelectWithColumnList,  
                                                               (joinSourceDf)).selectExpr(sourceColumns))  
    logTaskProgress(cursor,batchTaskId,"Read from the source based on select expression and where condition")
    return sourceSelectWhereDF  
  except Exception as e:
    errorMessage="Exception occured while reading destination columns from the source: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
    assert False 

# COMMAND ----------

# DBTITLE 1,function to cache databricks table
def cacheDatabricksTable (tableName,columnsRestriction,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #calculate if there are columns to restrict or select *
    columnsToSelect = "*" if len(columnsRestriction) == 0 else columnsRestriction
    
    #capture the start time
    startTime = datetime.now()
    #cache the table
    tableToCache = spark.read.table(tableName).select(columnsToSelect)
    tableToCache.cache()
    
    #trigger the cache
    rowCount = tableToCache.count()
    
    #get the end time
    endTime = datetime.now()
    
    elapsedTime = (endTime - startTime)
    
    logTaskProgress(cursor,batchTaskId,"Successfully cached table :{} with columns '{}' in time:{}".format(tableName, ','.join(columnsToSelect), str(elapsedTime)))
  except Exception as e:
    errorMessage="Exception occured while while executiong cacheDatabricksTable: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
    assert False   

# COMMAND ----------

# DBTITLE 1,Function to get notebook jobId
def getNotebookJobId():
  try:
    #Get Notebook info by running dbutils command and covert into json dictionary
    batchNotebookInfo = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
    #From notebook info get jobId
    batchJobId = batchNotebookInfo["tags"]["jobId"] 
    #Return the jobId
    return batchJobId
  except:
    return -1

# COMMAND ----------

# DBTITLE 1,Function to get batch level object row count
def getBatchLevelObjectRowCount(objectName):
  try:
    #Run function to get the jobId
    batchJobId = getNotebookJobId()
    #Verify batchJobId if it is -1.So, something wrong to get the jobid.
    if batchJobId != -1 :
      #For that particular jobId and obejct name get the count by describe table metrics
      batchLevelObjectRowCount = spark.sql("DESCRIBE HISTORY {}".format(objectName)).select("operationMetrics.numOutputRows").where("operation = 'WRITE' and job.jobId = {}".format(batchJobId)).collect()[0][0]
    else:
      #Return default value -1 to indicate something wrong to get the count
      batchLevelObjectRowCount = -1
    
    return batchLevelObjectRowCount   
  except:
    return -1

# COMMAND ----------

# DBTITLE 1,Function to convert Pandas DF to Spark DF (Spark 3 Upgrade)
def convertPandasToSparkDfWithSchema(pandasDf,schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  #Convert the pandas DataFrame to sparkDataframe without schema
  try:
    sparkDf=spark.createDataFrame(pandasDf)
    existingSchema=sparkDf.schema
    schemaToUpdate=schema
    #Read the existing Schema with the simple string 
    existingSchemaSplitList=existingSchema.simpleString().replace('struct<','').replace('>','').split(',')

    schemaToUpdateSplitList=schemaToUpdate.simpleString().replace('struct<','').replace('>','').split(',')
    differenceColumnList=[]# Define an empty list to store column differences
    dropColumnList=[]# Define an empty list to store any columns that need to be dropped
    for item in schemaToUpdateSplitList: # For each item in schema to update list 
      index=schemaToUpdateSplitList.index(item) # Store the index of the list to use in next steps
      if  item.split(':')[0] != existingSchemaSplitList[index].split(':')[0]: # If the column names are different in schemas these need to be added using with column and the column which are added while converting to spark from pandas need to be dropped
        dropColumnList.append(existingSchemaSplitList[index].split(':')[0])
        differenceColumnList.append((item.split(':')[0],existingSchemaSplitList[index].split(':')[0],item.split(':')[1]))
      else:  
        if item.split(':')[1] != existingSchemaSplitList[index].split(':')[1]:  # If the column names are same and data types are different the existing column need to be overwritten with new type using with column
          differenceColumnList.append((item.split(':')[0],existingSchemaSplitList[index].split(':')[0],item.split(':')[1]))
  #   print(differenceColumnList)
  #   print(dropColumnList)

   #reduce function takes 3 parameters(functiontto iterate,list of iterables,Initialiser.here intiialirser will be the dataframe
  #apply the reduce with differenceColumnList
        #select the source columns
        # dataframe as initializer.
    finalSparkDf=(reduce(lambda dataframe,columnList:dataframe.withColumn(columnList[0],eval("col('"+columnList[1]+"').cast('"+columnList[2]+"')")) #Function To apply


                         ,differenceColumnList  #Loop through list
                         ,sparkDf #Intiializer
                        ) 
                  .drop(*dropColumnList)
                 )
  #   finalSparkDf=
    return finalSparkDf
  except Exception as e:
    errorMessage="Exception occured while converting pandas to spark dataframe: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
    assert False 

# COMMAND ----------

# DBTITLE 1,Function to get usp_get_object_reference_details stored procedure
def getObjectReferenceDetails(objectName,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #execution the usp_get_object_reference_details stored procedures
    objectReferenceDetails = pd.read_sql_query("exec [config].[usp_get_object_reference_details] {}".format(objectName),conn)
    #Log the progress
    logTaskProgress(cursor,batchTaskId,"executed usp_get_object_reference_details stored procedures")
    
    return objectReferenceDetails
  except Exception as e:
    errorMessage="Exception occured while executing usp_get_object_reference_details stored procedures: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: removeDuplicateColumnsFromString
def removeDuplicateColumnsFromString(columnsString,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #split the sting on , to list
    columnList = columnsString.split(',')
    #strip spacces from teh beginning and end of each item in the list
    spaceStrippedColumnList = [p.strip() for p in columnList]
    #remove duplicate columns
    distinctColumnList = list( dict.fromkeys(spaceStrippedColumnList) )  
    #convert distinct columns back to string
    stringColumnList = ",".join(distinctColumnList)
    
    return stringColumnList
  except Exception as e:
    errorMessage="Exception occured while executing removeDuplicateColumnsFromString function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  

# COMMAND ----------

# DBTITLE 1,Function: dataframeJsonWrapper
def dataframeJsonWrapper(df,sourceField,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):  #Jsonwrapper function to find multiple nested struct fields and combined into single column 'Body'

  try:
    #Identify which fields are nested and store them in  a list
    fieldList=df.schema.fields
    structFieldList=[]
    for field in fieldList:
      if 'StructType' in str(field.dataType):
        structFieldList.append(field.name)

    #Prepare stringStructFieldList from above created list    
    stringStructFieldList=list(map(lambda field:'str_'+field,structFieldList))

    #Create a dataframe by converting the json columns into string columns and drop the unnecessarycolumns
    stringDf=reduce(lambda dataframe,column:dataframe.withColumn('str_'+column,to_json(col(column))),structFieldList,df).drop(*structFieldList)

    #Prepare a initial expression string by using sorucefield
    InitialExpressionString='concat('+'lit('+"'"+'{"OuterJSON":{'+'"'+sourceField+'"'+':{'+"'"+'),'

    #Prepare mainexpressionstring by using the string fields
    mainExpressionString=','.join(list(map(lambda field : 'lit('+"'"+'"'+field+'":'+"'"+'),'+"'"+'str_'+field+"'" if structFieldList.index(field)==0 else 'lit('+"'"+',"'+field+'":'+"'"+'),'+"'"+'str_'+field+"'" ,structFieldList)))

    #Prepare final expression string
    FinalExpression=InitialExpressionString+mainExpressionString+',lit('+"'"+"}}}"+"'"+'))'

    #Prepare final dataframe by evalutiong
    finalDf=stringDf.withColumn('Body',eval(FinalExpression)).drop(*stringStructFieldList)

    return finalDf
  except Exception as e:
    errorMessage="Exception occurred when executing dataframeJsonWrapper function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: applyScdType2onDest To apply scd type 2 dimensions
def applyScdType2onDest(sourceDF,targetObjectName,columnList,currentTs,cursor,batchTaskId,batchId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):  #Function to apply scd type 2 on the tables

  try:
    logTaskProgress(cursor,batchTaskId,"Start of applyScdType2onDest function")
    #Create the temporary view for source dataframe and active Destination tables
    rc=sourceDF.rdd.isEmpty()
    if rc:
        logTaskProgress(cursor,batchTaskId,"Source dataframe is empty, no further work required")
    else:
        lastUpdateDate= int(currentTs.strftime('%Y%m%d'))
        lastUpdatedTimestamp=currentTs
        active_destinationDf=spark.read.table(targetObjectName).where('LakeValidToTimestamp is null' )
        active_destinationDf.createOrReplaceTempView('activeDestinationVw')
        sourceDF.createOrReplaceTempView('fullRefreshsourceVw')
        columnNameList=list(map(lambda x: x[0],columnList))

    #     Extract the columns and add the alias to the column names  , svw used for the source and tob is used for the destiantion
        selectColumns=",".join(map(str,['svw.'+x[0] for x in  columnList]))
        destColumns=",".join(map(str,['tob.'+x[0] for x in  columnList]))
    #     Prepare the merge command with the sub query to have the updated rows from source as inserted columns in the destination, Merge and Haskey are generated from the HashBussinessKey and Hashvalue columns .
        if 'HashedPartitionKey' in columnNameList:
                mergeCommand='''MERGE INTO {0} tob USING (select {1},CASE WHEN svw.HashedBusinessKey is Null THEN  tob.HashedBusinessKey 
                                ELSE Null 
                                END as mergeKey,svw.HashValue as HashKey 
                                from {2} tob
                                join fullRefreshsourceVw svw on tob.HashedPartitionKey=svw.HashedPartitionKey and tob.HashedBusinessKey=svw.HashedBusinessKey and  tob.HashValue<>svw.HashValue
                                where tob.LakeValidToTimestamp is NULL 
                                Union all
                                select {3},HashedBusinessKey as mergeKey,HashValue as HashKey from fullRefreshsourceVw svw
                                Union all
                                select {4}, tob.HashedBusinessKey mergeKey,-1 as HashKey 
                                                from {5} tob
                                LEFT join fullRefreshsourceVw svw on tob.HashedPartitionKey=svw.HashedPartitionKey and tob.HashedBusinessKey=svw.HashedBusinessKey and tob.LakeValidToTimestamp is NULL 
                                where svw.HashedBusinessKey is null and tob.LakeValidToTimestamp is NULL 
                                ) svw  ON svw.MergeKey =tob.HashedBusinessKey and tob.HashedPartitionKey=svw.HashedPartitionKey

                                WHEN MATCHED AND svw.HashKey!=tob.HashValue AND tob.LakeValidToTimestamp is NULL
                                THEN UPDATE  SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                                -- WHEN MATCHED AND s.HashBussinessKeyValue=s.HashBussinessKeyValue  
                                WHEN NOT MATCHED THEN INSERT * '''.format(targetObjectName,selectColumns,targetObjectName,selectColumns,destColumns,targetObjectName,str(lastUpdatedTimestamp),lastUpdateDate,str(lastUpdatedTimestamp),batchId)
        else:
                mergeCommand='''MERGE INTO {0} tob USING (select {1},CASE WHEN svw.HashedBusinessKey is Null THEN  tob.HashedBusinessKey 
                                ELSE Null 
                                END as mergeKey,svw.HashValue as HashKey 
                                from {2} tob
                                join fullRefreshsourceVw svw on  tob.HashedBusinessKey=svw.HashedBusinessKey and  tob.HashValue<>svw.HashValue
                                where tob.LakeValidToTimestamp is NULL 
                                Union all
                                select {3},HashedBusinessKey as mergeKey,HashValue as HashKey from fullRefreshsourceVw svw
                                Union all
                                select {4}, tob.HashedBusinessKey mergeKey,-1 as HashKey 
                                                from {5} tob
                                LEFT join fullRefreshsourceVw svw on tob.HashedBusinessKey=svw.HashedBusinessKey and tob.LakeValidToTimestamp is NULL 
                                where svw.HashedBusinessKey is null and tob.LakeValidToTimestamp is NULL 
                                ) svw  ON svw.MergeKey =tob.HashedBusinessKey 

                                WHEN MATCHED AND svw.HashKey!=tob.HashValue  AND tob.LakeValidToTimestamp is NULL
                                THEN UPDATE  SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                                -- WHEN MATCHED AND s.HashBussinessKeyValue=s.HashBussinessKeyValue  
                                WHEN NOT MATCHED THEN INSERT * '''.format(targetObjectName,selectColumns,targetObjectName,selectColumns,destColumns,targetObjectName,str(lastUpdatedTimestamp),lastUpdateDate,str(lastUpdatedTimestamp),batchId)
        spark.sql(mergeCommand)
        logTaskProgress(cursor,batchTaskId,"End of applyScdType2onDest function")
   #print(mergeCommand)

   
  except Exception as e:
    errorMessage="Exception occurred when executing applyScdType2onDest function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False
    

# COMMAND ----------

# DBTITLE 1,Function: applyScdType2onDestWithDelete To apply scd type 2 dimensions
def applyScdType2onDestWithDelete(sourceDF,targetObjectName,columnList,currentTs,cursor,batchTaskId,batchId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):  #Function to apply scd type 2 on the tables
  try:
    logTaskProgress(cursor,batchTaskId,"Start of applyScdType2onDestWithDelete function")
    #Create the temporary view for source dataframe and active Destination tables
    rc=sourceDF.rdd.isEmpty()
    if rc:
        logTaskProgress(cursor,batchTaskId,"Source dataframe is empty, no further work required")
    else:
        lastUpdateDate= int(currentTs.strftime('%Y%m%d'))
        lastUpdatedTimestamp=currentTs
        sourceDF.createOrReplaceTempView('fullRefreshsourceVw')

        #Extract the columns and add the alias to the column names, svw used for the source and tob is used for the destination
        selectColumns=",".join(map(str,['svw.'+x[0] for x in  columnList]))
        destColumns=",".join(map(str,['tob.'+x[0] for x in  columnList]))
        columnNameList=list(map(lambda x: x[0],columnList))
        #Prepare the merge command with the sub query to have the updated rows from source as inserted columns in the destination, Merge and Haskey are generated from the HashBussinessKey and Hashvalue columns.
        if 'HashedPartitionKey' in columnNameList:
          mergeCommand='''MERGE INTO {0} tob 
                          USING (
                            --get existing rows to be re-inserted
                                select {1}, CASE WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey 
                                            ELSE Null 
                                            END as mergeKey, svw.HashValue as HashKey 
                                from {2} tob
                                  join fullRefreshsourceVw svw on tob.HashedPartitionKey=svw.HashedPartitionKey 
                                                                and tob.HashedBusinessKey=svw.HashedBusinessKey 
                                                                and tob.HashValue<>svw.HashValue
                                where tob.LakeValidToTimestamp is NULL 
                                    and tob.lakeDeletedTimestamp is NULL
                                Union all
                           --get existing rows to be re-inserted, which were deleted (hash value not in join)
                                select {1}, CASE WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey 
                                            ELSE Null 
                                            END as mergeKey, svw.HashValue as HashKey  
                                from {2} tob
                                  join fullRefreshsourceVw svw on tob.HashedPartitionKey=svw.HashedPartitionKey 
                                                                  and tob.HashedBusinessKey=svw.HashedBusinessKey 
                                where tob.LakeValidToTimestamp is NULL 
                                    and tob.lakeDeletedTimestamp is Not NULL
                                union all
                            --get rows to be closed
                                select {3},HashedBusinessKey as mergeKey,HashValue as HashKey 
                                from fullRefreshsourceVw svw
                                Union all
                            --get rows to be marked as deleted
                                select {4}, tob.HashedBusinessKey mergeKey,-1 as HashKey 
                                from {5} tob
                                  left join fullRefreshsourceVw svw on tob.HashedPartitionKey=svw.HashedPartitionKey 
                                                                    and tob.HashedBusinessKey=svw.HashedBusinessKey 
                                where svw.HashedBusinessKey is NULL
                                    and tob.lakeDeletedTimestamp is NULL
                                    and tob.LakeValidToTimestamp is Null
                          ) svw  ON svw.MergeKey = tob.HashedBusinessKey and tob.HashedPartitionKey = svw.HashedPartitionKey
                          --mark as deleted
                          WHEN MATCHED AND svw.HashKey = -1 And tob.lakeDeletedTimestamp Is Null AND tob.LakeValidToTimestamp is NULL
                            THEN UPDATE SET tob.lakeDeletedTimestamp='{6}',tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --close normal records
                          WHEN MATCHED AND svw.HashKey!=tob.HashValue AND tob.LakeValidToTimestamp is NULL And tob.lakeDeletedTimestamp is null and svw.HashKey!=-1		
                            THEN UPDATE SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --close deleted records
                          WHEN MATCHED AND tob.LakeValidToTimestamp is NULL and tob.lakeDeletedTimestamp is not null
                            THEN UPDATE SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --new rows
                          WHEN NOT MATCHED 
                            THEN INSERT * '''.format(targetObjectName,selectColumns,targetObjectName,selectColumns,destColumns,targetObjectName,str(lastUpdatedTimestamp),lastUpdateDate,str(lastUpdatedTimestamp),batchId)
        else:
          mergeCommand='''MERGE INTO {0} tob 
                          USING (
                            --get existing rows to be re-inserted
                                select {1}, CASE WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey 
                                            ELSE Null 
                                            END as mergeKey, svw.HashValue as HashKey 
                                from {2} tob
                                  join fullRefreshsourceVw svw on tob.HashedBusinessKey=svw.HashedBusinessKey 
                                                                and tob.HashValue<>svw.HashValue
                                where tob.LakeValidToTimestamp is NULL 
                                    and tob.lakeDeletedTimestamp is NULL
                                Union all
                           --get existing rows to be re-inserted, which were deleted (hash value not in join)
                                select {1}, CASE WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey 
                                            ELSE Null 
                                            END as mergeKey, svw.HashValue as HashKey  
                                from {2} tob
                                  join fullRefreshsourceVw svw on tob.HashedBusinessKey=svw.HashedBusinessKey 
                                where tob.LakeValidToTimestamp is NULL 
                                    and tob.lakeDeletedTimestamp is Not NULL
                                union all
                            --get rows to be closed
                                select {3},HashedBusinessKey as mergeKey,HashValue as HashKey 
                                from fullRefreshsourceVw svw
                                Union all
                            --get rows to be marked as deleted
                                select {4}, tob.HashedBusinessKey mergeKey,-1 as HashKey 
                                from {5} tob
                                  left join fullRefreshsourceVw svw on tob.HashedBusinessKey=svw.HashedBusinessKey 
                                where svw.HashedBusinessKey is NULL
                                    and tob.lakeDeletedTimestamp is NULL
                                    and tob.LakeValidToTimestamp is Null
                          ) svw  ON svw.MergeKey = tob.HashedBusinessKey
                          --mark as deleted
                          WHEN MATCHED AND svw.HashKey = -1 And tob.lakeDeletedTimestamp Is Null AND tob.LakeValidToTimestamp is NULL
                            THEN UPDATE SET tob.lakeDeletedTimestamp='{6}',tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --close normal records
                          WHEN MATCHED AND svw.HashKey!=tob.HashValue AND tob.LakeValidToTimestamp is NULL And tob.lakeDeletedTimestamp is null and svw.HashKey!=-1		
                            THEN UPDATE SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --close deleted records
                          WHEN MATCHED AND tob.LakeValidToTimestamp is NULL and tob.lakeDeletedTimestamp is not null
                            THEN UPDATE SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --new rows
                          WHEN NOT MATCHED 
                            THEN INSERT * '''.format(targetObjectName,selectColumns,targetObjectName,selectColumns,destColumns,targetObjectName,str(lastUpdatedTimestamp),lastUpdateDate,str(lastUpdatedTimestamp),batchId)
        
        #print(mergeCommand)
        spark.sql(mergeCommand)
        logTaskProgress(cursor,batchTaskId,"End of applyScdType2onDestWithDelete function")

  except Exception as e:
    errorMessage="Exception occurred when executing applyScdType2onDestWithDelete function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: Apply Scd for Fulldataset in Conformed
                                  
def applyScdType2onDestWithDelete_conf(sourceDF,targetObjectName,columnList,currentTs,batchEffectiveDatetime,cursor,batchTaskId,batchId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):  #Function to apply scd type 2 on the tables
  try:
    logTaskProgress(cursor,batchTaskId,"Start of applyScdType2onDestWithDelete function")
    #Create the temporary view for source dataframe and active Destination tables
    rc=sourceDF.rdd.isEmpty()
    if rc:
        logTaskProgress(cursor,batchTaskId,"Source dataframe is empty, no further work required")
    else:
        lastUpdateDate= int(currentTs.strftime('%Y%m%d'))
        lastUpdatedTimestamp=currentTs
        sourceDF.createOrReplaceTempView('fullRefreshsourceVw')

        #Extract the columns and add the alias to the column names, svw used for the source and tob is used for the destination
        selectColumns=",".join(map(str,['svw.'+x[0] for x in  columnList]))
        destColumns=",".join(map(str,['tob.'+x[0] for x in  columnList]))
        columnNameList=list(map(lambda x: x[0],columnList))
        #Prepare the merge command with the sub query to have the updated rows from source as inserted columns in the destination, Merge and Haskey are generated from the HashBussinessKey and Hashvalue columns.
        if 'HashedPartitionKey' in columnNameList:
          mergeCommand='''MERGE INTO {0} tob 
                          USING (
                            --get existing rows to be re-inserted
                                select {1}, CASE WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey 
                                            ELSE Null 
                                            END as mergeKey, svw.HashValue as HashKey 
                                from {2} tob
                                  join fullRefreshsourceVw svw on tob.HashedPartitionKey=svw.HashedPartitionKey 
                                                                and tob.HashedBusinessKey=svw.HashedBusinessKey 
                                                                and tob.HashValue<>svw.HashValue
                                where tob.LakeValidToTimestamp is NULL 
                                    and tob.lakeDeletedTimestamp is NULL
                                Union all
                           --get existing rows to be re-inserted, which were deleted (hash value not in join)
                                select {1}, CASE WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey 
                                            ELSE Null 
                                            END as mergeKey, svw.HashValue as HashKey  
                                from {2} tob
                                  join fullRefreshsourceVw svw on tob.HashedPartitionKey=svw.HashedPartitionKey 
                                                                  and tob.HashedBusinessKey=svw.HashedBusinessKey 
                                where tob.LakeValidToTimestamp is NULL 
                                    and tob.lakeDeletedTimestamp is Not NULL
                                union all
                            --get rows to be closed
                                select {3},HashedBusinessKey as mergeKey,HashValue as HashKey 
                                from fullRefreshsourceVw svw
                                Union all
                            --get rows to be marked as deleted
                                select {4}, tob.HashedBusinessKey mergeKey,-1 as HashKey 
                                from {5} tob
                                  left join fullRefreshsourceVw svw on tob.HashedPartitionKey=svw.HashedPartitionKey 
                                                                    and tob.HashedBusinessKey=svw.HashedBusinessKey 
                                where svw.HashedBusinessKey is NULL
                                    and tob.lakeDeletedTimestamp is NULL
                                    and tob.LakeValidToTimestamp is Null
                          ) svw  ON svw.MergeKey = tob.HashedBusinessKey and tob.HashedPartitionKey = svw.HashedPartitionKey
                          --mark as deleted
                          WHEN MATCHED AND svw.HashKey = -1 And tob.lakeDeletedTimestamp Is Null AND tob.LakeValidToTimestamp is NULL
                            THEN UPDATE SET tob.lakeDeletedTimestamp='{6}',tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --close normal records
                          WHEN MATCHED AND svw.HashKey!=tob.HashValue AND tob.LakeValidToTimestamp is NULL And tob.lakeDeletedTimestamp is null and svw.HashKey!=-1		
                            THEN UPDATE SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --close deleted records
                          WHEN MATCHED AND tob.LakeValidToTimestamp is NULL and tob.lakeDeletedTimestamp is not null
                            THEN UPDATE SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --new rows
                          WHEN NOT MATCHED 
                            THEN INSERT * '''.format(targetObjectName,selectColumns,targetObjectName,selectColumns,destColumns,targetObjectName,str(batchEffectiveDatetime),lastUpdateDate,str(lastUpdatedTimestamp),batchId)
        else:
          mergeCommand='''MERGE INTO {0} tob 
                          USING (
                            --get existing rows to be re-inserted
                                select {1}, CASE WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey 
                                            ELSE Null 
                                            END as mergeKey, svw.HashValue as HashKey 
                                from {2} tob
                                  join fullRefreshsourceVw svw on tob.HashedBusinessKey=svw.HashedBusinessKey 
                                                                and tob.HashValue<>svw.HashValue
                                where tob.LakeValidToTimestamp is NULL 
                                    and tob.lakeDeletedTimestamp is NULL
                                Union all
                           --get existing rows to be re-inserted, which were deleted (hash value not in join)
                                select {1}, CASE WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey 
                                            ELSE Null 
                                            END as mergeKey, svw.HashValue as HashKey  
                                from {2} tob
                                  join fullRefreshsourceVw svw on tob.HashedBusinessKey=svw.HashedBusinessKey 
                                where tob.LakeValidToTimestamp is NULL 
                                    and tob.lakeDeletedTimestamp is Not NULL
                                union all
                            --get rows to be closed
                                select {3},HashedBusinessKey as mergeKey,HashValue as HashKey 
                                from fullRefreshsourceVw svw
                                Union all
                            --get rows to be marked as deleted
                                select {4}, tob.HashedBusinessKey mergeKey,-1 as HashKey 
                                from {5} tob
                                  left join fullRefreshsourceVw svw on tob.HashedBusinessKey=svw.HashedBusinessKey 
                                where svw.HashedBusinessKey is NULL
                                    and tob.lakeDeletedTimestamp is NULL
                                    and tob.LakeValidToTimestamp is Null
                          ) svw  ON svw.MergeKey = tob.HashedBusinessKey
                          --mark as deleted
                          WHEN MATCHED AND svw.HashKey = -1 And tob.lakeDeletedTimestamp Is Null AND tob.LakeValidToTimestamp is NULL
                            THEN UPDATE SET tob.lakeDeletedTimestamp='{6}',tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --close normal records
                          WHEN MATCHED AND svw.HashKey!=tob.HashValue AND tob.LakeValidToTimestamp is NULL And tob.lakeDeletedTimestamp is null and svw.HashKey!=-1		
                            THEN UPDATE SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --close deleted records
                          WHEN MATCHED AND tob.LakeValidToTimestamp is NULL and tob.lakeDeletedTimestamp is not null
                            THEN UPDATE SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --new rows
                          WHEN NOT MATCHED 
                            THEN INSERT * '''.format(targetObjectName,selectColumns,targetObjectName,selectColumns,destColumns,targetObjectName,str(batchEffectiveDatetime),lastUpdateDate,str(lastUpdatedTimestamp),batchId)
        
        #print(mergeCommand)
        spark.sql(mergeCommand)
        logTaskProgress(cursor,batchTaskId,"End of applyScdType2onDestWithDelete function")

  except Exception as e:
    errorMessage="Exception occurred when executing applyScdType2onDestWithDelete function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: applyScdType2onDestWithDelete_CDC To apply scd type 2 for CDC data from RawDLT
def applyScdType2onDestWithDelete_CDC(sourceDF,targetObjectName,columnList,currentTs,cursor,batchTaskId,batchId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):  #Function to apply scd type 2 on the tables
  try:
    logTaskProgress(cursor,batchTaskId,"Start of applyScdType2onDestWithDelete_CDC function")
    #Create the temporary view for source dataframe and active Destination tables
    rc=sourceDF.rdd.isEmpty()
    if rc:
        logTaskProgress(cursor,batchTaskId,"Source dataframe is empty, no further work required")
    else:
        lastUpdateDate= int(currentTs.strftime('%Y%m%d'))
        lastUpdatedTimestamp=currentTs
        sourceDF.createOrReplaceTempView('fullRefreshsourceVw')

        #Extract the columns and add the alias to the column names, svw used for the source and tob is used for the destination
        selectColumns=",".join(map(str,['svw.'+x[0] for x in  columnList]))
        destColumns=",".join(map(str,['tob.'+x[0] for x in  columnList]))
        columnNameList=list(map(lambda x: x[0],columnList))
        #Prepare the merge command with the sub query to have the updated rows from source as inserted columns in the destination, Merge and Haskey are generated from the HashBussinessKey and Hashvalue columns.
        if 'HashedPartitionKey' in columnNameList:
          mergeCommand='''MERGE INTO {0} tob 
                          USING (
                            --get existing rows to be re-inserted
                                select {1}, CASE WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey 
                                            ELSE Null 
                                            END as mergeKey, svw.HashValue as HashKey 
                                from {2} tob
                                  join fullRefreshsourceVw svw on tob.HashedPartitionKey=svw.HashedPartitionKey 
                                                                and tob.HashedBusinessKey=svw.HashedBusinessKey 
                                                                and tob.HashValue<>svw.HashValue
                                where tob.LakeValidToTimestamp is NULL 
                                    and tob.lakeDeletedTimestamp is NULL
                                Union all
                           --get existing rows to be re-inserted, which were deleted (hash value not in join)
                                select {1}, CASE WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey 
                                            ELSE Null 
                                            END as mergeKey, svw.HashValue as HashKey  
                                from {2} tob
                                  join fullRefreshsourceVw svw on tob.HashedPartitionKey=svw.HashedPartitionKey 
                                                                  and tob.HashedBusinessKey=svw.HashedBusinessKey 
                                where tob.LakeValidToTimestamp is NULL 
                                    and tob.lakeDeletedTimestamp is Not NULL
                                union all
                            --get rows to be closed
                                select {3},HashedBusinessKey as mergeKey,HashValue as HashKey 
                                from fullRefreshsourceVw svw
                                Union all
                            --get rows to be marked as deleted
                                select {4}, tob.HashedBusinessKey mergeKey,-1 as HashKey 
                                from {5} tob
                                  left join fullRefreshsourceVw svw on tob.HashedPartitionKey=svw.HashedPartitionKey 
                                                                    and tob.HashedBusinessKey=svw.HashedBusinessKey 
                                where svw.HashedBusinessKey is NULL
                                    and tob.lakeDeletedTimestamp is NULL
                                    and tob.LakeValidToTimestamp is Null
                          ) svw  ON svw.MergeKey = tob.HashedBusinessKey and tob.HashedPartitionKey = svw.HashedPartitionKey
                          --mark as deleted
                          WHEN MATCHED AND svw.HashKey = -1 And tob.lakeDeletedTimestamp Is Null AND tob.LakeValidToTimestamp is NULL
                            THEN UPDATE SET tob.lakeDeletedTimestamp='{6}',tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --close normal records
                          WHEN MATCHED AND svw.HashKey!=tob.HashValue AND tob.LakeValidToTimestamp is NULL And tob.lakeDeletedTimestamp is null and svw.HashKey!=-1		
                            THEN UPDATE SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --close deleted records
                          WHEN MATCHED AND tob.LakeValidToTimestamp is NULL and tob.lakeDeletedTimestamp is not null
                            THEN UPDATE SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --new rows
                          WHEN NOT MATCHED 
                            THEN INSERT * '''.format(targetObjectName,selectColumns,targetObjectName,selectColumns,destColumns,targetObjectName,str(lastUpdatedTimestamp),lastUpdateDate,str(lastUpdatedTimestamp),batchId)
        else:
          mergeCommand='''MERGE INTO {0} tob 
                          USING (
                            --get existing rows to be re-inserted
                                select {1}, CASE WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey 
                                            ELSE Null 
                                            END as mergeKey, svw.HashValue as HashKey 
                                from {2} tob
                                  join fullRefreshsourceVw svw on tob.HashedBusinessKey=svw.HashedBusinessKey 
                                                                and tob.HashValue<>svw.HashValue
                                where tob.LakeValidToTimestamp is NULL 
                                    and tob.lakeDeletedTimestamp is NULL
                                Union all
                           --get existing rows to be re-inserted, which were deleted (hash value not in join)
                                select {1}, CASE WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey 
                                            ELSE Null 
                                            END as mergeKey, svw.HashValue as HashKey  
                                from {2} tob
                                  join fullRefreshsourceVw svw on tob.HashedBusinessKey=svw.HashedBusinessKey 
                                where tob.LakeValidToTimestamp is NULL 
                                    and tob.lakeDeletedTimestamp is Not NULL
                                union all
                            --get rows to be closed
                                select {3},HashedBusinessKey as mergeKey,HashValue as HashKey 
                                from fullRefreshsourceVw svw
                                Union all
                            --get rows to be marked as deleted
                                select {4}, tob.HashedBusinessKey mergeKey,-1 as HashKey 
                                from {5} tob
                                  left join fullRefreshsourceVw svw on tob.HashedBusinessKey=svw.HashedBusinessKey 
                                where svw.HashedBusinessKey is NULL
                                    and tob.lakeDeletedTimestamp is NULL
                                    and tob.LakeValidToTimestamp is Null
                          ) svw  ON svw.MergeKey = tob.HashedBusinessKey
                          --mark as deleted
                          WHEN MATCHED AND svw.HashKey = -1 And tob.lakeDeletedTimestamp Is Null AND tob.LakeValidToTimestamp is NULL
                            THEN UPDATE SET tob.lakeDeletedTimestamp='{6}',tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --close normal records
                          WHEN MATCHED AND svw.HashKey!=tob.HashValue AND tob.LakeValidToTimestamp is NULL And tob.lakeDeletedTimestamp is null and svw.HashKey!=-1		
                            THEN UPDATE SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --close deleted records
                          WHEN MATCHED AND tob.LakeValidToTimestamp is NULL and tob.lakeDeletedTimestamp is not null
                            THEN UPDATE SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                          --new rows
                          WHEN NOT MATCHED 
                            THEN INSERT * '''.format(targetObjectName,selectColumns,targetObjectName,selectColumns,destColumns,targetObjectName,str(lastUpdatedTimestamp),lastUpdateDate,str(lastUpdatedTimestamp),batchId)
        
        print(mergeCommand)
        spark.sql(mergeCommand)
        logTaskProgress(cursor,batchTaskId,"End of applyScdType2onDestWithDelete_CDC function")

  except Exception as e:
    errorMessage="Exception occurred when executing applyScdType2onDestWithDelete_CDC function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: applySCDType2onDestForBatchLoad To apply scd type 2 dimensions based on batch effective date
def applySCDType2onDestForBatchLoad(sourceDF,targetObjectName,columnList,batchEffectiveDatetime,currentTs,cursor,batchTaskId,batchId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    logTaskProgress(cursor,batchTaskId,"Start of applySCDType2onDestForBatchLoad function")
    #Create the temporary view for source dataframe and active Destination tables
    rc=sourceDF.rdd.isEmpty()    
    if rc:
        logTaskProgress(cursor,batchTaskId,"Source dataframe is empty, no further work required")
    else:
        #local variables
        lastUpdateDate= int(currentTs.strftime('%Y%m%d'))
        lastUpdatedTimestamp=currentTs

        #Extract the columns and add the alias to the column names  , svw used for the source and tob is used for the destiantion
        selectColumns=",".join(map(str,['svw.'+x[0] for x in  columnList]))
        destColumns=",".join(map(str,['tob.'+x[0] for x in  columnList]))

        #Create source view from DF
        sourceDF.createOrReplaceTempView('sourceVw')      
       

        #Prepare the merge command with the sub query to have the new changes and current active rows from target table, MergeKey and Hashkey are generated from the HashBussinessKey and Hashvalue columns.
        #It is expected to have only one row per HBK in sourceVw
        mergeCommand='''MERGE INTO {0} tob
                        USING (
                              ---get new records / to be maked as deleted
                              select {1}
                                  ,svw.HashedBusinessKey as mergeKey
                                  ,svw.HashValue as HashKey
                                  ,0 as ExistingRecord
                              from sourceVw svw
                              --get existing rows to be closed/deleted
                              Union all
                              select {2}
                                  ,tob.HashedBusinessKey as mergeKey
                                  ,tob.HashValue as HashKey
                                  ,1 ExistingRecord
                              from {3} tob
                              inner join sourceVw svw on 
                                                      --tob.HashedPartitionKey = svw.HashedPartitionKey
                                                      --and 
                                                      tob.HashedBusinessKey = svw.HashedBusinessKey
													  --existing records to be closed
                                                      and ((tob.HashValue != svw.HashValue) 
													  --mark existing deleted records as closed
													  or (svw.lakeDeletedTimestamp IS NULL and tob.lakeDeletedTimestamp is NOT NULL))
                              where tob.LakeValidToTimestamp is NULL  
                              ) svw ON 
                                    --tob.HashedPartitionKey = svw.HashedPartitionKey
                                    --AND 
                                    tob.HashedBusinessKey = svw.mergeKey 
									AND (tob.HashValue= svw.HashKey) 
                                    AND ((svw.lakeDeletedTimestamp IS NULL AND tob.lakeDeletedTimestamp IS NULL) OR (svw.lakeDeletedTimestamp IS NOT NULL AND tob.lakeDeletedTimestamp IS NULL) OR (svw.lakeDeletedTimestamp IS NOT NULL AND tob.lakeDeletedTimestamp IS NOT NULL))
                                    AND tob.LakeValidToTimestamp is NULL  ---- filter for Live records from Target bug fix 51744 
                        --mark as deleted
                        WHEN MATCHED AND svw.lakeDeletedTimestamp Is Not Null AND tob.LakeValidToTimestamp is NULL and tob.lakeDeletedTimestamp IS NULL 
                          THEN UPDATE SET tob.lakeDeletedTimestamp='{4}',tob.lakeLastUpdateDate={5},tob.lakeLastUpdateTimestamp='{6}',tob.lakeLastUpdatedBatchID={7}
                        --close records
                        WHEN MATCHED AND tob.LakeValidToTimestamp Is Null AND svw.ExistingRecord = 1
                          THEN UPDATE SET tob.LakeValidToTimestamp='{4}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={5},tob.lakeLastUpdateTimestamp='{6}',tob.lakeLastUpdatedBatchID={7}                        
                        --new records
                        WHEN NOT MATCHED THEN INSERT * '''.format(targetObjectName,selectColumns,destColumns,targetObjectName,str(batchEffectiveDatetime),lastUpdateDate,str(lastUpdatedTimestamp),batchId)
        spark.sql(mergeCommand)
        #print(mergeCommand)
        logTaskProgress(cursor,batchTaskId,"End of applySCDType2onDestForBatchLoad function")

  except Exception as e:
    errorMessage="Exception occurred when executing applySCDType2onDestForBatchLoad function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

def applySCDType2onDestForIncrSourceLoad(sourceDF,targetObjectName,columnList,currentTs,cursor,batchTaskId,batchId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):

  try:
    logTaskProgress(cursor,batchTaskId,"Start of applySCDType2onDestForIncrSourceLoad function")
    #Create the temporary view for source dataframe and active Destination tables
    rc=sourceDF.rdd.isEmpty()
    if rc:
        logTaskProgress(cursor,batchTaskId,"Source dataframe is empty, no further work required")
    else:
        lastUpdateDate= int(currentTs.strftime('%Y%m%d'))
        lastUpdatedTimestamp=currentTs
        active_destinationDf=spark.read.table(targetObjectName).where('LakeValidToTimestamp is null' )
        active_destinationDf.createOrReplaceTempView('activeDestinationVw')
        sourceDF.createOrReplaceTempView('fullRefreshsourceVw')
        columnNameList=list(map(lambda x: x[0],columnList))

    #     Extract the columns and add the alias to the column names  , svw used for the source and tob is used for the destiantion
        selectColumns=",".join(map(str,['svw.'+x[0] for x in  columnList]))
        destColumns=",".join(map(str,['tob.'+x[0] for x in  columnList]))
    #     Prepare the merge command with the sub query to have the updated rows from source as inserted columns in the destination, Merge and Haskey are generated from the HashBussinessKey and Hashvalue columns .
        if 'HashedPartitionKey' in columnNameList:
                mergeCommand='''MERGE INTO {0} tob USING (select {1},CASE WHEN svw.HashedBusinessKey is Null THEN  tob.HashedBusinessKey 
                                ELSE Null 
                                END as mergeKey,svw.HashValue as HashKey 
                                from {2} tob
                                join fullRefreshsourceVw svw on tob.HashedPartitionKey=svw.HashedPartitionKey and tob.HashedBusinessKey=svw.HashedBusinessKey and  tob.HashValue<>svw.HashValue
                                where tob.LakeValidToTimestamp is NULL 
                                Union all
                                select {3},HashedBusinessKey as mergeKey,HashValue as HashKey from fullRefreshsourceVw svw
                            
                                ) svw  ON svw.MergeKey =tob.HashedBusinessKey and tob.HashedPartitionKey=svw.HashedPartitionKey

                                WHEN MATCHED AND svw.HashKey!=tob.HashValue AND tob.LakeValidToTimestamp is NULL
                                THEN UPDATE  SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                                -- WHEN MATCHED AND s.HashBussinessKeyValue=s.HashBussinessKeyValue  
                                WHEN NOT MATCHED THEN INSERT * '''.format(targetObjectName,selectColumns,targetObjectName,selectColumns,destColumns,targetObjectName,str(lastUpdatedTimestamp),lastUpdateDate,str(lastUpdatedTimestamp),batchId)
        else:
                mergeCommand='''MERGE INTO {0} tob USING (select {1},CASE WHEN svw.HashedBusinessKey is Null THEN  tob.HashedBusinessKey 
                                ELSE Null 
                                END as mergeKey,svw.HashValue as HashKey 
                                from {2} tob
                                join fullRefreshsourceVw svw on  tob.HashedBusinessKey=svw.HashedBusinessKey and  tob.HashValue<>svw.HashValue
                                where tob.LakeValidToTimestamp is NULL 
                                Union all
                                select {3},HashedBusinessKey as mergeKey,HashValue as HashKey from fullRefreshsourceVw svw
                             
                                ) svw  ON svw.MergeKey =tob.HashedBusinessKey 

                                WHEN MATCHED AND svw.HashKey!=tob.HashValue  AND tob.LakeValidToTimestamp is NULL
                                THEN UPDATE  SET tob.LakeValidToTimestamp='{6}',tob.LakeIsActive=False,tob.lakeLastUpdateDate={7},tob.lakeLastUpdateTimestamp='{8}',tob.lakeLastUpdatedBatchID={9}
                                -- WHEN MATCHED AND s.HashBussinessKeyValue=s.HashBussinessKeyValue  
                                WHEN NOT MATCHED THEN INSERT * '''.format(targetObjectName,selectColumns,targetObjectName,selectColumns,destColumns,targetObjectName,str(lastUpdatedTimestamp),lastUpdateDate,str(lastUpdatedTimestamp),batchId)
        spark.sql(mergeCommand)
        logTaskProgress(cursor,batchTaskId,"End of applyScdType2onDest function")
   #print(mergeCommand)

   
  except Exception as e:
    errorMessage="Exception occurred when executing applyScdType2onDest function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: applyScdType2onDestForIncrLoad To apply scd type 2 dimensions for incremental input datasets
def applyScdType2onDestForIncrLoad(sourceDF,targetObjectName,cursor,batchTaskId,batchId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):  
#Function to apply scd type 2 on the tables
  try:
    #Create the temporary view for source dataframe and active Destination tables
    sourceDF.createOrReplaceTempView('incrimentalFullRefreshsourceVw')
    
    #1. If a row exists in conformed with same HBK as a closed row in incremental then copy over the closed timestamp of the oldest record in the view (i.e. join on HBK and validfromTimestamp, and set its is_active flag to false.
    #2. For all other rows in the view, insert them into conformed, including valid from, valid to and is_active flag.
    mergeCommand='''MERGE INTO {0} tob USING incrimentalFullRefreshsourceVw svw ON svw.HashedBusinessKey = tob.HashedBusinessKey AND svw.lakeValidFromTimestamp = tob.lakeValidFromTimestamp 
    
                    WHEN MATCHED AND svw.LakeIsActive==False
                    THEN UPDATE SET tob.LakeValidToTimestamp=svw.lakeValidToTimestamp,tob.LakeIsActive=False,tob.lakeLastUpdateDate=svw.lakeLastUpdateDate,tob.lakeLastUpdateTimestamp=svw.lakeLastUpdateTimestamp,tob.lakeLastUpdatedBatchID={1}
                    -- WHEN MATCHED AND s.HashBussinessKeyValue=s.HashBussinessKeyValue  
                    WHEN NOT MATCHED THEN INSERT * '''.format(targetObjectName,batchId)
    #print(mergeCommand)
    spark.sql(mergeCommand)

  except Exception as e:
    errorMessage="Exception occurred when executing applyScdType2onDest function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: readExcelFileIntoSparkDf
def readExcelFileIntoSparkDf(sourceLocation,sheet_name,usecols,header,skiprows,column_check,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #add prefix dbfs if not exists as is not read using spark library
    if sourceLocation[:5] != '/dbfs':
      sourceLocation = '/dbfs' + sourceLocation

    #create pandas dataframe
    pd_df = pd.read_excel(sourceLocation,
                          sheet_name=sheet_name, 
                          usecols=usecols,
                          header=header,
                          skiprows=skiprows,
                          engine='openpyxl')
    pd_df = pd_df.dropna(how='all')

    #delete the rows with null value in main column of the sheet
    if column_check is not None:
      pd_df = pd_df.dropna(subset=[column_check])

    pd_df=pd_df.applymap(str)
    #print(pd_df.dtypes)

    df = spark.createDataFrame(pd_df)
    #replace invalid chars from columns
    df = df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])
    df = df.select([F.col(col).alias(col.replace('(', '')) for col in df.columns])
    df = df.select([F.col(col).alias(col.replace(')', '')) for col in df.columns])

    logTaskProgress(cursor,batchTaskId,"executed function to read excel file into spark dataframe")
    
    return df
  except Exception as e:
    errorMessage="Exception occurred while reading excel file: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to Establish Synapse SQL Connection
# Function to Establish Synapse SQL Database connection
# This connection should only be used for synapse sql execute, it is not recommended to use this for read/write data 
def sqlDWConn(connectionString,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation,dbConn,dbCursor):  
  try:
    sqlDWConn = pyodbc.connect(connectionString, autocommit = True)
    sqlDWCursor = sqlDWConn.cursor()
    logTaskProgress(dbCursor, batchTaskId, "Established Database connection in the notebook")
    return sqlDWConn,sqlDWCursor
  except Exception as e:
    errorMessage="unable to establish DB connection: " + str(e)   
    assert False 

# COMMAND ----------

# DBTITLE 1,Close Synapse SQL Connection
# Function to Close Synapse SQL Database connection
def sqlDWCloseConn(sqlDWConn,sqlDWCursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation,dbConn,dbCursor):  
  try:
    sqlDWConn.close()
    logTaskProgress(dbCursor, batchTaskId, "Closed synapse sql data warehouse connection")   
  except Exception as e:
    errorMessage="unable to close synapse sql data warehouse connection: " + str(e)   
    assert False 

# COMMAND ----------

# DBTITLE 1,Function: getAllViewDefinitions
#function definition of spGetAllViewDefinitions
def getAllViewDefinitions(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:   
    viewDefintiions=pd.read_sql_query("exec config.usp_get_all_view_definitions",conn)  

    logTaskProgress(cursor,batchTaskId,"executed usp_get_all_view_definitions stored procedure")
    return viewDefintiions
  except Exception as e:
    errorMessage="Exception occured while execution of usp_get_all_view_definitions stored procedure: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: createAlterViews
def createAlterViews(viewDefintionsPandasDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    viewDefintionsDf= convertSinglePandasToSparkDf(viewDefintionsPandasDF,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)     
    for definition in viewDefintionsDf.collect():
      createOrReplaceView (definition[0],definition[1],cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      #print(definition[0])
  except Exception as e:
    errorMessage="Exception occured while execution of createAlterViews function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: getSourceToDestinationHighLevelDetails
#function definition of getSourceToDestinationHighLevelDetails that executes stored procedure and save the result as pandas dataframe
def getSourceToDestinationHighLevelDetails(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #execution of object high and low level stored procedures
    sourceDestinationTableDetails=pd.read_sql_query("exec [config].[usp_get_object_to_object_high_level] {}".format(batchTaskId),conn)   
    logTaskProgress(cursor,batchTaskId,"executed object to object high stored procedures")
    return sourceDestinationTableDetails
  except Exception as e:
    errorMessage="Exception occured while execution of object high stored procedures: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to execute Synapse SQL
#function definition of spExecHighAndLowLevel that executes stored procedure and save the result as pandas dataframe
def executeSynapseSQL(sqlDWCursor,sqlCursor,sqlCmd,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #execution of synapse sql command
    sqlDWCursor.execute(sqlCmd)  
    logTaskProgress(cursor,batchTaskId,"executed synapse sql command")    
  except Exception as e:
    errorMessage="Exception occured while execution of synapse sql command " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False


# COMMAND ----------

# DBTITLE 1,Function: populateObjectDateAvailability
def populateObjectDateAvailability(viewName,viewTables,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    retVal = False

    #exec proc
    cursor.execute("exec [audit].[usp_populate_object_date_availablity] {0},'{1}','{2}'".format(batchTaskId,viewName,viewTables))

    logTaskProgress(cursor,batchTaskId,"executed function to populate object date availability")
    retVal = True
    
    return retVal
  except Exception as e:
    errorMessage="Exception occured while execution of function to populate object date availability: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: populateObjectDateAvailabilityWithDataCheck
def populateObjectDateAvailabilityWithDataCheck(viewName,viewTables,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    retVal = False

    #exec proc
    dependentTableIsUpdatedDf= pd.read_sql_query("exec [audit].[usp_populate_object_date_availablity_with_data_check] {0},'{1}','{2}'".format(batchTaskId,viewName,viewTables),conn)
    logTaskProgress(cursor,batchTaskId,"executed function to populate object date availability")
    
    if dependentTableIsUpdatedDf['dependentTableIsUpdated'].iloc[0] == 1:
         retVal = True   
   
    return retVal
  except Exception as e:
    errorMessage="Exception occured while execution of function to populate object date availability: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: getViewDefinition
def getViewDefinition(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #exec proc to get view definition in data frame
    pdViewDef = pd.read_sql_query("exec [config].[usp_get_view_definition] {}".format(batchTaskId),conn)

    if (pdViewDef.shape[0] != 0):
        viewDefDf = spark.createDataFrame(pdViewDef)
    else:
        viewDefDf = None
        
    logTaskProgress(cursor,batchTaskId,"executed proc to get view definition")
    
    return viewDefDf
  except Exception as e:
    errorMessage="Exception occured while execution proc to get view definition: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: populateConformedMasterViewDateAvailablity
def populateConformedMasterViewDateAvailablity(cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:
        cursor.execute("exec [audit].[usp_populate_master_view_date_availablity] {0}".format(batchTaskId))
        logTaskProgress(cursor,batchTaskId,"Executed proc usp_populate_master_view_date_availablity")
    except Exception as e:
        errorMessage="Exception occured while execution proc usp_populate_master_view_date_availablity: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False

# COMMAND ----------

# DBTITLE 1,Function:usp_log_file_object_availability_dates
def populateFileObjectDateAvailability(cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:
        cursor.execute("exec [audit].[usp_log_file_object_availability_dates] {0}".format(batchTaskId))
        logTaskProgress(cursor,batchTaskId,"Executed proc usp_log_file_object_availability_dates")
    except Exception as e:
        errorMessage="Exception occured while execution proc usp_log_file_object_availability_dates: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False

# COMMAND ----------

# DBTITLE 1,Function: populateObjectDateAvailability
def populateObjectDateAvailability(viewName,viewTables,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    retVal = False

    #exec proc
    cursor.execute("exec [audit].[usp_populate_object_date_availablity] {0},'{1}','{2}'".format(batchTaskId,viewName,viewTables))

    logTaskProgress(cursor,batchTaskId,"executed function to populate object date availability")
    retVal = True
    
    return retVal
  except Exception as e:
    errorMessage="Exception occured while execution of function to populate object date availability: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: getViewDefinition
def getViewDefinition(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #exec proc to get view definition in data frame
    pdViewDef = pd.read_sql_query("exec [config].[usp_get_view_definition] {}".format(batchTaskId),conn)

    if (pdViewDef.shape[0] != 0):
        viewDefDf = spark.createDataFrame(pdViewDef)
    else:
        viewDefDf = None
        
    logTaskProgress(cursor,batchTaskId,"executed proc to get view definition")
    
    return viewDefDf
  except Exception as e:
    errorMessage="Exception occured while execution proc to get view definition: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Conformed Parameters
#if a varchar column is null from source
missing_string = 'UNKNOWN'
#if an integer column is null from source
missing_integer = -1
#if details are missing from a source
missing_source = 'NOT APPLICABLE'
#if a column is unmapped in MDS
missing_mdm = 'UNMAPPED'
#default value of start date
missing_startdate = '1900-01-01'
#default value of end date
missing_enddate = '9999-12-31'
#default value of stare date as string
missing_startdate_string = '19000101'
#default value of end date as string
missing_enddate_string = '99991231'

# COMMAND ----------

# DBTITLE 1,function: calculatelakeDeletedTimestamp
def calculatelakeDeletedTimestamp(lakeDeletedTimestamp,batchEffectiveDate):
  #returns batch effective date time when lakeDeletedTimestamp is not null
    lakeDeletedTimestampCalculated = None
    if lakeDeletedTimestamp is not None:
        lakeDeletedTimestampCalculated = batchEffectiveDate      
    return lakeDeletedTimestampCalculated

# COMMAND ----------

# DBTITLE 1,Register calculatelakeDeletedTimestamp
def calculatelakeDeletedTimestampUdfRegistration():
    global calculatelakeDeletedTimestampUdf 
    calculatelakeDeletedTimestampUdf=udf(calculatelakeDeletedTimestamp,TimestampType())

# COMMAND ----------

# DBTITLE 1,Check Directory Is Empty 
def checkDirectory(inputFolderPath,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    DirectoryList=dbutils.fs.ls(inputFolderPath)
    fileList=[]
    for x in DirectoryList:
        fileList.append(x[0].split(":")[1])
    if len(DirectoryList)>0:
        filesToLoad=True
    else:
        filesToLoad=False 
    logTaskProgress(cursor,batchTaskId,f"Checking Directory {inputFolderPath}")
    return filesToLoad,fileList  
  except Exception as e:
    errorMessage=f"Exception occured while CheckDirectory{inputFolderPath}: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
    assert False 

# COMMAND ----------

# DBTITLE 1,Func:selectDestFromSrcWithDelimiter
def selectDestFromSrcWithDelimiter(sourceFormat,sourceHeader,sourceLocation,sourceTableName,withColumnList,customSchema,
                         sourceColumns,whereExpression,delimiter,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #It uses the reduce function to apply the with column recursively and lamda function to add with columns
    #Filter the preselect and postselect withcolumns and use the reduce to create dataframe.
    preSelectWithColumnList=[withColumnTuple for withColumnTuple in withColumnList if withColumnTuple[3]=='preSelect']
    postSelectWithColumnList=[withColumnTuple for withColumnTuple in withColumnList if withColumnTuple[3]=='postSelect']
    #reduce function takes 3 parameters(functiontto iterate,list of iterables,Initialiser.here intiialirser will be the dataframe
      #apply the reduce with preselectwithColumnlist first
      #select the source columns
      #apply the reduce with postSelectWithColumnList using abve selected dataframe as initializer.
    sourceSelectWhereDF=reduce(lambda dataframe,columnList:dataframe.withColumn(columnList[0],eval(columnList[1])),postSelectWithColumnList,
                                reduce(lambda dataframe,columnList:dataframe.withColumn(columnList[0],eval(columnList[1])),preSelectWithColumnList,  
                                                               (spark.read.format(sourceFormat)
                                                                .option("delimiter",delimiter)
                                                                .schema(customSchema)
                                                                .load(sourceLocation)
                                                               .where(whereExpression))).selectExpr(sourceColumns))  
    logTaskProgress(cursor,batchTaskId,"Read from the source based on select expression and where condition")
    return sourceSelectWhereDF  
  except Exception as e:
    errorMessage="Exception occured while reading destination columns from the source: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
    assert False 

# COMMAND ----------

# DBTITLE 1,Func: Create standardised filter view
from pyspark.sql.functions import *
def createStandardisedFilterView(std_object
                                   ,prv_bed
                                   ,current_bed
                                   ,whereExpression
                                   ,chk_column
                                   ,apply_upper_trim
                                   ,filter_list
                                   ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:
#         Read the standardised table and apply incrimental where clause
        df=spark.read.table(std_object).where(whereExpression) 
        upper_chk_column = 'upper_chk_column'
#         df.display()
# Check if apply trim needed if so overwrite the existing column
        if apply_upper_trim=="y":
            df=df.withColumn(upper_chk_column,upper(trim(col(chk_column))))
        else :
            df=df.withColumn(upper_chk_column,col(chk_column))
# Apply Bed where clause to get the snapshot of the data and not later usual part on the lakeValidToTimestamp to get the updated rows
        Bed_whereclause=f"(lakeValidFromTimestamp<='{current_bed}' )"
        df=df.where(Bed_whereclause)
#         df.display()
#         Get the HBKS by applying the later part of the BED and Apply the inverse filter to get the hbks that might have the problem
        hbk_df=(df.where(f"('{current_bed}' < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP()))")
                .where(~(col(upper_chk_column).isin(filter_list)))
                .selectExpr("HashedBusinessKey as changed_HashedBusinessKey"))
#         hbk_df.display()
#         Joined back to df on hbk to get the hbks with issues
        df=df.join(hbk_df,df["HashedBusinessKey"]==hbk_df["changed_HashedBusinessKey"],how="Inner").drop("changed_HashedBusinessKey")
#         df.display()
        # with in a hashbusinesskey latest active row value is not equl to the first closed row value
        window_spec=Window.partitionBy("HashedBusinessKey")
#         print(Bed_whereclause)
# Get the records with multiple HBK records
        df=(df.withColumn("RecordsCount"
                          ,count(lit(1)).over(window_spec))
            .where("RecordsCount>1")
        #     .withColumn("test",row_number().over(window_spec.orderBy('lakeCreatedBatchID')))
#             Rank_1 by applying lakeValidFromtimestamp ascending and rank_2 with descending and filter rank_1=1 or rank_2=1 to get atmost two rows for given hbk
            .withColumn("rank_1",rank().over(window_spec.orderBy(col('lakeValidFromTimestamp'))))
            .withColumn("rank_2",rank().over(window_spec.orderBy(col('lakeValidFromTimestamp').desc())))
            .where("rank_1==1 or rank_2==1")
#             .withColumn("intermchanges",when(lead(col(chk_column),1).over(window_spec.orderBy('lakeCreatedBatchID'))==col(chk_column),lit(0))
#                         .otherwise(lit(1)))
            .withColumn("filterchanges",

                        #whether rank_1=1 means first change to the record  check whether that record for the filter comlum is in supplied list and lead of it means recent change not in filter list
                        when((col("rank_1")==1) & (col(upper_chk_column).isin(filter_list))&(~lead(col(upper_chk_column),1).over(window_spec.orderBy('lakeCreatedBatchID')).isin(filter_list))

                             ,lit(1)).otherwise(lit(0)))
       
        .where(" filterchanges=1")
#             Overwrite the lakeDeletedTimestamp to lakeValidToTimestamp and lakeValidToTimestamp to Null
            .withColumn("lakeDeletedTimestamp",col('lakeValidToTimestamp'))
            .withColumn("lakeValidToTimestamp",lit(None))
            .withColumn("lakeIsActive",lit(True))
            .drop("rank_1","rank_2","RecordsCount","filterchanges","upper_chk_column")
            )
        return df
    except Exception as e:
        errorMessage="Exception occured while execution of function to createStandardisedFilterView: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False

# COMMAND ----------

# DBTITLE 1,Func: Create standardised filter view multiple conditions
from pyspark.sql.functions import *
def createStandardisedMultipleFilterView(std_object
                                   ,prv_bed
                                   ,current_bed
                                   ,whereExpression
                                   ,queryWhereCondition
                                   ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:
#         Read the standardised table and apply incrimental where clause
        df=spark.read.table(std_object).where(whereExpression) 
#         df.display()
# Apply Bed where clause to get the snapshot of the data and not later usual part on the lakeValidToTimestamp to get the updated rows
        Bed_whereclause=f"(lakeValidFromTimestamp<='{current_bed}' )"
        df=df.where(Bed_whereclause)
#         df.display()
#         Get the HBKS by applying the later part of the BED and Apply the inverse filter to get the hbks that might have the problem
        hbk_df=(df.where(f"('{current_bed}' < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP()))")
                .where(~expr(queryWhereCondition))
                .selectExpr("HashedBusinessKey as changed_HashedBusinessKey"))
#         hbk_df.display()
#         Joined back to df on hbk to get the hbks with issues
        df=df.join(hbk_df,df["HashedBusinessKey"]==hbk_df["changed_HashedBusinessKey"],how="Inner").drop("changed_HashedBusinessKey")
#         df.display()
        # with in a hashbusinesskey latest active row value is not equl to the first closed row value
        window_spec=Window.partitionBy("HashedBusinessKey")
#         print(Bed_whereclause)
# Get the records with multiple HBK records
        df=(df.withColumn("RecordsCount"
                          ,count(lit(1)).over(window_spec))
            .where("RecordsCount>1")
        #     .withColumn("test",row_number().over(window_spec.orderBy('lakeCreatedBatchID')))
#             Rank_1 by applying lakeValidFromtimestamp ascending and rank_2 with descending and filter rank_1=1 or rank_2=1 to get atmost two rows for given hbk
            .withColumn("rank_1",rank().over(window_spec.orderBy(col('lakeValidFromTimestamp'))))
            .withColumn("rank_2",rank().over(window_spec.orderBy(col('lakeValidFromTimestamp').desc())))
            .where("rank_1==1 or rank_2==1")
            .withColumn('flag', when(expr(queryWhereCondition) ,lit(1)).otherwise(lit(0)))
            .withColumn("filterchanges",
                        #whether rank_1=1 means first change to the record  check whether that record for the filter comlum is in supplied list and lead of it means recent change not in filter list
                        when((col("rank_1")==1) & (col("flag")==1) & (lead(col('flag'),1).over(window_spec.orderBy('lakeCreatedBatchID')) != col('flag')),lit(1)).otherwise(lit(0)))
       
        .where(" filterchanges=1")
#             Overwrite the lakeDeletedTimestamp to lakeValidToTimestamp and lakeValidToTimestamp to Null
            .withColumn("lakeDeletedTimestamp",col('lakeValidToTimestamp'))
            .withColumn("lakeValidToTimestamp",lit(None))
            .withColumn("lakeIsActive",lit(True))
            .drop("rank_1","rank_2","RecordsCount","filterchanges","flag")
           )
        return df
    except Exception as e:
        errorMessage="Exception occured while execution of function to createStandardisedFilterView: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False

# COMMAND ----------

# DBTITLE 1,Func: Create standardised Grain view
from pyspark.sql.functions import *
def createStandardisedGrainView(
    std_object,
    prv_bed,
    current_bed,
    whereExpression,
    column_list,
    cursor,
    batchTaskId,
    adfPipelineName,
    clusterId,
    notebookName,
    errorLogFileLocation,
    isAboveFilterView
):
    try:
        bed_whereclause = f"(lakeValidFromTimestamp<='{current_bed}' )"
        df = spark.read.table(std_object).where(whereExpression).where(bed_whereclause)
        window_spec = Window.partitionBy("HashedBusinessKey")
        df=(df.withColumn("RecordsCount"
                          ,count(lit(1)).over(window_spec))
            .where("RecordsCount>1"))
        
        
        
#         df=(df.withColumn(
#             "chk_hbk", computeHashValueBigIntUdf(concatenateFunc(column_list))
#         ))
        # prv_bed='2022-11-04 15:29:30.513'

        # std_whereclause=" ((lakeLastUpdateTimestamp > CAST('2022-11-03 00:51:36.446' AS TIMESTAMP)) AND (lakeLastUpdateTimestamp <= CAST('2022-11-17 14:15:46.129' AS TIMESTAMP)))"
        
        # with in a hashbusinesskey latest active row value is not equl to the first closed row value
        df = (
            df
            #     .withColumn("test",row_number().over(window_spec.orderBy('lakeCreatedBatchID')))
            .withColumn(
                "rank_1",
                rank().over(window_spec.orderBy(col("lakeValidFromTimestamp").asc())),
            )
            .withColumn(
                "rank_2", rank().over(window_spec.orderBy(col("lakeValidFromTimestamp").desc()))
            )
            .where("rank_1==1 or rank_2==1")
            .withColumn("chk_hbk", computeHashValueBigIntUdf(concatenateFunc(column_list)))
            .withColumn(
                "filterchanges",
                when(
                    lead(col("chk_hbk"), 1).over(
                        window_spec.orderBy("lakeCreatedBatchID")
                    )
                    != col("chk_hbk"),
                    lit(1),
                ).otherwise(lit(0)),
            )
            # .withColumn('firstClosedRow',lead(chk_column,Window.unboundedPreceding))
            .where("filterchanges=1")
            .withColumn("lakeDeletedTimestamp", when(lit(isAboveFilterView) == 'True', when(col('lakeDeletedTimestamp').isNotNull(),col('lakeDeletedTimestamp')).otherwise(col('lakeValidToTimestamp'))).otherwise(col('lakeValidToTimestamp')))
            .withColumn("lakeValidToTimestamp", lit(None))
            .withColumn("lakeIsActive", lit(True))
            .drop("rank_1","rank_2","RecordsCount","filterchanges","chk_hbk")
        )
        return df
    except Exception as e:
        errorMessage = (
            "Exception occured while execution of function to createStandardisedGrainView: "
            + str(e)
        )
        logError(
            cursor,
            batchTaskId,
            errorMessage,
            adfPipelineName,
            clusterId,
            notebookName,
            errorLogFileLocation,
        )
        assert False

# COMMAND ----------

# DBTITLE 1,Func: Get Previous Bed
def getBatchTaskPreviousBed(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #exec proc to get view definition in data frame
    pbedDf = pd.read_sql_query("exec [config].[usp_get_batch_task_previous_Bed] {}".format(batchTaskId),conn)

    if (pbedDf.shape[0] != 0):
        pvBed= pbedDf.iloc[0]['batch_effective_datetime']
    else:
        pvBed = None
        
    logTaskProgress(cursor,batchTaskId,"executed proc to get previous bed for batch task id")
    
    return pvBed
  except Exception as e:
    errorMessage="Exception occured while execution proc to get previous bed for batch task id: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False