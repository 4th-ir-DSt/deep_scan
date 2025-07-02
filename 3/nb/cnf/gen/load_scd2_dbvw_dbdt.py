# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_na_dbvw_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data from databricks view to delta</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2021/12/06</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Parameter Name</th>
# MAGIC     <th>Parameter Description</th>
# MAGIC     <th>Example Parameter</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchId</td>
# MAGIC     <td>@batchId to retrieve batch details</td>
# MAGIC     <td>@batchId = 7</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchTaskId</td>
# MAGIC     <td>@batchTaskId to retrieve batchTask details</td>
# MAGIC     <td>@batchTaskId = 10</td>
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
# MAGIC   
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
# MAGIC     <td>13/07/2022</td>
# MAGIC     <td>HARISH N</td>
# MAGIC     <td>
# MAGIC       <br>Removed the create alter views as the views are created by seperate notebook, and we are not creating the views every data load</br>
# MAGIC     <br>Bdx Phase A integration changes , Included the view recreation changes</br></td>
# MAGIC   </tr>  
# MAGIC    <tr>
# MAGIC     <td>03/02/2023</td>
# MAGIC     <td>HARISH N</td>
# MAGIC     <td> Added code to Remove Duplicate values for the HBK with Delete and Active Row</td>
# MAGIC   </tr>  
# MAGIC   <tr>
# MAGIC     <td>23/02/2023</td>
# MAGIC     <td>HARISH N</td>
# MAGIC     <td> Modified the dependent tables updated funciton to inline with phase A and populating BED in the marke dates loaded function</td>
# MAGIC   </tr>  
# MAGIC   
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

# DBTITLE 1,Run log function notebook
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run standard function notebook
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DecimalType,ShortType
  from datetime import datetime,timedelta
  from pyspark.sql.functions import lit,col,explode,concat_ws,create_map,struct,collect_list,coalesce,rank,row_number
  from pyspark.sql import Window
  from functools import reduce
  from itertools import chain 
  from collections import defaultdict
  import json
  import sys
  from decimal import Decimal
except Exception as e:
  errorMessage="Exception occurred while import modules " + str(e)
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
  createdDate=CreatedDate
  lastUpdateDate=createdDate
  #get the hour
  CreatedHour = currentTs.hour
  createdHour = CreatedHour
  createdTimestamp = currentTs
  lastUpdateTimestamp = currentTs
#   createdTimestamp
  lastUpdatedTimestamp = currentTs
  createdTime=currentTs
  #use the same time for the date also
  date=currentTs
#   createdBatchID=
  #PARAMETER FOR logError
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
  
  #variable to retrieve the batch rows loaded or not
  retrieveBatchRowsLoaded = False
  
except Exception as e:
  errorMessage = "Exception occurred while variable initialisation :" + str(e)
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
  lastUpdateBatchId=batchId
  createdBatchId=batchId
  validToTimestamp=None
  deletedTimestamp=None
  isActive=True
  sourceID=sourceId
  BatchID=batchId
  
  #call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')  
except Exception as e:
  errorMessage = "Exception occured while getting parameters and initiliasing error log location: " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn=dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')

#call function sqlDbConn to establish Database connection with given scope and key values
try:
  conn,cursor = sqlDbConn(dbconn,
                          batchTaskId,                          
                          adfPipelineName,
                          clusterId,
                          notebookName,
                          errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Successfully Established SQL Connection")
except Exception as e:
  errorMessage="unable to establish DB connection: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Get the source view definition in a dataframe
try:
    viewDefDf = getViewDefinition(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
    if (viewDefDf != None):
        viewDefDict = viewDefDf.collect()[0].asDict()

        #get values in variables
        viewName = viewDefDict['ViewName']
        viewDefinition = ""
        if "Bordereaux" in viewName:
            viewDefinition = str(viewDefDict['ViewDefinition']).replace(" Standardised_Intrali4444.dbo_Uploads "," Standardised_Intrali4444_dbo_Uploads_tmpvw ")
        else:
            viewDefinition = viewDefDict['ViewDefinition']
        viewTables = viewDefDict['ViewTables']
        
        logTaskProgress(cursor,batchTaskId,"executed function getViewDefinition")
    else:
        logTaskProgress(cursor,batchTaskId,"executed function getViewDefinition but metadata not found for view")
        raise Exception('Error raised - Metadata not found for view definition')
        
except Exception as e:
  errorMessage="Exception occurred when calling function getViewDefinition: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #call function
  dependentTableIsUpdated = populateObjectDateAvailabilityWithDataCheck(viewName,viewTables,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

  logTaskProgress(cursor,batchTaskId,"executed function to populate object date availability")
except Exception as e:
  errorMessage="Exception occured while execution of function to populate object date availability: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Get metadata - Execute high and low level stored procedures
#call spExecHighAndLowLevel function to retrieve values from store procedure and store in pandas dataframe                                         
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
  #sourceDestinationFieldsDetails =sourceDestinationFieldsDetails.fillna('-1')
  logTaskProgress(cursor,batchTaskId,"Executed high and low level store procedures")
except Exception as e:
  errorMessage="Exception occured while execution of object high and low level stored procedures: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Specify schema For High Level and Low Level store proc by calling funciton
#call spExecHighAndLowLevel function to retrieve values from store procedure and store in pandas dataframe                                         
try:        
  #Schema for High level stor proc as pandas stores different null values for different types having an issue while conveting to spark df for infering types for null columns.
  sourceDestinationFieldsDetailsSchema = getStructForHighAndLowLevelDf()
  logTaskProgress(cursor,batchTaskId,"Got the schema for destination field details")
except Exception as e:
  errorMessage="Exception occured while executing function getStructForHighAndLowLevelDf: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 


# COMMAND ----------

# DBTITLE 1,Transform metadata into spark dataframe
#call Transform_pd_df_to_Pyspark_df function to convert pandas to pyspark dataframe and store return values in variables
try:
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
  #the id of the source object taken from teh procedure output
  sourceObjectId = srcDesDict['source_object_id']  
  #the name of the source taken from the procedure output
  sourceTableName = srcDesDict['source_object_name']
#   if "Bordereaux" in sourceTableName:
#     sourceTableName = sourceTableName.replace(".","_")
  #the calculated where expression to be applied in source dataframe
  whereExpression = srcDesDict['source_where_clause']
  #the xml start string to be applied when calculating xml fields
  xmlStartString = srcDesDict['source_XML_string_prefix']
  #the id of the target object taken from the procedure ouptut
  targetObjectId=srcDesDict['destination_object_id']
  #the name of the target table taken from the procedure output
  targetObjectName=srcDesDict['destination_object_name']
  #the format of the target taken from the procedure output
  targetFormat=srcDesDict['destination_format']  
  #Check Target object has Pii attributes
  calculatePiiHash=srcDesDict['calculate_pii_hash']
    
#     pksFieldsList,type2FieldsList
  primaryKeyFieldsList = srcAndDesFieldsDF.where("primary_key_order is not null").orderBy('primary_key_order').agg(collect_list(srcAndDesFieldsDF.destination_object_attribute_name)).collect()[0][0]
  type2FieldsList = srcAndDesFieldsDF.where("track_type_2_changes>0").orderBy('destination_object_attribute_order').agg(collect_list(srcAndDesFieldsDF.destination_object_attribute_name)).collect()[0][0]
    #use batch effective date flag
  useBatchEffectiveDatetime = srcDesDict['use_batch_effective_datetime']
  #get batch effective date
  batchEffectiveDatetime = srcDesDict['batch_effective_datetime']
  batch_effective_datetime= srcDesDict['batch_effective_datetime']
  createdTimestamp = currentTs
  
  logTaskProgress(cursor,batchTaskId,"Got dataframe values into variables")
except Exception as e:
  errorMessage="Unable to retrieve the dataframe values as variables: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Get Previous BED
try:
  prv_bed = getBatchTaskPreviousBed(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"executed function to get previous BED")
except Exception as e:
  errorMessage="Exception occured while execution of function to get previousBed: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Create Filter Standardised View
try:

    if "Bordereaux" in viewName and dependentTableIsUpdated == True:
        std_object='Standardised_Intrali4444.dbo_Uploads'
        current_bed=batchEffectiveDatetime
        chk_column='status'
        apply_upper_trim=""
        status_qry = "select distinct status from standardised_intrali4444.dbo_uploadstatus where description = 'Complete' "
        df = spark.sql(status_qry)
        filter_list= df.rdd.flatMap(lambda x:x).collect() #[filter_list_value,]
        if prv_bed is not None:
            std_df=createStandardisedFilterView(std_object
                                                ,prv_bed
                                                ,current_bed
                                                ,whereExpression
                                                ,chk_column
                                                ,apply_upper_trim
                                                ,filter_list
                                                ,cursor
                                                ,batchTaskId
                                                ,adfPipelineName
                                                ,clusterId
                                                ,notebookName
                                                ,errorLogFileLocation)
            #display(std_df)
            union_df=spark.read.table(std_object).unionByName(std_df)
        else :
            union_df=spark.read.table(std_object)

        union_df.createOrReplaceTempView('Standardised_Intrali4444_dbo_Uploads_tmpvw')
    else:
        logTaskProgress(cursor,batchTaskId,"No need to create a view for {} table as it is not BDX related ".format(viewName))

except Exception as e:
  errorMessage="Exception Occured while creating the filter view " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Register Hash Functions And Create Views
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
     viewDefinition=viewDefinition.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
     createOrReplaceTemporaryView(viewName,viewDefinition,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
     logTaskProgress(cursor,batchTaskId,f"{viewName} View recreated")
     computeHashValueUdfRegistration()
     logTaskProgress(cursor,batchTaskId,"Hash udfs are registered")
except Exception as e:
  errorMessage="Unable register Hash udfs: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Get Column List Details
#call Get_Select_And_With_Column_Details to get Columnlist tuple
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
    columnList = getSelectAndWithColumnDetails(srcAndDesFieldsDF
                                               ,cursor 
                                               ,batchTaskId
                                               ,adfPipelineName
                                               ,clusterId
                                               ,notebookName
                                               ,errorLogFileLocation)
    logTaskProgress(cursor,batchTaskId,"successfully called function getSelectAndWithColumnDetails to retrieve the column list")
except Exception as e:
  errorMessage="Unable to retrieve he column Details for With Expression and select: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Get With And Select Column Expressions
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
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
    
    logTaskProgress(cursor,batchTaskId,"with column and select column expressions are created")
    
except Exception as e:
  errorMessage="Unable to retrieve with and selectedExpressions: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Selection of destination columns from source
# call select_dest_from_src function to select destination column from the source and store return values in a variable.
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
    sourceSelectWhereDF = selectDestFromSrcTable(sourceTableName
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

# DBTITLE 1,Remove Duplicate values for the HBK with Delete and Active Row
try:
  
  if dependentTableIsUpdated == True:
    #Get only active rows if a deleted and active record present for same HBK
    window_spec=Window.partitionBy("HashedBusinessKey")
    sourceSelectWhereDF_rank=(sourceSelectWhereDF
                              .withColumn("RankedHbk"
                                          ,row_number().over(window_spec.orderBy(
                                              coalesce(col('lakeDeletedTimestamp')
                                                      ,lit('9999-12-31T00:00:00.000')
                                                      )
                                             .desc()))))
    sourceSelectWhereDF_rank=(sourceSelectWhereDF_rank
                              .where("RankedHbk=1")
                              .drop("RankedHbk"))
    
    logTaskProgress(cursor,batchTaskId,"Removed Duplicates from soruce view which has deletes and active rows for same HBK")
except Exception as e:
  errorMessage="Exception occured while deleting duplicates : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
  assert False

# COMMAND ----------

# DBTITLE 1,Apply ScdType2 Dimensions
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
      applySCDType2onDestForBatchLoad(sourceSelectWhereDF_rank
                                      ,targetObjectName
                                      ,columnList
                                      ,batchEffectiveDatetime
                                      ,currentTs
                                      ,cursor
                                      ,batchTaskId
                                      ,batchId
                                      ,adfPipelineName
                                      ,clusterId
                                      ,notebookName
                                      ,errorLogFileLocation)
except Exception as e:
  errorMessage="Unable to apply the scd type 2: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Mark the dates as loaded into the destination
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
    conn, cursor = markDatesLoaded(targetObjectId
                                  ,sourceObjectId
                                  ,batchEffectiveDatetime
                                  ,batchEffectiveDatetime
                                  ,dbconn
                                  ,conn
                                  ,cursor
                                  ,batchTaskId
                                  ,adfPipelineName
                                  ,clusterId
                                  ,notebookName
                                  ,errorLogFileLocation)

    logTaskProgress(cursor,batchTaskId,"Loaded dates are marked for destination object")
except Exception as e:
  errorMessage="Exception occurred while marking the dates as loaded: " + str(e)
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