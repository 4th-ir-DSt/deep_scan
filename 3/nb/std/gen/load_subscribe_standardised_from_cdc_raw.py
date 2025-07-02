# Databricks notebook source
# MAGIC %md
# MAGIC ### Completed
# MAGIC 1. Added new column (threshold_mins..) to config.tbl_source
# MAGIC 1. Updated HighLevelStored proc for Standardised Threshold changes
# MAGIC 1. Updated Structure of Standardised Delta Tables in ADB
# MAGIC 1. Metadata changes
# MAGIC    - Update tbl_object.where_clause to have lower case table names same as found in rawDLT
# MAGIC     ```
# MAGIC     update config.tbl_object 
# MAGIC     SET where_clause = lower(where_clause)
# MAGIC     --select lower(where_clause),* from config.tbl_object 
# MAGIC     where 1=1
# MAGIC     AND object_name like '%standardised_Subscribe.dbo_%'
# MAGIC     and is_active=1 
# MAGIC     and where_clause is not null
# MAGIC     ```
# MAGIC    ---
# MAGIC    - tbl_object string_xml_prefix = ¦
# MAGIC    ```
# MAGIC    update config.tbl_object 
# MAGIC    SET xml_string_prefix ='¦'
# MAGIC    -- select xml_string_prefix,* from config.tbl_object 
# MAGIC    where 1=1
# MAGIC    AND object_name= 'Raw_Subscribe.cdc_subscribe'
# MAGIC    ```
# MAGIC    ---
# MAGIC    - lower case dest obj default value   
# MAGIC    ```
# MAGIC    commit tran A
# MAGIC    begin tran A
# MAGIC    update config.tbl_object_to_object_map
# MAGIC    SET destination_object_attribute_default_value = LOWER(destination_object_attribute_default_value)
# MAGIC    --destination_object_attribute_default_value = 'UPPER(TRIM(¦structured_message_map.polid¬))'  --Check with Chitr/Harish if this is needed in Stand
# MAGIC    --Select top 100 * from config.tbl_object_to_object_map
# MAGIC    where 1=1
# MAGIC    and destination_object_definition_id IN
# MAGIC    			(
# MAGIC    			SELECT object_definition_id--,obj.object_name,* 
# MAGIC    			  FROM config.tbl_object_definition def
# MAGIC    			  JOIN config.tbl_object obj on obj.object_id = def.object_id
# MAGIC    			 WHERE obj.[object_id] 
# MAGIC    				IN (
# MAGIC    					select top 1 object_id from config.tbl_object 
# MAGIC    					where is_active=1
# MAGIC    					--and object_name like '%stand%AnlyCdSplt%' 
# MAGIC                        and object_name like '%stand%abbr%' 
# MAGIC    					)
# MAGIC    				)
# MAGIC    AND destination_object_attribute_default_value like '%¦structured_message_map%'
# MAGIC    ```
# MAGIC 1. HVR API checks
# MAGIC 1. dbo_Bkr Obfuscation columns   
# MAGIC 1. Review metadta changes with Ben
# MAGIC 
# MAGIC ### To do
# MAGIC 1. hvr_commit_hour in the where_clause
# MAGIC 1. Review HighLevel proc changes with Chitr, Harish 
# MAGIC 1. Review naming convention of notebook, objects etc

# COMMAND ----------

# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC ## Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_standardised_from_cdc_raw</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data from Raw delta live table with CDC data to standardised delta table (based on notebook load_scd2_dbdt_dbdt)</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Manmohan</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/12/20</td></tr>
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
# MAGIC     <td>2022/12/20</td>
# MAGIC     <td>Manmohan</td>
# MAGIC     <td>Initial Version</td>
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

# DBTITLE 1,Run HVR functions notebook
# MAGIC %run ../../util/spe/hvr_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
    import pandas as pd
    import pyodbc
    from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DecimalType,ShortType
    from datetime import datetime,timedelta
    from pyspark.sql.functions import lit,col,explode,concat_ws,create_map,struct,collect_list,coalesce,sequence 
    from functools import reduce
    from itertools import chain 
    from collections import defaultdict
    import json
    import sys
    from decimal import Decimal
    
    #newly added to this notebook
    from pyspark.sql.functions import row_number, desc, to_timestamp
    from pyspark.sql import Window  
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
  errorMessage = "Exception occurred while getting parameters and initiliasing error log location: " + str(e)
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

# DBTITLE 1,Not reqd anymore: Get Table Name from Batch_task_id
# get the destination standardised table name for the batch_task_id 
# try:
#     query = f"""
# 				SELECT dob.object_name as table_name
#                   FROM audit.tbl_batch_task bt
#                   JOIN config.vw_task_object_map tom 
# 					ON bt.task_id=tom.task_id
#                   JOIN config.tbl_object dob 
# 				    ON tom.object_id=dob.object_id
#                  WHERE batch_task_id = {batchTaskId}
#                    AND lower(designation) = 'destination' 
#             """
    
#     pd_df = pd.read_sql_query(query,conn)
#     sprk_df = spark.createDataFrame(pd_df)
#     getTableName = sprk_df.select("table_name").first()[0]
#     tableName = getTableName.lstrip(getTableName[0:27]).lower()  # trimming the standardised_subscribe.dbo_ from the string
#     print(tableName)
#     logTaskProgress(cursor,batchTaskId,"Succesfully get the outputdatetime from tblObjectAvailability")
# except Exception as e:
#     errorMessage="Exception occured while fetching datetime from tblObjectAvailability " + str(e)
#     logToFile(errorLogFileLocation,errorMessage)
#     assert False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code

# COMMAND ----------

# DBTITLE 1,Get metadata - Execute high and low level stored procedures
#call spExecHighAndLowLevel function to retrieve values from store procedure and store in pandas dataframe                                         
try:        
    sourceDestinationTableDetails,sourceDestinationFieldsDetails = spExecHighAndLowLevel_HVR(conn
                                                                                       ,cursor
                                                                                       ,batchTaskId
                                                                                       ,adfPipelineName
                                                                                       ,clusterId
                                                                                       ,notebookName
                                                                                       ,errorLogFileLocation)

    #Replace the nulls with 'none' as this sp always returns single record no need to specify schema
    sourceDestinationTableDetails=sourceDestinationTableDetails.fillna('none')
    logTaskProgress(cursor,batchTaskId,"Executed high and low level store procedures")
except Exception as e:
    errorMessage="Exception occurred while execution of object high and low level stored procedures: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Specify schema For High Level and Low Level store proc by calling funciton
#Schema for High level stor proc as pandas stores different null values for different types having an issue while conveting to spark df for infering types for null columns.
sourceDestinationFieldsDetailsSchema = getStructForHighAndLowLevelDf()
logTaskProgress(cursor,batchTaskId,"Got the schema for destination field details")

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

# display(srcAndDesTableDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * 
# MAGIC -- FROM raw_subscribe.cdc_subscribe
# MAGIC -- WHERE 
# MAGIC -- (table_name="dbo.ccy") 
# MAGIC -- AND ((hvr_commit_date = 20230103) OR (hvr_commit_date >= 20230104 AND hvr_commit_date < 20230201) OR (hvr_commit_date = 20230201)) AND ((hvr_commit_timestamp > CAST('2023-01-03 11:43:17.999' AS TIMESTAMP)) AND (hvr_commit_timestamp <= CAST('2023-02-01 18:07:06.000' AS TIMESTAMP)))

# COMMAND ----------

# DBTITLE 1,Check if RawDlt has data with Where Clause applied
try:
    rawDltHasData = False
    logTaskProgress(cursor,batchTaskId,"Checking if RawDLT has data for the where clause")
    
    #Get Where clause to be applied on Raw DLT
    source_where_clause = srcAndDesTableDF.select("source_where_clause").collect()

    #Build Query string with Where Clause
    rawDltQuery = f"""
        Select *
        from  raw_subscribe.cdc_subscribe
        where {source_where_clause[0][0]}
    """

    #Execute rawDLT query
    rawDltWithWhereDF = spark.sql(rawDltQuery)

    # check if source RawDLT has data for the Where Clause
    rawDltHasData = not(rawDltWithWhereDF.rdd.isEmpty())
    print(rawDltHasData)
except Exception as e:
    errorMessage="Unable to check if RawDLT has data for the where clause: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False    

# COMMAND ----------

# display(rawDltWithWhereDF)

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
  jsonStartString = srcDesDict['source_XML_string_prefix']
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
  primaryKeyFieldsList=srcAndDesFieldsDF.where("primary_key_order is not null").orderBy('primary_key_order').agg(collect_list(srcAndDesFieldsDF.destination_object_attribute_name)  ).collect()[0][0]
  type2FieldsList=srcAndDesFieldsDF.where("track_type_2_changes>0").orderBy('destination_object_attribute_order').agg(collect_list(srcAndDesFieldsDF.destination_object_attribute_name)).collect()[0][0]

  datetime_from = srcDesDict['datetime_from']
  datetime_to   = srcDesDict['datetime_to']

  logTaskProgress(cursor,batchTaskId,"Got dataframe values into variables")
except Exception as e:
  errorMessage="Unable to retrieve the dataframe values as variables: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# for _ in sourceColumns:
#     print(_)

# COMMAND ----------

# DBTITLE 1,Get Pii Hash Details And Initialize Pii Hash Variable Expressions
try:
  if calculatePiiHash==1 and rawDltHasData:
    try:
      #'target_column_name', 'expression', 'computed_type', 'source_column', 'target_column_type', 'source_column_type'
      objectPiiHashDetailsDf=pd.read_sql_query("exec config.usp_get_object_pii_hash_details {0},{1},{2},{3}".format(targetObjectId,sourceId,'['+targetObjectName+']',batchId),conn)
      logTaskProgress(cursor,batchTaskId,"Executed pii hash details store procedure")
    except Exception as e:
      errorMessage="Unable get the hash details from database: " + str(e)
      logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      assert False  
      
    try:
      #These are the variables to be used from metatdata,Should match the metadataname
      #Get the Pii hash variable functions
     
      PiiHashVersion,PiiHashColumnFunctionList,PiiHashTraceabilityColumnFunctionList=piiGetHashVariables(objectPiiHashDetailsDf
                                                                                                                ,batchId
                                                                                                                ,cursor
                                                                                                                ,batchTaskId
                                                                                                                ,adfPipelineName
                                                                                                                ,clusterId
                                                                                                                ,notebookName
                                                                                                                ,errorLogFileLocation)
      logTaskProgress(cursor,batchTaskId,"pii hash variables initialized")
    except Exception as e:
      errorMessage="Unable to Get the Pii hash variable functions: " + str(e)
      logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      assert False   
      
except Exception as e:
  errorMessage="Unable to run pii hash task: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False        

# COMMAND ----------

# DBTITLE 1,Register Hash Functions
try:
    if rawDltHasData:
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
    if rawDltHasData:
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
    if rawDltHasData:
        #Get With Column Expression and Select Expression Details by calling the function
        withExpressionListFinal,columnListFinal=getWithExpressionsAndSelectList(columnList
                                                                          ,jsonStartString
                                                                          ,cursor
                                                                          ,batchTaskId
                                                                          ,adfPipelineName
                                                                          ,clusterId
                                                                          ,notebookName
                                                                          ,errorLogFileLocation)
        logTaskProgress(cursor,batchTaskId,"with column and select column expressions are created")
        #Prepare select expression
        selectExpression=[item[1] for item in columnListFinal]
        
        # Add fields HVR required for Ordering and sorting
        hvr_fields = ['simple_op_code AS simple_op_code', 'hvr_commit_timestamp AS hvr_commit_timestamp','hvr_int_seq AS hvr_int_seq']
        for _ in hvr_fields:
            if _ not in selectExpression:
                selectExpression.append(_)
        
        #Get join fileds list
        joinFieldsList=[c[1] for c in columnList  if c[2]=='join']
except Exception as e:
    errorMessage="Unable to retrieve with and selectedExpressions: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# withExpressionListFinal HBK, hashVals
# whereExpression filtering
# for _ in selectExpression:: #selectExpression 
#     print(_)

# COMMAND ----------

# DBTITLE 1,Selection of destination columns from source
try:
  #Verify is any join field is there from low level stored procedure
  if rawDltHasData:
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
  errorMessage="Exception occurred while reading destination columns from the source: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
  assert False

# COMMAND ----------

# windowSpec = (Window.partitionBy(primaryKeyFieldsList)
#                          .orderBy(desc("hvr_commit_timestamp"),desc("hvr_int_seq"))
#                       )

# display(sourceSelectWhereDF
#         .withColumn("row_number",row_number().over(windowSpec))
#         .select("row_number","Abbr","Mng","simple_op_code","HashedBusinessKey","hvr_commit_timestamp","hvr_int_seq","TmStp")
#         .where("lower(mng) like '%hvr4%'")
#         .orderBy(desc("hvr_commit_timestamp"),desc("hvr_int_seq"),desc("TmStp")))

# COMMAND ----------

# display(sourceSelectWhereDF)
# rawDltHasData

# COMMAND ----------

# DBTITLE 1,Filter source dataframe records for latest versions
# Filter out older versions of the raw records in the sourceDataframe 
# Filter out certain simple_op_codes? before update?
try:
    if rawDltHasData:
        logTaskProgress(cursor,batchTaskId,"Filtering source dataframe for latest versions")

        ##filter out CDC event types such as before-change 

        # Get active Simple_Op_codes 
        activeSimpleOpCodes = list({v["simple_op_code"] for (k,v) in op_type_dict_extended.items() if v["process"]==1})

        # Apply filter on source df
        sourceSelectWhereDF = sourceSelectWhereDF.filter(
                                sourceSelectWhereDF.simple_op_code.isin(list(activeSimpleOpCodes))
                                )


        ##TODO : Get SOurce OrderBy field list from metadata 

        # Define partiton window clause 
        windowSpec = (Window.partitionBy(primaryKeyFieldsList)
                         .orderBy(desc("hvr_commit_timestamp"),desc("hvr_int_seq"))
                      )

        # Add row_number column 
        tempDf1 = sourceSelectWhereDF.withColumn("row_number",row_number().over(windowSpec))
        # display(newDf)

        # filter records with Row_number =1
        tempDf2 = tempDf1.where(tempDf1["row_number"]=='1')

        # Drop row_number column 
        sourceSelectWhereLatestDF = tempDf2.drop("row_number")
        # display(sourceSelectWhereLatestDF)
except Exception as e:
    errorMessage="Error occured while filtering source dataframe records for latest versions: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Apply ScdType2WithDelete Dimensions
try:
    if rawDltHasData:
        logTaskProgress(cursor,batchTaskId,"Calling Scd2 function")
        applyScdType2onDestWithDelete_CDC(sourceSelectWhereLatestDF,targetObjectName,columnList,currentTs,cursor,batchTaskId,batchId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except Exception as e:
    errorMessage="Unable to apply the scd type 2: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# %sql MERGE INTO standardised_Subscribe_HVR.dbo_Abbreviation tob USING (
#   --get existing rows to be re-inserted
#   select
#     svw.lakeCreatedTimeStamp,
#     svw.hvr_int_seq,
#     svw.lakeValidFromTimestamp,
#     svw.TmStp,
#     svw.lakeCreatedDate,
#     svw.HashedBusinessKey,
#     svw.lakeIsActive,
#     svw.HashValue,
#     svw.lakeLastUpdatedBatchID,
#     svw.lakeLastUpdateDate,
#     svw.hvr_commit_timestamp,
#     svw.simple_op_code,
#     svw.lakeLastUpdateTimeStamp,
#     svw.Mng,
#     svw.refresh,
#     svw.Abbr,
#     svw.sourceID,
#     svw.lakeDeletedTimestamp,
#     svw.lakeCreatedBatchID,
#     svw.lakeValidToTimestamp,
#     CASE
#       WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey
#       ELSE Null
#     END as mergeKey,
#     svw.HashValue as HashKey
#   from
#     standardised_Subscribe_HVR.dbo_Abbreviation tob
#     join fullRefreshsourceVw svw on tob.HashedBusinessKey = svw.HashedBusinessKey
#     and tob.HashValue <> svw.HashValue
#   where
#     tob.LakeValidToTimestamp is NULL
#     and tob.lakeDeletedTimestamp is NULL
#   Union all
#     --get existing rows to be re-inserted, which were deleted (hash value not in join)
#   select
#     svw.lakeCreatedTimeStamp,
#     svw.hvr_int_seq,
#     svw.lakeValidFromTimestamp,
#     svw.TmStp,
#     svw.lakeCreatedDate,
#     svw.HashedBusinessKey,
#     svw.lakeIsActive,
#     svw.HashValue,
#     svw.lakeLastUpdatedBatchID,
#     svw.lakeLastUpdateDate,
#     svw.hvr_commit_timestamp,
#     svw.simple_op_code,
#     svw.lakeLastUpdateTimeStamp,
#     svw.Mng,
#     svw.refresh,
#     svw.Abbr,
#     svw.sourceID,
#     svw.lakeDeletedTimestamp,
#     svw.lakeCreatedBatchID,
#     svw.lakeValidToTimestamp,
#     CASE
#       WHEN svw.HashedBusinessKey is Null THEN tob.HashedBusinessKey
#       ELSE Null
#     END as mergeKey,
#     svw.HashValue as HashKey
#   from
#     standardised_Subscribe_HVR.dbo_Abbreviation tob
#     join fullRefreshsourceVw svw on tob.HashedBusinessKey = svw.HashedBusinessKey
#   where
#     tob.LakeValidToTimestamp is NULL
#     and tob.lakeDeletedTimestamp is Not NULL
#   union all
#     --get rows to be closed
#   select
#     svw.lakeCreatedTimeStamp,
#     svw.hvr_int_seq,
#     svw.lakeValidFromTimestamp,
#     svw.TmStp,
#     svw.lakeCreatedDate,
#     svw.HashedBusinessKey,
#     svw.lakeIsActive,
#     svw.HashValue,
#     svw.lakeLastUpdatedBatchID,
#     svw.lakeLastUpdateDate,
#     svw.hvr_commit_timestamp,
#     svw.simple_op_code,
#     svw.lakeLastUpdateTimeStamp,
#     svw.Mng,
#     svw.refresh,
#     svw.Abbr,
#     svw.sourceID,
#     svw.lakeDeletedTimestamp,
#     svw.lakeCreatedBatchID,
#     svw.lakeValidToTimestamp,
#     HashedBusinessKey as mergeKey,
#     HashValue as HashKey
#   from
#     fullRefreshsourceVw svw
#   Union all
#     --get rows to be marked as deleted
#   select
#     tob.lakeCreatedTimeStamp,
#     tob.hvr_int_seq,
#     tob.lakeValidFromTimestamp,
#     tob.TmStp,
#     tob.lakeCreatedDate,
#     tob.HashedBusinessKey,
#     tob.lakeIsActive,
#     tob.HashValue,
#     tob.lakeLastUpdatedBatchID,
#     tob.lakeLastUpdateDate,
#     tob.hvr_commit_timestamp,
#     tob.simple_op_code,
#     tob.lakeLastUpdateTimeStamp,
#     tob.Mng,
#     tob.refresh,
#     tob.Abbr,
#     tob.sourceID,
#     tob.lakeDeletedTimestamp,
#     tob.lakeCreatedBatchID,
#     tob.lakeValidToTimestamp,
#     tob.HashedBusinessKey mergeKey,
#     -1 as HashKey
#   from
#     standardised_Subscribe_HVR.dbo_Abbreviation tob
#     left join fullRefreshsourceVw svw on tob.HashedBusinessKey = svw.HashedBusinessKey
#   where
#     svw.HashedBusinessKey is NULL
#     and tob.lakeDeletedTimestamp is NULL
#     and tob.LakeValidToTimestamp is Null
# ) svw ON svw.MergeKey = tob.HashedBusinessKey --mark as deleted
# WHEN MATCHED
# AND svw.HashKey = -1
# And tob.lakeDeletedTimestamp Is Null
# AND tob.LakeValidToTimestamp is NULL THEN
# UPDATE
# SET
#   tob.lakeDeletedTimestamp = '2023-02-01 12:51:30.880000',
#   tob.lakeLastUpdateDate = 20230201,
#   tob.lakeLastUpdateTimestamp = '2023-02-01 12:51:30.880000',
#   tob.lakeLastUpdatedBatchID = 596 --close normal records
#   WHEN MATCHED
#   AND svw.HashKey != tob.HashValue
#   AND tob.LakeValidToTimestamp is NULL
#   And tob.lakeDeletedTimestamp is null
#   and svw.HashKey != -1 THEN
# UPDATE
# SET
#   tob.LakeValidToTimestamp = '2023-02-01 12:51:30.880000',
#   tob.LakeIsActive = False,
#   tob.lakeLastUpdateDate = 20230201,
#   tob.lakeLastUpdateTimestamp = '2023-02-01 12:51:30.880000',
#   tob.lakeLastUpdatedBatchID = 596 --close deleted records
#   WHEN MATCHED
#   AND tob.LakeValidToTimestamp is NULL
#   and tob.lakeDeletedTimestamp is not null THEN
# UPDATE
# SET
#   tob.LakeValidToTimestamp = '2023-02-01 12:51:30.880000',
#   tob.LakeIsActive = False,
#   tob.lakeLastUpdateDate = 20230201,
#   tob.lakeLastUpdateTimestamp = '2023-02-01 12:51:30.880000',
#   tob.lakeLastUpdatedBatchID = 596 --new rows
#   WHEN NOT MATCHED THEN
# INSERT
#   *

# COMMAND ----------

# DBTITLE 1,Mark the dates as loaded into the destination
# Update the open record in objectDatesAvailability with date_times_from/to as used in the where clause
try:
    conn, cursor = markDatesLoaded(targetObjectId
                                ,sourceObjectId
                                ,datetime_from
                                ,datetime_to
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
    errorMessage="Exception occurred when calling the function markDatesLoaded: " + str(e)
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