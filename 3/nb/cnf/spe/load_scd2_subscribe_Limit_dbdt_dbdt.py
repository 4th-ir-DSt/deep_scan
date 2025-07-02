# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_limit_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for limit table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Rahul</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/03/23</td></tr>
# MAGIC </table>
# MAGIC 
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
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC     <td></td>
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

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  from pyspark.sql.functions import rank,desc,col,lag, when, lit, concat,row_number,collect_list, coalesce, concat_ws
  from pyspark.sql import Window
  from pyspark.sql.types import LongType,StringType,IntegerType,BooleanType,DecimalType,StructField,StructType,ShortType
  from functools import reduce
  from decimal import *
  from datetime import datetime
  import sys
except Exception as e:
  errorMessage="Exception occured while import modules " + str(e)
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
  
  date=currentTs
  
  #PARAMETER FOR logError
  errorMessage = ''
  adfPipelineName = ''
  clusterId = ''
  notebookName = ''
  batchId = -1
  batchTaskId = -1

  #parameter for log_task_end
  batchTaskSourceRows = 0
  batchTaskRowsLoaded = 0
  batchTaskRejectRows = 0
  batchTaskResult = ''
  batchTaskResultLocation = ''

except Exception as e:
  errorMessage = "Exception occured while variable initialisation :" + str(e)
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
  
  #call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')  
except Exception as e:
  errorMessage = "Exception occured while getting parameters and initiliasing error log location: " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')

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

#----FETCHING ACCOUNTING PERIOD FROM SUBSCRIBE----------#

# src_acc_perd="select accprd from standardised_subscribe.dbo_msgaccprdst where trim(lower(st)) = 'o' AND lower(trim(trncgy))='usm' AND {batch_effective_datetime} >= LakeValidFromTimestamp AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())"

#----FETCHING ACCOUNTING PERIOD FROM MDM----------#

acc_perd_match="select array_join(array_sort(collect_set(Accounting_Period)),',') from standardised_mdm.mdm_accounting_period_mapping where trim(lower(source_name)) = 'subscribe' AND accounting_period_close_date_time is null AND {batch_effective_datetime} >= LakeValidFromTimestamp AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())"

acc_perd_unmatch="select Accounting_Period from standardised_mdm.mdm_accounting_period_mapping where trim(lower(source_name)) = 'subscribe' AND accounting_period_close_date_time is null AND {batch_effective_datetime} >= LakeValidFromTimestamp AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp()) order by Accounting_Period ASC limit 1"


# COMMAND ----------

# DBTITLE 1,Prepare SQL Query
sqlQuery= """
SELECT
Concat(lmt.polid, '_', lmt.unitpsu)                                                              AS Policy_Header_Reference,
Concat(lmt.polid, '_', lmt.unitpsu)                                                              AS Policy_Section_Reference,
COALESCE(NULLIF(upper(Trim(lmt.lmtty)),''), {missing_string})                                    AS Limit_Type,
Concat(lmt.polid, '_', lmt.unitpsu)                                                              AS Coverage_Reference,
Concat(lmt.polid, '_', lmt.unitpsu)                                                              AS Item_Reference,
{missing_mdm}                                                                                    AS Peril_Code,
{missing_mdm}                                                                                    AS Country_Alpha_3_Code,
COALESCE(NULLIF(Trim(ccypsu), ''), {missing_string})                                             AS Limit_Currency,
CAST( CASE
    WHEN Upper(lmtty) = 'LIMIT' and coalesce(main.LmtAmt, 0.00) > 0.00 THEN main.LmtAmt
    ELSE coalesce(lmt.amt, 0.00)
END AS DECIMAL(28,2) )                                                                           AS Limit_Amount,
cast(COALESCE(ourshr / NULLIF(amt,0), 0) as decimal(19,8))                                       AS Limit_Percentage,
CASE
    WHEN Upper(lmtty) = 'EXCESS' THEN COALESCE(NULLIF(Trim(main.exsboa),''), {missing_string})
    WHEN Upper(lmtty) = 'LIMIT' THEN COALESCE(NULLIF(Trim(main.lmtboa),''), {missing_string})
    ELSE {missing_string}
END                                                                                              AS Basis_Of_Attachment_Code,
-------- Updated to 'N' from 'False' as per discussion with Jon
'N'                                                                                              AS Limit_Status,
CASE 
    WHEN trans_dt.lakedeletedtimestamp is not null THEN {missing_startdate} 
    ELSE coalesce(trans_dt.dttm, {missing_startdate})
END                                                                                              AS Source_Transaction_Date,
'N'                                                                                              AS Fire_Following_Indicator,
CASE
    WHEN Upper(lmtty) = 'LIMIT' THEN 'Y' ELSE 'N'
END                                                                                              AS Main_Limit_Indicator,
CASE 
    WHEN left(trim(replace(trans_dt.dttm,'-','')),6) IN ({acc_perd_match_res}) THEN left(trim(replace(trans_dt.dttm,'-','')),6)
    ELSE {acc_perd_unmatch_res}
END                                                                                              AS accounting_period,
COALESCE(left(trim(replace(trans_dt.dttm,'-','')),6),'1900')                                     AS source_accounting_period,
greatest(lmt.lakelastupdatedate, main.lakelastupdatedate, ip.lakelastupdatedate, 
    trans_dt.lakelastupdatedate)                                                                 AS lakelastupdatedate,
greatest(lmt.lakelastupdatetimestamp, main.lakelastupdatetimestamp,  ip.lakelastupdatetimestamp, 
    trans_dt.lakelastupdatetimestamp)                                                            AS lakelastupdatetimestamp,
CASE 
    WHEN coalesce(lmt.lakeDeletedTimestamp, main.lakeDeletedTimestamp, ip.lakeDeletedTimestamp) IS NULL 
    THEN NULL ELSE {batch_effective_datetime} 
END                                                                                              AS lakeDeletedTimestamp
FROM      
(
select * from standardised_subscribe.dbo_polmltlmt
WHERE {batch_effective_datetime} >= lakevalidfromtimestamp
AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) lmt

--to fetch only those policies that exists in universe/main table
INNER JOIN standardised_subscribe.dbo_polmain main
ON upper(lmt.polid) = upper(main.polid)
AND upper(lmt.unitpsu) = upper(main.unitpsu)
AND {batch_effective_datetime} >= main.lakevalidfromtimestamp
AND {batch_effective_datetime} < COALESCE(main.lakevalidtotimestamp, CURRENT_TIMESTAMP())

--to fetch the policies which are inward
INNER JOIN standardised_subscribe.dbo_inpol ip
ON upper(lmt.polid) =  upper(ip.polid)
AND upper(lmt.unitpsu) =  upper(ip.unitpsu)
AND {batch_effective_datetime} >= ip.lakevalidfromtimestamp
AND {batch_effective_datetime} < COALESCE(ip.lakevalidtotimestamp, CURRENT_TIMESTAMP())

--to fetch source transaction date
LEFT JOIN
(
SELECT Upper(polid) AS polid
       ,unitpsu
       ,min(dttm) as dttm
       ,max(lakelastupdatedate) as lakelastupdatedate
       ,max(lakelastupdatetimestamp) as lakelastupdatetimestamp
       ,NULLIF(MAX(COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01')),'9999-12-31T01:01:01') AS lakeDeletedTimestamp
FROM standardised_subscribe.dbo_polrevhist
        where {batch_effective_datetime} >= LakeValidFromTimestamp 
        AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP()) 
GROUP BY Upper(polid), unitpsu
) trans_dt
ON upper(lmt.polid) = upper(trans_dt.polid)
AND upper(lmt.unitpsu) = upper(trans_dt.unitpsu)
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
    
    #declare variables with hard coded values 
    #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
    #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

    #standardised_subscribe.dbo_polanlycd,
    viewName = 'dummy_Dummy.dummyVwLimit'
    viewTables = 'standardised_Subscribe.dbo_PolMain, standardised_subscribe.dbo_polmltlmt, standardised_subscribe.dbo_inpol, standardised_subscribe.dbo_polrevhist'
    
    #call function
    dependentTableIsUpdated = populateObjectDateAvailabilityWithDataCheck(viewName,viewTables,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
    logTaskProgress(cursor,batchTaskId,"executed function to populate object date availability")
except Exception as e:
    errorMessage="Exception occured while execution of function to populate object date availability: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Get metadata - Execute high level stored procedure
#call spExecHighLevel function to retrieve values from store procedure and store in pandas dataframe                                         
try:        
  sourceDestinationTableDetails = getSourceToDestinationHighLevelDetails(conn
                                                                         ,cursor
                                                                         ,batchTaskId
                                                                         ,adfPipelineName
                                                                         ,clusterId
                                                                         ,notebookName
                                                                         ,errorLogFileLocation)

  #Replace the nulls with 'none' as this sp always returns single record no need to specify schema
  sourceDestinationTableDetails=sourceDestinationTableDetails.fillna('none')
  logTaskProgress(cursor,batchTaskId,"Executed high level store procedure")
except Exception as e:
  errorMessage="Exception occured while execution of object high level stored procedure: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Transform high level metadata into spark dataframe
#call Transform_pd_df_to_Pyspark_df function to convert pandas to pyspark dataframe and store return values in variables
try:
  srcAndDesTableDF=spark.createDataFrame(sourceDestinationTableDetails)

  logTaskProgress(cursor,batchTaskId,"Converted sourceDestinationTableDetails pandas to spark dataframe")
except Exception as e:
  errorMessage="Unable to convert pandas to spark dataframe: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Transform high level metadata into spark dataframe & Get values into variables
try:
  #convert pandas dataframe into spark data frame
  srcAndDesTableDF = spark.createDataFrame(sourceDestinationTableDetails)

  #collect dataframe row as a dictionary
  srcDesDict = srcAndDesTableDF.collect()[0].asDict()

  #the id of the source object taken from the procedure output
  sourceObjectId = srcDesDict['source_object_id']  
  #the name of the source taken from the procedure output
  sourceTableName = srcDesDict['source_object_name']
  #the calculated where expression to be applied in source dataframe
  whereExpression = srcDesDict['source_where_clause']
  #the id of the target object taken from the procedure ouptut
  targetObjectId=srcDesDict['destination_object_id']
  #the name of the target table taken from the procedure output
  targetObjectName=srcDesDict['destination_object_name']
  #use batch effective date flag
  useBatchEffectiveDatetime = srcDesDict['use_batch_effective_datetime']
  #get batch effective date
  batchEffectiveDatetime = srcDesDict['batch_effective_datetime']

  logTaskProgress(cursor,batchTaskId,"Got dataframe values into variables")
except Exception as e:
  errorMessage="Unable to retrieve the dataframe values as variables: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Register hash udf
try:
  if dependentTableIsUpdated == True:
    computeHashValueUdfRegistration()
    logTaskProgress(cursor,batchTaskId,"Hash udfs are registered")
except Exception as e:
  errorMessage="Unable register Hash udfs: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Get Metadata - Exec get primary keys and type2 fields stored procedure
#call spExecGetPKsType2Fields function to retrieve values from store procedure and store in pandas dataframe                                         
try:
  if dependentTableIsUpdated == True:
    pksType2FieldsDetails = spExecGetPKsType2Fields(conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)

    logTaskProgress(cursor,batchTaskId,"Executed get primary keys and type2 fields store procedure")
except Exception as e:
  errorMessage="Exception occured while execution get primary keys and type2 fields store procedure: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Transform PKs & Type2 metadata into spark dataframe & Get values into list variables
#convert function to convert pandas to pyspark dataframe and store return values in variables
try:
  if dependentTableIsUpdated == True:
    fieldsSchema = getStructForPKsType2Df()

    pksType2FieldsDF = convertPandasToSparkDfWithSchema(pksType2FieldsDetails,fieldsSchema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation).na.fill('')

    pksFieldsList = pksType2FieldsDF.where("primary_key_order is not null").orderBy('primary_key_order').agg(collect_list(pksType2FieldsDF.object_attribute_name)).collect()[0][0]
    type2FieldsList = pksType2FieldsDF.where("track_type_2_changes==True").orderBy('object_attribute_name').agg(collect_list(pksType2FieldsDF.object_attribute_name)).collect()[0][0]

    logTaskProgress(cursor,batchTaskId,"Converted pksType2FieldsDetails pandas to spark dataframe")
except Exception as e:
  errorMessage="Unable to convert pksType2FieldsDetails pandas to spark dataframe: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Create dataframe from standardised data
try:
    if dependentTableIsUpdated == True:

        #Replace value of batch effective date in SQL Query
        acc_perd_match_query = acc_perd_match.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
        acc_perd_match_res_tmp = spark.sql(acc_perd_match_query)
        acc_perd_match_res = acc_perd_match_res_tmp.collect()[0][0]
        acc_perd_unmatch_query = acc_perd_unmatch.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
        acc_perd_unmatch_res_tmp = spark.sql(acc_perd_unmatch_query)
        acc_perd_unmatch_res = acc_perd_unmatch_res_tmp.collect()[0][0]
        if acc_perd_unmatch_res_tmp.count() == 0:
            acc_perd_unmatch='190001'
        
        #Replace value of batch effective date in SQL Query
        sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                   missing_mdm = "'" + str(missing_mdm) + "'", 
                                   missing_string = "'" + str(missing_string) + "'", 
                                   missing_startdate = "'" + str(missing_startdate) + "'", 
                                   acc_perd_match_res = str(acc_perd_match_res), 
                                   acc_perd_unmatch_res = str(acc_perd_unmatch_res))
        
        #Execute query
        dfCnf = spark.sql(sqlQuery)
        
        logTaskProgress(cursor,batchTaskId,"dataframe created")
except Exception as e:
    errorMessage="error in creating dataframe: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Add incremental filter AND add HBK & audit columns
try:
  if dependentTableIsUpdated == True:
    #add incremental filter
    dfCnf = dfCnf.where(whereExpression)
    
    #set to True if conformed table has HashedPartitionKey column
    hasHPK = False
    #call function to add audit and hbk columns
    dfCnf = addAuditAndHBKColumnsOnDF(dfCnf,pksFieldsList,type2FieldsList,currentTs,batchEffectiveDatetime,hasHPK,sourceId,batchId,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
   
    #get columns list
    allColumns = dfCnf.columns
    columnList = list(map(lambda x:(x,x),allColumns))
    
    logTaskProgress(cursor,batchTaskId,"Added HBK & Audit columns AND applied incremental filter")
except Exception as e:
  errorMessage="Exception occurred while adding HBK & audit columns OR applying incremental filter: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Apply ScdType2 
try:
  if dependentTableIsUpdated == True:
    applySCDType2onDestForBatchLoad(dfCnf,targetObjectName,columnList,batchEffectiveDatetime,currentTs,cursor,batchTaskId,batchId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    logTaskProgress(cursor,batchTaskId,"Applied ScdType2")
except Exception as e:
  errorMessage="Unable to apply the scd type 2: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 
 

# COMMAND ----------

# DBTITLE 1,Mark the dates as loaded into the destination
try:
  if dependentTableIsUpdated == True:
    conn, cursor = markDatesLoaded(targetObjectId
                                  ,sourceObjectId
                                  ,batchEffectiveDatetime if useBatchEffectiveDatetime == True else currentTs
                                  ,batchEffectiveDatetime if useBatchEffectiveDatetime == True else currentTs
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

# DBTITLE 1,Complete task and close connection
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