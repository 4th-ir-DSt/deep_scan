# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_party_signing_transaction_role_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Party_signing_transaction_role table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Rahul</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/03/29</td></tr>
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

# DBTITLE 1,Get Previous BED
try:
    prv_bed = getBatchTaskPreviousBed(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    logTaskProgress(cursor,batchTaskId,"executed function to get previous BED")
except Exception as e:
    errorMessage="Exception occured while execution of function to get previousBed: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Fetch Country_Div with Count based on Insdid
fetch_cty_divQuery = """
SELECT insdid,
       Domicile_Location_Sub_Division_Code,
       unitpsu,
       lakelastupdatedate,
       lakelastupdatetimestamp,
       lakeDeletedTimestamp
       --cnt_subdivision
FROM (
SELECT insdid,
       Domicile_Location_Sub_Division_Code,
       unitpsu,
       lakelastupdatedate,
       lakelastupdatetimestamp,
       lakeDeletedTimestamp
       --Count(distinct Domicile_Location_Sub_Division_Code) OVER (partition BY insdid ORDER BY insdid) AS cnt_subdivision
FROM (
      SELECT insdid,
			 cd 																			 AS Domicile_Location_Sub_Division_Code,
			 unitpsu,
			 Max(lakelastupdatedate)      													 AS lakelastupdatedate,
			 Max(lakelastupdatetimestamp) 													 AS lakelastupdatetimestamp,
             NULLIF(MAX(COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01')),'9999-12-31T01:01:01') AS lakeDeletedTimestamp
       FROM (
             SELECT pinsd.insdid,
                    panyl.cd,
                    panyl.unitpsu,
                    greatest(pmain.lakelastupdatedate, panyl.lakelastupdatedate, 
                    ip.lakelastupdatedate, pinsd.lakelastupdatedate)                         AS lakelastupdatedate,
                    greatest(pmain.lakelastupdatetimestamp, panyl.lakelastupdatetimestamp, 
                    ip.lakelastupdatetimestamp, pinsd.lakelastupdatetimestamp) 	             AS lakelastupdatetimestamp,
                    CASE 
                        WHEN coalesce(pmain.lakeDeletedTimestamp, panyl.lakeDeletedTimestamp, ip.lakeDeletedTimestamp, 
                        pinsd.lakeDeletedTimestamp) Is Not Null THEN {batch_effective_datetime} ELSE NULL 
                    END                                                                      AS lakeDeletedTimestamp
             FROM  standardised_subscribe.dbo_polmain pmain
             
             INNER JOIN standardised_subscribe_dbo_polanlycd_tmpvw panyl
			 ON  upper(panyl.polid) = upper(pmain.polid)
			 AND upper(panyl.unitpsu) = upper(pmain.unitpsu)
             AND ({queryWhereCondition_polanlycd})
             AND {batch_effective_datetime} >= pmain.LakeValidFromTimestamp 
             and {batch_effective_datetime} < coalesce(pmain.LakeValidToTimestamp, current_timestamp())
             AND {batch_effective_datetime} >= panyl.LakeValidFromTimestamp 
             and {batch_effective_datetime} < coalesce(panyl.LakeValidToTimestamp, current_timestamp())
                    
             INNER JOIN standardised_subscribe.dbo_inpol ip
			 ON  upper(pmain.polid) = upper(ip.polid)
			 AND upper(pmain.unitpsu) = upper(ip.unitpsu)
             AND {batch_effective_datetime} >= pmain.LakeValidFromTimestamp 
             and {batch_effective_datetime} < coalesce(pmain.LakeValidToTimestamp, current_timestamp())
             AND {batch_effective_datetime} >= ip.LakeValidFromTimestamp 
             and {batch_effective_datetime} < coalesce(ip.LakeValidToTimestamp, current_timestamp())     
                    
             INNER JOIN standardised_subscribe.dbo_polinsd pinsd
             ON  upper(pinsd.polid) = upper(pmain.polid)
             AND upper(pinsd.unitpsu) = upper(pmain.unitpsu)
             AND {batch_effective_datetime} >= pmain.LakeValidFromTimestamp 
             and {batch_effective_datetime} < coalesce(pmain.LakeValidToTimestamp, current_timestamp())
             AND {batch_effective_datetime} >= pinsd.LakeValidFromTimestamp 
             and {batch_effective_datetime} < coalesce(pinsd.LakeValidToTimestamp, current_timestamp())
             ) a
       GROUP BY insdid, cd, unitpsu
      ) cntrydiv
      ) cntrydiv_fnl
"""

# COMMAND ----------

# DBTITLE 1,Query_join
joinQuery = """
--fetching transactions as well as party from USMMain that exists in PolInsd
SELECT 
CONCAT_WS('_', u.TrnId, u.TrnCgy, COALESCE(usmd.instno, '01'))    AS Signing_Transaction_Reference,
Concat(polinsd.insdid, '_', Domicile_Location_Sub_Division_Code)  AS Party_Code,
Role_Type,
{missing_string}                                                  AS Psu,
u.polid,
u.unitpsu,
'N'                                                               AS main_party_signing_transaction_role_indicator,
GREATEST(u.lakelastupdatedate, insd.lakelastupdatedate, polinsd.lakelastupdatedate, CtyD.lakelastupdatedate, 
    anly.lakelastupdatedate, usmd.lakelastupdatedate)             AS lakelastupdatedate, 
GREATEST(u.lakelastupdatetimestamp, insd.lakelastupdatetimestamp, polinsd.lakelastupdatetimestamp, CtyD.lakelastupdatetimestamp, 
    anly.lakelastupdatetimestamp, usmd.lakelastupdatetimestamp)   AS lakelastupdatetimestamp,
CASE 
    WHEN coalesce(u.lakeDeletedTimestamp, insd.lakeDeletedTimestamp, polinsd.lakeDeletedTimestamp, 
    CtyD.lakeDeletedTimestamp, anly.lakeDeletedTimestamp) IS NOT NULL THEN {batch_effective_datetime} ELSE NULL 
END                                                               AS lakeDeletedTimestamp
FROM standardised_subscribe.dbo_usmmain u

-- to fetch the role_type of the policies
INNER JOIN
(
    SELECT polid,
           unitpsu,
           insdid,
           CASE
              WHEN UPPER(TRIM(insdty)) IN ('INSURED', 'REINSURED', 'LESSEE', 'OBLIGOR', 'CEDENT', 'GUARANTOR') THEN UPPER(TRIM(insdty))
              ELSE 'ADDITIONAL_INSURED'
           END                                                    AS role_type,
           lakelastupdatedate,
           lakelastupdatetimestamp,
           lakeDeletedTimestamp
    FROM   standardised_subscribe_dbo_polinsd_tmpvw
    WHERE  ({queryWhereCondition_polinsd})
    AND  {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND  {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
) polinsd 
ON  UPPER(u.polid) = UPPER(polinsd.polid)
AND UPPER(u.unitpsu) = UPPER(polinsd.unitpsu)

INNER JOIN
(
    SELECT InsdId, Domicile_Location_Sub_Division_Code, unitpsu, 
    lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp
    FROM getcountrydiv
) CtyD
ON Trim(polinsd.insdid) = Trim(CtyD.insdid)

INNER JOIN
(
   SELECT InsdId, lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp
    FROM standardised_subscribe.dbo_insd
    WHERE  {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND    {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
) insd
ON polinsd.insdid = insd.insdid

INNER JOIN
(
   SELECT PolId, Cd, lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp
    FROM standardised_subscribe_dbo_polanlycd_tmpvw
    WHERE  {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND    {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
    AND    ({queryWhereCondition_polanlycd})
) anly
ON  polinsd.PolId = anly.polid
AND CtyD.Domicile_Location_Sub_Division_Code = anly.Cd

--to bring installment number 
LEFT JOIN standardised_subscribe.dbo_usmdefd usmd
ON CONCAT(u.TrnId, '_', u.TrnCgy) = CONCAT(usmd.TrnId, '_', usmd.TrnCgy)
AND {batch_effective_datetime} >= usmd.LakeValidFromTimestamp 
AND {batch_effective_datetime} < COALESCE(usmd.LakeValidToTimestamp, CURRENT_TIMESTAMP())
AND usmd.lakeDeletedTimestamp IS NULL

WHERE {batch_effective_datetime} >= u.LakeValidFromTimestamp 
AND   {batch_effective_datetime} < COALESCE(u.LakeValidToTimestamp, CURRENT_TIMESTAMP())


UNION ALL


--fetching transactions as well as party from USMMain that exists in Bkr
SELECT
CONCAT_WS('_', u.TrnId, u.TrnCgy, COALESCE(usmd.instno, '01'))    AS Signing_Transaction_Reference,
u.bkrcd                                                           AS Party_Code,
'BROKER'                                                          AS Role_Type,
u.bkrpsu                                                          AS Psu,
u.polid,
u.unitpsu,
'N'                                                               AS main_party_signing_transaction_role_indicator,
GREATEST(u.lakelastupdatedate, bkr.lakelastupdatedate,
    usmd.lakelastupdatedate)                                      AS lakelastupdatedate,  
GREATEST(u.lakelastupdatetimestamp, bkr.lakelastupdatetimestamp,
    usmd.lakelastupdatetimestamp)                                 AS lakelastupdatetimestamp,
CASE 
    WHEN coalesce(u.lakeDeletedTimestamp, bkr.lakeDeletedTimestamp) IS NOT NULL THEN {batch_effective_datetime} ELSE NULL 
END                                                               AS lakeDeletedTimestamp
FROM standardised_subscribe.dbo_usmmain u

INNER JOIN
(
 SELECT bkrcd,
        bkrpsu,
        lakelastupdatedate,
        lakelastupdatetimestamp,
        lakeDeletedTimestamp,
        ROW_NUMBER() OVER(PARTITION BY bkrcd, bkrpsu 
            ORDER BY coalesce(lakeDeletedTimestamp, '9999-12-31T01:01:01.000') desc, bkrseqid DESC) AS rnk
 FROM   standardised_subscribe.dbo_bkr
 WHERE  {batch_effective_datetime} >= LakeValidFromTimestamp 
 AND    {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
) bkr
ON  TRIM(bkr.bkrcd) = TRIM(u.bkrcd)
AND TRIM(UPPER(bkr.bkrpsu))=TRIM(UPPER(u.bkrpsu))
AND Rnk = 1

--to bring installment number 
LEFT JOIN standardised_subscribe.dbo_usmdefd usmd
ON CONCAT(u.TrnId, '_', u.TrnCgy) = CONCAT(usmd.TrnId, '_', usmd.TrnCgy)
AND {batch_effective_datetime} >= usmd.LakeValidFromTimestamp 
AND {batch_effective_datetime} < COALESCE(usmd.LakeValidToTimestamp, CURRENT_TIMESTAMP())
AND usmd.lakeDeletedTimestamp IS NULL

WHERE {batch_effective_datetime} >= u.LakeValidFromTimestamp 
AND   {batch_effective_datetime} < COALESCE(u.LakeValidToTimestamp, CURRENT_TIMESTAMP())
"""

# COMMAND ----------

# DBTITLE 1,Prepare SQL Query
sqlQuery = """
SELECT 
Signing_Transaction_Reference,
CONCAT(TRIM(UPPER(party_code)), '_', TRIM(COALESCE(UPPER(NULLIF(Jquery.Psu, '?')), {missing_string})), '_', TRIM(UPPER(role_type)))                                                 AS Party_Reference,
CASE 
    WHEN role_type = 'BROKER' THEN role_type
    ELSE CONCAT(TRIM(UPPER(party_code)), '_', TRIM(COALESCE(UPPER(NULLIF(Jquery.Psu, '?')), {missing_string})), '_', TRIM(UPPER(role_type)))
END                                                           AS Party_Role_Reference ,   
role_type                                                     AS Role_Type,
CASE
    WHEN CONCAT(UpdMain1.polid, UpdMain1.unitpsu, UpdMain1.bkrno) IS NOT NULL THEN 'Y' 
    ELSE main_party_signing_transaction_role_indicator
END                                                           AS Main_Party_Signing_Transaction_Role_Indicator,
GREATEST(Jquery.lakelastupdatedate, UpdMain1.lakelastupdatedate)            AS LakeLastUpdateDate,
GREATEST(Jquery.lakelastupdatetimestamp, UpdMain1.lakelastupdatetimestamp)  AS LakeLastUpdateTimestamp,
CASE 
    WHEN Jquery.lakeDeletedTimestamp IS NOT NULL THEN {batch_effective_datetime} ELSE NULL
END                                                           AS lakeDeletedTimestamp
FROM univ Jquery

-- to update Main_Party_Signing_Transaction_Role_Indicator with Flag 'Y'
LEFT JOIN standardised_subscribe.dbo_polmain UpdMain1
ON UPPER(Jquery.polid) = UPPER(UpdMain1.polid)
AND UPPER(Jquery.unitpsu) = UPPER(UpdMain1.unitpsu)
AND TRIM(Jquery.party_code) = TRIM(UpdMain1.bkrno)
AND {batch_effective_datetime} >= UpdMain1.LakeValidFromTimestamp 
AND {batch_effective_datetime} < COALESCE(UpdMain1.LakeValidToTimestamp, CURRENT_TIMESTAMP())
AND UpdMain1.lakeDeletedTimestamp IS NULL
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above
  

  #,standardised_subscribe.dbo_PolAnlyCd
  viewName = 'dummy_Dummy.dummyVwParty_Signing_Transaction_Role'
  viewTables = 'standardised_Subscribe.dbo_PolMain, standardised_subscribe.dbo_PolInsd, standardised_subscribe.dbo_bkr, standardised_subscribe.dbo_UsmMain, standardised_subscribe.dbo_UsmDefd, standardised_subscribe.dbo_polanlycd, standardised_subscribe.dbo_inpol, standardised_subscribe.dbo_insd'

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

# DBTITLE 1,Create Filter Standardised View
try:
    std_object_polanlycd = 'standardised_subscribe.dbo_polanlycd'
    current_bed = batchEffectiveDatetime
    queryWhereCondition_polanlycd = """
    upper(trim(ty)) = 'DOMICILE' AND NULLIF(trim(cd),'') IS NOT NULL
    """
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_polanlycd = createStandardisedMultipleFilterView(std_object_polanlycd,
                                                                prv_bed,
                                                                current_bed,
                                                                whereExpression,
                                                                queryWhereCondition_polanlycd,
                                                                cursor,
                                                                batchTaskId,
                                                                adfPipelineName,
                                                                clusterId,
                                                                notebookName,
                                                                errorLogFileLocation)
        union_df_polanlycd=spark.read.table(std_object_polanlycd).unionByName(std_df_polanlycd)
    else :
        union_df_polanlycd=spark.read.table(std_object_polanlycd)
        
    union_df_polanlycd.createOrReplaceTempView('standardised_subscribe_dbo_polanlycd_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_polanlycd_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create Filter Standardised View
try:
    std_object_polinsd = 'standardised_subscribe.dbo_polinsd'
    current_bed = batchEffectiveDatetime
    queryWhereCondition_polinsd = """ NULLIF(insdid, '') IS NOT NULL"""
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_polinsd = createStandardisedMultipleFilterView(std_object_polinsd,
                                                              prv_bed,
                                                              current_bed,
                                                              whereExpression,
                                                              queryWhereCondition_polinsd,
                                                              cursor,
                                                              batchTaskId,
                                                              adfPipelineName,
                                                              clusterId,
                                                              notebookName,
                                                              errorLogFileLocation)
        union_df_polinsd=spark.read.table(std_object_polinsd).unionByName(std_df_polinsd)
    else :
        union_df_polinsd=spark.read.table(std_object_polinsd)
        
    union_df_polinsd.createOrReplaceTempView('standardised_subscribe_dbo_polinsd_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_polinsd_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create dataframe from standardised data
try:
    if dependentTableIsUpdated == True:
        
        #Creating temp tables for temp queries
        fetch_cty_divQuery = fetch_cty_divQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'",
                                                       missing_string = "'" + str(missing_string) + "'",
                                                       missing_startdate = "'" + str(missing_startdate) + "'", 
                                                       missing_mdm = "'" + str(missing_mdm) + "'",
                                                       queryWhereCondition_polanlycd = str(queryWhereCondition_polanlycd))
        fetch_cty_div = spark.sql(fetch_cty_divQuery)
        fetch_cty_div.createOrReplaceTempView('getCountryDiv')
        
        #Creating temp tables for temp queries 
        joinQuery = joinQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                     missing_string = "'" +str(missing_string) + "'",
                                     queryWhereCondition_polanlycd = str(queryWhereCondition_polanlycd) ,
                                     queryWhereCondition_polinsd = str(queryWhereCondition_polinsd))
        dfjoin = spark.sql(joinQuery)
        dfjoin.createOrReplaceTempView('univ')
        
        #Replace value of batch effective date in SQL Query
        sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'",
                                   missing_string = "'" +str(missing_string) + "'")
        
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