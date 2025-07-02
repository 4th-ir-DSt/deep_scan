# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_Policy_Section_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Policy_Section table in conformed zone</td></tr>
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

# DBTITLE 1,Prepare Policy Bulk Indicator Query
SQLQuery_PolBulkInd = """
SELECT main.polid                                                                         AS Policy_Reference,
MAX(GREATEST(main.lakelastupdatedate, ip.lakelastupdatedate, mop.lakelastupdatedate, pac.lakelastupdatedate, 
    insd.lakelastupdatedate, bcm.lakelastupdatedate) )                                    AS lakelastupdatedate,
MAX(GREATEST(main.lakelastupdatetimestamp, ip.lakelastupdatetimestamp, mop.lakelastupdatetimestamp, pac.lakelastupdatetimestamp, 
    insd.lakelastupdatetimestamp, bcm.lakelastupdatetimestamp))                           AS lakelastupdatetimestamp
FROM 
(
  select * from standardised_subscribe.dbo_polmain 
  WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
  AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
  AND lakeDeletedTimestamp IS NULL
) main

INNER JOIN standardised_subscribe.dbo_inpol ip
ON Upper(main.polid) = Upper(ip.polid)
AND main.unitpsu = ip.unitpsu
AND ip.lakeDeletedTimestamp IS NULL
AND {batch_effective_datetime} >= IP.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(IP.LakeValidToTimestamp, current_timestamp())
       
INNER JOIN standardised_subscribe.dbo_polanlycd mop
ON Upper(main.polid) = Upper(mop.polid)
AND main.unitpsu = mop.unitpsu
AND mop.lakeDeletedTimestamp IS NULL
AND {batch_effective_datetime} >= mop.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(mop.LakeValidToTimestamp, current_timestamp())      
AND upper(trim(mop.ty)) = 'MOP' and trim(mop.cd) = '116'
       
INNER JOIN standardised_subscribe.dbo_polanlycd pac
ON Upper(main.polid) = Upper(pac.polid)
AND main.unitpsu = pac.unitpsu
AND pac.lakeDeletedTimestamp IS NULL
AND {batch_effective_datetime} >= pac.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(pac.LakeValidToTimestamp, current_timestamp())      
AND lower(trim(pac.ty)) = 'subclass'

INNER JOIN standardised_subscribe.dbo_Insd insd
ON Upper(main.insdid) = Upper(insd.insdid)
AND insd.lakeDeletedTimestamp IS NULL
AND {batch_effective_datetime} >= insd.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(insd.LakeValidToTimestamp, current_timestamp())      
AND upper(trim(insd.InsdNm)) IN ('CANOPIUS ASIA PTE LTD', 'CANOPIUS AUSTRALIA & PACIFIC')

INNER JOIN standardised_mdm.mdm_business_classification_mapping bcm 
ON Upper(pac.cd) = Upper(bcm.Sub_Class_Code)
AND bcm.lakeDeletedTimestamp IS NULL
AND {batch_effective_datetime} >= bcm.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(bcm.LakeValidToTimestamp, current_timestamp())      
AND (upper(trim(pac.dsc)) LIKE '%AUSTRALIA%' OR upper(trim(pac.dsc)) LIKE '%SINGAPORE%' OR upper(trim(bcm.Sub_Class_Code)) = 'CRE')
AND CASE 
        WHEN upper(trim(pac.dsc)) LIKE '%AUSTRALIA%' THEN 2019
        WHEN upper(trim(bcm.Sub_Class_Code)) = 'CRE' THEN 2020 -- Singapore
        ELSE 2020 -- Singapore
    END <= ip.acctgyr
group by main.polid
"""

# COMMAND ----------

# DBTITLE 1,Prepare SQL Query
sqlQuery= """
SELECT 
       Concat(main.polid, '_', main.unitpsu)                        AS Policy_Header_Reference, 
       Concat(main.polid, '_', main.unitpsu)                        AS Policy_Section_Reference,
       COALESCE(iso_ref.Location_Sub_Region_Code, {missing_mdm})    AS Risk_Location_Sub_Region_Code,
       COALESCE(div.Location_Country_Code, iso_ref.Location_Country_Code, {missing_mdm}) AS Risk_Country_Alpha_3_Code,
       COALESCE(div.Location_Sub_Division_Code, {missing_mdm})      AS Risk_Location_Sub_Division_Code,
       main.polid                                                   AS Policy_Reference,
       COALESCE(NULLIF(Trim(sub_cls.cd), ''), {missing_string})     AS Sub_Class_Code,
       Coalesce(main.incpdt, {missing_startdate_string})            AS Inception_Date,
       Coalesce(main.expydt, {missing_startdate_string})            AS Expiry_Date,
       {missing_startdate_string}                                   AS Long_Term_Agreement_Expiry_Date,
       {missing_integer}                                            AS Credit_Period,
       Coalesce(IP.acctgyr, {missing_startdate_string})             AS Year_of_Account,
       {missing_integer}                                            AS Settlement_Frequency,
       Coalesce(main.cncldys, 0)                                    AS Notice_Period,
       {missing_string}                                             AS Attachment_Priority,
       COALESCE(NULLIF(Trim(tfc.cd), ''), {missing_string})         AS Source_Trust_Fund_Code,
       COALESCE(conduct_risk_rating, {missing_string})              AS Conduct_Risk_Rating,
       CASE 
           WHEN lnk.topolid IS NOT NULL THEN 'Y' ELSE 'N'
       END                                                          AS Renewable_Indicator,
       CASE 
           WHEN nvpolind.Policy_Section_Reference IS NOT NULL THEN 'Y' ELSE 'N'
       END                                                          AS Novated_Policy_Indicator,
       'N'                                                          AS Aggregated_Data_Input_Policy_Indicator,
       CASE 
           WHEN pbi.Policy_Reference is not null THEN 'Y' ELSE 'N' 
       END                                                          AS Bulk_Policy_Indicator,
       CASE 
            WHEN plsecnr.polId is NOT NULL THEN 'Y' ELSE 'N' 
       END                                                          AS Payment_Assignment_Policy_Indicator,
       'N'                                                          AS Unrecognised_External_Policy_Indicator,
       Greatest(main.lakelastupdatedate, IP.lakelastupdatedate, lnk.lakelastupdatedate, tfc.lakelastupdatedate, 
       sub_cls.lakelastupdatedate, plsecnr.lakelastupdatedate, pbi.lakelastupdatedate, loc.lakelastupdatedate, 
       iso_ref.lakelastupdatedate, div.lakelastupdatedate, 
       nvpolind.lakelastupdatedate)                                 AS LakeLastUpdateDate,
       Greatest(main.lakelastupdatetimestamp, IP.lakelastupdatetimestamp, lnk.lakelastupdatetimestamp, tfc.lakelastupdatetimestamp, 
       sub_cls.lakelastupdatetimestamp, plsecnr.lakelastupdatetimestamp, pbi.lakelastupdatetimestamp, loc.lakelastupdatetimestamp, 
       iso_ref.lakelastupdatetimestamp, div.lakelastupdatetimestamp,        
       nvpolind.lakelastupdatetimestamp)                            AS LakeLastUpdateTimestamp,
       CASE 
            WHEN coalesce(main.lakeDeletedTimestamp, IP.lakeDeletedTimestamp) IS NOT NULL 
            THEN {batch_effective_datetime} ELSE NULL 
       END                                                          AS lakeDeletedTimestamp
FROM   
(
    select * from standardised_subscribe.dbo_polmain 
    WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
) main

--to fetch accounting year from inwards policy
INNER JOIN standardised_subscribe.dbo_inpol ip
ON Upper(main.polid) = Upper(ip.polid)
AND main.unitpsu = ip.unitpsu
AND {batch_effective_datetime} >= IP.LakeValidFromTimestamp 
and {batch_effective_datetime} < coalesce(IP.LakeValidToTimestamp, current_timestamp())

LEFT JOIN 
(
    select PolId, UnitPsu, lakelastupdatedate, lakelastupdatetimestamp
    from standardised_subscribe.dbo_PolSectNarr
    where trim(upper(sectty)) = 'DUMMY ATTACHMENT POLICY' 
    AND trim(upper(sectDsc)) = 'Y'
    AND lakeDeletedTimestamp IS NULL
    AND {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
    QUALIFY row_number() OVER(partition BY upper(polid), unitpsu ORDER BY sectnarrseqno DESC) = 1
) plsecnr
ON Upper(main.polid) = Upper(plsecnr.polid)
AND main.unitpsu = plsecnr.unitpsu

--to fetch the policies that have been renewed
LEFT JOIN 
(
SELECT Upper(topolid)                                                                  AS ToPolId,
       tounitpsu,
       Max(lakelastupdatedate)                                                         AS LakeLastUpdateDate,
       Max(lakelastupdatetimestamp)                                                    AS LakeLastUpdateTimestamp
FROM   standardised_subscribe.dbo_pollnk
WHERE  lnkty IN ( 1, 16 )
AND    topolid <> frpolid
AND    lakeDeletedTimestamp IS NULL
AND    {batch_effective_datetime} >= LakeValidFromTimestamp 
AND    {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
GROUP  BY Upper(topolid),
           tounitpsu
) lnk
ON Upper(main.polid) = lnk.topolid
AND main.unitpsu = lnk.tounitpsu
AND {batch_effective_datetime} >= main.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(main.LakeValidToTimestamp, current_timestamp())

LEFT JOIN standardised_subscribe.dbo_polanlycd sub_cls
ON Upper(main.polid) = Upper(sub_cls.polid)
AND main.unitpsu = sub_cls.unitpsu
AND Upper(trim(sub_cls.ty)) = 'SUBCLASS'  
AND sub_cls.lakeDeletedTimestamp IS NULL
AND {batch_effective_datetime} >= sub_cls.LakeValidFromTimestamp 
and {batch_effective_datetime} < coalesce(sub_cls.LakeValidToTimestamp, current_timestamp())

LEFT JOIN standardised_subscribe.dbo_polanlycd tfc
ON Upper(main.polid) = Upper(tfc.polid)
AND main.unitpsu = tfc.unitpsu
AND tfc.lakeDeletedTimestamp IS NULL
AND upper(trim(tfc.Ty)) = 'TFC'
AND {batch_effective_datetime} >= tfc.LakeValidFromTimestamp 
and {batch_effective_datetime} < coalesce(tfc.LakeValidToTimestamp, current_timestamp())

LEFT JOIN 
(
    SELECT cd, polid, unitpsu, lakelastupdatedate, lakelastupdatetimestamp
    FROM standardised_subscribe.dbo_polanlycd 
    WHERE upper(trim(ty)) = 'GEOCODE' 
    AND NULLIF(trim(cd),'') IS NOT NULL
    AND lakeDeletedTimestamp IS NULL
    AND {batch_effective_datetime} >= LakeValidFromTimestamp 
    and {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
) loc
ON Upper(main.polid) = Upper(loc.polid)
AND main.unitpsu = loc.unitpsu 

LEFT JOIN 
(
  SELECT source_location_code,
         Location_Sub_Division_Code  AS iso_div_cd,
         Location_Country_Code,
         Location_Sub_Region_Code,
         lakelastupdatedate, lakelastupdatetimestamp
  FROM   standardised_mdm.mdm_location_mapping
  WHERE  trim(lower(source_name)) = 'subscribe'
  AND lakeDeletedTimestamp IS NULL
  AND {batch_effective_datetime} >= LakeValidFromTimestamp 
  AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
 ) iso_ref
ON loc.cd = iso_ref.source_location_code

LEFT JOIN 
(
    SELECT 
    code                             AS div_cd,
    Location_Sub_Division_Code,
    Location_Country_Code,
    lakelastupdatedate, lakelastupdatetimestamp
    from standardised_mdm.mdm_location_sub_division
    WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
    AND lakeDeletedTimestamp IS NULL
) div
ON iso_ref.iso_div_cd = div.div_cd

LEFT JOIN standardised_mdm.mdm_novated_policy_indicator nvpolind
ON Upper(main.polid) = Upper(nvpolind.Policy_Section_Reference)
AND nvpolind.lakeDeletedTimestamp IS NULL
AND {batch_effective_datetime} >= nvpolind.LakeValidFromTimestamp 
and {batch_effective_datetime} < coalesce(nvpolind.LakeValidToTimestamp, current_timestamp())

LEFT JOIN PolBulkInd pbi
ON Upper(main.polid) = Upper(pbi.Policy_Reference)

--to fetch condust risk rating as per V1.9.7
LEFT JOIN
(
	SELECT Upper(trim(polid)) AS PolId, unitpsu, 
    sectdsc                                                                    AS Conduct_Risk_Rating,
	lakelastupdatedate, lakelastupdatetimestamp
	FROM standardised_subscribe.dbo_polsectnarr
	WHERE Upper(Trim(sectty)) IN ('CONDUCT RISK RATING')
    AND lakeDeletedTimestamp IS NULL
    AND {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
    QUALIFY Row_number() OVER(partition BY Upper(polid), unitpsu ORDER BY sectnarrseqno DESC) = 1
) risk
ON main.polid = risk.polid
AND main.unitpsu = risk.unitpsu
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  # standardised_subscribe.dbo_outpol, 
  viewName = 'dummy_Dummy.dummyVwPolicy_Section'
  viewTables = 'standardised_Subscribe.dbo_PolMain,standardised_subscribe.dbo_inpol, standardised_subscribe.dbo_polanlycd, standardised_subscribe.dbo_pollnk, standardised_mdm.mdm_novated_policy_indicator, standardised_subscribe.dbo_Insd, standardised_mdm.mdm_business_classification_mapping, standardised_mdm.mdm_location_mapping, standardised_mdm.mdm_location_sub_division,standardised_subscribe.dbo_polsectnarr'

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
    
    SQLQuery_PolBulkInd = SQLQuery_PolBulkInd.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                               missing_string = "'" +str(missing_string) + "'",
                               missing_startdate_string = "'" +str(missing_startdate_string) + "'", 
                               missing_mdm = "'" +str(missing_mdm) + "'")
    #Replace value of batch effective date in SQL Query
    sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                               missing_string = "'" +str(missing_string) + "'",
                               missing_startdate_string = "'" +str(missing_startdate_string) + "'",
                               missing_integer = missing_integer, missing_mdm = "'" +str(missing_mdm) + "'")
    
    #Execute temp query 
    dfCnf_PolBulkInd = spark.sql(SQLQuery_PolBulkInd)
    dfCnf_PolBulkInd.createOrReplaceTempView("PolBulkInd")
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