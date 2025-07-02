# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_party_section_header_role_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Party_Policy_Section_Role table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Manmohan</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/03/25</td></tr>
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

# DBTITLE 1,Universe
# performed the union operation of tables and performing join with Polmain table
tempPartyUniverse_df = """
SELECT polid,
       unitpsu,
       role_type,
       party_code,
       main_party_policy_section_role_indicator,
       MAX(lakelastupdatedate)                                                                 AS lakelastupdatedate,
       MAX(lakelastupdatetimestamp)                                                            AS lakelastupdatetimestamp,
       NULLIF(MAX(COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01')),'9999-12-31T01:01:01') AS lakeDeletedTimestamp,
       psu
FROM 
(
    SELECT polinsd.polid,
        polinsd.unitpsu,
        CASE
             WHEN insdty IN ( 'INSURED','REINSURED','LESSEE','OBLIGOR','CEDENT','GUARANTOR' ) THEN UPPER(insdty)
             ELSE 'ADDITIONAL_INSURED'
        END                                                                                    AS role_type,
        Concat(polinsd.insdid, '_', CtyD.Domicile_Location_Sub_Division_Code)                  AS Party_Code,
        CASE
               WHEN insdty IN ('INSURED', 'LESSEE') THEN 'Y' 
               WHEN insdty = 'REINSURED' and Row_Number() OVER (partition BY polinsd.Polid, polinsd.insdty ORDER BY polinsd.Tmstp ASC) = '1' THEN 'Y'
               ELSE 'N'
        END                                                                                    AS main_party_policy_section_role_indicator,
        Greatest(polinsd.lakelastupdatedate, CtyD.lakelastupdatedate, 
        insd.lakelastupdatedate, anly.lakelastupdatedate)                                      AS LakeLastUpdateDate,
        Greatest(polinsd.lakelastupdatetimestamp, CtyD.lakelastupdatetimestamp, 
        insd.lakelastupdatetimestamp, anly.lakelastupdatetimestamp)                            AS LakeLastUpdateTimestamp,
        CASE 
            WHEN COALESCE(polinsd.lakeDeletedTimestamp, CtyD.lakeDeletedTimestamp, insd.lakeDeletedTimestamp, anly.lakeDeletedTimestamp) 
                Is Not Null THEN {batch_effective_datetime} 
            ELSE NULL 
        END                                                                                    AS lakeDeletedTimestamp,
        {missing_string}                                                                       AS psu
        FROM
        (
            SELECT InsdId, PolId, UnitPsu, upper(trim(insdty)) AS insdty, Tmstp,
            lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp
            FROM standardised_subscribe_dbo_polinsd_tmpvw
            WHERE  ({queryWhereCondition_polinsd})
            AND    {batch_effective_datetime} >= LakeValidFromTimestamp 
            AND    {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
        ) polinsd
        
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
    
    
    UNION ALL
    
    
    SELECT main.polid,
           main.unitpsu,
           'BROKER'                                                                            AS role_type,
           main.bkrno                                                                          AS party_code,
           'N'                                                                                 AS main_party_policy_section_role_indicator,
           GREATEST(main.lakelastupdatedate, bkr.lakelastupdatedate)                           AS lakelastupdatedate,
           GREATEST(main.lakelastupdatetimestamp, bkr.lakelastupdatetimestamp)                 AS lakelastupdatetimestamp,
           CASE 
                WHEN coalesce(main.lakeDeletedTimestamp, bkr.lakeDeletedTimestamp) IS NOT NULL THEN {batch_effective_datetime} 
                ELSE NULL 
           END                                                                                 AS lakeDeletedTimestamp,
           bkr.bkrpsu                                                                          AS psu
    FROM   standardised_subscribe.dbo_polmain main
    
    INNER JOIN
    (
     SELECT bkrcd,
            bkrpsu,
            lakelastupdatedate,
            lakelastupdatetimestamp,
            lakeDeletedTimestamp,
            ROW_NUMBER() OVER( PARTITION BY bkrcd, UPPER(bkrpsu) 
                ORDER BY COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01') DESC, bkrseqid DESC ) AS rnk
     FROM   standardised_subscribe.dbo_bkr
     WHERE  {batch_effective_datetime} >= LakeValidFromTimestamp 
     AND    {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
    ) bkr
    ON main.bkrno = bkr.bkrcd AND upper(main.bkrpsu) = upper(bkr.bkrpsu) AND rnk = 1
    AND {batch_effective_datetime} >= main.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(main.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    
    
    UNION ALL
    
    
    SELECT polid,
           unitpsu,
           CASE 
                WHEN ty = 'MARKETLEAD' THEN 'MARKET_LEAD'
                WHEN ty = 'LLOYDSLEAD' THEN 'LLOYDS_LEAD'
                WHEN ty = 'SLPLEAD'    THEN 'SLIP_LEAD'
                WHEN ty = 'AGREEPARTY' THEN 'AGREEMENT_PARTY'
            ELSE ty
           END                                                                                 AS role_type,
           cd                                                                                  AS party_code,
           'N'                                                                                 AS main_party_policy_section_role_indicator,
           GREATEST(polanly.lakelastupdatedate, anly.lakelastupdatedate)                       AS lakelastupdatedate,
           GREATEST( polanly.lakelastupdatetimestamp, anly.lakelastupdatetimestamp )           AS lakelastupdatetimestamp,
           CASE 
               WHEN coalesce(polanly.lakeDeletedTimestamp, anly.lakeDeletedTimestamp) IS NOT NULL THEN {batch_effective_datetime} 
               ELSE NULL 
           END                                                                                 AS lakeDeletedTimestamp,
           {missing_string} 		                                                           AS psu
    FROM
    (
        SELECT PolId, UnitPsu, upper(trim(cd)) AS cd, upper(trim(ty)) AS ty, 
        lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp
        FROM   standardised_subscribe_dbo_polanlycd_lead_tmpvw
        WHERE  ({queryWhereCondition_polanlycd_lead})
        AND    {batch_effective_datetime} >= LakeValidFromTimestamp 
        AND    {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
    ) polanly
    
    ----- perform inner join with dbo_anly 08th AUg 2022 (refer to query present in party) -----
    INNER JOIN
    (
       SELECT upper(trim(anlycd)) AS anlycd, upper(trim(anlyty)) AS anlyty,
       lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp
       FROM   standardised_subscribe_dbo_anly_tmpvw
       WHERE  ({queryWhereCondition_anly})
       AND {batch_effective_datetime} >= LakeValidFromTimestamp 
       AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
    ) anly
    ON polanly.cd = anly.anlycd
    AND polanly.ty = anly.anlyty


    UNION ALL
    
    
    SELECT main.polid,
           main.unitpsu,
           'UNDERWRITER'                                                                       AS role_type,
           upper(trim(main.uwr))                                                               AS party_code,
           'Y'                                                                                 AS main_party_policy_section_role_indicator,
           main.lakelastupdatedate                                                             AS lakelastupdatedate,
           main.lakelastupdatetimestamp                                                        AS lakelastupdatetimestamp,
           CASE 
                WHEN main.lakeDeletedTimestamp IS NOT NULL THEN {batch_effective_datetime} 
                ELSE NULL 
           END                                                                                 AS lakeDeletedTimestamp,
           upper(trim(main.uwr))                                                               AS psu
    FROM   standardised_subscribe.dbo_polmain main
    WHERE {batch_effective_datetime} >= main.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(main.LakeValidToTimestamp, CURRENT_TIMESTAMP())
) univ
GROUP BY polid,
         unitpsu,
         role_type,
         party_code,
         main_party_policy_section_role_indicator,
         psu
"""

# COMMAND ----------

# DBTITLE 1,Prepare SQL Query
sqlQuery="""
SELECT 
    CONCAT(MAIN.polid, '_', MAIN.unitpsu)                                               AS Policy_Section_Reference,
    CONCAT(MAIN.polid, '_', MAIN.unitpsu)                                               AS Policy_Header_Reference, 
    CONCAT(TRIM(UPPER(tpu.party_code)), '_', TRIM(COALESCE(UPPER(NULLIF(tpu.psu, '?')), {missing_string})), '_', TRIM(UPPER(tpu.role_type)))                                                                                                             AS Party_Reference,
    CASE 
        WHEN TPU.role_type IN ('UNDERWRITER', 'MARKET_LEAD', 'LLOYDS_LEAD', 'SLIP_LEAD', 'AGREEMENT_PARTY', 'BROKER')
        THEN TPU.role_type
        ELSE CONCAT(TRIM(UPPER(tpu.party_code)), '_', TRIM(COALESCE(UPPER(NULLIF(tpu.psu, '?')), {missing_string})), '_', TRIM(UPPER(tpu.role_type)))
    END                                                                                 AS Party_Role_Reference ,    
    TPU.role_type                                                                       AS Role_Type,
    CASE 
        WHEN UpdMain1.lakeDeletedTimestamp is not null then 'N'
        WHEN role_type = 'BROKER' AND CONCAT(UpdMain1.polid, UpdMain1.unitpsu, UpdMain1.bkrno) IS NOT NULL THEN 'Y'
        ELSE COALESCE(TPU.main_party_policy_section_role_indicator,'N') 
    END                                                                                 AS Main_Party_Policy_Section_Role_Indicator,
    GREATEST(MAIN.lakelastupdatedate, ip.lakelastupdatedate, 
        TPU.lakelastupdatedate, UpdMain1.lakelastupdatedate)                            AS LakeLastUpdateDate,
    GREATEST(MAIN.lakelastupdatetimestamp, ip.lakelastupdatetimestamp, 
        TPU.lakelastupdatetimestamp, UpdMain1.lakelastupdatetimestamp)                  AS LakeLastUpdateTimestamp,
    CASE 
          WHEN coalesce(MAIN.lakeDeletedTimestamp, ip.lakeDeletedTimestamp, TPU.lakeDeletedTimestamp) IS NOT NULL THEN {batch_effective_datetime} 
          ELSE NULL 
     END                                                                                AS lakeDeletedTimestamp
FROM temppartyuniverse TPU

    INNER JOIN standardised_subscribe.dbo_polmain MAIN
    -- to fetch policies that exists in main table
    ON UPPER(MAIN.polid) = UPPER(TPU.polid) AND UPPER(MAIN.unitpsu) = UPPER(TPU.unitpsu)
    AND {batch_effective_datetime} >= MAIN.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(MAIN.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    
    INNER JOIN standardised_subscribe.dbo_inpol ip
    -- to fetch policies inwards policies
    ON UPPER(ip.polid) = UPPER(TPU.polid) AND UPPER(ip.unitpsu) = UPPER(TPU.unitpsu)
    AND {batch_effective_datetime} >= ip.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(ip.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    
    LEFT JOIN standardised_subscribe.dbo_polmain UpdMain1
    -- to update the Main_Party_Policy_Section_Role_Indicator with Flag "1"
    ON UPPER(TPU.polid) = UPPER(UpdMain1.polid)
    AND UPPER(TPU.unitpsu) = UPPER(UpdMain1.unitpsu)
    AND TPU.party_code = UpdMain1.bkrno AND UPPER(TPU.role_type) = 'BROKER'
    AND {batch_effective_datetime} >= UpdMain1.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(UpdMain1.LakeValidToTimestamp, CURRENT_TIMESTAMP()) 
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwParty_Policy_Section_Role'
  viewTables = 'standardised_subscribe.dbo_Bkr, standardised_subscribe.dbo_PolAnlyCd, standardised_subscribe.dbo_PolInsd, standardised_subscribe.dbo_PolMain, standardised_subscribe.dbo_inpol, standardised_subscribe.dbo_insd, standardised_subscribe.dbo_anly'

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

# DBTITLE 1,Create Filter Standardised View
try:
    std_object_polanlycd = 'standardised_subscribe.dbo_polanlycd'
    current_bed = batchEffectiveDatetime
    queryWhereCondition_polanlycd_lead = """
    UPPER(TRIM(ty)) IN ('MARKETLEAD','AGREEPARTY','LLOYDSLEAD','SLPLEAD') AND NULLIF(trim(cd), '') IS NOT NULL
    """
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_polanlycd_lead = createStandardisedMultipleFilterView(std_object_polanlycd,
                                                                 prv_bed,
                                                                 current_bed,
                                                                 whereExpression,
                                                                 queryWhereCondition_polanlycd_lead,
                                                                 cursor,
                                                                 batchTaskId,
                                                                 adfPipelineName,
                                                                 clusterId,
                                                                 notebookName,
                                                                 errorLogFileLocation)
        union_df_polanlycd_lead=spark.read.table(std_object_polanlycd).unionByName(std_df_polanlycd_lead)
    else:
        union_df_polanlycd_lead=spark.read.table(std_object_polanlycd)
        
    union_df_polanlycd_lead.createOrReplaceTempView('standardised_subscribe_dbo_polanlycd_lead_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_polanlycd_lead_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create Filter Standardised View
try:
    std_object_anly = 'standardised_subscribe.dbo_anly'
    current_bed = batchEffectiveDatetime
    queryWhereCondition_anly = """
    Upper(Trim(anlyty)) IN ('LLOYDSLEAD', 'AGREEPARTY', 'MARKETLEAD', 'SLPLEAD') AND Nullif(Trim(anlycd), '') IS NOT NULL
    """
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_anly = createStandardisedMultipleFilterView(std_object_anly,
                                                           prv_bed,
                                                           current_bed,
                                                           whereExpression,
                                                           queryWhereCondition_anly,
                                                           cursor,
                                                           batchTaskId,
                                                           adfPipelineName,
                                                           clusterId,
                                                           notebookName,
                                                           errorLogFileLocation)
        union_df_anly=spark.read.table(std_object_anly).unionByName(std_df_anly)
    else :
        union_df_anly=spark.read.table(std_object_anly)
        
    union_df_anly.createOrReplaceTempView('standardised_subscribe_dbo_anly_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_anly_tmpvw created")

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
        
        #Replace value of batch effective date in SQL Query
        tempPartyUniverse_Query = tempPartyUniverse_df.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                                           missing_string = "'" +str(missing_string) + "'",
                                                           queryWhereCondition_polanlycd = str(queryWhereCondition_polanlycd),
                                                           queryWhereCondition_polinsd = str(queryWhereCondition_polinsd), 
                                                           queryWhereCondition_polanlycd_lead = str(queryWhereCondition_polanlycd_lead), 
                                                           queryWhereCondition_anly = str(queryWhereCondition_anly))
        #Creating dataframes with temp queries
        tempPartyUniverse_df1 = spark.sql(tempPartyUniverse_Query)
        
        #creating the views of table used in main sqlQuery
        tempPartyUniverse_df1.createOrReplaceTempView("tempPartyUniverse")
       
        #Replace value of batch effective date in SQL Query
        sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'"
                                  , missing_string = "'" +str(missing_string) + "'")
        
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