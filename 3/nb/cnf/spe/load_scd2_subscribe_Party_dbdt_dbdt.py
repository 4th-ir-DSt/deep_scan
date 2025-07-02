# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_Party_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Party table in conformed zone</td></tr>
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
# MAGIC     <td>03-04-2023</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Updated parttion column queries for underwriter party codes in Union Query</td>
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
  from pyspark.sql.functions import rank,desc,col,lag, when, lit, concat,row_number,collect_list, coalesce, concat_ws, first,last,rank,max,lead,upper,trim,size, count
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
SELECT 
insdid,
Domicile_Location_Sub_Division_Code,
unitpsu,
lakelastupdatedate,
lakelastupdatetimestamp,
lakeDeletedTimestamp
FROM 
(
    SELECT 
    insdid,
    Domicile_Location_Sub_Division_Code,
    unitpsu,
    lakelastupdatedate,
    lakelastupdatetimestamp,
    lakeDeletedTimestamp
    FROM 
    (
        SELECT 
        insdid,
		cd                                                                                  AS Domicile_Location_Sub_Division_Code,
		unitpsu,
		Max(lakelastupdatedate)                                                             AS lakelastupdatedate,
		Max(lakelastupdatetimestamp)                                                        AS lakelastupdatetimestamp,
        NULLIF(MAX(COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01'))
            ,'9999-12-31T01:01:01')                                                         AS lakeDeletedTimestamp
        FROM 
        (
            SELECT 
            pinsd.insdid,
            panyl.cd,
            panyl.unitpsu,
            greatest(pmain.lakelastupdatedate, panyl.lakelastupdatedate, 
            ip.lakelastupdatedate, pinsd.lakelastupdatedate)                                AS lakelastupdatedate,
            greatest(pmain.lakelastupdatetimestamp, panyl.lakelastupdatetimestamp, 
            ip.lakelastupdatetimestamp, pinsd.lakelastupdatetimestamp)                      AS lakelastupdatetimestamp,
            CASE 
                WHEN coalesce(pmain.lakeDeletedTimestamp, panyl.lakeDeletedTimestamp, ip.lakeDeletedTimestamp, 
                    pinsd.lakeDeletedTimestamp) Is Not Null THEN {batch_effective_datetime} 
                ELSE NULL 
            END                                                                             AS lakeDeletedTimestamp
            FROM  standardised_subscribe.dbo_polmain pmain
             
            INNER JOIN standardised_subscribe_dbo_polanlycd_tmpvw panyl
            ON  upper(panyl.polid) = upper(pmain.polid)
            AND upper(panyl.unitpsu) = upper(pmain.unitpsu)
            AND upper(trim(panyl.ty)) IN ({string_filter_polanlycd}) AND NULLIF(trim(panyl.cd),'') IS NOT NULL
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

# DBTITLE 1,Active Brokers
bkr_activeQuery = """
SELECT trim(bkrcd)                                                                          AS bkrcd,
       upper(COALESCE(NULLIF(( bkrnm ), 'dc937b59892'), {missing_string}))                  AS bkrnm,
       upper(COALESCE(NULLIF(( bkrpsu ), 'dc937b59892'), {missing_string}))                 AS bkrpsu,
       COALESCE(NULLIF(telno, 'dc937b59892'), {missing_string})                             AS TelNo,
       COALESCE(NULLIF(addr1, 'dc937b59892'), {missing_string})                             AS Addr1,
       COALESCE(NULLIF(addr2, 'dc937b59892'), {missing_string})                             AS Addr2,
       COALESCE(NULLIF(addr3, 'dc937b59892'), {missing_string})                             AS Addr3,
       COALESCE(NULLIF(addr4, 'dc937b59892'), {missing_string})                             AS Addr4,
       lakelastupdatedate,
       lakelastupdatetimestamp,
       lakeDeletedTimestamp
--keeping only active records from the broker table
FROM   (
        SELECT bkrcd,
               bkrnm,
               bkrpsu,
               telno,
               addr1,
               addr2,
               addr3,
               addr4,
               lakelastupdatedate,
               lakelastupdatetimestamp,
               lakeDeletedTimestamp,
               ROW_NUMBER() OVER(PARTITION BY bkrcd, UPPER(bkrpsu) 
                 ORDER BY coalesce(lakeDeletedTimestamp, '9999-12-31T01:01:01.000') desc, bkrseqid DESC) AS rnk
        FROM   standardised_subscribe.dbo_bkr
        WHERE  {batch_effective_datetime} >= LakeValidFromTimestamp 
        AND    {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
        )x
WHERE  rnk = 1 
"""

# COMMAND ----------

# DBTITLE 1,Active Broker + USM
bkr_active_UsmQuery = """
SELECT bkrcd,
       bkrnm,
       bkrpsu,
       telno,
       addr1,
       addr2,
       addr3,
       addr4,
       {missing_mdm}  as Domicile_Location_Sub_Country_Code,
       {missing_mdm}  as Domicile_Location_Sub_Division_Code,
       {missing_mdm}  as Domicile_Location_Sub_Region_Code,
       lakelastupdatedate,
       lakelastupdatetimestamp,
       lakeDeletedTimestamp
FROM   bkr_active

UNION ALL

SELECT Trim(bkrcd)                                                                          AS bkrcd,
       Concat_ws('', 'Unknown Broker - ', bkrcd, ' - ', 
       COALESCE(Upper(Trim(NULLIF(bkrpsu, '?'))), {missing_string}))                        AS bkr_nm,
       Max(COALESCE(Upper(Trim(NULLIF(bkrpsu, '?'))), {missing_string}))                    AS bkrpsu,
       {missing_string}                                                                     AS TelNo,
       {missing_string}                                                                     AS Addr1,
       {missing_string}                                                                     AS Addr2,
       {missing_string}                                                                     AS Addr3,
       {missing_string}                                                                     AS Addr4,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Country_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Division_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Region_Code,
       max(usm_univ.lakelastupdatedate)                                                     AS lakelastupdatedate,
       max(usm_univ.lakelastupdatetimestamp)                                                AS lakelastupdatetimestamp,
       CASE when NULLIF(MAX(COALESCE(usm_univ.lakeDeletedTimestamp,'9999-12-31T01:01:01')),'9999-12-31T01:01:01')  Is Not Null
       THEN {batch_effective_datetime} ELSE NULL END                                        AS lakeDeletedTimestamp
FROM   
( select * from standardised_subscribe.dbo_usmmain U
WHERE  NOT EXISTS (SELECT DISTINCT bkrcd, bkrpsu
                       FROM   bkr_active x
                       WHERE  u.bkrcd = x.bkrcd
                       AND COALESCE(Upper(Trim(NULLIF(bkrpsu, '?'))), {missing_string}) = x.bkrpsu)
       AND {batch_effective_datetime} >= LakeValidFromTimestamp 
       AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp()) ) usm_univ
GROUP  BY 1, 2, 4, 5, 6, 7, 8, 9, 10, 11
"""

# COMMAND ----------

# DBTITLE 1,Query (Getting all Party details from each Party Type)
unionQuery = """

-- To fetch party details for Individual/Organisation
-- for each insdid there should be only one country_div. Otherwise update insdid as insdid_countryDiv
SELECT 
       case 
           when iso_ref.lakedeletedtimestamp is not null or div.lakedeletedtimestamp is not null then {missing_mdm} 
           else COALESCE(div.Location_Sub_Division_Code, {missing_mdm})
       end                                                                                  AS Domicile_Location_Sub_Division_Code,
       case 
           when iso_ref.lakedeletedtimestamp is not null or div.lakedeletedtimestamp is not null then {missing_mdm} 
           else COALESCE(div.Location_Country_Code, iso_ref.Location_Country_Code, {missing_mdm})
       end                                                                                  AS Domicile_Location_Sub_Country_Code,
       case 
           when iso_ref.lakedeletedtimestamp is not null then {missing_mdm} 
           else COALESCE(iso_ref.Location_Sub_Region_Code, {missing_mdm})
       end                                                                                  AS Domicile_Location_Sub_Region_Code,
       Concat(insd.insdid, '_', CtyD.Domicile_Location_Sub_Division_Code)                   AS PartyCode,
       COALESCE(NULLIF(TRIM(InsdNm),''),{missing_string})                                   AS PartyName,
       polinsd.role_type                                                                    AS role_type,
       {missing_string}                                                                     AS Contact_Number,
       {missing_string}                                                                     AS Pseudonym,
       {missing_string}                                                                     AS Address_Line_1,
       {missing_string}                                                                     AS Address_Line_2,
       {missing_string}                                                                     AS Address_Line_3,
       {missing_string}                                                                     AS Address_Line_4,
       {missing_string}                                                                     AS City,
       {missing_string}                                                                     AS Postal_Code,
       {missing_string}                                                                     AS Email,
       {missing_startdate}                                                                  AS Date_Incorporated,
       {missing_string}                                                                     AS Company_Number,
       {missing_string}                                                                     AS Standard_Industrial_Classification_Code,
       Greatest(insd.lakelastupdatedate, CtyD.lakelastupdatedate, polinsd.lakelastupdatedate, 
       iso_ref.lakelastupdatedate, div.lakelastupdatedate)                                  AS LakeLastUpdateDate,
       Greatest(insd.lakelastupdatetimestamp, CtyD.lakelastupdatetimestamp, polinsd.lakelastupdatetimestamp, 
       iso_ref.lakelastupdatetimestamp, div.lakelastupdatetimestamp)                        AS LakeLastUpdateTimestamp,
       CASE 
           WHEN COALESCE(insd.lakeDeletedTimestamp, CtyD.lakeDeletedTimestamp, polinsd.lakeDeletedTimestamp) 
               Is Not Null THEN {batch_effective_datetime} 
           ELSE NULL 
       END                                                                                  AS lakeDeletedTimestamp
FROM  standardised_subscribe.dbo_insd insd

INNER JOIN getcountrydiv CtyD
ON Trim(insd.insdid) = Trim(CtyD.insdid)

--- Updated from Left Join TO Inner Join because role type is grain and can not be defaulted 08th August 2022 ---
INNER JOIN 
(
    SELECT CASE
             WHEN Upper(insdty) IN ( 'INSURED', 'REINSURED', 'LESSEE', 'OBLIGOR', 'CEDENT', 'GUARANTOR' ) 
             THEN Upper(insdty) ELSE 'ADDITIONAL_INSURED'
           END                                                                              AS ROLE_TYPE,
           insdid                                                                           AS Party_Code,	
           Max(lakelastupdatedate)                                                          AS LakeLastUpdateDate,
           Max(lakelastupdatetimestamp)                                                     AS LakeLastUpdateTimestamp,
           NULLIF(MAX(COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01')),'9999-12-31T01:01:01') AS lakeDeletedTimestamp
    FROM   standardised_subscribe.dbo_polinsd
    WHERE  {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND    {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp()) 
    GROUP  BY CASE
                WHEN Upper(insdty) IN ( 'INSURED', 'REINSURED', 'LESSEE', 'OBLIGOR', 'CEDENT', 'GUARANTOR' )
                THEN Upper(insdty) ELSE 'ADDITIONAL_INSURED'
              END,
              insdid
) polinsd
ON insd.insdid = polinsd.party_code

LEFT JOIN 
(
  SELECT case when lakedeletedtimestamp is not null then null else source_location_code END             AS source_location_code,
         case when lakedeletedtimestamp is not null then null else Location_Sub_Division_Code END       AS iso_div_cd,
         case when lakedeletedtimestamp is not null then null else Location_Country_Code END            AS Location_Country_Code,
         case when lakedeletedtimestamp is not null then null else Location_Sub_Region_Code END         AS Location_Sub_Region_Code,
         lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp
  FROM   standardised_mdm.mdm_location_mapping
  WHERE  trim(lower(source_name)) = 'subscribe'
  AND    {batch_effective_datetime} >= lakevalidfromtimestamp
  AND    {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) iso_ref
ON CtyD.Domicile_Location_Sub_Division_Code = iso_ref.source_location_code

LEFT JOIN 
(
    SELECT 
    case when lakeDeletedTimestamp is not null then null else code end                                  AS div_cd,
    case when lakeDeletedTimestamp is not null then null else Location_Sub_Division_Code end            AS Location_Sub_Division_Code,
    case when lakeDeletedTimestamp is not null then null else Location_Country_Code end                 AS Location_Country_Code,
    lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp
    FROM  standardised_mdm.mdm_location_sub_division
    WHERE {batch_effective_datetime} >= lakevalidfromtimestamp
    AND   {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) div
ON iso_ref.iso_div_cd = div.div_cd
                  
WHERE  {batch_effective_datetime} >= insd.LakeValidFromTimestamp 
AND    {batch_effective_datetime} < coalesce(insd.LakeValidToTimestamp, current_timestamp())

UNION 

SELECT {missing_mdm}                                                                        AS Domicile_Location_Sub_Division_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Country_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Region_Code,
       upper(trim(anlycd))                                                                  AS PartyCode,
       REPLACE(anlynm, '  ', ' ')                                                           AS PartyName,
       CASE 
            WHEN UPPER(TRIM(anlyty)) = 'MARKETLEAD' THEN 'MARKET_LEAD'
            WHEN UPPER(TRIM(anlyty)) = 'LLOYDSLEAD' THEN 'LLOYDS_LEAD'
            WHEN UPPER(TRIM(anlyty)) = 'SLPLEAD'    THEN 'SLIP_LEAD'
            WHEN UPPER(TRIM(anlyty)) = 'AGREEPARTY' THEN 'AGREEMENT_PARTY'
            ELSE upper(trim(anlyty))
       END                                                                                  AS ROLE_TYPE,
      {missing_string}                  							                        AS Contact_Number,
      {missing_string}                  							                        AS Pseudonym,
      {missing_string}                  							                        AS Address_Line_1,
      {missing_string}                  							                        AS Address_Line_2,
      {missing_string}                  							                        AS Address_Line_3,
      {missing_string}                  							                        AS Address_Line_4,
      {missing_string}                  							                        AS City,
      {missing_string}                  							                        AS Postal_Code,
      {missing_string}                  							                        AS Email,
      {missing_startdate}                                                                   AS Date_Incorporated,
      {missing_string}                  							                        AS Company_Number,
      {missing_string}                  							                        AS Standard_Industrial_Classification_Code,
      lakelastupdatedate,
      lakelastupdatetimestamp,
      lakeDeletedTimestamp
FROM  standardised_subscribe_dbo_anly_tmpvw
WHERE Upper(Trim(anlyty)) IN ({string_filter_anly})
      AND Nullif(Trim(anlycd), '') IS NOT NULL
      AND {batch_effective_datetime} >= LakeValidFromTimestamp 
      AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())

UNION 

-- To fetch party details for active Broker
SELECT Domicile_Location_Sub_Division_Code,
       Domicile_Location_Sub_Country_Code,
       Domicile_Location_Sub_Region_Code,
       bkrcd                   										                        AS PartyCode,
       bkrnm                   										                        AS PartyName,
       'BROKER'                										                        AS ROLE_TYPE,
       telno                   										                        AS Contact_Number,
       bkrpsu                  										                        AS Pseudonym,
       addr1                   										                        AS Address_Line_1,
       addr2                   										                        AS Address_Line_2,
       addr3                   										                        AS Address_Line_3,
       addr4                   										                        AS Address_Line_4,
       {missing_string}                                                                     AS City,
       {missing_string}                                                                     AS Postal_Code,
       {missing_string}                                                                     AS Email,
       {missing_startdate}                                                                  AS Date_Incorporated,
       {missing_string}                                                                     AS Company_Number,
       {missing_string}                                                                     AS Standard_Industrial_Classification_Code,
       lakelastupdatedate,
       lakelastupdatetimestamp,
       lakeDeletedTimestamp
FROM   broker_usm_active_univ

UNION 

-- To fetch party details for Underwriter
SELECT {missing_mdm}                                                                        AS Domicile_Location_Sub_Division_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Country_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Region_Code,
       uwr                                                                                  AS PartyCode,
       COALESCE(nm , {missing_string})                                                      AS PartyName,
       'UNDERWRITER'                                                                        AS ROLE_TYPE,
       {missing_string}                                                                     AS Contact_Number,
       upper(trim(uwrpsu))                                                                  AS Pseudonym,
       {missing_string}               								                        AS Address_Line_1,
       {missing_string}               								                        AS Address_Line_2,
       {missing_string}               								                        AS Address_Line_3,
       {missing_string}               								                        AS Address_Line_4,
       {missing_string}               								                        AS City,
       {missing_string}               								                        AS Postal_Code,
       {missing_string}               								                        AS Email,
       {missing_startdate}              							                        AS Date_Incorporated,
       {missing_string}               								                        AS Company_Number,
       {missing_string}               								                        AS Standard_Industrial_Classification_Code,
       
       Greatest(main.lakelastupdatedate, uwr.lakelastupdatedate)                                  AS LakeLastUpdateDate,
       Greatest(main.lakelastupdatetimestamp, uwr.lakelastupdatetimestamp)                        AS LakeLastUpdateTimestamp,
     CASE 
           WHEN COALESCE(main.lakeDeletedTimestamp, uwr.lakeDeletedTimestamp) 
               Is Not Null THEN {batch_effective_datetime} 
           ELSE NULL 
       END                                                                                  AS lakeDeletedTimestamp
FROM 
(
    SELECT uwr, UnitPsu,lakelastupdatedate,lakelastupdatetimestamp,lakeDeletedTimestamp FROM standardised_subscribe.dbo_polmain
    WHERE Nullif(uwr, '') IS NOT NULL 
    AND {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
    QUALIFY ROW_NUMBER() OVER(PARTITION BY Uwr, UnitPsu 
        ORDER BY coalesce(lakeDeletedTimestamp, '9999-12-31T01:01:01.000') desc) = 1
) main
LEFT JOIN standardised_subscribe.dbo_uwr uwr
ON main.uwr = uwr.uwrpsu
and main.UnitPsu = uwr.UnitPsu
AND {batch_effective_datetime} >= uwr.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(uwr.LakeValidToTimestamp, current_timestamp())

UNION 

--- TO Fetch details from Claim Insured
SELECT 
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Division_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Country_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Region_Code,
       UPPER(SectDsc)                                                                       AS PartyCode,
       UPPER(SectDsc)                                                                       AS PartyName,
       'CLAIM_INSURED'                                                                      AS Role_Type,
       {missing_string}                                                                     AS Contact_Number,
       {missing_string}                                                                     AS Pseudonym,
       {missing_string}                                                                     AS Address_Line_1,
       {missing_string}                                                                     AS Address_Line_2,
       {missing_string}                                                                     AS Address_Line_3,
       {missing_string}                                                                     AS Address_Line_4,
       {missing_string}                                                                     AS City,
       {missing_string}                                                                     AS Postal_Code,
       {missing_string}                                                                     AS Email,
       {missing_startdate}                                                                  AS Date_Incorporated,
       {missing_string}                                                                     AS Company_Number,
       {missing_string}                                                                     AS Standard_Industrial_Classification_Code,
       max(lakelastupdatedate)                                                              AS lakelastupdatedate,
       max(lakelastupdatetimestamp)                                                         AS lakelastupdatetimestamp,
       NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') 
                                                                                            AS lakeDeletedTimestamp
FROM standardised_subscribe_dbo_trnsectnarr_insured_tmpvw
WHERE upper(sectty) IN ({string_filter_trnsectnarr_insured})
     AND {batch_effective_datetime} >= LakeValidFromTimestamp 
     AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
GROUP BY PartyCode, PartyName

UNION 

--- TO Fetch details from Claim Handler
SELECT 
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Division_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Country_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Region_Code,
       UPPER(SectDsc)                                                                       AS PartyCode,
       UPPER(SectDsc)                                                                       AS PartyName,
       'CLAIM_HANDLER'                                                                      AS Role_Type,
       {missing_string}                                                                     AS Contact_Number,
       {missing_string}                                                                     AS Pseudonym,
       {missing_string}                                                                     AS Address_Line_1,
       {missing_string}                                                                     AS Address_Line_2,
       {missing_string}                                                                     AS Address_Line_3,
       {missing_string}                                                                     AS Address_Line_4,
       {missing_string}                                                                     AS City,
       {missing_string}                                                                     AS Postal_Code,
       {missing_string}                                                                     AS Email,
       {missing_startdate}                                                                  AS Date_Incorporated,
       {missing_string}                                                                     AS Company_Number,
       {missing_string}                                                                     AS Standard_Industrial_Classification_Code,
       max(lakelastupdatedate)                                                              AS lakelastupdatedate,
       max(lakelastupdatetimestamp)                                                         AS lakelastupdatetimestamp,
       NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') 
                                                                                            AS lakeDeletedTimestamp
FROM standardised_subscribe_dbo_trnsectnarr_loss_adjuster_tmpvw
where upper(sectty) IN ({string_filter_trnsectnarr_loss_adjuster})
     AND {batch_effective_datetime} >= LakeValidFromTimestamp 
     AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
GROUP BY PartyCode, PartyName

UNION 

--- TO Fetch details from Claim Broker
SELECT 
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Division_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Country_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Region_Code,
       UPPER(SectDsc)                                                                       AS PartyCode,
       UPPER(SectDsc)                                                                       AS PartyName,
       'CLAIM_BROKER'                                                                       AS Role_Type,
       {missing_string}                                                                     AS Contact_Number,
       {missing_string}                                                                     AS Pseudonym,
       {missing_string}                                                                     AS Address_Line_1,
       {missing_string}                                                                     AS Address_Line_2,
       {missing_string}                                                                     AS Address_Line_3,
       {missing_string}                                                                     AS Address_Line_4,
       {missing_string}                                                                     AS City,
       {missing_string}                                                                     AS Postal_Code,
       {missing_string}                                                                     AS Email,
       {missing_startdate}                                                                  AS Date_Incorporated,
       {missing_string}                                                                     AS Company_Number,
       {missing_string}                                                                     AS Standard_Industrial_Classification_Code,
       max(lakelastupdatedate)                                                              AS lakelastupdatedate,
       max(lakelastupdatetimestamp)                                                         AS lakelastupdatetimestamp,
       NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') 
                                                                                            AS lakeDeletedTimestamp
FROM standardised_subscribe_dbo_trnsectnarr_broker_tmpvw
where upper(sectty) IN ({string_filter_trnsectnarr_broker})
     AND {batch_effective_datetime} >= LakeValidFromTimestamp 
     AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
GROUP BY PartyCode, PartyName

UNION 

--- TO Fetch details from ORIGINAL INSURED
SELECT 
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Division_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Country_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Region_Code,
       UPPER(SectDsc)                                                                       AS PartyCode,
       UPPER(SectDsc)                                                                       AS PartyName,
       'CLAIM_ORIGINAL_INSURED'                                                             AS Role_Type,
       {missing_string}                                                                     AS Contact_Number,
       {missing_string}                                                                     AS Pseudonym,
       {missing_string}                                                                     AS Address_Line_1,
       {missing_string}                                                                     AS Address_Line_2,
       {missing_string}                                                                     AS Address_Line_3,
       {missing_string}                                                                     AS Address_Line_4,
       {missing_string}                                                                     AS City,
       {missing_string}                                                                     AS Postal_Code,
       {missing_string}                                                                     AS Email,
       {missing_startdate}                                                                  AS Date_Incorporated,
       {missing_string}                                                                     AS Company_Number,
       {missing_string}                                                                     AS Standard_Industrial_Classification_Code,
       max(lakelastupdatedate)                                                              AS lakelastupdatedate,
       max(lakelastupdatetimestamp)                                                         AS lakelastupdatetimestamp,
       NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') 
                                                                                            AS lakeDeletedTimestamp
FROM standardised_subscribe_dbo_trnsectnarr_original_insured_tmpvw
where upper(sectty) IN ({string_filter_trnsectnarr_original_insured})
     AND {batch_effective_datetime} >= LakeValidFromTimestamp 
     AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
GROUP BY PartyCode, PartyName

UNION 

--- TO Fetch details from CLAIMANT
SELECT 
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Division_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Country_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Region_Code,
       UPPER(SectDsc)                                                                       AS PartyCode,
       UPPER(SectDsc)                                                                       AS PartyName,
       'CLAIM_CLAIMANT'                                                                     AS Role_Type,
       {missing_string}                                                                     AS Contact_Number,
       {missing_string}                                                                     AS Pseudonym,
       {missing_string}                                                                     AS Address_Line_1,
       {missing_string}                                                                     AS Address_Line_2,
       {missing_string}                                                                     AS Address_Line_3,
       {missing_string}                                                                     AS Address_Line_4,
       {missing_string}                                                                     AS City,
       {missing_string}                                                                     AS Postal_Code,
       {missing_string}                                                                     AS Email,
       {missing_startdate}                                                                  AS Date_Incorporated,
       {missing_string}                                                                     AS Company_Number,
       {missing_string}                                                                     AS Standard_Industrial_Classification_Code,
       max(lakelastupdatedate)                                                              AS lakelastupdatedate,
       max(lakelastupdatetimestamp)                                                         AS lakelastupdatetimestamp,
       NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') 
                                                                                            AS lakeDeletedTimestamp
FROM standardised_subscribe_dbo_trnsectnarr_claimant_tmpvw
where upper(sectty) IN ({string_filter_trnsectnarr_claimant})
     AND {batch_effective_datetime} >= LakeValidFromTimestamp 
     AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
GROUP BY PartyCode, PartyName

UNION 

--- TO Fetch details from Bureau Lead
SELECT 
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Division_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Country_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Region_Code,
       UPPER(SectDsc)                                                                       AS PartyCode,
       UPPER(SectDsc)                                                                       AS PartyName,
       'CLAIM_BUREAU_LEAD'                                                                  AS Role_Type,
       {missing_string}                                                                     AS Contact_Number,
       {missing_string}                                                                     AS Pseudonym,
       {missing_string}                                                                     AS Address_Line_1,
       {missing_string}                                                                     AS Address_Line_2,
       {missing_string}                                                                     AS Address_Line_3,
       {missing_string}                                                                     AS Address_Line_4,
       {missing_string}                                                                     AS City,
       {missing_string}                                                                     AS Postal_Code,
       {missing_string}                                                                     AS Email,
       {missing_startdate}                                                                  AS Date_Incorporated,
       {missing_string}                                                                     AS Company_Number,
       {missing_string}                                                                     AS Standard_Industrial_Classification_Code,
       max(lakelastupdatedate)                                                              AS lakelastupdatedate,
       max(lakelastupdatetimestamp)                                                         AS lakelastupdatetimestamp,
       NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') 
                                                                                            AS lakeDeletedTimestamp
FROM standardised_subscribe_dbo_trnsectnarr_bureau_lead_tmpvw
where upper(sectty) IN ({string_filter_trnsectnarr_bureau_lead})
     AND {batch_effective_datetime} >= LakeValidFromTimestamp 
     AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
GROUP BY PartyCode, PartyName

UNION 

--- TO Fetch details from LAWYER
SELECT 
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Division_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Country_Code,
       {missing_mdm}                                                                        AS Domicile_Location_Sub_Region_Code,
       UPPER(SectDsc)                                                                       AS PartyCode,
       UPPER(SectDsc)                                                                       AS PartyName,
       'CLAIM_LAWYER'                                                                       AS Role_Type,
       {missing_string}                                                                     AS Contact_Number,
       {missing_string}                                                                     AS Pseudonym,
       {missing_string}                                                                     AS Address_Line_1,
       {missing_string}                                                                     AS Address_Line_2,
       {missing_string}                                                                     AS Address_Line_3,
       {missing_string}                                                                     AS Address_Line_4,
       {missing_string}                                                                     AS City,
       {missing_string}                                                                     AS Postal_Code,
       {missing_string}                                                                     AS Email,
       {missing_startdate}                                                                  AS Date_Incorporated,
       {missing_string}                                                                     AS Company_Number,
       {missing_string}                                                                     AS Standard_Industrial_Classification_Code,
       max(lakelastupdatedate)                                                              AS lakelastupdatedate,
       max(lakelastupdatetimestamp)                                                         AS lakelastupdatetimestamp,
       NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') 
                                                                                            AS lakeDeletedTimestamp
FROM standardised_subscribe_dbo_trnsectnarr_lawyer_tmpvw
where upper(sectty) IN ({string_filter_trnsectnarr_lawyer})
     AND {batch_effective_datetime} >= LakeValidFromTimestamp 
     AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
GROUP BY PartyCode, PartyName
"""

# COMMAND ----------

# DBTITLE 1,Prepare SQL Query
sqlQuery= """
SELECT 
Concat(Trim(Upper(partycode)), '_', 
    Trim(COALESCE(Upper(NULLIF(pseudonym, '?')), {missing_string})), '_',
    Trim(Upper(role_type)))                                              AS Party_Reference,
---updated as per MDS changes---
case 
    when Trim(Lower(role_type)) = 'insured' then Domicile_Location_Sub_Division_Code else {missing_mdm} 
end                                                                      AS Domicile_Location_Sub_Division_Code,
---updated as per MDS changes---
case 
    when Trim(Lower(role_type)) = 'insured' then Domicile_Location_Sub_Country_Code else {missing_mdm} 
end                                                                      AS Domicile_Country_Alpha_3_Code,
---updated as per MDS changes---
case 
    when Trim(Lower(role_type)) = 'insured' then Domicile_Location_Sub_Region_Code else {missing_mdm} 
end                                                                      AS Domicile_Location_Sub_Region_Code,
Trim(Upper(PartyCode))                                                   AS Party_Code,
CASE 
    WHEN Trim(Lower(partyname)) = 'unknown broker - -' THEN 'UNKNOWN BROKER' ELSE Trim(Upper(partyname)) 
END                                                                      AS Party_Name,
contact_number                                                           AS Contact_Number,
Trim(COALESCE(Upper(NULLIF(pseudonym, '?')), {missing_string}))          AS Pseudonym,
address_line_1                                                           AS Address_Line_1,
address_line_2                                                           AS Address_Line_2,
address_line_3                                                           AS Address_Line_3,
address_line_4                                                           AS Address_Line_4,
email                                                                    AS Email,
date_incorporated                                                        AS Date_Incorporated,
company_number                                                           AS Company_Number,
standard_industrial_classification_code                                  AS Standard_Industrial_Classification_Code,
CASE 
    WHEN part_ref.lakedeletedtimestamp is null and part_ref.Code IS NOT NULL THEN 'Y' ELSE 'N'
END                                                                      AS Terms_Of_Business_Agreement_Indicator,
CASE 
    WHEN affi_ref.lakedeletedtimestamp is null and affi_ref.Code IS NOT NULL THEN 'Y' ELSE 'N'
END                                                                      AS Affiliate_Indicator,
postal_code                                                              AS Postal_Code,
city                                                                     AS City,
COALESCE(role_type, {missing_string})                                    AS Role_Type,
greatest(vw.lakelastupdatedate, part_ref.lakelastupdatedate, 
    affi_ref.lakelastupdatedate)                                         AS LakeLastUpdateDate,
greatest(vw.lakelastupdatetimestamp, part_ref.lakelastupdatetimestamp, 
    affi_ref.lakelastupdatetimestamp)                                    AS LakeLastUpdateTimestamp,
CASE 
    WHEN vw.lakeDeletedTimestamp Is Not Null THEN {batch_effective_datetime} ELSE NULL 
END                                                                      AS lakeDeletedTimestamp
FROM vwpartyentityunion vw

--MDS JOIN to fetch Terms_Of_Business_Agreement_Indicator
LEFT JOIN standardised_mdm.mdm_terms_of_business_agreement_indicator part_ref
ON   vw.PartyCode = part_ref.Party_Code
AND  lower(part_ref.Source_Name) = 'subscribe'
AND {batch_effective_datetime} >= part_ref.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(part_ref.LakeValidToTimestamp, current_timestamp())

--MDS JOIN to fetch Affiliate_Indicator
LEFT JOIN standardised_mdm.mdm_affiliate_indicator affi_ref
ON   vw.PartyCode = affi_ref.Party_Code
AND {batch_effective_datetime} >= affi_ref.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(affi_ref.LakeValidToTimestamp, current_timestamp())
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwParty'
  viewTables = 'standardised_subscribe.dbo_polmain, standardised_subscribe.dbo_PolAnlyCd, standardised_subscribe.dbo_inPol, standardised_subscribe.dbo_PolInsd, standardised_subscribe.dbo_bkr, standardised_subscribe.dbo_UsmMain, standardised_subscribe.dbo_Anly, standardised_subscribe.dbo_Insd, standardised_subscribe.dbo_Uwr, standardised_mdm.mdm_terms_of_business_agreement_indicator, standardised_mdm.mdm_affiliate_indicator, standardised_mdm.mdm_location_mapping, standardised_subscribe.dbo_trnsectnarr, standardised_mdm.mdm_location_sub_division'

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

# DBTITLE 1,Create Filter Standardised View For dbo_polanlycd (DOMICILE)
try:
    std_object_polanlycd = 'standardised_subscribe.dbo_polanlycd'
    current_bed = batchEffectiveDatetime
    chk_column_polanlycd = 'ty'
    apply_upper_trim_polanlycd = "y"
    filter_list_polanlycd = ['DOMICILE']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_polanlycd = createStandardisedFilterView(std_object_polanlycd,
                                                        prv_bed,
                                                        current_bed,
                                                        whereExpression,
                                                        chk_column_polanlycd,
                                                        apply_upper_trim_polanlycd,
                                                        filter_list_polanlycd,
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

# DBTITLE 1,Create Filter Standardised View For dbo_anly
try:
    std_object_anly = 'standardised_subscribe.dbo_anly'
    current_bed = batchEffectiveDatetime
    chk_column_anly = 'anlyty'
    apply_upper_trim_anly = "y"
    filter_list_anly = ['LLOYDSLEAD',
                        'AGREEPARTY',
                        'MARKETLEAD',
                        'SLPLEAD']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_anly = createStandardisedFilterView(std_object_anly,
                                                   prv_bed,
                                                   current_bed,
                                                   whereExpression,
                                                   chk_column_anly,
                                                   apply_upper_trim_anly,
                                                   filter_list_anly,
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

# DBTITLE 1,Create Filter Standardised View For dbo_trnsectnarr (INSURED)
try:
    std_object_trnsectnarr_insured = 'standardised_subscribe.dbo_trnsectnarr'
    current_bed = batchEffectiveDatetime    
    chk_column_trnsectnarr_insured = 'sectty'
    apply_upper_trim_trnsectnarr_insured = "y"
    filter_list_trnsectnarr_insured = ['INSURED']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_trnsectnarr_insured = createStandardisedFilterView(std_object_trnsectnarr_insured,
                                                                  prv_bed,
                                                                  current_bed,
                                                                  whereExpression,
                                                                  chk_column_trnsectnarr_insured,
                                                                  apply_upper_trim_trnsectnarr_insured,
                                                                  filter_list_trnsectnarr_insured,
                                                                  cursor,
                                                                  batchTaskId,
                                                                  adfPipelineName,
                                                                  clusterId,
                                                                  notebookName,
                                                                  errorLogFileLocation)
        
        union_df_trnsectnarr_insured=spark.read.table(std_object_trnsectnarr_insured).unionByName(std_df_trnsectnarr_insured)
    else :
        union_df_trnsectnarr_insured=spark.read.table(std_object_trnsectnarr_insured)
        
    union_df_trnsectnarr_insured.createOrReplaceTempView('standardised_subscribe_dbo_trnsectnarr_insured_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_trnsectnarr_insured_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create Filter Standardised View For dbo_trnsectnarr (LOSS ADJUSTER)
try:
    std_object_trnsectnarr_loss_adjuster = 'standardised_subscribe.dbo_trnsectnarr'
    current_bed = batchEffectiveDatetime
    chk_column_trnsectnarr_loss_adjuster = 'sectty'
    apply_upper_trim_trnsectnarr_loss_adjuster = "y"
    filter_list_trnsectnarr_loss_adjuster = ['LOSS ADJUSTER']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_trnsectnarr_loss_adjuster = createStandardisedFilterView(std_object_trnsectnarr_loss_adjuster,
                                                                        prv_bed,
                                                                        current_bed,
                                                                        whereExpression,
                                                                        chk_column_trnsectnarr_loss_adjuster,
                                                                        apply_upper_trim_trnsectnarr_loss_adjuster,
                                                                        filter_list_trnsectnarr_loss_adjuster,
                                                                        cursor,
                                                                        batchTaskId,
                                                                        adfPipelineName,
                                                                        clusterId,
                                                                        notebookName,
                                                                        errorLogFileLocation)
        union_df_trnsectnarr_loss_adjuster = spark.read.table(std_object_trnsectnarr_loss_adjuster).unionByName(std_df_trnsectnarr_loss_adjuster)
    else :
        union_df_trnsectnarr_loss_adjuster = spark.read.table(std_object_trnsectnarr_loss_adjuster)
        
    union_df_trnsectnarr_loss_adjuster.createOrReplaceTempView('standardised_subscribe_dbo_trnsectnarr_loss_adjuster_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_trnsectnarr_loss_adjuster_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create Filter Standardised View For dbo_trnsectnarr (BROKER)
try:
    std_object_trnsectnarr_broker = 'standardised_subscribe.dbo_trnsectnarr'
    current_bed = batchEffectiveDatetime
    chk_column_trnsectnarr_broker = 'sectty'
    apply_upper_trim_trnsectnarr_broker = "y"
    filter_list_trnsectnarr_broker = ['BROKER']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_trnsectnarr_broker = createStandardisedFilterView(std_object_trnsectnarr_broker,
                                                                 prv_bed,
                                                                 current_bed,
                                                                 whereExpression,
                                                                 chk_column_trnsectnarr_broker,
                                                                 apply_upper_trim_trnsectnarr_broker,
                                                                 filter_list_trnsectnarr_broker,
                                                                 cursor,
                                                                 batchTaskId,
                                                                 adfPipelineName,
                                                                 clusterId,
                                                                 notebookName,
                                                                 errorLogFileLocation)
        union_df_trnsectnarr_broker=spark.read.table(std_object_trnsectnarr_broker).unionByName(std_df_trnsectnarr_broker)
    else :
        union_df_trnsectnarr_broker=spark.read.table(std_object_trnsectnarr_broker)
        
    union_df_trnsectnarr_broker.createOrReplaceTempView('standardised_subscribe_dbo_trnsectnarr_broker_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_trnsectnarr_broker_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create Filter Standardised View For dbo_trnsectnarr (ORIGINAL INSURED)
try:
    std_object_trnsectnarr_original_insured = 'standardised_subscribe.dbo_trnsectnarr'
    current_bed = batchEffectiveDatetime
    chk_column_trnsectnarr_original_insured = 'sectty'
    apply_upper_trim_trnsectnarr_original_insured = "y"
    filter_list_trnsectnarr_original_insured = ['ORIGINAL INSURED']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_trnsectnarr_original_insured = createStandardisedFilterView(std_object_trnsectnarr_original_insured,
                                                                           prv_bed,
                                                                           current_bed,
                                                                           whereExpression,
                                                                           chk_column_trnsectnarr_original_insured,
                                                                           apply_upper_trim_trnsectnarr_original_insured,
                                                                           filter_list_trnsectnarr_original_insured,
                                                                           cursor,
                                                                           batchTaskId,
                                                                           adfPipelineName,
                                                                           clusterId,
                                                                           notebookName,
                                                                           errorLogFileLocation)
        union_df_trnsectnarr_original_insured = spark.read.table(std_object_trnsectnarr_original_insured).unionByName(std_df_trnsectnarr_original_insured)
    else :
        union_df_trnsectnarr_original_insured=spark.read.table(std_object_trnsectnarr_original_insured)
        
    union_df_trnsectnarr_original_insured.createOrReplaceTempView('standardised_subscribe_dbo_trnsectnarr_original_insured_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_trnsectnarr_original_insured_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create Filter Standardised View For dbo_trnsectnarr (CLAIMANT)
try:
    std_object_trnsectnarr_claimant = 'standardised_subscribe.dbo_trnsectnarr'
    current_bed = batchEffectiveDatetime
    chk_column_trnsectnarr_claimant = 'sectty'
    apply_upper_trim_trnsectnarr_claimant = "y"
    filter_list_trnsectnarr_claimant = ['CLAIMANT']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_trnsectnarr_claimant = createStandardisedFilterView(std_object_trnsectnarr_claimant,
                                                                   prv_bed,
                                                                   current_bed,
                                                                   whereExpression,
                                                                   chk_column_trnsectnarr_claimant,
                                                                   apply_upper_trim_trnsectnarr_claimant,
                                                                   filter_list_trnsectnarr_claimant,
                                                                   cursor,
                                                                   batchTaskId,
                                                                   adfPipelineName,
                                                                   clusterId,
                                                                   notebookName,
                                                                   errorLogFileLocation)
        
        union_df_trnsectnarr_claimant=spark.read.table(std_object_trnsectnarr_claimant).unionByName(std_df_trnsectnarr_claimant)
    else :
        union_df_trnsectnarr_claimant=spark.read.table(std_object_trnsectnarr_claimant)
        
    union_df_trnsectnarr_claimant.createOrReplaceTempView('standardised_subscribe_dbo_trnsectnarr_claimant_tmpvw')

    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_trnsectnarr_claimant_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create Filter Standardised View For dbo_trnsectnarr (BUREAU LEAD)
try:
    std_object_trnsectnarr_bureau_lead = 'standardised_subscribe.dbo_trnsectnarr'
    current_bed = batchEffectiveDatetime
    chk_column_trnsectnarr_bureau_lead = 'sectty'
    apply_upper_trim_trnsectnarr_bureau_lead = "y"
    filter_list_trnsectnarr_bureau_lead = ['BUREAU LEAD']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_trnsectnarr_bureau_lead = createStandardisedFilterView(std_object_trnsectnarr_bureau_lead,
                                                                      prv_bed,
                                                                      current_bed,
                                                                      whereExpression,
                                                                      chk_column_trnsectnarr_bureau_lead,
                                                                      apply_upper_trim_trnsectnarr_bureau_lead,
                                                                      filter_list_trnsectnarr_bureau_lead,
                                                                      cursor,
                                                                      batchTaskId,
                                                                      adfPipelineName,
                                                                      clusterId,
                                                                      notebookName,
                                                                      errorLogFileLocation)
        union_df_trnsectnarr_bureau_lead = spark.read.table(std_object_trnsectnarr_bureau_lead).unionByName(std_df_trnsectnarr_bureau_lead)
    else :
        union_df_trnsectnarr_bureau_lead=spark.read.table(std_object_trnsectnarr_bureau_lead)
        
    union_df_trnsectnarr_bureau_lead.createOrReplaceTempView('standardised_subscribe_dbo_trnsectnarr_bureau_lead_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_trnsectnarr_bureau_lead_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create Filter Standardised View For dbo_trnsectnarr (LAWYER)
try:
    std_object_trnsectnarr_lawyer = 'standardised_subscribe.dbo_trnsectnarr'
    current_bed = batchEffectiveDatetime
    chk_column_trnsectnarr_lawyer = 'sectty'
    apply_upper_trim_trnsectnarr_lawyer = "y"
    filter_list_trnsectnarr_lawyer = ['LAWYER']

    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_trnsectnarr_lawyer = createStandardisedFilterView(std_object_trnsectnarr_lawyer,
                                                                 prv_bed,
                                                                 current_bed,
                                                                 whereExpression,
                                                                 chk_column_trnsectnarr_lawyer,
                                                                 apply_upper_trim_trnsectnarr_lawyer,
                                                                 filter_list_trnsectnarr_lawyer,
                                                                 cursor,
                                                                 batchTaskId,
                                                                 adfPipelineName,
                                                                 clusterId,
                                                                 notebookName,
                                                                 errorLogFileLocation)
    
        union_df_trnsectnarr_lawyer=spark.read.table(std_object_trnsectnarr_lawyer).unionByName(std_df_trnsectnarr_lawyer)
    else :
        union_df_trnsectnarr_lawyer=spark.read.table(std_object_trnsectnarr_lawyer)
    
    union_df_trnsectnarr_lawyer.createOrReplaceTempView('standardised_subscribe_dbo_trnsectnarr_lawyer_tmpvw')

    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_trnsectnarr_lawyer_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create dataframe from standardised data
try:
   
   if dependentTableIsUpdated == True:
        
    #Creating temp tables for temp queries
    string_filter_polanlycd="','".join(filter_list_polanlycd)
    quote_String_polanlycd="'"+string_filter_polanlycd+"'"
    
    fetch_cty_divQuery = fetch_cty_divQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                                   missing_string = "'" + str(missing_string) + "'", 
                                                   missing_startdate = "'" + str(missing_startdate) + "'", 
                                                   missing_mdm = "'" + str(missing_mdm) + "'", 
                                                   string_filter_polanlycd=str(quote_String_polanlycd))
    fetch_cty_div = spark.sql(fetch_cty_divQuery)
    fetch_cty_div.createOrReplaceTempView('getCountryDiv')
    
    bkr_activeQuery = bkr_activeQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                             missing_string = "'" + str(missing_string) + "'", 
                                             missing_startdate = "'" +str(missing_startdate) + "'", 
                                             missing_mdm = "'" + str(missing_mdm) + "'")
    bkr_active = spark.sql(bkr_activeQuery)
    bkr_active.createOrReplaceTempView('bkr_active')
    
    bkr_active_UsmQuery = bkr_active_UsmQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                                     missing_string = "'" + str(missing_string) + "'", 
                                                     missing_startdate = "'" + str(missing_startdate) + "'", 
                                                     missing_mdm = "'" + str(missing_mdm) + "'")
    bkr_active_Usm = spark.sql(bkr_active_UsmQuery)
    bkr_active_Usm.createOrReplaceTempView('broker_usm_active_univ')
    
    string_filter_anly="','".join(filter_list_anly)
    quote_String_anly="'"+string_filter_anly+"'"
    
    string_filter_trnsectnarr_insured="','".join(filter_list_trnsectnarr_insured)
    quote_String_trnsectnarr_insured="'"+string_filter_trnsectnarr_insured+"'"
    
    string_filter_trnsectnarr_loss_adjuster="','".join(filter_list_trnsectnarr_loss_adjuster)
    quote_String_trnsectnarr_loss_adjuster="'"+string_filter_trnsectnarr_loss_adjuster+"'"
    
    string_filter_trnsectnarr_broker="','".join(filter_list_trnsectnarr_broker)
    quote_String_trnsectnarr_broker="'"+string_filter_trnsectnarr_broker+"'"
    
    string_filter_trnsectnarr_original_insured="','".join(filter_list_trnsectnarr_original_insured)
    quote_String_trnsectnarr_original_insured="'"+string_filter_trnsectnarr_original_insured+"'"
    
    string_filter_trnsectnarr_claimant="','".join(filter_list_trnsectnarr_claimant)
    quote_String_trnsectnarr_claimant="'"+string_filter_trnsectnarr_claimant+"'"
    
    string_filter_trnsectnarr_bureau_lead="','".join(filter_list_trnsectnarr_bureau_lead)
    quote_String_trnsectnarr_bureau_lead="'"+string_filter_trnsectnarr_bureau_lead+"'"
    
    string_filter_trnsectnarr_lawyer="','".join(filter_list_trnsectnarr_lawyer)
    quote_String_trnsectnarr_lawyer="'"+string_filter_trnsectnarr_lawyer+"'"
    
    unionQuery = unionQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                   missing_string = "'" + str(missing_string) + "'", 
                                   missing_startdate = "'" +str(missing_startdate) + "'", 
                                   missing_mdm = "'" + str(missing_mdm) + "'", 
                                   string_filter_anly=str(quote_String_anly), 
                                   string_filter_trnsectnarr_insured=str(quote_String_trnsectnarr_insured), 
                                   string_filter_trnsectnarr_loss_adjuster=str(quote_String_trnsectnarr_loss_adjuster), 
                                   string_filter_trnsectnarr_broker=str(quote_String_trnsectnarr_broker), 
                                   string_filter_trnsectnarr_original_insured=str(quote_String_trnsectnarr_original_insured), 
                                   string_filter_trnsectnarr_claimant=str(quote_String_trnsectnarr_claimant), 
                                   string_filter_trnsectnarr_bureau_lead=str(quote_String_trnsectnarr_bureau_lead), 
                                   string_filter_trnsectnarr_lawyer=str(quote_String_trnsectnarr_lawyer))
    dfunion = spark.sql(unionQuery)
    dfunion.createOrReplaceTempView('vwPartyEntityUnion')
    
    #Replace value of batch effective date in SQL Query
    sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                               missing_string = "'" +str(missing_string) + "'", 
                               missing_startdate = "'" +str(missing_startdate) + "'", 
                               missing_mdm = "'" + str(missing_mdm) + "'")
    
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