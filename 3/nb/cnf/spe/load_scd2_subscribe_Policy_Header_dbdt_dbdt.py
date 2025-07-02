# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_Policy_Header_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Policy_Header table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Rahul</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/03/24</td></tr>
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

# DBTITLE 1,Inward Policy IDs
inwards_policy_univQuery = """
SELECT polid,
       unitpsu,
       Max(lakelastupdatedate)                                                                AS LakeLastUpdateDate,
       Max(lakelastupdatetimestamp)                                                           AS LakeLastUpdateTimestamp,
       NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),
           '9999-12-31T01:01:01.001+0000')                                                    AS lakeDeletedTimestamp
FROM (
       SELECT Upper(polid)                                                                    AS polid,
       unitpsu,
       lakelastupdatedate,
       lakelastupdatetimestamp,
       lakeDeletedTimestamp
       FROM   standardised_subscribe.dbo_polmain main
       WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
       AND   {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
       
       UNION ALL
       
       SELECT Upper(polid)                                                                    AS polid,
       unitpsu,
       lakelastupdatedate,
       lakelastupdatetimestamp,
       lakeDeletedTimestamp
       FROM   standardised_subscribe.dbo_inpol inpol
       WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
       AND   {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
       
       UNION ALL
       
       SELECT Upper(polid)                                                                    AS polid,
       unitpsu,
       lakelastupdatedate,
       lakelastupdatetimestamp,
       lakeDeletedTimestamp
       FROM   standardised_subscribe.dbo_polanlycd analyst_cd
       WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
       AND   {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
       
       UNION ALL
       
       SELECT
       --producing broker--
       Upper(policy_broker.polid)                                                             AS polid,
       policy_broker.unitpsu,
       Greatest(policy_broker.lakelastupdatedate, broker.lakelastupdatedate)                  AS LakeLastUpdateDate,
       Greatest(policy_broker.lakelastupdatetimestamp, broker.lakelastupdatetimestamp)        AS LakeLastUpdateTimestamp,
       CASE 
           WHEN coalesce(policy_broker.lakeDeletedTimestamp, broker.lakeDeletedTimestamp) Is Not Null
           THEN {batch_effective_datetime} ELSE NULL 
       END                                                                                    AS lakeDeletedTimestamp
       FROM   standardised_subscribe.dbo_polbkr policy_broker
              
       INNER JOIN standardised_subscribe.dbo_bkr broker
       ON broker.bkrseqid = policy_broker.bkrseqid
       AND {batch_effective_datetime} >= policy_broker.LakeValidFromTimestamp 
       AND {batch_effective_datetime} < coalesce(policy_broker.LakeValidToTimestamp, current_timestamp())
       AND {batch_effective_datetime} >= broker.LakeValidFromTimestamp 
       AND {batch_effective_datetime} < coalesce(broker.LakeValidToTimestamp, current_timestamp())    
       
       UNION ALL
       
       SELECT Upper(polid)                                                                    AS polid,
       unitpsu,
       lakelastupdatedate,
       lakelastupdatetimestamp,
       lakeDeletedTimestamp
       FROM  standardised_subscribe.dbo_polpmdeds deds
       WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
       AND   {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
     ) x
GROUP BY 1, 2
"""

# COMMAND ----------

# DBTITLE 1,Policies which are present in Policy layer but not in the Inward Policy Universe
pol_lyr_univQuery = """
SELECT pm.polid,
       pm.unitpsu,
       max(Greatest(pm.lakelastupdatedate, inpols.lakelastupdatedate))                        AS LakeLastUpdateDate,
       max(Greatest(pm.lakelastupdatetimestamp, inpols.lakelastupdatetimestamp))              AS LakeLastUpdateTimestamp,
       CASE 
           WHEN max(pm.lakeDeletedTimestamp) Is Not Null THEN {batch_effective_datetime} ELSE NULL
       END                                                                                    AS lakeDeletedTimestamp
FROM (
        SELECT 
        polid, 
        unitpsu, 
        max(lakelastupdatedate)                                                               AS lakelastupdatedate, 
        max(lakelastupdatetimestamp)                                                          AS lakelastupdatetimestamp,  
        NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),
            '9999-12-31T01:01:01.001+0000')                                                   AS lakeDeletedTimestamp
        FROM standardised_subscribe.dbo_pollyr
        WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
        AND   {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
        AND Substring(Trim(polid), 8, 1) = 'X'
        GROUP BY
        polid, 
        unitpsu
) pm

-- excluding the policies present in the inward policies universe
LEFT JOIN inpol_universe inpols
ON inpols.polid = Upper(pm.polid)
AND inpols.unitpsu = pm.unitpsu
AND inpols.lakeDeletedTimestamp IS NULL

WHERE inpols.polid IS NULL
AND   inpols.unitpsu IS NULL 
GROUP  BY 1, 2
"""

# COMMAND ----------

# DBTITLE 1,Combining Policy IDs from Policy Layer Universe and Inward Policy Universe
policy_universe_inpol_pollyrQuery = """
SELECT Upper(pm.polid)                                                                        AS polid,
Upper(pm.unitpsu)                                                                             AS unitpsu,
pm.polty,
pm.dsc,
ip.calcln,
pm.umr,
'N'                                                                                           AS Package_Indicator,
'N'                                                                                           AS Program_Header_Indicator,
'N'                                                                                           AS Package_Header_Indicator,
Greatest(pm.lakelastupdatedate, ip.lakelastupdatedate)                                        AS LakeLastUpdateDate,
Greatest(pm.lakelastupdatetimestamp, ip.lakelastupdatetimestamp)                              AS LakeLastUpdateTimestamp,
CASE 
    WHEN coalesce(pm.lakeDeletedTimestamp, ip.lakeDeletedTimestamp) Is Not Null
    THEN {batch_effective_datetime} ELSE NULL
END                                                                                           AS lakeDeletedTimestamp
FROM   standardised_subscribe.dbo_polmain pm

INNER JOIN standardised_subscribe.dbo_inpol ip
ON Trim(Upper(ip.polid)) = Trim(Upper(pm.polid))
AND ip.unitpsu = pm.unitpsu
AND {batch_effective_datetime} >= pm.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(pm.LakeValidToTimestamp, current_timestamp())
AND {batch_effective_datetime} >= ip.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(ip.LakeValidToTimestamp, current_timestamp())
    
UNION ALL

--program pols
SELECT Upper(pm.polid),
Upper(pm.unitpsu),
pm.polty,
pm.dsc,
ip.calcln,
pm.umr,
'N'                                                                                           AS Package_Indicator,
'N'                                                                                           AS Program_Header_Indicator,
'N'                                                                                           AS Package_Header_Indicator,
Greatest(pm.lakelastupdatedate, t.lakelastupdatedate, ip.lakelastupdatedate)                  AS LakeLastUpdateDate,
Greatest(pm.lakelastupdatetimestamp, t.lakelastupdatetimestamp, ip.lakelastupdatetimestamp)   AS LakeLastUpdateTimestamp,
CASE 
    WHEN coalesce(pm.lakeDeletedTimestamp, t.lakeDeletedTimestamp, ip.lakeDeletedTimestamp) Is Not Null
    THEN {batch_effective_datetime} ELSE NULL
END                                                                                           AS lakeDeletedTimestamp
FROM   standardised_subscribe.dbo_pollyr pm

INNER JOIN standardised_subscribe.dbo_inpollyr ip
ON Upper(ip.polid) = Upper(pm.polid)
AND ip.unitpsu = pm.unitpsu
AND {batch_effective_datetime} >= pm.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(pm.LakeValidToTimestamp, current_timestamp())
AND {batch_effective_datetime} >= ip.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(ip.LakeValidToTimestamp, current_timestamp())
     
INNER JOIN pol_lyr_universe t
ON Upper(t.polid) = upper( pm.polid )
AND t.unitpsu = pm.unitpsu
AND {batch_effective_datetime} >= pm.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(pm.LakeValidToTimestamp, current_timestamp())
"""

# COMMAND ----------

# DBTITLE 1,Fetching Metric Columns
metric_calcnQuery = """
SELECT Trim(Upper(univ.polid))                                                              AS PolId,
univ.UnitPsu,
COALESCE(SCC, 'GBP')                                                                        AS Gross_Premium_Limit_Ccy,
0.00                                                                                        AS Gross_Premium_Limit_Our_Share,
CASE
  -- If the Settlement Currency is not Valid we convert it into GBP and fetch ROE 
  WHEN SCC IS NULL THEN CAST(COALESCE(pml.amt/roe.Rate, 0.00) AS DECIMAL(28,2))
  -- If the Settlement Currency is Valid we do not perform any conversion
  ELSE CAST(pml.amt AS DECIMAL(28,2))
END                                                                                         AS Gross_Premium_Limit,
Greatest(univ.lakelastupdatedate, pml.lakelastupdatedate, main.lakelastupdatedate, 
    roe.lakelastupdatedate, mds_curr.lakelastupdatedate)                                    AS lakelastupdatedate,
Greatest(univ.lakelastupdatetimestamp, pml.lakelastupdatetimestamp, main.lakelastupdatetimestamp, 
    roe.lakelastupdatetimestamp, mds_curr.lakelastupdatetimestamp)                          AS lakelastupdatetimestamp,
CASE 
    WHEN coalesce(univ.lakeDeletedTimestamp, pml.lakeDeletedTimestamp) Is Not Null 
    THEN {batch_effective_datetime} ELSE NULL 
END                                                                                         AS lakeDeletedTimestamp
FROM policy_universe univ

INNER JOIN standardised_subscribe.dbo_polmltlmt pml
ON  Upper(pml.polid) = Upper(univ.polid)
AND pml.unitpsu = univ.unitpsu
AND {batch_effective_datetime} >= pml.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(pml.LakeValidToTimestamp, current_timestamp())
AND Trim(Upper(pml.lmtty)) = 'GROSS PREMIUM LIMIT'

-- To fetch Policy Written Dates for calculating ROE
LEFT JOIN 
( 
  SELECT PolId, UnitPsu, make_Date(
                              SUBSTRING(WtnDt FROM 1 FOR 4), 
                              SUBSTRING(WtnDt FROM 5 FOR 2), 
                              SUBSTRING(WtnDt FROM 7 FOR 2)
                                  ) as WtnDt,
  lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp
  FROM standardised_subscribe.dbo_polmain
  WHERE lakeDeletedTimestamp IS NULL
  AND '2023-04-10T10:54:44.960' >= LakeValidFromTimestamp 
  AND '2023-04-10T10:54:44.960' < coalesce(LakeValidToTimestamp, current_timestamp())
) main
ON  Upper(univ.polid) = Upper(main.polid)
AND univ.unitpsu = main.unitpsu

-- To identify Valid Settlement Currencies that exists in MDS
LEFT JOIN
(
    SELECT PolId, UnitPsu, UPPER(Currency_Code) AS SCC,
    max(GREATEST(ent_cd.lakelastupdatedate, ent_curr.lakelastupdatedate))                   AS lakelastupdatedate,
    max(GREATEST(ent_cd.lakelastupdatetimestamp, ent_curr.lakelastupdatetimestamp))         AS lakelastupdatetimestamp
    FROM
    (
        SELECT PolId, UnitPsu, UnitId, lakelastupdatedate, lakelastupdatetimestamp
        FROM standardised_subscribe.dbo_inpolptpt
        WHERE lakeDeletedTimestamp IS NULL
        AND {batch_effective_datetime} >= lakeValidFromTimestamp 
        AND {batch_effective_datetime} < coalesce(lakeValidToTimestamp, current_timestamp())
    ) ent_cd
    LEFT JOIN
    (
        SELECT Business_Entity_Code, Currency_Code, lakelastupdatedate, lakelastupdatetimestamp
        FROM standardised_mdm.mdm_Entity_Currency_Type
        WHERE Entity_Currency_Type = 'Settlement_Currency'
        AND lakeDeletedTimestamp IS NULL
        AND {batch_effective_datetime} >= lakeValidFromTimestamp 
        AND {batch_effective_datetime} < coalesce(lakeValidToTimestamp, current_timestamp())
    ) ent_curr
    ON ent_cd.UnitId = ent_curr.Business_Entity_Code
    Group by PolId, UnitPsu, UPPER(Currency_Code)
) mds_curr
ON UPPER(pml.polid) = UPPER(mds_curr.polid)
AND pml.unitpsu = mds_curr.unitpsu
AND UPPER(pml.CcyPsu) = mds_curr.SCC

--to fetch rate of exchange for the policy
LEFT JOIN
(
  SELECT 
	Source_Currency,
	Quote_Date AS Exchange_Rate_Date,
    Rate,
	lakelastupdatedate,
    lakelastupdatetimestamp
	FROM
	(
	  SELECT ROW_NUMBER() Over(partition by Source_Currency, Target_Currency, Quote_Date 
 			ORDER BY lakeValidFromTimeStamp DESC) AS rnk, *
	  FROM Standardised_FX_Rate.Exchange_Rate_Daily
      WHERE Target_Currency = 'GBP' 
	  AND lakeDeletedTimestamp IS NULL
	  AND {batch_effective_datetime} >= lakeValidFromTimestamp 
	  AND {batch_effective_datetime} < coalesce(lakeValidToTimestamp, current_timestamp())
	) exch_daily
	WHERE rnk = 1
) roe
ON Upper(pml.CcyPsu) = roe.Source_Currency
AND SCC IS NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY univ.PolId, univ.UnitPsu 
  ORDER BY Abs(DateDiff(WtnDt, Exchange_Rate_Date)), Exchange_Rate_Date ASC) = 1
"""

# COMMAND ----------

# DBTITLE 1,Fetching No Package Section (Parent)
parent_pckgQuery = """
SELECT frpolid,
       frunitpsu,
       no_package_sections,
       Max(lakelastupdatedate)                                           AS lakelastupdatedate,
       Max(lakelastupdatetimestamp)                                      AS lakelastupdatetimestamp,
       NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') AS lakeDeletedTimestamp
FROM   (
        SELECT pll.frpolid,
        pll.frunitpsu,
        Count(*) OVER (partition BY Upper(pll.frpolid), pll.frunitpsu)   AS no_package_sections,
        greatest(pll.lakelastupdatedate, p.lakelastupdatedate)           AS lakelastupdatedate,
        Greatest(pll.lakelastupdatetimestamp, p.lakelastupdatetimestamp) AS lakelastupdatetimestamp,
        CASE 
            WHEN coalesce(pll.lakeDeletedTimestamp, p.lakeDeletedTimestamp) Is Not Null
            THEN {batch_effective_datetime} ELSE NULL
        END                                                              AS lakeDeletedTimestamp
        from standardised_subscribe.dbo_pollnk pll

        INNER JOIN policy_universe p
        ON  upper(p.polid) = upper(pll.frpolid)
        AND p.unitpsu = pll.frunitpsu
        AND {batch_effective_datetime} >= pll.LakeValidFromTimestamp 
        and {batch_effective_datetime} < coalesce(pll.LakeValidToTimestamp, current_timestamp())

        -- get the latest versions
        WHERE pll.lnkty = 2
        
        -- we do not want to consider deleted record for metrics
        AND pll.lakeDeletedTimestamp IS NULL
        ) x
GROUP BY 1, 2, 3
"""

# COMMAND ----------

# DBTITLE 1,Fetching No_Package_Sections (Child)
child_pckgQuery = """
SELECT topolid,
       tounitpsu,
       no_package_sections,
       Max(lakelastupdatedate)                                          AS lakelastupdatedate,
       Max(lakelastupdatetimestamp)                                     AS lakelastupdatetimestamp,
       NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') AS lakeDeletedTimestamp
FROM   (
       SELECT pll.topolid,
       pll.tounitpsu,
       Count(*) OVER (partition BY Upper(pll.frpolid), pll.frunitpsu)   AS no_package_sections,
       greatest(pll.lakelastupdatedate, p.lakelastupdatedate)           AS lakelastupdatedate,
       Greatest(pll.lakelastupdatetimestamp, p.lakelastupdatetimestamp) AS lakelastupdatetimestamp,
       CASE 
           when coalesce(pll.lakeDeletedTimestamp, p.lakeDeletedTimestamp) Is Not Null
           THEN {batch_effective_datetime} ELSE NULL 
       END                                                              AS lakeDeletedTimestamp
       from standardised_subscribe.dbo_pollnk pll

       INNER JOIN policy_universe p
       ON  upper(p.polid) = upper(pll.frpolid)
       AND p.unitpsu = pll.frunitpsu
       AND {batch_effective_datetime} >= pll.LakeValidFromTimestamp 
       and {batch_effective_datetime} < coalesce(pll.LakeValidToTimestamp, current_timestamp())

       -- get the latest versions
       WHERE pll.lnkty = 2
       
       -- we do not want to consider deleted record for metrics
       AND pll.lakeDeletedTimestamp IS NULL
       ) x
GROUP BY 1, 2, 3
"""

# COMMAND ----------

# DBTITLE 1,Fetching No_Programs (Parent)
pgm_prntQuery = """
SELECT frpolid,
       frunitpsu,
       no_programs,
       Max(lakelastupdatedate)                                          AS lakelastupdatedate,
       Max(lakelastupdatetimestamp)                                     AS lakelastupdatetimestamp,
       NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') AS lakeDeletedTimestamp
FROM   (
       SELECT DISTINCT pll.frpolid,
       pll.frunitpsu,
       Count(*) OVER (partition BY Upper(pll.frpolid), pll.frunitpsu)   AS no_programs,
       greatest(pll.lakelastupdatedate, p.lakelastupdatedate)           AS lakelastupdatedate,
       Greatest(pll.lakelastupdatetimestamp, p.lakelastupdatetimestamp) AS lakelastupdatetimestamp,
       CASE 
           WHEN coalesce(pll.lakeDeletedTimestamp, p.lakeDeletedTimestamp) Is Not Null
           THEN {batch_effective_datetime} ELSE NULL 
       END                                                              AS lakeDeletedTimestamp
       from standardised_subscribe.dbo_pollyrlnk pll

       INNER JOIN policy_universe p 
       ON upper(p.polid) = upper(pll.topolid)
       AND p.unitpsu = pll.tounitpsu
       AND {batch_effective_datetime} >= pll.LakeValidFromTimestamp 
       AND {batch_effective_datetime} < coalesce(pll.LakeValidToTimestamp, current_timestamp())

       -- get the latest versions
       WHERE pll.lnkty = 11
       
       -- we do not want to consider deleted record for metrics
       AND pll.lakeDeletedTimestamp IS NULL
       ) a
GROUP BY 1, 2, 3
"""

# COMMAND ----------

# DBTITLE 1,Fetching No_Programs (Child)
pgm_chldQuery = """
SELECT topolid,
       tounitpsu,
       no_programs,
       Max(lakelastupdatedate)                                          AS lakelastupdatedate,
       Max(lakelastupdatetimestamp)                                     AS lakelastupdatetimestamp,
       NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') AS lakeDeletedTimestamp
FROM   (
       SELECT DISTINCT pll.topolid,
       pll.tounitpsu,
       Count(*) OVER (partition BY Upper(pll.frpolid), pll.frunitpsu)   AS no_programs,
       greatest(pll.lakelastupdatedate, p.lakelastupdatedate)           AS lakelastupdatedate,
       Greatest(pll.lakelastupdatetimestamp, p.lakelastupdatetimestamp) AS lakelastupdatetimestamp,
       CASE 
           when coalesce(pll.lakeDeletedTimestamp, p.lakeDeletedTimestamp) Is Not Null
           THEN {batch_effective_datetime} ELSE NULL 
       END                                                              AS lakeDeletedTimestamp
       from  standardised_subscribe.dbo_pollyrlnk pll

       INNER JOIN policy_universe p 
       ON upper(p.polid) = upper(pll.topolid)
       AND p.unitpsu = pll.tounitpsu
       AND {batch_effective_datetime} >= pll.LakeValidFromTimestamp 
       AND {batch_effective_datetime} < coalesce(pll.LakeValidToTimestamp, current_timestamp())

       -- get the latest versions
       WHERE pll.lnkty = 11
       
       -- we do not want to consider deleted record for metrics
       AND pll.lakeDeletedTimestamp IS NULL
       ) a
GROUP BY topolid, tounitpsu, no_programs
"""

# COMMAND ----------

# DBTITLE 1,To fetch Business Type Code, Sub Type Code and Cover_Type_Code
bus_cvr_cdQuery = """
SELECT pm.polid,
       pm.unitpsu,
       CASE 
        WHEN bus_sub_ty.cd = '100' THEN 'BINDDEC'
        WHEN bus_sub_ty.cd = '101' THEN 'BINDNOD'
        WHEN bus_sub_ty.cd = '104' THEN 'DECLAR'
        WHEN bus_sub_ty.cd = '105' THEN 'LINEDEC' 
        WHEN bus_sub_ty.cd = '106' THEN 'LINENOD'
        WHEN bus_sub_ty.cd = '108' THEN 'OPNCOV'
        WHEN bus_sub_ty.cd = '109' and ipli.insdty = 'Reinsured' THEN 'OMFAC'
        WHEN bus_sub_ty.cd = '109' and ipli.insdty != 'Reinsured' THEN 'OMDIR' 
        WHEN bus_sub_ty.cd = '110' THEN 'TTYRI'
        WHEN bus_sub_ty.cd = '115' THEN 'TTYRET'
        WHEN bus_sub_ty.cd = '116' THEN 'SERCO'
        WHEN bus_sub_ty.cd = '117' THEN 'CONDEC'
        WHEN bus_sub_ty.cd = '118' THEN 'CONNOD'
        WHEN bus_sub_ty.cd = '119' THEN 'MASDEC'
        WHEN bus_sub_ty.cd = '120' THEN 'MASNOD'
        ELSE 'UNC' 
       END                                                                                             AS Business_Sub_Type_Code,
       CASE
         WHEN Trim(Upper(bndr.cd)) = 'FULL' THEN 'FB'
         WHEN Trim(Upper(bndr.cd)) = 'LIMITED' THEN 'LB'
         WHEN (
              -- Default Covers and Declarations on certain MOP codes to Full Binder to ensure included in Intrali feed.
              (Trim(Upper(p.derived_writing_pattern)) = 'SL' AND Upper(Trim(polty)) <> 'INPROP TTY' )
               OR (p.derived_writing_pattern = 'UF' AND Upper(Trim(ip.st)) IN ( 'DECLAR', 'DECLARCASH' ) ) 
               )
              AND Trim(mop.cd) IN ( '101', '106', '110', '116', '118', '120' ) THEN 'FB'
         ELSE NULL
       end                                                                                             AS Cover_Type_Code,
       Greatest(pm.lakelastupdatedate, ip.lakelastupdatedate, p.lakelastupdatedate, mop.lakelastupdatedate, 
       bndr.lakelastupdatedate, bus_sub_ty.lakelastupdatedate, ipli.lakelastupdatedate)                AS LakeLastUpdateDate,
       
       Greatest(pm.lakelastupdatetimestamp, ip.lakelastupdatetimestamp, p.lakelastupdatetimestamp, mop.lakelastupdatetimestamp,
       bndr.lakelastupdatetimestamp, bus_sub_ty.lakelastupdatetimestamp, ipli.lakelastupdatetimestamp) AS LakeLastUpdateTimestamp,
       CASE 
           WHEN pm.lakeDeletedTimestamp  Is Not Null THEN {batch_effective_datetime} ELSE NULL 
       END                                                                                             AS lakeDeletedTimestamp
FROM   
(
    select * from standardised_subscribe.dbo_polmain
    where {batch_effective_datetime} >= LakeValidFromTimestamp 
    and {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
)pm

LEFT JOIN    
(
    select 
    polid, 
    unitpsu, 
    case 
     when lakeDeletedTimestamp is not null then {missing_string} 
     else st 
    end as st, 
    lakelastupdatedate, 
    lakelastupdatetimestamp, 
    lakeDeletedTimestamp
    from   
    standardised_subscribe.dbo_inpol 
    where {batch_effective_datetime} >= LakeValidFromTimestamp 
    and {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
) ip
ON pm.polid = ip.polid
AND pm.unitpsu = ip.unitpsu

---added this section as logic for business sub type is updated
LEFT JOIN 
(
    select 
    polid, 
    unitpsu,
    case 
        when lakeDeletedTimestamp is not null then {missing_string} else cd 
    end as cd, 
    lakelastupdatedate,
    lakelastupdatetimestamp,
    lakeDeletedTimestamp
    from
    standardised_subscribe.dbo_polanlycd 
    where Trim(Upper(ty)) = 'MOP'
    AND {batch_effective_datetime} >= LakeValidFromTimestamp 
    and {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
)bus_sub_ty
ON pm.polid = bus_sub_ty.polid
and pm.unitpsu = bus_sub_ty.unitpsu

Left join 
(
    select 
    InsdId,
    polid,
    unitpsu,
    case 
        when lakedeletedtimestamp is not null then {missing_string} else coalesce(insdty, {missing_string}) 
    end as insdty,
    lakelastupdatedate,
    lakelastupdatetimestamp,
    lakedeletedtimestamp
    from
    standardised_subscribe.dbo_PolInsd
    where {batch_effective_datetime} >= LakeValidFromTimestamp 
    and {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp()) 
) ipli
ON pm.polid = ipli.PolId
AND pm.UnitPsu = ipli.UnitPsu
and pm.Insdid=ipli.InsdId
  
LEFT JOIN 
(
    select 
    polid, 
    unitpsu,
    case 
        when lakeDeletedTimestamp is not null then {missing_string} else cd 
    end as cd, 
    lakelastupdatedate,
    lakelastupdatetimestamp,
    lakeDeletedTimestamp
    from
    standardised_subscribe.dbo_polanlycd 
    where Trim(Upper(ty)) = 'MOP'
    AND {batch_effective_datetime} >= LakeValidFromTimestamp 
    and {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
)mop
ON pm.polid = mop.polid
and pm.unitpsu = mop.unitpsu

LEFT JOIN 
(
    select 
    polid, 
    unitpsu,
    case 
        when lakeDeletedTimestamp is not null then {missing_string} else cd 
    end as cd, 
    lakelastupdatedate,
    lakelastupdatetimestamp,
    lakeDeletedTimestamp
    from
    standardised_subscribe.dbo_polanlycd 
    where Trim(Upper(ty)) = 'BINDERTYPE'
    AND {batch_effective_datetime} >= LakeValidFromTimestamp 
    and {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
)bndr
ON pm.polid = bndr.polid
AND pm.unitpsu = bndr.unitpsu
                 
LEFT JOIN 
(
    select 
    polid,
    unitpsu,
    case 
        when lakeDeletedTimestamp is not null then {missing_string} else Derived_Writing_Pattern 
    end as Derived_Writing_Pattern,
    LakeLastUpdateDate,
    LakeLastUpdateTimestamp,
    lakeDeletedTimestamp
    from
(
    SELECT
    p1.polid, 
    p1.unitpsu,
    CASE
        WHEN Upper(Trim(pa_sc.cd)) IN ('CRLM', 'CRSW', 'CRJW' ) THEN 'SL'
        -- Specialist Consumer Products Override
        WHEN Upper(Trim(polty)) IN ('INPROP TTY', 'INWARD X/L' ) AND Upper(Trim(boc)) = 'RAD' THEN 'SL'
        WHEN Upper(Trim(polty)) IN ('INPROP TTY', 'INWARD X/L' ) AND Upper(Trim(boc)) <> 'RAD' THEN 'UF'
        WHEN ( Trim(pa_mop.cd) ) = '108' AND Upper(Trim(boc)) = 'RAD' THEN 'SL'
        WHEN ( Trim(pa_mop.cd) ) IN ('100', '101', '105', '106','110', '116', '117', '118' ) THEN 'SL'
        WHEN ( Trim(pa_mop.cd) ) IN ('104', '108', '109', '115', '119', '120' ) THEN 'UF'
        WHEN Upper(Trim(pa_mop.cd)) = 'UNCODED' THEN 'UF'
        ELSE 'UF'
    END AS Derived_Writing_Pattern,
    Greatest(p1.lakelastupdatedate, pa_mop.lakelastupdatedate, pa_sc.lakelastupdatedate)                AS LakeLastUpdateDate,
    Greatest(p1.lakelastupdatetimestamp, pa_mop.lakelastupdatetimestamp, pa_sc.lakelastupdatetimestamp) AS LakeLastUpdateTimestamp,
    CASE 
        when p1.lakeDeletedTimestamp Is Not Null THEN {batch_effective_datetime} ELSE NULL
    END                                                                                                 AS lakeDeletedTimestamp
FROM  
(
    select * from standardised_subscribe.dbo_polmain 
    WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
)p1
                  
LEFT JOIN 
(
    select 
    polid, 
    unitpsu,
    case 
        when lakeDeletedTimestamp is not null then {missing_string} else cd 
    end as cd, 
    lakelastupdatedate,
    lakelastupdatetimestamp,
    lakeDeletedTimestamp
    from
    standardised_subscribe.dbo_polanlycd 
    where Trim(Upper(ty)) = 'MOP'
    AND {batch_effective_datetime} >= LakeValidFromTimestamp 
    and {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
) pa_mop
ON p1.polid = pa_mop.polid
AND p1.unitpsu=pa_mop.unitpsu

LEFT JOIN 
(
    select 
    polid, 
    unitpsu,
    case 
        when lakeDeletedTimestamp is not null then {missing_string} else cd 
    end as cd, 
    lakelastupdatedate,
    lakelastupdatetimestamp,
    lakeDeletedTimestamp
    from
    standardised_subscribe.dbo_polanlycd 
    where Trim(Upper(ty)) = 'SUBCLASS'
    AND {batch_effective_datetime} >= LakeValidFromTimestamp 
    and {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
) pa_sc
ON p1.polid = pa_sc.polid
AND p1.unitpsu=pa_sc.unitpsu
) x
)p
ON pm.polid = p.polid
AND pm.unitpsu = p.unitpsu               
"""

# COMMAND ----------

# DBTITLE 1,To fetch Product Risk
prod_rskQuery = """
select
b.PolId,
b.UnitPsu,
COALESCE(NULLIF(trim(b.SectDsc),''),{missing_string}) AS Product_Risk,
LakeLastUpdateDate,
LakeLastUpdateTimestamp,
lakeDeletedTimestamp
FROM 
(
    SELECT UPPER(PolId) AS PolId, UnitPsu, SectTy, SectDsc, LakeLastUpdateDate, LakeLastUpdateTimestamp,lakeDeletedTimestamp,
    row_number() over(partition by UPPER(PolId), UnitPsu, SectTy 
    order by coalesce(lakeDeletedTimestamp, '9999-12-31T01:01:01.000') desc, SectNarrSeqNo desc) as rnk
    FROM standardised_subscribe.dbo_PolSectNarr
    WHERE upper(trim(SectTy)) IN ('PRODUCT RISK')
    AND {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
) b
where b.rnk = 1
"""

# COMMAND ----------

# DBTITLE 1,Joining all tables to bring in attributes and Updating indicator as 0
policy_header_iniQuery = """
SELECT 
    Policy_Header_Reference,
    Policy_Header_Description,
    Method_Of_Placement_Code,
    Method_Of_Placement_Name,
    Cover_Type_Code,
    Late_Order_Indicator,
    Renewal_Indicator,
    Gross_Premium_Income_Limit_Currency,
    Gross_Premium_Income_Limit_Amount,
    CASE WHEN Gross_Premium_Income_Limit_Percentage <= 0 THEN 0
       WHEN Gross_Premium_Income_Limit_Percentage > 100 THEN 100
       ELSE Gross_Premium_Income_Limit_Percentage 
    END                                           AS Gross_Premium_Income_Limit_Percentage,
    --Update Package Header for items not included in the pol layer link tables
    CASE 
        WHEN Package_Indicator = 'N' AND SUBSTRING(substring_index(Policy_Header_Reference,'_',1), 9, 1) = 'X' then 'Y'
        ELSE Package_Indicator 
    END                                           AS Package_Indicator,
    ----Update Program Header for items not included in the pol layer link tables--
    CASE 
        WHEN Program_Header_Indicator = 'N' AND SUBSTRING(substring_index(Policy_Header_Reference,'_',1), 8, 1) = 'X' then 'Y' 
        ELSE Program_Header_Indicator 
    END                                           AS Program_Header_Indicator,
    --Update Package Header for items not included in the pol layer link tables
    CASE 
        WHEN Package_Header_Indicator = 'N' AND SUBSTRING(substring_index(Policy_Header_Reference,'_',1), 9, 1) = 'X' then 'Y'
        ELSE Package_Header_Indicator 
    END                                           AS Package_Header_Indicator,
    Number_Program_Layers,
    Number_Package_Sections,
    Unique_Market_Reference,
    Product_Risk,
    Business_Type_Code,
    Business_Type_Name,
    Business_Sub_Type_Code,
    Business_Sub_Type_Name,
    Usage_Type_Code,
    LakeLastUpdateDate,
    LakeLastUpdateTimestamp,
    lakeDeletedTimestamp
FROM	
(
    SELECT 
    Concat(univ.polid, '_', univ.unitpsu)          AS Policy_Header_Reference,
    Coalesce(univ.dsc, {missing_string})           AS Policy_Header_Description,
    case 
        when method_ref.lakedeletedtimestamp is not null then {missing_mdm} 
        else Coalesce(method_ref.Method_Of_Placement_Code, {missing_mdm})  
    end                                            AS Method_Of_Placement_Code,
    case 
        when method_ref.lakedeletedtimestamp is not null then {missing_mdm} 
        else Coalesce(method_ref.Method_Of_Placement_Name, {missing_mdm})  
    end                                            AS Method_Of_Placement_Name,
    case 
        when cd.lakedeletedtimestamp is not null then {missing_string} 
        else Coalesce(cd.cover_type_code, {missing_string}) 
    end                                            AS Cover_Type_Code,
    'N'                                            AS Late_Order_Indicator,
    case
        WHEN renew_ind.lakedeletedtimestamp is null and renew_ind.topolid IS NOT NULL THEN 1
        ELSE 0
    end                                            AS Renewal_Indicator,
    case 
        when metric_calc.lakedeletedtimestamp is not null then {missing_string} 
        else coalesce(metric_calc.gross_premium_limit_ccy, {missing_string})                                   
    end                                            AS Gross_Premium_Income_Limit_Currency,
    cast(case 
        when metric_calc.lakedeletedtimestamp is not null then 0 
        else coalesce(metric_calc.gross_premium_limit, 0)                                       
    end as decimal(22,8))                          AS Gross_Premium_Income_Limit_Amount,
    cast(case 
        when metric_calc.lakedeletedtimestamp is not null then 0 
        else coalesce(metric_calc.gross_premium_limit_our_share, 0)                             
    end as decimal(22,8))                          AS Gross_Premium_Income_Limit_Percentage,
    case
        when  ph.lakedeletedtimestamp is null and ph.frpolid = univ.polid AND ph.frunitpsu = univ.unitpsu THEN 'Y'
        ELSE package_indicator
    end                                            AS Package_Indicator,
    case
        when pgm.lakedeletedtimestamp is null and pgm.frpolid = univ.polid AND pgm.frunitpsu = univ.unitpsu THEN 'Y'
        ELSE program_header_indicator
    end                                            AS Program_Header_Indicator,
    case
        when ph.lakedeletedtimestamp is null and ph.frpolid = univ.polid AND ph.frunitpsu = univ.unitpsu THEN 'Y'
        ELSE package_header_indicator
    end                                            AS Package_Header_Indicator,
    case
        when pgm.lakedeletedtimestamp is null and pgm.frpolid = univ.polid and pgm.frunitpsu = univ.unitpsu THEN pgm.no_programs
        ELSE 1
    end                                            AS Number_Program_Layers,
    case
        when ph.lakedeletedtimestamp is null and ph.frpolid = univ.polid AND ph.frunitpsu = univ.unitpsu THEN ph.no_package_sections
        ELSE 1
    end                                            AS Number_Package_Sections,
    Coalesce(univ.umr, {missing_string})           AS Unique_Market_Reference,
    case 
        when prod_rsk.lakedeletedtimestamp is not null then {missing_string} 
        else Coalesce(Nullif(Trim(prod_rsk.product_risk), ''), {missing_string}) 
    end                                            AS Product_Risk,
    case 
        when besub_ref.lakedeletedtimestamp is not null then {missing_mdm} 
        else Coalesce(besub_ref.Business_Type_Code, {missing_mdm}) 
    end                                            AS Business_Type_Code,
    case 
        when besub_ref.lakedeletedtimestamp is not null then {missing_mdm} 
        else Coalesce(besub_ref.Business_Type_Name, {missing_mdm}) 
    end                                            AS Business_Type_Name,
    case 
        when cd.lakedeletedtimestamp is not null then {missing_mdm} 
        else Coalesce(cd.Business_Sub_Type_Code, {missing_string}) 
    end                                            AS Business_Sub_Type_Code,
    case 
        when besub_ref.lakedeletedtimestamp is not null then {missing_mdm} 
        else Coalesce(besub_ref.Business_Sub_Type_Description, {missing_mdm}) 
    end AS Business_Sub_Type_Name,
    {missing_string}                               AS Usage_Type_Code,
    Greatest(univ.lakelastupdatedate, mop.lakelastupdatedate, cd.lakelastupdatedate, 
    metric_calc.lakelastupdatedate, ph.lakelastupdatedate, pgm.lakelastupdatedate, 
    renew_ind.lakelastupdatedate, prod_rsk.lakelastupdatedate, method_ref.lakelastupdatedate, 
    besub_ref.lakelastupdatedate)                  AS LakeLastUpdateDate,
    Greatest(univ.lakelastupdatetimestamp, mop.lakelastupdatetimestamp, cd.lakelastupdatetimestamp, 
    metric_calc.lakelastupdatetimestamp, ph.lakelastupdatetimestamp, pgm.lakelastupdatetimestamp, 
    renew_ind.lakelastupdatetimestamp, prod_rsk.lakelastupdatetimestamp, method_ref.lakelastupdatetimestamp, 
    besub_ref.lakelastupdatetimestamp)             AS LakeLastUpdateTimestamp,
    CASE 
        WHEN univ.lakeDeletedTimestamp Is Not Null THEN {batch_effective_datetime} ELSE NULL 
    END AS lakeDeletedTimestamp

FROM   policy_universe univ

--to fetch the method of placement codes
LEFT JOIN 
(select cd, polid, unitpsu, lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp 
from  standardised_subscribe.dbo_polanlycd where Trim(Upper(ty)) = 'MOP' 
AND {batch_effective_datetime} >= LakeValidFromTimestamp 
and {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
)mop
ON Upper(univ.polid) = Upper(mop.polid)
AND Upper(univ.unitpsu) = Upper(mop.unitpsu)
			
LEFT JOIN bus_cvr_cd cd
ON univ.polid = cd.polid
AND univ.unitpsu = cd.unitpsu
                
---MDS feild: Business sub type code and business type code 
LEFT JOIN standardised_mdm.mdm_business_sub_type besub_ref
ON trim(cd.business_sub_type_code) = trim(besub_ref.business_sub_type_code)
AND {batch_effective_datetime} >= besub_ref.lakevalidfromtimestamp
AND {batch_effective_datetime} < COALESCE(besub_ref.lakevalidtotimestamp, CURRENT_TIMESTAMP()) 
            
LEFT JOIN metric_calc
ON univ.polid = metric_calc.polid
AND univ.unitpsu = metric_calc.unitpsu

LEFT OUTER JOIN pgm_prnt pgm
ON pgm.frpolid = univ.polid
AND pgm.frunitpsu = univ.unitpsu

LEFT OUTER JOIN package_prnt ph
ON ph.frpolid = univ.polid
AND ph.frunitpsu = univ.unitpsu
                            
--we need to update renewable indicator flag, in case we bring in inactive records, the records are getting dropped---
LEFT JOIN 
(
  SELECT topolid, tounitpsu,
  Max(lakelastupdatedate)                          AS LakeLastUpdateDate,
  Max(lakelastupdatetimestamp)                     AS LakeLastUpdateTimestamp,
  NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') AS lakeDeletedTimestamp
  FROM   standardised_subscribe.dbo_pollnk
  WHERE  lnkty IN ( 1, 16 ) AND topolid <> frpolid
  AND {batch_effective_datetime} >= LakeValidFromTimestamp 
  and {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
  GROUP  BY topolid, tounitpsu
) renew_ind
ON univ.polid = renew_ind.topolid
AND renew_ind.tounitpsu = univ.unitpsu

LEFT JOIN prod_rsk
ON prod_rsk.polid = univ.polid
AND prod_rsk.unitpsu = univ.unitpsu
                
-- MDS join to fetch Method_Of_Placement_Name
LEFT JOIN standardised_mdm.mdm_method_of_placement_mapping method_ref
ON mop.cd = method_ref.Source_Method_Of_Placement_Code
AND  lower(source_name)='subscribe'
AND {batch_effective_datetime} >= method_ref.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(method_ref.LakeValidToTimestamp, current_timestamp())
) upd
"""

# COMMAND ----------

# DBTITLE 1,Prepare SQL Query
sqlQuery= """
SELECT 
policy_header_reference                            AS Policy_Header_Reference,
policy_header_description                          AS Policy_Header_Description,
unique_market_reference                            AS Unique_Market_Reference,
usage_type_code                                    AS Usage_Type_Code,
business_sub_type_code                             AS Business_Sub_Type_Code,
business_type_name                                 AS Business_Type_Description,
business_type_code                                 AS Business_Type_Code,
business_sub_type_name                             AS Business_Sub_Type_Description, 
method_of_placement_code                           AS Method_Of_Placement_Code,
method_of_placement_name                           AS Method_Of_Placement_Description,   
late_order_indicator                               AS Late_Order_Indicator,
       ---updated 0/1 to Y/N as per latest changes---
case 
    when renewal_indicator = 1 then 'Y' else 'N' 
end                                                AS Renewal_Indicator,
gross_premium_income_limit_currency                AS Gross_Premium_Income_Limit_Currency,
gross_premium_income_limit_amount                  AS Gross_Premium_Income_Limit_Amount,
gross_premium_income_limit_percentage              AS Gross_Premium_Income_Limit_Percentage,	   
       ---updated true/false to Y/N as per latest changes---
CASE
    WHEN upd1.lakedeletedtimestamp is null and univ.policy_header_reference = Concat(upd1.topolid, '_', upd1.tounitpsu) THEN 'Y'
    ELSE package_indicator
END                                                AS Package_Indicator,
       ---updated true/false to Y/N as per latest changes---
CASE
    WHEN upd2.lakedeletedtimestamp is null and univ.policy_header_reference = Concat(upd2.topolid, '_', upd2.tounitpsu) THEN 'Y'
    ELSE Program_Header_Indicator
END                                                AS Program_Header_Indicator,	   
-- removed update from child for Package_Header_Indicator on 30th Aug as per cofirmation from Alex---
-- CASE
--     WHEN upd1.lakedeletedtimestamp is null and univ.policy_header_reference = Concat(upd1.topolid, '_', upd1.tounitpsu) THEN 'Y'
-- ELSE Package_Header_Indicator
-- END                                                AS Package_Header_Indicator,  
Package_Header_Indicator,
CASE
    WHEN upd2.lakedeletedtimestamp is null and univ.policy_header_reference = Concat(upd2.topolid, '_', upd2.tounitpsu) THEN upd2.no_programs
    ELSE number_program_layers
END                                                AS Number_Of_Program_Layers,       
CASE
    WHEN upd1.lakedeletedtimestamp is null and univ.policy_header_reference = Concat(upd1.topolid, '_', upd1.tounitpsu) THEN upd1.no_package_sections
    ELSE number_package_sections
END                                                AS Number_Of_Package_Sections,
product_risk                                       AS Product_Risk, 
Greatest(univ.lakelastupdatedate, upd1.lakelastupdatedate, 
upd2.lakelastupdatedate)                           AS LakeLastUpdateDate,
Greatest(univ.lakelastupdatetimestamp, upd1.lakelastupdatetimestamp,
upd2.lakelastupdatetimestamp)                      AS LakeLastUpdateTimestamp,
CASE 
    WHEN univ.lakeDeletedTimestamp IS NOT NULL THEN {batch_effective_datetime} ELSE NULL 
END                                                AS lakeDeletedTimestamp
FROM   policy_header_ini univ

LEFT OUTER JOIN package_child upd1
ON univ.policy_header_reference = Concat(upd1.topolid, '_', upd1.tounitpsu)
             
LEFT OUTER JOIN pgm_chld upd2
ON univ.policy_header_reference = Concat(upd2.topolid, '_', upd2.tounitpsu)
 """

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwPolicy_Header'
  viewTables = 'standardised_Subscribe.dbo_PolMain,standardised_subscribe.dbo_InPol,standardised_subscribe.dbo_PolAnlyCd, standardised_subscribe.dbo_PolBkr, standardised_subscribe.dbo_PolPmDeds, standardised_subscribe.dbo_PolLyr, standardised_subscribe.dbo_InPolLyr, standardised_subscribe.dbo_PolMltLmt, standardised_subscribe.dbo_ROE, standardised_subscribe.dbo_PolLnk, standardised_subscribe.dbo_PolLyrLnk, standardised_subscribe.dbo_PolSectNarr, standardised_subscribe.dbo_Bkr, standardised_subscribe.dbo_PolInsd, standardised_mdm.mdm_business_sub_type, standardised_mdm.mdm_method_of_placement_mapping, standardised_mdm.mdm_Entity_Currency_Type, standardised_subscribe.dbo_inpolptpt'

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
    
        #Replace value of batch effective date in temp queries
        inwards_policy_univQuery = inwards_policy_univQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
        pol_lyr_univQuery = pol_lyr_univQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
        policy_universe_inpol_pollyrQuery = policy_universe_inpol_pollyrQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
        metric_calcnQuery = metric_calcnQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
        parent_pckgQuery = parent_pckgQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
        child_pckgQuery = child_pckgQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
        pgm_prntQuery = pgm_prntQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
        pgm_chldQuery = pgm_chldQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
        bus_cvr_cdQuery = bus_cvr_cdQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                                 missing_string = "'" + str(missing_string) + "'")
        prod_rskQuery = prod_rskQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                             missing_string = "'" + str(missing_string) + "'")
        policy_header_iniQuery = policy_header_iniQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'",
                                                               missing_string = "'" + str(missing_string) + "'", 
                                                               missing_mdm = "'" + str(missing_mdm) + "'")
        
        #Replace value of batch effective date in SQL Main Query
        sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                   missing_integer = (missing_integer), 
                                   missing_string = "'" + str(missing_string) + "'", 
                                   missing_mdm = "'" + str(missing_mdm) + "'")
        
        #create temp table for inward Policy IDs
        inwards_policy_univDf = spark.sql(inwards_policy_univQuery)
        inwards_policy_univDf.createOrReplaceTempView('inpol_universe')
        
        #create temp table for Policies which are present in Policy layer but not in the Inward Policy Universe
        pol_lyr_univDf = spark.sql(pol_lyr_univQuery)
        pol_lyr_univDf.createOrReplaceTempView('pol_lyr_universe')
        
        #create temp table for combining Policy IDs from Policy Layer Universe and Inward Policy Universe
        policy_universe_inpol_pollyrDf = spark.sql(policy_universe_inpol_pollyrQuery)
        policy_universe_inpol_pollyrDf.createOrReplaceTempView('policy_universe')
        
        #create temp table for fetching Metric Columns
        metric_calcnDf = spark.sql(metric_calcnQuery)
        metric_calcnDf.createOrReplaceTempView('metric_calc')
        
        #create temp table for fetching No Package Section (Parent)
        parent_pckgDf = spark.sql(parent_pckgQuery)
        parent_pckgDf.createOrReplaceTempView('package_prnt')
        
        #create temp table for fetching No_Package_Sections (Child)
        child_pckgDf = spark.sql(child_pckgQuery)
        child_pckgDf.createOrReplaceTempView('package_child')
        
        #create temp table for fetching No_Programs (Parent)
        pgm_prntDf = spark.sql(pgm_prntQuery)
        pgm_prntDf.createOrReplaceTempView('pgm_prnt')
        
        #create temp table for fetching No_Programs (Child)
        pgm_chldDf = spark.sql(pgm_chldQuery)
        pgm_chldDf.createOrReplaceTempView('pgm_chld')
        
        #create temp table to fetch Business Type Code, Sub Type Code and Cover_Type_Code
        bus_cvr_cdDf = spark.sql(bus_cvr_cdQuery)
        bus_cvr_cdDf.createOrReplaceTempView('bus_cvr_cd')
        
         #create temp table to fetch Product Risk
        prod_rskDf = spark.sql(prod_rskQuery)
        prod_rskDf.createOrReplaceTempView('prod_rsk')
        
        #create temp table for joining all tables to bring in attributes and Updating indicator as 0
        policy_header_iniDf = spark.sql(policy_header_iniQuery)
        policy_header_iniDf.createOrReplaceTempView('policy_header_ini')
        
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