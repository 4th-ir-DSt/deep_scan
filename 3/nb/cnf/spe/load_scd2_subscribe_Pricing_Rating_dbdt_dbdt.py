# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_pricing_rating_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Pricing & Rating table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Akhilesh</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/03/21</td></tr>
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

#----FETCHING ACCOUNTING PERIOD FROM SUBSCRIBE----------#

# src_acc_perd="select accprd from standardised_subscribe.dbo_msgaccprdst where trim(lower(st)) = 'o' AND lower(trim(trncgy))='usm' AND {batch_effective_datetime} >= LakeValidFromTimestamp AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())"

#----FETCHING ACCOUNTING PERIOD FROM MDM----------#

acc_perd_match="select array_join(array_sort(collect_set(Accounting_Period)),',') from standardised_mdm.mdm_accounting_period_mapping where trim(lower(source_name)) = 'subscribe' AND accounting_period_close_date_time is null AND {batch_effective_datetime} >= LakeValidFromTimestamp AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())"

acc_perd_unmatch="select Accounting_Period from standardised_mdm.mdm_accounting_period_mapping where trim(lower(source_name)) = 'subscribe' AND accounting_period_close_date_time is null AND {batch_effective_datetime} >= LakeValidFromTimestamp AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp()) order by Accounting_Period ASC limit 1"


# COMMAND ----------

# DBTITLE 1,Prepare SQL Query
sqlQuery = """ 
WITH vwPolSect AS
(
SELECT b.Polid, b.Unitpsu
	 , MAX(CASE
		 	    WHEN UPPER(TRIM(b.Sectty)) = 'BENCHMARK LOSS RATIO' THEN TRIM(b.Sectdsc)
		 	END)                                                                   AS Bench_Loss_Ratio
	 , MAX(CASE
		 		WHEN UPPER(TRIM(b.Sectty)) = 'NO RENEWAL LINK-OVERRIDE RATE CHNG' THEN TRIM(b.Sectdsc)
		 	END)                                                                   AS No_Renewal_Link_Override_Rate_Change
	 , MAX(CASE
		 		WHEN UPPER(TRIM(b.Sectty)) = 'BENCHMARK %' THEN TRIM(b.Sectdsc)
		 	END)                                                                   AS Benchmark_Percentage
	 , MAX(CASE
		 		WHEN UPPER(TRIM(b.Sectty)) = 'ULR %' THEN TRIM(b.Sectdsc)
		 	END)                                                                   AS Risk_Model_ULR
	 , MAX(b.Lakelastupdatedate)                                                   AS LakeLastUpdateDate
	 , MAX(b.Lakelastupdatetimestamp)                                              AS LakeLastUpdateTimestamp
    FROM   
    (
        SELECT Polid, Unitpsu, Sectty, Sectdsc,
               Lakelastupdatedate, Lakelastupdatetimestamp, LakeDeletedTimestamp,
               ROW_NUMBER() OVER(PARTITION BY UPPER(Polid), Unitpsu, Sectty ORDER BY sectnarrseqno DESC)  AS rnk
        FROM   standardised_subscribe.dbo_polsectnarr
        WHERE UPPER(TRIM(Sectty)) IN  ('BENCHMARK LOSS RATIO', 'BENCHMARK %', 'ULR %', 'NO RENEWAL LINK-OVERRIDE RATE CHNG')
        AND   {batch_effective_datetime} >= LakeValidFromTimestamp 
        AND   {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
        -- we are not considering deleted records for metrics, while aggregating the metrics
        AND   lakeDeletedTimestamp IS NULL
    ) b
    WHERE b.Rnk = 1
    GROUP BY b.Polid, b.Unitpsu
),

--**Query shared by Canopius**--

Premium AS 
(
      SELECT pm.PolId, pm.UnitPsu, rsk.Cd, rsk.SeqNo, pm.OCC, pm.SCC
	 , cast(pm.MktOrigPMAmt * (rsk.Pctg / 100) as decimal(28,2))                   AS Whole_OCC
	 , cast(pm.Calc * (rsk.Pctg / 100) as decimal(28,2))                           AS Whole_SCC
	 , GREATEST (pm.Lakelastupdatedate, rsk.Lakelastupdatedate, up.Lakelastupdatedate, 
        pl.Lakelastupdatedate, pcd.Lakelastupdatedate, ipp.Lakelastupdatedate)     AS LakeLastUpdateDate
	 , GREATEST (pm.Lakelastupdatetimestamp, rsk.Lakelastupdatetimestamp, up.Lakelastupdatetimestamp, pl.Lakelastupdatetimestamp, 
        pcd.Lakelastupdatetimestamp, ipp.Lakelastupdatetimestamp)                  AS LakeLastUpdateTimestamp
	 , CASE 
	      WHEN COALESCE ( pm.LakeDeletedTimestamp, rsk.LakeDeletedTimestamp, up.LakeDeletedTimestamp,
              pl.LakeDeletedTimestamp, pcd.LakeDeletedTimestamp, ipp.LakeDeletedTimestamp) IS NULL THEN NULL 
          ELSE {batch_effective_datetime} 
       END                                                                         AS LakeDeletedTimestamp
     FROM 

    --bringing in Pm Amount for each Policy from the Premium table
    (
         SELECT Polid, Unitpsu, Scc, Occ
         , SUM( MktOrigPMAmt )                                                     AS MktOrigPMAmt
         , SUM( MktOrigPMAmt * 
           CASE 
               WHEN OCC = SCC THEN 1 
               ELSE cast( 1 / NULLIF(SettROE, 0) as decimal(28,18) )  
           END )                                                                   AS Calc
         , MAX(lakelastupdatedate)                                                 AS LakeLastUpdateDate
         , MAX(lakelastupdatetimestamp)                                            AS LakeLastUpdateTimestamp
         , NULLIF(MAX(COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000'))
                 ,'9999-12-31T01:01:01.001+0000')                                  AS LakeDeletedTimestamp
         FROM  standardised_subscribe.dbo_polpm
         WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
         AND   {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
         
         -- we are not considering deleted records for metrics, while aggregating the metrics
         AND   lakeDeletedTimestamp IS NULL
         
         GROUP BY 1,2,3,4
    ) pm

    --to consider policies that are coming in from line 
    INNER JOIN
    (
         SELECT Polid, Unitpsu
         , MAX(lakelastupdatedate)                                                 AS LakeLastUpdateDate
         , MAX(lakelastupdatetimestamp)                                            AS LakeLastUpdateTimestamp
         , NULLIF(MAX(COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000'))
                 ,'9999-12-31T01:01:01.001+0000')                                  AS LakeDeletedTimestamp
         FROM  standardised_subscribe.dbo_inpolptpt
         WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
         AND   {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
         GROUP BY 1,2
    ) ipp
    ON pm.PolId = ipp.PolId
    AND pm.UnitPsu = ipp.UnitPsu

    --bringing in Percentage from Anlycdsplt table to split the amount from Premium table among different risk codes
    INNER JOIN 
    (
         SELECT Polid, Unitpsu, Cd, Pctg,SeqNo,
         LakeLastUpdateDate, LakeLastUpdateTimestamp, LakeDeletedTimestamp
         FROM standardised_subscribe_dbo_AnlyCdSplt_tmpvw
         WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
         AND   {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
         AND   Ty = ({string_filter_AnlyCdSplt})
    ) rsk 
    ON rsk.PolId = pm.PolId 
    AND rsk.UnitPsu = pm.UnitPsu

    --Keeping only inward policies--
    INNER JOIN standardised_subscribe.dbo_inpol up 
    ON up.Polid = pm.Polid 
    AND up.UnitPsu = pm.UnitPsu
    AND {batch_effective_datetime} >= up.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < coalesce(up.LakeValidToTimestamp, current_timestamp())
    
    --Keeping only those policies that are present in the PolMain table--
    INNER JOIN standardised_subscribe.dbo_polmain pl 
    ON pl.polid = pm.polid 
    AND pl.UnitPsu = pm.UnitPsu
    AND {batch_effective_datetime} >= pl.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < coalesce(pl.LakeValidToTimestamp, current_timestamp())
     
    INNER JOIN standardised_subscribe_dbo_PolAnlyCd_tmpvw pcd
    ON pm.PolId = pcd.polid 
    AND pm.UnitPsu = pcd.UnitPsu
    AND {batch_effective_datetime} >= pcd.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < coalesce(pcd.LakeValidToTimestamp, current_timestamp())
    WHERE pcd.ty = ({string_filter_PolAnlyCd})
),

METRIC AS
(
     SELECT current_metric.Polid 
     , current_metric.Unitpsu
     , Original_Currency
     , Settlement_Currency
     , Lloyds_Risk_Code
     , Lloyds_Risk_Code_Sequence_Number
     , Current_Whole_Premium_Written_OCC                                           AS Current_Whole_Premium_Written_OCC
     , Current_Whole_Premium_Written_SCC                                           AS Current_Whole_Premium_Written_SCC
     , ep.Whole_OCC                                                                AS Expiring_Whole_Premium_Written_OCC
     , ep.Whole_SCC                                                                AS Expiring_Whole_Premium_Written_SCC
     , GREATEST (current_metric.Lakelastupdatedate , ep.Lakelastupdatedate, 
         frpol.Lakelastupdatedate)                                                 AS LakeLastUpdateDate
     , GREATEST (current_metric.Lakelastupdatetimestamp, ep.Lakelastupdatetimestamp, 
         frpol.Lakelastupdatetimestamp)                                            AS Lakelastupdatetimestamp
     , CASE 
	      WHEN current_metric.LakeDeletedTimestamp IS NULL THEN NULL ELSE {batch_effective_datetime} 
       END                                                                         AS LakeDeletedTimestamp
     FROM
    (
        SELECT  cp.Polid
        , cp.Unitpsu
        , cp.Whole_OCC                                                             AS Current_Whole_Premium_Written_OCC
        , cp.Whole_SCC                                                             AS Current_Whole_Premium_Written_SCC
        , cp.OCC                                                                   AS Original_Currency
        , cp.SCC                                                                   AS Settlement_Currency
        , cp.Cd                                                                    AS Lloyds_Risk_Code
        , cp.SeqNo                                                                 AS Lloyds_Risk_Code_Sequence_Number
        , GREATEST ( cp.Lakelastupdatedate, pl.Lakelastupdatedate )                AS LakeLastUpdateDate
        , GREATEST ( cp.Lakelastupdatetimestamp, pl.Lakelastupdatetimestamp )      AS LakeLastUpdateTimestamp
        , CASE 
             WHEN COALESCE(cp.LakeDeletedTimestamp, pl.lakeDeletedTimestamp) IS NULL THEN NULL 
             ELSE {batch_effective_datetime} 
          END                                                                      AS LakeDeletedTimestamp
        FROM 
        (
            SELECT  Polid, Unitpsu, OCC, SCC, CD, Whole_OCC, Whole_SCC, SeqNo,
            Lakelastupdatedate, Lakelastupdatetimestamp, LakeDeletedTimestamp
            FROM Premium
        ) cp
    
        INNER JOIN 
        (
            SELECT  Topolid, ToUnitPsu
             , MAX(LakeLastUpdateDate)                                             AS LakeLastUpdateDate
             , MAX(LakeLastUpdateTimestamp)                                        AS LakeLastUpdateTimestamp
             , NULLIF(MAX(COALESCE(lakeDeletedTimestamp,
                 '9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000')  AS LakeDeletedTimestamp
            FROM standardised_subscribe.dbo_Pollnk
            WHERE lnkty=1
            AND {batch_effective_datetime} >= LakeValidFromTimestamp 
            AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
            GROUP BY 1, 2
        ) pl
        ON cp.Polid=pl.Topolid
        AND cp.UnitPsu=pl.ToUnitPsu
    ) current_metric

    LEFT JOIN 
    (
         SELECT
           CASE WHEN Lakedeletedtimestamp IS NOT NULL THEN NULL ELSE Frpolid END   AS Frpolid
         , CASE WHEN Lakedeletedtimestamp IS NOT NULL THEN NULL ELSE FrUnitPsu END AS FrUnitPsu
         , MAX(LakeLastUpdateDate)                                                 AS LakeLastUpdateDate
         , MAX(LakeLastUpdateTimestamp)                                            AS LakeLastUpdateTimestamp
         , NULLIF(MAX(COALESCE(lakeDeletedTimestamp,
             '9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000')      AS LakeDeletedTimestamp
         FROM standardised_subscribe.dbo_Pollnk
         WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
         AND   {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
         GROUP BY 1, 2
    ) frpol
    ON frpol.frpolid=current_metric.polid
    AND frpol.FrUnitPsu=current_metric.UnitPsu

    LEFT JOIN 
    (
        SELECT 
          CASE WHEN Lakedeletedtimestamp IS NOT NULL THEN NULL ELSE Polid END      AS Polid
        , CASE WHEN Lakedeletedtimestamp IS NOT NULL THEN NULL ELSE UnitPsu END    AS UnitPsu
        , SUM(Whole_OCC)                                                           AS Whole_OCC
        , SUM(Whole_SCC)                                                           AS Whole_SCC
        , MAX(LakeLastUpdateDate)                                                  AS LakeLastUpdateDate
        , MAX(LakeLastUpdateTimestamp)                                             AS LakeLastUpdateTimestamp
        , NULLIF(MAX(COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000'))
                ,'9999-12-31T01:01:01.001+0000')                                   AS LakeDeletedTimestamp
        FROM Premium 
        
        -- we are not considering deleted records for metrics, while aggregating the metrics
        WHERE   lakeDeletedTimestamp IS NULL
         
        GROUP BY 1, 2
    ) ep
    ON ep.polid=frpol.frpolid
    AND ep.UnitPsu=frpol.FrUnitPsu
)

--**CONFORMED QUERY STARTS HERE**--

SELECT CONCAT(fld.polid, '_', fld.unitpsu)                                         AS Policy_Section_reference
     , CONCAT(fld.polid, '_', fld.unitpsu)                                         AS Policy_Header_reference
     , COALESCE(Original_Currency, {missing_string})                               AS Original_Currency
     , COALESCE(Settlement_Currency, {missing_string})                             AS Settlement_Currency
     , COALESCE(Lloyds_Risk_Code, {missing_string})                                AS Lloyds_Risk_Code
     , Lloyds_Risk_Code_Sequence_Number                                            AS Lloyds_Risk_Code_Sequence_Number
     , CAST(COALESCE(replace(risk_model_ulr, '%',''), 0.00) AS DECIMAL(19,8))      AS Risk_Model_Ultimate_Loss_Ratio_Percentage
     , CAST(COALESCE(pctg0, 0.00) AS DECIMAL(19,8))                                AS Change_in_Pure_Rate_Percentage
     , CAST(COALESCE(pctg10, 0.00) AS DECIMAL(19,8))                               AS Change_In_Breadth_Cover_Percentage
     , CAST(COALESCE(pctg3, 0.00) + COALESCE(pctg4, 0.00) AS DECIMAL(19,8))        AS Change_in_Deduction_Attachment_Point_Percentage
     , CAST(COALESCE(pctg6, 0.00) + COALESCE(pctg8, 0.00) AS DECIMAL(19,8))        AS Change_in_Other_Factors_Percentage
     , CAST((
         COALESCE(Current_Whole_Premium_Written_OCC, 0.00) * COALESCE(benchmark_percentage, 0.00)
       ) AS decimal(28,2))                                                         AS Current_Whole_Benchmark_OCC
     , CAST((
         COALESCE(Current_Whole_Premium_Written_SCC, 0.00) * COALESCE(benchmark_percentage, 0.00)
       ) AS decimal(28,2))                                                         AS Current_Whole_Benchmark_SCC
     , COALESCE(Current_Whole_Premium_Written_OCC, 0.00)                           AS Current_Whole_Premium_Written_OCC
     , COALESCE(Current_Whole_Premium_Written_SCC, 0.00)                           AS Current_Whole_Premium_Written_SCC
     , COALESCE(Expiring_Whole_Premium_Written_OCC, 0.00)                          AS Expiring_Whole_Premium_Written_OCC
     , COALESCE(Expiring_Whole_Premium_Written_SCC, 0.00)                          AS Expiring_Whole_Premium_Written_SCC
     , CAST(COALESCE(Benchmark_percentage, 0.00) AS DECIMAL(19,8))                 AS Benchmark_Percentage
     , CAST(COALESCE(Rol, 0.00) AS DECIMAL(19,8))                                  AS Rate_On_Line_Percentage
     , CASE 
         WHEN UPPER(No_renewal_link_override_rate_change) IN ('YES', 'Y') THEN 'Y'
         ELSE 'N' 
       END                                                                         AS No_Renewal_Link_Override_Rate_Change
     , CAST(COALESCE(Bench_loss_ratio, 0.00) AS DECIMAL(19,8))                     AS Benchmark_Loss_Ratio_Percentage
     , CASE 
          WHEN trans_dt.Lakedeletedtimestamp IS NOT NULL THEN {missing_startdate} 
          ELSE COALESCE(trans_dt.dttm, {missing_startdate})
       END                                                                         AS Source_Transaction_Date
     , CASE 
           WHEN left(trim(replace(trans_dt.dttm,'-','')),6) IN ({acc_perd_match_res}) THEN left(trim(replace(trans_dt.dttm,'-','')),6)
           ELSE {acc_perd_unmatch_res}
       END                                                                         AS accounting_period
     , COALESCE(left(trim(replace(trans_dt.dttm,'-','')),6),'1900')                AS source_accounting_period  
     , GREATEST (fld.lakelastupdatedate, main.lakelastupdatedate, ip.lakelastupdatedate, narr.lakelastupdatedate,
              trans_dt.lakelastupdatedate, metric.lakelastupdatedate)              AS LakeLastUpdateDate
     , GREATEST (fld.lakelastupdatetimestamp, main.lakelastupdatetimestamp, ip.lakelastupdatetimestamp, narr.lakelastupdatetimestamp,
              trans_dt.lakelastupdatetimestamp, metric.lakelastupdatetimestamp)    AS LakeLastUpdateTimestamp
     , CASE
            WHEN COALESCE(fld.lakeDeletedTimestamp, main.lakeDeletedTimestamp, ip.lakeDeletedTimestamp, 
                metric.lakeDeletedTimestamp) IS NULL THEN NULL ELSE {batch_effective_datetime} 
       END                                                                         AS lakeDeletedTimestamp
FROM    standardised_subscribe.dbo_polflds fld

--keeping the policies from PolMain table
INNER JOIN standardised_subscribe.dbo_polmain main
ON Upper(fld.polid) = Upper(main.polid)
AND Upper(fld.unitpsu) = Upper(main.unitpsu)
AND {batch_effective_datetime} >= fld.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(fld.LakeValidToTimestamp, current_timestamp())
AND {batch_effective_datetime} >= main.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(main.LakeValidToTimestamp, current_timestamp())

-- Fetchg only inwards policies
INNER JOIN standardised_subscribe.dbo_inpol ip
ON Upper(fld.polid) = Upper(ip.polid)
AND Upper(fld.unitpsu) = Upper(ip.unitpsu)
AND {batch_effective_datetime} >= fld.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(fld.LakeValidToTimestamp, current_timestamp())
AND {batch_effective_datetime} >= ip.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(ip.LakeValidToTimestamp, current_timestamp())

--Fetching metric cols
INNER JOIN Metric
ON Upper(fld.polid) = Upper(metric.polid)
AND Upper(fld.unitpsu) = Upper(metric.unitpsu)
AND {batch_effective_datetime} >= fld.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(fld.LakeValidToTimestamp, current_timestamp())

--fetching Bench_Loss_Ratio, Conduct_Risk_Rating, No_Renewal_Link_Override_Rate_Change, Benchmark_Percentage, Risk_Model_ULR
LEFT JOIN vwPolSect narr
ON Upper(fld.polid) = Upper(narr.polid)
AND fld.unitpsu = narr.unitpsu
AND {batch_effective_datetime} >= fld.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(fld.LakeValidToTimestamp, current_timestamp())

--to fetch source transaction date
LEFT JOIN
(
    SELECT  polid                                                                  AS Polid
           , unitpsu
           , MIN(dttm)                                                             AS Dttm
           , MAX(lakelastupdatedate)                                               AS lakelastupdatedate
           , MAX(lakelastupdatetimestamp)                                          AS lakelastupdatetimestamp
           , NULLIF(MAX(COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01'))
               ,'9999-12-31T01:01:01')                                             AS LakeDeletedTimestamp
    FROM standardised_subscribe.dbo_polrevhist
    WHERE {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND   {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP()) 
    GROUP BY polid, unitpsu
) trans_dt
ON upper(fld.polid) = upper(trans_dt.polid)
AND upper(fld.unitpsu) = upper(trans_dt.unitpsu)

WHERE {batch_effective_datetime} >= fld.LakeValidFromTimestamp 
AND   {batch_effective_datetime} < coalesce(fld.LakeValidToTimestamp, current_timestamp())
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwPricing_Rating'
  viewTables = 'standardised_Subscribe.dbo_PolMain ,standardised_Subscribe.dbo_PolSectNarr ,standardised_subscribe.dbo_inpol ,standardised_subscribe.dbo_polflds ,standardised_subscribe.dbo_polrevhist ,standardised_subscribe.dbo_polpm ,standardised_subscribe.dbo_InPolPtpt ,standardised_subscribe.dbo_AnlyCdSplt ,standardised_subscribe.dbo_PolANlyCd, standardised_subscribe.dbo_pollnk, standardised_mdm.mdm_accounting_period_mapping'

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
    std_object_PolAnlyCd = 'standardised_subscribe.dbo_PolAnlyCd'
    current_bed = batchEffectiveDatetime
    chk_column = 'ty'
    apply_upper_trim = "y"
    filter_list_PolAnlyCd = ['STATSCODE']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_PolAnlyCd = createStandardisedFilterView(std_object_PolAnlyCd,
                                                        prv_bed,
                                                        current_bed,
                                                        whereExpression,
                                                        chk_column,
                                                        apply_upper_trim,
                                                        filter_list_PolAnlyCd,
                                                        cursor,
                                                        batchTaskId,
                                                        adfPipelineName,
                                                        clusterId,
                                                        notebookName,
                                                        errorLogFileLocation)
        union_df_PolAnlyCd=spark.read.table(std_object_PolAnlyCd).unionByName(std_df_PolAnlyCd)
    else :
        union_df_PolAnlyCd=spark.read.table(std_object_PolAnlyCd)
        
    union_df_PolAnlyCd.createOrReplaceTempView('standardised_subscribe_dbo_PolAnlyCd_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_PolAnlyCd_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create Filter Standardised View
try:
    std_object_AnlyCdSplt = 'standardised_subscribe.dbo_AnlyCdSplt'
    current_bed = batchEffectiveDatetime
    chk_column = 'Ty'
    apply_upper_trim = "y"
    filter_list_AnlyCdSplt = ['RSK']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_AnlyCdSplt = createStandardisedFilterView(std_object_AnlyCdSplt,
                                                         prv_bed,
                                                         current_bed,
                                                         whereExpression,
                                                         chk_column,
                                                         apply_upper_trim,
                                                         filter_list_AnlyCdSplt,
                                                         cursor,
                                                         batchTaskId,
                                                         adfPipelineName,
                                                         clusterId,
                                                         notebookName,
                                                         errorLogFileLocation)
        union_df_AnlyCdSplt = spark.read.table(std_object_AnlyCdSplt).unionByName(std_df_AnlyCdSplt)
    else :
        union_df_AnlyCdSplt = spark.read.table(std_object_AnlyCdSplt)
        
    union_df_AnlyCdSplt.createOrReplaceTempView('standardised_subscribe_dbo_AnlyCdSplt_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_AnlyCdSplt_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
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
        
        string_filter_PolAnlyCd = "','".join(filter_list_PolAnlyCd)
        string_filter_AnlyCdSplt = "','".join(filter_list_AnlyCdSplt)
        #Replace value of batch effective date in SQL Query
        sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                   missing_string = "'" + str(missing_string) + "'", 
                                   missing_startdate = "'" + str(missing_startdate) + "'", 
                                   acc_perd_match_res = str(acc_perd_match_res), 
                                   string_filter_PolAnlyCd = "'" + str(string_filter_PolAnlyCd) + "'", 
                                   string_filter_AnlyCdSplt = "'" + str(string_filter_AnlyCdSplt) + "'", 
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