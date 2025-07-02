# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_Claim_Component_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Claim_Component table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Nikunj</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/07/14</td></tr>
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

# DBTITLE 1,Prepare Claim Universe
univ="""
SELECT 
coalesce(upper(nullif(trim(main.BPR), '')), {missing_string})                    AS Claim_Component_Reference,
coalesce(upper(nullif(trim(main.BPR), '')), {missing_string})                    AS Claim_Reference,
COALESCE(cmtb.Loss_Code, cmteg.Loss_Code, cmtc.Loss_Code, cmtp.Loss_Code, 
    cmte.Loss_Code, {missing_mdm})                                               AS Loss_Code,
COALESCE(cmtb.Loss_Code_Type, cmteg.Loss_Code_Type, cmtc.Loss_Code_Type, cmtp.Loss_Code_Type, 
    cmte.Loss_Code_Type, {missing_mdm})                                          AS Loss_Code_Type,
coalesce(TrnAnly_FIL2.AnlyCd, main.Filcd2, {missing_string})                     AS FIL_2_Code,
coalesce(TrnAnly_FIL4.AnlyCd, main.Filcd4, {missing_string})                     AS FIL_4_Code,
COALESCE(loc_cntry_ref.Country_Alpha_3_Code, {missing_mdm})                      AS Country_Alpha_3_Code,
COALESCE(main.StaCd, {missing_string})                                           AS Location_Sub_Division_Code,
COALESCE(NULLIF(main.PcsCd,''), {missing_string})                                AS Source_PCS_Catastrophe_Code,
COALESCE(TrnAnly_EVT.AnlyCd,main.EvtCd,{missing_string})                         AS Source_Event_Code,
COALESCE(evt.EvtDesc, {missing_mdm})                                             AS Source_Event_Description,
COALESCE(NULLIF(main.CatCd,''), {missing_string})                                AS Source_Lloyds_Cat_Code,
COALESCE(evt.GrpEvtCd, {missing_mdm})                                            AS Source_Group_Event_Code,
{missing_string}                                                                 AS Loss_Post_Code,
main.Dolfr                                                                       AS Loss_From_Date,
main.Dolto                                                                       AS Loss_To_Date,
coalesce(main.LossLocn, {missing_string})                                        AS Loss_Address_Line_1,
{missing_string}                                                                 AS Loss_Address_Line_2,
{missing_string}                                                                 AS Loss_Address_Line_3,
{missing_string}                                                                 AS Loss_Address_Line_4, 
coalesce(main.Lmtexs, {missing_string})                                          AS Claim_Component_Limit,
DECODE(UPPER(main.Blkindr),'Y', 'Y', 'N')                                        AS Block_Claim_Indicator,
'N'                                                                              AS Litigation_Indicator,
coalesce(main.Warindr, 'N')                                                      AS War_Indicator,
CASE 
    WHEN coalesce(TrnAnly_FIL4.AnlyCd, main.Filcd4) LIKE 'LF%' THEN 'Y' 
    ELSE 'N' 
END                                                                              AS Loss_Fund_Indicator,
'N'                                                                              AS Placeholder_Claim_Indicator,
CASE
    --For Marine claims, suffix the CKY to the OCR
    WHEN LEFT(Upper(Trim(main.Bpr)), 1) = 'M' THEN Concat_ws('',Upper(nullif(Trim(main.Ocr),'')), Upper(Trim(main.Cky)))
    --For Non-marine claims, use the OCR as is
    WHEN LEFT(Upper(Trim(main.Bpr)), 1) IN ( 'N', 'A' ) THEN coalesce(Upper(Trim(main.Ocr)),{missing_string})
    ELSE coalesce(Upper(Trim(main.Bpr)),{missing_string}) 
END                                                                              AS Claim_Office_Reference,
CASE
    WHEN trim(upper(main.BOC)) = 'U' THEN 'U - Uncoded'
    WHEN trim(upper(main.BOC)) = 'C' THEN 'C - Claims made during'
    WHEN trim(upper(main.BOC)) = 'L' THEN 'L - Losses occuring during'
    WHEN trim(upper(main.BOC)) = 'P' THEN 'P - Portfolio transfer'
    WHEN trim(upper(main.BOC)) = 'R' THEN 'R - Risk attaching during'
    WHEN trim(upper(main.BOC)) = 'T' THEN 'T - T.B.A'
    WHEN trim(upper(main.BOC)) = 'V' THEN 'V - Various dates'
    ELSE 'No cover period'
END                                                                              AS Claim_Basis,
coalesce(TrnAnly_TFC.AnlyCd, main.TFC, {missing_string})                         AS Source_Trust_Fund_Code,
{missing_string}                                                                 AS Claim_Type_Current,
{missing_string}                                                                 AS Claim_Type,
{missing_string}                                                                 AS Claim_Type_Description,
Greatest(movt.Lakelastupdatedate, main.Lakelastupdatedate, loc_cntry_ref.Lakelastupdatedate, 
    evt.Lakelastupdatedate, cmtb.Lakelastupdatedate, cmteg.Lakelastupdatedate, cmtc.Lakelastupdatedate, 
    cmtp.Lakelastupdatedate, cmte.Lakelastupdatedate, TrnAnly_TFC.Lakelastupdatedate, TrnAnly_FIL2.Lakelastupdatedate, 
    TrnAnly_FIL4.Lakelastupdatedate, TrnAnly_EVT.Lakelastupdatedate)             AS lakelastupdatedate,
Greatest(movt.Lakelastupdatetimestamp, main.Lakelastupdatetimestamp, loc_cntry_ref.Lakelastupdatetimestamp,
    evt.Lakelastupdatetimestamp, cmtb.lakelastupdatetimestamp, cmteg.lakelastupdatetimestamp, cmtc.lakelastupdatetimestamp, 
    cmtp.lakelastupdatetimestamp, cmte.lakelastupdatetimestamp, TrnAnly_TFC.lakelastupdatetimestamp, 
    TrnAnly_FIL2.lakelastupdatetimestamp, TrnAnly_FIL4.lakelastupdatetimestamp, 
    TrnAnly_EVT.lakelastupdatetimestamp)                                         AS lakelastupdatetimestamp,
CASE 
    WHEN coalesce(movt.lakeDeletedTimestamp, main.lakeDeletedTimestamp) IS NOT NULL THEN {batch_effective_datetime} ELSE NULL 
END                                                                              AS lakeDeletedTimestamp
FROM   
(
   SELECT 
   move.BPR, 
   Max(Trnid)                                                                    AS TrnID, 
   Max(Trncgy)                                                                   AS Trncgy,
   Max(MovtSeqNo)                                                                AS MovtSeqNo,
   max(Greatest(move.Lakelastupdatedate, lastMove.Lakelastupdatedate))           AS lakelastupdatedate,
   max(Greatest(move.Lakelastupdatetimestamp, lastMove.Lakelastupdatetimestamp)) AS lakelastupdatetimestamp,
   CASE 
       WHEN NULLIF(max(coalesce(
       lastMove.lakeDeletedTimestamp, coalesce(move.lakeDeletedTimestamp, '9999-12-31T01:01:01.001+0000')
                                )),'9999-12-31T01:01:01.001+0000') IS NOT NULL THEN {batch_effective_datetime}
       ELSE NULL  
   END                                                                           AS lakeDeletedTimestamp
   FROM standardised_subscribe_dbo_scmmovt_tmpvw move

   --to bring in transaction id corresponding to latest movement    
   INNER JOIN 
   (
        SELECT 
        BPR, 
        Max(Dttm)                                                                AS LastMovtDt,
        Max(Lakelastupdatedate)                                                  AS lakelastupdatedate,
        Max(Lakelastupdatetimestamp)                                             AS lakelastupdatetimestamp,
        NULLIF(Max(coalesce(lakeDeletedTimestamp,
                '9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') AS lakeDeletedTimestamp
        FROM standardised_subscribe_dbo_scmmovt_tmpvw
        where {batch_effective_datetime} >= lakevalidfromtimestamp
        AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
        GROUP BY BPR
   ) lastMove
   ON  lastMove.BPR = move.BPR
   AND lastMove.LastMovtDt = move.Dttm
   AND {batch_effective_datetime} >= move.lakevalidfromtimestamp
   AND {batch_effective_datetime} < COALESCE(move.lakevalidtotimestamp, CURRENT_TIMESTAMP())
   GROUP BY move.BPR
) movt

INNER JOIN 
(
    Select BPR, Dolfr, Dolto, LossLocn, Lmtexs, Blkindr, Warindr, Tlindr, 
    BOC, TFC, PCSCd, CatCd, LossNarr1, LossNarr2, LossNarr3, unitid, Polid, 
    Filcd2, Filcd4, Stacd, Ctrypsu, evtcd, unitpsu, TrnId, TrnCgy, OCR, Cky, AccYr,
    Lakelastupdatedate, Lakelastupdatetimestamp, lakeDeletedTimestamp
    from standardised_subscribe_dbo_scmmain_tmpvw
    WHERE {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) main
ON upper(trim(movt.Trnid)) = upper(trim(main.Trnid))
AND upper(trim(movt.Trncgy)) = upper(trim(main.Trncgy))

LEFT JOIN 
(
    SELECT case when lakedeletedtimestamp is not null then null else Country_Alpha_3_Code end AS Country_Alpha_3_Code,
           case when lakedeletedtimestamp is not null then null else Country_Alpha_2_Code end AS Country_Alpha_2_Code,
           Lakelastupdatedate,
           Lakelastupdatetimestamp,
           lakeDeletedTimestamp
    FROM standardised_mdm.mdm_location_country 
    WHERE {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) loc_cntry_ref
ON upper(trim(main.CtryPsu)) = upper(trim(loc_cntry_ref.Country_Alpha_2_Code))

LEFT JOIN 
(
    SELECT case when lakedeletedtimestamp is not null then null else evtcd    END AS EvtCd,
           case when lakedeletedtimestamp is not null then null else evtdesc  END AS EvtDesc,
           case when lakedeletedtimestamp is not null then null else GrpEvtCd END AS GrpEvtCd,
           Lakelastupdatedate,
           Lakelastupdatetimestamp,
           lakeDeletedTimestamp
    FROM standardised_subscribe.dbo_evt 
    WHERE {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) evt
ON upper(trim(main.evtcd)) = upper(trim(evt.evtcd))

LEFT JOIN 
(
    SELECT Loss_Code, Loss_Code_Type, Lakelastupdatedate, lakelastupdatetimestamp
    FROM standardised_mdm.mdm_Loss_Event_header
    WHERE Loss_Code_Type = 'BPR'
    AND lakeDeletedTimestamp IS NULL
    AND {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) cmtb
ON  main.BPR = cmtb.Loss_Code

LEFT JOIN 
(
    SELECT Loss_COde, Loss_Code_Type, Lakelastupdatedate, lakelastupdatetimestamp
    FROM standardised_mdm.mdm_Loss_Event_header
    WHERE Loss_Code_Type = 'Event_Group_Code'
    AND lakeDeletedTimestamp IS NULL
    AND {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) cmteg
ON  evt.GrpEvtCd = cmteg.loss_code

LEFT JOIN 
(
    SELECT Loss_COde, Loss_Code_Type, Lakelastupdatedate, lakelastupdatetimestamp
    FROM standardised_mdm.mdm_Loss_Event_header
    WHERE Loss_Code_Type = 'Lloyds_Cat_Code'
    AND lakeDeletedTimestamp IS NULL
    AND {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) cmtc
ON  main.CatCd = cmtc.loss_code

LEFT JOIN 
(
    SELECT Loss_COde, Loss_Code_Type, Lakelastupdatedate, lakelastupdatetimestamp
    FROM standardised_mdm.mdm_Loss_Event_header
    WHERE Loss_Code_Type = 'PCS_Cat_Code'
    AND lakeDeletedTimestamp IS NULL
    AND {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) cmtp
ON  main.PcsCd = cmtp.loss_code

LEFT JOIN 
(
    SELECT Loss_Code, Loss_Code_Type, Lakelastupdatedate, lakelastupdatetimestamp
    FROM standardised_mdm.mdm_Loss_Event_header
    WHERE Loss_Code_Type = 'Event_Code'
    AND lakeDeletedTimestamp IS NULL
    AND {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) cmte
ON  main.EvtCd = cmte.Loss_Code

--to fetch TFC Related Details
LEFT JOIN
(
    SELECT TrnId, TrnCgy, MovtSeqNo, AnlyCd, Lakelastupdatedate, Lakelastupdatetimestamp
    from standardised_subscribe.dbo_TrnAnly
    WHERE UPPER(TRIM(AnlyTy)) = 'TFC' AND TrnCgy = 'SCM' AND lakeDeletedTimestamp IS NULL
    AND {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) TrnAnly_TFC
ON  upper(movt.TrnId)  = upper(TrnAnly_TFC.TrnId)
AND upper(movt.TrnCgy) = upper(TrnAnly_TFC.TrnCgy)
AND upper(movt.MovtSeqNo) = upper(TrnAnly_TFC.MovtSeqNo)

--to fetch FIL2 Related Details
LEFT JOIN
(
    SELECT TrnId, TrnCgy, MovtSeqNo, AnlyCd, Lakelastupdatedate, Lakelastupdatetimestamp
    from standardised_subscribe.dbo_TrnAnly
    WHERE UPPER(TRIM(AnlyTy)) = 'FIL2' AND TrnCgy = 'SCM' AND lakeDeletedTimestamp IS NULL
    AND {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) TrnAnly_FIL2
ON  upper(movt.TrnId)  = upper(TrnAnly_FIL2.TrnId)
AND upper(movt.TrnCgy) = upper(TrnAnly_FIL2.TrnCgy)
AND upper(movt.MovtSeqNo) = upper(TrnAnly_FIL2.MovtSeqNo)

--to fetch FIL4 Related Details
LEFT JOIN
(
    SELECT TrnId, TrnCgy, MovtSeqNo, AnlyCd, Lakelastupdatedate, Lakelastupdatetimestamp
    from standardised_subscribe.dbo_TrnAnly
    WHERE UPPER(TRIM(AnlyTy)) = 'FIL4' AND TrnCgy = 'SCM' AND lakeDeletedTimestamp IS NULL
    AND {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) TrnAnly_FIL4
ON  upper(movt.TrnId)  = upper(TrnAnly_FIL4.TrnId)
AND upper(movt.TrnCgy) = upper(TrnAnly_FIL4.TrnCgy)
AND upper(movt.MovtSeqNo) = upper(TrnAnly_FIL4.MovtSeqNo)

--to fetch EVT Related Details
LEFT JOIN
(
    SELECT TrnId, TrnCgy, MovtSeqNo, AnlyCd, Lakelastupdatedate, Lakelastupdatetimestamp
    from standardised_subscribe.dbo_TrnAnly
    WHERE UPPER(TRIM(AnlyTy)) = 'EVT' AND TrnCgy = 'SCM' AND lakeDeletedTimestamp IS NULL
    AND {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) TrnAnly_EVT
ON  upper(movt.TrnId)  = upper(TrnAnly_EVT.TrnId)
AND upper(movt.TrnCgy) = upper(TrnAnly_EVT.TrnCgy)
AND upper(movt.MovtSeqNo) = upper(TrnAnly_EVT.MovtSeqNo)
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
    #declare variables with hard coded values 
    #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
    #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

    viewName = 'dummy_Dummy.dummyVwClaim_Component'
    viewTables = 'standardised_subscribe.dbo_scmmovt, standardised_subscribe.dbo_scmmain, standardised_mdm.mdm_location_country, standardised_subscribe.dbo_Evt, standardised_subscribe.dbo_GrpEvt, standardised_mdm.mdm_Loss_Event_header, standardised_subscribe.dbo_TrnAnly'

    #call function
    dependentTableIsUpdated = populateObjectDateAvailabilityWithDataCheck(viewName,
                                                                          viewTables,
                                                                          cursor,
                                                                          batchTaskId,
                                                                          adfPipelineName,
                                                                          clusterId,
                                                                          notebookName,
                                                                          errorLogFileLocation)

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

# DBTITLE 1,Create standardised Grain View
try:
    std_object_scmmovt = 'standardised_subscribe.dbo_scmmovt'
    current_bed = batchEffectiveDatetime
    chk_column_list = ['BPR']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_scmmovt = createStandardisedGrainView(
                                            std_object_scmmovt,
                                            prv_bed,
                                            current_bed,
                                            whereExpression,
                                            chk_column_list,
                                            cursor,
                                            batchTaskId,
                                            adfPipelineName,
                                            clusterId,
                                            notebookName,
                                            errorLogFileLocation,
                                            False  #pass True if there is filter view on same table else False
                                        )
        union_df_scmmovt=spark.read.table(std_object_scmmovt).unionByName(std_df_scmmovt)
    else :
        union_df_scmmovt=spark.read.table(std_object_scmmovt)
    
    union_df_scmmovt.createOrReplaceTempView('standardised_subscribe_dbo_scmmovt_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_scmmovt_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create standardised Grain View
try:
    std_object_scmmain = 'standardised_subscribe.dbo_scmmain'
    current_bed = batchEffectiveDatetime
    chk_column_list = ['BPR']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_scmmain=createStandardisedGrainView(
                                            std_object_scmmain,
                                            prv_bed,
                                            current_bed,
                                            whereExpression,
                                            chk_column_list,
                                            cursor,
                                            batchTaskId,
                                            adfPipelineName,
                                            clusterId,
                                            notebookName,
                                            errorLogFileLocation,
                                            False  #pass True if there is filter view on same table else False
                                        )
        union_df_scmmain=spark.read.table(std_object_scmmain).unionByName(std_df_scmmain)
    else :
        union_df_scmmain=spark.read.table(std_object_scmmain)
    
    union_df_scmmain.createOrReplaceTempView('standardised_subscribe_dbo_scmmain_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_scmmain_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create dataframe from standardised data
try:
    if dependentTableIsUpdated == True:
        
        #Replace value of batch effective date in SQL Query
        univ = univ.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'"
                                   , missing_string = "'" +str(missing_string) + "'"
                                   , missing_mdm = "'" +str(missing_mdm) + "'")
        dfCnf = spark.sql(univ)
    
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
        applySCDType2onDestForBatchLoad(dfCnf,
                                    targetObjectName,
                                    columnList,
                                    batchEffectiveDatetime,
                                    currentTs,
                                    cursor,
                                    batchTaskId,
                                    batchId,
                                    adfPipelineName,
                                    clusterId,
                                    notebookName,
                                    errorLogFileLocation)
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