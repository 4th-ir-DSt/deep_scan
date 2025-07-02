# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_EPI_Transaction_Detail_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for EPI_Transaction table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Nikunj</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/07/04</td></tr>
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

# DBTITLE 1,Universe
univQuery ="""
with inpolptpt as --bringing in cols from inpolptpt
(
select unitid
       ,polid
       ,unitpsu
       ,spltpctg
       ,lakelastupdatedate
       ,lakelastupdatetimestamp
       ,lakeDeletedTimestamp
from standardised_subscribe.dbo_inpolptpt
    where {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP()) 
),
polpm as --bringing in cols from polpm
(
select polid
       ,unitpsu
       ,occ
       ,pmseqno
       ,mktorigpmamt
       ,CASE WHEN SCC = OCC THEN 1 ELSE settroe END                                           AS settroe
       ,settpmamt
       ,lakelastupdatedate
       ,lakelastupdatetimestamp
       ,lakeDeletedTimestamp
from standardised_subscribe_dbo_polpm_tmpvw
    where {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
    --**Added below filter as per bug 43493**--
    AND ({queryWhereCondition_polpm})
),
anlycdsplt  as --bringing in cols from anlycdsplt
(
select polid
      ,unitpsu
      ,Cd
      ,SeqNo
      ,pctg
      ,lakelastupdatedate
      ,lakelastupdatetimestamp
      ,lakeDeletedTimestamp
from standardised_subscribe_dbo_anlycdsplt_tmpvw
    where upper(ty) IN ({queryWhereCondition_AnlyCdSplt})
    AND {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP()) 
),
inpol as --bringing in cols from inpol
(
select polid
    ,unitpsu
    ,acctgyr
    ,bkrage
    ,calcln
    ,sgndordpctg
    ,lakelastupdatedate
    ,lakelastupdatetimestamp
    ,lakeDeletedTimestamp
from standardised_subscribe.dbo_inpol
    where {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP()) 
),
polmain as --bringing in cols from polmain
(
select polid
    ,unitpsu
    ,wtnordpctg
    ,lakelastupdatedate
    ,lakelastupdatetimestamp
    ,lakeDeletedTimestamp
from standardised_subscribe.dbo_polmain
    where {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP()) 
),
polpmdeds as
(
SELECT Upper(polid) AS polid
    ,unitpsu
    ,max(lakelastupdatedate) as lakelastupdatedate
    ,max(lakelastupdatetimestamp) as lakelastupdatetimestamp
    ,NULLIF(MAX(COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01')),'9999-12-31T01:01:01')  AS lakeDeletedTimestamp
    ,Sum(CASE
            WHEN calcuse = 1 THEN actdedamt ELSE 0.0
     END)                                                                                     AS Deductions_Percentage
    ,Sum(CASE
            WHEN calcuse = 1 AND DEDTY = 'Ceding Comm' THEN ActDedAmt ELSE 0.0
     END)                                                                                     AS Ceding_commission_Percentage
FROM standardised_subscribe.dbo_polpmdeds 
    where {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
    -- we do not want to consider deleted record for metrics as we are doing a sum
    AND lakeDeletedTimestamp IS NULL
    GROUP BY Upper(polid), unitpsu
),
polanlycd as
(
SELECT
    Upper(polid) AS polid
    , unitpsu
    , cd
    , lakelastupdatedate
    , lakelastupdatetimestamp
FROM standardised_subscribe.dbo_polanlycd
    WHERE upper(trim(ty)) = 'SUBCLASS'
    AND {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
)

select 
    upper(Concat(pm.polid, '_', pm.unitpsu))                                                  AS Policy_Header_Reference,
    COALESCE(up.acctgyr,'1900')                                                               AS acctgyr,
    COALESCE(pm.occ,{missing_string})                                                         AS OCC,
    pm.pmseqno,
    coalesce(nullif(trim(ipp.unitid),''),{missing_string})                                    AS unitid,
    pm.mktorigpmamt,
    rsk.pctg,
    ipp.spltpctg,
    pm.settroe,
    pl.wtnordpctg,
    up.bkrage,
    up.sgndordpctg,
    up.calcln,
    pm.settpmamt,
    coalesce(nullif(rsk.cd, ''), {missing_string})                                            AS cd ,
    rsk.SeqNo                                                                                 AS SeqNo,
    coalesce(nullif(anly.cd, ''), {missing_string})                                           AS SubClassCode,
    CASE 
        when deds.lakedeletedtimestamp is not null then 0.0 else deds.Deductions_Percentage 
    END                                                                                       AS Deductions_Percentage,
    CASE 
        when deds.lakedeletedtimestamp is not null then 0.0 else deds.Ceding_commission_Percentage 
    END                                                                                       AS Ceding_commission_Percentage,
    Greatest(pm.lakelastupdatedate, ipp.lakelastupdatedate, rsk.lakelastupdatedate, 
        up.lakelastupdatedate, pl.lakelastupdatedate, deds.lakelastupdatedate, 
        anly.lakelastupdatedate)                                                              AS LakeLastUpdateDate,
    Greatest(pm.lakelastupdatetimestamp, ipp.lakelastupdatetimestamp, rsk.lakelastupdatetimestamp, 
        up.lakelastupdatetimestamp, pl.lakelastupdatetimestamp, deds.lakelastupdatetimestamp, 
        anly.lakelastupdatetimestamp)                                                         AS LakeLastUpdateTimestamp,
    CASE 
        when coalesce(pm.lakeDeletedTimestamp, ipp.lakeDeletedTimestamp, rsk.lakeDeletedTimestamp, 
            up.lakeDeletedTimestamp, pl.lakeDeletedTimestamp) Is Not Null THEN {batch_effective_datetime} 
        ELSE NULL 
    END                                                                                       AS lakeDeletedTimestamp
from polpm pm
 
INNER JOIN inpolptpt ipp
ON Upper(ipp.polid) = Upper(pm.polid)
AND Upper(ipp.unitpsu) = Upper(pm.unitpsu)

-- to fetch the risk percentage
INNER JOIN anlycdsplt  rsk
ON Upper(rsk.polid) = Upper(pm.polid)
AND Upper(rsk.unitpsu) = Upper(pm.unitpsu)
            
-- to fetch calcn
INNER JOIN inpol up 
ON Upper(up.polid) = Upper(pm.polid)
AND Upper(up.unitpsu) = Upper(pm.unitpsu)
            
-- to keep policies which are only present in pol main
INNER JOIN polmain pl
ON Upper(pl.polid) = Upper(pm.polid)
AND Upper(pl.unitpsu) = Upper(pm.unitpsu)

-- to fetch the deduction percentage
LEFT JOIN polpmdeds deds
ON Upper(deds.polid) = Upper(pm.polid)
AND deds.unitpsu = pm.unitpsu

-- to fetch subclass
LEFT JOIN polanlycd anly
ON Upper(pm.polid) = Upper(anly.polid)
AND pm.unitpsu = anly.unitpsu
"""

# COMMAND ----------

# DBTITLE 1,Prepare SQL Query
sqlQuery= """
select
--**TRANSACTION TYPE --> GN**--
pmseqno                                                                                       AS Estimated_Premium_Sequence_Number
,Policy_Header_Reference                                                                      AS Policy_Section_Reference
,Policy_Header_Reference
,unitid                                                                                       AS Business_Entity_Code
,cd                                                                                           AS Lloyds_Risk_Code
,SeqNo                                                                                        AS Lloyds_Risk_Code_Sequence_Number 
,'01'                                                                                         AS Transaction_line_Number
,occ                                                                                          AS Original_Currency
,Policy_Header_Reference                                                                      AS Item_Reference
, 'GN'                                                                                        AS Transaction_line_code
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100) * coalesce(SgndOrdPctg/100, wtnordpctg/100) * coalesce(( 1 / NULLIF(SettROE, 0)), 0) * 
    ((100 - coalesce(Bkrage, 0) - coalesce(Deductions_Percentage, 0)) / 100)
) as decimal(28,2))                                                                           AS Market_SCC
,cast(SUM(
    COALESCE(SettPMAmt * (Pctg / 100) * (SpltPctg / 100), 0) 
    + COALESCE(MktOrigPMAmt * (Pctg / 100) * (SpltPctg / 100) * (CalcLn / 100)
    * (CASE WHEN unitid = '4445'  and SubClassCode IS NOT NULL THEN coalesce(Ceding_commission_Percentage, 0) ELSE 0.00 end / 100)
    * coalesce(( 1 / NULLIF(SettROE, 0)), 0), 0)
) as decimal(28,2))                                                                           AS Line_SCC
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100) * coalesce(( 1 / NULLIF(SettROE, 0)), 0) * 
    ((100 - coalesce(Bkrage, 0) - coalesce(Deductions_Percentage, 0)) / 100)
) as decimal(28,2))                                                                           AS Whole_SCC 
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100) * coalesce(SgndOrdPctg/100, wtnordpctg/100)	* 
    ((100 - coalesce(Bkrage, 0) - coalesce(Deductions_Percentage, 0)) / 100)
) as decimal(28,2))                                                                           AS Market_OCC
,cast(SUM(
    COALESCE(MktOrigPMAmt * (Pctg / 100) * (SpltPctg / 100) * (CalcLn / 100), 0)
    * (
        ((100 - coalesce(bkrage, 0) - coalesce(Deductions_Percentage, 0)) / 100)
         + (CASE WHEN unitid = '4445'  and SubClassCode IS NOT NULL THEN coalesce(Ceding_commission_Percentage, 0) ELSE 0 end / 100)
       )
) as decimal(28,2))                                                                           AS Line_OCC
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100) * 
    ((100 - coalesce(Bkrage, 0) - coalesce(Deductions_Percentage, 0)) / 100)
) as decimal(28,2))                                                                           AS Whole_OCC 
,lakelastupdatedate
,lakelastupdatetimestamp
,CASE 
    WHEN lakeDeletedTimestamp Is Not Null THEN {batch_effective_datetime} ELSE NULL 
END                                                                                           AS lakeDeletedTimestamp
from universe
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 17, 18, 19

union all

select
--**TRANSACTION TYPE --> GG**--
pmseqno as Estimated_Premium_Sequence_Number
,Policy_Header_Reference                                                                      AS Policy_Section_Reference
,Policy_Header_Reference
,unitid                                                                                       AS Business_Entity_Code
,cd                                                                                           AS Lloyds_Risk_Code
,SeqNo                                                                                        AS Lloyds_Risk_Code_Sequence_Number 
,'02'                                                                                         AS Transaction_line_Number
,occ                                                                                          AS Original_Currency
,Policy_Header_Reference                                                                      AS Item_Reference
, 'GG'                                                                                        AS Transaction_line_code
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100) * coalesce(SgndOrdPctg/100, wtnordpctg/100) * coalesce(( 1 / NULLIF(SettROE, 0)), 0)
) as decimal(28,2))                                                                           AS Market_SCC
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100) * (SpltPctg / 100)* (CalcLn / 100) * coalesce(( 1 / NULLIF(SettROE, 0)), 0)
) as decimal(28,2))                                                                           AS Line_SCC
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100) * coalesce(( 1 / NULLIF(SettROE, 0)), 0)
) as decimal(28,2))                                                                           AS Whole_SCC
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100) * coalesce(SgndOrdPctg/100, wtnordpctg/100)
) as decimal(28,2))                                                                           AS Market_OCC
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100) * (SpltPctg / 100) * (CalcLn / 100)
) as decimal(28,2))                                                                           AS Line_OCC
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100)
) as decimal(28,2))                                                                           AS Whole_OCC  
,lakelastupdatedate
,lakelastupdatetimestamp
,CASE 
    WHEN lakeDeletedTimestamp Is Not Null THEN {batch_effective_datetime} ELSE NULL 
END AS lakeDeletedTimestamp
from universe
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 17, 18, 19

union all

select
--**TRANSACTION TYPE --> ACQ**--
pmseqno as Estimated_Premium_Sequence_Number
,Policy_Header_Reference                                                                      AS Policy_Section_Reference
,Policy_Header_Reference
,unitid                                                                                       AS Business_Entity_Code
,cd                                                                                           AS Lloyds_Risk_Code
,SeqNo                                                                                        AS Lloyds_Risk_Code_Sequence_Number 
,'03'                                                                                         AS Transaction_line_Number
,occ                                                                                          AS Original_Currency
,Policy_Header_Reference                                                                      AS Item_Reference
, 'ACQ'                                                                                       AS Transaction_line_code
,cast(SUM( 
    MktOrigPMAmt * (Pctg / 100) * coalesce(SgndOrdPctg/100, wtnordpctg/100) * coalesce((1 / NULLIF(SettROE, 0)), 0) * 
    ((coalesce(Bkrage, 0) + coalesce(Deductions_Percentage, 0)) / 100)
) as decimal(28,2))                                                                           AS Market_SCC
,cast(SUM(
    --Gross Gross Minus Gross Net
    COALESCE(MktOrigPMAmt * (Pctg / 100) * (SpltPctg / 100)* (CalcLn / 100) * coalesce(( 1 / NULLIF(SettROE, 0)), 0), 0) -
    COALESCE(
        COALESCE(SettPMAmt * (Pctg / 100) * (SpltPctg / 100), 0) 
        + COALESCE(MktOrigPMAmt * (Pctg / 100) * (SpltPctg / 100) * (CalcLn / 100)
        * (CASE WHEN unitid = '4445'  and SubClassCode IS NOT NULL THEN coalesce(Ceding_commission_Percentage, 0) ELSE 0.00 end / 100)
        * coalesce(( 1 / NULLIF(SettROE, 0)), 0), 0), 0
    )
) as decimal(28,2))                                                                           AS Line_SCC
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100) * ( 1 / NULLIF(SettROE, 0)) * 
    ((COALESCE(Bkrage,0.00) + COALESCE(Deductions_Percentage,0.00)) / 100)
) as decimal(28,2))                                                                           AS Whole_SCC
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100) * coalesce(SgndOrdPctg/100, wtnordpctg/100) * 
    ((coalesce(Bkrage, 0) + coalesce(Deductions_Percentage, 0)) / 100)
) as decimal(28,2))                                                                           AS Market_OCC
,cast(SUM(
    --Gross Gross Minus Gross Net
    COALESCE(MktOrigPMAmt * (Pctg / 100) * (SpltPctg / 100) * (CalcLn / 100), 0) - 
    COALESCE(
        COALESCE(MktOrigPMAmt * (Pctg / 100) * (SpltPctg / 100) * (CalcLn / 100), 0)
        * (
            ((100 - coalesce(bkrage, 0) - coalesce(Deductions_Percentage, 0)) / 100)
            + (CASE WHEN unitid = '4445'  and SubClassCode IS NOT NULL THEN coalesce(Ceding_commission_Percentage, 0) ELSE 0 end / 100)
          ), 0
    )
) as decimal(28,2))                                                                           AS Line_OCC
,cast(SUM(
    MktOrigPMAmt * (Pctg / 100) * (COALESCE(Bkrage,0.00) + COALESCE(Deductions_Percentage,0.00)) / 100
) as decimal(28,2))                                                                           AS Whole_OCC
,lakelastupdatedate
,lakelastupdatetimestamp
,CASE 
    WHEN lakeDeletedTimestamp Is Not Null THEN {batch_effective_datetime} ELSE NULL 
END AS lakeDeletedTimestamp
from universe
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 17, 18, 19
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwEPI_Transaction_Detail'
  viewTables = 'standardised_subscribe.dbo_polpm, standardised_subscribe.dbo_inpolptpt, standardised_subscribe.dbo_polmain, standardised_subscribe.dbo_anlycdsplt, standardised_subscribe.dbo_polpmdeds, standardised_subscribe.dbo_inpol, standardised_subscribe.dbo_polanlycd'

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
    std_object_polpm = 'standardised_subscribe.dbo_polpm'
    current_bed = batchEffectiveDatetime
    queryWhereCondition_polpm = """
    TrnTy = '1401'
    AND OCC is not null --Checked with Dan, we should not have any record with OCC as null
    """
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_polpm = createStandardisedMultipleFilterView(std_object_polpm, 
                                                            prv_bed,
                                                            current_bed,
                                                            whereExpression,
                                                            queryWhereCondition_polpm,
                                                            cursor,
                                                            batchTaskId,
                                                            adfPipelineName,
                                                            clusterId,
                                                            notebookName,
                                                            errorLogFileLocation)
        union_df_polpm=spark.read.table(std_object_polpm).unionByName(std_df_polpm)
    else :
        union_df_polpm=spark.read.table(std_object_polpm)
        
    union_df_polpm.createOrReplaceTempView('standardised_subscribe_dbo_polpm_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_polpm_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create Filter Standardised View
try:
    std_object_anlycdsplt = 'standardised_subscribe.dbo_anlycdsplt'
    current_bed = batchEffectiveDatetime
    chk_column = 'ty'
    apply_upper_trim = "y"
    filter_list2 = ['RSK']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_anlycdsplt = createStandardisedFilterView(std_object_anlycdsplt,
                                                         prv_bed,
                                                         current_bed,
                                                         whereExpression,
                                                         chk_column,
                                                         apply_upper_trim,
                                                         filter_list2,
                                                         cursor,
                                                         batchTaskId,
                                                         adfPipelineName,
                                                         clusterId,
                                                         notebookName,
                                                         errorLogFileLocation)
        union_df_anlycdsplt=spark.read.table(std_object_anlycdsplt).unionByName(std_df_anlycdsplt)
    else :
        union_df_anlycdsplt=spark.read.table(std_object_anlycdsplt)
        
    union_df_anlycdsplt.createOrReplaceTempView('standardised_subscribe_dbo_anlycdsplt_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_anlycdsplt_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create dataframe from standardised data
try:
    if dependentTableIsUpdated == True:    
        current_bed
        queryWhereCondition_AnlyCdSplt="','".join(filter_list2)
        
        #Replace value of batch_effective_datetime in temporary queries
        univQuery = univQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                     missing_string = "'" + str(missing_string) + "'", 
                                     missing_mdm = "'" + str(missing_mdm) + "'", \
                                     queryWhereCondition_polpm = str(queryWhereCondition_polpm), 
                                     queryWhereCondition_AnlyCdSplt = "'" + str(queryWhereCondition_AnlyCdSplt) + "'")

        #Creating dataframes with temp queries
        univQuery = spark.sql(univQuery)

        #creating the table view with dataframes
        univQuery.createOrReplaceTempView('universe')

        #Replace value of batch effective date in SQL Query
        sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")

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