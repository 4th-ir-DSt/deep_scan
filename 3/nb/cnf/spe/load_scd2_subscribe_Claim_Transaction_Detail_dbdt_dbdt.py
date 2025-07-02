# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_Claim_Transaction_Detail_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Claim_Transaction_Detail table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Nikunj</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/07/05</td></tr>
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

# DBTITLE 1,Universe
univQuery="""
with SCMMovt as --bringing in data from SCM Movt
(
  select
    trnid,
    trncgy,
    MovtSeqNo,
    SCC,
    OCC,
    mktptdocc,
    SynPTDAdjd,
    MktPTDFeeOcc,
    SynOsAdjd,
    MktOsFee,
    MktOs,
    lakelastupdatedate,
    lakelastupdatetimestamp,
    lakeDeletedTimestamp
  from standardised_subscribe.dbo_SCMMovt
  where {batch_effective_datetime} >= LakeValidFromTimestamp
  AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
),
SCMMain as --bringing in data from SCMMain
(
  select
    trnid,
    trncgy,
    COALESCE(AdjrNm, {missing_string})                                                           AS AdjrNm,
    SgndLn,
    FilCd4,
    UnitId,
    PdROE,
    OSROE,
    AccYr,
    polid,
    unitpsu,
    lakelastupdatedate,
    lakelastupdatetimestamp,
    lakeDeletedTimestamp
  from standardised_subscribe.dbo_SCMMain
  where {batch_effective_datetime} >= LakeValidFromTimestamp
  AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
),
Inpol as --bringing in data from Inpol
(
  select
    case when lakeDeletedTimestamp is not null then {missing_string} else polid end              AS polid,
    case when lakeDeletedTimestamp is not null then {missing_string} else unitpsu end            AS unitpsu,
    case when lakeDeletedTimestamp is not null then NULL else calcln end                         AS calcln,
    case when lakeDeletedTimestamp is not null then NULL else SgndOrdPctg end                    AS SgndOrdPctg,
    lakelastupdatedate,
    lakelastupdatetimestamp,
    lakeDeletedTimestamp
  from standardised_subscribe.dbo_Inpol
  where {batch_effective_datetime} >= LakeValidFromTimestamp
  AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
)
select
  concat_ws('_',upper(trim(mov.trnid)),upper(trim(mov.trncgy)),mov.MovtSeqNo)                    AS Claim_Transaction_Reference,
  mov.mktptdocc,
  ip.SgndOrdPctg,
  COALESCE(mov.SCC, {missing_string})                                                            AS SCC,
  COALESCE(mov.OCC, {missing_string})                                                            AS OCC,
  mov.SynPTDAdjd,
  mov.MktPTDFeeOcc,
  mn.SgndLn,
  ip.calcln,
  CASE WHEN mov.OCC = mov.SCC THEN 1 ELSE mn.PdROE END                                           AS PdROE,
  CASE WHEN mov.OCC = mov.SCC THEN 1 ELSE mn.OSROE END                                           AS OSROE,
  mov.SynOsAdjd,
  mov.MktOs,
  mov.MktOsFee,
  COALESCE(mn.FilCd4, {missing_string})                                                          AS FilCd4,
  COALESCE(mn.UnitId, {missing_string})                                                          AS UnitId,
  COALESCE(mn.AccYr, '1900')                                                                     AS AccYr,
  mn.AdjrNm,
  greatest(mov.lakelastupdatedate, mn.lakelastupdatedate, ip.lakelastupdatedate)                 AS lakelastupdatedate,
  greatest(mov.lakelastupdatetimestamp, mn.lakelastupdatetimestamp, 
      ip.lakelastupdatetimestamp)                                                                AS lakelastupdatetimestamp,
  CASE 
      WHEN COALESCE(mov.lakeDeletedTimestamp, mn.lakeDeletedTimestamp) IS NOT NULL THEN {batch_effective_datetime} 
      ELSE NULL 
  END                                                                                            AS lakeDeletedTimestamp
from SCMMovt mov

inner join SCMMain mn 
on mn.trnid = mov.trnid
and mn.trncgy = mov.trncgy

left join InPol ip 
on mn.polid = ip.polid
and mn.unitpsu = ip.unitpsu
"""

# COMMAND ----------

# DBTITLE 1,Prepare SQL query
sqlQuery="""
SELECT
  --TRANSACTION TYPE--> Indemnity Paid--
  Claim_Transaction_Reference,
  '01'                                                                                          AS Transaction_Line_Number,
  'PIN'                                                                                         AS Transaction_Line_Code,
  cast(
    COALESCE(SynPTDAdjd / COALESCE(NULLIF(sgndln / 100, 0), NULLIF(calcln / 100, 0)), 0.00) - 
    COALESCE(MktPTDFeeOcc / COALESCE(NULLIF(PdROE, 0), NULLIF(OSROE, 0)), 0.00)
  as decimal(28,2))                                                                             AS Market_SCC,
  cast(
    COALESCE(SynPTDAdjd, 0.00) - 
    COALESCE(MktPTDFeeOcc / COALESCE(NULLIF(PdROE, 0), NULLIF(OSROE, 0)) * SgndLn / 100.0, 0.00)
  as decimal(28,2))                                                                             AS Line_SCC,
  cast(coalesce(
    (
        COALESCE(SynPTDAdjd / COALESCE(NULLIF(sgndln / 100, 0), NULLIF(calcln / 100, 0)), 0.00) - 
        COALESCE(MktPTDFeeOcc / COALESCE(NULLIF(PdROE, 0), NULLIF(OSROE, 0)), 0.00)
    ) 
    / NULLIF(SgndOrdPctg / 100, 0), 0.0
  ) as decimal(28,2))                                                                           AS Whole_SCC,
  cast(coalesce(mktptdocc,0.0) as decimal(28,2))                                                AS Market_OCC,
  cast(
    CASE
      WHEN SCC = OCC THEN COALESCE(SynPTDAdjd, 0.00) - COALESCE((MktPTDFeeOcc * SgndLn / 100), 0.00)
      ELSE COALESCE(MktPTDOcc * SgndLn / 100.0, 0.00)
    END
  as decimal(28,2))                                                                             AS Line_OCC,
  cast(coalesce(mktptdocc / NULLIF(SgndOrdPctg / 100, 0.00),0.00) as decimal(28,2))             AS Whole_OCC,
  lakelastupdatedate,
  lakelastupdatetimestamp,
  lakeDeletedTimestamp
from
  univ

-----------------------------------------------
UNION ALL
-----------------------------------------------

SELECT
  --TRANSACTION TYPE--> FEE PAID--
  Claim_Transaction_Reference,
  '02'                                                                                          AS Transaction_Line_Number,
  'PFE'                                                                                         AS Transaction_Line_Code,
  cast(coalesce(
    MktPTDFeeOcc / COALESCE(NULLIF(PdROE, 0), NULLIF(OSROE, 0)), 0.0
  ) as decimal(28,2))                                                                           AS Market_SCC,
  cast(coalesce(
    MktPTDFeeOcc / COALESCE(NULLIF(PdROE, 0), NULLIF(OSROE, 0)) * SgndLn / 100.0, 0.0
  ) as decimal(28,2))                                                                           AS Line_SCC,
  cast(coalesce(
    MktPTDFeeOcc / COALESCE(NULLIF(PdROE, 0), NULLIF(OSROE, 0)) / NULLIF(SgndOrdPctg / 100, 0), 0.0
  ) as decimal(28,2))                                                                           AS Whole_SCC,
  cast(coalesce(
    CASE
      WHEN SCC = OCC THEN MktPTDFeeOcc / COALESCE(NULLIF(PdROE, 0), NULLIF(OSROE, 0))
      ELSE MktPTDFeeOcc
    END, 0.0
  ) as decimal(28,2))                                                                           AS Market_OCC,
  cast(coalesce(MktPTDFeeOcc * SgndLn / 100.0, 0.0)  as decimal(28,2))                          AS Line_OCC,
  cast(coalesce(
    CASE
      WHEN SCC = OCC THEN MktPTDFeeOcc / COALESCE(NULLIF(PdROE, 0), NULLIF(OSROE, 0))
      ELSE MktPTDFeeOcc
    END 
    / NULLIF(SgndOrdPctg / 100, 0), 0.0
  ) as decimal(28,2))                                                                           AS Whole_OCC,
  lakelastupdatedate,
  lakelastupdatetimestamp,
  lakeDeletedTimestamp
from
  univ

-----------------------------------------------
UNION ALL
-----------------------------------------------

SELECT
  --TRANSACTION TYPE--> Indemnity OS--
  Claim_Transaction_Reference,
  '03'                                                                                          AS Transaction_Line_Number,
  'OIN'                                                                                         AS Transaction_Line_Code,
  cast(
    COALESCE(SynOsAdjd / COALESCE(NULLIF(sgndln / 100, 0), NULLIF(calcln / 100, 0)), 0.00) - 
    COALESCE(MktOsFee / COALESCE(NULLIF(OSROE, 0), NULLIF(PdROE, 0)), 0.00)
  as decimal(28,2))                                                                             AS Market_SCC,
  cast(
    coalesce(SynOsAdjd , 0) - 
    coalesce(MktOsFee / COALESCE(NULLIF(OSROE, 0), NULLIF(PdROE, 0)) * SgndLn / 100, 0.0)
  as decimal(28,2))                                                                             AS Line_SCC,
  cast(coalesce(
    (
        COALESCE(SynOsAdjd / COALESCE(NULLIF(sgndln / 100, 0), NULLIF(calcln / 100, 0)), 0.00) - 
        COALESCE(MktOsFee / COALESCE(NULLIF(OSROE, 0), NULLIF(PdROE, 0)), 0.00)
    )
    / NULLIF(SgndOrdPctg / 100,0), 0.0
  ) as decimal(28,2))                                                                           AS Whole_SCC,
  cast(coalesce(
    CASE
      WHEN SCC = OCC THEN SynOsAdjd / NULLIF(sgndln / 100, 0)
      ELSE MktOs
    END, 0.0
  ) as decimal(28,2))                                                                           AS Market_OCC,
  cast(coalesce(
    CASE
      WHEN SCC = OCC THEN SynOsAdjd
      ELSE MktOs * SgndLn / 100
    END, 0.0
  ) as decimal(28,2))                                                                           AS Line_OCC,
  cast(coalesce(
    CASE
      WHEN SCC = OCC THEN SynOsAdjd / NULLIF(sgndln / 100, 0)
      ELSE MktOs
    END 
    / NULLIF(SgndOrdPctg / 100, 0), 0.0
  ) as decimal(28,2))                                                                           AS Whole_OCC,
  lakelastupdatedate,
  lakelastupdatetimestamp,
  lakeDeletedTimestamp
FROM
  univ
WHERE trim(upper(AdjrNm)) <> 'ACRX'

-----------------------------------------------
UNION ALL
-----------------------------------------------

SELECT
  --TRANSACTION TYPE--> FEE OS--
  Claim_Transaction_Reference,
  '04'                                                                                          AS Transaction_Line_Number,
  'OFE'                                                                                         AS Transaction_Line_Code,
  cast(coalesce(
    MktOsFee / COALESCE(NULLIF(OSROE, 0), NULLIF(PdROE, 0)), 0.0
  ) as decimal(28,2))                                                                           AS Market_SCC,
  cast(coalesce(
    (MktOsFee / COALESCE(NULLIF(OSROE, 0), NULLIF(PdROE, 0)) * SgndLn / 100), 0.0
  ) as decimal(28,2))                                                                           AS Line_SCC,
  cast(coalesce(
    (MktOsFee / COALESCE(NULLIF(OSROE, 0), NULLIF(PdROE, 0)))/NULLIF(SgndOrdPctg/100,0), 0.0
  ) as decimal(28,2))                                                                           AS Whole_SCC,
  cast(coalesce(
    CASE
      WHEN SCC = OCC THEN MktOsFee / COALESCE(NULLIF(OSROE, 0), NULLIF(PdROE, 0))
      ELSE MktOsFee
    END, 0.0
  ) as decimal(28,2))                                                                           AS Market_OCC,
  cast(coalesce(
    CASE
      WHEN SCC = OCC THEN (MktOsFee / COALESCE(NULLIF(OSROE, 0), NULLIF(PdROE, 0)) * SgndLn / 100)
      ELSE (MktOsFee * SgndLn / 100)
    END, 0.0
  ) as decimal(28,2))                                                                           AS Line_OCC,
  cast(coalesce(
    CASE
      WHEN SCC = OCC THEN MktOsFee / COALESCE(NULLIF(OSROE, 0), NULLIF(PdROE, 0))
      ELSE MktOsFee
    END 
    / NULLIF(SgndOrdPctg / 100, 0), 0.0
  ) as decimal(28,2))                                                                           AS Whole_OCC,
  lakelastupdatedate,
  lakelastupdatetimestamp,
  lakeDeletedTimestamp
FROM
  univ  
WHERE trim(upper(AdjrNm)) <> 'ACRX'

-----------------------------------------------
UNION ALL
-----------------------------------------------

SELECT
  --TRANSACTION TYPE--> Additional Outstanding Indemnity--
  Claim_Transaction_Reference,
  '05'                                                                                          AS Transaction_Line_Number,
  'AOI'                                                                                         AS Transaction_Line_Code,
  cast(coalesce(
    SynOsAdjd / COALESCE(NULLIF(sgndln / 100, 0), NULLIF(calcln / 100, 0)), 0.0
  ) as decimal(28,2))                                                                           AS Market_SCC,
  cast(coalesce(SynOsAdjd, 0.0) as decimal(28,2))                                               AS Line_SCC,
  cast(coalesce(
    SynOsAdjd / COALESCE(NULLIF(sgndln / 100, 0), NULLIF(calcln / 100, 0)) / NULLIF(SgndOrdPctg / 100,0), 0.0
  ) as decimal(28,2))                                                                           AS Whole_SCC,
  cast(coalesce(
    CASE
      WHEN SCC = OCC THEN SynOsAdjd / NULLIF(sgndln / 100, 0)
      ELSE SynOsAdjd * (COALESCE(NULLIF(OSROE, 0), NULLIF(PdROE, 0),0) / NULLIF(sgndln/100,0))
    END, 0.0
  ) as decimal(28,2))                                                                           AS Market_OCC,
  cast(coalesce(
    CASE
      WHEN SCC = OCC THEN SynOsAdjd
      ELSE SynOsAdjd * COALESCE(NULLIF(OSROE, 0), NULLIF(PdROE, 0),0)
    END, 0.0
  ) as decimal(28,2))                                                                           AS Line_OCC,
  cast(coalesce(
    CASE
      WHEN SCC = OCC THEN SynOsAdjd / NULLIF(sgndln / 100, 0)
      ELSE SynOsAdjd * (COALESCE(NULLIF(OSROE, 0), NULLIF(PdROE, 0),0) / NULLIF(sgndln/100,0))
    END 
    / NULLIF(SgndOrdPctg / 100, 0), 0.0
  ) as decimal(28,2))                                                                           AS Whole_OCC,
  lakelastupdatedate,
  lakelastupdatetimestamp,
  lakeDeletedTimestamp
FROM
  univ  
WHERE trim(upper(AdjrNm)) = 'ACRX'

-----------------------------------------------
UNION ALL
-----------------------------------------------

SELECT
  --TRANSACTION TYPE--> Paid Claim Loss Fund--
  Claim_Transaction_Reference,
  '07'                                                                                          AS Transaction_Line_Number,
  'PCLF'                                                                                        AS Transaction_Line_Code,
  cast(
      COALESCE(SynPTDAdjd / COALESCE(NULLIF(sgndln/100,0), NULLIF(calcln/100,0)), 0.00)
      -- Below line removed post bug 51811 raised by Dan
      -- - COALESCE(MktPTDFeeOcc / COALESCE(NULLIF(PdROE, 0),NULLIF(OSROE,0)), 0.00)
  *
  CASE WHEN FilCd4 LIKE 'LF%' THEN 1.0 
       ELSE 0.0 
  END *
  CASE 
	   WHEN cast(UnitId as int) = 839 AND AccYr IN(2002, 2003) THEN 1.0
	   WHEN cast(UnitId as int) = 529 AND AccYr = 2001 THEN 1.0
	   WHEN cast(UnitId as int) = 529 THEN 0.0
       -- Early Canopius Syndicates
	   WHEN cast(UnitId as int) IN(0270, 0463, 0544, 0657, 0741, 2741, 0839) THEN 0.0
	   -- CreeChurch Syndicates
	   WHEN cast(UnitId as int) IN(0962, 1607, 2607, 2962, 3786) THEN 0.0
	   ELSE 1.0 
  END as decimal(28,2))                                                                         AS Market_SCC,
  cast(
      COALESCE(NULLIF(SynPTDAdjd, 0), 0.00)
      -- Below line removed and also (SynPTDAdjd * -1) updated post bug 51811 raised by Dan 
      -- - COALESCE(MktPTDFeeOcc / COALESCE(NULLIF(PdROE,0), NULLIF(OSROE,0)) * NULLIF(SgndLn / 100.0, 0.0), 0.00)
  *
  CASE 
       WHEN FilCd4 LIKE 'LF%' 
       THEN 1.0 ELSE 0.0 
  END  *
  CASE 
	   WHEN cast(UnitId as int) = 839 AND AccYr IN(2002, 2003) THEN 1.0
	   WHEN cast(UnitId as int) = 529 AND AccYr = 2001 THEN 1.0
	   WHEN cast(UnitId as int) = 529 THEN 0.0
	   -- Early Canopius Syndicates
	   WHEN cast(UnitId as int) IN(0270, 0463, 0544, 0657, 0741, 2741, 0839) THEN 0.0
	   -- CreeChurch Syndicates
	   WHEN cast(UnitId as int) IN(0962, 1607, 2607, 2962, 3786) THEN 0.0
	   ELSE 1.0 
  END as decimal(28,2))                                                                         AS Line_SCC,
  cast(coalesce(
        COALESCE(SynPTDAdjd / COALESCE(NULLIF(sgndln / 100,0), NULLIF(calcln / 100, 0)), 0.00) 
        -- Below line removed post bug 51811 raised by Dan 
        -- - COALESCE(MktPTDFeeOcc / COALESCE(NULLIF(PdROE, 0), NULLIF(OSROE,0)), 0.00)
    / NULLIF(SgndOrdPctg / 100, 0), 0.0) * 
  CASE 
      WHEN FilCd4 LIKE 'LF%' THEN 1.0 
      ELSE 0.0 
  END *                                                                 
  CASE 
	  WHEN cast(UnitId as int) = 839 AND AccYr IN(2002, 2003) THEN 1.0
	  WHEN cast(UnitId as int) = 529 AND AccYr = 2001 THEN 1.0
	  WHEN cast(UnitId as int) = 529 THEN 0.0
	  -- Early Canopius Syndicates
	  WHEN cast(UnitId as int) IN(0270, 0463, 0544, 0657, 0741, 2741, 0839) THEN 0.0
	  -- CreeChurch Syndicates
	  WHEN cast(UnitId as int) IN(0962, 1607, 2607, 2962, 3786) THEN 0.0
	  ELSE 1.0 
  END as decimal(28,2))                                                                         AS Whole_SCC,
  cast(coalesce(mktptdocc, 0.0) *
  CASE 
       WHEN FilCd4 LIKE 'LF%' THEN 1.0 
       ELSE 0.0 
  END *
  CASE 
	   WHEN cast(UnitId as int) = 839 AND AccYr IN(2002, 2003) THEN 1.0
	   WHEN cast(UnitId as int) = 529 AND AccYr = 2001 THEN 1.0
	   WHEN cast(UnitId as int) = 529 THEN 0.0
	   -- Early Canopius Syndicates
	   WHEN cast(UnitId as int) IN(0270, 0463, 0544, 0657, 0741, 2741, 0839) THEN 0.0
	   -- CreeChurch Syndicates
	   WHEN cast(UnitId as int) IN(0962, 1607, 2607, 2962, 3786) THEN 0.0
	   ELSE 1.0 
  END as decimal(28,2))                                                                         AS Market_OCC,
  cast(
    CASE 
        WHEN SCC = OCC THEN COALESCE(SynPTDAdjd, 0.00)
        -- Below line removed post bug 51811 raised by Dan 
        -- - COALESCE((MktPTDFeeOcc * SgndLn / 100), 0.00) 
        ELSE COALESCE(MktPTDOcc * SgndLn / 100, 0.00)
    END *
    CASE 
        WHEN FilCd4 LIKE 'LF%' 
        THEN 1.0 ELSE 0.0 
    END * 
    CASE 
		WHEN cast(UnitId as int) = 839 AND AccYr IN(2002, 2003) THEN 1.0
		WHEN cast(UnitId as int) = 529 AND AccYr = 2001 THEN 1.0
		WHEN cast(UnitId as int) = 529 THEN 0.0
		-- Early Canopius Syndicates
		WHEN cast(UnitId as int) IN (0270, 0463, 0544, 0657, 0741, 2741, 0839) THEN 0.0
		-- CreeChurch Syndicates
		WHEN cast(UnitId as int) IN (0962, 1607, 2607, 2962, 3786) THEN 0.0
		ELSE 1.0 
    END as decimal(28,2))                                                                       AS Line_OCC,
  cast(
  coalesce(mktptdocc / NULLIF(SgndOrdPctg/100,0), 0.00) *  
  CASE 
      WHEN FilCd4 LIKE 'LF%' THEN 1.0 
      ELSE 0.0 
  END * 
  CASE 
	  WHEN cast(UnitId as int) = 839 AND AccYr IN(2002, 2003) THEN 1.0
	  WHEN cast(UnitId as int) = 529 AND AccYr = 2001 THEN 1.0
	  WHEN cast(UnitId as int) = 529 THEN 0.0
	  -- Early Canopius Syndicates
	  WHEN cast(UnitId as int) IN(0270, 0463, 0544, 0657, 0741, 2741, 0839) THEN 0.0
	  -- CreeChurch Syndicates
	  WHEN cast(UnitId as int) IN(0962, 1607, 2607, 2962, 3786) THEN 0.0
	  ELSE 1.0 
  END as decimal(28,2))                                                                         AS Whole_OCC,
  lakelastupdatedate,
  lakelastupdatetimestamp,
  lakeDeletedTimestamp
FROM
  univ  

-----------------------------------------------
UNION ALL
-----------------------------------------------
-- confirmation given by Mohit 
SELECT
  --TRANSACTION TYPE--> Additional Outstanding Fee--
  Claim_Transaction_Reference,
  '06'                                                                                          AS Transaction_Line_Number,
  'AOF'                                                                                         AS Transaction_Line_Code,
  cast(0.0 as decimal(28,2))                                                                    AS Market_SCC,
  cast(0.0 as decimal(28,2))                                                                    AS Line_SCC,
  cast(0.0 as decimal(28,2))                                                                    AS Whole_SCC,
  cast(0.0 as decimal(28,2))                                                                    AS Market_OCC,
  cast(0.0 as decimal(28,2))                                                                    AS Line_OCC,
  cast(0.0 as decimal(28,2))                                                                    AS Whole_OCC,
  lakelastupdatedate,
  lakelastupdatetimestamp,
  lakeDeletedTimestamp
FROM
  univ  
WHERE trim(upper(AdjrNm)) = 'ACRX'
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwClaim_Transaction_Detail'
  viewTables = 'standardised_subscribe.dbo_scmmovt, standardised_subscribe.dbo_scmmain, standardised_subscribe.dbo_Inpol'

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
        
        #Replace value of batch_effective_datetime in temporary queries
        univQuery_1 = univQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                       missing_string = "'" + str(missing_string) + "'", 
                                       missing_mdm = "'" + str(missing_mdm) + "'")
        
        #Creating dataframes with temp queries
        df_univ = spark.sql(univQuery_1)
        #creating the views of table used in main sqlQuery
        df_univ.createOrReplaceTempView("univ")
        
        
        
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