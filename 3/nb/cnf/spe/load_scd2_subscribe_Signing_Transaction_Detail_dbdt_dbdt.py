# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_Signing_Transaction_Detail_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Signing_Transaction_Detail table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Nikunj</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/06/07</td></tr>
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
univ_query=("""
SELECT Concat_ws('_',usmi.trnid,usmi.trncgy,COALESCE(usmd.instno, '01'))              AS Signing_Transaction_Reference,
    COALESCE(usmi.occ, {missing_string})                                              AS OCC,
    COALESCE(usmi.scc, {missing_string})                                              AS SCC,
    COALESCE(usmi.qulgcgycd, {missing_string})                                        AS QulgCgyCd,
    usmd.netamt, 
    usmi.synnetamt,
    CASE 
        WHEN usmi.occ = usmi.scc THEN 1
        ELSE usmi.roe
    END                                                                               AS ROE, 
    CASE 
        WHEN usmi.ttldisc <= 0.00 THEN 0.00
        WHEN usmi.ttldisc >= 100.00 THEN 100.00
        ELSE COALESCE(usmi.ttldisc, 0.00)
    END                                                                               AS TtlDisc,
    CASE 
        WHEN usmi.sgndln <= 0.00 THEN 0.00
        WHEN usmi.sgndln >= 100.00 THEN 100.00
        ELSE usmi.sgndln
    END                                                                               AS SgndLn,
    CASE 
        WHEN usmi.sgndord <= 0.00 THEN 0.00
        WHEN usmi.sgndord >= 100.00 THEN 100.00
        ELSE usmi.sgndord
    END                                                                               AS SgndOrd,
    COALESCE(usmi.cac, {missing_string})                                              AS CAC,
    COALESCE(usmi.UnitId, {missing_string})                                           AS UnitId,
    COALESCE(usmi.AccYr, '1900')                                                      AS AccYr,
    COALESCE(usmi.FILCd4, {missing_string})                                           AS FILCd4,
    OverseasTax, 
    greatest(usmi.lakelastupdatedate, usmd.lakelastupdatedate, ot.lakelastupdatedate) AS lakelastupdatedate,
    greatest(usmi.lakeLastUpdateTimestamp, usmd.lakeLastUpdateTimestamp, 
        ot.lakeLastUpdateTimestamp)                                                   AS lakelastupdatetimestamp,
    CASE 
        when usmi.lakeDeletedTimestamp Is Not Null THEN {batch_effective_datetime}
        ELSE NULL
    END                                                                               AS lakeDeletedTimestamp
FROM standardised_subscribe.dbo_usmmain usmi

--to bring installment number as well as other details from USmDeferred
LEFT JOIN  
(
    select trnid
           ,trncgy
           ,unitpsu
           ,case when lakedeletedtimestamp is not null then null else instno end as instno
           ,case when lakedeletedtimestamp is not null then null else netamt end as netamt
           ,lakelastupdatedate
           ,lakeLastUpdateTimestamp
           ,lakeDeletedTimestamp
    from standardised_subscribe.dbo_usmdefd 
    where {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) usmd
ON upper(usmi.trnid) = upper(usmd.trnid)
AND upper(usmi.trncgy)= upper(usmd.trncgy)
AND upper(usmi.unitpsu)= upper(usmd.unitpsu)

--to bring overseas tax details
LEFT JOIN 
(
    SELECT case when lakeDeletedTimestamp is not null then null else TrnId   END AS TrnId, 
           case when lakeDeletedTimestamp is not null then null else TrnCgy  END AS TrnCgy, 
           case when lakeDeletedTimestamp is not null then null else UnitPsu END AS UnitPsu,
           CAST(SUBSTRING(Narr, 21,16) AS DECIMAL(16,2)) * 
               CASE WHEN SUBSTRING(Narr, 37,1) = '-' THEN -1 ELSE 1 END          AS OverseasTax,
           lakelastupdatedate,
           lakeLastUpdateTimestamp,
           lakeDeletedTimestamp
    FROM standardised_subscribe.dbo_UsmNarr
    WHERE lnno='T' AND narrty='T'
    AND {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
) ot
ON upper(usmi.TrnId)=upper(ot.trnid)
AND upper(usmi.TrnCgy)=upper(ot.TrnCgy)
AND upper(usmi.UnitPsu)=upper(ot.UnitPsu)

where {batch_effective_datetime} >= usmi.lakevalidfromtimestamp
AND {batch_effective_datetime} < COALESCE(usmi.lakevalidtotimestamp, CURRENT_TIMESTAMP())
""")

# COMMAND ----------

# DBTITLE 1,Prepare SQL Query
sqlQuery= """
WITH CTE_CAC_Net AS
    (
        SELECT 
		'0'  CAC_QulgCgyCd, 'USM_Treaty_Transaction' Measure, 'TTY' Transaction_Line, '00' as Transaction_Code
		UNION ALL 
		-- USM_Inward_Premium
		SELECT '1', 'USM_Premium', 'GN', '01' as Transaction_Code  UNION ALL 
		SELECT '2', 'USM_Additional_Premium', 'GN', '01' as Transaction_Code UNION ALL 
		SELECT '3', 'USM_Return_Premium', 'GN', '01' as Transaction_Code UNION ALL 
		-- USM_Inward_Reinstatement
		SELECT '1H', 'USM_ReInstatement_Premium', 'RIN', '03' as Transaction_Code UNION ALL 
		SELECT '2H', 'USM_ReInstatement_Additional_Premium', 'RIN', '03' as Transaction_Code UNION ALL 
		SELECT '3H', 'USM_ReInstatement_Return_Premium', 'RIN', '03' as Transaction_Code UNION ALL 
		-- USM_Inward_Profit_Commission
		SELECT '1I', 'USM_Profit_Commission_Premium', 'PC', '05' as Transaction_Code  UNION ALL 
		SELECT '2I', 'USM_Profit_Commission_Additional_Premium', 'PC', '05' as Transaction_Code UNION ALL 
		SELECT '3I', 'USM_Profit_Commission_Return_Premium', 'PC', '05' as Transaction_Code UNION ALL 
		-- USM_Inward_Paid_Claim
		SELECT '4', 'USM_Claim', 'PIN' , '04' as Transaction_Code UNION ALL 
		SELECT '5', 'USM_Refund_Claim', 'PIN', '04' as Transaction_Code  UNION ALL 
		SELECT '4H', 'USM_Claim', 'PIN', '04' as Transaction_Code  UNION ALL 
		SELECT '5H', 'USM_Refund_Claim', 'PIN', '04' as Transaction_Code  UNION ALL 
		SELECT '4I', 'USM_Claim', 'PIN', '04' as Transaction_Code  UNION ALL 
		SELECT '5I', 'USM_Refund_Claim', 'PIN', '04' as Transaction_Code
    ), 
CTE_CAC_Gross AS 
    (
		SELECT '1' CAC_QulgCgyCd, 'USM_Premium_Gross' Measure, 'GG' Transaction_Line, '02' as Transaction_Code UNION ALL 
		SELECT '2', 'USM_Additional_Premium_Gross', 'GG', '02' as Transaction_Code UNION ALL 
		SELECT '3', 'USM_Return_Premium_Gross', 'GG', '02' as Transaction_Code
    ), 
CTE_CAC_Acq_Costs AS 		---- USM_Inward_Reinstatement_Acq
    (
        SELECT '1' CAC_QulgCgyCd, 'USM_Premium' Measure, 'ACQ' Transaction_Line, '06' as Transaction_Code  UNION ALL 
		SELECT '2', 'USM_Additional_Premium', 'ACQ', '06' as Transaction_Code UNION ALL 
		SELECT '3', 'USM_Return_Premium', 'ACQ', '06' as Transaction_Code
    ) 

SELECT Signing_Transaction_Reference
      ,cac.Transaction_Code                                                                              AS Transaction_Line_Number
      ,COALESCE(NULLIF(Trim(cac.Transaction_Line),''), {missing_string})                                 AS Transaction_Line_Code
      ,CAST(SUM(
             COALESCE(netamt,synnetamt) 
			 * 1 / coalesce(NULLIF(Round((coalesce(NULLIF((100-TtlDisc)/100,0),1)), 6), 0), 1.0)
             * 1 / coalesce(NULLIF(SgndLn/100,0),1)
			    ) as DECIMAL(28,2))                                                                      AS Market_SCC
      ,CAST(SUM(
             COALESCE(netamt,synnetamt) * 1 / coalesce(NULLIF(Round((coalesce(NULLIF((100-TtlDisc)/100,0),1)), 6), 0), 1.0)
                ) as DECIMAL(28,2))                                                                      AS Line_SCC
      ,CAST(SUM(
             COALESCE(netamt,synnetamt) 
			 * 1 / coalesce(NULLIF(Round((coalesce(NULLIF((100-TtlDisc)/100,0),1)), 6), 0), 1.0)
			 * 1 / coalesce(NULLIF(SgndLn/100,0),1)
			 * 1 / coalesce(NULLIF(SgndOrd/100,0),1)
                ) as DECIMAL(28,2))                                                                      AS Whole_SCC
      ,CAST(SUM(
			 COALESCE(netamt,synnetamt) 
			 * 1 / coalesce(NULLIF(Round((coalesce(NULLIF((100-TtlDisc)/100,0),1)), 6), 0), 1.0)
			 * coalesce(NULLIF(ROE, 0.0), 1.0)
			 * 1 / coalesce(NULLIF(SgndLn/100,0),1)
				) as DECIMAL(28,2))                                                                      AS Market_OCC
      ,CAST(SUM(
             COALESCE(netamt,synnetamt) 
			 * 1 / coalesce(NULLIF(Round((coalesce(NULLIF((100-TtlDisc)/100,0),1)), 6), 0), 1.0)
			 * coalesce(NULLIF(ROE, 0.0), 1.0)
				) as DECIMAL(28,2))                                                                      AS Line_OCC
      ,CAST(SUM(
			 COALESCE(netamt,synnetamt) 
			 * 1 / coalesce(NULLIF(Round((coalesce(NULLIF((100-TtlDisc)/100,0),1)), 6), 0), 1.0)
			 * coalesce(NULLIF(ROE, 0.0), 1.0)
			 * 1 / coalesce(NULLIF(SgndLn/100,0),1)
			 * 1 / coalesce(NULLIF(SgndOrd/100,0),1)
				) AS DECIMAL(28,2))                                                                      AS Whole_OCC
      ,lakelastupdatedate
      ,lakeLastUpdateTimestamp
      ,lakeDeletedTimestamp 
from univ
--to bring the transaction types
INNER JOIN cte_cac_gross cac
ON cac.cac_qulgcgycd = concat(univ.cac, CASE univ.qulgcgycd WHEN 'H' THEN 'H' WHEN 'I' THEN 'I' ELSE '' END)
group by 1, 2, 3, 10, 11, 12


union all


SELECT Signing_Transaction_Reference
      ,cac.Transaction_Code                                                                              AS Transaction_Line_Number
      ,COALESCE(NULLIF(Trim(cac.Transaction_Line),''), {missing_string})                                 AS Transaction_Line_Code
      ,CAST(SUM(
              COALESCE(Netamt, SynNetAmt, 0.0) * 1 / coalesce(NULLIF(SgndLn/100,0),1)
                ) AS DECIMAL(28,2))                                                                      AS Market_SCC
      ,CAST(SUM(
             COALESCE(Netamt, SynNetAmt, 0.0)
                ) AS DECIMAL(28,2))                                                                      AS Line_SCC
      ,CAST(SUM(
             COALESCE(Netamt, SynNetAmt, 0.0) * 1 / coalesce(NULLIF(SgndLn/100,0),1) * 1 / coalesce(NULLIF(SgndOrd/100,0),1)
                ) AS DECIMAL(28,2))                                                                      AS Whole_SCC
      ,CAST(SUM(
             COALESCE(NetAmt ,SynNetAmt, 0.0 ) * coalesce(NULLIF(ROE, 0.0), 1.0) * 1 / coalesce(NULLIF(SgndLn/100,0),1)
				) AS DECIMAL(28,2))                                                                      AS Market_OCC
      ,CAST(SUM(
             COALESCE(NetAmt * coalesce(NULLIF(ROE, 0.0), 1.0), SynNetAmt * coalesce(NULLIF(ROE, 0.0), 1.0) , 0.0)
                )  AS DECIMAL(28,2))                                                                     AS Line_OCC
      ,CAST(SUM(
             COALESCE( NetAmt, SynNetAmt, 0.0) 
             * coalesce(NULLIF(ROE, 0.0), 1.0) 
             * 1 / coalesce(NULLIF(SgndLn/100,0),1)
			 * 1 / coalesce(NULLIF(SgndOrd/100,0),1)
				) AS DECIMAL(28,2))                                                                      AS Whole_OCC    
      ,lakelastupdatedate
      ,lakeLastUpdateTimestamp
      ,lakeDeletedTimestamp
from univ
--to bring the transaction types
INNER JOIN cte_cac_net cac
ON cac.cac_qulgcgycd = concat(univ.cac, CASE univ.qulgcgycd WHEN 'H' THEN 'H' WHEN 'I' THEN 'I' ELSE '' END)
group by 1, 2, 3, 10, 11, 12


union all


SELECT Signing_Transaction_Reference
      ,cac.Transaction_Code                                                                              AS Transaction_Line_Number
      ,COALESCE(NULLIF(Trim(cac.Transaction_Line),''), {missing_string})                                 AS Transaction_Line_Code
      ,CAST(SUM(
             COALESCE(netamt,synnetamt) 
			 * (1 / COALESCE(NULLIF(Round((COALESCE(NULLIF((100-TtlDisc)/100,0),1)), 6), 0), 1.0) - 1)
             * 1 / COALESCE(NULLIF(SgndLn/100,0),1)
			    ) as DECIMAL(28,2))                                                                      AS Market_SCC	
      ,CAST(SUM(
             COALESCE(netamt,synnetamt) * (1 / COALESCE(NULLIF(Round((COALESCE(NULLIF((100-TtlDisc)/100,0),1)), 6), 0), 1.0) - 1)
                ) AS DECIMAL(28,2))                                                                      AS Line_SCC
      ,CAST(SUM(
             COALESCE(netamt,synnetamt) 
			 * (1 / COALESCE(NULLIF(Round((COALESCE(NULLIF((100-TtlDisc)/100,0),1)), 6), 0), 1.0) - 1)
			 * 1 / COALESCE(NULLIF(SgndLn/100,0),1)
			 * 1 / COALESCE(NULLIF(SgndOrd/100,0),1)
                ) as DECIMAL(28,2))                                                                      AS Whole_SCC
      ,CAST(SUM(
             COALESCE(netamt,synnetamt) 
             * (1 / COALESCE(NULLIF(Round((COALESCE(NULLIF((100-TtlDisc)/100,0),1)), 6), 0), 1.0) - 1)
             * COALESCE(NULLIF(ROE, 0.0), 1.0)
             * 1 / COALESCE(NULLIF(SgndLn/100,0),1)
                ) as DECIMAL(28,2))                                                                      AS Market_OCC
      ,CAST(SUM(
             COALESCE(netamt,synnetamt) 
             * (1 / COALESCE(NULLIF(Round((COALESCE(NULLIF((100-TtlDisc)/100,0),1)), 6), 0), 1.0) - 1)
             * COALESCE(NULLIF(ROE, 0.0), 1.0)
				)   as DECIMAL(28,2))                                                                    AS Line_OCC
      ,CAST(SUM(
			 COALESCE(netamt,synnetamt) 
			 * (1 / COALESCE(NULLIF(Round((COALESCE(NULLIF((100-TtlDisc)/100,0),1)), 6), 0), 1.0) - 1)
			 * COALESCE(NULLIF(ROE, 0.0), 1.0)
			 * 1 / COALESCE(NULLIF(SgndLn/100,0),1)
			 * 1 / COALESCE(NULLIF(SgndOrd/100,0),1)
				) as DECIMAL(28,2))                                                                      AS Whole_OCC
      ,lakelastupdatedate
      ,lakeLastUpdateTimestamp
      ,lakeDeletedTimestamp
from univ
--to bring the transaction types
INNER JOIN CTE_CAC_Acq_Costs cac
ON cac.cac_qulgcgycd = concat(univ.cac, CASE univ.qulgcgycd WHEN 'H' THEN 'H' WHEN 'I' THEN 'I' ELSE '' END)
group by 1, 2, 3, 10, 11, 12


UNION ALL


SELECT univ.Signing_Transaction_Reference,
'07'                                                                                                     AS Transaction_Code,
'OIT'                                                                                                    AS Transaction_Line_Code,
CAST(SUM(OverseasTax / COALESCE(NULLIF(ROE,0),1)) as DECIMAL(28,2))                                      AS Market_SCC,
CAST(SUM(OverseasTax / COALESCE(NULLIF(ROE,0),1) * (COALESCE(sgndln,0)/100)) AS DECIMAL(28,2))           AS Line_SCC,
CAST(SUM(OverseasTax/(COALESCE(NULLIF(SgndOrd,0),100)/100)/COALESCE(NULLIF(ROE,0),1)) as DECIMAL(28,2))  AS Whole_SCC,
CAST(SUM(OverseasTax) as DECIMAL(28,2))                                                                  AS Market_OCC,
CAST(SUM(OverseasTax * (COALESCE(sgndln,0)/100)) AS DECIMAL(28,2))                                       AS Line_OCC,
CAST(SUM(OverseasTax/(COALESCE(NULLIF(SgndOrd,0),100)/100)) as DECIMAL(28,2))                            AS Whole_OCC,
lakelastupdatedate,
lakeLastUpdateTimestamp,
lakeDeletedTimestamp
FROM univ
GROUP BY univ.Signing_Transaction_Reference,
lakelastupdatedate,
lakeLastUpdateTimestamp,
lakeDeletedTimestamp
HAVING Market_OCC <> 0

UNION ALL

SELECT univ.Signing_Transaction_Reference,
'08'                                                                                                     AS Transaction_Code,
'PCLF'                                                                                                   AS Transaction_Line_Code,
CAST(SUM(COALESCE(Netamt, SynNetAmt, 0.0)
							* 1 / COALESCE(NULLIF(SgndLn/100,0),1)
							* CASE WHEN FILCd4 LIKE 'LF%' THEN 1 ELSE 0 END
							* CASE 
								WHEN UnitId = 839 AND AccYr IN(2002, 2003) THEN 1
								WHEN UnitId = 839 THEN 0
								WHEN UnitId = 529 AND AccYr = 2001 THEN 1
								WHEN UnitId = 529 THEN 0
								-- Early Canopius Syndicates
								WHEN UnitId IN(0270, 0463, 0544, 0657, 0741, 2741, 0839) THEN 0
								-- CreeChurch Syndicates
								WHEN UnitId IN(0962, 1607, 2607, 2962, 3786) THEN 0
								ELSE 1
							END) as DECIMAL(28,2))                                                       AS Market_SCC,
CAST(SUM(COALESCE(Netamt, SynNetAmt, 0.0) 
						* CASE WHEN FILCd4 LIKE 'LF%' THEN 1 ELSE 0 END
						* CASE 
								WHEN UnitId = 839 AND AccYr IN(2002, 2003) THEN 1
								WHEN UnitId = 839 THEN 0
								WHEN UnitId = 529 AND AccYr = 2001 THEN 1
								WHEN UnitId = 529 THEN 0
								-- Early Canopius Syndicates
								WHEN UnitId IN(0270, 0463, 0544, 0657, 0741, 2741, 0839) THEN 0
								-- CreeChurch Syndicates
								WHEN UnitId IN(0962, 1607, 2607, 2962, 3786) THEN 0
								ELSE 1
							END) AS DECIMAL(28,2))                                                       AS Line_SCC,
CAST(SUM(COALESCE(Netamt, SynNetAmt, 0.0)
					* 1 / COALESCE(NULLIF(SgndLn/100,0),1)
					* 1 / COALESCE(NULLIF(SgndOrd/100,0),1)
					* CASE WHEN FILCd4 LIKE 'LF%' THEN 1 ELSE 0 END
					* CASE 
							WHEN UnitId = 839 AND AccYr IN(2002, 2003) THEN 1
							WHEN UnitId = 839 THEN 0
							WHEN UnitId = 529 AND AccYr = 2001 THEN 1
							WHEN UnitId = 529 THEN 0
							-- Early Canopius Syndicates
							WHEN UnitId IN(0270, 0463, 0544, 0657, 0741, 2741, 0839) THEN 0
							-- CreeChurch Syndicates
							WHEN UnitId IN(0962, 1607, 2607, 2962, 3786) THEN 0
							ELSE 1
						END) as DECIMAL(28,2))                                                           AS Whole_SCC,
CAST(SUM(COALESCE(NetAmt ,SynNetAmt, 0.0 )
				* COALESCE(NULLIF(ROE, 0.0), 1.0)
				* 1 / COALESCE(NULLIF(SgndLn/100,0),1)			
				* CASE WHEN FILCd4 LIKE 'LF%' THEN 1 ELSE 0 END
				* CASE 
						WHEN UnitId = 839 AND AccYr IN(2002, 2003) THEN 1
						WHEN UnitId = 839 THEN 0
						WHEN UnitId = 529 AND AccYr = 2001 THEN 1
						WHEN UnitId = 529 THEN 0
						-- Early Canopius Syndicates
						WHEN UnitId IN(0270, 0463, 0544, 0657, 0741, 2741, 0839) THEN 0
						-- CreeChurch Syndicates
						WHEN UnitId IN(0962, 1607, 2607, 2962, 3786) THEN 0
						ELSE 1
					END) as DECIMAL(28,2))                                                               AS Market_OCC,
CAST(SUM(COALESCE(NetAmt * COALESCE(NULLIF(ROE, 0.0), 1.0), SynNetAmt 
				         * COALESCE(NULLIF(ROE, 0.0), 1.0), 0.0)
			             * CASE WHEN FILCd4 LIKE 'LF%' THEN 1 ELSE 0 END
						 * CASE 
								WHEN UnitId = 839 AND AccYr IN(2002, 2003) THEN 1
								WHEN UnitId = 839 THEN 0
								WHEN UnitId = 529 AND AccYr = 2001 THEN 1
								WHEN UnitId = 529 THEN 0
								-- Early Canopius Syndicates
								WHEN UnitId IN(0270, 0463, 0544, 0657, 0741, 2741, 0839) THEN 0
								-- CreeChurch Syndicates
								WHEN UnitId IN(0962, 1607, 2607, 2962, 3786) THEN 0
								ELSE 1
							END) as DECIMAL(28,2))                                                       AS Line_OCC,
CAST(SUM(COALESCE(NetAmt, SynNetAmt, 0.0)
				* COALESCE(NULLIF(ROE, 0.0), 1.0)
				* 1 / COALESCE(NULLIF(SgndLn/100,0),1)
				* 1 / COALESCE(NULLIF(SgndOrd/100,0),1)
				* CASE WHEN FILCd4 LIKE 'LF%' THEN 1 ELSE 0 END
				* CASE 
						WHEN UnitId = 839 AND AccYr IN(2002, 2003) THEN 1
						WHEN UnitId = 839 THEN 0
						WHEN UnitId = 529 AND AccYr = 2001 THEN 1
						WHEN UnitId = 529 THEN 0
						-- Early Canopius Syndicates
						WHEN UnitId IN(0270, 0463, 0544, 0657, 0741, 2741, 0839) THEN 0
						-- CreeChurch Syndicates
						WHEN UnitId IN(0962, 1607, 2607, 2962, 3786) THEN 0
						ELSE 1
					END ) as DECIMAL(28,2))                                                            AS Whole_OCC,
lakelastupdatedate,
lakeLastUpdateTimestamp,
lakeDeletedTimestamp
FROM univ
LEFT JOIN CTE_cac_net cac
ON cac.cac_qulgcgycd = concat(univ.cac, CASE univ.qulgcgycd WHEN 'H' THEN 'H' WHEN 'I' THEN 'I' ELSE '' END)
and cac.Measure='USM_Claim'
GROUP BY 1, 2, 3, 10, 11, 12
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwSigning_Transaction_Detail'
  viewTables = 'standardised_subscribe.dbo_UsmMain, standardised_subscribe.dbo_UsmDefd,standardised_subscribe.dbo_usmnarr'

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
        df_univ_query = univ_query.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                          missing_string = "'" + str(missing_string) + "'", 
                                          missing_startdate_string = "'" + str(missing_startdate_string) + "'", 
                                          missing_mdm = "'" + str(missing_mdm) + "'")
    
        df_Univ = spark.sql(df_univ_query)
        df_Univ.createOrReplaceTempView('univ')

        #Replace value of batch effective date in SQL Query
        sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                   missing_string = "'" + str(missing_string) + "'")
    
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