# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_Claim_Transaction_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Claim_Transaction table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Nikunj Srivastava</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/07/13</td></tr>
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
# MAGIC     <td>06/12/2022</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Removed the quotes around acc_perd_match_res and acc_perd_unmatch_res variables as these are intigers</td>
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

#----FETCHING ACCOUNTING PERIOD FROM SUBSCRIBE----------#

# src_acc_perd="select accprd from standardised_subscribe.dbo_msgaccprdst where trim(lower(st)) = 'o' AND lower(trim(trncgy))='scm' AND {batch_effective_datetime} >= LakeValidFromTimestamp AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())"

#----FETCHING ACCOUNTING PERIOD FROM MDM----------#

acc_perd_match="select array_join(array_sort(collect_set(Accounting_Period)),',') from standardised_mdm.mdm_accounting_period_mapping where trim(lower(source_name)) = 'subscribe' AND accounting_period_close_date_time is null AND {batch_effective_datetime} >= LakeValidFromTimestamp AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())"

acc_perd_unmatch="select Accounting_Period from standardised_mdm.mdm_accounting_period_mapping where trim(lower(source_name)) = 'subscribe' AND accounting_period_close_date_time is null AND {batch_effective_datetime} >= LakeValidFromTimestamp AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp()) order by Accounting_Period ASC limit 1"

# COMMAND ----------

# DBTITLE 1,Prepare SQL Query
sqlQuery="""

WITH USMMain as --to bring in Singning_Transaction_Reference
(
    select 
    main.trnid                                                                  AS scm_trnid, 
    main.trncgy                                                                 AS scm_trncgy,
    max(usm.trnid)                                                              AS usm_trnid, 
    max(usm.unitpsu)                                                            AS usm_unitpsu, 
    max(usm.trncgy)                                                             AS usm_trncgy,
    max(greatest(main.lakelastupdatedate, usm.lakelastupdatedate))              AS lakelastupdatedate,
    max(greatest(main.lakelastupdatetimestamp, usm.lakelastupdatetimestamp))    AS lakelastupdatetimestamp,
    CASE 
        WHEN NULLIF(max(coalesce(main.lakeDeletedTimestamp,coalesce(usm.lakeDeletedTimestamp, '9999-12-31T01:01:01.001+0000')))
                        ,'9999-12-31T01:01:01.001+0000') Is Not Null
        THEN {batch_effective_datetime} ELSE NULL 
    END                                                                         AS lakeDeletedTimestamp
    from standardised_subscribe.dbo_scmmain main
    inner join 
    (
        select trnid, trncgy, unitpsu,LPSONo, LPSODt, UnitId, SLN, PolId, NetAmt, 
        lakelastupdatedate, lakelastupdatetimestamp,lakeDeletedTimestamp
        from  standardised_subscribe.dbo_usmmain 
        where {batch_effective_datetime} >= LakeValidFromTimestamp 
        AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
    ) usm 
    on usm.LPSONo = main.LpsoNo
    and usm.LPSODt = main.LpsoDt
    and usm.UnitId = main.UnitId
    and usm.SLN = main.SLN
    and usm.PolId = main.PolId
    where abs(main.SynPTDAdjd - usm.NetAmt) <= 1
    and {batch_effective_datetime} >= main.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(main.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    group by 1,2
),
    
Business_Process_Duplicate_Claim_Indicator as --query shared by Canopius
(
    select 
    s.trnid, 
    s.trncgy,
    greatest(s.lakelastupdatedate, p.lakelastupdatedate, pac.lakelastupdatedate, pac2.lakelastupdatedate, 
    ip.lakelastupdatedate, i.lakelastupdatedate)                                AS lakelastupdatedate,
    greatest(s.lakelastupdatetimestamp, p.lakelastupdatetimestamp, pac.lakelastupdatetimestamp, pac2.lakelastupdatetimestamp, 
    ip.lakelastupdatetimestamp, i.lakelastupdatetimestamp)                      AS lakelastupdatetimestamp,
    CASE 
        WHEN coalesce(s.lakeDeletedTimestamp, p.lakeDeletedTimestamp, pac.lakeDeletedTimestamp, pac2.lakeDeletedTimestamp, 
        ip.lakeDeletedTimestamp, i.lakeDeletedTimestamp) Is Not Null
        THEN {batch_effective_datetime} ELSE NULL 
    END                                                                         AS lakeDeletedTimestamp
    from standardised_subscribe.dbo_SCMMain s
    
    INNER JOIN standardised_subscribe.dbo_PolMain p
    on s.polid=p.polid
    and s.unitpsu=p.unitpsu
    AND {batch_effective_datetime} >= s.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(s.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    AND {batch_effective_datetime} >= p.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(p.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    
    INNER JOIN standardised_subscribe.dbo_PolAnlyCd pac
    ON pac.PolId = p.polid
    AND pac.UnitPsu = p.unitpsu
    AND pac.Ty = 'MOP'
    AND {batch_effective_datetime} >= pac.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(pac.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    AND {batch_effective_datetime} >= p.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(p.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    
    INNER JOIN 
    (
        select polid, unitpsu, ty, dsc, cd, lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp,
        CASE WHEN Dsc LIKE '%AUSTRALIA%' THEN 2019 WHEN Cd = 'CRE' THEN 2020 end as year 
        from standardised_subscribe.dbo_PolAnlyCd 
        WHERE Ty = 'SUBCLASS'
        AND {batch_effective_datetime} >= LakeValidFromTimestamp 
        AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
    ) pac2
    ON pac2.PolId = p.polid
    AND pac2.UnitPsu = p.unitpsu
    AND {batch_effective_datetime} >= p.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(p.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    
    INNER JOIN standardised_subscribe.dbo_InPol ip
    ON ip.PolId = p.polid
    AND ip.UnitPsu = p.unitpsu
    AND {batch_effective_datetime} >= ip.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(ip.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    AND {batch_effective_datetime} >= p.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(p.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    
    INNER JOIN standardised_subscribe.dbo_Insd i
    ON p.insdid=i.insdid
    AND {batch_effective_datetime} >= i.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(i.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    AND {batch_effective_datetime} >= p.LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(p.LakeValidToTimestamp, CURRENT_TIMESTAMP())
    
    WHERE pac.cd='116' and ip.AcctgYr >= pac2.year
    --insdNM is a PII field, hence this filter will make the overall result null
    and i.insdNm IN ('CANOPIUS ASIA PTE LTD','CANOPIUS AUSTRALIA & PACIFIC') 
    AND (pac2.Dsc LIKE '%AUSTRALIA%' OR pac2.Dsc LIKE '%SINGAPORE%' OR pac2.Cd = 'CRE')
    group by 1, 2, 3, 4, 5
)


------CONFORMED QUERY STARTS HERE--------------
select 
concat_ws('_', upper(trim(mv.trnid)),upper(trim(mv.trncgy)),trim(mv.movtseqno)) AS Claim_Transaction_Reference,
Concat_ws('_',usm_trnid,usm_trncgy,COALESCE(defd.instno, '01'))                 AS signing_transaction_reference,
Concat_ws('_',Upper(Trim(mv.trnid)),Upper(Trim(mv.trncgy)),trim(mv.movtseqno))  AS Claim_Message_Detail_Reference,
COALESCE(mv.src_trans_typ, {missing_string})                                    AS Transaction_Type_Code,
COALESCE(mv.OCC, {missing_string})                                              AS Original_Currency,
COALESCE(mv.SCC, {missing_string})                                              AS Settlement_Currency,
COALESCE(mv.src_trans_typ, {missing_string})                                    AS Source_Transaction_Type_Code,
mv.DtTm                                                                         AS Source_Transaction_Date,
CASE 
    WHEN mv.AccPrd IN ({acc_perd_match_res}) THEN mv.AccPrd
    ELSE {acc_perd_unmatch_res}
END                                                                             AS accounting_period,
COALESCE(mv.AccPrd, '190001')                                                   AS source_accounting_period,
COALESCE(mv.AccPrd, '190001')                                                   AS Message_Accounting_Period,
CASE WHEN claim_ind.trnid is not null  THEN 'Y' ELSE 'N' END                    AS Business_Process_Duplicate_Claim_Indicator,
CASE WHEN lower(trim(mv.mrn)) like 'm%' THEN 'Y' ELSE 'N' END                   AS Manual_Transaction_Indicator,
CASE 
    WHEN mv.UnitId = 4445 THEN 'Y'
    WHEN mv.AccPrd >= 200901 THEN 'N'
    WHEN mv.UnitId = 962 THEN 'Y'
    WHEN mv.UnitId = 1607 THEN 'Y'
    WHEN mv.UnitId = 2607 THEN 'Y'
    WHEN mv.UnitId = 2962 THEN 'Y'
    WHEN mv.UnitId = 3786 THEN 'Y'
    WHEN mv.UnitId = 8015 THEN 'Y'
    ELSE 'N' 
END                                                                             AS Legacy_Business_Entity_Indicator,
CASE 
    WHEN mv.OCC = mv.SCC THEN 1 ELSE cast(1/NULLIF(main.PdRoe,0) AS DECIMAL(28,18))
END                                                                             AS Multiplication_Rate_Of_Exchange,
CASE 
    WHEN mv.OCC = mv.SCC THEN 1 ELSE cast(1/NULLIF(main.OsRoe,0) AS DECIMAL(28,18))
END                                                                             AS Outstanding_Multiplication_Rate_Of_Exchange,
CASE 
    WHEN mv.OCC = mv.SCC THEN 1 ELSE CAST(main.PdRoe AS DECIMAL(28,18))
END                                                                             AS Division_Rate_Of_Exchange,
CASE 
    WHEN mv.OCC = mv.SCC THEN 1 ELSE CAST(main.OsRoe AS DECIMAL(28,18))
END                                                                             AS Outstanding_Division_Rate_Of_Exchange,
------------------------------------------------Added as per v.1.10------------------------------------------------
COALESCE(mv.AccYr,'1900')                                                       AS Claim_Transaction_Year_Of_Account,
mv.TrnSeqNo                                                                     AS Claim_Movement_Number,
mv.MovtSeqNo                                                                    AS Claim_Movement_Sequence_Number,
COALESCE(mv.dttm,{missing_startdate_string})                                    AS Claim_Advice_Date,
{missing_string}                                                                AS Claim_Movement_Status,
COALESCE(main.CMCd, {missing_string})                                           AS Claim_Movement_Type,
COALESCE(main.CurrNarrA, {missing_string})                                      AS Claim_Advice_Narrative_1,
COALESCE(main.CurrNarrB, {missing_string})                                      AS Claim_Advice_Narrative_2,
greatest(main.lakelastupdatedate, mv.lakelastupdatedate, USMMain.lakelastupdatedate, 
    defd.lakelastupdatedate, claim_ind.lakelastupdatedate)                      AS lakelastupdatedate,
greatest(main.lakelastupdatetimestamp, mv.lakelastupdatetimestamp, USMMain.lakelastupdatetimestamp, 
    defd.lakelastupdatetimestamp, claim_ind.lakelastupdatetimestamp)            AS lakelastupdatetimestamp,
CASE 
    WHEN COALESCE(mv.lakeDeletedTimestamp, main.lakeDeletedTimestamp) Is Not Null THEN {batch_effective_datetime} ELSE NULL 
END                                                                             AS lakeDeletedTimestamp

--to bring in movement details
from 
(
   select mvt.*, 'SCM' as src_trans_typ from standardised_subscribe.dbo_scmmovt mvt
   where  {batch_effective_datetime} >= LakeValidFromTimestamp 
   AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP()) 
) mv 

--to bring in transaction details
INNER JOIN 
(
    select trnid, trncgy, 
    AccPrd, OsRoe, PdRoe, CMCd, CurrNarrA, CurrNarrB, 
    lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp        
	from standardised_subscribe.dbo_scmmain 
    where {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP()) 
) main 
on  main.trnid  = mv.trnid
and main.trncgy = mv.trncgy
    
--to bring in signing reference
LEFT JOIN
(
    select 
    case when lakedeletedtimestamp is not null then null else scm_trnid end     AS scm_trnid, 
    case when lakedeletedtimestamp is not null then null else scm_trncgy end    AS scm_trncgy,
    case when lakedeletedtimestamp is not null then null else usm_trnid end     AS usm_trnid, 
    case when lakedeletedtimestamp is not null then null else usm_trncgy end    AS usm_trncgy,
    lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp   
    from USMMain
) USMMain
on  main.trnid  = USMMain.scm_trnid
and main.trncgy = USMMain.scm_trncgy

--to bring in instalment number
LEFT JOIN
(
    select case when lakedeletedtimestamp is not null then null else trnid end  AS trnid, 
    case when lakedeletedtimestamp is not null then null else trncgy end        AS trncgy, 
    instno, lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp   
    from standardised_subscribe.dbo_usmdefd 
    where {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < COALESCE(LakeValidToTimestamp, CURRENT_TIMESTAMP())
)defd
on  USMMain.usm_trnid  = defd.trnid
and USMMain.usm_trncgy = defd.trncgy

--to bring in Business Process Duplicate Claim Indicator
LEFT JOIN
(
    select case when lakedeletedtimestamp is not null then null else trnid end  AS trnid,
    trncgy, lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp   
    from Business_Process_Duplicate_Claim_Indicator
) claim_ind
on  claim_ind.trnid  = mv.trnid
and claim_ind.trncgy = mv.trncgy
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwClaim_Transaction'
  viewTables = 'standardised_subscribe.dbo_scmmain, standardised_subscribe.dbo_usmmain, standardised_subscribe.dbo_PolMain, standardised_subscribe.dbo_PolAnlyCd, standardised_subscribe.dbo_InPol, standardised_subscribe.dbo_Insd, standardised_subscribe.dbo_scmmovt, standardised_subscribe.dbo_usmdefd'

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

try:
    if dependentTableIsUpdated == True:          
        
        acc_perd_match_query = acc_perd_match.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
        acc_perd_match_res_tmp = spark.sql(acc_perd_match_query)
        acc_perd_match_res = acc_perd_match_res_tmp.collect()[0][0]
        acc_perd_unmatch_query = acc_perd_unmatch.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'")
        acc_perd_unmatch_res_tmp = spark.sql(acc_perd_unmatch_query)
        acc_perd_unmatch_res = acc_perd_unmatch_res_tmp.collect()[0][0]
        if acc_perd_unmatch_res_tmp.count() == 0:
            acc_perd_unmatch='190001'
            
        #Replace value of batch effective date in SQL Query
        sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                   missing_string = "'" + str(missing_string) + "'", 
                                   missing_startdate_string = "'" + str(missing_startdate_string) + "'", 
                                   acc_perd_match_res =  str(acc_perd_match_res), 
                                   acc_perd_unmatch_res =  str(acc_perd_unmatch_res))
        
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