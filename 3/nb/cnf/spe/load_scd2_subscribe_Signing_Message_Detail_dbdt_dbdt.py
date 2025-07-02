# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_Signing_Message_Details_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Signing_Message_Details table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Kaveti</td></tr>
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

# DBTITLE 1,Prepare SQL Query
sqlQuery= """
SELECT
       signg_msg.signing_message_detail_reference                                                   AS Signing_Message_Detail_Reference,
       signg_msg.fil_2_code                                                                         AS FIL_2_Code,
       signg_msg.fil_4_code                                                                         AS FIL_4_Code,
       signg_msg.lloyds_risk_code                                                                   AS Lloyds_Risk_Code,
       signg_msg.signed_line_percentage                                                             AS Signed_Line_Percentage,
       signg_msg.signing_date                                                                       AS Signing_Date,
       signg_msg.signing_number                                                                     AS Signing_Number,
       signg_msg.signing_version                                                                    AS Signing_Version,
       signg_msg.ffl_code                                                                           AS FFL_Code,
       signg_msg.dti_code                                                                           AS DTI_Code,
       signg_msg.accounting_type                                                                    AS Accounting_Type,
       signg_msg.adjustable_indicator                                                               AS Adjustable_Indicator,
       signg_msg.attachment_indicator                                                               AS Attachment_Indicator,
       signg_msg.audit_aviation                                                                     AS Audit_Aviation,
       signg_msg.audit_marine                                                                       AS Audit_Marine,
       signg_msg.audit_non_marine                                                                   AS Audit_Non_Marine,
       signg_msg.broker_1_reference                                                                 AS Broker_1_Reference,
       signg_msg.broker_2_reference                                                                 AS Broker_2_Reference,
       signg_msg.bulk_settlement_indicator                                                          AS Bulk_Settlement_Indicator,
       signg_msg.bureau_treaty_number                                                               AS Bureau_Treaty_Number,
       signg_msg.claim_paid_abroad_indicator                                                        AS Claim_Paid_Abroad_Indicator,
       signg_msg.Country_Of_Origin_Code                                                             AS Country_Of_Origin_Code,
       signg_msg.Country_Of_Risk_Code                                                               AS Country_Of_Risk_Code,
       signg_msg.end_of_month_indicator                                                             AS End_Of_Month_Indicator,
       signg_msg.entry_type                                                                         AS Entry_Type,
       signg_msg.instalment_type                                                                    AS Instalment_Type,
       signg_msg.lloyds_central_accounting_category_code                                            AS Lloyds_Central_Accounting_Category_Code,
       signg_msg.lloyds_line_percentage                                                             AS Lloyds_Line_Percentage,
       signg_msg.lloyds_line_type                                                                   AS Lloyds_Line_Type,
       signg_msg.lloyds_proc_date                                                                   AS Lloyds_Proc_Date,
       signg_msg.market_type                                                                        AS Market_Type,
       signg_msg.outwards_ri_reference                                                              AS Outwards_RI_Reference,
       signg_msg.original_claim_reference                                                           AS Original_Claim_Reference,
       signg_msg.original_signing_reference                                                         AS Original_Signing_Reference,
       signg_msg.planned_settlement_date                                                            AS Planned_Settlement_Date,
       signg_msg.policy_from_date                                                                   AS Policy_From_Date,
       signg_msg.policy_to_date                                                                     AS Policy_To_Date,
       signg_msg.proc_period                                                                        AS Proc_Period,
       signg_msg.proc_type                                                                          AS Proc_Type,
       signg_msg.qualification_category                                                             AS Qualification_Category,
       signg_msg.reserve_balance_brought_forward                                                    AS Reserve_Balance_Brought_Forward,
       signg_msg.reserve_balance_carry_forward                                                      AS Reserve_Balance_Carry_Forward,
       signg_msg.deferred_balance_brought_forward                                                   AS Deferred_Balance_Brought_Forward,
       signg_msg.deferred_balance_carry_forward                                                     AS Deferred_Balance_Carry_Forward,
       signg_msg.signing_reference                                                                  AS Signing_Reference,
       signg_msg.syndicate_ri_broker_info                                                           AS Syndicate_RI_Broker_Info,
       signg_msg.total_discount                                                                     AS Total_Discount,
       signg_msg.transaction_period_from_date                                                       AS Transaction_Period_From_Date,
       signg_msg.transaction_period_to_date                                                         AS Transaction_Period_To_Date,
       signg_msg.treaty_serial_number                                                               AS Treaty_Serial_Number,
       signg_msg.unique_market_reference                                                            AS Unique_Market_Reference,
       signg_msg.unique_claim_reference                                                             AS Unique_Claim_Reference,
       signg_msg.unique_transaction_reference                                                       AS Unique_Transaction_Reference,
       signg_msg.source_trust_fund_code                                                             AS Source_Trust_Fund_Code,
       CASE
         WHEN gqd_trns_typ.lakedeletedtimestamp IS NOT NULL THEN {missing_mdm}
         ELSE COALESCE(NULLIF(Trim(gqd_trns_typ.gqd_transaction_type_code), ''), {missing_mdm})
       END                                                                                          AS GQD_Transaction_Type_Code,
       CASE
         WHEN gqd_trns_typ.lakedeletedtimestamp IS NOT NULL THEN {missing_mdm}
         ELSE COALESCE(NULLIF(Trim(gqd_trns_typ.gqd_transaction_type_description), ''),{missing_mdm})
       END                                                                                          AS GQD_Transaction_Type_Description,
       signg_msg.Syndicate_Line_Number                                                              AS Syndicate_Line_Number,
       Greatest(signg_msg.lakelastupdatedate, gqd_trns_typ.lakelastupdatedate)                      AS lakelastupdatedate,
       Greatest(signg_msg.lakelastupdatetimestamp, gqd_trns_typ.lakelastupdatetimestamp)            AS lakelastupdatetimestamp,
       CASE
         WHEN signg_msg.lakedeletedtimestamp IS NULL THEN NULL ELSE {batch_effective_datetime}
       END                                                                                          AS lakeDeletedTimestamp
FROM   
(
SELECT CAST(usm.sgndln AS DECIMAL(19,8))                                                            AS Signed_Line_Percentage,
       COALESCE(usm.lpsodt, {missing_startdate_string})                                             AS Signing_Date,
       usm.lpsono                                                                                   AS Signing_Number,
       usm.trnverno                                                                                 AS Signing_Version,
       Concat_ws('_',usm.trnid, usm.trncgy)                                                         AS Signing_Message_Detail_Reference,
       COALESCE(usm.lloaccgty, {missing_string})                                                    AS Accounting_Type,
       CASE
         WHEN usm.adjindr IN ( 'Y', '1' ) THEN 'Y' ELSE 'N'
       END                                                                                          AS Adjustable_Indicator,
       CASE
         WHEN usm.atchindr IN ( 'Y', '1' ) THEN 'Y' ELSE 'N'
       END                                                                                          AS Attachment_Indicator,
       COALESCE(NULLIF(usm.aua, ''), {missing_string})                                              AS Audit_Aviation,
       COALESCE(NULLIF(usm.aum, ''), {missing_string})                                              AS Audit_Marine,
       COALESCE(NULLIF(usm.aun, ''), {missing_string})                                              AS Audit_Non_Marine,
       CASE
         WHEN usm.bulksettindr IN ( 'Y', '1', 'true' ) THEN 'Y' ELSE 'N'
       END                                                                                          AS Bulk_Settlement_Indicator,
       COALESCE(NULLIF(usm.burttyno, ''), {missing_string})                                         AS Bureau_Treaty_Number,
       CASE
         WHEN usm.cpaindr IN ( 'Y', '1', 'true' ) THEN 'Y' ELSE 'N'
       END                                                                                          AS Claim_Paid_Abroad_Indicator,
       COALESCE(NULLIF(usm.ctryorig, ''), {missing_string})                                         AS Country_Of_Origin_Code,
       COALESCE(NULLIF(usm.ctryrsk, ''), {missing_string})                                          AS Country_Of_Risk_Code,
       CAST(usm.defdbalbf AS DECIMAL(28,2))                                                         AS Deferred_Balance_Brought_Forward,
       CAST(usm.defdbalcf AS DECIMAL(28,2))                                                         AS Deferred_Balance_Carry_Forward,
       CASE
         WHEN ctrl.lakedeletedtimestamp IS NOT NULL THEN 'N'
         WHEN ctrl.endofmn IN ( 'Y', '1', 'true' )  THEN 'Y'
         ELSE 'N'
       END                                                                                          AS End_Of_Month_Indicator,
       COALESCE(usm.entty, {missing_string})                                                        AS Entry_Type,
       COALESCE(NULLIF(usm.instty, ''), {missing_string})                                           AS Instalment_Type,
       COALESCE(NULLIF(usm.cac, ''), {missing_string})                                              AS Lloyds_Central_Accounting_Category_Code,
       CAST(COALESCE(usm.llottlln, 0.00) AS DECIMAL(19,8))                                          AS Lloyds_Line_Percentage,
       COALESCE(usm.llolnty, {missing_string})                                                      AS Lloyds_Line_Type,
       usm.lloprodt                                                                                 AS Lloyds_Proc_Date,
       COALESCE(usm.mktty, {missing_string})                                                        AS Market_Type,
       COALESCE(NULLIF(Upper(usm.owdriref), 'NULL'), {missing_string})                              AS Outwards_RI_Reference,
       COALESCE(usm.ocr, {missing_string})                                                          AS Original_Claim_Reference,
       Concat_ws('_', usm.origlpsodt, usm.origlpsono)                                               AS Original_Signing_Reference,
       COALESCE(usm.plndsettduedt, {missing_startdate_string})                                      AS Planned_Settlement_Date,
       COALESCE(usm.polfrdt, {missing_startdate_string})                                            AS Policy_From_Date,
       COALESCE(usm.poltodt, {missing_enddate_string})                                              AS Policy_To_Date,
       usm.accprd                                                                                   AS Proc_Period,
       COALESCE(usm.lloprocd, {missing_string})                                                     AS Proc_Type,
       COALESCE(usm.qulgcgycd, {missing_string})                                                    AS Qualification_Category,
       CAST(usm.resbalbf AS DECIMAL(28,2))                                                          AS Reserve_Balance_Brought_Forward,
       CAST(usm.resbalcf AS DECIMAL(28,2))                                                          AS Reserve_Balance_Carry_Forward,
       Concat_ws('_', usm.lpsodt, usm.lpsono, usm.trnverno)                                         AS Signing_Reference,
       COALESCE(usm.synribkrinfo, {missing_string})                                                 AS Syndicate_RI_Broker_Info,
       CAST(usm.ttldisc AS DECIMAL(28,2))                                                           AS Total_Discount,
       COALESCE(usm.prdtrnfrdt, {missing_startdate_string})                                         AS Transaction_Period_From_Date,
       COALESCE(usm.llottyserno, {missing_string})                                                  AS Treaty_Serial_Number,
       COALESCE(usm.umr, {missing_string})                                                          AS Unique_Market_Reference,
       COALESCE(usm.ucr, {missing_string})                                                          AS Unique_Claim_Reference,
       COALESCE(usm.trnref, {missing_string})                                                       AS Unique_Transaction_Reference,
       COALESCE(usm.tfc, {missing_string})                                                          AS Source_Trust_Fund_Code,
       COALESCE(NULLIF(Trim(usm.filcd4), ''),{missing_string})                                      AS FIL_4_Code,
       COALESCE(NULLIF(Trim(usm.filcd2), ''),{missing_string})                                      AS FIL_2_Code,
       COALESCE(NULLIF(Trim(usm.rskcd), ''),{missing_string})                                       AS Lloyds_Risk_Code,
       COALESCE(NULLIF(usm.filcd4b, ''), {missing_string})                                          AS FFL_Code,
       COALESCE(NULLIF(usm.dti, ''), {missing_string})                                              AS DTI_Code,
       COALESCE(usm.prdtrntodt, {missing_enddate_string})                                           AS Transaction_Period_To_Date,
       COALESCE(usm.bkrref, {missing_string})                                                       AS Broker_1_Reference,
       COALESCE(usm.bkrref2, {missing_string})                                                      AS Broker_2_Reference,
       COALESCE(Concat(usm.polid, '_', usm.unitpsu), {missing_string})                              AS Policy_Section_Reference,
       Concat_ws('_',usm.trnid, usm.trncgy)                                                         AS Signing_Transaction_Reference,
       COALESCE(Concat(usm.polid, '_', usm.unitpsu), {missing_string})                              AS Policy_Header_Reference,
       COALESCE(usm.SLN, '-1')                                                                      AS Syndicate_Line_Number,
       CASE
         WHEN bs_en.lakedeletedtimestamp IS NOT NULL THEN {missing_mdm}
         WHEN bs_en.lloyds_brussels_indicator = 'Y' THEN 'PT'
         WHEN dti IN ( '1', '2', '3', '4', '5', '6', '7', '8' ) THEN
           CASE
             WHEN RIGHT(filcd4, 1) = '1' THEN 'DI'
             WHEN RIGHT(filcd4, 1) = '2' THEN 'FR'
             WHEN RIGHT(filcd4, 1) = '3' THEN 'NT'
             WHEN RIGHT(filcd4, 1) = '4' THEN 'PT'
             ELSE NULL
           END
         WHEN dti NOT IN ( '1', '2', '3', '4', '5', '6', '7', '8' ) THEN
           CASE
             WHEN RIGHT(filcd4, 1) = '1' THEN 'DI'
             WHEN RIGHT(filcd4, 1) = '2' THEN 'IF'
             WHEN RIGHT(filcd4, 1) = '3' THEN 'IN'
             WHEN RIGHT(filcd4, 1) = '4' THEN 'PT'
             ELSE NULL
           END
       END                                                                                                    AS GQD_Transaction_Type,
       Greatest(usm.lakelastupdatedate, ctrl.lakelastupdatedate, bs_en.lakelastupdatedate)                    AS lakelastupdatedate,
       Greatest(usm.lakelastupdatetimestamp, ctrl.lakelastupdatetimestamp, bs_en.lakelastupdatetimestamp)     AS lakelastupdatetimestamp,
       CASE
         WHEN usm.lakedeletedtimestamp IS NULL THEN NULL ELSE {batch_effective_datetime}
       END                                                                                                    AS lakeDeletedTimestamp
FROM   standardised_subscribe.dbo_usmmain usm

--to fetch the end of month 
LEFT JOIN standardised_subscribe.dbo_usmctrl ctrl
ON Upper(usm.accprd) = Upper(ctrl.accprd)
AND Upper(usm.mrn) = Upper(ctrl.mrn)
AND {batch_effective_datetime} >= ctrl.lakevalidfromtimestamp
AND {batch_effective_datetime} < COALESCE(ctrl.lakevalidtotimestamp,CURRENT_TIMESTAMP())

LEFT JOIN standardised_mdm.mdm_business_entity bs_en
ON usm.unitid = bs_en.business_entity_code
AND {batch_effective_datetime} >= bs_en.lakevalidfromtimestamp
AND {batch_effective_datetime} < COALESCE(bs_en.lakevalidtotimestamp, CURRENT_TIMESTAMP())
       
WHERE {batch_effective_datetime} >= usm.lakevalidfromtimestamp
AND {batch_effective_datetime} < COALESCE(usm.lakevalidtotimestamp, CURRENT_TIMESTAMP())
) signg_msg
        
LEFT JOIN standardised_mdm.mdm_gqd_transaction_type gqd_trns_typ
ON signg_msg.gqd_transaction_type = gqd_trns_typ.gqd_transaction_type_code
AND {batch_effective_datetime} >= gqd_trns_typ.lakevalidfromtimestamp
AND {batch_effective_datetime} < COALESCE(gqd_trns_typ.lakevalidtotimestamp,CURRENT_TIMESTAMP()) 
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  #standardised_mdm.mdm_lloyds_risk_code,standardised_mdm.mdm_fil_4,standardised_mdm.mdm_fil_2, 
  viewName = 'dummy_Dummy.dummyVwSigning_Message_Detail'
  viewTables = 'standardised_Subscribe.dbo_usmmain, standardised_subscribe.dbo_usmctrl, standardised_mdm.mdm_business_entity, standardised_mdm.mdm_gqd_transaction_type'

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
    sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                               missing_mdm = "'" + str(missing_mdm) + "'" , 
                               missing_string = "'" + str(missing_string) + "'" , 
                               missing_startdate_string = "'" + str(missing_startdate_string) + "'" , 
                               missing_enddate_string = "'" + str(missing_enddate_string) + "'")
    
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