# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_Policy_Activity_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Policy_Activity table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Akhilesh</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2023/03/23</td></tr>
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
# MAGIC     <td>10/01/2023</td>
# MAGIC     <td>Manoj Kumar</td>
# MAGIC     <td>Updated Logic as per solution of Bug: 45004</td>
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

# DBTITLE 1,UDFs for validating date
#for %m/%d/yy
def validateDate_8_1(date):
    try:
        datetime.strptime(date, '%d/%m/%y')
        return True
    except ValueError:
        isValidDate = False
        return isValidDate
    
sqlContext.udf.register('isdt_1', validateDate_8_1)


#for %m/%d/yyyy
def validateDate_8_2(date):
    try:
        datetime.strptime(date, '%d/%m/%Y')
        return True
    except ValueError:
        isValidDate = False
        return isValidDate

sqlContext.udf.register('isdt_2', validateDate_8_2)


#for dates present in this format--> 1 Dec 2020
def validateDate_11(date):
    try:
        datetime.strptime(date, '%d %b %Y')
        return True
    except ValueError:
        isValidDate = False
        return isValidDate
    
sqlContext.udf.register('isdt_3', validateDate_11)

#for dates present in this format--> 1 December 2020
def validateDate_12(date):
    try:
        datetime.strptime(date, '%d %B %Y')
        return True
    except ValueError:
        isValidDate = False
        return isValidDate
    
sqlContext.udf.register('isdt_4', validateDate_12)

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

# DBTITLE 1,Prepare SQL Query
#Universe 1
sqlUniv1= """
SELECT Upper(Concat(Trim(ip.polid), '_', ip.unitpsu))                   AS Policy_Header_Reference,
       Upper(Concat(Trim(ip.polid), '_', ip.unitpsu))                   AS Policy_Section_Reference,
       'Policy Status'                                                  AS Activity_Type,
       {batch_effective_datetime}                                       AS Activity_Date,
       ip.st                                                            AS Activity_Status,
       {missing_string}                                                 AS Activity_Outcome,
       Greatest(pm.lakelastupdatedate, ip.lakelastupdatedate)           AS LakeLastUpdateDate,
       Greatest(pm.lakelastupdatetimestamp, ip.lakelastupdatetimestamp) AS LakeLastUpdateTimestamp,
       CASE 
             WHEN coalesce(pm.lakeDeletedTimestamp, ip.lakeDeletedTimestamp) IS NOT NULL THEN {batch_effective_datetime} 
             ELSE NULL 
       END                                                              AS lakeDeletedTimestamp

FROM standardised_subscribe.dbo_polmain pm

INNER JOIN standardised_subscribe.dbo_inpol ip
ON  pm.polid = ip.polid
AND pm.unitpsu = ip.unitpsu
AND {batch_effective_datetime} >= pm.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(pm.LakeValidToTimestamp, current_timestamp())
AND {batch_effective_datetime} >= ip.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(ip.LakeValidToTimestamp, current_timestamp())

LEFT JOIN conformed_subscribe.Policy_Activity cnf_pa
ON  Concat(pm.polid, '_', pm.unitpsu) = cnf_pa.Policy_Header_Reference
AND Concat(pm.polid, '_', pm.unitpsu) = cnf_pa.Policy_Section_Reference
AND ip.st = cnf_pa.Policy_Activity_Status
AND cnf_pa.Policy_Activity_Type = 'Policy Status'
AND {batch_effective_datetime} >= cnf_pa.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(cnf_pa.LakeValidToTimestamp, current_timestamp())
AND cnf_pa.lakeDeletedTimestamp IS NULL

WHERE cnf_pa.HashedBusinessKey IS NULL
                    
UNION ALL

SELECT Upper(Concat(Trim(pm.polid), '_', pm.unitpsu))                   AS Policy_Header_Reference,
       Upper(Concat(Trim(pm.polid), '_', pm.unitpsu))                   AS Policy_Section_Reference,
       'Entry Status'                                                   AS Activity_Type,
       {batch_effective_datetime}                                       AS Activity_Date,
       pm.entst                                                         AS Activity_Status,
       {missing_string}                                                 AS Activity_Outcome,
       Greatest(pm.lakelastupdatedate, ip.lakelastupdatedate)           AS LakeLastUpdateDate,
       Greatest(pm.lakelastupdatetimestamp, ip.lakelastupdatetimestamp) AS LakeLastUpdateTimestamp,
       CASE 
             WHEN coalesce(pm.lakeDeletedTimestamp, ip.lakeDeletedTimestamp) IS NOT NULL THEN {batch_effective_datetime} 
             ELSE NULL 
       END                                                              AS lakeDeletedTimestamp
       
FROM standardised_subscribe.dbo_polmain pm

INNER JOIN standardised_subscribe.dbo_inpol ip
ON  pm.polid = ip.polid
AND pm.unitpsu = ip.unitpsu
AND {batch_effective_datetime} >= pm.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(pm.LakeValidToTimestamp, current_timestamp())
AND {batch_effective_datetime} >= ip.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(ip.LakeValidToTimestamp, current_timestamp())

LEFT JOIN conformed_subscribe.Policy_Activity cnf_pa
ON  Concat(pm.polid, '_', pm.unitpsu) = cnf_pa.Policy_Header_Reference
AND Concat(pm.polid, '_', pm.unitpsu) = cnf_pa.Policy_Section_Reference
AND pm.entst = cnf_pa.Policy_Activity_Status
AND cnf_pa.Policy_Activity_Type = 'Entry Status'
AND {batch_effective_datetime} >= cnf_pa.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(cnf_pa.LakeValidToTimestamp, current_timestamp())
AND cnf_pa.lakeDeletedTimestamp IS NULL

WHERE cnf_pa.HashedBusinessKey IS NULL
"""

#Universe 2
tmpUniv2 = """
SELECT Upper(Concat(Trim(a.polid), '_', a.unitpsu))                     AS Policy_Header_Reference,
       Upper(Concat(Trim(a.polid), '_', a.unitpsu))                     AS Policy_Section_Reference,
       CASE
         WHEN Upper(a.sectty) IN ( 'CONTRACT CERTAINTY COMPLETED DATE' ) THEN 'Contract Certainty'
         WHEN Upper(a.sectty) IN ( 'DATE PEER REVIEW COMPLETED' ) THEN 'Peer Review'
       END                                                              AS Activity_Type,
       CASE
         --WHEN Length(Trim(a.sectdsc)) <= 5 THEN {missing_startdate} 
         WHEN (
                 ( 
                      Isdt_1(Trim(a.sectdsc)) = true
                   OR Isdt_2(Trim(a.sectdsc)) = true
                   OR Isdt_3(Trim(a.sectdsc)) = true
                   OR Isdt_4(Trim(a.sectdsc)) = true 
                  )
                AND Length(a.sectdsc) > 5 
              ) 
         THEN COALESCE(To_date(a.sectdsc, 'd MMM y'), To_date(a.sectdsc, 'd/M/y'), a.sectdsc)
         ELSE {missing_startdate}
       END                                                              AS Activity_Date,
       'Completed'                                                      AS Activity_Status,
       {missing_string}                                                 AS Activity_Outcome,
       lakelastupdatedate,
       lakelastupdatetimestamp,
       lakeDeletedTimestamp
FROM   (
        SELECT Upper(polid)                                             AS PolId,
               unitpsu,
               sectty,
               sectdsc,
               sectnarrseqno,
               lakelastupdatedate,
               lakelastupdatetimestamp,
               lakeDeletedTimestamp,
               Row_number() OVER(partition BY Upper(polid), unitpsu, sectty 
               ORDER BY coalesce(lakeDeletedTimestamp, '9999-12-31T01:01:01.000') desc, sectnarrseqno DESC) AS rnk
        FROM   standardised_subscribe_dbo_polsectnarr_tmpvw
        WHERE  ({queryWhereCondition_polsectnarr})
               AND {batch_effective_datetime} >= LakeValidFromTimestamp 
               and {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
        ) a
WHERE  a.rnk = 1 
"""

#Restricting inwards policy from universe 2
sqlUniv2="""
SELECT policy_header_reference,
       policy_section_reference,
       COALESCE(NULLIF(activity_type, ''), {missing_string})            AS Activity_Type,   
       activity_date,
       COALESCE(NULLIF(activity_status, ''), {missing_string})          AS Activity_Status,
       activity_outcome,
       Greatest(univ_2.lakelastupdatedate, main.lakelastupdatedate,
       inpol.lakelastupdatedate)                                        AS LakeLastUpdateDate,
       Greatest(univ_2.lakelastupdatetimestamp, main.lakelastupdatetimestamp,
       inpol.lakelastupdatetimestamp)                                   AS LakeLastUpdateTimestamp,
       CASE 
            WHEN coalesce(univ_2.lakeDeletedTimestamp, main.lakeDeletedTimestamp, inpol.lakeDeletedTimestamp) IS NOT NULL 
            THEN {batch_effective_datetime} ELSE NULL 
       END                                                              AS lakeDeletedTimestamp
FROM   univ_2

INNER JOIN standardised_subscribe.dbo_polmain main
--to bring in policies which only exist in polmain table
ON Concat(main.polid, '_', main.unitpsu) = univ_2.policy_header_reference
AND {batch_effective_datetime} >= main.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(main.LakeValidToTimestamp, current_timestamp())

INNER JOIN standardised_subscribe.dbo_inpol inpol
ON Concat(inpol.polid, '_', inpol.unitpsu) = univ_2.policy_header_reference
AND {batch_effective_datetime} >= inpol.LakeValidFromTimestamp 
AND {batch_effective_datetime} < coalesce(inpol.LakeValidToTimestamp, current_timestamp())
"""

# COMMAND ----------

#Final query
sqlQuery = """
SELECT 
    policy_section_reference,
    policy_header_reference,
    COALESCE(NULLIF(activity_type, ''), {missing_string})               AS Policy_Activity_Type,
    activity_date                                                       AS Policy_Activity_Date,
    COALESCE(NULLIF(activity_status, ''), {missing_string})             AS Policy_Activity_Status,
    activity_outcome                                                    AS Policy_Activity_Outcome,
    univ.lakelastupdatedate                                             AS LakeLastUpdateDate,
    univ.lakelastupdatetimestamp                                        AS LakeLastUpdateTimestamp,
    CASE 
        WHEN univ.lakeDeletedTimestamp IS NOT NULL THEN {batch_effective_datetime} 
        ELSE NULL 
    END                                                                 AS lakeDeletedTimestamp
FROM univ 
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwPolicy_Activity'
  viewTables = 'standardised_Subscribe.dbo_PolMain,standardised_Subscribe.dbo_PolSectNarr,standardised_Subscribe.dbo_inpol'

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
    std_object_polsectnarr = 'standardised_subscribe.dbo_polsectnarr'
    current_bed = batchEffectiveDatetime
    queryWhereCondition_polsectnarr = """
    Upper(sectty) IN ('CONTRACT CERTAINTY COMPLETED DATE', 'DATE PEER REVIEW COMPLETED') AND Length(sectdsc) > 5
    """
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_polsectnarr = createStandardisedMultipleFilterView(std_object_polsectnarr,
                                                                  prv_bed,
                                                                  current_bed,
                                                                  whereExpression,
                                                                  queryWhereCondition_polsectnarr,
                                                                  cursor,
                                                                  batchTaskId,
                                                                  adfPipelineName,
                                                                  clusterId,
                                                                  notebookName,
                                                                  errorLogFileLocation)
        union_df_polsectnarr=spark.read.table(std_object_polsectnarr).unionByName(std_df_polsectnarr)
    else :
        union_df_polsectnarr=spark.read.table(std_object_polsectnarr)
        
    union_df_polsectnarr.createOrReplaceTempView('standardised_subscribe_dbo_polsectnarr_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_polsectnarr_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create dataframe from standardised data
try:
    if dependentTableIsUpdated == True:
    
        #Replace value of batch effective date in SQL Query
        sqlUniv1 = sqlUniv1.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                   missing_string = "'" +str(missing_string) + "'", 
                                   missing_startdate = "'" +str(missing_startdate) + "'")
    
        tmpUniv2 = tmpUniv2.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'",
                                   missing_startdate = "'" +str(missing_startdate) + "'",
                                   missing_string = "'" +str(missing_string) + "'",
                                   queryWhereCondition_polsectnarr = str(queryWhereCondition_polsectnarr))
    
        sqlUniv2 = sqlUniv2.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'",
                                   missing_startdate = "'" +str(missing_startdate) + "'",
                                   missing_string = "'" +str(missing_string) + "'")
    
        sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                   missing_string = "'" +str(missing_string) + "'", 
                                   missing_mdm = "'" +str(missing_mdm) + "'")

        #set timeparsepolicy
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        
        #Execute queries
        df1 = spark.sql(sqlUniv1)
        
        tmpdf = spark.sql(tmpUniv2)
        tmpdf.createOrReplaceTempView('univ_2')
        
        df2 = spark.sql(sqlUniv2)
        univ = df1.unionAll(df2)
        univ.createOrReplaceTempView('univ')
    
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