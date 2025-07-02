# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_Policy_Characteristic_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Policy_Characteristic table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Akhilesh</td></tr>
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
# MAGIC     <td>18-01-2023</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Added Filter standardised view logic to handle the updates from standardiset to go as deletes in conformed</td>
# MAGIC   </tr>  
# MAGIC    <tr>
# MAGIC     <td>06-02-2023</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Updated Filter Standardised view logic with New design</td>
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
  from pyspark.sql.functions import rank,desc,col,lag, when, lit, concat,row_number,collect_list, coalesce, concat_ws,approx_count_distinct,first,last,rank,max,lead,upper,trim,count
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

# DBTITLE 1,UDFs for validating date and numeric values
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


#for formats like 1 Dec 2020
def validateDate_11(date):
    try:
        datetime.strptime(date, '%d %b %Y')
        return True
    except ValueError:
        isValidDate = False
        return isValidDate
sqlContext.udf.register('isdt_3', validateDate_11)

#for formats like 1 December 2020
def validateDate_12(date):
    try:
        datetime.strptime(date, '%d %B %Y')
        return True
    except ValueError:
        isValidDate = False
        return isValidDate

sqlContext.udf.register('isdt_4', validateDate_12)

def is_numeric(s):
    try:
        float(s)
        return 1
    except ValueError:
        return 0

sqlContext.udf.register('udf_isnumeric', is_numeric)

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
sqlQuery= """
SELECT
Concat(b.polid,'_',b.unitpsu)                                                     AS Policy_Header_Reference,
COALESCE(NULLIF(b.sectty, ''), {missing_string})                                  AS Source_Policy_Characteristic_Name,
CASE
    WHEN (
         Isdt_1(Trim(sectdsc)) = false
         OR Isdt_2(Trim(sectdsc)) = false
         OR Isdt_3(Trim(sectdsc)) = false
         OR Isdt_4(Trim(sectdsc)) = false
         OR Length(b.sectdsc) < 8
         OR Length(b.sectdsc) > 12
         OR Udf_isnumeric(Ltrim(Rtrim(b.sectdsc))) <> 1
         OR Upper(b.sectty) NOT LIKE 'ULR %' 
         OR bkr.bkrCd IS NOT NULL 
         ) THEN b.sectdsc
END                                                                               AS Policy_Characteristic_Description,
CASE
    WHEN Length(Trim(b.sectdsc)) <= 5 THEN {missing_startdate}
    WHEN ( 
         ( Isdt_1(Trim(sectdsc)) = true
         OR Isdt_2(Trim(sectdsc)) = true
         OR Isdt_3(Trim(sectdsc)) = true
         OR Isdt_4(Trim(sectdsc)) = true )
         AND Length(b.sectdsc) BETWEEN 8 AND 12
         ) THEN COALESCE(To_date(b.sectdsc,'dd MMM y'), To_date(b.sectdsc,'d/M/yy'), b.sectdsc)
    ELSE {missing_startdate}
END                                                                               AS Policy_Characteristic_Date,
CASE
    WHEN Udf_isnumeric(Trim(b.sectdsc)) = 1 AND (Upper(b.sectty)) NOT LIKE 'ULR %' AND bkr.bkrCd IS NULL 
    THEN COALESCE(CAST(NULLIF(TRIM(b.sectdsc),'') as DECIMAL(28,2)), 0.00)
    ELSE 0.00
END                                                                               AS Policy_Characteristic_Amount,
CASE 
    WHEN char_ref.lakeDeletedTimestamp is not null then {missing_mdm} 
    ELSE COALESCE(NULLIF(char_ref.Policy_Characteristic_Data_Type, ''), {missing_mdm})  
END                                                                               AS Policy_Characteristic_Data_Type,
CASE
    WHEN char_ref.lakeDeletedTimestamp is not null THEN {missing_mdm} 
    ELSE COALESCE(NULLIF(char_ref.Policy_Characteristic_Name, ''), {missing_mdm})
END                                                                               AS Policy_Characteristic_Name,
greatest(b.lakelastupdatedate, main.lakelastupdatedate, ip.lakelastupdatedate, 
char_ref.lakelastupdatedate,bkr.lakelastupdatedate)                               AS lakelastupdatedate,
greatest(b.lakelastupdatetimestamp, main.lakelastupdatetimestamp, ip.lakelastupdatetimestamp, 
char_ref.lakelastupdatetimestamp,bkr.lakelastupdatetimestamp)                     AS lakelastupdatetimestamp,
CASE 
    WHEN coalesce(b.lakeDeletedTimestamp, main.lakeDeletedTimestamp, ip.lakeDeletedTimestamp) IS NOT NULL 
    THEN {batch_effective_datetime} ELSE NULL 
END                                                                               AS lakeDeletedTimestamp

--bringing in Policy ID, Section Type and Section Description of the required Section Type--
FROM       
(
    SELECT   upper(polid) AS polid,
             unitpsu,
             sectty,
             sectdsc,
             sectnarrseqno,
             lakevalidfromtimestamp,
             lakevalidtotimestamp,
             lakeisactive,
             lakelastupdatedate,
             lakelastupdatetimestamp,
             lakeDeletedTimestamp,
             row_number() OVER(partition BY upper(polid), unitpsu, sectty 
             ORDER BY coalesce(lakeDeletedTimestamp, '9999-12-31T01:01:01.000') desc, sectnarrseqno DESC) AS rnk
    FROM     standardised_subscribe_dbo_polsectnarr_tmpvw
    WHERE    upper(sectty) IN ({string_filter})
    AND {batch_effective_datetime} >= LakeValidFromTimestamp 
    AND {batch_effective_datetime} < coalesce(LakeValidToTimestamp, current_timestamp())
) b

-- to fetch only those policies FROM SectNarr that existis in Main table
INNER JOIN standardised_subscribe.dbo_polmain main 
ON  upper(b.polid) = upper(main.polid)
AND upper(b.unitpsu) = upper(main.unitpsu)
AND {batch_effective_datetime} >= main.LakeValidFromTimestamp 
and {batch_effective_datetime} < coalesce(main.LakeValidToTimestamp, current_timestamp())

--to fetch the policies which are inward
INNER JOIN standardised_subscribe.dbo_inpol ip
ON Upper(b.polid) = Upper(ip.polid)
AND Upper(b.unitpsu) = Upper(ip.unitpsu)
AND {batch_effective_datetime} >= ip.lakevalidfromtimestamp
AND {batch_effective_datetime} < COALESCE(ip.lakevalidtotimestamp, CURRENT_TIMESTAMP())

LEFT JOIN
(
    select BkrCd, max(lakelastupdatedate) lakelastupdatedate, max(lakelastupdatetimestamp) lakelastupdatetimestamp,
    NULLIF(max(coalesce(lakeDeletedTimestamp,'9999-12-31T01:01:01.001+0000')),'9999-12-31T01:01:01.001+0000') AS lakeDeletedTimestamp
    from standardised_subscribe.dbo_Bkr 
    WHERE {batch_effective_datetime} >= lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP()) group by BkrCd
) bkr
ON Upper(Trim(b.sectdsc)) = Upper(bkr.BkrCd)

-- MDS join to fetch Policy_Characteristic_Name
INNER JOIN standardised_mdm.mdm_policy_characteristic_type char_ref
ON b.sectty = char_ref.Source_Policy_Characteristic_Name
AND  lower(char_ref.Source_Name) = 'subscribe'
AND {batch_effective_datetime} >= char_ref.LakeValidFromTimestamp 
and {batch_effective_datetime} < coalesce(char_ref.LakeValidToTimestamp, current_timestamp())

WHERE b.rnk = 1
 """

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwPolicy_Characteristic'
  viewTables = 'standardised_Subscribe.dbo_PolMain,standardised_Subscribe.dbo_PolSectNarr,standardised_subscribe.dbo_inpol,standardised_mdm.mdm_policy_characteristic_type,standardised_subscribe.dbo_Bkr'

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
    std_object = 'standardised_subscribe.dbo_polsectnarr'
    current_bed = batchEffectiveDatetime
    chk_column = 'sectty'
    apply_upper_trim = "y"
    filter_list = ['INDUSTRY SECTION',
                   'NBC FLAG',
                   'EXCESS REPORTER',
                   'FAC OBLIG DECLARATION',
                   'WAITING PERIOD (IN HOURS)',
                   'NEW JERSEY SLA',
                   'SURPLUS LINES STATE',
                   'SURPLUS LINES LICENSE NUMBER',
                   'US REGIONAL STATUS',
                   'US SEGMENT',
                   'WATERTRACE REFERENCE',
                   'WAR AND TERRORISM WRITEBACK',
                   'ULR %',
                   'ULR CODE',
                   'STATS NAME',
                   'STATS CODE',
                   'SERADATA ID',
                   'SATELLITE NAME',
                   'RSCC',
                   'REVENUE CURRENCY',
                   'REVENUE',
                   'REGULATORY CLIENT CLASSIFICATION',
                   'PRIVATE TERRORISM',
                   'PREMIUM PAYMENT WARRANTY (PPW)',
                   'MAXIMUM EXPECTED DEDUCTIONS (%)',
                   'MAINTENANCE PERIOD MONTHS',
                   'HEADCOUNT',
                   'CLASH LOCATION',
                   'CASUALTY GROUPING',
                   'BULKING LINESLIP?',
                   'ASSISTANT',
                   'AMERICAN DEPOSIT RECEIPT',
                   'AGGS REQUIRED']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df = createStandardisedFilterView(std_object,
                                              prv_bed,
                                              current_bed,
                                              whereExpression,
                                              chk_column,
                                              apply_upper_trim,
                                              filter_list,
                                              cursor,
                                              batchTaskId,
                                              adfPipelineName,
                                              clusterId,
                                              notebookName,
                                              errorLogFileLocation)
        union_df=spark.read.table(std_object).unionByName(std_df)
    else :
        union_df=spark.read.table(std_object)
        
    union_df.createOrReplaceTempView('standardised_subscribe_dbo_polsectnarr_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_polsectnarr_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create dataframe from standardised data
try:
    if dependentTableIsUpdated == True:

        #set datetime parse policy
        spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")
        string_filter="','".join(filter_list)
        #Replace value of batch effective date in SQL Query
        sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                                   missing_string = "'" +str(missing_string) + "'", 
                                   missing_mdm = "'" +str(missing_mdm) + "'", 
                                   missing_startdate = "'" +str(missing_startdate) + "'",
                                   string_filter = "'" +str(string_filter) + "'")
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