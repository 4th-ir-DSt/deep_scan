# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_mdm_Office_Line_Relationship_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Office Line Relationship table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Rahul</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/03/22</td></tr>
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
  from pyspark.sql.functions import rank,desc,col,lag, when, lit, concat,row_number,collect_list, coalesce, concat_ws, first,last,rank,max,lead,upper,trim,size, count
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

# DBTITLE 1,Prepare SQL Query
sqlQuery = """
WITH 
MDSData AS 
(
	-- GET MDS Data from UnPivoted MDS table for applicable Policies from PolAnlyCd
    SELECT
    pac.polid,
    pac.unitpsu,
    MDS.office_code,
    MDS.office_relationship_type,      
    GREATEST(pac.LakeLastUpdateDate, MDS.LakeLastUpdateDate, ip.LakeLastUpdateDate)                AS LakeLastUpdateDate,
    GREATEST(pac.LakeLastUpdateTimestamp, MDS.LakeLastUpdateTimestamp, ip.LakeLastUpdateTimestamp) AS LakeLastUpdateTimestamp, 
    CASE 
        WHEN COALESCE(pac.lakeDeletedTimestamp, MDS.lakeDeletedTimestamp, ip.LakeLastUpdateDate) IS NOT NULL 
        THEN {batch_effective_datetime} ELSE NULL
    END                                                                                            AS lakeDeletedTimestamp
    FROM standardised_subscribe_dbo_PolAnlyCd_tmpvw pac       
    INNER JOIN 
    (
         --- Retrieve latest 'Valid' record of the Unpivoted MDS data
         SELECT  Sub_Class_Code,
         Year,
         Office_Code, 
         Office_RelationShip_Type, 
         MAX(lakelastupdatedate)                                                                   AS LakeLastUpdateDate,
         MAX(lakelastupdatetimestamp)                                                              AS LakeLastUpdateTimestamp,        
         NULLIF(MAX(COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01')),'9999-12-31T01:01:01')   AS lakeDeletedTimestamp
         FROM (
                 --- UNPIVOTING OF MDS Data from 3 different columns to be used as Office_Code
                 SELECT Sub_Class_Code
                ,Year
                ----------Using 'UNCODED' instead of 'UNMAPPED' since MDS's default is 'UNCODED'
                ,STACK(
                       3,'Underwriting_Office',COALESCE(Underwriting_Office_Code,'UNCODED'),
                         'Fin104_Office',COALESCE(Fin104_Office_Code,'UNCODED'),
                         'Claim_Office',COALESCE(Claim_Office_Code,'UNCODED')
                      )                                                     AS (Office_RelationShip_Type, Office_Code)
                ----------Using 'UNCODED' instead of 'UNMAPPED' since MDS's default is 'UNCODED'
                ,lakelastupdatedate
                ,lakelastupdatetimestamp
                ,lakeDeletedTimestamp
                 FROM standardised_mdm.mdm_Business_Classification_Mapping
                 WHERE {batch_effective_datetime} >= lakeValidFromTimestamp
                 AND {batch_effective_datetime} < COALESCE(lakeValidToTimestamp, CURRENT_TIMESTAMP())
             ) MDS_Unpiv
         GROUP BY Sub_Class_Code,
         Year,
         Office_Code, 
         Office_RelationShip_Type
    ) MDS
    ON UPPER(TRIM(pac.cd)) = UPPER(TRIM(MDS.sub_class_code))
    AND UPPER(TRIM(pac.ty)) IN ({string_filter_PolAnlyCd})
    AND {batch_effective_datetime} >= pac.lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(pac.lakevalidtotimestamp, CURRENT_TIMESTAMP())
    INNER JOIN
    (
        SELECT PolId, UnitPsu, AcctgYr, lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp 
        from standardised_subscribe.dbo_InPol
        WHERE {batch_effective_datetime} >= lakeValidFromTimestamp
        AND {batch_effective_datetime} < COALESCE(lakeValidToTimestamp, CURRENT_TIMESTAMP())
    ) ip
    ON  pac.PolId = ip.PolId
    AND pac.UnitPsu = ip.UnitPsu
    AND MDS.Year = ip.AcctgYr
    AND {batch_effective_datetime} >= pac.lakevalidfromtimestamp
    AND {batch_effective_datetime} < COALESCE(pac.lakevalidtotimestamp, CURRENT_TIMESTAMP())
),
SubscribeData AS 
(
    SELECT
    PolID,
    unitpsu,
    CASE 
        WHEN SectDsc = 'NO - SNIC' THEN 'SNIC'
        WHEN SectDsc = 'NO - SFMI' THEN 'SFMI'
        ELSE 'NA'           
    END                                                                     AS office_code,
    'Fronting_Office'                                                       AS Office_Relationship_Type,
    lakeLastUpdateDate, lakeLastUpdateTimestamp, lakeDeletedTimestamp
    FROM
    (  
       --Eliminating duplicates as PolSectNarr is at lower granularity
       SELECT polid, unitpsu, SectDsc,
       lakeLastUpdateDate, lakeLastUpdateTimestamp, lakeDeletedTimestamp,
       ROW_NUMBER() OVER( PARTITION BY polid, unitpsu
                          ORDER BY COALESCE(lakeDeletedTimestamp,'9999-12-31T01:01:01') DESC, SectNarrSeqNo DESC ) AS rnk
       FROM standardised_subscribe_dbo_polsectnarr_tmpvw
       WHERE UPPER(TRIM(SectTy)) IN ({string_filter_polsectnarr})
       AND {batch_effective_datetime} >= lakevalidfromtimestamp
       AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
    ) PolSectNarr 
    WHERE rnk = 1 --Eliminating duplicates as PolSectNarr is at lower granularity     
),	
Univ AS 
(
    SELECT PolId,
	UnitPsu,
	Office_code,
	Office_Relationship_Type,
	lakelastupdatedate,
	lakelastupdatetimestamp,
	lakeDeletedTimestamp
	FROM MDSData
    
	UNION ALL
    
	SELECT PolId,
	UnitPsu,
	Office_code,
	Office_Relationship_Type,
	lakelastupdatedate,
	lakelastupdatetimestamp,
	lakeDeletedTimestamp
	FROM SubscribeData
)

SELECT 
-- Fronting Office relationship Type to be poluated as 'NA' wherever office_code is 'London' as it is not considered to be Fronted
COALESCE(Univ.office_relationship_type, {missing_string})                   AS Office_relationship_type,
Iptpt.UnitId                                                                AS Business_Entity_Code,
CONCAT(Iptpt.polid, '_', Iptpt.unitpsu)                                     AS Policy_Section_Reference,
CONCAT(Iptpt.polid, '_', Iptpt.unitpsu)                                     AS Policy_Header_Reference,
UPPER(TRIM(univ.office_code))                                               AS Office_Code,
GREATEST(univ.lakelastupdatedate, Iptpt.lakelastupdatedate)                 AS lakelastupdatedate,
GREATEST(univ.lakelastupdatetimestamp, Iptpt.lakelastupdatetimestamp)       AS lakelastupdatetimestamp,
CASE
    WHEN COALESCE(univ.lakeDeletedTimestamp, Iptpt.lakeDeletedTimestamp) IS NULL THEN NULL ELSE {batch_effective_datetime} 
END                                                                         AS lakeDeletedTimestamp
FROM Univ

-- Fetch the policies from participant table that exists in main table
INNER JOIN Standardised_Subscribe.dbo_InPolPtPt Iptpt 
ON univ.PolId = Iptpt.polid
AND univ.unitpsu = Iptpt.UnitPsu
AND {batch_effective_datetime} >= Iptpt.lakevalidfromtimestamp
AND {batch_effective_datetime} < COALESCE(Iptpt.lakevalidtotimestamp, CURRENT_TIMESTAMP())
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwOffice_Line_Relationship'
  viewTables = 'standardised_mdm.mdm_Business_Classification_Mapping, Standardised_Subscribe.dbo_InPolPtPt, standardised_subscribe.dbo_PolAnlyCd, Standardised_Subscribe.dbo_PolSectNarr,standardised_subscribe.dbo_InPol'

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

# DBTITLE 1,Create Filter Standardised View For dbo_PolAnlyCd
try:
    std_object_PolAnlyCd = 'standardised_subscribe.dbo_PolAnlyCd'
    current_bed = batchEffectiveDatetime
    chk_column_PolAnlyCd = 'Ty'
    apply_upper_trim_PolAnlyCd = "y"
    filter_list_PolAnlyCd = ['SUBCLASS']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_PolAnlyCd = createStandardisedFilterView(std_object_PolAnlyCd,
                                                    prv_bed,
                                                    current_bed,
                                                    whereExpression,
                                                    chk_column_PolAnlyCd,
                                                    apply_upper_trim_PolAnlyCd,
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

# DBTITLE 1,Create Filter Standardised View For dbo_polsectnarr
try:
    std_object_polsectnarr = 'standardised_subscribe.dbo_polsectnarr'
    current_bed = batchEffectiveDatetime
    chk_column_polsectnarr = 'SectTy'
    apply_upper_trim_polsectnarr = "y"
    filter_list_polsectnarr = ['LONDON BOUND LINE']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_polsectnarr = createStandardisedFilterView(std_object_polsectnarr,
                                                          prv_bed,
                                                          current_bed,
                                                          whereExpression,
                                                          chk_column_polsectnarr,
                                                          apply_upper_trim_polsectnarr,
                                                          filter_list_polsectnarr,
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
    string_filter_PolAnlyCd="','".join(filter_list_PolAnlyCd)
    quote_String_PolAnlyCd="'"+string_filter_PolAnlyCd+"'"
    
    string_filter_polsectnarr="','".join(filter_list_polsectnarr)
    quote_String_polsectnarr="'"+string_filter_polsectnarr+"'"
    
    sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                               missing_mdm = "'" + str(missing_mdm) + "'", 
                               missing_string = "'" + str(missing_string) + "'", 
                               string_filter_PolAnlyCd=str(quote_String_PolAnlyCd),
                               string_filter_polsectnarr=str(quote_String_polsectnarr))
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