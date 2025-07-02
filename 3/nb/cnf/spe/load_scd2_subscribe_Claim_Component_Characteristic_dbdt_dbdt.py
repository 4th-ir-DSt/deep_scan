# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_subscribe_Claim_Component_Characteristic_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Claim_Component_Characteristic table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Rahul</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/04/29</td></tr>
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

# DBTITLE 1,UDFs for validating date and numeric values
# MAGIC %py
# MAGIC 
# MAGIC #for %m/%d/yy
# MAGIC from datetime import datetime
# MAGIC def validateDate_8_1(date):
# MAGIC     try:
# MAGIC         datetime.strptime(date, '%d/%m/%y')
# MAGIC         return True
# MAGIC     except ValueError:
# MAGIC         isValidDate = False
# MAGIC         return isValidDate
# MAGIC 
# MAGIC sqlContext.udf.register('isdt_1', validateDate_8_1)
# MAGIC 
# MAGIC #for %m/%d/yyyy
# MAGIC def validateDate_8_2(date):
# MAGIC     try:
# MAGIC         datetime.strptime(date, '%d/%m/%Y')
# MAGIC         return True
# MAGIC     except ValueError:
# MAGIC         isValidDate = False
# MAGIC         return isValidDate
# MAGIC 
# MAGIC sqlContext.udf.register('isdt_2', validateDate_8_2)
# MAGIC 
# MAGIC 
# MAGIC #for formats like 1 Dec 2020
# MAGIC def validateDate_11(date):
# MAGIC     try:
# MAGIC         datetime.strptime(date, '%d %b %Y')
# MAGIC         return True
# MAGIC     except ValueError:
# MAGIC         isValidDate = False
# MAGIC         return isValidDate
# MAGIC sqlContext.udf.register('isdt_3', validateDate_11)
# MAGIC 
# MAGIC #for formats like 1 December 2020
# MAGIC def validateDate_12(date):
# MAGIC     try:
# MAGIC         datetime.strptime(date, '%d %B %Y')
# MAGIC         return True
# MAGIC     except ValueError:
# MAGIC         isValidDate = False
# MAGIC         return isValidDate
# MAGIC 
# MAGIC sqlContext.udf.register('isdt_4', validateDate_12)
# MAGIC 
# MAGIC def is_numeric(s):
# MAGIC     try:
# MAGIC         float(s)
# MAGIC         return 1
# MAGIC     except ValueError:
# MAGIC         return 0
# MAGIC 
# MAGIC sqlContext.udf.register('udf_isnumeric', is_numeric)

# COMMAND ----------

# DBTITLE 1,Prepare SQL Query
sqlQuery="""
SELECT
coalesce(BPR, {missing_string})                                         AS Claim_Component_Reference,
sectty                                                                  AS Claim_Component_Characteristic_Name,
CASE
    WHEN (
          Isdt_1(Trim(sectdsc)) = true
          OR Isdt_2(SectDsc) = true
          OR Isdt_3(SectDsc) = true
          OR Isdt_4(SectDsc) = true
          ) AND Length(SectDsc) BETWEEN 8 AND 12 
    THEN 'DATE'
    WHEN Udf_isnumeric(SectDsc) = 1 AND sectty NOT LIKE 'ULR %' 
    THEN 'AMOUNT'
    ELSE 'CHARACTER' 
END                                                                     AS Claim_Component_Characteristic_Data_Type,
CASE
    WHEN Length(SectDsc) <= 5 THEN {missing_startdate}
    WHEN (
          (Isdt_1(SectDsc) = true
          OR Isdt_2(SectDsc) = true
          OR Isdt_3(SectDsc) = true
          OR Isdt_4(SectDsc) = true)
          AND Length(sectdsc) BETWEEN 8 AND 12
          ) THEN COALESCE(To_date(sectdsc,'dd MMM y'), To_date(sectdsc,'d/M/yy'), sectdsc)
    ELSE {missing_startdate} 
END                                                                     AS Claim_Component_Characteristic_Date,
CASE
    WHEN (
          Isdt_1(SectDsc)    = false
          OR Isdt_2(SectDsc) = false
          OR Isdt_3(SectDsc) = false
          OR Isdt_4(SectDsc) = false
          OR Length(SectDsc) < 8
          OR Length(SectDsc) > 12
          OR Udf_isnumeric(SectDsc) <> 1
          OR sectty NOT LIKE 'ULR %' 
          ) THEN SectDsc ELSE {missing_string} 
END                                                                     AS Claim_Component_Characteristic_Description,
CASE
    WHEN Udf_isnumeric(SectDsc) = 1 AND sectty NOT LIKE 'ULR %' 
    THEN CAST(SectDsc as DECIMAL(28,2))
    ELSE 0.00
END                                                                     AS Claim_Component_Characteristic_Amount,
lakelastupdatedate,
lakelastupdatetimestamp,
lakeDeletedTimestamp
    
FROM (
     SELECT BPR, upper(trim(SectTy))                                                    AS SectTy,
            upper(trim(SectDsc)) AS SectDsc,
            greatest(narr.lakelastupdatedate, main.lakelastupdatedate)                  AS lakelastupdatedate,
            greatest(narr.lakelastupdatetimestamp, main.lakelastupdatetimestamp)        AS lakelastupdatetimestamp,
            CASE 
                WHEN coalesce(narr.lakeDeletedTimestamp, main.lakeDeletedTimestamp) IS NOT NULL THEN {batch_effective_datetime} 
                ELSE NULL 
            END                                                                         AS lakeDeletedTimestamp,
            row_number() OVER(partition BY BPR, upper(trim(sectty)) 
            ORDER BY coalesce(narr.lakeDeletedTimestamp, 
                '9999-12-31T01:01:01.000') desc, TrnSectNarrId desc)                    AS rnk
     FROM   standardised_subscribe_dbo_TrnSectNarr_tmpvw narr
     
     INNER JOIN 
     (
          select upper(nullif(trim(BPR),''))                                            AS BPR, 
          TrnCgy, 
          upper(concat(trim(BPR), trim(CKY), trim(UnitID), trim(SLN)))                  AS updated_Trnid, 
          max(lakelastupdatedate)                                                       AS lakelastupdatedate, 
          max(lakelastupdatetimestamp)                                                  AS lakelastupdatetimestamp,
          NULLIF(max(coalesce(lakeDeletedTimestamp,
              '9999-12-31T01:01:01')),'9999-12-31T01:01:01')                            AS lakeDeletedTimestamp
          from standardised_subscribe_dbo_scmmain_tmpvw
          WHERE {batch_effective_datetime} >= lakevalidfromtimestamp
          AND {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
          group by upper(nullif(trim(BPR),'')), TrnCgy,
          upper(CONCAT(trim(BPR), trim(CKy), trim(UnitID), trim(SLN)))
     ) main 
     ON upper(trim(narr.trnid)) = updated_Trnid
     AND upper(trim(narr.TrnCgy)) = upper(trim(main.TrnCgy)) 
     WHERE  upper(trim(sectty)) IN ({string_filter_TrnSectNarr})
     AND {batch_effective_datetime} >= narr.lakevalidfromtimestamp
     AND {batch_effective_datetime} < COALESCE(narr.lakevalidtotimestamp, CURRENT_TIMESTAMP())
     ) fnl
where rnk = 1
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwClaim_Component_Characteristic'
  viewTables = 'standardised_subscribe.dbo_scmmain, standardised_subscribe.dbo_trnsectnarr'

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
    std_object_TrnSectNarr = 'standardised_subscribe.dbo_TrnSectNarr'
    current_bed = batchEffectiveDatetime
    chk_column = 'sectty'
    apply_upper_trim = "y"
    filter_list=['ANNUAL REVIEW',
                 'BORDEREAU DUE',
                 'BROKER',
                 'BUREAU LEAD',
                 'CAUSE OF LOSS',
                 'CLAIM NAME',
                 'CLAIMANT',
                 'FILE CLOSURE',
                 'INSURED',
                 'LAWYER',
                 'LOSS ADJUSTER',
                 'MISCELLANEOUS',
                 'ORIGINAL INSURED']
    
    if prv_bed is not None and dependentTableIsUpdated == True:
        std_df_TrnSectNarr = createStandardisedFilterView(std_object_TrnSectNarr, 
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
        union_df_TrnSectNarr = spark.read.table(std_object_TrnSectNarr).unionByName(std_df_TrnSectNarr)
    else :
        union_df_TrnSectNarr = spark.read.table(std_object_TrnSectNarr)
        
    union_df_TrnSectNarr.createOrReplaceTempView('standardised_subscribe_dbo_trnSectNarr_tmpvw')
    
    logTaskProgress(cursor,batchTaskId,"standardised_subscribe_dbo_trnSectNarr_tmpvw created")

except Exception as e:
    errorMessage="error in creating temp view: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Create Standardised Grain View
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
        string_filter_TrnSectNarr = "','".join(filter_list)
        #Replace value of batch effective date in SQL Query
        sqlQuery = sqlQuery.format(batch_effective_datetime = "'" + str(batchEffectiveDatetime) + "'", 
                               missing_string = "'" + str(missing_string) + "'",
                               missing_startdate = "'" + str(missing_startdate) + "'",
                               string_filter_TrnSectNarr = "'" + str(string_filter_TrnSectNarr) + "'")
    
        #set timeparsepolicy
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    
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