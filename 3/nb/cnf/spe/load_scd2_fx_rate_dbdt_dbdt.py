# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_scd2_fx_rate_dbdt_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data for Fx rate table in conformed zone</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Harish N</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/09/23</td></tr>
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
# MAGIC     <td>10/11/2022</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Updated View to deal with duplicates</td>
# MAGIC   </tr>  
# MAGIC    <tr>
# MAGIC     <td>17/11/2022</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Updated exchange rate types for daily and month end rate to inline with mds entries</td>
# MAGIC   </tr> 
# MAGIC   <tr>
# MAGIC     <td>02/03/2023</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Updates for the historical data changes</td>
# MAGIC   </tr> 
# MAGIC 
# MAGIC    <tr>
# MAGIC     <td>16/03/2023</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Updates for the exchage rate source historical data changes</td>
# MAGIC   </tr> 
# MAGIC   
# MAGIC     <tr>
# MAGIC     <td>17/03/2023</td>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>Updates for division and multiplication rate of exchange for daily exchange rate</td>
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

# DBTITLE 1,Create Temp view For Monthly Adhoc
adhoc_not_monthly_grain=spark.sql("""
select *
from Standardised_FX_Rate.Exchange_Rate_Ad_Hoc where Exchange_Rate_Source <>'Historical_adhoc' and last_day(Start_Date)<>End_Date
""")
adhoc_not_monthly_grain.withColumn('exp',explode(expr('sequence(Start_Date, End_Date, interval 1 month)'))).withColumn('Start_Date',col('exp')).withColumn('End_Date',last_day('exp')).createOrReplaceTempView('adhoc_not_monthly_grain')

# COMMAND ----------

# DBTITLE 1,Prepare SQL Query
sqlQuery = """
with cte_dailyrate AS
(
    SELECT 
    Source_Currency                AS From_Currency_Code,
    Target_Currency                AS To_Currency_Code,
    quote_date                     AS Exchange_Rate_Effective_From,
    quote_date                     AS Exchange_Rate_Effective_To,
    CAST(1/Rate AS DECIMAL(28,18)) AS Division_Rate_Of_Exchange,
    CAST(Rate AS DECIMAL(28,18)) AS Multiplication_Rate_of_Exchange,
    Exchange_Rate_Source,
    CASE 
        WHEN quote_date=last_day(quote_date) THEN 1 else 0 
    END                            AS last_Day_month,
    lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp 
    FROM Standardised_FX_Rate.Exchange_Rate_Daily
    WHERE {batch_effective_datetime} >= lakevalidfromtimestamp
    AND   {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
),

cte_Exchange AS
(
    select 
    From_Currency_Code,
    To_Currency_Code,
    Exchange_Rate_Effective_From,
    Exchange_Rate_Effective_To,
    Division_Rate_Of_Exchange,
    Multiplication_Rate_of_Exchange,
    'Exchange Rate Daily' AS Exchange_Rate_Source,
    'Daily Rate'                   AS Exchange_Rate_Type_Code,
    lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp 
    from cte_dailyrate
    
    union all
    
    select 
    From_Currency_Code,
    To_Currency_Code,
    Exchange_Rate_Effective_From,
    Exchange_Rate_Effective_To,
    Division_Rate_Of_Exchange,
    Multiplication_Rate_of_Exchange,
    'Exchange Rate Adhoc' AS Exchange_Rate_Source,
    'Month End Rate'               AS Exchange_Rate_Type_Code,
    lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp 
    from cte_dailyrate where last_Day_month = 1
    
    union all
    
    select  
    Source_Currency                AS From_Currency_Code,
    target_currency                AS To_Currency_Code,
    End_Date                       AS Exchange_Rate_Effective_From,
    End_Date                       AS Exchange_Rate_Effective_To, 
    Rate                           AS Division_Rate_Of_Exchange,
    CAST(1/Rate as decimal(22,8)) AS Multiplication_Rate_of_Exchange,
    'Exchange Rate Adhoc'                       AS Exchange_Rate_Source, 
    Rate_Type                      AS Exchange_Rate_Type_Code,
    lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp  
    from adhoc_not_monthly_grain
    WHERE {batch_effective_datetime} >= lakevalidfromtimestamp
    AND   {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
  
    
    union all
    
    select  
    Source_Currency                AS From_Currency_Code,
    target_currency                AS To_Currency_Code,
    End_Date                       AS Exchange_Rate_Effective_From,
    End_Date                       AS Exchange_Rate_Effective_To, 
    Rate                           AS Division_Rate_Of_Exchange,
    CAST(1/Rate as decimal(22,8)) AS Multiplication_Rate_of_Exchange,
    'Exchange Rate Adhoc'                       AS Exchange_Rate_Source, 
    Rate_Type                      AS Exchange_Rate_Type_Code,
    lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp  
    from Standardised_FX_Rate.Exchange_Rate_Ad_Hoc
    WHERE
    {batch_effective_datetime} >= lakevalidfromtimestamp
    AND   {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
    AND 
    (Exchange_Rate_Source <>'Historical_adhoc' and last_day(Start_Date)=End_Date)
    
    union all
    
    select  
    Source_Currency                AS From_Currency_Code,
    target_currency                AS To_Currency_Code,
    CASE 
        WHEN End_Date=last_day(End_Date) THEN End_Date else Start_Date
    END                            AS Exchange_Rate_Effective_From,
    End_Date                       AS Exchange_Rate_Effective_To, 
    Rate                           AS Division_Rate_Of_Exchange,
    CAST(1/Rate as decimal(22,8)) AS Multiplication_Rate_of_Exchange,
    'Exchange Rate Adhoc'                       AS Exchange_Rate_Source, 
    Rate_Type                      AS Exchange_Rate_Type_Code,
    lakelastupdatedate, lakelastupdatetimestamp, lakeDeletedTimestamp  
    from Standardised_FX_Rate.Exchange_Rate_Ad_Hoc
    WHERE 
    {batch_effective_datetime} >= lakevalidfromtimestamp
    AND   {batch_effective_datetime} < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())
    AND 
    Exchange_Rate_Source ='Historical_adhoc' 
)
        
SELECT
Exchange_Rate_Effective_From,
Exchange_Rate_Effective_To,
Exchange_Rate_Type_Code,
From_Currency_Code,
To_Currency_Code,
Exchange_Rate_Source,
max(Division_Rate_Of_Exchange)       AS Division_Rate_Of_Exchange,
max(Multiplication_Rate_of_Exchange) AS Multiplication_Rate_of_Exchange,
Max(lakelastupdatedate)              AS lakelastupdatedate,
Max(lakelastupdatetimestamp)         AS lakelastupdatetimestamp,
Max(lakedeletedtimestamp)            AS lakeDeletedTimestamp
from cte_Exchange
group by 
Exchange_Rate_Effective_From,
Exchange_Rate_Effective_To,
Exchange_Rate_Type_Code,
From_Currency_Code,
To_Currency_Code,
Exchange_Rate_Source
"""

# COMMAND ----------

# DBTITLE 1,Populate object date availability
try:
  #declare variables with hard coded values 
  #viewName - ‘dummyVw[conformed table name]’. It has a prefix ‘dummyVw’ and name of conformed table 
  #viewTables - ‘standardised.table1,standardised.table2’. Contains comma separated standardised table names used in query above

  viewName = 'dummy_Dummy.dummyVwExchange_Rate'
  viewTables = 'Standardised_FX_Rate.Exchange_Rate_Ad_Hoc,Standardised_FX_Rate.Exchange_Rate_Daily'

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
                               missing_mdm = "'" + str(missing_mdm) +"'")
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