# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_fx_rate_historical_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Load historical rates for daily and adhoc</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Harish</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2023/03/02</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC ## Notebook Parameters
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Parameter Name</th>
# MAGIC     <th>Parameter Description</th>
# MAGIC     <th>Example Parameter</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchId</td>
# MAGIC     <td>@batchId to retrieve batch details</td>
# MAGIC     <td>@batchId = 1</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchTaskId</td>
# MAGIC     <td>@batchTaskId to retrieve batchTask details</td>
# MAGIC     <td>@batchTaskId = 1</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>adfPipelineName</td>
# MAGIC     <td>@adfPipelineName to retrieve DataFactory pipelinename details</td>
# MAGIC     <td>@adfPipelineName = 'testpipepline'</td>
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
# MAGIC     <th>17/03/2023</th>
# MAGIC     <th>HARISH N</th>
# MAGIC     <th>Updated the daily rate source query</th>
# MAGIC   </tr>
# MAGIC   
# MAGIC    <tr>
# MAGIC     <th>21/03/2023</th>
# MAGIC     <th>HARISH N</th>
# MAGIC     <th>Updated the  source queries to exclude negative rates and exclude unknown currencies</th>
# MAGIC   </tr>
# MAGIC  
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

# DBTITLE 1,Import Source Daily Historical
# MAGIC %md
# MAGIC 
# MAGIC #### Run the below query on 2DEVSQLGRAFT01 and save it as csv  in the storage used for extsynapse 'extsynapse/fx_historical_data/Historical_Fx_daily_rates.csv'
# MAGIC ```
# MAGIC 
# MAGIC    SELECT
# MAGIC     [EFFECTIVE_DATE],
# MAGIC     [EFFECTIVE_YEAR],
# MAGIC     [EFFECTIVE_YEAR_MONTH],
# MAGIC     [EFFECTIVE_QUARTER],
# MAGIC     1.0 / [EXCHANGE_RATE] as EXCHANGE_RATE,
# MAGIC     bc.CURRENCY_ISO_CODE AS from_ccy,
# MAGIC     tc.CURRENCY_ISO_CODE AS to_ccy,
# MAGIC     ac.RATE_TYPE_DESCRIPTION AS rate_type
# MAGIC FROM
# MAGIC     [ANV_DWH_FDS].[ANVFDS].[FACT_FX] fx
# MAGIC     JOIN ANV_DWH_FDS.[ANVFDS].[DIM_EXCHANGE_RATE_TYPE] ac ON ac.exchange_rate_type_id = fx.[EXCHANGE_RATE_TYPE_ID]
# MAGIC     JOIN ANV_DWH_FDS.[ANVFDS].DIM_CURRENCY bc ON fx.BASE_CURRENCY_ID = bc.CURRENCY_ID
# MAGIC     JOIN ANV_DWH_FDS.[ANVFDS].DIM_CURRENCY tc ON fx.TARGET_CURRENCY_ID = tc.CURRENCY_ID
# MAGIC where
# MAGIC     tc.CURRENCY_ISO_CODE = 'GBP' --and bc.CURRENCY_ISO_CODE ='YUM'  AND  RATE_TYPE_DESCRIPTION='QMA YTD Average' -->
# MAGIC     AND upper(RATE_TYPE_DESCRIPTION) = 'DAILY RATE'
# MAGIC     and EFFECTIVE_DATE <= '2023-01-31'
# MAGIC     and EXCHANGE_RATE > 0.0
# MAGIC     and bc.CURRENCY_ISO_CODE not IN (
# MAGIC         'TR3',
# MAGIC         'IT2',
# MAGIC         'EC1',
# MAGIC         'MXF',
# MAGIC         'IT3',
# MAGIC         'BFP',
# MAGIC         'IRQ',
# MAGIC         'JP1',
# MAGIC         'EQE',
# MAGIC         'NG2',
# MAGIC         'TR2',
# MAGIC         'TR6',
# MAGIC         'KE1'
# MAGIC     )
# MAGIC 
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Import Source adhoc  Historical
# MAGIC %md
# MAGIC 
# MAGIC #### Run the below query on 2DEVSQLGRAFT01 and save it as csv  in the storage used for extsynapse 'extsynapse/fx_historical_data/Historical_Fx_adhoc_rates.csv'
# MAGIC ```
# MAGIC SELECT 
# MAGIC       [EFFECTIVE_DATE]
# MAGIC       ,[EFFECTIVE_YEAR]
# MAGIC       ,[EFFECTIVE_YEAR_MONTH]
# MAGIC       ,[EFFECTIVE_QUARTER]
# MAGIC       ,[EXCHANGE_RATE]
# MAGIC      , bc.CURRENCY_ISO_CODE                     AS from_ccy
# MAGIC      , tc.CURRENCY_ISO_CODE                        AS to_ccy
# MAGIC      , ac.RATE_TYPE_DESCRIPTION                    AS rate_type
# MAGIC 
# MAGIC   FROM [ANV_DWH_FDS].[ANVFDS].[FACT_FX] fx
# MAGIC    JOIN ANV_DWH_FDS.[ANVFDS].[DIM_EXCHANGE_RATE_TYPE] ac
# MAGIC     ON ac.exchange_rate_type_id                           = fx.[EXCHANGE_RATE_TYPE_ID]
# MAGIC   JOIN ANV_DWH_FDS.[ANVFDS].DIM_CURRENCY              bc
# MAGIC     ON fx.BASE_CURRENCY_ID                                = bc.CURRENCY_ID
# MAGIC   JOIN ANV_DWH_FDS.[ANVFDS].DIM_CURRENCY               tc
# MAGIC     ON fx.TARGET_CURRENCY_ID                               = tc.CURRENCY_ID
# MAGIC     where tc.CURRENCY_ISO_CODE='GBP'  
# MAGIC     -- tc.CURRENCY_ISO_CODE='GBP'  and bc.CURRENCY_ISO_CODE ='YUM'  AND  RATE_TYPE_DESCRIPTION='QMA YTD Average'
# MAGIC     
# MAGIC     AND 
# MAGIC     upper(RATE_TYPE_DESCRIPTION)  NOT IN ('DAILY RATE','MONTH END RATE')
# MAGIC     
# MAGIC     
# MAGIC     and EXCHANGE_RATE>0.0 and bc.CURRENCY_ISO_CODE not  IN ('TR3','IT2','EC1','MXF','IT3','BFP','IRQ','JP1','EQE','NG2','TR2','TR6','KE1')
# MAGIC 
# MAGIC 
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Import BusinessPlan
# MAGIC %md
# MAGIC #### Run the below query on Warehouse6\cdw and save it as csv  in the storage used for extsynapse 'extsynapse/fx_historical_data/Historical_Fx_adhoc_rates_Business_Plan.csv'
# MAGIC ```
# MAGIC SELECT
# MAGIC     roe.ROE_Type AS rate_type --  , roe.ROE_Type_code  AS roe_type_code
# MAGIC ,
# MAGIC     fc.YoA AS [EFFECTIVE_YEAR],
# MAGIC     fc.Quarter_Key AS [EFFECTIVE_QUARTER],
# MAGIC     scc.SCC AS from_ccy,
# MAGIC     sccto.SCC AS to_ccy,
# MAGIC     CAST(fc.roe AS DECIMAL(18, 8)) AS EXCHANGE_RATE --  , fc.RateMissingFlag AS rate_missing_flag
# MAGIC     --  ,fc.*
# MAGIC     -- *
# MAGIC FROM
# MAGIC     [GlobalData].[Fact].[ctb_ROE_Quarterly] fc
# MAGIC     JOIN [GlobalData].Dimension.cvw_ROE_Type roe ON roe.roe_type_key = fc.ROE_Type_Key
# MAGIC     JOIN [GlobalData].Dimension.ctb_SCC scc ON scc.SCC_Key = fc.SCC_Key
# MAGIC     JOIN [GlobalData].Dimension.ctb_SCC sccto ON sccto.SCC_Key = fc.SCC_To_Key
# MAGIC WHERE
# MAGIC     roe.roe_type = 'Business Plan'
# MAGIC     AND fc.From_Currency_Code != ''
# MAGIC     and fc.YoA <= 2023 --    AND fc.To_Currency_Code   != ''
# MAGIC     --    
# MAGIC     and sccto.SCC = 'GBP'
# MAGIC     and CAST(fc.roe AS DECIMAL(18, 8)) > 0.0
# MAGIC     and scc.SCC not IN (
# MAGIC         'TR3',
# MAGIC         'IT2',
# MAGIC         'EC1',
# MAGIC         'MXF',
# MAGIC         'IT3',
# MAGIC         'BFP',
# MAGIC         'IRQ',
# MAGIC         'JP1',
# MAGIC         'EQE',
# MAGIC         'NG2',
# MAGIC         'TR2',
# MAGIC         'TR6',
# MAGIC         'KE1'
# MAGIC     )
# MAGIC 
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Logging functions
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Standard Functions
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Oanda Functions
# MAGIC %run ../../util/spe/oanda_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DecimalType,ShortType,TimestampType, ArrayType
  from pyspark.sql import Window
  from datetime import datetime,timedelta
  from pyspark.sql.functions import lit,col,explode,concat_ws,create_map,struct,collect_list,coalesce,from_json,to_json,concat,expr,min,max,last,rank,when,last_day,dayofmonth,month,lead,lag,quarter,row_number,lead,lag
  from functools import reduce
  from itertools import chain 
  from collections import defaultdict
  import json
  import sys
  from decimal import Decimal
except Exception as e:
  errorMessage="Exception occurred while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise variables
currentTs=datetime.now()
  #get the last 3 positions of the microseconds as we need to remove them
currentTsMicroseconds = int(str(currentTs.strftime('%f'))[3:6])
#take away the last three microseconds
currentTs = currentTs - timedelta(microseconds=currentTsMicroseconds)

#get the date as an int format
CreatedDate = int(currentTs.strftime('%Y%m%d'))
createdDate=CreatedDate
lastUpdateDate=createdDate
#get the hour
CreatedHour = currentTs.hour
createdHour = CreatedHour
createdTimestamp = currentTs
lastUpdateTimestamp = currentTs
#   createdTimestamp
lastUpdatedTimestamp = currentTs
createdTime=currentTs
#use the same time for the date also
date=currentTs
#   createdBatchID=
#PARAMETER FOR logError
errorMessage = ''
adfPipelineName = ''
clusterId = ''
notebookName = ''
batchId = -1
batchTaskId = -1

#parameter for log_task_end
batchTaskStatus = ''
batchTaskSourceRows = 0
batchTaskRowsLoaded = 0
batchTaskRejectRows = 0
batchTaskResult = ''
batchTaskResultLocation = ''
batchTaskProgressMessage = '' 

#variable to retrieve the batch rows loaded or not
retrieveBatchRowsLoaded = False

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
    clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    dbutils.widgets.text("clusterId",clusterId)
#     clusterId  =  dbutils.widgets.get("clusterId")
    
    #assign the source and batch to other variables that are referenced from the metadata
    sourceId=sourceId
    createdBatchId=batchId
    lastUpdateBatchId=batchId
    
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
    logTaskProgress(cursor,batchTaskId,"Successfully established SQL connection")
except Exception as e:
    errorMessage="Unable to establish SQL connection: " + str(e)
    logToFile(errorLogFileLocation,errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Update This from which date the onda API need to run (In dev keep it more close to current date )
quoteDate='2023-02-27 23:59:59.987000'

# COMMAND ----------

# DBTITLE 1,Load Historical - Daily
df=spark.read.format('csv').option('header',True).load('/mnt/extsynapse/fx_historical_data/Historical_Fx_daily_rates.csv').withColumn('Quote_Date',col('EFFECTIVE_DATE').cast('date')).withColumn("currency_pair",concat_ws(',',col('from_ccy'),col('to_ccy')))
date_Df=df.where("Quote_Date<>'1900-01-01'").groupBy("currency_pair").agg(min('Quote_Date').alias("start_Date"),max('Quote_Date').alias("end_Date")).withColumn('Quote_Date', explode(expr('sequence(start_Date, end_Date, interval 1 day)')))

w = Window.partitionBy("currency_pair").orderBy("Quote_Date")

finalDf = date_Df.join(df, ["currency_pair", "Quote_Date"], "left").select(
    "currency_pair",
    "Quote_Date",
    *[last(col(c), ignorenulls=True).over(w).alias(c)
      for c in df.columns if c not in ("currency_pair", "Quote_Date")
     ]
)
schema='array<struct<ask:string,bid:string,date:string,target_Currency:string>>'
df2=(finalDf.distinct().withColumn('lakeCreatedDate',lit(createdDate))
 .withColumn('lakeCreatedTimeStamp',lit(createdTimestamp))
 .withColumn('lakeCreatedBatchID',lit(-1).cast('Long'))
 .withColumn('CDR_DATE',col('Quote_Date').cast('date'))
#  .withColumn('Quote_Date',col('CDR_DATE'))
 .withColumn('Base_Currency',col('FROM_CCY'))
 .withColumn('Quotes',concat(lit('['),to_json(
     
         struct(col('TO_CCY').alias('target_Currency')
                ,col('QUOTE_DATE').alias('date')
                ,col('EXCHANGE_RATE').alias('ask')
                ,col('EXCHANGE_RATE').alias('bid')
               )
     ))
 ).withColumn('Quotes',from_json(to_json(
     
         struct(col('TO_CCY').alias('target_Currency')
                ,col('CDR_DATE').alias('date')
                ,col('EXCHANGE_RATE').alias('ask')
                ,col('EXCHANGE_RATE').alias('bid')
               )
     ),schema=schema))
     .withColumn('Meta',from_json(lit(None),schema='struct<request_time:string,skipped_currencies:array<string>>'))
     .withColumn('Exchange_Rate_Source',lit('Historical_daily'))
    
    )
selectColumns=['lakeCreatedDate',
 'lakeCreatedTimeStamp',
 'lakeCreatedBatchID',
 'Quote_Date',
 'Base_Currency',
 'Quotes',
 'Meta',
 'Exchange_Rate_Source']
(df2.select(selectColumns).distinct().write
     .mode("append")
     .format('delta')
     .saveAsTable('Raw_FX_Rate.Exchange_Rate_Daily'))
rawTargetObjectName='Raw_FX_Rate.Exchange_Rate_Daily'
markQuoteDateAsLoaded(cursor,batchTaskId,rawTargetObjectName,quoteDate,createdTimestamp, adfPipelineName,clusterId,notebookName,errorLogFileLocation)


# COMMAND ----------

# DBTITLE 1,Load Historical Adhoc
w = Window.partitionBy("currency_pair","EFFECTIVE_YEAR").orderBy(col("Quote_Date").desc())
Business_plan_df=(spark.read.format('csv').option('header',True)
                  .load('/mnt/extsynapse/fx_historical_data/Historical_Fx_adhoc_rates_Business_Plan.csv')
                  .withColumn('EFFECTIVE_DATE',concat(col('EFFECTIVE_YEAR'),lit('-'),lit('01-01')).cast('Date'))             
                  .withColumn('Quote_Date',col('EFFECTIVE_DATE'))
                  .withColumn('EFFECTIVE_YEAR',col('EFFECTIVE_YEAR').cast('int'))
                  .withColumn('EFFECTIVE_YEAR_MONTH',concat(col('EFFECTIVE_YEAR'),lit('01')).cast('int'))
                  .withColumn("currency_pair",concat_ws(',',col('from_ccy'),col('to_ccy'),col('rate_type')))
                  .withColumn('EFFECTIVE_DATE',col('EFFECTIVE_DATE').cast('date')))
duplicates=Window.partitionBy("currency_pair","Quote_Date").orderBy(col("Quote_Date").desc())
Business_plan_df=Business_plan_df.withColumn("duplicate_remove",row_number().over(duplicates)).where('duplicate_remove==1').drop("duplicate_remove")

df=(spark.read.format('csv').option('header',True)
    .load('/mnt/extsynapse/fx_historical_data/Historical_Fx_adhoc_rates.csv')
    .where("rate_type <> 'Month End Rate'")
    .withColumn('Quote_Date',col('EFFECTIVE_DATE').cast('date'))
    .withColumn('EFFECTIVE_YEAR',col('EFFECTIVE_YEAR').cast('int'))
    .withColumn("currency_pair",concat_ws(',',col('from_ccy'),col('to_ccy'),col('rate_type')))
    .withColumn('EFFECTIVE_DATE',col('EFFECTIVE_DATE').cast('date')))
duplicates=Window.partitionBy("currency_pair","Quote_Date").orderBy(col("Quote_Date").desc())
df=df.withColumn("duplicate_remove",row_number().over(duplicates)).where('duplicate_remove==1').drop("duplicate_remove").withColumn('EFFECTIVE_DATE',when(
    ((month(col('EFFECTIVE_DATE'))!=lit(1).cast('int') )& 
    (coalesce(lead(col("EFFECTIVE_QUARTER"),1).over(w),lit(''))!=col('EFFECTIVE_QUARTER')) 
    & (coalesce(lag(col("EFFECTIVE_QUARTER"),1).over(w),lit(''),col('EFFECTIVE_QUARTER'))!=col('EFFECTIVE_QUARTER')))
    ,concat(col('EFFECTIVE_YEAR'),lit('-'),3 * quarter(col('EFFECTIVE_DATE')) - 2 ,lit('-01')).cast('date')).otherwise( col('EFFECTIVE_DATE')))


df=df.unionByName(Business_plan_df)
df1=df.withColumn("Rank",rank().over(w)).where("Rank=1")
# df.display()
year_df=(df1.groupBy("currency_pair").agg(min('EFFECTIVE_YEAR').alias("start_Year"),max('EFFECTIVE_YEAR').alias("end_Year"))
         .withColumn('EFFECTIVE_YEAR', explode(expr('sequence(start_Year, end_Year)'))))
w = Window.partitionBy("currency_pair").orderBy("EFFECTIVE_YEAR")

finalDf = year_df.join(df1, ["currency_pair", "EFFECTIVE_YEAR"], "left").select(
    "currency_pair",
    "EFFECTIVE_YEAR",
    *[last(col(c), ignorenulls=True).over(w).alias(c)
      for c in df.columns if c not in ("currency_pair", "EFFECTIVE_YEAR")
     ]
)

Missing_Years_Df1=(finalDf
                   .withColumn("filter_flag"
                               ,when(((col('EFFECTIVE_YEAR_MONTH').cast('Int')/100).cast('INT'))==col('EFFECTIVE_YEAR')
                                     ,lit(0))
                               .otherwise(lit(1)))
                   .withColumn("EFFECTIVE_DATE"
                            ,when(((col('EFFECTIVE_YEAR_MONTH').cast('Int')/100).cast('INT'))==col('EFFECTIVE_YEAR')
                                  ,col('EFFECTIVE_DATE'))
                            .otherwise(concat(col('EFFECTIVE_YEAR'),lit('-'),lit('12-01')).cast('Date')))
                   .where('filter_flag=1'))
Missing_Years_Df2=Missing_Years_Df1.withColumn("EFFECTIVE_DATE",concat(col('EFFECTIVE_YEAR'),lit('-'),lit('01-01')).cast('Date'))
Missing_Years_Df=Missing_Years_Df1.unionByName(Missing_Years_Df2)
# Missing_Years_Df.display()
added_missing_years=df.unionByName(Missing_Years_Df.select(*[col(c) for c in df.columns]))
month_df=(added_missing_years.groupBy("currency_pair","EFFECTIVE_YEAR").agg(min('EFFECTIVE_DATE').alias("start_month"),max('EFFECTIVE_DATE').alias("end_month"))
          .withColumn("end_month",when (month(col("end_month"))==lit('12')
                                        ,col('end_month'))
                      .otherwise(concat(col('EFFECTIVE_YEAR'),lit('-'),lit('12-31')).cast('Date')))
          .withColumn('EFFECTIVE_DATE', explode(expr('sequence(start_month, end_month,interval 1 MONTH)')))
          .drop("EFFECTIVE_YEAR"))

# month_df.display()
w2 = Window.partitionBy("currency_pair").orderBy("EFFECTIVE_DATE")
finalDf_adhoc = month_df.join(added_missing_years, ["currency_pair", "EFFECTIVE_DATE"], "left").select(
    "currency_pair",
    "EFFECTIVE_DATE",
    *[last(col(c), ignorenulls=True).over(w2).alias(c)
      for c in df.columns if c not in ("currency_pair", "EFFECTIVE_DATE")
     ]
)
# finalDf_adhoc.display()
adhoc_table_to_append_Df=(finalDf_adhoc
                          .withColumn('lakeCreatedDate',lit(createdDate))
                          .withColumn('lakeCreatedTimeStamp',lit(createdTimestamp))
                          .withColumn('lakeCreatedBatchID',lit(-1).cast('Long'))
                          .withColumn('Start_Date',col('EFFECTIVE_DATE').cast('date'))
                          .withColumn('End_Date',when(dayofmonth(col('EFFECTIVE_DATE').cast('date'))==1
                                                      ,last_day(col('EFFECTIVE_DATE')))
                                      .otherwise(col('EFFECTIVE_DATE')))
                          .withColumn('lakeCreatedBatchID',lit(-1).cast('Long'))
                          .withColumn('Source_Currency',col('from_ccy'))
                          .withColumn('Target_Currency',col('to_ccy'))
                          .withColumn('Rate_Type',col('rate_type'))
                          .withColumn('Rate',col('EXCHANGE_RATE').cast(DecimalType(22,8)))
                          .withColumn('Exchange_Rate_Source',lit('Historical_adhoc'))
                          .withColumn('Exchange_Rate_Source_Filename',lit('Historical_adhoc_Data')))
selectColumns=['lakeCreatedDate',
 'lakeCreatedTimeStamp',
 'lakeCreatedBatchID',
 'Start_Date',
 'End_Date',
 'Source_Currency',
 'Target_Currency',
 'Rate_Type',
 'Rate',
 'Exchange_Rate_Source',
 'Exchange_Rate_Source_Filename']

(adhoc_table_to_append_Df.where('EFFECTIVE_DATE<="2023-01-31"').distinct().select(selectColumns).write
     .mode("append")
     .format('delta')
     .saveAsTable('Raw_FX_Rate.Exchange_Rate_Ad_Hoc'))