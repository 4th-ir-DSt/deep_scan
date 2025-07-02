# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("databaseIDL", "POLARIS", 
                         ["POLARIS_DEV", "POLARIS_TEST","POLARIS"], "Snowflake IDL database Env ")

dbutils.widgets.dropdown("schemaIDL", "CUDM", 
                         ["CUDM", "SADM","OPDM", "SHDM"], "Snowflake IDL schema ")

dbutils.widgets.dropdown("databaseSTAGE", "CDM", 
                         ["CDM","CDM_DEV","CDM_TEST"], "Snowflake STAGE database Env ")

dbutils.widgets.dropdown("schemaSTAGE", "STAGING", 
                         ["STAGING"], "Snowflake STAGE schema ")

dbutils.widgets.dropdown("sfIDLTable", "SFMC_CLICK", 
                         ["SFMC_CLICK"], "Snowflake table ")

dbutils.widgets.dropdown("DBDatabase", "cdm_prod", 
                         ["cdm_dev","cdm_qa","cdm_prod"], "Databricks cdm database Env ")

dbutils.widgets.dropdown("truncate", "True", 
                         ["True", "False"], "Truncate before staging ")

# dbutils.widgets.dropdown("delta", "sfMax", 
#                          ["sfMax", "*", "integer"], "Determines length of lookback period ")

dbutils.widgets.dropdown("IDLtype", "Upsert", 
                         ["Upsert", "Insert"], "Type of IDL ")

dbutils.widgets.dropdown("sfLogSchema", "AUDIT", 
                         ["AUDIT"], "Log Schema")

dbutils.widgets.dropdown("sfLogTable", "PIPELINE_EXECUTION_LOG", 
                         ["PIPELINE_EXECUTION_LOG"], "Log Table")

# COMMAND ----------

# In case cluster doesn't have Snowflake connector (!)
dbutils.library.installPyPI("snowflake-connector-python")
dbutils.library.restartPython()

# COMMAND ----------

import platform, sys, os
from pyspark.sql import SparkSession
import snowflake.connector as conn
from datetime import date
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan,isnull, when, count, col, lit, to_timestamp, concat, concat_ws
import pyspark
from datetime import datetime, timedelta
from pytz import timezone
import numpy as np

print('Platform = ', platform.platform())  
print('Version of Spark = ', spark.version)
print('Python version = ', sys.version)
spark = SparkSession.builder.getOrCreate()
print('Spark session information = ', spark)

# COMMAND ----------

# DBTITLE 1,Execution Log Parameters
# Table for storing logs 
schemaLog  = dbutils.widgets.get("sfLogSchema")
sfLogTable = dbutils.widgets.get("sfLogTable")
cdmDBEnv = dbutils.widgets.get("DBDatabase")
# Execution ID = Name of Notebook + Timestamp with Millisecond + Random Number
nbName   = "Prod_IDL_SFMC_CLICK"
tz       = timezone('EST')
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
rnumber  = int(np.random.rand() * 1000000)

execId   = nbName + "_" + tmpstamp + str(rnumber)

# COMMAND ----------

# DBTITLE 1,Fetch snowflake connection string 
# MAGIC %run ../Snowflake/ConnectionString

# COMMAND ----------

# DBTITLE 1,Fetch Execution Logger
# MAGIC %run ../Governance/ExecutionLogger

# COMMAND ----------

# DBTITLE 1,Connection Parameters
# Display connection settings
warehouse = "PROD_ETL_WH"
sfAccount = 'inspire.east-us-2.azure'

# For incremental load (IDL) of data marts
database  = dbutils.widgets.get("databaseIDL")
schema    = dbutils.widgets.get("schemaIDL")
sfTable   = dbutils.widgets.get("sfIDLTable")

# For staging before IDL
databaseSTAGE  = dbutils.widgets.get("databaseSTAGE")
schemaSTAGE    = dbutils.widgets.get("schemaSTAGE")
cdm_table      = 'trg_mktg_email_click_plr'
stage_cdm_table      = 'trg_mktg_email_click_plr'

connOptionsIDL   = Options(database,schema,warehouse)
connOptionsSTAGE = Options(databaseSTAGE,schemaSTAGE,warehouse)

print('Connection parameters for incremental load (IDL) of data marts: ')
print(connOptionsIDL)

print('\n Connection parameters for staging before IDL: ')
print(connOptionsSTAGE)

# COMMAND ----------

# Connection cursors for IDL & STAGING
curIDL  = conn.connect(user=connOptionsIDL['sfUser'], 
                   password=connOptionsIDL['sfPassword'], 
                   account=sfAccount,
                   warehouse=connOptionsIDL['sfWarehouse'], 
                   database=connOptionsIDL['sfDatabase'], 
                  ).cursor()

curSTAGE = conn.connect(user=connOptionsSTAGE['sfUser'], 
                   password=connOptionsSTAGE['sfPassword'], 
                   account=sfAccount,
                   warehouse=connOptionsSTAGE['sfWarehouse'], 
                   database=connOptionsSTAGE['sfDatabase'], 
                  ).cursor()

# COMMAND ----------

# DBTITLE 1,Start Logging
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
log = "Started Notebook {} IDL Job for {}.{} on {}".format(nbName, schema, sfTable, tmpstamp)
executionLogger(curIDL, database, schemaLog, sfLogTable, execId, log, tmpstamp)

# COMMAND ----------

# DBTITLE 1,Determine length, type & location of delta load
#######################
# LENGTH OF DELTA LOAD
#######################
# 'delta' variable inputs:
#   '*'     : load entire cdm data
#   int     : number of days to subtract from current date 
#   'sfMax' : only fetch data in CDM that's beyond the current max date in Snowflake

#######################
# TYPE OF DELTA LOAD
#######################
# 'tuncate' variable is a boolean:
#   True  : table in staging area on Snowflake is truncated before delta is loaded 
#   False : delta is appended to table in staging area on Snowflake 

# 'IDLtype' variable inputs:
#   'merge'  : use merge into statement -> update & insert based on primary key (pk)
#   'insert' : use insert statement     -> only insert new records (used for fact data where no pk)

delta    = 'sfMax'
truncate = dbutils.widgets.get("truncate")
IDLtype  = dbutils.widgets.get("IDLtype")

# COMMAND ----------

# DBTITLE 1,Get latest data from CDM
if delta == '*':
  loadDate = 0
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(cdmDBEnv,cdm_table, loadDate))
  
elif delta == 'sfMax':
  # Get current max date in Snowflake table
  loadDate = curIDL.execute("SELECT MAX(CAST(CdmLoadDate AS INT)) FROM {}.{}".format(schema, sfTable)).fetchone()[0] 
  if loadDate is None:
     loadDate = 0  
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(cdmDBEnv,cdm_table, loadDate))
  print('Snowflake max date in {}.{} is: '.format(schema, sfTable) + str(loadDate))

elif type(delta) == int:
  tz = timezone('EST')
  loadDate = (datetime.now(tz)- timedelta(days=delta)).strftime('%Y%m%d')
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(cdmDBEnv,cdm_table, loadDate))

else:
  print('Error: Invalid input for delta !')

if df:  
  nrows = df.count()
  print('Delta load dataframe lenght in rows: ' + str(nrows))

# COMMAND ----------


df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

df = df.fillna('')

# COMMAND ----------

df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

df = df.fillna(0, subset=['OYBAccountID','isunique','TriggeredSendCustomerKey'])

# COMMAND ----------

df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

# DBTITLE 1,Add primary key column
df = df.withColumn('SfmcClickID', concat_ws("", col("BatchID"), col("SubscriberKey"), col("EventDate"), col("LinkName"), col("JobID"), col("URL")))

# COMMAND ----------

# DBTITLE 1,Drop duplicates
df = df.dropDuplicates()
nrowsDD = df.count()
print('Delta load dataframe lenght in rows after dedup: ' + str(nrowsDD))

if nrows != nrowsDD:
  print('Duplicates found in delta load - identical rows !')

# COMMAND ----------

# DBTITLE 1,Stage delta load in Snowflake
if nrows > 0:  
  if truncate == 'True':
    mode = "overwrite"
    try:
      curSTAGE.execute("TRUNCATE TABLE {}.{}".format(schemaSTAGE, stage_cdm_table))
    except: 
      print("Error truncating table {}.{} in Snowflake staging !".format(schemaSTAGE, stage_cdm_table))
  else:
    mode = "append"
    
  try:
    df.write\
      .format("snowflake")\
      .options(**connOptionsSTAGE)\
      .option("dbtable",  "{}.{}".format(schemaSTAGE, stage_cdm_table))\
      .mode(mode)\
      .save()
    print("{}.{} successfully loaded into Snowflake stage table {} !".format(schema, cdm_table, stage_cdm_table))
  except:
    print("Error loading table {}.{} into Snowflake stage table {} !".format(schemaSTAGE, cdm_table, stage_cdm_table))
    raise

else:
  print("No new data in CDM !")

# COMMAND ----------

# DBTITLE 1,Merge - Insert Fact
if nrows > 0: 
  if IDLtype == 'Upsert':
    qry_string = """
    MERGE INTO {}.{}.{} SNOW
    USING (SELECT 
        SfmcClickId
        ,AccountId
        ,OYBAccountId
        ,JobId
        ,ListId
        ,BatchId
        ,BrandId
        ,SubscriberId
        ,SubscriberKey
        ,IsUnique
        ,Domain
        ,URL
        ,LinkName
        ,LinkContent
        ,TriggeredSendCustomerKey
        ,EventDate
        ,CdmLoadDate
        ,CAST(TO_VARCHAR(CURRENT_DATE()::date, 'yyyymmdd') AS INT) AS LoadId
        ,TO_TIMESTAMP(CONVERT_TIMEZONE ('EST', current_timestamp)) AS LoadDttm
    FROM {}.{}.{}) CDM

    ON SNOW.SfmcClickID = CDM.SfmcClickId 

    WHEN MATCHED AND 
    (
        not(equal_null(SNOW.SfmcClickID,CDM.SfmcClickId))
        OR not(equal_null(SNOW.AccountID,CDM.AccountId))
        OR not(equal_null(SNOW.OYBAccountID,CDM.OYBAccountId))
        OR not(equal_null(SNOW.JobID,CDM.JobId))
        OR not(equal_null(SNOW.ListID,CDM.ListId))
        OR not(equal_null(SNOW.BatchID,CDM.BatchId))
        OR not(equal_null(SNOW.BrandId,CDM.BrandId))
        OR not(equal_null(SNOW.SubscriberID,CDM.SubscriberId))
        OR not(equal_null(SNOW.SubscriberKey,CDM.SubscriberKey))
        OR not(equal_null(SNOW.IsUnique,CDM.IsUnique))
        OR not(equal_null(SNOW.Domain,CDM.Domain))
        OR not(equal_null(SNOW.URL,CDM.URL))
        OR not(equal_null(SNOW.LinkName,CDM.LinkName))
        OR not(equal_null(SNOW.LinkContent,CDM.LinkContent))
        OR not(equal_null(SNOW.TriggeredSendCustomerKey,CDM.TriggeredSendCustomerKey))
        OR not(equal_null(SNOW.EventDate,CDM.EventDate))
        OR not(equal_null(SNOW.CdmLoadDate,CDM.CdmLoadDate))
    )

    THEN UPDATE SET 
        SNOW.SfmcClickID               = CDM.SfmcClickId
        ,SNOW.AccountID                = CDM.AccountId
        ,SNOW.OYBAccountID             = CDM.OYBAccountId
        ,SNOW.JobID                    = CDM.JobId
        ,SNOW.ListID                   = CDM.ListId
        ,SNOW.BatchID                  = CDM.BatchId
        ,SNOW.BrandId                  = CDM.BrandId
        ,SNOW.SubscriberID             = CDM.SubscriberId
        ,SNOW.SubscriberKey            = CDM.SubscriberKey
        ,SNOW.IsUnique                 = CDM.IsUnique
        ,SNOW.Domain                   = CDM.Domain
        ,SNOW.URL                      = CDM.URL
        ,SNOW.LinkName                 = CDM.LinkName
        ,SNOW.LinkContent              = CDM.LinkContent
        ,SNOW.TriggeredSendCustomerKey = CDM.TriggeredSendCustomerKey
        ,SNOW.EventDate                = CDM.EventDate
        ,SNOW.CdmLoadDate              = CDM.CdmLoadDate
        ,SNOW.LoadDttm                 = CDM.LoadDttm

    WHEN NOT MATCHED THEN INSERT (
        SfmcClickID
        ,AccountID
        ,OYBAccountID
        ,JobID
        ,ListID
        ,BatchID
        ,BrandId
        ,SubscriberID
        ,SubscriberKey
        ,IsUnique
        ,Domain
        ,URL
        ,LinkName
        ,LinkContent
        ,TriggeredSendCustomerKey
        ,EventDate
        ,CdmLoadDate
        ,LoadID
        ,LoadDttm )
      VALUES   (
        CDM.SfmcClickId
        ,CDM.AccountId
        ,CDM.OYBAccountId
        ,CDM.JobId
        ,CDM.ListId
        ,CDM.BatchId
        ,CDM.BrandId
        ,CDM.SubscriberId
        ,CDM.SubscriberKey
        ,CDM.IsUnique
        ,CDM.Domain
        ,CDM.URL
        ,CDM.LinkName
        ,CDM.LinkContent
        ,CDM.TriggeredSendCustomerKey
        ,CDM.EventDate
        ,CDM.CdmLoadDate
        ,CDM.LoadID
        ,CDM.LoadDttm
      )
    """.format(database, schema, sfTable, databaseSTAGE, schemaSTAGE, stage_cdm_table)
          
    curIDL.execute(qry_string)
  elif IDLtype == 'Insert':
    curIDL.execute("""
    INSERT INTO {}.{}.{}
    (
      AccountID
      ,OYBAccountID
      ,JobID
      ,ListID
      ,BatchID
      ,BrandId
      ,SubscriberID
      ,SubscriberKey
      ,IsUnique
      ,Domain
      ,URL
      ,LinkName
      ,LinkContent
      ,TriggeredSendCustomerKey
      ,EventDate
      ,LoadID
      ,LoadDttm
      ,SfmcClickID
    )

    SELECT 
      AccountId
      ,OYBAccountId
      ,JobId
      ,ListId
      ,BatchId
      ,BrandId
      ,SubscriberId
      ,SubscriberKey
      ,IsUnique
      ,Domain
      ,URL
      ,LinkName
      ,LinkContent
      ,TriggeredSendCustomerKey
      ,EventDate
      ,CAST(TO_VARCHAR(CURRENT_DATE()::date, 'yyyymmdd') AS INT) AS LoadId
      ,TO_TIMESTAMP(CONVERT_TIMEZONE ('EST',current_timestamp)) AS LoadDttm
      ,SfmcClickId

    FROM {}.{}.{}
    """.format(database, schema, sfTable, databaseSTAGE, schemaSTAGE, stage_cdm_table)
    )        
  else:
    print(" IDL Type not valid !")

# COMMAND ----------

# DBTITLE 1,Safety Check Query
pk = 'AccountID'
query = "select count(distinct({})) as UniqKeys, count({}) as TotalKeys from {}.{}"

sfCount  = curIDL.execute(query.format(pk,pk, schema, sfTable)).fetchall()
cdmCount = sqlContext.sql(query.format(pk,pk, cdmDBEnv, cdm_table))
cdmCount = [(cdmCount.select('UniqKeys').collect()[0].UniqKeys, cdmCount.select('TotalKeys').collect()[0].TotalKeys)]

if sfCount != cdmCount: 
  print('Missmatch between CDM & Snowflake data: unequal sizes of tables!')
  print('-Snowflake {}.{}.{} has '.format(database, schema, sfTable) + str(sfCount))
  print('-CDM {}.{} has '.format(cdmDBEnv,cdm_table) + str(cdmCount))

# COMMAND ----------

result_string = "IDL_SFMC_Complete Notebook complete - Data loaded from {}.{} count: {} into datamart table {} count: {}.".format(cdmDBEnv,cdm_table, cdmCount,
sfTable, sfCount)
print(result_string)
dbutils.notebook.exit(result_string)

# COMMAND ----------

# DBTITLE 1,End Logging
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
log = "End of Notebook {} IDL Job for {}.{} on {}".format(nbName, schema, sfTable, tmpstamp)
executionLogger(curIDL, database, schemaLog, sfLogTable, execId, log, tmpstamp)