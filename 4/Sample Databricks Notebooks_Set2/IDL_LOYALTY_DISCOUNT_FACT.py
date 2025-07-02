# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("DataBricks", "cdm_prod", 
                         ["cdm_qa", "cdm_prod","cdm_dev"], "Databricks IDL database Env ")

dbutils.widgets.dropdown("databaseIDL", "POLARIS", 
                         ["POLARIS_DEV", "POLARIS_TEST","POLARIS"], "Snowflake IDL database Env ")

dbutils.widgets.dropdown("schemaIDL", "CUDM", 
                         ["CUDM", "SADM","OPDM", "SHDM"], "Snowflake IDL schema ")

dbutils.widgets.dropdown("databaseSTAGE", "CDM", 
                         ["CDM","CDM_TEST","CDM_DEV"], "Snowflake STAGE database Env ")

dbutils.widgets.dropdown("schemaSTAGE", "STAGING", 
                         ["STAGING"], "Snowflake STAGE schema ")

dbutils.widgets.dropdown("sfIDLTable", "LOYALTY_DISCOUNT_FACT", 
                         ["LOYALTY_DISCOUNT_FACT"], "Snowflake table ")

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
from pyspark.sql.functions import isnan, when, count, col, lit, to_timestamp, concat, concat_ws
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

# Execution ID = Name of Notebook + Timestamp with Millisecond + Random Number
nbName   = "Prod_IDL_LOYALTY_DISCOUNT_FACT"
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
databricks =dbutils.widgets.get("DataBricks")

# For incremental load (IDL) of data marts
database  = dbutils.widgets.get("databaseIDL")
schema    = dbutils.widgets.get("schemaIDL")
sfTable   = dbutils.widgets.get("sfIDLTable")

# For staging before IDL
databaseSTAGE  = dbutils.widgets.get("databaseSTAGE")
schemaSTAGE    = dbutils.widgets.get("schemaSTAGE")
cdm_table      = 'tran_loyalty_discount_plr'
stage_cdm_table      = 'tran_loyalty_discount_plr'

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
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(databricks,cdm_table, loadDate))
  
elif delta == 'sfMax':
  # Get current max date in Snowflake table
  #loadDate = curIDL.execute("SELECT MAX(TO_DATE(CAST(CdmLoadDate AS STRING), 'YYYYMMDD')) FROM {}.{}.{}".format(schema, sfTable)).fetchone()[0].strftime('%Y%m%d')
  loadDate = curIDL.execute("SELECT IFNULL(MAX(TO_DATE(CAST(CdmLoadDate AS STRING), 'YYYYMMDD')),'19700815') FROM {}.{}.{}".format(database,schema, sfTable)).fetchone()[0].strftime('%Y%m%d')
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(databricks,cdm_table, loadDate))
  print('Snowflake max date in {}.{} is: '.format(schema, sfTable) + str(loadDate))

elif type(delta) == int:
  tz = timezone('EST')
  loadDate = (datetime.now(tz)- timedelta(days=delta)).strftime('%Y%m%d')
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(databricks,cdm_table, loadDate))

else:
  print('Error: Invalid input for delta !')

if df:  
  nrows = df.count()
  print('Delta load dataframe length in rows: ' + str(nrows))

# COMMAND ----------

# DBTITLE 1,Drop duplicates
df = df.dropDuplicates()
nrowsDD = df.count()
print('Delta load dataframe lenght in rows after dedup: ' + str(nrowsDD))

if nrows != nrowsDD:
  print('Duplicates found in delta load - identical rows !')
else:
    print('No duplicates found in delta load')

# COMMAND ----------

# DBTITLE 1,Stage delta load in Snowflake
if nrows > 0:  
  if truncate == 'True':
    mode = "overwrite"
    try:
      curSTAGE.execute("TRUNCATE TABLE {}.{}.{}".format(databaseSTAGE,schemaSTAGE, stage_cdm_table))
    except: 
      print("Error truncating table {}.{} in Snowflake staging !".format(schemaSTAGE, stage_cdm_table))
  else:
    mode = "append"
    
  try:
    df.write \
      .format("snowflake") \
      .options(**connOptionsSTAGE) \
      .option("dbtable",  "{}.{}.{}".format(databaseSTAGE,schemaSTAGE, stage_cdm_table)) \
      .mode(mode) \
      .save()
    print("{}.{} successfully loaded into Snowflake staging table {}.{}.{} !".format(databricks,cdm_table,databaseSTAGE,schemaSTAGE, stage_cdm_table))
  except:
    print("Error loading table {}.{} in Snowflake staging table {} !".format(schemaSTAGE, cdm_table,stage_cdm_table))  

else:
  print("No new data in CDM !")

# COMMAND ----------

# DBTITLE 1,Merge - Insert Fact
if nrows > 0: 
  if IDLtype == 'Upsert':
    print("{} has no primary key !".format(cdm_table))
    curIDL.execute("""
    MERGE INTO {}.{}.{} SNOW
    USING (
    SELECT 
        TRANDISCOUNTID
        ,TRANSACTIONID
        ,OFFERCODE
        ,SOURCEITEMID
        ,TRY_TO_NUMBER(DISCOUNTAMOUNT,10,5) DISCOUNTAMOUNT
        ,DISCOUNTDESC
        ,BRANDID
        ,SOURCE
        ,CDMLOADDATE
        ,CAST(TO_VARCHAR(CURRENT_DATE()::date, 'yyyymmdd') AS INT) AS LoadID
        ,TO_TIMESTAMP(CONVERT_TIMEZONE ('EST', current_timestamp)) AS LoadDttm
    FROM {}.{}.{}) CDM

    ON SNOW.TranDiscountId = CDM.TranDiscountId
    AND SNOW.BrandId = CDM.BrandId
    AND SNOW.TransactionId = CDM.TransactionId
    AND COALESCE(SNOW.SOURCEITEMID,'') = COALESCE(CDM.SOURCEITEMID,'')
    AND SNOW.Source = CDM.Source 

    WHEN MATCHED AND 
    (
       not(equal_null(SNOW.DiscountAmount,CDM.DiscountAmount))
      OR not(equal_null(SNOW.DiscountDesc,CDM.DiscountDesc))
      OR not(equal_null(SNOW.CdmLoadDate,CDM.CdmLoadDate))
      OR not(equal_null(SNOW.OfferCode,CDM.OfferCode))
    )

    THEN UPDATE SET 
      SNOW.DiscountAmount =  CDM.DiscountAmount
      ,SNOW.DiscountDesc   =  CDM.DiscountDesc
      ,SNOW.CdmLoadDate    =  CDM.CdmLoadDate  
      ,SNOW.LoadDttm       =  CDM.LoadDttm
      ,SNOW.OfferCode      =  CDM.OfferCode

    WHEN NOT MATCHED THEN INSERT (
        TranDiscountId
        ,TransactionId
        ,OfferCode
        ,SOURCEITEMID
        ,DiscountAmount
        ,DiscountDesc
        ,BrandId
        ,Source
        ,CdmLoadDate
        ,LoadID
        ,LoadDttm
      )
      VALUES   (
       CDM.TranDiscountId
        ,CDM.TransactionId
        ,CDM.OfferCode
        ,CDM.SOURCEITEMID
        ,CDM.DiscountAmount
        ,CDM.DiscountDesc
        ,CDM.BrandId
        ,CDM.Source
        ,CDM.CdmLoadDate
        ,CDM.LoadID
        ,CDM.LoadDttm
        )
    """.format(database, schema, sfTable, databaseSTAGE, schemaSTAGE, stage_cdm_table)
    )           
    
  elif IDLtype == 'Insert':
    print("{} has got a unique primary key --> do 'merge'".format(cdm_table))
  else:
    print(" IDL Type not valid !")
    
else:
  print("No new data in CDM !")    

# COMMAND ----------

# DBTITLE 1,End Logging
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
log = "End of Notebook {} IDL Job for {}.{} on {}".format(nbName, schema, sfTable, tmpstamp)
executionLogger(curIDL, database, schemaLog, sfLogTable, execId, log, tmpstamp)

# COMMAND ----------

