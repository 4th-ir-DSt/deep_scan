# Databricks notebook source
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

dbutils.widgets.dropdown("sfIDLTable", "CHANNEL_DIM", 
                         ["CHANNEL_DIM"], "Snowflake table ")

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
from pyspark.sql.functions import isnan, when, count, col, lit, to_timestamp,concat
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

# DBTITLE 1,Fetch snowflake connection string 
# MAGIC %run ../Snowflake/ConnectionString

# COMMAND ----------

# DBTITLE 1,Fetch Execution Logger
# MAGIC %run ../Governance/ExecutionLogger

# COMMAND ----------

# DBTITLE 1,Execution Log Parameters
# Table for storing logs 
schemaLog  = dbutils.widgets.get("sfLogSchema")
sfLogTable = dbutils.widgets.get("sfLogTable")

# Execution ID = Name of Notebook + Timestamp with Millisecond + Random Number
nbName   = "Prod_IDL_CHANNEL_DIM"
tz       = timezone('EST')
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
rnumber  = int(np.random.rand() * 1000000)

execId   = nbName + "_" + tmpstamp + str(rnumber)

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
cdm_table1      = 'tran_channel_plr'
cdm_table2      = 'tran_orderchannel_hierarchy_plr'
cdm_table3      = 'tran_fulfillmentchannel_hierarchy_plr'

stage_cdm_table1  = 'tran_channel_plr'
stage_cdm_table2  = 'tran_orderchannel_hierarchy_plr'
stage_cdm_table3  = 'tran_fulfillmentchannel_hierarchy_plr'

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

delta    = '*' #'sfMax'
truncate = dbutils.widgets.get("truncate")
IDLtype  = dbutils.widgets.get("IDLtype")

# COMMAND ----------

# DBTITLE 1,Get latest data from CDM
if delta == '*':
  loadDate = 0
  df1 = sqlContext.sql("SELECT * FROM {}.{} ".format(databricks,cdm_table1))
  df2 = sqlContext.sql("SELECT * FROM {}.{} ".format(databricks,cdm_table2))
  df3 = sqlContext.sql("SELECT * FROM {}.{} ".format(databricks,cdm_table3))
  
elif delta == 'sfMax':
  # Get current max date in Snowflake table
  #loadDate = curIDL.execute("SELECT MAX(TO_DATE(CAST(CdmLoadDate AS STRING), 'YYYYMMDD')) FROM {}.{}.{}".format(schema, sfTable)).fetchone()[0].strftime('%Y%m%d')
  loadDate = curIDL.execute("SELECT IFNULL(MAX(TO_DATE(CAST(CdmLoadDate AS STRING), 'YYYYMMDDHHMM')),'197008150000') FROM {}.{}.{}".format(database,schema, sfTable)).fetchone()[0].strftime('%Y%m%d%H%M') 
  if loadDate is None:
     loadDate = 0

  df1 = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}' or CAST(CdmUpdateDate AS INT) >= '{}'".format(databricks,cdm_table1, loadDate,loadDate))
  df2 = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}' or CAST(CdmUpdateDate AS INT) >= '{}'".format(databricks,cdm_table2, loadDate,loadDate))
  df3 = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}' or CAST(CdmUpdateDate AS INT) >= '{}'".format(databricks,cdm_table3, loadDate,loadDate))
  print('Snowflake max date in {}.{} is: '.format(schema, sfTable) + str(loadDate))

elif type(delta) == int:
  tz = timezone('EST')
  loadDate = (datetime.now(tz)- timedelta(days=delta)).strftime('%Y%m%d%H%M')
  df1 = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}' or CAST(CdmUpdateDate AS INT) >= '{}'".format(databricks,cdm_table1, loadDate,loadDate))
  df2 = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}' or CAST(CdmUpdateDate AS INT) >= '{}'".format(databricks,cdm_table2, loadDate,loadDate))
  df3 = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}' or CAST(CdmUpdateDate AS INT) >= '{}'".format(databricks,cdm_table3, loadDate,loadDate))

else:
  print('Error: Invalid input for delta !')

if df1:  
  nrows1 = df1.count()
  print('Delta load dataframe length in rows: ' + str(nrows1))
if df2:  
  nrows2 = df2.count()
  print('Delta load dataframe length in rows: ' + str(nrows2))  
if df3:  
  nrows3 = df3.count()
  print('Delta load dataframe length in rows: ' + str(nrows3))

# COMMAND ----------

# DBTITLE 1,Drop duplicates
# df = df.dropDuplicates()
# nrowsDD = df.count()
# print('Delta load dataframe length in rows after dedup: ' + str(nrowsDD))

# if nrows != nrowsDD:
#   print('Duplicates found in delta load - identical rows !')

# COMMAND ----------

# DBTITLE 1,Stage delta load in Snowflake (tran_channel_plr)
if nrows1 > 0:  
  if truncate == 'True':
    mode = "overwrite"
    try:
      curSTAGE.execute("TRUNCATE TABLE {}.{}.{}".format(databaseSTAGE,schemaSTAGE, stage_cdm_table1))
    except: 
      print("Error truncating table {}.{} in Snowflake staging !".format(schemaSTAGE, stage_cdm_table1))
  else:
    mode = "append"
    
  try:
    df1.write \
      .format("snowflake") \
      .options(**connOptionsSTAGE) \
      .option("dbtable",  "{}.{}.{}".format(databaseSTAGE,schemaSTAGE, stage_cdm_table1)) \
      .mode(mode) \
      .save()
    print("{}.{} successfully loaded into Snowflake staging table {}.{}.{} !".format(databricks,cdm_table1,databaseSTAGE,schemaSTAGE, stage_cdm_table1))
  except:
    print("Error loading table {}.{} in Snowflake staging table {} !".format(schemaSTAGE, cdm_table1,stage_cdm_table1))  

else:
  print("No new data in CDM ({})!".format(cdm_table1))

# COMMAND ----------

# DBTITLE 1,tran_orderchannel_hierarchy_plr
if nrows2 > 0:  
  if truncate == 'True':
    mode = "overwrite"
    try:
      curSTAGE.execute("TRUNCATE TABLE {}.{}.{}".format(databaseSTAGE,schemaSTAGE, stage_cdm_table2))
    except: 
      print("Error truncating table {}.{} in Snowflake staging !".format(schemaSTAGE, stage_cdm_table2))
  else:
    mode = "append"
    
  try:
    df2.write \
      .format("snowflake") \
      .options(**connOptionsSTAGE) \
      .option("dbtable",  "{}.{}.{}".format(databaseSTAGE,schemaSTAGE, stage_cdm_table2)) \
      .mode(mode) \
      .save()
    print("{}.{} successfully loaded into Snowflake staging table {}.{}.{} !".format(databricks,cdm_table2,databaseSTAGE,schemaSTAGE, stage_cdm_table2))
  except:
    print("Error loading table {}.{} in Snowflake staging table {} !".format(schemaSTAGE, cdm_table2,stage_cdm_table2))  

else:
  print("No new data in CDM ({})!".format(cdm_table2))

# COMMAND ----------

# DBTITLE 1,tran_fulfillmentchannel_hierarchy_plr
if nrows3 > 0:  
  if truncate == 'True':
    mode = "overwrite"
    try:
      curSTAGE.execute("TRUNCATE TABLE {}.{}.{}".format(databaseSTAGE,schemaSTAGE, stage_cdm_table3))
    except: 
      print("Error truncating table {}.{} in Snowflake staging !".format(schemaSTAGE, stage_cdm_table3))
  else:
    mode = "append"
    
  try:
    df3.write \
      .format("snowflake") \
      .options(**connOptionsSTAGE) \
      .option("dbtable",  "{}.{}.{}".format(databaseSTAGE,schemaSTAGE, stage_cdm_table3)) \
      .mode(mode) \
      .save()
    print("{}.{} successfully loaded into Snowflake staging table {}.{}.{} !".format(databricks,cdm_table3,databaseSTAGE,schemaSTAGE, stage_cdm_table3))
  except:
    print("Error loading table {}.{} in Snowflake staging table {} !".format(schemaSTAGE, cdm_table3,stage_cdm_table3))  

else:
  print("No new data in CDM ({})!".format(cdm_table3))

# COMMAND ----------

nrows = nrows1 + nrows2 + nrows3
merge_condition = f"""
    MERGE INTO {database}.{schema}.{sfTable} SNOW
    USING (select tcp.ChannelId,tcp.OrderChannelName,tcp.FulfillmentChannelName,och3.OrderChannelHierarchyName as OrderChannelHierarchyName_L3,
             och2.OrderChannelHierarchyName as OrderChannelHierarchyName_L2,
             och1.OrderChannelHierarchyName as OrderChannelHierarchyName_L1,
             ffh3.FulfillmentChannelHierarchyName as FulfillmentChannelHierarchyName_L3,
             ffh2.FulfillmentChannelHierarchyName as FulfillmentChannelHierarchyName_L2,
             ffh1.FulfillmentChannelHierarchyName as FulfillmentChannelHierarchyName_L1,
             tcp.Source,
             och3.Source as HierarchySource,
             tcp.BrandId,
             tcp.CDMLoadDate,tcp.CDMUpdateDate
from {databaseSTAGE}.{schemaSTAGE}.{stage_cdm_table1} as tcp
left join {databaseSTAGE}.{schemaSTAGE}.{stage_cdm_table2} as och3 on tcp.MDMOrderChannelHierarchyId = och3.MDMOrderChannelHierarchyId
left join {databaseSTAGE}.{schemaSTAGE}.{stage_cdm_table2} as och2 on och2.MDMOrderChannelHierarchyId = och3.ParentOrderChannelHierarchyId
left join {databaseSTAGE}.{schemaSTAGE}.{stage_cdm_table2} as och1 on och1.MDMOrderChannelHierarchyId = och2.ParentOrderChannelHierarchyId
left join {databaseSTAGE}.{schemaSTAGE}.{stage_cdm_table3} as ffh3 on tcp.MDMFulfillmentChannelHierarchyId = ffh3.MDMFulfillmentChannelHierarchyId
left join {databaseSTAGE}.{schemaSTAGE}.{stage_cdm_table3} as ffh2 on ffh2.MDMFulfillmentChannelHierarchyId = ffh3.ParentFulfillmentChannelHierarchyId
left join {databaseSTAGE}.{schemaSTAGE}.{stage_cdm_table3} as ffh1 on ffh1.MDMFulfillmentChannelHierarchyId = ffh2.ParentFulfillmentChannelHierarchyId) CDM

    ON SNOW.ChannelId = CDM.ChannelId 
    WHEN MATCHED AND 
           ( IFNULL(SNOW.OrderChannelName,'')                   != IFNULL(CDM.OrderChannelName,'')
          OR IFNULL(SNOW.FulfillmentChannelName,'')             != IFNULL(CDM.FulfillmentChannelName,'')
          OR IFNULL(SNOW.OrderChannelHierarchyName_L3,'')       != IFNULL(CDM.OrderChannelHierarchyName_L3,'')
          OR IFNULL(SNOW.OrderChannelHierarchyName_L2,'')       != IFNULL(CDM.OrderChannelHierarchyName_L2,'')
          OR IFNULL(SNOW.OrderChannelHierarchyName_L1,'')       != IFNULL(CDM.OrderChannelHierarchyName_L1,'')
          OR IFNULL(SNOW.FulfillmentChannelHierarchyName_L3,'') != IFNULL(CDM.FulfillmentChannelHierarchyName_L3,'')
          OR IFNULL(SNOW.FulfillmentChannelHierarchyName_L2,'') != IFNULL(CDM.FulfillmentChannelHierarchyName_L2,'')
          OR IFNULL(SNOW.FulfillmentChannelHierarchyName_L1,'') != IFNULL(CDM.FulfillmentChannelHierarchyName_L1,'')
          OR IFNULL(SNOW.Source,'')                             != IFNULL(CDM.Source,'')
          OR IFNULL(SNOW.HierarchySource,'')                     != IFNULL(CDM.HierarchySource,'')
          OR IFNULL(SNOW.BrandId,'')                            != IFNULL(CDM.BrandId,''))
    THEN UPDATE SET 
       SNOW.OrderChannelName                   =  CDM.OrderChannelName
      ,SNOW.FulfillmentChannelName             =  CDM.FulfillmentChannelName
      ,SNOW.OrderChannelHierarchyName_L3       =  CDM.OrderChannelHierarchyName_L3
      ,SNOW.OrderChannelHierarchyName_L2       =  CDM.OrderChannelHierarchyName_L2
      ,SNOW.OrderChannelHierarchyName_L1       =  CDM.OrderChannelHierarchyName_L1
      ,SNOW.FulfillmentChannelHierarchyName_L3 =  CDM.FulfillmentChannelHierarchyName_L3
      ,SNOW.FulfillmentChannelHierarchyName_L2 =  CDM.FulfillmentChannelHierarchyName_L2
      ,SNOW.FulfillmentChannelHierarchyName_L1 =  CDM.FulfillmentChannelHierarchyName_L1
      ,SNOW.Source                             =  CDM.Source
      ,SNOW.HierarchySource                     =  CDM.HierarchySource
      ,SNOW.BrandId                            =  CDM.BrandId
    WHEN NOT MATCHED THEN INSERT (
         ChannelId
        ,OrderChannelName
        ,FulfillmentChannelName
        ,OrderChannelHierarchyName_L3
        ,OrderChannelHierarchyName_L2
        ,OrderChannelHierarchyName_L1
        ,FulfillmentChannelHierarchyName_L3
        ,FulfillmentChannelHierarchyName_L2
        ,FulfillmentChannelHierarchyName_L1
        ,Source
        ,HierarchySource
        ,BrandId
        ,CDMLoadDate
        ,CDMUpdateDate
      )
      VALUES   (
         cdm.ChannelId
        ,cdm.OrderChannelName
        ,cdm.FulfillmentChannelName
        ,cdm.OrderChannelHierarchyName_L3
        ,cdm.OrderChannelHierarchyName_L2
        ,cdm.OrderChannelHierarchyName_L1
        ,cdm.FulfillmentChannelHierarchyName_L3
        ,cdm.FulfillmentChannelHierarchyName_L2
        ,cdm.FulfillmentChannelHierarchyName_L1
        ,cdm.Source
        ,cdm.HierarchySource
        ,cdm.BrandId
        ,cdm.CDMLoadDate
        ,cdm.CDMUpdateDate)"""

# COMMAND ----------

# DBTITLE 1,Merge - Insert Fact
if nrows > 0: 
  if IDLtype == 'Upsert':
    curIDL.execute(merge_condition) 
    print("{}.{}.{} successfully loaded into Snowflake !".format(database,schema,sfTable))    
  elif IDLtype == 'Insert':
    print("{} has got a unique primary key --> do 'merge'".format(sfTable))
    
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

