# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("DataBricks", "cdm_prod", 
                         ["cdm_dev", "cdm_qa","cdm_prod"], "Databricks IDL database Env ")

dbutils.widgets.dropdown("databaseSTAGE", "CDM", 
                         ["CDM_DEV","CDM_TEST", "CDM"], "Snowflake STAGE database Env ")

dbutils.widgets.dropdown("schemaSTAGE", "STAGING", 
                         ["STAGING"], "Snowflake STAGE schema ")

dbutils.widgets.dropdown("sfIDLTable", "cust_address_plr", 
                         ["cust_address_plr"], "Snowflake table ")

dbutils.widgets.dropdown("truncate", "True", 
                         ["True", "False"], "Truncate before staging ")

dbutils.widgets.dropdown("delta", "*", 
                          ["sfMax", "*", "integer"], "Determines length of lookback period ")

#dbutils.widgets.dropdown("IDLtype", "Insert", 
#                         ["Upsert", "Insert"], "Type of IDL ")

dbutils.widgets.dropdown("sfLogSchema", "AUDIT", 
                         ["AUDIT"], "Log Schema")

dbutils.widgets.dropdown("sfLogTable", "PIPELINE_EXECUTION_LOG", 
                         ["PIPELINE_EXECUTION_LOG"], "Log Table")

# COMMAND ----------

# In case cluster doesn't have Snowflake connector (!)
dbutils.library.installPyPI("snowflake-connector-python")
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import platform, sys, os
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col, lit, to_timestamp, concat
import pyspark

import snowflake.connector as conn
from datetime import datetime
from pytz import timezone

print('Platform = ',platform.platform())  
print('Version of Spark = ',spark.version)
print('Python version = ',sys.version)
spark = SparkSession.builder.getOrCreate()
print('Spark session information = ', spark)

# COMMAND ----------

# DBTITLE 1,Execution Log Parameters
# Table for storing logs 
schemaLog  = dbutils.widgets.get("sfLogSchema")
sfLogTable = dbutils.widgets.get("sfLogTable")

# Execution ID = Name of Notebook + Timestamp with Millisecond + Random Number
nbName   = "Prod_IDL_CUST_ADDRESS_PLR"
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

# Display connection settings
warehouse = "PROD_ETL_WH"
sfAccount = 'inspire.east-us-2.azure'


#Source CDM table
databricks = dbutils.widgets.get("DataBricks")
cdm_table  = 'cust_address_plr'

# For incremental load (IDL) of staging area
sfTable   = dbutils.widgets.get("sfIDLTable")

# For staging before IDL
databaseSTAGE  = dbutils.widgets.get("databaseSTAGE")
schemaSTAGE    = dbutils.widgets.get("schemaSTAGE")

# For Auditing information logging in Polaris
database  = "Polaris"
schema    = "Audit"

connOptionsIDL   = Options(database,schema,warehouse)
connOptionsSTAGE = Options(databaseSTAGE,schemaSTAGE,warehouse)

print('\n Connection parameters for staging before IDL: ')
print(connOptionsSTAGE)

# COMMAND ----------

# Connection cursors for IDL in STAGING

curSTAGE = conn.connect(user=connOptionsSTAGE['sfUser'], 
                   password=connOptionsSTAGE['sfPassword'], 
                   account=sfAccount,
                   warehouse=connOptionsSTAGE['sfWarehouse'], 
                   database=connOptionsSTAGE['sfDatabase'], 
                  ).cursor()

# COMMAND ----------

curIDL  = conn.connect(user=connOptionsIDL['sfUser'], 
                   password=connOptionsIDL['sfPassword'], 
                   account=sfAccount,
                   warehouse=connOptionsIDL['sfWarehouse'], 
                   database=connOptionsIDL['sfDatabase'], 
                  ).cursor()

# COMMAND ----------

# DBTITLE 1,Start Logging
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
log = "Started Notebook {} IDL Job for {}.{} on {}".format(nbName, schema, sfTable, tmpstamp)
executionLogger(curIDL, database, schemaLog, sfLogTable, execId, log, tmpstamp)

# COMMAND ----------

# DBTITLE 1,Determine length & type of delta load
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

delta    = dbutils.widgets.get("delta")
# delta    = '*'
truncate = dbutils.widgets.get("truncate")
#IDLtype  = dbutils.widgets.get("IDLtype")

# COMMAND ----------

# DBTITLE 1,Get latest data from CDM
if delta == '*':
  loadDate = 0
  df = sqlContext.sql("SELECT * FROM {}.{} ".format(databricks,cdm_table))
  
elif delta == 'sfMax':
  # Get current max date in Snowflake table
  loadDate = curSTAGE.execute("SELECT MAX(CAST(CdmLoadDate AS INT)) FROM {}.{}.{}".format(databaseSTAGE,schemaSTAGE, sfTable)).fetchone()[0]
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) > '{}'".format(databricks,cdm_table, loadDate))
  print('Snowflake max date in {}.{}.{} is: '.format(databaseSTAGE, schemaSTAGE, sfTable) + str(loadDate))

elif type(delta) == int:
  tz = timezone('EST')
  loadDate = (datetime.now(tz)- timedelta(days=delta)).strftime('%Y%m%d')
  df = sqlContext.sql("SELECT * FROM {}.{}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(databricks,cdm_table, loadDate))

else:
  print('Error: Invalid input for delta !')

if df:  
  nrows = df.count()
  print('Delta load dataframe length in rows: ' + str(nrows))

# COMMAND ----------

# DBTITLE 1,Add auditing columns
tz = timezone('EST')
df = df.withColumn('LoadID',  lit(int(datetime.now(tz).strftime('%Y%m%d'))))
df = df.withColumn('LoadDttm',  lit(datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S.%s')))

# COMMAND ----------

# DBTITLE 1,Data quality - Number of duplicates
df = df.dropDuplicates()
nrowsDD = df.count()
print('Delta load dataframe length in rows after dedup: ' + str(nrowsDD))

if nrows != nrowsDD:
  print('Duplicates found in delta load - identical rows !')

# COMMAND ----------

# DBTITLE 1,Stage delta load in Snowflake
if nrowsDD > 0:  
  if truncate == 'True':
    mode = "overwrite"
    try:
      curSTAGE.execute("TRUNCATE TABLE {}.{}.{}".format(databaseSTAGE,schemaSTAGE, sfTable))      
    except: 
      print("Error truncating table {}.{}.{} in Snowflake !".format(databaseSTAGE,schemaSTAGE, sfTable)) 
      
  else:
    mode = "append"
    
  try:
    df.write \
      .format("snowflake") \
      .options(**connOptionsSTAGE) \
      .option("dbtable",  "{}.{}.{}".format(databaseSTAGE,schemaSTAGE, sfTable)) \
      .mode(mode) \
      .save()
    print("{}.{}.{} successfully loaded into Snowflake !".format(databaseSTAGE, schemaSTAGE, sfTable))
  except:
    print("Error loading table {}.{}.{} in Snowflake !".format(databaseSTAGE, schemaSTAGE, sfTable))
    raise
  
else:
  print("No new data in CDM !")

# COMMAND ----------

# DBTITLE 1,End Logging
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
log = "End of Notebook {} IDL Job for {}.{} on {}".format(nbName, schema, sfTable, tmpstamp)
executionLogger(curIDL, database, schemaLog, sfLogTable, execId, log, tmpstamp)