# Databricks notebook source
# DBTITLE 1,Remove all of the selection on top
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Build all of the drop down Widgets

dbutils.widgets.dropdown("DataBricks", "cdm_prod", 
                         ["cdm_qa", "cdm_prod","cdm_dev"], "Databricks IDL database Env ")
dbutils.widgets.dropdown("DataBricksTable", "customer_enriched_plr", 
                         ["customer_enriched_plr", "site_development_plr","str_ops_salesforecast_qtrhr_plr"], "Databricks IDL table ")

dbutils.widgets.dropdown("Warehouse", "PROD_ETL_WH", 
                         ["PROD_ETL_WH", "LOAD_WH"], "Snowflake IDL Warehouse Env ")

dbutils.widgets.dropdown("databaseSTAGE", "CDM", 
                         ["CDM","STAGING","TMP"], "Snowflake STAGE database Env ")
dbutils.widgets.dropdown("schemaSTAGE", "STAGING", 
                         ["STAGING","TMP"], "Snowflake STAGE schema ")
dbutils.widgets.dropdown("sfSTAGETable", "customer_enriched_plr", 
                         ["customer_enriched_plr"], "Snowflake STAGE table ")

dbutils.widgets.dropdown("databaseIDL", "POLARIS_TEST", 
                         ["POLARIS_DEV", "POLARIS_TEST","POLARIS","CDM","ADLS"], "Snowflake IDL database Env ")
dbutils.widgets.dropdown("schemaIDL", "CUDM", 
                         ["CUDM", "SADM","OPDM", "SHDM","STAGING","TMP"], "Snowflake IDL schema ")
dbutils.widgets.dropdown("sfIDLTable", "CUSTOMER_ENRICHMENT_DIM", 
                         ["CUSTOMER_ENRICHMENT_DIM"], "Snowflake Base table ")


dbutils.widgets.dropdown("truncate", "True", 
                         ["True", "False"], "Truncate before staging ")

dbutils.widgets.dropdown("delta", "sfMax", 
                         ["sfMax", "*", "integer"], "Determines length of lookback period ")
#if History flag is set to False, history start or end date will not have any impact
dbutils.widgets.dropdown("historyflag", "False", 
                         ["False", "True"], "History Flag ")
dbutils.widgets.dropdown("historyloadstart", "20200118", 
                         ["17530101","20200118","20190101", "20190201", "20190301"], "History Load Start Date ")
dbutils.widgets.dropdown("historyloadend", "20200118", 
                         ["17530101","20200118","20190110", "20190210", "20190310"], "History Load End Date ")


dbutils.widgets.dropdown("IDLtype", "Upsert", 
                         ["Upsert", "Insert"], "Type of IDL ")

# COMMAND ----------

# DBTITLE 1,Snowflake connector
# In case cluster doesn't have Snowflake connector (!)
dbutils.library.installPyPI("snowflake-connector-python")
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
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

print('Platform = ', platform.platform())  
print('Version of Spark = ', spark.version)
print('Python version = ', sys.version)
spark = SparkSession.builder.getOrCreate()
print('Spark session information = ', spark)

# COMMAND ----------

# DBTITLE 1,Fetch Snow Flake Connection 
# MAGIC 
# MAGIC %run ../Connection/ConnectionString

# COMMAND ----------

#%run /Users/rnandi@inspirebrands.com/LoadToSnowflake/Connection/ConnectionString

# COMMAND ----------

# DBTITLE 1,Connection Parameters
# Display connection settings
databricks = dbutils.widgets.get("DataBricks")
cdm_table = dbutils.widgets.get("DataBricksTable")
warehouse = dbutils.widgets.get("Warehouse")
sfAccount = 'inspire.east-us-2.azure'

# For incremental load (IDL) of data marts (Base)
database  = dbutils.widgets.get("databaseIDL")
schema    = dbutils.widgets.get("schemaIDL")
sfTable   = dbutils.widgets.get("sfIDLTable")

# For staging before IDL(Landing)
databaseSTAGE  = dbutils.widgets.get("databaseSTAGE")
schemaSTAGE    = dbutils.widgets.get("schemaSTAGE")
sfSTAGETable   = dbutils.widgets.get("sfSTAGETable")

#cdm_table  = 'site_development_plr'


connOptionsIDL   = Options(database,schema,warehouse)
connOptionsSTAGE = Options(databaseSTAGE,schemaSTAGE,warehouse)

print('Connection parameters for incremental load (IDL) of data marts: ')
print(connOptionsIDL)

print('\n Connection parameters for staging before IDL: ')
print(connOptionsSTAGE)

# COMMAND ----------

# DBTITLE 1,Connection Cursor
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

# DBTITLE 1,Determine length, type of delta load
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
#   'upsert'  : use merge into statement -> update & insert based on primary key (pk)
#   'insert' : use insert statement     -> only insert new records (used for fact data where no pk)

#'historyflag' variable inputs:
#   'True' will load data taking inputs from 'historyloadstart' and 'historyloadend' date and load that chunck of data to staging
#   'False' will have no impact on historical data load

delta    = dbutils.widgets.get("delta")
#delta = 10
truncate = dbutils.widgets.get("truncate")
IDLtype  = dbutils.widgets.get("IDLtype")
historyflag = dbutils.widgets.get("historyflag")
historyloadstart = dbutils.widgets.get("historyloadstart")
historyloadend = dbutils.widgets.get("historyloadend")


# COMMAND ----------

# DBTITLE 1,Get latest data from CDM
if historyflag=='True':
  df = sqlContext.sql("SELECT * FROM {}.{} where  TO_DATE(CdmLoadDate,'yyyy-MM-dd') between TO_DATE('{}','yyyyMMdd') and TO_DATE('{}','yyyyMMdd')".format(databricks,cdm_table,historyloadstart,historyloadend))
  #print('I am inside historyflag '+str(historyflag)+' '+str(historyloadstart) + ' '+str(historyloadend))
else:

  if delta == '*':
    #df = sqlContext.sql("SELECT * FROM {}.{} limit 10".format(databricks,cdm_table))
    df = sqlContext.sql("SELECT * FROM {}.{}".format(databricks,cdm_table))

  elif delta == 'sfMax':
    # Get current max date in Snowflake table
    loadDate = curIDL.execute("SELECT IFNULL(MAX(TO_DATE(CAST(CDMLOADDATE AS STRING), 'yyyy-MM-dd')),'1753-01-01') FROM {}.{}.{}".format(database,schema, sfTable)).fetchone()[0].strftime('%Y-%m-%d')
    #df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}' AND CAST(CdmLoadDate AS INT)<> '{}'".format(databricks,cdm_table, loadDate,'20200122'))
    df = sqlContext.sql("SELECT * FROM {}.{} WHERE TO_DATE(CdmLoadDate,'yyyy-MM-dd') > '{}'".format(databricks,cdm_table, loadDate))
    #print('Snowflake max date in {}.{} is: '.format(schema, sfTable) + str(loadDate))
    print('Snowflake max date in {}.{} is: '.format(schemaSTAGE, sfSTAGETable) + str(loadDate))

  elif type(delta) == int:
    tz = timezone('EST')
    loadDate = (datetime.now(tz)- timedelta(days=delta)).strftime('%Y-%m-%d')
    df = sqlContext.sql("SELECT * FROM {}.{} WHERE TO_DATE(CdmLoadDate,'yyyy-MM-dd') >= '{}'".format(databricks,cdm_table, loadDate))

  else:
    print('Error: Invalid input for delta !')

if df:  
  nrows = df.count()
  print('Delta load in {} dataframe length in rows: '.format(cdm_table) + str(nrows))
  #print(f'Delta load in {cdm_table} dataframe length in rows: {str(nrows)} ' )
  

# COMMAND ----------

# DBTITLE 1,Drop duplicates
#df = df.dropDuplicates()
#nrowsDD = df.count()
#print('Delta load dataframe length in rows after dedup: ' + str(nrowsDD))
#
#if nrows != nrowsDD:
#  print('Duplicates found in delta load - identical rows !')

# COMMAND ----------

# DBTITLE 1,Stage source delta data to Snowflake
if df.count() > 0:  
  if truncate=='True':
    try:
      curSTAGE.execute("TRUNCATE TABLE IF EXISTS {}.{}.{}".format(databaseSTAGE,schemaSTAGE, sfSTAGETable))
      mode = "overwrite"
    except: 
      print("Error truncating table {}.{}.{} in Snowflake !".format(databaseSTAGE,schemaSTAGE, sfSTAGETable))
      raise
  else:
    if historyflag=='True':      
        curSTAGE.execute("DELETE FROM {}.{}.{} WHERE TO_DATE(CdmLoadDate,'yyyy-MM-dd') between TO_DATE('{}','yyyyMMdd') and TO_DATE('{}','yyyyMMdd') ".format(databaseSTAGE,schemaSTAGE, sfSTAGETable,historyloadstart,historyloadend))
        mode = "append"
    else:
      mode = "append"
    
  try:
    df.write \
      .format("snowflake") \
      .options(**connOptionsSTAGE) \
      .option("dbtable",  "{}.{}.{}".format(databaseSTAGE,schemaSTAGE, sfSTAGETable)) \
      .mode(mode) \
      .save()
    print("{}.{}.{} successfully loaded into Snowflake !".format(databaseSTAGE,schemaSTAGE, sfSTAGETable))
  except:
    print("Error loading table {}.{}.{} in Snowflake !".format(databaseSTAGE,schemaSTAGE, sfSTAGETable))  
    raise

else:
  print("No new data in CDM !")

# COMMAND ----------

