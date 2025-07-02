# Databricks notebook source
# DBTITLE 1,Remove all of the selection on top
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Build all of the drop down Widgets

dbutils.widgets.dropdown("DataBricks", "cdm_prod", 
                         ["cdm_qa", "cdm_prod","cdm_dev"], "Databricks IDL database Env ")
dbutils.widgets.dropdown("DataBricksTable", "EXT_WEATHER_HISTORY_PLR", 
                         ["EXT_WEATHER_HISTORY_PLR"], "Databricks IDL table ")

dbutils.widgets.dropdown("Warehouse", "non_prod_etl_wh", 
                         ["non_prod_etl_wh"], "Snowflake IDL Warehouse Env ")

dbutils.widgets.dropdown("databaseSTAGE", "CDM", 
                         ["CDM_TEST","STAGING","TMP","CDM"], "Snowflake STAGE database Env ")
dbutils.widgets.dropdown("schemaSTAGE", "STAGING", 
                         ["STAGING","TMP"], "Snowflake STAGE schema ")
dbutils.widgets.dropdown("sfSTAGETable", "EXT_WEATHER_HISTORY_PLR", 
                         ["EXT_WEATHER_HISTORY_PLR"], "Snowflake STAGE table ")

dbutils.widgets.dropdown("databaseIDL", "POLARIS", 
                         ["POLARIS_DEV", "POLARIS_TEST","POLARIS","CDM","ADLS"], "Snowflake IDL database Env ")
dbutils.widgets.dropdown("schemaIDL", "SHDM", 
                         ["CUDM", "SADM","OPDM", "SHDM","STAGING","TMP"], "Snowflake IDL schema ")
dbutils.widgets.dropdown("sfIDLTable", "WEATHER_HOURLY_FACT", 
                         ["WEATHER_HOURLY_FACT","location_hierarchy"], "Snowflake Base table ")

dbutils.widgets.dropdown("truncate", "False", 
                         ["True", "False"], "Truncate before staging ")

dbutils.widgets.dropdown("delta", "sfMax", 
                         ["sfMax", "*", "integer"], "Determines length of lookback period ")
#if History flag is set to False, history start or end date will not have any impact
dbutils.widgets.dropdown("historyflag", "False", 
                         ["False", "True"], "History Flag ")
dbutils.widgets.dropdown("historyloadstart", "20200330", 
                         ["17530101","20200330","20190101", "20190201", "20190301"], "History Load Start Date ")
dbutils.widgets.dropdown("historyloadend", "20200330", 
                         ["17530101","20200330","20190110", "20190210", "20190310"], "History Load End Date ")


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
#set this to backload number of days
load_numberofdaysback=2

if historyflag=='True':
  df = sqlContext.sql("SELECT * FROM {}.{} where CdmLoadDate  between '{}' and '{}'".format(databricks,cdm_table,historyloadstart,historyloadend))
  #print('I am inside historyflag '+str(historyflag)+' '+str(historyloadstart) + ' '+str(historyloadend))
else:
  isTableExists=curSTAGE.execute("select count(1) from {}.INFORMATION_SCHEMA.tables where table_type = 'BASE TABLE' and upper(table_name) = upper('{}')".format(databaseSTAGE,sfSTAGETable)).fetchone()[0]
  print(isTableExists)
  if isTableExists==0:
      delta = '*'
      
  if delta == '*':
    #df = sqlContext.sql("SELECT * FROM {}.{} limit 10".format(databricks,cdm_table))
    df = sqlContext.sql("SELECT * FROM {}.{}".format(databricks,cdm_table))
    

  elif delta == 'sfMax':
    # Get current max date in Snowflake table
    
  
    loadDate = curIDL.execute("select IFNULL(min(cdmloaddate),'17530101') cdmloaddate from (select distinct top {} cdmloaddate from {}.{}.{} order by cdmloaddate desc)".format(load_numberofdaysback,databaseSTAGE
    ,schemaSTAGE, sfSTAGETable)).fetchone()[0]
    print(loadDate)
    curIDL.execute("delete from  {}.{}.{} where cdmloaddate>='{}'".format(databaseSTAGE,schemaSTAGE, sfSTAGETable,loadDate))
    #df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) > '{}'".format(databricks,cdm_table, loadDate))
    print("delete data from table {}.{}.{} since date {}".format(databaseSTAGE,schemaSTAGE, sfSTAGETable,loadDate))
    print(datetime.now())
    df = sqlContext.sql("SELECT * FROM {}.{} WHERE CdmLoadDate >= '{}'".format(databricks,cdm_table, loadDate)) 
    
    print('Snowflake max date in {}.{}.{} is: '.format(databaseSTAGE,schemaSTAGE, sfSTAGETable) + str(loadDate))


  else:
    print('Error: Invalid input for delta !')

if df:  
  nrows = df.count()
  print('Delta load from Data Bricks CDM.{} dataframe length in rows: '.format(cdm_table) + str(nrows))
  #print(f'Delta load in {cdm_table} dataframe length in rows: {str(nrows)} ' )
  

# COMMAND ----------

# DBTITLE 1,Stage source delta data to Snowflake
#if df.count() > 0:
if nrows > 0:
  if truncate=='True':
    try:
      curSTAGE.execute("TRUNCATE TABLE IF EXISTS {}.{}.{}".format(databaseSTAGE,schemaSTAGE, sfSTAGETable))
      mode = "overwrite"
    except: 
      print("Error truncating table {}.{}.{} in Snowflake !".format(databaseSTAGE,schemaSTAGE, sfSTAGETable))
      raise
  else:
    if historyflag=='True':      
        #curSTAGE.execute("DELETE FROM {}.{}.{} WHERE CAST(CdmLoadDate AS INT) between '{}' and '{}' ".format(databaseSTAGE,schemaSTAGE, sfSTAGETable,historyloadstart,historyloadend))
        curSTAGE.execute("DELETE FROM {}.{}.{} WHERE CdmLoadDate between '{}' and '{}' ".format(databaseSTAGE,schemaSTAGE, sfSTAGETable,historyloadstart,historyloadend))
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

