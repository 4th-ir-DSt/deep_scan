# Databricks notebook source
# DBTITLE 1,Remove all of the selection on top
#dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Build all of the drop down Widgets

dbutils.widgets.dropdown("DataBricks", "cdm_qa",
                         ["cdm_qa", "cdm_prod","cdm_dev"], "Databricks IDL database Env ")

dbutils.widgets.dropdown("Warehouse", "LARGE_WH", 
                         ["LARGE_WH", "LOAD_WH"], "Snowflake IDL Warehouse Env ")

dbutils.widgets.dropdown("databaseSTAGE", "CDM_TEST",
                         ["CDM_DEV","CDM_TEST","STAGING","TMP","CDM"], "Snowflake STAGE database Env ")

dbutils.widgets.dropdown("schemaSTAGE", "STAGING", 
                         ["STAGING","TMP"], "Snowflake STAGE schema ")

dbutils.widgets.dropdown("truncate", "True", 
                         ["True", "False"], "Truncate before staging ")

dbutils.widgets.dropdown("delta", "sfMax",
                         ["sfMax", "*", "integer"], "Determines length of lookback period ")

dbutils.widgets.dropdown("brand", "",
                         ["jj", "sonic", "bww", "arbys", ""])

dbutils.widgets.text("table_list", "STR_OPS_SCHEDULEDANDACTUALHOURS_PLR,STR_OPS_LABOR_QTRHR_PLR,STR_OPS_EMPLOYEEPAY_DAY_PLR", "Table List")

dbutils.widgets.text("number_of_days_back", "10", "Number of CdmLoadDate Back")

dbutils.widgets.dropdown("mode", "append",
                         ["append", "overwrite","Load Mode"])


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
# MAGIC %run ../../Snowflake/ConnectionString

# COMMAND ----------

# DBTITLE 1,Connection Parameters
# Display connection settings
databricks = dbutils.widgets.get("DataBricks")
warehouse = dbutils.widgets.get("Warehouse")
sfAccount = 'inspire.east-us-2.azure'

databaseSTAGE = dbutils.widgets.get("databaseSTAGE")
schemaSTAGE = dbutils.widgets.get("schemaSTAGE")
connOptionsSTAGE = Options(databaseSTAGE,schemaSTAGE,warehouse)

print('\n Connection parameters for staging before IDL: ')
print(connOptionsSTAGE)

# COMMAND ----------

# DBTITLE 1,Connection Cursor
curSTAGE = conn.connect(
    user=connOptionsSTAGE['sfUser'],
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
# 'truncate' variable is a boolean:
#   True  : table in staging area on Snowflake is truncated before delta is loaded 
#   False : delta is appended to table in staging area on Snowflake 

#'historyflag' variable inputs:
#   'True' will load data taking inputs from 'historyloadstart' and 'historyloadend' date and load that chunck of data to staging
#   'False' will have no impact on historical data load

delta = dbutils.widgets.get("delta")
truncate = dbutils.widgets.get("truncate")
brand = dbutils.widgets.get("brand")
brand_and_condition = "AND BrandId = '{}'".format(brand) if brand else ""
brand_where_condition = "WHERE BrandId = '{}'".format(brand) if brand else ""
#set this to backload number of days
number_of_days_back = dbutils.widgets.get("number_of_days_back")
mode =  dbutils.widgets.get("mode")

# COMMAND ----------

# DBTITLE 1,List Of CDM Tables need data load to SnowFlake Stage
#cdmtablelist = ["STR_OPS_SCHEDULEDANDACTUALHOURS_PLR","STR_OPS_LABOR_QTRHR_PLR","STR_OPS_EMPLOYEEPAY_DAY_PLR"]
table_list = getArgument("table_list")
cdmtablelist = table_list.replace(" ", "").split(",")
print(cdmtablelist)

# COMMAND ----------

# DBTITLE 1,Get and Load Incremental or Bulk data from Data Bricks to SnowFlake Stage


for tablename in cdmtablelist:
  cdm_table = tablename 
  sfSTAGETable=tablename
  if delta == '*':
    df = sqlContext.sql("""SELECT * FROM {}.{}  {}""".format(databricks, cdm_table, brand_where_condition))
  elif delta == 'sfMax':
    loadDate = curSTAGE.execute("""SELECT IFNULL(MIN(cdmloaddate),'17530101') cdmloaddate 
                                         FROM 
                                             (SELECT DISTINCT TOP {} cdmloaddate 
                                             FROM  {}.{}.{} 
                                             {} 
                                             ORDER BY cdmloaddate DESC)""".format(number_of_days_back, databaseSTAGE, schemaSTAGE, sfSTAGETable, brand_where_condition)).fetchone()[0]
    df = sqlContext.sql("""SELECT * FROM {}.{} 
                             WHERE CdmLoadDate >= '{}' 
                             {}""".format(databricks, cdm_table, loadDate, brand_and_condition))
    print('Snowflake max date in {}.{}.{} is: '.format(databaseSTAGE,schemaSTAGE, sfSTAGETable) + str(loadDate))
  else:
    print('Error: Invalid input for delta !')
        
  if truncate == 'True':
    curSTAGE.execute("TRUNCATE TABLE IF EXISTS {}.{}.{}".format(databaseSTAGE,schemaSTAGE, sfSTAGETable))
    print("Truncate flag is set to True") 
  elif truncate == 'False' and delta == 'sfMax':
    print("Truncate flag is set to False") 
    curSTAGE.execute("""DELETE FROM  {}.{}.{} 
                       WHERE cdmloaddate>='{}' {}""".format(databaseSTAGE, schemaSTAGE, sfSTAGETable,loadDate, brand_and_condition))
    print("delete data from table {}.{}.{} {} since date {}".format(databaseSTAGE, schemaSTAGE, sfSTAGETable, brand_where_condition, loadDate))
  else:
    print("Truncate flag is set to False and no delete data") 
                
  if df.count() > 0:
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
  else:
    print("No new data in CDM !")