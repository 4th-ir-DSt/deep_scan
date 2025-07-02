# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("databaseIDL", "POLARIS",
                         ["POLARIS_DEV", "POLARIS_TEST","POLARIS"], "Snowflake IDL database Env ")

dbutils.widgets.dropdown("schemaIDL", "SHDM",
                         ["CUDM", "SADM","OPDM", "SHDM"], "Snowflake IDL schema ")

dbutils.widgets.dropdown("Warehouse", "NON_PROD_ETL_WH",
                         ["NON_PROD_ETL_WH", "LOAD_WH"], "Snowflake IDL Warehouse Env ")

# dbutils.widgets.dropdown("databaseSTAGE", "CDM",
#                          ["CDM"], "Snowflake STAGE database Env ")

# dbutils.widgets.dropdown("schemaSTAGE", "STAGING",
#                          ["STAGING"], "Snowflake STAGE schema ")

dbutils.widgets.dropdown("sfIDLTable", "DIM_DATE_ARBYS",
                         ["DIM_DATE_ARBYS"], "Snowflake table ")

# dbutils.widgets.dropdown("truncate", "True",
#                          ["True", "False"], "Truncate before staging ")

# dbutils.widgets.dropdown("delta", "sfMax",
#                          ["sfMax", "*", "integer"], "Determines length of lookback period ")

# dbutils.widgets.dropdown("IDLtype", "Upsert",
#                          ["Upsert", "Insert"], "Type of IDL ")

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

print('Platform = ', platform.platform())
print('Version of Spark = ', spark.version)
print('Python version = ', sys.version)
spark = SparkSession.builder.getOrCreate()
print('Spark session information = ', spark)

# COMMAND ----------

# DBTITLE 1,Fetch snowflake connection string
# MAGIC %run ../Snowflake/ConnectionString

# COMMAND ----------

# DBTITLE 1,Connection Parameters
# Display connection settings
warehouse = dbutils.widgets.get("Warehouse")
#warehouse = "LARGE_WH"
sfAccount = 'inspire.east-us-2.azure'

# For incremental load (IDL) of data marts
database  = dbutils.widgets.get("databaseIDL")
schema    = dbutils.widgets.get("schemaIDL")
sfTable   = dbutils.widgets.get("sfIDLTable")

connOptionsIDL   = Options(database,schema,warehouse)

print('Connection parameters for incremental load (IDL) of data marts: ')
print(connOptionsIDL)

# COMMAND ----------

# Connection cursors for IDL & STAGING
curIDL  = conn.connect(user=connOptionsIDL['sfUser'],
                   password=connOptionsIDL['sfPassword'],
                   account=sfAccount,
                   warehouse=connOptionsIDL['sfWarehouse'],
                   database=connOptionsIDL['sfDatabase'],
                  ).cursor()

# COMMAND ----------

# DBTITLE 1,Get full data from ADLS
df = sqlContext.read.format('com.databricks.spark.csv')\
              .option("delimiter", '|')\
              .options(header='true',inferschema = 'false')\
              .load("adl://ibue2prod01udpadls1.azuredatalakestore.net/landing/arbys/db/edw/dim_date/batch/processed/")

if df:
  nrows = df.count()
  print('Delta load dataframe length in rows: ' + str(nrows))

# COMMAND ----------

# DBTITLE 1,Stage delta load in Snowflake
if nrows > 0:
  mode = "overwrite"
  curIDL.execute("TRUNCATE TABLE {}.{}".format(schema,sfTable))
  try:
    df.write \
      .format("snowflake") \
      .options(**connOptionsIDL) \
      .option("dbtable",  "{}.{}".format(schema, sfTable)) \
      .mode(mode) \
      .save()
    print("{}.{} successfully loaded into Snowflake !".format(schema, sfTable))
  except:
    print("Error loading table {}.{} in Snowflake !".format(schema, cdm_table))

else:
  print("No new data in ADLS !")