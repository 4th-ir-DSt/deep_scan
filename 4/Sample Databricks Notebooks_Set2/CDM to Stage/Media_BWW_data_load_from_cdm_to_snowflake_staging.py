# Databricks notebook source
# MAGIC %md
# MAGIC # Media_BWW_data_load_from_cdm_to_snowflake_staging
# MAGIC * __*Author(s) :*__ Razvan Stefanita, Jaheer Mohammed
# MAGIC - __*email:* __ rstefanita@inspirebrands.com, jmohammed@inspirebrands.com
# MAGIC * __*Version :*__ 0.0.2
# MAGIC * __*Date :*__ May 07, 2021
# MAGIC * __*Description :*__ Media_Data_Load_from_CDM_to_Snowflake
# MAGIC * __*Update History :* __Fixed ConnectionString path
# MAGIC * __*Update History :*__ Nov 16 (Jaheer Mohammed): Modified the warehouse name and connection string name
# MAGIC * __*Update History :*__ Dec 1 (Jaheer Mohammed): Updated the connection string details and also modified the warehouse name
# MAGIC * __*Update History :*__ Dec 30 (Jaheer Mohammed): Updated the connection string by adding the SNowflake options code to define user and role from env widget
# MAGIC * __*Update History :*__ Dec 30 (Jaheer Mohammed): Added the UAT Widgets, UAT and PROD Warehouse widgets
# MAGIC ***
# MAGIC ## Description
# MAGIC This workbook is part of loading the data from CDM tables to SnowFlake staging tables for both media and external tables

# COMMAND ----------

# MAGIC %md ### Import Functions and Widgets

# COMMAND ----------

# DBTITLE 1,SnowFlake Connector
# In case cluster doesn't have Snowflake connector (!)
dbutils.library.installPyPI("snowflake-connector-python")
dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import explode
from datetime import datetime, date, timedelta, time
from pyspark.sql import Row, Window
from pyspark.sql.functions import lit, concat, date_trunc, when, col, udf, unix_timestamp, to_date, max, sum, lag, to_timestamp, coalesce
import platform, sys, os
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql import functions as F
import pyspark

import snowflake.connector as conn
from datetime import datetime
from pytz import timezone

dbutils.widgets.dropdown("env","dev",["dev", "qa", "uat", "prod"], "Deployment env")

dbutils.widgets.dropdown("DataBricks_Env", "cdm_dev", 
                         ["cdm_dev", "cdm_qa", "cdm_uat", "cdm_prod"], "Databricks Database Env ")

dbutils.widgets.dropdown("SnowFlake_Stg_DB", "CDM_DEV", 
                         ["CDM_DEV", "CDM_TEST", "CDM_UAT", "CDM"], "Snowflake STAGE database Env ")

dbutils.widgets.dropdown("SnowFlake_WareHouse", "IRB_PRCSSA_IRB_WH", 
                         ["IRB_PRCSSA_IRB_WH", "IRB_PRCSSA_IRB_UAT_WH", "IRB_PRCSSA_IRB_PROD_WH"], "Snowflake Warehouse")

dbutils.widgets.dropdown("SnowFlake_Schema", "STAGING",
                        ["STAGING"], "Snowflake Schema")

env = getArgument("env")
db_env = getArgument("DataBricks_Env")
snowflake_db_env = getArgument("SnowFlake_Stg_DB")
sflake_schema = getArgument("SnowFlake_Schema")
sflake_wh = getArgument("SnowFlake_WareHouse")


media_tables = ["media_channel_plr", "media_strategytype_plr", "media_channeltype_plr", "media_spendtype_plr", "media_partner_plr", "media_campaign_plr", "media_strategy_plr",  "media_campaigndetail_plr", "media_spend_week_plr",  "media_tracker_week_plr"]

print("Environment: " +  env)
print("DB_Environment: " +  db_env)
print("SnowFlake Staging Database:  " + snowflake_db_env)
print("SnowFlake_Schema:  " + sflake_schema)
print("SnowFlake_WareHouse:  " + sflake_wh)

# COMMAND ----------

# # Use secret manager to get the login name and password for the Snowflake user
# # user = dbutils.secrets.get(scope="<scope>", key="<username key>")
# # password = dbutils.secrets.get(scope="<scope>", key="<password key>")

if env == "dev":
  user = "IRB_PRCSSA"
  role = "IRB_OBJOWNR_Dev_Role"
  password = dbutils.secrets.get(scope = "udp-keyvault-databricks", key = "snowflake-IRB-PRCSSA")
  
elif env == "qa":
  user = "IRB_PRCSSA"
  role = "IRB_OBJOWNR_QA_Role"
  password = dbutils.secrets.get(scope = "udp-keyvault-databricks", key = "snowflake-IRB-PRCSSA")

elif env == "uat":
  user = "IRB_PRCSSA_UAT"
  role = "IRB_OBJOWNR_UAT_Role"
  password = dbutils.secrets.get(scope = "udp-keyvault-databricks", key = "snowflake-IRB-PRCSSA-UAT")

elif env == "prod":
  user = "IRB_PRCSSA_PROD"
  role = "IRB_OBJOWNR_PROD_Role"
  password = dbutils.secrets.get(scope = "udp-keyvault-databricks", key = "snowflake-IRB-PRCSSA-PROD")

# Snowflake connection options

# -- Static way
# database = "POLARIS_DEV"
# schema = "CUDM"
# options = dict(sfUrl="inspire.east-us-2.azure.snowflakecomputing.com",
#                sfUser= user,
#                sfPassword= password,
#                sfDatabase= database,
#                sfSchema= schema,
#                sfWarehouse="LOAD_WH")

# -- Dynamic way
def Options(database, schema, warehouse):
  options = dict(sfUrl= "inspire.east-us-2.azure.snowflakecomputing.com",
               sfUser= user,
               sfPassword= password,
               sfRole = role,
               sfDatabase= database,
               sfSchema= schema,
               sfWarehouse= warehouse)
  return options

print(user)
print(role)

# COMMAND ----------

# DBTITLE 1,Functions to Read/Write from ADLS to SnowFlake
def read_adls_landing(path, table_name, delimiter='|',ifs='false'):
    try:
        df = sqlContext.read.format('com.databricks.spark.csv')\
              .option("delimiter", delimiter)\
              .options(header='true',inferschema = ifs)\
              .load(path)
        return df
    except Exception as e:
        print(build_exception_message("reading", table_name, e))
    
def getSnowflakeOptions(snowflake_database, snowflake_schema, snowflake_warehouse):

    # snowflake connection options
    options = {
      "sfUrl" : "inspire.east-us-2.azure.snowflakecomputing.com/",
      "sfUser" : user,
      "sfRole" : role,
      "sfPassword" : password,
      "sfDatabase" : snowflake_database,
      "sfSchema" : snowflake_schema,
      "sfWarehouse" : snowflake_warehouse
    }

    return options
  
def writeToSnowflake(data_frame, snowflake_table, snowflake_options):
        data_frame.write\
        .mode('overwrite')\
        .format("snowflake")\
        .options(**snowflake_options)\
        .option("dbtable", snowflake_table)\
        .save()


def load_data(src_env, sflake_options, sflake_env, sflakeSchema, tblName):
  df = spark.sql("select * from "+src_env+"."+tblName)
  return df

# COMMAND ----------

options = getSnowflakeOptions(snowflake_db_env, sflake_schema, sflake_wh)
table_list = media_tables   
for table in table_list:
  print(table)
  df_media = load_data(db_env, options, snowflake_db_env, sflake_schema, table)
  writeToSnowflake(df_media, table, options)