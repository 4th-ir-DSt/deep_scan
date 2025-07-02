# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("databaseIDL", "POLARIS", 
                         ["POLARIS_DEV", "POLARIS_TEST","POLARIS"], "Snowflake IDL database Env ")

dbutils.widgets.dropdown("schemaIDL", "CUDM", 
                         ["CUDM", "SADM","OPDM", "SHDM"], "Snowflake IDL schema ")

dbutils.widgets.dropdown("sfIDLTable", "ROLL_VISIT_COUNT_BWW", 
                         ["ROLL_VISIT_COUNT_BWW"], "Snowflake table ")

dbutils.widgets.dropdown("sfLogSchema", "AUDIT", 
                         ["AUDIT"], "Log Schema")

dbutils.widgets.dropdown("sfLogTable", "PIPELINE_EXECUTION_LOG", 
                         ["PIPELINE_EXECUTION_LOG"], "Log Table")

# dbutils.widgets.dropdown("truncate", "True", 
#                          ["True", "False"], "Truncate before staging ")

# dbutils.widgets.dropdown("IDLtype", "Upsert", 
#                          ["Upsert", "Insert"], "Type of IDL ")

dbutils.widgets.dropdown("sfLogSchema", "AUDIT", 
                         ["AUDIT"], "Log Schema")

dbutils.widgets.dropdown("sfLogTable", "PIPELINE_EXECUTION_LOG", 
                         ["PIPELINE_EXECUTION_LOG"], "Log Table")

# COMMAND ----------

dbutils.library.installPyPI("snowflake-connector-python")
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import platform, sys, os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from datetime import datetime, date, timedelta
import snowflake.connector as conn
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.types import IntegerType, DateType
import pyspark

import numpy as np
from pytz import timezone

print('Platform = ',platform.platform())  
print('Version of Spark = ',spark.version)
print('Python version = ',sys.version)
spark = SparkSession.builder.getOrCreate()
print('Spark session information = ', spark)

# COMMAND ----------

# DBTITLE 1,Fetch Execution Logger
# MAGIC %run ../Governance/ExecutionLogger

# COMMAND ----------

# DBTITLE 1,Execution Log Parameters
# Table for storing logs 
schemaLog  = dbutils.widgets.get("sfLogSchema")
sfLogTable = dbutils.widgets.get("sfLogTable")

# Execution ID = Name of Notebook + Timestamp with Millisecond + Random Number
nbName   = "Fact_IDL_ROLL_VISIT_COUNT_BWW"
tz       = timezone('EST')
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
rnumber  = int(np.random.rand() * 1000000)

execId   = nbName + "_" + tmpstamp + str(rnumber)

# COMMAND ----------

# DBTITLE 1,Fetch snowflake connection string 
# MAGIC %run ../Snowflake/ConnectionString

# COMMAND ----------

# Display connection settings
warehouse = 'PROD_ETL_WH'
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

# DBTITLE 1,Start Logging
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
log = "Started Notebook {} IDL Job for {}.{} on {}".format(nbName, schema, sfTable, tmpstamp)
executionLogger(curIDL, database, schemaLog, sfLogTable, execId, log, tmpstamp)

# COMMAND ----------

# DBTITLE 1,Update ROLL_VISIT_COUNT_BWW
try:
  curIDL.execute("""
  CREATE OR REPLACE TABLE {}.{}.{} AS

  WITH
  CUSTOMERS AS(
  SELECT distinct profile_id FROM (
    SELECT distinct profile_id
    FROM "{}"."CUDM"."CAMPAIGN_AUDIENCE_LOG"
      UNION
    SELECT distinct profile_id
    FROM "{}"."CUDM"."TM_CAMPAIGN_AUDIENCE_LOG")
  ),

  MISSING_PROFILES AS(
  SELECT profile_id
  FROM "{}"."CUDM"."CUSTOMER_DIM" dim
  RIGHT JOIN CUSTOMERS c
      on c.profile_id= customerid
  where customerid is null
  ),

  ALL_PROFILE_IDS AS(
  SELECT distinct profile_id FROM(
    SELECT distinct profile_id
    FROM MISSING_PROFILES
    UNION
    SELECT distinct customerid as profile_id
    FROM "{}"."CUDM"."CUSTOMER_DIM"
    where brandid = 'bww')
  ),

  //10,608,777
  PROFILE_LENGTH AS(
  SELECT DISTINCT profile_id, enrollstartdate, DATEDIFF(day,enrollstartdate::date,current_date::date) as Membershiplength
  FROM ALL_PROFILE_IDS
      LEFT JOIN {}."CUDM"."CUSTOMER_DIM"
  on profile_id = customerid
  where profile_id is not null
  AND   (enrollstartdate IS NOT NULL AND enrollstartdate != '')
  ),

  No_Start_Profiles as(
  //2,500,085
  SELECT distinct AP.profile_id, pl.enrollstartdate, membershiplength
  FROM Profile_length pl
      RIGHT JOIN ALL_PROFILE_IDS ap
  on ap.profile_id = pl.profile_id
  where pl.enrollstartdate is null or pl.enrollstartdate = ''
  ),

  ALL_DATES as(
  SELECT date as report_week, year, month, dayofweek, weekofyear
  FROM {}."SHDM"."DATE_DIM"
  where dayofweek = 0
  and year >=2016
  and date <= Current_date
  ),

  ALL_REPORT_WEEKS AS(
  SELECT *
  FROM PROFILE_LENGTH
  Cross JOIN All_Dates
  ),

  ELIMINATE_DATES AS(
  SELECT *
  FROM All_Report_weeks
  where report_week>= enrollstartdate
  ),

  NO_Start_Weeks as(
  SELECT *
  FROM No_Start_Profiles
  Cross JOIN All_Dates
  ),

  PROFILE_DATE AS(
  SELECT *, dateadd(day,-365,report_week:: date) as Report_week_start
  FROM Eliminate_dates
  UNION
  SELECT *, dateadd(day,-365,report_week:: date) as Report_week_start
  FROM No_Start_Weeks
  ), 

  TRX_DATE AS(
  SELECT tv.*, date as business_date_2
  FROM "{}"."CUDM"."combined_transactions_view" tv
      LEFT JOIN {}."SHDM"."DATE_DIM" dd
  on date_id = businessdate
  ),

  DATE_RANGE AS(
  SELECT pd.profile_id, enrollstartdate, membershiplength, report_week as Report_week_end, Report_week_start, transactionid, 
  business_date_2 as Business_date, dollarnetvalue
  FROM PROFILE_DATE pd
      LEFT JOIN TRX_DATE ctv
  on pd.profile_id = ctv.profileid
  where business_date_2 < report_week_end and business_date_2>= report_week_start
   )

  --VISIT_COUNT AS(
  SELECT pd.profile_id, pd.enrollstartdate, pd.membershiplength, report_week, count(distinct transactionid) as Roll_Transactions, count(distinct business_date) as Roll_Buying_Days, sum(dollarnetvalue) as Roll_revenue
  from Date_Range dr
    RIGHT JOIN Profile_date pd
  on pd.profile_id = dr.profile_id
    and pd.report_week = dr.report_week_end
    Group by pd.profile_id, pd.enrollstartdate, pd.membershiplength, report_week;
  """.format(database, schema, sfTable, *np.repeat(database, 8))
  )
  
  log = "Successfully loaded delta into {}.{}.{} ".format(database, schema, sfTable)
  executionLogger(curIDL, database, schemaLog, sfLogTable, execId, log, tmpstamp)
  
except: 
  log = "Error loading delta into {}.{}.{} ".format(database, schema, sfTable)
  executionLogger(curIDL, database, schemaLog, sfLogTable, execId, log, tmpstamp)

# COMMAND ----------

# DBTITLE 1,End Logging
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
log = "End of Notebook {} IDL Job for {}.{} on {}".format(nbName, schema, sfTable, tmpstamp)
executionLogger(curIDL, database, schemaLog, sfLogTable, execId, log, tmpstamp)