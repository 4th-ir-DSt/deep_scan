# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("DataBricks", "cdm_prod", 
                         ["cdm_qa", "cdm_prod","cdm_dev"], "Databricks IDL database Env ")

dbutils.widgets.dropdown("databaseIDL", "POLARIS", 
                         ["POLARIS_DEV", "POLARIS_TEST","POLARIS"], "Snowflake IDL database Env ")

dbutils.widgets.dropdown("schemaIDL", "CUDM", 
                         ["CUDM", "SADM","OPDM", "SHDM"], "Snowflake IDL schema ")

dbutils.widgets.dropdown("databaseSTAGE", "cdm", 
                         ["cdm","cdm_test","cdm_dev"], "Snowflake STAGE database Env ")

dbutils.widgets.dropdown("schemaSTAGE", "STAGING", 
                         ["STAGING"], "Snowflake STAGE schema ")

dbutils.widgets.dropdown("sfIDLTable", "LOYALTY_TRANSACTION_FACT", 
                         ["LOYALTY_TRANSACTION_FACT"], "Snowflake table ")

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
nbName   = "Prod_IDL_LOYALTY_TRANSACTION_FACT"
tz       = timezone('EST')
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
rnumber  = int(np.random.rand() * 1000000)

execId   = nbName + "_" + tmpstamp + str(rnumber)

# COMMAND ----------

# DBTITLE 1,Fetch snowflake connection string 
# MAGIC %run ./Connection/ConnectionString

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
cdm_table      = 'tran_loyalty_transactions_plr'
stage_cdm_table      = 'tran_loyalty_transactions_plr'

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

# DBTITLE 1,Add primary key column
# Add primary key column: OrderId, TransactionId, BrandId
df = df.withColumn('LoyTranID', concat_ws("", col("OrderId"), col("TransactionId"), col("BrandId")))

# COMMAND ----------

# DBTITLE 1,Drop duplicates
df = df.dropDuplicates()
#nrowsDD = df.count()
#print('Delta load dataframe lenght in rows after dedup: ' + str(nrowsDD))

#if nrows != nrowsDD:
#  print('Duplicates found in delta load - identical rows !')

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
    curIDL.execute("""
    MERGE INTO {}.{}.{} SNOW
    USING (SELECT 
        LoyTranID
        ,EmployeeId
        ,EmployeeName
        ,OrderId
        ,MemberId as ProfileId
        ,BrandId
        ,MinDayPartId
        ,MaxDayPartId
        ,SuspendTXNId
        ,TransactionId
        ,MemberCardNumber
        ,CheckNumber
        ,TO_NUMBER(DOLLARNETVALUE,38,5) DollarNetValue
        ,TO_NUMBER(ELIGIBLEREVENUE,38,5) EligibleRevenue
        ,RestaurantNumber
        ,MethodOfAttachment
        ,TransStatus
        ,MemberPhoneNumber
        ,ReasonCode
        ,SuspendTXNStatus
        ,TO_TIMESTAMP(BusinessDate,'YYYYMMDD') BusinessDate
        ,CdmLoadDate
        ,CAST(TO_VARCHAR(CURRENT_DATE()::date, 'yyyymmdd') AS INT) AS LoadID
        ,TO_TIMESTAMP(CONVERT_TIMEZONE ('EST', current_timestamp)) AS LoadDttm
    FROM {}.{}.{}) CDM

    ON SNOW.LoyTranID = CDM.LoyTranID

    WHEN MATCHED AND 
    (
      
       not(equal_null(SNOW.EmployeeId,CDM.EmployeeId))
      OR not(equal_null(SNOW.EmployeeName,CDM.EmployeeName))
      OR not(equal_null(SNOW.ProfileId,CDM.ProfileId))
      OR not(equal_null(SNOW.MinDayPartId,CDM.MinDayPartId))
      OR not(equal_null(SNOW.MaxDayPartId,CDM.MaxDayPartId))
      OR not(equal_null(SNOW.SuspendTXNId,CDM.SuspendTXNId))
      OR not(equal_null(SNOW.TransactionId,CDM.TransactionId))
      OR not(equal_null(SNOW.MemberCardNumber,CDM.MemberCardNumber))
      OR not(equal_null(SNOW.CheckNumber,CDM.CheckNumber))
      OR not(equal_null(SNOW.DollarNetValue,CDM.DollarNetValue))
      OR not(equal_null(SNOW.EligibleRevenue,CDM.EligibleRevenue))
      OR not(equal_null(SNOW.RestaurantNumber,CDM.RestaurantNumber))
      OR not(equal_null(SNOW.MethodOfAttachment,CDM.MethodOfAttachment))
      OR not(equal_null(SNOW.TransStatus,CDM.TransStatus))
      OR not(equal_null(SNOW.MemberPhoneNumber,CDM.MemberPhoneNumber))
      OR not(equal_null(SNOW.ReasonCode,CDM.ReasonCode))
      OR not(equal_null(SNOW.SuspendTXNStatus,CDM.SuspendTXNStatus))
      OR not(equal_null(SNOW.BusinessDate,CDM.BusinessDate))
      OR not(equal_null(SNOW.CdmLoadDate,CDM.CdmLoadDate))    
    )

    THEN UPDATE SET 
        SNOW.EmployeeId           =  CDM.EmployeeId
        ,SNOW.EmployeeName         =  CDM.EmployeeName
        ,SNOW.ProfileId            =  CDM.ProfileId
        ,SNOW.MinDayPartId         =  CDM.MinDayPartId
        ,SNOW.MaxDayPartId         =  CDM.MaxDayPartId
        ,SNOW.SuspendTXNId         =  CDM.SuspendTXNId
        ,SNOW.TransactionId        =  CDM.TransactionId
        ,SNOW.MemberCardNumber     =  CDM.MemberCardNumber
        ,SNOW.CheckNumber          =  CDM.CheckNumber
        ,SNOW.DollarNetValue       =  CDM.DollarNetValue
        ,SNOW.EligibleRevenue      =  CDM.EligibleRevenue
        ,SNOW.RestaurantNumber     =  CDM.RestaurantNumber
        ,SNOW.MethodOfAttachment   =  CDM.MethodOfAttachment
        ,SNOW.TransStatus          =  CDM.TransStatus
        ,SNOW.MemberPhoneNumber    =  CDM.MemberPhoneNumber
        ,SNOW.ReasonCode           =  CDM.ReasonCode
        ,SNOW.SuspendTXNStatus     =  CDM.SuspendTXNStatus
        ,SNOW.BusinessDate         =  CDM.BusinessDate
        ,SNOW.CdmLoadDate          =  CDM.CdmLoadDate      
        ,SNOW.LoadDttm             =  CDM.LoadDttm

    WHEN NOT MATCHED THEN INSERT (
        LoyTranID
        ,EmployeeId
        ,EmployeeName
        ,OrderId
        ,ProfileId
        ,BrandId
        ,MinDayPartId
        ,MaxDayPartId
        ,SuspendTXNId
        ,TransactionId
        ,MemberCardNumber
        ,CheckNumber
        ,DollarNetValue
        ,EligibleRevenue
        ,RestaurantNumber
        ,MethodOfAttachment
        ,TransStatus
        ,MemberPhoneNumber
        ,ReasonCode
        ,SuspendTXNStatus
        ,BusinessDate
        ,CdmLoadDate
        ,LoadID
        ,LoadDttm
      )
      VALUES   (
        CDM.LoyTranID
        ,CDM.EmployeeId
        ,CDM.EmployeeName
        ,CDM.OrderId
        ,CDM.ProfileId
        ,CDM.BrandId
        ,CDM.MinDayPartId
        ,CDM.MaxDayPartId
        ,CDM.SuspendTXNId
        ,CDM.TransactionId
        ,CDM.MemberCardNumber
        ,CDM.CheckNumber
        ,CDM.DollarNetValue
        ,CDM.EligibleRevenue
        ,CDM.RestaurantNumber
        ,CDM.MethodOfAttachment
        ,CDM.TransStatus
        ,CDM.MemberPhoneNumber
        ,CDM.ReasonCode
        ,CDM.SuspendTXNStatus
        ,CDM.BusinessDate
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

# DBTITLE 1,Safety Check Query
pk = 'TransactionId'
query = "select count(distinct({})) as UniqKeys, count({}) as TotalKeys from {}.{}"

sfCount  = curIDL.execute(query.format(pk,pk, schema, sfTable)).fetchall()
cdmCount = sqlContext.sql(query.format(pk,pk, databricks, cdm_table))
cdmCount = [(cdmCount.select('UniqKeys').collect()[0].UniqKeys, cdmCount.select('TotalKeys').collect()[0].TotalKeys)]

if sfCount != cdmCount: 
  print('Missmatch between CDM & Snowflake data: unequal sizes of tables!')
  print('-Snowflake {}.{}.{} has '.format(database, schema, sfTable) + str(sfCount))
  print('-CDM {}.{} has '.format(databricks,cdm_table) + str(cdmCount))

# COMMAND ----------

# DBTITLE 1,End Logging
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
log = "End of Notebook {} IDL Job for {}.{} on {}".format(nbName, schema, sfTable, tmpstamp)
executionLogger(curIDL, database, schemaLog, sfLogTable, execId, log, tmpstamp)

# COMMAND ----------

