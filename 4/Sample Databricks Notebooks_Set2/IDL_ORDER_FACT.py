# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("databaseIDL", "POLARIS", 
                         ["POLARIS_DEV", "POLARIS_TEST","POLARIS"], "Snowflake IDL database Env ")

dbutils.widgets.dropdown("schemaIDL", "SADM", 
                         ["CUDM", "SADM","OPDM", "SHDM"], "Snowflake IDL schema ")

dbutils.widgets.dropdown("databaseSTAGE", "CDM", 
                         ["CDM"], "Snowflake STAGE database Env ")

dbutils.widgets.dropdown("schemaSTAGE", "STAGING", 
                         ["STAGING"], "Snowflake STAGE schema ")

dbutils.widgets.dropdown("sfIDLTable", "ORDER_FACT", 
                         ["ORDER_FACT"], "Snowflake table ")

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
nbName   = "Prod_IDL_ORDER_FACT"
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

# For incremental load (IDL) of data marts
database  = dbutils.widgets.get("databaseIDL")
schema    = dbutils.widgets.get("schemaIDL")
sfTable   = dbutils.widgets.get("sfIDLTable")

# For staging before IDL
databaseSTAGE  = dbutils.widgets.get("databaseSTAGE")
schemaSTAGE    = dbutils.widgets.get("schemaSTAGE")
cdm_table      = 'tran_order_plr'
stage_cdm_table      = 'tran_order_plr'

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
  df = sqlContext.sql("SELECT * FROM cdm_prod.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(cdm_table, loadDate))
  
elif delta == 'sfMax':
  # Get current max date in Snowflake table
  loadDate = curIDL.execute("SELECT MAX(CAST(CdmLoadDate AS INT)) FROM {}.{}".format(schema, sfTable)).fetchone()[0]
  df = sqlContext.sql("SELECT * FROM cdm_prod.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(cdm_table, loadDate))
  print('Snowflake max date in {}.{} is: '.format(schema, sfTable) + str(loadDate))

elif type(delta) == int:
  tz = timezone('EST')
  loadDate = (datetime.now(tz)- timedelta(days=delta)).strftime('%Y%m%d')
  df = sqlContext.sql("SELECT * FROM cdm_prod.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(cdm_table, loadDate))

else:
  print('Error: Invalid input for delta !')

if df:  
  nrows = df.count()
  print('Delta load dataframe length in rows: ' + str(nrows))

# COMMAND ----------

# DBTITLE 1,Add primary key column
# # Add primary key column: OrderLineId, OrderId, StoreId, BrandId, IsVoided, IsCleared, productid
# df = df.withColumn("UniqOrderId", concat_ws("", col("OrderId"), col("StoreId"), col("IsVoid"), 
#                                                 col("IsRefund"), col("Checknumber"), col("source"), col("BrandId")
#                                             )
#                   )

# COMMAND ----------

# DBTITLE 1,Data quality
# 1. Drop duplicates
df = df.dropDuplicates()

# 2. Filter out orphaned records
df = df.filter(df.OrderId.isNotNull())

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
      curSTAGE.execute("TRUNCATE TABLE {}.{}".format(schemaSTAGE, stage_cdm_table))      
    except: 
      print("Error truncating table {}.{} in Snowflake stage !".format(schemaSTAGE, stage_cdm_table))
  else:
    mode = "append"
    
  try:
    df.write \
      .format("snowflake") \
      .options(**connOptionsSTAGE) \
      .option("dbtable", "{}.{}".format(schemaSTAGE, stage_cdm_table)) \
      .mode(mode) \
      .save()
    print("{}.{} successfully loaded into Snowflake stage table {} !".format(schemaSTAGE, stage_cdm_table, stage_cdm_table))
  except:
    print("Error loading table {}.{} in Snowflake !".format(schemaSTAGE, cdm_table))  
    raise

else:
  print("No new data in CDM !")

# COMMAND ----------

# DBTITLE 1,Merge - Insert Fact
if nrows > 0: 
  if IDLtype == 'Upsert':
     curIDL.execute("""
      MERGE INTO {}.{}.{} SNOW
      USING (
      SELECT 
        CONCAT(
          IFNULL(OrderId,''), 
          IFNULL(OrderName,''),
          IFNULL(StoreId,''), 
          IFNULL(IsVoid,''), 
          IFNULL(IsRefund,''), 
          IFNULL(Checknumber,''), 
          IFNULL(Source,''), 
          IFNULL(BrandId,'')
          ) AS UniqOrderId
        ,OrderId
        ,EmployeeId
        ,CustomerID
        ,StoreId
        ,RestaurantKey
        ,BrandId
        ,CheckNumber
        ,OrderName
        ,CustomerName
        ,LoyaltyNumber
        ,GrossQuantity
        ,GrossAmount
        ,DiscountAmount
        ,NetAmount
        ,SurchargeAmount
        ,TaxAmount
        ,PaymentAmount
        ,Gratuity
        ,InspireId
        ,IsClosed
        ,IsFutureOrder
        ,IsVoid
        ,IsRefund
        ,IsTaxExempt
        ,GuestCount
        ,FirstSendTime
        ,OpenedTime
        ,ClosedTime
        ,TimeKey
        ,BusinessDate
        ,CdmLoaddate
        ,CAST(TO_VARCHAR(CURRENT_DATE()::date, 'yyyymmdd') AS INT) AS LoadID
        ,TO_TIMESTAMP(CONVERT_TIMEZONE ('EST', current_timestamp)) AS LoadDttm
      FROM {}.{}.{}
      WHERE CAST(CdmLoadDate AS INT) >= '{}') CDM

      ON SNOW.UniqOrderId = CDM.UniqOrderId

      WHEN MATCHED AND 
      (
        SNOW.UniqOrderId        !=  CDM.UniqOrderId
        OR SNOW.OrderId         !=  CDM.OrderId 
        OR SNOW.EmployeeId      !=  CDM.EmployeeId 
        OR SNOW.CustomerID      !=  CDM.CustomerID 
        OR SNOW.StoreId         !=  CDM.StoreId 
        OR SNOW.RestaurantKey   !=  CDM.RestaurantKey 
        OR SNOW.BrandId         !=  CDM.BrandId 
        OR SNOW.CheckNumber     !=  CDM.CheckNumber 
        OR SNOW.OrderName       !=  CDM.OrderName 
        OR SNOW.CustomerName    !=  CDM.CustomerName 
        OR SNOW.LoyaltyNumber   !=  CDM.LoyaltyNumber 
        OR SNOW.GrossQuantity   !=  CDM.GrossQuantity 
        OR SNOW.GrossAmount     !=  CDM.GrossAmount 
        OR SNOW.DiscountAmount  !=  CDM.DiscountAmount 
        OR SNOW.NetAmount       !=  CDM.NetAmount 
        OR SNOW.SurchargeAmount !=  CDM.SurchargeAmount 
        OR SNOW.TaxAmount       !=  CDM.TaxAmount 
        OR SNOW.PaymentAmount   !=  CDM.PaymentAmount 
        OR SNOW.Gratuity        !=  CDM.Gratuity 
        OR SNOW.InspireId       !=  CDM.InspireId
        OR SNOW.IsClosed        !=  CDM.IsClosed 
        OR SNOW.IsFutureOrder   !=  CDM.IsFutureOrder 
        OR SNOW.IsVoid          !=  CDM.IsVoid 
        OR SNOW.IsRefund        !=  CDM.IsRefund 
        OR SNOW.IsTaxExempt     !=  CDM.IsTaxExempt 
        OR SNOW.GuestCount      !=  CDM.GuestCount 
        OR SNOW.FirstSendTime   !=  CDM.FirstSendTime 
        OR SNOW.OpenedTime      !=  CDM.OpenedTime 
        OR SNOW.ClosedTime      !=  CDM.ClosedTime 
        OR SNOW.TimeKey         !=  CDM.TimeKey 
        OR SNOW.BusinessDate    !=  CDM.BusinessDate 
        OR SNOW.CdmLoadDate     !=  CDM.CdmLoadDate  
      )

      THEN UPDATE SET 
        SNOW.UniqOrderId      =  CDM.UniqOrderId
        ,SNOW.OrderId         =  CDM.OrderId 
        ,SNOW.EmployeeId      =  CDM.EmployeeId 
        ,SNOW.CustomerID      =  CDM.CustomerID 
        ,SNOW.StoreId         =  CDM.StoreId 
        ,SNOW.RestaurantKey   =  CDM.RestaurantKey 
        ,SNOW.BrandId         =  CDM.BrandId 
        ,SNOW.CheckNumber     =  CDM.CheckNumber 
        ,SNOW.OrderName       =  CDM.OrderName 
        ,SNOW.CustomerName    =  CDM.CustomerName 
        ,SNOW.LoyaltyNumber   =  CDM.LoyaltyNumber 
        ,SNOW.GrossQuantity   =  CDM.GrossQuantity 
        ,SNOW.GrossAmount     =  CDM.GrossAmount 
        ,SNOW.DiscountAmount  =  CDM.DiscountAmount 
        ,SNOW.NetAmount       =  CDM.NetAmount 
        ,SNOW.SurchargeAmount =  CDM.SurchargeAmount 
        ,SNOW.TaxAmount       =  CDM.TaxAmount 
        ,SNOW.PaymentAmount   =  CDM.PaymentAmount 
        ,SNOW.Gratuity        =  CDM.Gratuity 
        ,SNOW.InspireId       =  CDM.InspireId
        ,SNOW.IsClosed        =  CDM.IsClosed 
        ,SNOW.IsFutureOrder   =  CDM.IsFutureOrder 
        ,SNOW.IsVoid          =  CDM.IsVoid 
        ,SNOW.IsRefund        =  CDM.IsRefund 
        ,SNOW.IsTaxExempt     =  CDM.IsTaxExempt 
        ,SNOW.GuestCount      =  CDM.GuestCount 
        ,SNOW.FirstSendTime   =  CDM.FirstSendTime 
        ,SNOW.OpenedTime      =  CDM.OpenedTime 
        ,SNOW.ClosedTime      =  CDM.ClosedTime 
        ,SNOW.TimeKey         =  CDM.TimeKey 
        ,SNOW.BusinessDate    =  CDM.BusinessDate 
        ,SNOW.CdmLoadDate     =  CDM.CdmLoadDate   
        ,SNOW.LoadDttm        =  CDM.LoadDttm

      WHEN NOT MATCHED THEN INSERT (
        UniqOrderId
        ,OrderId
        ,EmployeeId
        ,CustomerID
        ,StoreId
        ,RestaurantKey
        ,BrandId
        ,CheckNumber
        ,OrderName
        ,CustomerName
        ,LoyaltyNumber
        ,GrossQuantity
        ,GrossAmount
        ,DiscountAmount
        ,NetAmount
        ,SurchargeAmount
        ,TaxAmount
        ,PaymentAmount
        ,Gratuity
        ,InspireId
        ,IsClosed
        ,IsFutureOrder
        ,IsVoid
        ,IsRefund
        ,IsTaxExempt
        ,GuestCount
        ,FirstSendTime
        ,OpenedTime
        ,ClosedTime
        ,TimeKey
        ,BusinessDate
        ,CdmLoaddate
        ,LoadID
        ,LoadDttm
        )

      VALUES   (
        CDM.UniqOrderId
        ,CDM.OrderId
        ,CDM.EmployeeId
        ,CDM.CustomerID
        ,CDM.StoreId
        ,CDM.RestaurantKey
        ,CDM.BrandId
        ,CDM.CheckNumber
        ,CDM.OrderName
        ,CDM.CustomerName
        ,CDM.LoyaltyNumber
        ,CDM.GrossQuantity
        ,CDM.GrossAmount
        ,CDM.DiscountAmount
        ,CDM.NetAmount
        ,CDM.SurchargeAmount
        ,CDM.TaxAmount
        ,CDM.PaymentAmount
        ,CDM.Gratuity
        ,CDM.InspireId
        ,CDM.IsClosed
        ,CDM.IsFutureOrder
        ,CDM.IsVoid
        ,CDM.IsRefund
        ,CDM.IsTaxExempt
        ,CDM.GuestCount
        ,CDM.FirstSendTime
        ,CDM.OpenedTime
        ,CDM.ClosedTime
        ,CDM.TimeKey
        ,CDM.BusinessDate
        ,CDM.CdmLoaddate
        ,CDM.LoadID
        ,CDM.LoadDttm
        )
      """.format(database, schema, sfTable, databaseSTAGE, schemaSTAGE, stage_cdm_table, loadDate)
      )             
    
  elif IDLtype == 'Insert':
    curIDL.execute("""
    INSERT INTO {}.{}.{}
    (
     OrderId
      ,EmployeeId
      ,CustomerID
      ,StoreId
      ,RestaurantKey
      ,BrandId
      ,CheckNumber
      ,OrderName
      ,CustomerName
      ,LoyaltyNumber
      ,GrossQuantity
      ,GrossAmount
      ,DiscountAmount
      ,NetAmount
      ,SurchargeAmount
      ,TaxAmount
      ,PaymentAmount
      ,Gratuity
      ,InspireId
      ,IsClosed
      ,IsFutureOrder
      ,IsVoid
      ,IsRefund
      ,IsTaxExempt
      ,GuestCount
      ,FirstSendTime
      ,OpenedTime
      ,ClosedTime
      ,TimeKey
      ,BusinessDate
      ,Cdmloaddate
      ,LoadID
      ,LoadDttm
    )

    SELECT
     OrderId
      ,EmployeeId
      ,CustomerID
      ,StoreId
      ,RestaurantKey
      ,BrandId
      ,CheckNumber
      ,OrderName
      ,CustomerName
      ,LoyaltyNumber
      ,GrossQuantity
      ,GrossAmount
      ,DiscountAmount
      ,NetAmount
      ,SurchargeAmount
      ,TaxAmount
      ,PaymentAmount
      ,Gratuity
      ,IsClosed
      ,IsFutureOrder
      ,IsVoid
      ,IsRefund
      ,IsTaxExempt
      ,GuestCount
      ,FirstSendTime
      ,OpenedTime
      ,ClosedTime
      ,TimeKey
      ,BusinessDate
      ,Cdmloaddate
      ,CAST(TO_VARCHAR(CURRENT_DATE()::date, 'yyyymmdd') AS INT) AS LoadID
      ,TO_TIMESTAMP(CONVERT_TIMEZONE ('EST',current_timestamp)) AS LoadDttm

    FROM {}.{}.{}
    """.format(database, schema, sfTable, databaseSTAGE, schemaSTAGE, stage_cdm_table)
    )        
    print("{}.{} successfully loaded into Snowflake !".format(schema, sfTable))
  else:
    print(" IDL Type not valid !")
    
else:
  print("No new data in CDM !")    

# COMMAND ----------

# DBTITLE 1,End Logging
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
log = "End of Notebook {} IDL Job for {}.{} on {}".format(nbName, schema, sfTable, tmpstamp)
executionLogger(curIDL, database, schemaLog, sfLogTable, execId, log, tmpstamp)