# Databricks notebook source
dbutils.widgets.dropdown("DataBricks", "cdm_prod", 
                         ["cdm_qa", "cdm_prod","cdm_dev"], "Databricks IDL database Env ")

dbutils.widgets.dropdown("databaseIDL", "POLARIS", 
                         ["POLARIS_DEV", "POLARIS_TEST","POLARIS"], "Snowflake IDL database Env ")

dbutils.widgets.dropdown("schemaIDL", "CUDM", 
                         ["CUDM", "SADM","OPDM", "SHDM"], "Snowflake IDL schema ")

dbutils.widgets.dropdown("databaseSTAGE", "CDM", 
                         ["CDM","CDM_DEV","CDM_TEST"], "Snowflake STAGE database Env ")

dbutils.widgets.dropdown("schemaSTAGE", "STAGING", 
                         ["STAGING"], "Snowflake STAGE schema ")

dbutils.widgets.dropdown("sfIDLTable", "CUSTOMER_CERTIFICATE_FACT", 
                         ["CUSTOMER_CERTIFICATE_FACT"], "Snowflake table ")

dbutils.widgets.dropdown("truncate", "False", 
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
nbName   = "Prod_IDL_CUSTOMER_CERTIFICATE_FACT"
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
databricks =dbutils.widgets.get("DataBricks")

# For incremental load (IDL) of data marts
database  = dbutils.widgets.get("databaseIDL")
schema    = dbutils.widgets.get("schemaIDL")
sfTable   = dbutils.widgets.get("sfIDLTable")

# For staging before IDL
databaseSTAGE  = dbutils.widgets.get("databaseSTAGE")
schemaSTAGE    = dbutils.widgets.get("schemaSTAGE")
cdm_table      = 'trg_mktg_certificate_plr'
stage_cdm_table      = 'trg_mktg_certificate_plr'

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
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(databricks,cdm_table, loadDate))
  
elif delta == 'sfMax':
  # Get current max date in Snowflake table
  loadDate = curIDL.execute("SELECT MAX(TO_DATE(CAST(CdmLoadDate AS STRING), 'YYYYMMDD')) FROM {}.{}".format(schema, sfTable)).fetchone()[0].strftime('%Y%m%d')  
  if loadDate is None:
     loadDate = 0
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

# DBTITLE 1,Drop duplicates
df = df.dropDuplicates()
nrowsDD = df.count()
print('Delta load dataframe length in rows after dedup: ' + str(nrowsDD))

if nrows != nrowsDD:
  print('Duplicates found in delta load - identical rows !')

# COMMAND ----------

# DBTITLE 1,Add primary key column
# Add primary key column: (dnkn) MemberId, CertificateNumber, TransactionId, Source,CertificateStatus,BrandId, TransactionNumber
#                         (other brands) MemberId, CertificateNumber, TransactionId, Source,CertificateStatus,BrandId

df = df.withColumn('CustomerCertificateID', when(df.BrandId == 'dnkn',
                                              concat_ws("", col("MemberId"), col("CertificateNumber"),
                                                            col("TransactionId"), col("Source"),
                                                            col("CertificateStatus"), col("BrandId"), col("TransactionNumber")))
                                         .otherwise(concat_ws("", col("MemberId"), col("CertificateNumber"),
                                                            col("TransactionId"), col("Source"),
                                                            col("CertificateStatus"), col("BrandId"))))

# COMMAND ----------

# DBTITLE 1,Stage delta load in Snowflake
if nrows > 0:  
  if truncate == 'True':
    mode = "overwrite"
    try:
      curSTAGE.execute("TRUNCATE TABLE {}.{}".format(schemaSTAGE, stage_cdm_table))
    except: 
      print("Error truncating table {}.{} in Snowflake stage !".format(schemaSTAGE, stage_cdm_table))
  else:
    mode = "append"
    
  try:
    df.write\
      .format("snowflake")\
      .options(**connOptionsSTAGE)\
      .option("dbtable",  "{}.{}".format(schemaSTAGE, stage_cdm_table))\
      .mode(mode) \
      .save()
    print("{}.{} successfully loaded into Snowflake stage table {} !".format(schema, cdm_table,stage_cdm_table))
  except:
    print("Error loading table {}.{} in Snowflake stage table {} !".format(schemaSTAGE, cdm_table, stage_cdm_table))  
    raise

else:
  print("No new data in CDM !")

# COMMAND ----------

# DBTITLE 1,Merge - Upsert Fact
if nrows > 0: 
  if IDLtype == 'Upsert':
    curIDL.execute("""
    MERGE INTO {}.{}.{} SNOW
    USING (
    SELECT 
        CustomerCertificateID
        ,MemberId
        ,Offer_Cd
        ,TransactionId
        ,BrandId
        ,MemberCardNumber
        ,RewardName
        ,CertificateNumber
        ,TRY_TO_NUMBER(PriceInPoints) PriceInPoints
        ,RewardCode
        ,TransactionNumber
        ,CertificateType
        ,CertificateStatus
        ,PLU
        ,TRY_TO_DECIMAL(ItemDollarValue, 12, 4) ItemDollarValue
        ,TRY_TO_NUMBER(QuantityOrdered) QuantityOrdered
        ,TRY_TO_BOOLEAN(Privacy) Privacy
        ,Source
        ,TRY_TO_TIMESTAMP(StartDate) StartDate
        ,TRY_TO_TIMESTAMP(EndDate) EndDate
        ,TRY_TO_TIMESTAMP(RewardOrderDate) RewardOrderDate
        ,TRY_TO_TIMESTAMP(ModifiedDate) ModifiedDate
        ,TRY_TO_TIMESTAMP(LoadedDate) LoadedDate
        ,CdmLoadDate
        ,CAST(TO_VARCHAR(CURRENT_DATE()::date, 'yyyymmdd') AS INT) AS LoadID
        ,TO_TIMESTAMP(CONVERT_TIMEZONE ('EST', current_timestamp)) AS LoadDttm
    FROM {}.{}.{}) CDM

    ON SNOW.CustomerCertificateID = CDM.CustomerCertificateID

    WHEN MATCHED AND 
    (
      not(equal_null(SNOW.CustomerCertificateID,CDM.CustomerCertificateID))
      OR not(equal_null(SNOW.ProfileId,CDM.MemberId))
      OR not(equal_null(SNOW.Offer_Cd,CDM.Offer_Cd))
      OR not(equal_null(SNOW.TransactionId,CDM.TransactionId))  
      OR not(equal_null(SNOW.BrandId,CDM.BrandId))  
      OR not(equal_null(SNOW.MemberCardNumber,CDM.MemberCardNumber))  
      OR not(equal_null(SNOW.RewardName,CDM.RewardName))        
      OR not(equal_null(SNOW.CertificateNumber,CDM.CertificateNumber))  
      OR not(equal_null(SNOW.PriceInPoints,CDM.PriceInPoints))        
      OR not(equal_null(SNOW.RewardCode,CDM.RewardCode))        
      OR not(equal_null(SNOW.TransactionNumber,CDM.TransactionNumber))        
      OR not(equal_null(SNOW.CertificateType,CDM.CertificateType))        
      OR not(equal_null(SNOW.CertificateStatus,CDM.CertificateStatus))        
      OR not(equal_null(SNOW.PLU,CDM.PLU))        
      OR not(equal_null(SNOW.ItemDollarValue,CDM.ItemDollarValue))        
      OR not(equal_null(SNOW.QuantityOrdered,CDM.QuantityOrdered))        
      OR not(equal_null(SNOW.Privacy,CDM.Privacy))        
      OR not(equal_null(SNOW.Source,CDM.Source))        
      OR not(equal_null(SNOW.StartDate,CDM.StartDate))        
      OR not(equal_null(SNOW.EndDate,CDM.EndDate))        
      OR not(equal_null(SNOW.RewardOrderDate,CDM.RewardOrderDate))        
      OR not(equal_null(SNOW.ModifiedDate,CDM.ModifiedDate))        
      OR not(equal_null(SNOW.LoadedDate,CDM.LoadedDate))        
      OR not(equal_null(SNOW.CdmLoadDate,CDM.CdmLoadDate))  
    )

    THEN UPDATE SET 
      SNOW.CustomerCertificateID =  CDM.CustomerCertificateID  
      ,SNOW.ProfileId          =  CDM.MemberId  
      ,SNOW.Offer_Cd           =  CDM.Offer_Cd
      ,SNOW.TransactionId      =  CDM.TransactionId  
      ,SNOW.BrandId            =  CDM.BrandId  
      ,SNOW.MemberCardNumber   =  CDM.MemberCardNumber  
      ,SNOW.RewardName         =  CDM.RewardName        
      ,SNOW.CertificateNumber  =  CDM.CertificateNumber  
      ,SNOW.PriceInPoints      =  CDM.PriceInPoints        
      ,SNOW.RewardCode         =  CDM.RewardCode        
      ,SNOW.TransactionNumber  =  CDM.TransactionNumber        
      ,SNOW.CertificateType    =  CDM.CertificateType        
      ,SNOW.CertificateStatus  =  CDM.CertificateStatus        
      ,SNOW.PLU                =  CDM.PLU        
      ,SNOW.ItemDollarValue    =  CDM.ItemDollarValue        
      ,SNOW.QuantityOrdered    =  CDM.QuantityOrdered        
      ,SNOW.Privacy            =  CDM.Privacy        
      ,SNOW.Source             =  CDM.Source        
      ,SNOW.StartDate          =  CDM.StartDate        
      ,SNOW.EndDate            =  CDM.EndDate        
      ,SNOW.RewardOrderDate    =  CDM.RewardOrderDate        
      ,SNOW.ModifiedDate       =  CDM.ModifiedDate        
      ,SNOW.LoadedDate         =  CDM.LoadedDate      
      ,SNOW.CdmLoadDate        =  CDM.CdmLoadDate  
      ,SNOW.LoadDttm           =  CDM.LoadDttm

    WHEN NOT MATCHED THEN INSERT (
        CustomerCertificateID
        ,ProfileId
        ,Offer_Cd
        ,TransactionId
        ,BrandId
        ,MemberCardNumber
        ,RewardName
        ,CertificateNumber
        ,PriceInPoints
        ,RewardCode
        ,TransactionNumber
        ,CertificateType
        ,CertificateStatus
        ,PLU
        ,ItemDollarValue
        ,QuantityOrdered
        ,Privacy
        ,Source
        ,StartDate
        ,EndDate
        ,RewardOrderDate
        ,ModifiedDate
        ,LoadedDate
        ,CdmLoadDate
        ,LoadID
        ,LoadDttm
      )
      VALUES   (
        CDM.CustomerCertificateID
        ,CDM.MemberId
        ,CDM.Offer_Cd
        ,CDM.TransactionId
        ,CDM.BrandId
        ,CDM.MemberCardNumber
        ,CDM.RewardName
        ,CDM.CertificateNumber
        ,CDM.PriceInPoints
        ,CDM.RewardCode
        ,CDM.TransactionNumber
        ,CDM.CertificateType
        ,CDM.CertificateStatus
        ,CDM.PLU
        ,CDM.ItemDollarValue
        ,CDM.QuantityOrdered
        ,CDM.Privacy
        ,CDM.Source
        ,CDM.StartDate
        ,CDM.EndDate
        ,CDM.RewardOrderDate
        ,CDM.ModifiedDate
        ,CDM.LoadedDate
        ,CDM.CdmLoadDate
        ,CDM.LoadID
        ,CDM.LoadDttm
      )
    """.format(database, schema, sfTable, databaseSTAGE, schemaSTAGE, stage_cdm_table)
    )           
    
  elif IDLtype == 'Insert':
    curIDL.execute("""
    INSERT INTO {}.{}.{}
    (
      ProfileId
      ,Offer_Cd
      ,TransactionId
      ,BrandId
      ,MemberCardNumber
      ,RewardName
      ,CertificateNumber
      ,PriceInPoints
      ,RewardCode
      ,TransactionNumber
      ,CertificateType
      ,CertificateStatus
      ,PLU
      ,ItemDollarValue
      ,QuantityOrdered
      ,Privacy
      ,Source
      ,StartDate
      ,EndDate
      ,RewardOrderDate
      ,ModifiedDate
      ,LoadedDate
      ,LoadID
      ,LoadDttm
    )

    SELECT
      MemberId
      ,Offer_Cd
      ,TransactionId
      ,BrandId
      ,MemberCardNumber
      ,RewardName
      ,CertificateNumber
      ,PriceInPoints
      ,RewardCode
      ,TransactionNumber
      ,CertificateType
      ,CertificateStatus
      ,PLU
      ,ItemDollarValue
      ,QuantityOrdered
      ,Privacy
      ,Source
      ,StartDate
      ,EndDate
      ,RewardOrderDate
      ,ModifiedDate
      ,LoadedDate
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

