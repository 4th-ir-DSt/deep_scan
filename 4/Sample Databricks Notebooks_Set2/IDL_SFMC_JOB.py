# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("databaseIDL", "POLARIS", 
                         ["POLARIS_DEV", "POLARIS_TEST","POLARIS"], "Snowflake IDL database Env ")

dbutils.widgets.dropdown("schemaIDL", "CUDM", 
                         ["CUDM", "SADM","OPDM", "SHDM"], "Snowflake IDL schema ")

dbutils.widgets.dropdown("databaseSTAGE", "CDM", 
                         ["CDM","CDM_DEV","CDM_TEST"], "Snowflake STAGE database Env ")

dbutils.widgets.dropdown("schemaSTAGE", "STAGING", 
                         ["STAGING"], "Snowflake STAGE schema ")

dbutils.widgets.dropdown("sfIDLTable", "SFMC_JOB", 
                         ["SFMC_JOB"], "Snowflake table ")

dbutils.widgets.dropdown("DBDatabase", "cdm_prod", 
                         ["cdm_dev","cdm_qa","cdm_prod"], "Databricks cdm database Env ")

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
cdmDBEnv = dbutils.widgets.get("DBDatabase")
# Execution ID = Name of Notebook + Timestamp with Millisecond + Random Number
nbName   = "Prod_IDL_SFMC_JOB"
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
# truncate = True
# IDLtype  = 'insert'

truncate = dbutils.widgets.get("truncate")
IDLtype  = dbutils.widgets.get("IDLtype")

cdm_table = 'trg_mktg_email_job_plr'
stage_cdm_table      = 'trg_mktg_email_job_plr'

# COMMAND ----------

# DBTITLE 1,Get latest data from CDM
if delta == '*':
  loadDate = 0
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(cdmDBEnv,cdm_table, loadDate))
  
elif delta == 'sfMax':
  # Get current max date in Snowflake table
  loadDate = curIDL.execute("SELECT MAX(CAST(CdmLoadDate AS INT)) FROM {}.{}".format(schema, sfTable)).fetchone()[0] 
  if loadDate is None:
    loadDate = 0
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) > '{}'".format(cdmDBEnv,cdm_table, loadDate))
  print('Snowflake max date in {}.{} is: '.format(schema, sfTable) + str(loadDate))
  print("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) > '{}'".format(cdmDBEnv,cdm_table, loadDate))

elif type(delta) == int:
  tz = timezone('EST')
  loadDate = (datetime.now(tz)- timedelta(days=delta)).strftime('%Y%m%d')
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= '{}'".format(cdmDBEnv,cdm_table, loadDate))

else:
  print('Error: Invalid input for delta !')

if df:  
  nrows = df.count()
  print('Delta load dataframe lenght in rows: ' + str(nrows))

# COMMAND ----------

# DBTITLE 1,Drop duplicates
# df = df.dropDuplicates()
# nrowsDD = df.count()
# print('Delta load dataframe lenght in rows after dedup: ' + str(nrowsDD))

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
    df.write \
      .format("snowflake") \
      .options(**connOptionsSTAGE) \
      .option("dbtable",  "{}.{}".format(schemaSTAGE, stage_cdm_table)) \
      .mode(mode) \
      .save()
    print("{}.{} successfully loaded into Snowflake stage {} !".format(schema, sfTable, stage_cdm_table))
  except:
    print("Error loading table {}.{} into Snowflake stage {} !".format(schemaSTAGE, cdm_table, stage_cdm_table))  

else:
  print("No new data in CDM !")

# COMMAND ----------

# DBTITLE 1,Upsert Fact
if nrows > 0: 
  if IDLtype == 'Upsert':
   
    qry_string = """
    MERGE INTO {}.{}.{} SNOW
    USING 
    (
      WITH CTE_TEMP AS 
      (
        SELECT
            JobId
            ,EmailId
            ,AccountId
            ,AccountUserId
            ,EventId
            ,TriggererSendDefinitionObjectId
            ,BrandId
            ,FromName
            ,FromEmail
            ,SchedTime
            ,PickupTime
            ,DeliveredTime
            ,IsMultipart
            ,JobType
            ,JobStatus
            ,ModifiedBy
            ,MODIFIEDDATE
            ,EmailName
            ,EmailSubject
            ,IsWrapped
            ,TestEmailAddr
            ,Category
            ,BccEmail
            ,OriginalSchedTime
            ,CreatedDate
            ,CharacterSet
            ,IPAddress
            ,SalesForceTotalSubscriberCount
            ,SalesForceErrorSubscriberCount
            ,SendType
            ,DynamicEmailSubject
            ,SuppressTracking
            ,SendClassificationType
            ,SendClassification
            ,ResolveLinksWithCurrentData
            ,EmailSendDefinition
            ,DeduplicateByEmail
            ,TriggeredSendCustomerKey
            ,CDMLOADDATE
            ,ROW_NUMBER() OVER (PARTITION BY JobID ORDER BY CDMLOADDATE) AS R	
          FROM {}.{}.{}
         )
         
           SELECT
        JobID
        ,EmailID
        ,AccountID
        ,AccountUserID
        ,EventID
        ,TriggererSendDefinitionObjectID
        ,BrandId
        ,FromName
        ,FromEmail
        ,TRY_TO_TIMESTAMP(SCHEDTIME) SchedTime
        ,TRY_TO_TIMESTAMP(PICKUPTIME) PickupTime
        ,TRY_TO_TIMESTAMP(DELIVEREDTIME) DeliveredTime
        ,TRY_TO_BOOLEAN(ISMULTIPART) IsMultipart
        ,JobType
        ,JobStatus
        ,ModifiedBy
        ,TRY_TO_TIMESTAMP(MODIFIEDDATE) MODIFIEDDATE
        ,EmailName
        ,EmailSubject
        ,TRY_TO_BOOLEAN(ISWRAPPED) IsWrapped
        ,TestEmailAddr
        ,Category
        ,BccEmail
        ,OriginalSchedTime
        ,TRY_TO_TIMESTAMP(CREATEDDATE) CreatedDate
        ,CharacterSet
        ,IPAddress
        ,SalesForceTotalSubscriberCount
        ,SalesForceErrorSubscriberCount
        ,SendType
        ,DynamicEmailSubject
        ,SuppressTracking
        ,SendClassificationType
        ,SendClassification
        ,ResolveLinksWithCurrentData
        ,EmailSendDefinition
        ,TRY_TO_BOOLEAN(DEDUPLICATEBYEMAIL) DeduplicateByEmail
        ,TriggeredSendCustomerKey
        ,CdmLoadDate
              ,CAST(TO_VARCHAR(CURRENT_DATE()::date, 'yyyymmdd') AS INT) AS LoadID
              ,TO_TIMESTAMP(CONVERT_TIMEZONE ('EST',current_timestamp)) AS LoadDttm
           FROM CTE_TEMP
		   WHERE R = 1
            
      ) AS TMP
      ON  SNOW.JobID = TMP.JobID
      
      WHEN MATCHED AND (
          not(equal_null(SNOW.JobID,TMP.JobID))
          OR not(equal_null(SNOW.EmailID,TMP.EmailID))
          OR not(equal_null(SNOW.AccountID,TMP.AccountID))
          OR not(equal_null(SNOW.AccountUserID,TMP.AccountUserID))
          OR not(equal_null(SNOW.EventID,TMP.EventID))
          OR not(equal_null(SNOW.TriggererSendDefinitionObjectID,TMP.TriggererSendDefinitionObjectID))
          OR not(equal_null(SNOW.BrandId,TMP.BrandId))
          OR not(equal_null(SNOW.FromName,TMP.FromName))
          OR not(equal_null(SNOW.FromEmail,TMP.FromEmail))
          OR not(equal_null(SNOW.SchedTime,TMP.SchedTime))
          OR not(equal_null(SNOW.PickupTime,TMP.PickupTime))
          OR not(equal_null(SNOW.DeliveredTime,TMP.DeliveredTime))
          OR not(equal_null(SNOW.IsMultipart,TMP.IsMultipart))
          OR not(equal_null(SNOW.JobType,TMP.JobType))
          OR not(equal_null(SNOW.JobStatus,TMP.JobStatus))
          OR not(equal_null(SNOW.ModifiedBy,TMP.ModifiedBy))
          OR not(equal_null(SNOW.EmailName,TMP.EmailName))
          OR not(equal_null(SNOW.EmailSubject,TMP.EmailSubject))
          OR not(equal_null(SNOW.IsWrapped,TMP.IsWrapped))
          OR not(equal_null(SNOW.TestEmailAddr,TMP.TestEmailAddr))
          OR not(equal_null(SNOW.Category,TMP.Category))
          OR not(equal_null(SNOW.BccEmail,TMP.BccEmail))
          OR not(equal_null(SNOW.OriginalSchedTime,TMP.OriginalSchedTime))
          OR not(equal_null(SNOW.CreatedDate,TMP.CreatedDate))
          OR not(equal_null(SNOW.CharacterSet,TMP.CharacterSet))
          OR not(equal_null(SNOW.IPAddress,TMP.IPAddress))
          OR not(equal_null(SNOW.SalesForceTotalSubscriberCount,TMP.SalesForceTotalSubscriberCount))
          OR not(equal_null(SNOW.SalesForceErrorSubscriberCount,TMP.SalesForceErrorSubscriberCount))
          OR not(equal_null(SNOW.SendType,TMP.SendType))
          OR not(equal_null(SNOW.DynamicEmailSubject,TMP.DynamicEmailSubject))
          OR not(equal_null(SNOW.SuppressTracking,TMP.SuppressTracking))
          OR not(equal_null(SNOW.SendClassificationType,TMP.SendClassificationType))
          OR not(equal_null(SNOW.SendClassification,TMP.SendClassification))
          OR not(equal_null(SNOW.ResolveLinksWithCurrentData,TMP.ResolveLinksWithCurrentData))
          OR not(equal_null(SNOW.EmailSendDefinition,TMP.EmailSendDefinition))
          OR not(equal_null(SNOW.DeduplicateByEmail,TMP.DeduplicateByEmail))
          OR not(equal_null(SNOW.TriggeredSendCustomerKey,TMP.TriggeredSendCustomerKey))
          OR not(equal_null(SNOW.CdmLoadDate,TMP.CdmLoadDate))
          OR not(equal_null(SNOW.MODIFIEDDATE,TMP.MODIFIEDDATE))
      )
      
      THEN UPDATE SET
          SNOW.JobID                               = TMP.JobID
          ,SNOW.EmailID                            = TMP.EmailID
          ,SNOW.AccountID                          = TMP.AccountID
          ,SNOW.AccountUserID                      = TMP.AccountUserID
          ,SNOW.EventID                            = TMP.EventID
          ,SNOW.TriggererSendDefinitionObjectID    = TMP.TriggererSendDefinitionObjectID
          ,SNOW.BrandId                            = TMP.BrandId
          ,SNOW.FromName                           = TMP.FromName
          ,SNOW.FromEmail                          = TMP.FromEmail
          ,SNOW.SchedTime                          = TMP.SchedTime
          ,SNOW.PickupTime                         = TMP.PickupTime
          ,SNOW.DeliveredTime                      = TMP.DeliveredTime
          ,SNOW.IsMultipart                        = TMP.IsMultipart
          ,SNOW.JobType                            = TMP.JobType
          ,SNOW.JobStatus                          = TMP.JobStatus
          ,SNOW.ModifiedBy                         = TMP.ModifiedBy
          ,SNOW.EmailName                          = TMP.EmailName
          ,SNOW.EmailSubject                       = TMP.EmailSubject
          ,SNOW.IsWrapped                          = TMP.IsWrapped
          ,SNOW.TestEmailAddr                      = TMP.TestEmailAddr
          ,SNOW.Category                           = TMP.Category
          ,SNOW.BccEmail                           = TMP.BccEmail
          ,SNOW.OriginalSchedTime                  = TMP.OriginalSchedTime
          ,SNOW.CreatedDate                        = TMP.CreatedDate
          ,SNOW.CharacterSet                       = TMP.CharacterSet
          ,SNOW.IPAddress                          = TMP.IPAddress
          ,SNOW.SalesForceTotalSubscriberCount     = TMP.SalesForceTotalSubscriberCount
          ,SNOW.SalesForceErrorSubscriberCount     = TMP.SalesForceErrorSubscriberCount
          ,SNOW.SendType                           = TMP.SendType
          ,SNOW.DynamicEmailSubject                = TMP.DynamicEmailSubject
          ,SNOW.SuppressTracking                   = TMP.SuppressTracking
          ,SNOW.SendClassificationType             = TMP.SendClassificationType
          ,SNOW.SendClassification                 = TMP.SendClassification
          ,SNOW.ResolveLinksWithCurrentData        = TMP.ResolveLinksWithCurrentData
          ,SNOW.EmailSendDefinition                = TMP.EmailSendDefinition
          ,SNOW.DeduplicateByEmail                 = TMP.DeduplicateByEmail
          ,SNOW.TriggeredSendCustomerKey           = TMP.TriggeredSendCustomerKey
          ,SNOW.CdmLoadDate                        = TMP.CdmLoadDate
          ,SNOW.LoadDttm                           = TMP.LoadDttm
          ,SNOW.MODIFIEDDATE                       = TMP.MODIFIEDDATE
      
      WHEN NOT MATCHED THEN INSERT (
          JobID
          ,EmailID
          ,AccountID
          ,AccountUserID
          ,EventID
          ,TriggererSendDefinitionObjectID
          ,BrandId
          ,FromName
          ,FromEmail
          ,SchedTime
          ,PickupTime
          ,DeliveredTime
          ,IsMultipart
          ,JobType
          ,JobStatus
          ,ModifiedBy
          ,MODIFIEDDATE
          ,EmailName
          ,EmailSubject
          ,IsWrapped
          ,TestEmailAddr
          ,Category
          ,BccEmail
          ,OriginalSchedTime
          ,CreatedDate
          ,CharacterSet
          ,IPAddress
          ,SalesForceTotalSubscriberCount
          ,SalesForceErrorSubscriberCount
          ,SendType
          ,DynamicEmailSubject
          ,SuppressTracking
          ,SendClassificationType
          ,SendClassification
          ,ResolveLinksWithCurrentData
          ,EmailSendDefinition
          ,DeduplicateByEmail
          ,TriggeredSendCustomerKey
          ,CdmLoadDate
          ,LoadID
          ,LoadDttm
      )
      
      VALUES (
          TMP.JobID
          ,TMP.EmailID
          ,TMP.AccountID
          ,TMP.AccountUserID
          ,TMP.EventID
          ,TMP.TriggererSendDefinitionObjectID
          ,TMP.BrandId
          ,TMP.FromName
          ,TMP.FromEmail
          ,TMP.SchedTime
          ,TMP.PickupTime
          ,TMP.DeliveredTime
          ,TMP.IsMultipart
          ,TMP.JobType
          ,TMP.JobStatus
          ,TMP.ModifiedBy
          ,TMP.MODIFIEDDATE
          ,TMP.EmailName
          ,TMP.EmailSubject
          ,TMP.IsWrapped
          ,TMP.TestEmailAddr
          ,TMP.Category
          ,TMP.BccEmail
          ,TMP.OriginalSchedTime
          ,TMP.CreatedDate
          ,TMP.CharacterSet
          ,TMP.IPAddress
          ,TMP.SalesForceTotalSubscriberCount
          ,TMP.SalesForceErrorSubscriberCount
          ,TMP.SendType
          ,TMP.DynamicEmailSubject
          ,TMP.SuppressTracking
          ,TMP.SendClassificationType
          ,TMP.SendClassification
          ,TMP.ResolveLinksWithCurrentData
          ,TMP.EmailSendDefinition
          ,TMP.DeduplicateByEmail
          ,TMP.TriggeredSendCustomerKey
          ,TMP.CdmLoadDate
          ,TMP.LoadID
          ,TMP.LoadDttm
        )
      """.format(database,schema,sfTable,databaseSTAGE,schemaSTAGE,stage_cdm_table)
    curIDL.execute(qry_string) 
    
  elif IDLtype == 'insert':
    curIDL.execute("""
    INSERT INTO {}.{}.{}
    (
      JobID
      ,EmailID
      ,AccountID
      ,AccountUserID
      ,EventID
      ,TriggererSendDefinitionObjectID
      ,BrandId
      ,FromName
      ,FromEmail
      ,SchedTime
      ,PickupTime
      ,DeliveredTime
      ,IsMultipart
      ,JobType
      ,JobStatus
      ,ModifiedBy
      ,MODIFIEDDATE
      ,EmailName
      ,EmailSubject
      ,IsWrapped
      ,TestEmailAddr
      ,Category
      ,BccEmail
      ,OriginalSchedTime
      ,CreatedDate
      ,CharacterSet
      ,IPAddress
      ,SalesForceTotalSubscriberCount
      ,SalesForceErrorSubscriberCount
      ,SendType
      ,DynamicEmailSubject
      ,SuppressTracking
      ,SendClassificationType
      ,SendClassification
      ,ResolveLinksWithCurrentData
      ,EmailSendDefinition
      ,DeduplicateByEmail
      ,TriggeredSendCustomerKey
      ,LoadID
      ,LoadDttm
    )

    SELECT 
      JobID
      ,EmailID
      ,AccountID
      ,AccountUserID
      ,EventID
      ,TriggererSendDefinitionObjectID
      ,BrandId
      ,FromName
      ,FromEmail
      ,SchedTime
      ,PickupTime
      ,DeliveredTime
      ,IsMultipart
      ,JobType
      ,JobStatus
      ,ModifiedBy
      ,MODIFIEDDATE
      ,EmailName
      ,EmailSubject
      ,IsWrapped
      ,TestEmailAddr
      ,Category
      ,BccEmail
      ,OriginalSchedTime
      ,CreatedDate
      ,CharacterSet
      ,IPAddress
      ,SalesForceTotalSubscriberCount
      ,SalesForceErrorSubscriberCount
      ,SendType
      ,DynamicEmailSubject
      ,SuppressTracking
      ,SendClassificationType
      ,SendClassification
      ,ResolveLinksWithCurrentData
      ,EmailSendDefinition
      ,DeduplicateByEmail
      ,TriggeredSendCustomerKey
      ,CAST(TO_VARCHAR(CURRENT_DATE()::date, 'yyyymmdd') AS INT) AS LoadID
      ,TO_TIMESTAMP(CONVERT_TIMEZONE ('EST',current_timestamp)) AS LoadDttm

    FROM {}.{}.{}
    """.format(database, schema, sfTable, databaseSTAGE, schemaSTAGE, stage_cdm_table)
    )        
  else:
    print(" IDL Type not valid !")

# COMMAND ----------

# DBTITLE 1,Safety Check Query
pk = 'JobID'
query = "select count(distinct({})) as UniqKeys, count({}) as TotalKeys from {}.{}"

sfCount  = curIDL.execute(query.format(pk,pk, schema, sfTable)).fetchall()
cdmCount = sqlContext.sql(query.format(pk,pk, cdmDBEnv, cdm_table))
cdmCount = [(cdmCount.select('UniqKeys').collect()[0].UniqKeys, cdmCount.select('TotalKeys').collect()[0].TotalKeys)]

if sfCount != cdmCount: 
  print('Missmatch between CDM & Snowflake data: unequal sizes of tables!')
  print('-Snowflake {}.{}.{} has '.format(database, schema, sfTable) + str(sfCount))
  print('-CDM {}.{} has '.format(cdmDBEnv,cdm_table) + str(cdmCount))

# COMMAND ----------

# DBTITLE 1,End Logging
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
log = "End of Notebook {} IDL Job for {}.{} on {}".format(nbName, schema, sfTable, tmpstamp)
executionLogger(curIDL, database, schemaLog, sfLogTable, execId, log, tmpstamp)

# COMMAND ----------

