# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("DataBricks", "cdm_dev", 
                         ["cdm_dev", "cdm_qa","cdm_prod"], "Databricks IDL database Env ")

dbutils.widgets.dropdown("databaseIDL", "POLARIS_DEV", 
                         ["POLARIS_DEV", "POLARIS_TEST","POLARIS"], "Snowflake IDL database Env ")

dbutils.widgets.dropdown("schemaIDL", "CUDM", 
                         ["CUDM", "SADM","OPDM", "SHDM"], "Snowflake IDL schema ")

dbutils.widgets.dropdown("databaseSTAGE", "CDM_DEV", 
                         ["CDM_DEV","CDM_TEST", "CDM"], "Snowflake STAGE database Env ")

dbutils.widgets.dropdown("schemaSTAGE", "STAGING", 
                         ["STAGING"], "Snowflake STAGE schema ")

dbutils.widgets.dropdown("sfIDLTable", "CUSTOMER_DIM", 
                         ["CUSTOMER_DIM"], "Snowflake table ")

dbutils.widgets.dropdown("truncate", "True", 
                         ["True", "False"], "Truncate before staging ")

dbutils.widgets.dropdown("delta", "sfMax", 
                          ["sfMax", "*", "integer"], "Determines length of lookback period ")

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
from pyspark.sql.functions import isnan, when, count, col, lit, to_timestamp,concat
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
nbName   = "Prod_IDL_CUSTOMER_DIMENSION"
tz       = timezone('EST')
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
rnumber  = int(np.random.rand() * 1000000)

execId   = nbName + "_" + tmpstamp + str(rnumber)

# COMMAND ----------

# DBTITLE 1,Fetch snowflake connection string 
# MAGIC 
# MAGIC %run ./Connection/ConnectionString

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

databricks = dbutils.widgets.get("DataBricks")
cdm_table  = 'cust_profile_plr'
stage_cdm_table = 'cust_profile_plr'

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
#   'merge'  : use merge into statement -> update & insert based on primary key (pk)
#   'insert' : use insert statement     -> only insert new records (used for fact data where no pk)

delta    = dbutils.widgets.get("delta")
truncate = dbutils.widgets.get("truncate")
IDLtype  = dbutils.widgets.get("IDLtype")

# COMMAND ----------

# DBTITLE 1,Get latest data from CDM
if delta == '*':
  df = sqlContext.sql("SELECT * FROM {}.{}".format(databricks,cdm_table))
  
elif delta == 'sfMax':
  # Get current max date in Snowflake table --> cust_profile_plr in CDM.STAGING
  loadDate = curSTAGE.execute("SELECT CAST(TO_VARCHAR(DATEADD(DAY,-10,TO_DATE(TO_VARCHAR(MAX(CAST(CdmLoadDate AS INT))),'yyyyMMdd')),'yyyyMMdd') AS INT) FROM {}.{}".format(schemaSTAGE, stage_cdm_table)).fetchone()[0]
  loadDate_dnkn = curSTAGE.execute("SELECT CAST(TO_VARCHAR(DATEADD(DAY,-60,TO_DATE(TO_VARCHAR(MAX(CAST(CdmLoadDate AS INT))),'yyyyMMdd')),'yyyyMMdd') AS INT) FROM {}.{}".format(schemaSTAGE, stage_cdm_table)).fetchone()[0]
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CASE WHEN BRANDID NOT LIKE 'dnkn' THEN CAST(CdmLoadDate AS INT) >= {} ELSE CAST(CdmLoadDate AS INT) >= {} end".format(databricks, cdm_table, loadDate,loadDate_dnkn))
  print('Snowflake max date in {}.{} is: '.format(schema, sfTable) + str(loadDate))

elif type(delta) == int:
  tz = timezone('EST')
  loadDate = (datetime.now(tz)- timedelta(days=delta)).strftime('%Y%m%d')
  df = sqlContext.sql("SELECT * FROM {}.{} WHERE CAST(CdmLoadDate AS INT) >= {}".format(databricks, cdm_table, loadDate))

else:
  print('Error: Invalid input for delta !')

if df:  
  nrows = df.count()
  print('Delta load in {} dataframe length in rows: '.format(cdm_table) + str(nrows))

# COMMAND ----------

# DBTITLE 1,Add Auditing columns 
tz = timezone('EST')
df = df.withColumn('LoadID',  lit(int(datetime.now(tz).strftime('%Y%m%d'))))
df = df.withColumn('LoadDttm',  lit(datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S.%s')))

# COMMAND ----------

# DBTITLE 1,Stage cust_profile_plr delta load in Snowflake
if nrows > 0:  
  if truncate == 'True':
    mode = "overwrite"
    try:
      curSTAGE.execute("TRUNCATE TABLE {}.{}".format(schemaSTAGE, stage_cdm_table))      
    except: 
      print("Error truncating table {}.{} in Snowflake stage !".format(schemaSTAGE, stage_cdm_table))
      raise
  else:
    mode = "append"
    
  try:
    df.write \
      .format("snowflake") \
      .options(**connOptionsSTAGE) \
      .option("dbtable",  "{}.{}".format(schemaSTAGE, stage_cdm_table)) \
      .mode(mode) \
      .save()
    print("{}.{} successfully loaded into Snowflake stage table {} !".format(schemaSTAGE, cdm_table, stage_cdm_table))
  except:
    print("Error loading table {}.{} in Snowflake stage table {} !".format(schemaSTAGE, cdm_table, stage_cdm_table))  
    raise

else:
  print("No new data in CDM !")

# COMMAND ----------

# DBTITLE 1,Upsert
if nrows > 0: 
  if IDLtype == 'Upsert':
    qry_scrpt = f"""
      MERGE INTO {database}.{schema}.{sfTable} SNOW
        USING (
         WITH 
          mobile as(
            SELECT
              CustomerId,
              MobileNumber,
              MobileDeviceId,
              PushNotificationOptIn,
              SMSOptIn,
              BrandId,
              InspireId
            FROM
              (
              SELECT
              MemberId as CustomerId,
              MobileNumber,
              MobileDeviceId,
              PushNotificationOptIn,
              SMSOptIn,
              BrandId,
              InspireId,
              ModifiedDate,
              ROW_NUMBER() OVER(Partition by InspireId order by ModifiedDate desc) as rn
              from
              {databaseSTAGE}.{schemaSTAGE}.cust_mobile_plr
              ) as t
            WHERE t.rn = 1
          ), 

          email as(
          SELECT
            CustomerId,
            EmailId,
            EmailOptoutFlag,
            BrandId,
            InspireId
          FROM
            (
            SELECT
            MemberId as CustomerId,
            EmailId,
            EmailOptoutFlag,
            BrandId,
            InspireId,
            ModifiedDate,
            ROW_NUMBER() OVER(Partition by InspireId order by ModifiedDate desc) as rn
            from
            {databaseSTAGE}.{schemaSTAGE}.cust_email_plr
            ) as t
          WHERE t.rn = 1
          ),

          address as(
          SELECT
            CustomerId,
            AddressLine1,
            AddressLine2,
            City,
            State,
            ZipCode,
            OptIn,
            CountryCode,
            BrandId,
            InspireId
          FROM
            (
            SELECT
            MemberId as CustomerId,
            AddressLine1,
            AddressLine2,
            City,
            State,
            ZipCode,
            OptIn,
            BrandId,
            CountryCode,
            InspireId,
            ModifiedDate,
            ROW_NUMBER() OVER(Partition by InspireId order by ModifiedDate desc) as rn
            FROM
            {databaseSTAGE}.{schemaSTAGE}.cust_address_plr
            ) as t
          WHERE t.rn = 1
          ), 

          membership as(
          SELECT 
            CustomerId
            ,LoyaltyCardNumber
            ,LoyaltyProgramId
            ,ClosestStoreId
            ,EnrollStartDate
            ,EnrollmentChannel
            ,PointBalance
            ,MembershipStatus
            ,PointsExpireDate
            ,UnsubscribeDate
            ,LastLoginDate
            ,LastStatusChangeDate
            ,ProfileCompletedStatus
            ,ProfileCompletionDate
            ,SUBSCRIBERKEY
            ,SUBSCRIBERSOURCENAME
            ,Loyalty_Tier_Nm
            ,Loyalty_Tier_Expiration_Dt
            ,Loyalty_Elite_Visit_Cnt
            ,Loyalty_Tier_Change_Dt
            ,BrandId
          FROM 
            (
            SELECT
            MemberId as CustomerId
            ,MemberCardNumber as LoyaltyCardNumber
            ,LoyaltyProgramId
            ,ClosestStoreId
            ,EnrollStartDate
            ,EnrollmentChannel
            ,PointBalance
            ,MembershipStatus
            ,PointsExpireDate
            ,UnsubscribeDate
            ,LastLoginDate
            ,LastStatusChangeDate
            ,ProfileCompletedStatus
            ,ProfileCompletionDate
            ,SUBSCRIBERKEY
            ,SUBSCRIBERSOURCENAME
            ,Loyalty_Tier_Nm
            ,Loyalty_Tier_Expiration_Dt
            ,Loyalty_Elite_Visit_Cnt
            ,Loyalty_Tier_Change_Dt
            ,BrandId
            ,ModifiedDate
            ,ROW_NUMBER() OVER(Partition by CustomerId order by ModifiedDate desc) as rn
            FROM
            {databaseSTAGE}.{schemaSTAGE}.trg_mktg_member_plr
            ) as t
          WHERE t.rn = 1
          ),

          customer as(
          SELECT
            CustomerId
            ,FirstName
            ,LastName
            ,MiddleInitial
            ,DOB
            ,Gender
            ,CustomerStatus
            ,MaritalStatus
            ,ChildrenNumber
            ,Income
            ,HouseHoldId
            ,DeliverabilityStatus
            ,IgnoreFraudSuspendFlag
            ,LoadedDate
            ,ModifiedDate
            ,BirthMonth
            ,BirthYear
            ,IsBirthDateImplied
            ,Privacy
            ,BrandId
            ,CdmLoadDate
            ,InspireId
            ,AgencyId
            ,AgencyStatus
            ,InspireCustomerType           
            ,MDMId
            ,IsMDMIDDeleted
            ,Source
          FROM 
            (
            SELECT
            MemberId as CustomerId
            ,FirstName
            ,LastName
            ,MiddleInitial
            ,DOB
            ,Gender
            ,CustomerStatus
            ,MaritalStatus
            ,ChildrenNumber
            ,Income
            ,HouseHoldId
            ,DeliverabilityStatus
            ,IgnoreFraudSuspendFlag
            ,LoadedDate
            ,ModifiedDate
            ,BirthMonth
            ,BirthYear
            ,IsBirthDateImplied
            ,Privacy
            ,BrandId
            ,CdmLoadDate
            ,InspireId
            ,AgencyId
            ,AgencyStatus
            ,InspireCustomerType
            ,MDMId
            ,IsMDMIDDeleted
            ,Source            
            ,ROW_NUMBER() OVER(Partition by InspireId order by InspireId,CustomerId,CdmLoadDate DESC) as rn
            FROM
            {databaseSTAGE}.{schemaSTAGE}.cust_profile_plr
            where InspireId is not null and cdmloaddate is not null  and IS_REAL(TRY_TO_NUMERIC(CDMLOADDATE)) = 1
            ) as t
          WHERE t.rn = 1
          ),

          customer_dim as(
          SELECT
            c.CustomerId
            ,c.FirstName
            ,c.LastName
            ,c.MiddleInitial
            ,c.DOB
            ,c.Gender
            ,c.CustomerStatus
            ,c.MaritalStatus
            ,c.ChildrenNumber
            ,c.Income
            ,c.HouseHoldId
            ,c.DeliverabilityStatus
            ,c.IgnoreFraudSuspendFlag
            ,c.LoadedDate
            ,c.ModifiedDate
            ,c.BirthMonth
            ,c.BirthYear
            ,c.IsBirthDateImplied
            ,c.Privacy
            ,c.BrandId
            ,c.InspireId
            ,c.AgencyId
            ,c.AgencyStatus
            ,c.InspireCustomerType         
            ,c.MDMId
            ,c.IsMDMIDDeleted
            ,m.LoyaltyCardNumber
            ,m.LoyaltyProgramId
            ,m.ClosestStoreId
            ,m.EnrollStartDate
            ,m.EnrollmentChannel
            ,m.PointBalance
            ,m.MembershipStatus
            ,m.PointsExpireDate
            ,m.UnsubscribeDate
            ,m.LastLoginDate
            ,m.LastStatusChangeDate
            ,m.ProfileCompletedStatus
            ,m.ProfileCompletionDate
            ,m.SUBSCRIBERKEY
            ,m.SUBSCRIBERSOURCENAME
            ,m.Loyalty_Tier_Change_Dt
            ,m.Loyalty_Tier_Nm
            ,m.Loyalty_Tier_Expiration_Dt
            ,m.Loyalty_Elite_Visit_Cnt
            ,mb.MobileNumber
            ,mb.MobileDeviceId
            ,mb.PushNotificationOptIn
            ,mb.SMSOptIn
            ,e.EmailId
            ,e.EmailOptoutFlag
            ,a.AddressLine1
            ,a.AddressLine2
            ,a.City
            ,a.State
            ,a.ZipCode
            ,a.OptIn
            ,a.CountryCode
            ,c.CdmLoadDate
            ,c.Source

          FROM customer c
          LEFT OUTER JOIN membership   m ON c.CustomerId = m.CustomerId  AND c.BrandId = m.BrandId
          LEFT OUTER JOIN address      a ON c.InspireId = a.InspireId  AND c.BrandId = a.BrandId
          LEFT OUTER JOIN mobile      mb ON c.InspireId = mb.InspireId AND c.BrandId = mb.BrandId
          LEFT OUTER JOIN email        e ON c.InspireId = e.InspireId  AND c.BrandId = e.BrandId
          )
      
              SELECT 
          CustomerId 
          ,LoyaltyProgramId
          ,ClosestStoreId
          ,HouseHoldID
          ,EmailID
          ,MobileDeviceID
          ,BrandID
          ,InspireId
          ,AgencyId
          ,AgencyStatus
          ,InspireCustomerType
          ,MDMId
          ,IsMDMIDDeleted
          ,LoyaltyCardNumber
          ,FirstName
          ,LastName
          ,MiddleInitial
          ,DOB
          ,BirthMonth
          ,BirthYear
          ,TRY_TO_BOOLEAN(IsBirthDateImplied) IsBirthDateImplied
          ,Gender
          ,EnrollmentChannel
          ,CustomerStatus
          ,TRY_TO_NUMBER(PointBalance) PointBalance
          ,MembershipStatus
          ,TRY_TO_BOOLEAN(ProfileCompletedStatus) as ProfileCompletedStatus
          ,DeliverabilityStatus
          ,MobileNumber
          ,AddressLine1
          ,AddressLine2
          ,City
          ,State
          ,ZipCode
          ,CountryCode
          ,TRY_TO_BOOLEAN(EMAILOPTOUTFLAG) as EmailOptoutFlag
          ,PushNotificationOptin
          ,SMSOptin
          ,TRY_TO_BOOLEAN(Privacy) as Privacy
          ,try_to_timestamp(UnsubscribeDate) as UnsubscribeDate
          ,try_to_timestamp(PointsExpireDate) as PointsExpireDate
          ,try_to_timestamp(ENROLLSTARTDATE) as EnrollStartDate
          ,try_to_timestamp(LastLoginDate) as LastLoginDate
          ,try_to_timestamp(LastStatusChangeDate) as LastStatusChangeDate
          ,try_to_timestamp(ProfileCompletionDate) as ProfileCompletionDate
          ,SUBSCRIBERKEY
          ,SUBSCRIBERSOURCENAME
          ,try_to_timestamp_ntz(Loyalty_Tier_Change_Dt) as Loyalty_Tier_Change_Dttm
          ,Loyalty_Tier_Nm
          ,to_date(Loyalty_Tier_Expiration_Dt, 'yyyymmdd') as Loyalty_Tier_Expiration_Dt
          ,to_number(Loyalty_Elite_Visit_Cnt) as Loyalty_Elite_Visit_Cnt
          ,Source
          ,CdmLoadDate
          ,CAST(TO_VARCHAR(CURRENT_DATE()::date, 'yyyymmdd') AS INT) AS LoadID
          ,TO_TIMESTAMP(CONVERT_TIMEZONE ('EST', current_timestamp)) AS LoadDttm    
        FROM customer_dim) CDM_TEMP

      ON REPLACE(SNOW.InspireId,'-','') = REPLACE(CDM_TEMP.InspireId,'-','')

      WHEN MATCHED AND
      (
           not(equal_null(SNOW.LoyaltyProgramId,CDM_TEMP.LoyaltyProgramId))    
        OR not(equal_null(SNOW.ClosestStoreId,CDM_TEMP.ClosestStoreId))    
        OR not(equal_null(SNOW.HouseHoldID,CDM_TEMP.HouseHoldID))       
        OR not(equal_null(SNOW.EmailID,CDM_TEMP.EmailID))           
        OR not(equal_null(SNOW.MobileDeviceID,CDM_TEMP.MobileDeviceID))    
        OR not(equal_null(SNOW.BrandID,CDM_TEMP.BrandID))           
        OR not(equal_null(SNOW.LoyaltyCardNumber,CDM_TEMP.LoyaltyCardNumber)) 
        OR not(equal_null(SNOW.FirstName,CDM_TEMP.FirstName))         
        OR not(equal_null(SNOW.LastName,CDM_TEMP.LastName))          
        OR not(equal_null(SNOW.MiddleInitial,CDM_TEMP.MiddleInitial))     
        OR not(equal_null(SNOW.DOB,CDM_TEMP.DOB))               
        OR not(equal_null(SNOW.Gender,CDM_TEMP.Gender))            
        OR not(equal_null(SNOW.EnrollmentChannel,CDM_TEMP.EnrollmentChannel)) 
        OR not(equal_null(SNOW.CustomerStatus,CDM_TEMP.CustomerStatus))    
        OR not(equal_null(SNOW.PointBalance,CDM_TEMP.PointBalance))
        OR not(equal_null(SNOW.MembershipStatus,CDM_TEMP.MembershipStatus))
        OR not(equal_null(SNOW.ProfileCompletedStatus,CDM_TEMP.ProfileCompletedStatus))
        OR not(equal_null(SNOW.DeliverabilityStatus,CDM_TEMP.DeliverabilityStatus))
        OR not(equal_null(SNOW.MobileNumber,CDM_TEMP.MobileNumber))        
        OR not(equal_null(SNOW.AddressLine1,CDM_TEMP.AddressLine1))        
        OR not(equal_null(SNOW.AddressLine2,CDM_TEMP.AddressLine2))        
        OR not(equal_null(SNOW.City,CDM_TEMP.City))                
        OR not(equal_null(SNOW.State,CDM_TEMP.State))               
        OR not(equal_null(SNOW.ZipCode,CDM_TEMP.ZipCode))             
        OR not(equal_null(SNOW.EmailOptoutFlag,CDM_TEMP.EmailOptoutFlag))
        OR not(equal_null(SNOW.PushNotificationOptin,CDM_TEMP.PushNotificationOptin))  
        OR not(equal_null(SNOW.SMSOptin,CDM_TEMP.SMSOptin))               
        OR not(equal_null(SNOW.Privacy,CDM_TEMP.Privacy))
        OR not(equal_null(SNOW.UnsubscribeDate,CDM_TEMP.UnsubscribeDate))      
        OR not(equal_null(SNOW.PointsExpireDate,CDM_TEMP.PointsExpireDate))     
        OR not(equal_null(SNOW.EnrollStartDate,CDM_TEMP.EnrollStartDate))      
        OR not(equal_null(SNOW.LastLoginDate,CDM_TEMP.LastLoginDate))        
        OR not(equal_null(SNOW.LastStatusChangeDate,CDM_TEMP.LastStatusChangeDate)) 
        OR not(equal_null(SNOW.ProfileCompletionDate,CDM_TEMP.ProfileCompletionDate))
        OR not(equal_null(SNOW.BirthMonth,CDM_TEMP.BirthMonth))          
        OR not(equal_null(SNOW.BirthYear,CDM_TEMP.BirthYear))           
        OR not(equal_null(SNOW.IsBirthDateImplied,CDM_TEMP.IsBirthDateImplied)) 
        OR not(equal_null(SNOW.CustomerId,CDM_TEMP.CustomerId))           
        OR not(equal_null(SNOW.AgencyId,CDM_TEMP.AgencyId))             
        OR not(equal_null(SNOW.AgencyStatus,CDM_TEMP.AgencyStatus))         
        OR not(equal_null(SNOW.InspireCustomerType,CDM_TEMP.InspireCustomerType))  
        OR not(equal_null(SNOW.MDMId,CDM_TEMP.MDMId))                
        OR not(equal_null(SNOW.IsMDMIDDeleted,CDM_TEMP.IsMDMIDDeleted))       
        OR not(equal_null(SNOW.CdmLoadDate,CDM_TEMP.CdmLoadDate))          
        OR not(equal_null(SNOW.CountryCode,CDM_TEMP.CountryCode))          
        OR not(equal_null(SNOW.Source,CDM_TEMP.Source))               
        OR not(equal_null(SNOW.SUBSCRIBERKEY,CDM_TEMP.SUBSCRIBERKEY))
        OR not(equal_null(SNOW.SUBSCRIBERSOURCENAME,CDM_TEMP.SUBSCRIBERSOURCENAME))
        OR not(equal_null(SNOW.Loyalty_Tier_Nm,CDM_TEMP.Loyalty_Tier_Nm))
        OR not(equal_null(SNOW.Loyalty_Tier_Expiration_Dt,CDM_TEMP.Loyalty_Tier_Expiration_Dt))   
        OR not(equal_null(SNOW.Loyalty_Elite_Visit_Cnt,CDM_TEMP.Loyalty_Elite_Visit_Cnt))   
        OR not(equal_null(SNOW.Loyalty_Tier_Change_Dttm,CDM_TEMP.Loyalty_Tier_Change_Dttm))
      )

      THEN UPDATE SET 
        SNOW.InspireId               = CDM_TEMP.InspireId
        ,SNOW.CustomerId               = CDM_TEMP.CustomerId
        ,SNOW.LoyaltyProgramId        = CDM_TEMP.LoyaltyProgramId
        ,SNOW.ClosestStoreId          = CDM_TEMP.ClosestStoreId
        ,SNOW.HouseHoldID             = CDM_TEMP.HouseHoldID
        ,SNOW.EmailID                 = CDM_TEMP.EmailID
        ,SNOW.MobileDeviceID          = CDM_TEMP.MobileDeviceID
        ,SNOW.BrandID                 = CDM_TEMP.BrandID
        ,SNOW.LoyaltyCardNumber       = CDM_TEMP.LoyaltyCardNumber
        ,SNOW.FirstName               = CDM_TEMP.FirstName
        ,SNOW.LastName                = CDM_TEMP.LastName
        ,SNOW.MiddleInitial           = CDM_TEMP.MiddleInitial
        ,SNOW.DOB                     = CDM_TEMP.DOB
        ,SNOW.Gender                  = CDM_TEMP.Gender
        ,SNOW.EnrollmentChannel       = CDM_TEMP.EnrollmentChannel
        ,SNOW.CustomerStatus          = CDM_TEMP.CustomerStatus
        ,SNOW.PointBalance            = CDM_TEMP.PointBalance
        ,SNOW.MembershipStatus        = CDM_TEMP.MembershipStatus
        ,SNOW.ProfileCompletedStatus  = CDM_TEMP.ProfileCompletedStatus
        ,SNOW.DeliverabilityStatus    = CDM_TEMP.DeliverabilityStatus
        ,SNOW.MobileNumber            = CDM_TEMP.MobileNumber
        ,SNOW.AddressLine1            = CDM_TEMP.AddressLine1
        ,SNOW.AddressLine2            = CDM_TEMP.AddressLine2
        ,SNOW.City                    = CDM_TEMP.City
        ,SNOW.State                   = CDM_TEMP.State
        ,SNOW.ZipCode                 = CDM_TEMP.ZipCode
        ,SNOW.EmailOptoutFlag         = CDM_TEMP.EmailOptoutFlag
        ,SNOW.PushNotificationOptin   = CDM_TEMP.PushNotificationOptin
        ,SNOW.SMSOptin                = CDM_TEMP.SMSOptin
        ,SNOW.Privacy                 = CDM_TEMP.Privacy
        ,SNOW.UnsubscribeDate         = CDM_TEMP.UnsubscribeDate
        ,SNOW.PointsExpireDate        = CDM_TEMP.PointsExpireDate
        ,SNOW.EnrollStartDate         = CDM_TEMP.EnrollStartDate
        ,SNOW.LastLoginDate           = CDM_TEMP.LastLoginDate
        ,SNOW.LastStatusChangeDate    = CDM_TEMP.LastStatusChangeDate
        ,SNOW.ProfileCompletionDate   = CDM_TEMP.ProfileCompletionDate
        ,SNOW.LoadDttm                = CDM_TEMP.LoadDttm
        ,SNOW.BirthMonth   = CDM_TEMP.BirthMonth
        ,SNOW.BirthYear   = CDM_TEMP.BirthYear
        ,SNOW.IsBirthDateImplied   = CDM_TEMP.IsBirthDateImplied
        ,SNOW.AgencyId   = CDM_TEMP.AgencyId
        ,SNOW.AgencyStatus   = CDM_TEMP.AgencyStatus
        ,SNOW.InspireCustomerType   = CDM_TEMP.InspireCustomerType
        ,SNOW.MDMId   = CDM_TEMP.MDMId
        ,SNOW.IsMDMIDDeleted   = CDM_TEMP.IsMDMIDDeleted
        ,SNOW.CdmLoadDate = CDM_TEMP.CdmLoadDate
        ,SNOW.CountryCode = CDM_TEMP.CountryCode
        ,SNOW.Source = CDM_TEMP.Source
        ,SNOW.SUBSCRIBERKEY =CDM_TEMP.SUBSCRIBERKEY 
        ,SNOW.SUBSCRIBERSOURCENAME =CDM_TEMP.SUBSCRIBERSOURCENAME
        ,SNOW.Loyalty_Tier_Nm =CDM_TEMP.Loyalty_Tier_Nm
        ,SNOW.Loyalty_Tier_Expiration_Dt =CDM_TEMP.Loyalty_Tier_Expiration_Dt
        ,SNOW.Loyalty_Elite_Visit_Cnt =CDM_TEMP.Loyalty_Elite_Visit_Cnt
        ,SNOW.Loyalty_Tier_Change_Dttm =CDM_TEMP.Loyalty_Tier_Change_Dttm

      WHEN NOT MATCHED THEN INSERT (
          CustomerId 
          ,LoyaltyProgramId
          ,ClosestStoreId
          ,HouseHoldID
          ,EmailID
          ,MobileDeviceID
          ,BrandID
          ,InspireId
          ,AgencyId
          ,AgencyStatus
          ,InspireCustomerType      
          ,MDMId
          ,IsMDMIDDeleted
          ,LoyaltyCardNumber
          ,FirstName
          ,LastName
          ,MiddleInitial
          ,DOB
          ,BirthMonth
          ,BirthYear
          ,IsBirthDateImplied
          ,Gender
          ,EnrollmentChannel
          ,CustomerStatus
          ,PointBalance
          ,MembershipStatus
          ,ProfileCompletedStatus
          ,DeliverabilityStatus
          ,MobileNumber
          ,AddressLine1
          ,AddressLine2
          ,City
          ,State
          ,ZipCode
          ,CountryCode
          ,EmailOptoutFlag
          ,PushNotificationOptin
          ,SMSOptin
          ,Privacy
          ,UnsubscribeDate
          ,PointsExpireDate
          ,EnrollStartDate
          ,LastLoginDate
          ,LastStatusChangeDate
          ,ProfileCompletionDate
          ,SUBSCRIBERKEY
          ,SUBSCRIBERSOURCENAME
          ,Loyalty_Tier_Change_Dttm
          ,Loyalty_Tier_Nm
          ,Loyalty_Tier_Expiration_Dt
          ,Loyalty_Elite_Visit_Cnt
          ,Source
          ,LoadID
          ,LoadDttm
          ,CdmLoadDate
        )
        VALUES   (
          CDM_TEMP.CustomerId 
          ,CDM_TEMP.LoyaltyProgramId
          ,CDM_TEMP.ClosestStoreId
          ,CDM_TEMP.HouseHoldID
          ,CDM_TEMP.EmailID
          ,CDM_TEMP.MobileDeviceID
          ,CDM_TEMP.BrandID
          ,CDM_TEMP.InspireId
          ,CDM_TEMP.AgencyId
          ,CDM_TEMP.AgencyStatus
          ,CDM_TEMP.InspireCustomerType
          
          ,CDM_TEMP.MDMId
          ,CDM_TEMP.IsMDMIDDeleted
          ,CDM_TEMP.LoyaltyCardNumber
          ,CDM_TEMP.FirstName
          ,CDM_TEMP.LastName
          ,CDM_TEMP.MiddleInitial
          ,CDM_TEMP.DOB
		  ,CDM_TEMP.BirthMonth
          ,CDM_TEMP.BirthYear
          ,CDM_TEMP.IsBirthDateImplied
          ,CDM_TEMP.Gender
          ,CDM_TEMP.EnrollmentChannel
          ,CDM_TEMP.CustomerStatus
          ,CDM_TEMP.PointBalance
          ,CDM_TEMP.MembershipStatus
          ,CDM_TEMP.ProfileCompletedStatus
          ,CDM_TEMP.DeliverabilityStatus
          ,CDM_TEMP.MobileNumber
          ,CDM_TEMP.AddressLine1
          ,CDM_TEMP.AddressLine2
          ,CDM_TEMP.City
          ,CDM_TEMP.State
          ,CDM_TEMP.ZipCode
          ,CDM_TEMP.CountryCode
          ,CDM_TEMP.EmailOptoutFlag
          ,CDM_TEMP.PushNotificationOptin
          ,CDM_TEMP.SMSOptin
          ,CDM_TEMP.Privacy
          ,CDM_TEMP.UnsubscribeDate
          ,CDM_TEMP.PointsExpireDate
          ,CDM_TEMP.EnrollStartDate
          ,CDM_TEMP.LastLoginDate
          ,CDM_TEMP.LastStatusChangeDate
          ,CDM_TEMP.ProfileCompletionDate
          ,CDM_TEMP.SUBSCRIBERKEY
          ,CDM_TEMP.SUBSCRIBERSOURCENAME
          ,CDM_TEMP.Loyalty_Tier_Change_Dttm
          ,CDM_TEMP.Loyalty_Tier_Nm
          ,CDM_TEMP.Loyalty_Tier_Expiration_Dt
          ,CDM_TEMP.Loyalty_Elite_Visit_Cnt
          ,CDM_TEMP.Source
          ,CDM_TEMP.LoadID
          ,CDM_TEMP.LoadDttm
          ,CDM_TEMP.CdmLoadDate
        )
    """
    curIDL.execute(qry_scrpt)      
    
    print("{}.{} successfully loaded into Snowflake !".format(schema, sfTable))
  elif IDLtype == 'Insert':
    print("{} has got a primary key --> do 'merge'".format(cdm_table))
  else:
    print(" IDL Type not valid !")
    
else:
  print("No new data in CDM !")

# COMMAND ----------

print(qry_scrpt)

# COMMAND ----------

# DBTITLE 1,Safety Check Query
pk = 'InspireId'
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
 