# Databricks notebook source
dbutils.widgets.dropdown("DataBricks", "cdm_prod", 
                         ["cdm_qa", "cdm_prod","cdm_dev"], "Databricks IDL database Env ")

dbutils.widgets.dropdown("databaseIDL", "POLARIS", 
                         ["POLARIS_DEV", "POLARIS_TEST","POLARIS"], "Snowflake IDL database Env ")

dbutils.widgets.dropdown("schemaIDL", "CUDM", 
                         ["CUDM", "SADM","OPDM", "SHDM"], "Snowflake IDL schema ")

dbutils.widgets.dropdown("databaseSTAGE", "CDM", 
                         ["CDM","CDM_TEST","CDM_DEV"], "Snowflake STAGE database Env ")

dbutils.widgets.dropdown("schemaSTAGE", "STAGING", 
                         ["STAGING"], "Snowflake STAGE schema ")

dbutils.widgets.dropdown("sfIDLTable", "OFFER_DIM", 
                         ["OFFER_DIM"], "Snowflake table ")

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

# DBTITLE 1,Fetch snowflake connection string 
# MAGIC %run ../Snowflake/ConnectionString

# COMMAND ----------

# DBTITLE 1,Fetch Execution Logger
# MAGIC %run ../Governance/ExecutionLogger

# COMMAND ----------

# DBTITLE 1,Execution Log Parameters
# Table for storing logs 
schemaLog  = dbutils.widgets.get("sfLogSchema")
sfLogTable = dbutils.widgets.get("sfLogTable")

# Execution ID = Name of Notebook + Timestamp with Millisecond + Random Number
nbName   = "Prod_IDL_OFFER_DIM"
tz       = timezone('EST')
tmpstamp = datetime.now(tz).strftime('%Y%m%d%H%M%S%S') 
rnumber  = int(np.random.rand() * 1000000)

execId   = nbName + "_" + tmpstamp + str(rnumber)

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
cdm_table      = 'trg_mktg_offer_plr'
stage_cdm_table      = 'trg_mktg_offer_plr'

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
  #loadDate = curIDL.execute("SELECT MAX(TO_DATE(CAST(CdmLoadDate AS STRING), 'YYYYMMDD')) FROM {}.{}.{}".format(schema, sfTable)).fetchone()[0].strftime('%Y%m%d')
  loadDate = curIDL.execute("SELECT IFNULL(MAX(TO_DATE(CAST(CdmLoadDate AS STRING), 'YYYYMMDD')),'19700815') FROM {}.{}.{}".format(database,schema, sfTable)).fetchone()[0].strftime('%Y%m%d')
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
# df = df.dropDuplicates()
# nrowsDD = df.count()
# print('Delta load dataframe length in rows after dedup: ' + str(nrowsDD))

# if nrows != nrowsDD:
#   print('Duplicates found in delta load - identical rows !')

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

merge_condition = f"""
    MERGE INTO {database}.CUDM.OFFER_DIM SNOW
    USING (SELECT 
         OfferCode
          ,BrandId
          ,UnqOfferId
          ,Parent_Offer_Id
          ,VariantId
          ,OfferSeqKey
          ,OfferName
          ,OfferReportDesc
          ,OfferShortDesc
          ,OfferDesc
          ,TRY_TO_BOOLEAN(OPTINREQUIRED) OptInRequired
          ,Locations
          ,Exclusions
          ,OfferImageName
          ,TermsAndConditions
          ,MomentEligibility
          ,StandardExclusions
          ,OtherExclusions
          ,TRY_TO_NUMBER(NumberOfPointsAwarded) NumberOfPointsAwarded
          ,Limits
          ,StoreLocation
          ,DayPart
          ,MemberListNeeded
          ,StoreType
          ,TRY_TO_BOOLEAN(ONETIMEOFFER) OneTimeOffer
          ,TRY_TO_BOOLEAN(TRIVIANEEDED) TriviaNeeded
          ,TRY_TO_BOOLEAN(CHECKINFLAG) CheckInFlag
          ,OfferPriority
          ,OfferType
          ,Award
          ,ProductPLUs
          ,TriviaQuestion
          ,TriviaAnswers
          ,TRY_TO_BOOLEAN(TARGETOFFER) TargetOffer
          ,Privacy
          ,ValidityPeriod
          ,ValidityUnit
          ,DNAIncentiveType
          ,TRY_TO_NUMBER(DNAIncentiveValue,8,2) DNAIncentiveValue
          ,DNACampaignType
          ,DNADiscountProduct
          ,DNAProductCategory
          ,DNAActionRequired
          ,TRY_TO_NUMBER(DNAMinimumSpend) DNAMinimumSpend
          ,TRY_TO_NUMBER(Dnaofferweekincampaign) DNAOfferWeekInCampaign
          ,TRY_TO_TIMESTAMP(DATEOFFERVISIBLE) DateOfferVisible
          ,TRY_TO_TIMESTAMP(OFFERSTARTDATE) OfferStartDate
          ,TRY_TO_TIMESTAMP(OFFERENDDATE) OfferEndDate
          ,Source
          ,Source_Application_Typ
          ,DNA_Discount_Product_Level_Cd
          ,DNA_Offer_Redemption_Channel_Typ
          ,Status_Code
          ,Offer_Image2_Nm
          ,Sys_Offer_Id
          ,CdmLoadDate
          ,CAST(TO_VARCHAR(CURRENT_DATE()::date, 'yyyymmdd') AS INT) AS LoadID
          ,TO_TIMESTAMP(CONVERT_TIMEZONE ('EST', current_timestamp)) AS LoadDttm
    FROM {databaseSTAGE}.STAGING.trg_mktg_offer_plr) CDM

    ON SNOW.OfferCode = CDM.OfferCode 
       AND SNOW.BrandId = CDM.BrandId 
       AND SNOW.Source = CDM.Source

    WHEN MATCHED AND 
           ( IFNULL(SNOW.OfferCode,'')       != IFNULL(CDM.OfferCode,'')
          OR IFNULL(SNOW.BrandId,'')         != IFNULL(CDM.BrandId,'')
          OR IFNULL(SNOW.UnqOfferId              ,'') != IFNULL(CDM.UnqOfferId,'')
          OR IFNULL(SNOW.Parent_Offer_Id         ,'') != IFNULL(CDM.Parent_Offer_Id,'')
          OR IFNULL(SNOW.VariantId               ,'') != IFNULL(CDM.VariantId,'')
          OR IFNULL(SNOW.OfferSeqKey             ,'') != IFNULL(CDM.OfferSeqKey,'')
          OR IFNULL(SNOW.OfferName               ,'') != IFNULL(CDM.OfferName,'')
          OR IFNULL(SNOW.OfferReportDesc         ,'') != IFNULL(CDM.OfferReportDesc,'')
          OR IFNULL(SNOW.OfferShortDesc          ,'') != IFNULL(CDM.OfferShortDesc,'')
          OR IFNULL(SNOW.OfferDesc               ,'') != IFNULL(CDM.OfferDesc,'')
          OR IFNULL(TO_VARCHAR(SNOW.OptInRequired),'') != IFNULL(TO_VARCHAR(CDM.OptInRequired),'')
          OR IFNULL(SNOW.Locations               ,'') != IFNULL(CDM.Locations,'')
          OR IFNULL(SNOW.Exclusions              ,'') != IFNULL(CDM.Exclusions,'')
          OR IFNULL(SNOW.OfferImageName          ,'') != IFNULL(CDM.OfferImageName,'')
          OR IFNULL(SNOW.TermsAndConditions      ,'') != IFNULL(CDM.TermsAndConditions,'')
          OR IFNULL(SNOW.MomentEligibility       ,'') != IFNULL(CDM.MomentEligibility,'')
          OR IFNULL(SNOW.StandardExclusions      ,'') != IFNULL(CDM.StandardExclusions,'')
          OR IFNULL(SNOW.OtherExclusions         ,'') != IFNULL(CDM.OtherExclusions,'')
          OR IFNULL(SNOW.NumberOfPointsAwarded   ,-99) != IFNULL(CDM.NumberOfPointsAwarded,-99)
          OR IFNULL(SNOW.Limits                  ,'') != IFNULL(CDM.Limits,'')
          OR IFNULL(SNOW.StoreLocation           ,'') != IFNULL(CDM.StoreLocation,'')
          OR IFNULL(SNOW.DayPart                 ,'') != IFNULL(CDM.DayPart,'')
          OR IFNULL(SNOW.MemberListNeeded        ,'') != IFNULL(CDM.MemberListNeeded,'')
          OR IFNULL(SNOW.StoreType               ,'') != IFNULL(CDM.StoreType,'')
          OR IFNULL(TO_VARCHAR(SNOW.OneTimeOffer),'') != IFNULL(TO_VARCHAR(CDM.OneTimeOffer),'')
          OR IFNULL(TO_VARCHAR(SNOW.TriviaNeeded),'') != IFNULL(TO_VARCHAR(CDM.TriviaNeeded),'')
          OR IFNULL(TO_VARCHAR(SNOW.CheckInFlag) ,'') != IFNULL(TO_VARCHAR(CDM.CheckInFlag),'')
          OR IFNULL(SNOW.OfferPriority           ,'') != IFNULL(CDM.OfferPriority,'')
          OR IFNULL(SNOW.OfferType,'') != IFNULL(CDM.OfferType,'') 
          OR IFNULL(SNOW.ProductPLUs,'')!=IFNULL(CDM.ProductPLUs,'')
          OR IFNULL(SNOW.TriviaQuestion          ,'') != IFNULL(CDM.TriviaQuestion,'')
          OR IFNULL(SNOW.TriviaAnswers           ,'') != IFNULL(CDM.TriviaAnswers,'')
          OR IFNULL(TO_VARCHAR(SNOW.TargetOffer)        ,'') != IFNULL(TO_VARCHAR(CDM.TargetOffer),'')
          OR IFNULL(SNOW.Privacy                 ,'') != IFNULL(CDM.Privacy,'')
          OR IFNULL(SNOW.ValidityPeriod          ,'') != IFNULL(CDM.ValidityPeriod,'')
          OR IFNULL(SNOW.ValidityUnit            ,'') != IFNULL(CDM.ValidityUnit,'')
          OR IFNULL(SNOW.DNAIncentiveType        ,'') != IFNULL(CDM.DNAIncentiveType,'')
          OR IFNULL(SNOW.DNAIncentiveValue       ,-99) != IFNULL(CDM.DNAIncentiveValue,-99)
          OR IFNULL(SNOW.DNACampaignType         ,'') != IFNULL(CDM.DNACampaignType,'')
          OR IFNULL(SNOW.DNADiscountProduct      ,'') != IFNULL(CDM.DNADiscountProduct,'')
          OR IFNULL(SNOW.DNAProductCategory      ,'') != IFNULL(CDM.DNAProductCategory,'')
          OR IFNULL(SNOW.DNAActionRequired       ,'') != IFNULL(CDM.DNAActionRequired,'')
          OR IFNULL(SNOW.DNAMinimumSpend         ,-99) != IFNULL(CDM.DNAMinimumSpend,-99)
          OR IFNULL(SNOW.DNAOfferWeekInCampaign  ,-99) != IFNULL(CDM.DNAOfferWeekInCampaign,-99)
          OR IFNULL(SNOW.DateOfferVisible ,'1099-12-30 00:00:00.000') != IFNULL(CDM.DateOfferVisible,'1099-12-30 00:00:00.000')
          OR IFNULL(SNOW.OfferStartDate,'1099-12-30 00:00:00.000') != IFNULL(CDM.OfferStartDate,'1099-12-30 00:00:00.000')
          OR IFNULL(SNOW.OfferEndDate,'1099-12-30 00:00:00.000') != IFNULL(CDM.OfferEndDate,'1099-12-30 00:00:00.000')
          OR IFNULL(SNOW.Source_Application_Typ   ,'') != IFNULL(CDM.Source_Application_Typ,'')
          OR IFNULL(SNOW.DNA_Discount_Product_Level_Cd ,'') != IFNULL(CDM.DNA_Discount_Product_Level_Cd,'')
          OR IFNULL(SNOW.DNA_Offer_Redemption_Channel_Typ ,'') != IFNULL(CDM.DNA_Offer_Redemption_Channel_Typ,'')
          OR IFNULL(SNOW.Status_CD             ,'') != IFNULL(CDM.Status_Code,'')
          OR IFNULL(SNOW.Offer_Image2_Nm         ,'') != IFNULL(CDM.Offer_Image2_Nm,'')
          OR IFNULL(SNOW.System_Offer_Id            ,'') != IFNULL(CDM.Sys_Offer_Id,'')
          OR IFNULL(SNOW.CdmLoadDate             ,'') != IFNULL(CDM.CdmLoadDate,'')
          OR IFNULL(SNOW.Source, '') != IFNULL(CDM.Source,'')
          OR IFNULL(SNOW.Award                   ,'') != IFNULL(CDM.Award,''))
    THEN UPDATE SET 
     SNOW.OfferCode                 =  CDM.OfferCode
      ,SNOW.BrandId                 =  CDM.BrandId
      ,SNOW.UnqOfferId              =  CDM.UnqOfferId
      ,SNOW.Parent_Offer_Id         =  CDM.Parent_Offer_Id
      ,SNOW.VariantId               =  CDM.VariantId
      ,SNOW.OfferSeqKey             =  CDM.OfferSeqKey
      ,SNOW.OfferName               =  CDM.OfferName
      ,SNOW.OfferReportDesc         =  CDM.OfferReportDesc
      ,SNOW.OfferShortDesc          =  CDM.OfferShortDesc
      ,SNOW.OfferDesc               =  CDM.OfferDesc
      ,SNOW.OptInRequired           =  CDM.OptInRequired
      ,SNOW.Locations               =  CDM.Locations
      ,SNOW.Exclusions              =  CDM.Exclusions
      ,SNOW.OfferImageName          =  CDM.OfferImageName
      ,SNOW.TermsAndConditions      =  CDM.TermsAndConditions
      ,SNOW.MomentEligibility       =  CDM.MomentEligibility
      ,SNOW.StandardExclusions      =  CDM.StandardExclusions
      ,SNOW.OtherExclusions         =  CDM.OtherExclusions
      ,SNOW.NumberOfPointsAwarded   =  CDM.NumberOfPointsAwarded
      ,SNOW.Limits                  =  CDM.Limits
      ,SNOW.StoreLocation           =  CDM.StoreLocation
      ,SNOW.DayPart                 =  CDM.DayPart
      ,SNOW.MemberListNeeded        =  CDM.MemberListNeeded
      ,SNOW.StoreType               =  CDM.StoreType
      ,SNOW.OneTimeOffer            =  CDM.OneTimeOffer
      ,SNOW.TriviaNeeded            =  CDM.TriviaNeeded
      ,SNOW.CheckInFlag             =  CDM.CheckInFlag
      ,SNOW.OfferPriority           =  CDM.OfferPriority
      ,SNOW.OfferType               =  CDM.OfferType
      ,SNOW.ProductPLUs             =  CDM.ProductPLUs
      ,SNOW.TriviaQuestion          =  CDM.TriviaQuestion
      ,SNOW.TriviaAnswers           =  CDM.TriviaAnswers
      ,SNOW.TargetOffer             =  CDM.TargetOffer
      ,SNOW.Privacy                 =  CDM.Privacy
      ,SNOW.ValidityPeriod          =  CDM.ValidityPeriod
      ,SNOW.ValidityUnit            =  CDM.ValidityUnit
      ,SNOW.DNAIncentiveType        =  CDM.DNAIncentiveType
      ,SNOW.DNAIncentiveValue       =  CDM.DNAIncentiveValue
      ,SNOW.DNACampaignType         =  CDM.DNACampaignType
      ,SNOW.DNADiscountProduct      =  CDM.DNADiscountProduct
      ,SNOW.DNAProductCategory      =  CDM.DNAProductCategory
      ,SNOW.DNAActionRequired       =  CDM.DNAActionRequired
      ,SNOW.DNAMinimumSpend         =  CDM.DNAMinimumSpend
      ,SNOW.DNAOfferWeekInCampaign  =  CDM.DNAOfferWeekInCampaign
      ,SNOW.DateOfferVisible        =  CDM.DateOfferVisible
      ,SNOW.OfferStartDate          =  CDM.OfferStartDate
      ,SNOW.OfferEndDate            =  CDM.OfferEndDate
      ,SNOW.Source                  =  CDM.Source
      ,SNOW.Source_Application_Typ           =  CDM.Source_Application_Typ
      ,SNOW.DNA_Discount_Product_Level_Cd    =  CDM.DNA_Discount_Product_Level_Cd
      ,SNOW.DNA_Offer_Redemption_Channel_Typ =  CDM.DNA_Offer_Redemption_Channel_Typ
      ,SNOW.Status_CD               =  CDM.Status_Code
      ,SNOW.Offer_Image2_Nm         =  CDM.Offer_Image2_Nm
      ,SNOW.System_Offer_Id            =  CDM.Sys_Offer_Id
      ,SNOW.CdmLoadDate             =  CDM.CdmLoadDate
      ,SNOW.LoadDttm                =  CDM.LoadDttm
      ,SNOW.Award                   =  CDM.Award

    WHEN NOT MATCHED THEN INSERT (
        OfferCode
        ,BrandId
        ,UnqOfferId
        ,Parent_Offer_Id
        ,VariantId
        ,OfferSeqKey
        ,OfferName
        ,OfferReportDesc
        ,OfferShortDesc
        ,OfferDesc
        ,OptInRequired
        ,Locations
        ,Exclusions
        ,OfferImageName
        ,TermsAndConditions
        ,MomentEligibility
        ,StandardExclusions
        ,OtherExclusions
        ,NumberOfPointsAwarded
        ,Limits
        ,StoreLocation
        ,DayPart
        ,MemberListNeeded
        ,StoreType
        ,OneTimeOffer
        ,TriviaNeeded
        ,CheckInFlag
        ,OfferPriority
        ,OfferType
        ,Award
        ,ProductPLUs
        ,TriviaQuestion
        ,TriviaAnswers
        ,TargetOffer
        ,Privacy
        ,ValidityPeriod
        ,ValidityUnit
        ,DNAIncentiveType
        ,DNAIncentiveValue
        ,DNACampaignType
        ,DNADiscountProduct
        ,DNAProductCategory
        ,DNAActionRequired
        ,DNAMinimumSpend
        ,DNAOfferWeekInCampaign
        ,DateOfferVisible
        ,OfferStartDate
        ,OfferEndDate
        ,Source
        ,Source_Application_Typ
        ,DNA_Discount_Product_Level_Cd
        ,DNA_Offer_Redemption_Channel_Typ
        ,Status_CD
        ,Offer_Image2_Nm
        ,System_Offer_Id
        ,CdmLoadDate
        ,LoadID
        ,LoadDttm
      )
      VALUES   (
       CDM.OfferCode
        ,CDM.BrandId
        ,CDM.UnqOfferId
        ,CDM.Parent_Offer_Id
        ,CDM.VariantId
        ,CDM.OfferSeqKey
        ,CDM.OfferName
        ,CDM.OfferReportDesc
        ,CDM.OfferShortDesc
        ,CDM.OfferDesc
        ,CDM.OptInRequired
        ,CDM.Locations
        ,CDM.Exclusions
        ,CDM.OfferImageName
        ,CDM.TermsAndConditions
        ,CDM.MomentEligibility
        ,CDM.StandardExclusions
        ,CDM.OtherExclusions
        ,CDM.NumberOfPointsAwarded
        ,CDM.Limits
        ,CDM.StoreLocation
        ,CDM.DayPart
        ,CDM.MemberListNeeded
        ,CDM.StoreType
        ,CDM.OneTimeOffer
        ,CDM.TriviaNeeded
        ,CDM.CheckInFlag
        ,CDM.OfferPriority
        ,CDM.OfferType
        ,CDM.Award
        ,CDM.ProductPLUs
        ,CDM.TriviaQuestion
        ,CDM.TriviaAnswers
        ,CDM.TargetOffer
        ,CDM.Privacy
        ,CDM.ValidityPeriod
        ,CDM.ValidityUnit
        ,CDM.DNAIncentiveType
        ,CDM.DNAIncentiveValue
        ,CDM.DNACampaignType
        ,CDM.DNADiscountProduct
        ,CDM.DNAProductCategory
        ,CDM.DNAActionRequired
        ,CDM.DNAMinimumSpend
        ,CDM.DNAOfferWeekInCampaign
        ,CDM.DateOfferVisible
        ,CDM.OfferStartDate
        ,CDM.OfferEndDate
        ,CDM.Source
        ,CDM.Source_Application_Typ
        ,CDM.DNA_Discount_Product_Level_Cd
        ,CDM.DNA_Offer_Redemption_Channel_Typ
        ,CDM.Status_Code
        ,CDM.Offer_Image2_Nm
        ,CDM.Sys_Offer_Id
        ,CDM.CdmLoadDate
        ,CDM.LoadID
        ,CDM.LoadDttm)"""

# COMMAND ----------

# DBTITLE 1,Merge - Insert Fact
if nrows > 0: 
  if IDLtype == 'Upsert':
    curIDL.execute(merge_condition.format(database, schema, sfTable, databaseSTAGE, schemaSTAGE, stage_cdm_table))
    print("{}.{}.{} successfully loaded into Snowflake !".format(database,schema, cdm_table))    
  elif IDLtype == 'Insert':
    print("{} has got a unique primary key --> do 'merge'".format(cdm_table))
    
  else:
    print(" IDL Type not valid !")
    
else:
  print("No new data in CDM !")

# COMMAND ----------

# DBTITLE 1,Safety Check Query
pk = 'OfferCode'
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

