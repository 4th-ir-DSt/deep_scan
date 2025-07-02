# Databricks notebook source
Preliminary
1. Ensure that tables are correct and up to date on ETL cluster
2. Ensure the passthrough cluster is turned off

On read cluster
1. Ensure the cluster is NOT In passthrough mode
2. Drop all ETL tables using drop table if exists
3. Switch mounts to token based ( drop and recreate = true)
4. run create . alter notebook with createTableOnly set to True
5. shutdown NON passthrough cluster
6. start passthrough cluster
7. Switch mounts to passthrough ( drop and recreate = true)


1. We need to apply the changes to misc functions notebook and create/alter in dev
2. create a notebook for this purpose..

# COMMAND ----------

# DBTITLE 1,Run this to establish code for mounts
#code for mount scripts goes here

from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import col
import pandas as pd
import json
  
def getADLSTokenConfigs():
  configs = {"fs.azure.account.auth.type": "OAuth",
         "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
         "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = 'data-scope-01', key = "spn-az-edm-adlsg2-01-data-client-01"),
         "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = 'data-scope-01', key = "spn-az-edm-adlsg2-01-data-password-01"),
         "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/2a01fed0-99fe-4589-8d1f-8e7c1ca91bfd/oauth2/token",
         "fs.azure.createRemoteFileSystemDuringInitialization": "false"}
  return configs

def getConnection():
  connection = dbutils.secrets.get(scope = 'data-scope-01', key = "adlsgen2-dbrks-connection-01")
  return connection

def getPassthroughConfigs():
  configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
  }
  return configs

def initialiseMounts():
  Schema = StructType([
    StructField('mountPoint', StringType(), True),
    StructField('source', StringType(), True),
    StructField('encryptionType', StringType(), True)
  ])

  mounts = dbutils.fs.mounts()
  mountsDF = (convertPandasToSparkDfWithSchema(mounts,Schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)          
              .filter(col('mountPoint')
                      .contains('/mnt'))
              .selectExpr("mountPoint", "source")
              .select('mountPoint')
             ).toPandas()
  return mountsDF

def createMount_(container_name, sub_folder, mnt_point, configs, connection):
  print("creating mount point '{0}'".format(mnt_point))
  dbutils.fs.mount(
    source = "abfss://{0}@{1}.dfs.core.windows.net/{2}".format(container_name, connection, sub_folder),
    mount_point = mnt_point,
    extra_configs = configs)
  
def createMount(container_name, sub_folder, drop_and_recreate_if_exists, mountsDF, configs, connection):
  mnt_point = "/mnt/{0}/{1}".format(container_name, sub_folder)
  
  if mnt_point[len(mnt_point) - 1] == '/':
    mnt_point = mnt_point[0:len(mnt_point)-1]
  print(mnt_point)
  if mnt_point in mountsDF.values in mountsDF.values:
    print("mount point '{0}' exists".format(mnt_point))
    if drop_and_recreate_if_exists:
      print("dropping mount '{0}'".format(mnt_point))
      dbutils.fs.unmount(mnt_point)
        
      print("creating mount '{0}'".format(mnt_point))
      createMount_(container_name, sub_folder, mnt_point, configs, connection)
  else:
    print("creating mount '{0}'".format(mnt_point))
    createMount_(container_name, sub_folder, mnt_point, configs, connection)

def getMountList():
  mountList = [ ('refined','')
              , ('sensitive','')
              , ('person', '')
#               , ('raw', '')
#               , ('staging','')
#               , ('refined','')
#               , ('sensitive','')
#               , ('person', '')
#               , ('temp', '')
              ]
  return mountList

def recreateADLSTokenMounts():
  drop_and_recreate_if_exists = True

  mountsDF = initialiseMounts()
  connection = getConnection()
  configs = getADLSTokenConfigs()
  mountList = getMountList()

  for x in mountList:
    container_name = x[0]
    sub_folder = x[1]
    createMount(container_name, sub_folder, drop_and_recreate_if_exists, mountsDF, configs, connection)

def recreatePassthroughMounts():
  drop_and_recreate_if_exists = True

  mountsDF = initialiseMounts()
  connection = getConnection()
  configs = getPassthroughConfigs()
  mountList = getMountList()

  for x in mountList:
    container_name = x[0]
    sub_folder = x[1]
    createMount(container_name, sub_folder, drop_and_recreate_if_exists, mountsDF, configs, connection)

# COMMAND ----------

# MAGIC %md
# MAGIC #Preliminary

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Ensure that tables are correct and up to date on ETL cluster
# MAGIC 
# MAGIC Step 2 - Ensure passthrough cluster is turned off

# COMMAND ----------

# MAGIC %md
# MAGIC # Main

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Ensure the cluster is NOT in passthrough mode

# COMMAND ----------

# DBTITLE 1,Step 2 - Drop all ETL tables using drop table if exists
#drop table if exists script
tablesDF = spark.sql("show tables")

tablesDF = tablesDF.where("tableName like 'refined%' or tableName like 'sensitive%' or tableName like 'person%'")

for table in tablesDF.collect():
  tableName = table["tableName"]
  dropStatement = 'DROP TABLE IF EXISTS {}'.format(tableName)
  print(dropStatement)
#   spark.sql(dropStatement)

# COMMAND ----------

# DBTITLE 1,Step 3 - Switch mounts to token based ( drop and recreate = true)
# drop and create True

recreateADLSTokenMounts()

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 - run create . alter notebook with createTableOnly set to True
# MAGIC 
# MAGIC Step 5 - Shutdown NON passthrough cluster
# MAGIC 
# MAGIC Step 6 - Start passthrough cluster and attach this notebook
# MAGIC 
# MAGIC Step 7 - Run code initialisation cell "Run this to establish code for mounts"

# COMMAND ----------

# DBTITLE 1,Step 8 - Switch mounts to passthrough ( drop and recreate = true)
# drop and create True
# passthrough mode False

recreatePassthroughMounts()