# Databricks notebook source
# DBTITLE 1,Establish SYN Connection
import pyodbc
import numpy as np
import re
import pandas as pd
dbconnDw = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-syn-connection')
connDw = pyodbc.connect(dbconnDw, autocommit = True)
cursorDw = connDw.cursor()

# COMMAND ----------

# DBTITLE 1,DP_Policy_Data
sqlObject_dp_policy = """select * from Rec.vw_Policy_Data"""
object_dp_policy = pd.read_sql(sqlObject_dp_policy, connDw)
dfObject_dp_policy = spark.createDataFrame(object_dp_policy)
dfObject_dp_policy.createOrReplaceTempView("dp_policyData")

# COMMAND ----------

# DBTITLE 1,Store the data in the designated storage account _ dp_policydata
(dfObject_dp_policy
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .format("delta")
 .save("dbfs:/mnt/raw/delta/red_dp_recon/DP_POLICY_DATA"))

# COMMAND ----------

# DBTITLE 1,Create the dp_policydata table in red_dp_recon database 
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS red_dp_recon.dp_policydata;
# MAGIC CREATE TABLE red_dp_recon.dp_policydata
# MAGIC USING delta
# MAGIC OPTIONS (path "dbfs:/mnt/raw/delta/red_dp_recon/DP_POLICY_DATA")

# COMMAND ----------

# DBTITLE 1,DP_EPI_Data
sqlObject_dp_epi = """SELECT * FROM Rec.vw_EPI_Measures"""
object_dp_epi = pd.read_sql(sqlObject_dp_epi, connDw)
dfObject_dp_epi = spark.createDataFrame(object_dp_epi)
dfObject_dp_epi.createOrReplaceTempView("dp_epiData")

# COMMAND ----------

# DBTITLE 1,Store the data in the designated storage account _ dp_epidata
(dfObject_dp_epi
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .format("delta")
 .save("dbfs:/mnt/raw/delta/red_dp_recon/DP_EPI_DATA"))

# COMMAND ----------

# DBTITLE 1,Create the dp_epidata table in red_dp_recon database 
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS red_dp_recon.dp_epidata;
# MAGIC CREATE TABLE red_dp_recon.dp_epidata
# MAGIC USING delta
# MAGIC OPTIONS (path "dbfs:/mnt/raw/delta/red_dp_recon/DP_EPI_DATA")

# COMMAND ----------

# DBTITLE 1,DP_SIGNED_Data
sqlObject_dp_signed = """SELECT * FROM Rec.vw_Signed_Data"""
object_dp_signed = pd.read_sql(sqlObject_dp_signed, connDw)
dfObject_dp_signed = spark.createDataFrame(object_dp_signed)
dfObject_dp_signed.createOrReplaceTempView("dp_signedData")

# COMMAND ----------

# DBTITLE 1,Store the data in the designated storage account _ dp_signeddata
(dfObject_dp_signed
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .format("delta")
 .save("dbfs:/mnt/raw/delta/red_dp_recon/DP_SIGNED_DATA"))

# COMMAND ----------

# DBTITLE 1,Create the dp_signeddata table in red_dp_recon database 
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS red_dp_recon.dp_signeddata;
# MAGIC CREATE TABLE red_dp_recon.dp_signeddata
# MAGIC USING delta
# MAGIC OPTIONS (path "dbfs:/mnt/raw/delta/red_dp_recon/DP_SIGNED_DATA")

# COMMAND ----------

# DBTITLE 1,DP_CLAIMS_Data
sqlObject_dp_claims = """SELECT * FROM Rec.vw_Claim_Measures"""
object_dp_claims = pd.read_sql(sqlObject_dp_claims, connDw)
dfObject_dp_claims = spark.createDataFrame(object_dp_claims)
dfObject_dp_claims.createOrReplaceTempView("dp_claimsData")

# COMMAND ----------

# DBTITLE 1,Store the data in the designated storage account _ dp_claimsdata
(dfObject_dp_claims
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .format("delta")
 .save("dbfs:/mnt/raw/delta/red_dp_recon/DP_CLAIMS_DATA"))

# COMMAND ----------

# DBTITLE 1,Create the dp_claimsdata table in red_dp_recon database 
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS red_dp_recon.dp_claimsdata;
# MAGIC CREATE TABLE red_dp_recon.dp_claimsdata
# MAGIC USING delta
# MAGIC OPTIONS (path "dbfs:/mnt/raw/delta/red_dp_recon/DP_CLAIMS_DATA")