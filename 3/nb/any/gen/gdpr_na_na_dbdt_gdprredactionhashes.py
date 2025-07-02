# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC #Instructions to run the notebook
# MAGIC 
# MAGIC This notebook is responsible for the redaction of personal data from all the tables containing the inserted PII
# MAGIC <ul>
# MAGIC <li>Step 1: Copy this notebook into your workspace. Please don't run the original one.</li>
# MAGIC <li>Step 2: Run the copied notebook from cell 3 to cell 6 sequentially.</li>
# MAGIC <li>Step 3: In cell 7, input the PII hash list in the form:<br>
# MAGIC    &nbsp;pii_values = [<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;('pii_hash', 'pii_traceability_hash', pii_hash_version),<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;('pii_hash', 'pii_traceability_hash', pii_hash_version),<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;...<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;]
# MAGIC </li>
# MAGIC <li>Step 4: After inserting the Pii hash list, run cells 7 and 8 sequentially.</li>

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>gdpr_na_na_dbdt_gdprredactionhashes</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>This notebook will be responsible for redaction of personal data based on a PII attribute list</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2019/01/01</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC ## Notebook Parameters
# MAGIC <table>
# MAGIC     <tr>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC 
# MAGIC ## Change Log
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Date</th>
# MAGIC     <th>Changed By</th>
# MAGIC     <th>Change Description</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td></td>
# MAGIC     <td> </td>
# MAGIC     <td></td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC 
# MAGIC ##General Standards applying to each notebook
# MAGIC <ol>
# MAGIC   <li>Above information completed</li>
# MAGIC   <li>Deployed to approriate workspace folder path</li>
# MAGIC   <li>Commited to source control</li>
# MAGIC   <li>Code tested</li>
# MAGIC   <li>Only include libraries and imports that are being used - i.e. don't include something if it is not used</li>
# MAGIC   <li>Code commented</li>
# MAGIC   <li>Code Peer reviewed</li>
# MAGIC </ol>

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  import sys
  from datetime import datetime, timedelta
  from pyspark.sql.functions import lit,col,broadcast,concat_ws,concat
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
 

except Exception as e:
  errorMessage="Exception occured while importing modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run GDPR Function notebook
# MAGIC %run ../../util/spe/gdpr_functions

# COMMAND ----------

# DBTITLE 1,Input PII list
#Please input your PII list here in the form:
#pii_values = [
#   ('pii_hash', 'pii_traceability_hash', pii_hash_version),
#   ('pii_hash', 'pii_traceability_hash', pii_hash_version),
#   ...
#   ]

pii_values = [
             ]

# COMMAND ----------

# DBTITLE 1,Apply redaction based on the inserted hashes
#Apply redaction based on the inserted hashes
gdprFunctions.gdprRedactionHashes(pii_values)