# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC #Instructions to run the notebook
# MAGIC 
# MAGIC This notebook is responsible for the rectification of personal data from all the tables containing the inserted PIIs
# MAGIC <ul>
# MAGIC <li>Step 1: Copy this notebook into your workspace. Please don't run the original one.</li>
# MAGIC <li>Step 2: Run the copied notebook from cell 3 to cell 7 sequentially. The cell 7 output is the list of personal values that can be rectified.</li>
# MAGIC <li>Step 3: In cell 8, input the personal values (<b>please use only personal values provided in the cell 7 output</b>) to be rectified in the form:<br>
# MAGIC    &nbsp;personal_values = {<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"PersonalValueName" : "Value",<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"PersonalValueName" : "Value",<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"PersonalValueName" : "Value",<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;...<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}
# MAGIC </li>
# MAGIC <li>Step 3: In cell 9, input the PII hash list in the form:<br>
# MAGIC    &nbsp;pii_values = [<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;('pii_hash', 'pii_traceability_hash', pii_hash_version),<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;('pii_hash', 'pii_traceability_hash', pii_hash_version),<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;...<br>
# MAGIC    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;]
# MAGIC </li>
# MAGIC <li>Step 4: After inserting the personal values to rectify in cell 8 and Pii hash list in cell 9, run cells 8, 9 and 10 sequentially.</li>

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>gdpr_na_na_dbdt_gdprrectification</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>This notebook will be responsible for rectification of personal data</td></tr>
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
# MAGIC     <td>2019/01/01</td>
# MAGIC     <td>Framework</td>
# MAGIC     <td>Changes on the notebook instructions<br>Added the coalesce function to the import</td>
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
  import copy
  from datetime import datetime, timedelta
  from decimal import Decimal
  from pyspark.sql.functions import lit,col,broadcast,concat_ws,concat,when,coalesce
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

# DBTITLE 1,Possible personal data fields to rectify
conn, cursor, allPiiFieldsDF = gdprFunctions.getPiiFields()

# COMMAND ----------

# DBTITLE 1,Input personal data to be rectified
#Please input the personal data to be rectified here in the form:
#personal_values = {
#  "PersonalValueName" : "Value",
#  "PersonalValueName" : "Value",
#  "PersonalValueName" : "Value",
#  ...
#}

personal_values = {}

# COMMAND ----------

# DBTITLE 1,Input PII list
#Please input your PII list here in the form:
#pii_values = [
#   ('pii_hash', 'pii_traceability_hash', pii_hash_version),
#   ('pii_hash', 'pii_traceability_hash', pii_hash_version),
#   ...
#   ]
		
pii_values = []

# COMMAND ----------

# DBTITLE 1,Apply rectification
gdprFunctions.gdprRectification(personal_values, allPiiFieldsDF, pii_values, conn, cursor)