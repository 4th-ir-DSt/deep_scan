# Databricks notebook source
# DBTITLE 1,import modules
import json

# COMMAND ----------

# DBTITLE 1,Run standard_functions notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Drop table if exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS standardFunctions_getBatchLevelObjectRowCount;

# COMMAND ----------

# DBTITLE 1,Drop the table path to clean up the data
dbutils.fs.rm('/mnt/dataquality/unit_tests/standardFunction/tables/standardFunctions_getBatchLevelObjectRowCount',recurse = True)

# COMMAND ----------

# DBTITLE 1,create table to test getBatchLevelObjectRowCount
# MAGIC %sql
# MAGIC 
# MAGIC create table standardFunctions_getBatchLevelObjectRowCount (
# MAGIC id int,
# MAGIC name string,
# MAGIC salary int)
# MAGIC using DELTA
# MAGIC Location "/mnt/dataquality/unit_tests/standardFunction/tables/standardFunctions_getBatchLevelObjectRowCount";

# COMMAND ----------

# DBTITLE 1,Insert data into tables
# MAGIC %sql
# MAGIC insert into standardFunctions_getBatchLevelObjectRowCount  values (111,'aaa',4000)
# MAGIC                                                                  ,(222,'bbb',5000)
# MAGIC                                                                  ,(333,'ccc',6000);

# COMMAND ----------

# DBTITLE 1,Execute the function getBatchLevelObjectRowCount
objectName = 'standardFunctions_getBatchLevelObjectRowCount'
#Execute the function
batchLevelRowCount = getBatchLevelObjectRowCount(objectName)

# COMMAND ----------

# DBTITLE 1,Drop table if exists
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS standardFunctions_getBatchLevelObjectRowCount;

# COMMAND ----------

# DBTITLE 1,Drop the table path to clean up the data
dbutils.fs.rm('/mnt/dataquality/unit_tests/standardFunction/tables/standardFunctions_getBatchLevelObjectRowCount',recurse = True)

# COMMAND ----------

# DBTITLE 1,Exit the notebook
dbutils.notebook.exit(batchLevelRowCount)