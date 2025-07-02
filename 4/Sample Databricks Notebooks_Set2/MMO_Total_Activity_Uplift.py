# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC # Media_Total_Activity_Uplift - Table Creation in Snowflake
# MAGIC * __*Author(s) :*__ Andrei Buruenescu
# MAGIC - __*email:* __ aburuenescu@inspirebrands.com
# MAGIC * __*Version :*__ 1.0.3
# MAGIC * __*Description :*__ MMO_Total_Activity_Uplift - Table Creation in Snowflake
# MAGIC * __*Update History :*__ Sep 09 (Andrei Buruenescu): Removed incremental logic
# MAGIC * __*Update History :*__ Nov 05 (Adrian Roata): Adjusted the widgets to IDS instead of Teamspaces and updated the loadToDF function
# MAGIC * __*Update History :*__ Nov 15 (Jaheer Mohammed): Modified the warehouse name
# MAGIC * __*Update History :*__ Dec 1 (Jaheer Mohammed): Updated the connection string details and also modified the warehouse name
# MAGIC ***
# MAGIC ## Description
# MAGIC This workbook is used for MMO_Total_Activity_Uplift table creation in Snowlake.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("DataBricks_Env", "default", 
                         ["default"], "Databricks Database Env ")

dbutils.widgets.dropdown("SnowFlake_DB", "IDS_DEV", 
                         ["IDS_DEV", "IDS_QA", "IDS_UAT", "IDS_PROD"], "Snowflake Database Env ")

dbutils.widgets.dropdown("SnowFlake_WareHouse", "IRB_PRCSSA_IRB_WH", 
                         ["IRB_PRCSSA_IRB_WH"], "Snowflake Warehouse")

dbutils.widgets.dropdown("SnowFlake_Schema", "DATASTORE",
                        ["DATASTORE"], "Snowflake Schema")

dbutils.widgets.dropdown("cdm_table_name", "mmo_bww_total_model_output",
                        ["mmo_bww_total_model_output"], "CDM Table Name")



db_env = getArgument("DataBricks_Env")
snowflake_db = getArgument("SnowFlake_DB")
snowflake_schema = getArgument("SnowFlake_Schema")
snowflake_wh = getArgument("SnowFlake_WareHouse")
cdm_table_name = getArgument("cdm_table_name")
snowflake_table = "mmo_total_activity_uplift"

# COMMAND ----------

# MAGIC %run ../Snowflake/ConnectionString_Snowflake

# COMMAND ----------

def loadToDF(db_env, cdm_table_name, snowflake_options, snowflake_table):
  df = spark.sql("SELECT \
                    DMA_Level as DMACode, \
                    Week_Start as WeekStartDate, \
                    group1 as Group1, \
                    group2 as Group2, \
                    group3 as Group3, \
                    group4 as Group4, \
                    group5 as Group5, \
                    Trans_Uplift as TransUplift \
                  from " + db_env + "." + cdm_table_name)
  
     
  return df

# COMMAND ----------

def writeToSnowflake(data_frame, snowflake_table, write_mode, snowflake_options):
  data_frame.write\
            .mode(write_mode)\
            .format("snowflake")\
            .options(**snowflake_options)\
            .option("dbtable", snowflake_table)\
            .save()

# COMMAND ----------

snowflake_options = Options(snowflake_db, snowflake_schema, snowflake_wh)
df = loadToDF(db_env, cdm_table_name, snowflake_options, snowflake_table)
write_mode = "overwrite"

# COMMAND ----------

writeToSnowflake(df, snowflake_table, write_mode, snowflake_options)