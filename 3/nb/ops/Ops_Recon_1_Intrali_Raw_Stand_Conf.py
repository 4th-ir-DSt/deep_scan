# Databricks notebook source
# MAGIC %md ## Notebook for reconciliation of Raw, Standardised and Conformed layer

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
import pyodbc
import numpy as np
import re
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Add Widget to fetch Table Name
dbutils.widgets.removeAll()
dbutils.widgets.text("TableName","")
TableName = dbutils.widgets.get("TableName")

# COMMAND ----------

# DBTITLE 1,Setup Connection
#ASQLDB Metadata DB
dbconnMeta = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')
connMeta = pyodbc.connect(dbconnMeta, autocommit = True)
cursorMeta = connMeta.cursor()

#Synapse Datawarehouse
dbconnDw = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-syn-connection')
connDw = pyodbc.connect(dbconnDw, autocommit = True)
cursorDw = connDw.cursor()

# COMMAND ----------

displayHTML("""
<marquee direction="right" behavior="alternate" style="background:lightyellow;border:RED 3px SOLID">  
Raw Validation
</marquee>
""")

# COMMAND ----------

# DBTITLE 1,Record Count of Raw tables - should get no records in output
raw_test_result = []

if TableName != "":
    raw_tbl_list = ['raw_intrali4444.'+TableName]
    print("::>>>>Raw TableName provided by User")
else:
    print("::>>>>All Table included from raw_intrali4444 as no input was provided by user ")
    raw_tbl_list = spark.sql("SHOW TABLES IN raw_intrali4444").where("database!=''").select(concat("database",lit("."),"tableName")).rdd.flatMap(lambda x: x).collect()
for raw_tbl in range(len(raw_tbl_list)):
    if raw_tbl !=len(raw_tbl_list)-1:
        raw_test_result0 = "select '"+str(raw_tbl_list[raw_tbl])+"' as Table, count(1) as raw_cnt from "+str(raw_tbl_list[raw_tbl])+" UNION ALL"
        raw_test_result.append(raw_test_result0)
    else:
        raw_test_result0 = "select '"+str(raw_tbl_list[raw_tbl])+"' as Table, count(1) as raw_cnt from "+str(raw_tbl_list[raw_tbl])
        raw_test_result.append(raw_test_result0)
                
# Converting list to string
raw_test_result_string = ' '.join(raw_test_result).replace("raw_intrali4444.","")

# Executing SQL and storing result in Pandas DF 
spark.sql("use raw_intrali4444")
raw_finaldf = spark.sql(raw_test_result_string)

############################################################################################################################################

display(raw_finaldf.filter(raw_finaldf["raw_cnt"] == 0))

# COMMAND ----------

# DBTITLE 1,Grain Check of raw tables - should get no records in output
# get primary keys from standardized MDM for Raw intrali4444
sqlObject = """select replace(lower(a.object_name),'Standardised_intrali4444.','') as tableName, b.object_attribute_name, 
b.object_attribute_type, b.object_attribute_order, b.primary_key_order, b.track_type_2_changes, b.is_pii, b.is_sensitive
from  config.tbl_object a  left join config.tbl_object_definition b 
on a.object_id = b.object_id 
where lower(object_name) like 'standardised_intrali4444.%'
and is_active = 1 and primary_key_order IS NOT NULL"""
object = pd.read_sql(sqlObject, connMeta)
dfget_Keys = spark.createDataFrame(object)
# dfget_Keys.createOrReplaceTempView('config_tbl')

raw_tbl_col = ( dfget_Keys.groupby("tableName").agg(concat_ws(", ", collect_list(dfget_Keys.object_attribute_name)).alias("primary_keys")) )

raw_test_result = []

if TableName != "":
    raw_tbl_list = ['raw_intrali4444.'+TableName]
    print("::>>>>Raw TableName provided by User")
else:
    print("::>>>>All Table included from raw_intrali4444 as no input was provided by user ")
    raw_tbl_list = spark.sql("SHOW TABLES IN raw_intrali4444").where("database!=''").where("tableName not like 'vw%'").select("tableName").rdd.flatMap(lambda x: x).collect()

for raw_tbl in range(len(raw_tbl_list)):
    raw_col_list = raw_tbl_col.select("primary_keys").filter(raw_tbl_col["tableName"] == raw_tbl_list[raw_tbl]).rdd.flatMap(lambda x: x).collect()
#     print(raw_col_list)
    if raw_tbl !=len(raw_tbl_list)-1:
        for raw_col in range(len(raw_col_list)):
            raw_test_result0 = "select '"+str(raw_tbl_list[raw_tbl])+"' as Table, count(1) as raw_cnt from raw_intrali4444."+str(raw_tbl_list[raw_tbl])+" group by '"+str(raw_tbl_list[raw_tbl])+"', "+str(raw_col_list[raw_col])+" having count(1) > 1 UNION ALL"
            raw_test_result.append(raw_test_result0)
    else:
        for raw_col in range(len(raw_col_list)):
            if raw_col != len(raw_col_list)-1:
                raw_test_result0 = "select '"+str(raw_tbl_list[raw_tbl])+"' as Table, count(1) as raw_cnt from raw_intrali4444."+str(raw_tbl_list[raw_tbl])+" group by '"+str(raw_tbl_list[raw_tbl])+"', "+str(raw_col_list[raw_col])+" having count(1) > 1 UNION ALL"
                raw_test_result.append(raw_test_result0)
            else:
                raw_test_result0 = "select '"+str(raw_tbl_list[raw_tbl])+"' as Table, count(1) as raw_cnt from raw_intrali4444."+str(raw_tbl_list[raw_tbl])+" group by '"+str(raw_tbl_list[raw_tbl])+"', "+str(raw_col_list[raw_col])+" having count(1) > 1"
                raw_test_result.append(raw_test_result0)
                
raw_test_result_string = ' '.join(raw_test_result)

# Executing SQL and storing result in Pandas DF 
raw_pk_df = spark.sql(raw_test_result_string)
                
# Showing columns having null values
display(raw_pk_df.select("Table").distinct())

# COMMAND ----------

displayHTML("""
<marquee direction="right" behavior="alternate" style="background:lightyellow;border:RED 3px SOLID"> 
Standardised Validation
</marquee>
""")

# COMMAND ----------

# DBTITLE 1,Record Count of Standardised tables - should get no records in output
stand_test_result = []

if TableName != "":
    stand_tbl_list = ['standardised_intrali4444.'+TableName]
    print("::>>>>Standardised TableName provided by User")
else:
    print("::>>>>All Table included from standardised_intrali4444 as no input was provided by user ")
    stand_tbl_list = spark.sql("SHOW TABLES IN standardised_intrali4444").where("database!=''").where("tableName not like 'vw%'").select(concat("database",lit("."),"tableName")).rdd.flatMap(lambda x: x).collect()
for stand_tbl in range(len(stand_tbl_list)):
    if stand_tbl !=len(stand_tbl_list)-1:
        stand_test_result0 = "select '"+str(stand_tbl_list[stand_tbl])+"' as Table, count(1) as stand_cnt from "+str(stand_tbl_list[stand_tbl])+" WHERE lakeIsActive = True and lakeDeletedTimestamp IS NULL UNION ALL"
        stand_test_result.append(stand_test_result0)
    else:
        stand_test_result0 = "select '"+str(stand_tbl_list[stand_tbl])+"' as Table, count(1) as stand_cnt from "+str(stand_tbl_list[stand_tbl])+" WHERE lakeIsActive = True and lakeDeletedTimestamp IS NULL"
        stand_test_result.append(stand_test_result0)
                
# Converting list to string
stand_test_result_string = ' '.join(stand_test_result).replace("standardised_intrali4444.","")

# Executing SQL and storing result in Pandas DF 
spark.sql("use standardised_intrali4444")
stand_finaldf = spark.sql(stand_test_result_string)

############################################################################################################################################

display(stand_finaldf.filter(stand_finaldf["stand_cnt"] == 0))

# COMMAND ----------

# DBTITLE 1,Record Count Comparison Raw v/s Standardised Tables - should get no records in output
raw_stand_cnt = raw_finaldf.join(stand_finaldf, raw_finaldf["Table"] == stand_finaldf["Table"], "outer")
display(raw_stand_cnt.filter(raw_stand_cnt["raw_cnt"] != raw_stand_cnt["stand_cnt"]))

# COMMAND ----------

# DBTITLE 1,Grain Check for standardised tables - should get no records in output
stand_in_tbl = ''
stand_tbl_list = spark.sql("SHOW TABLES IN standardised_intrali4444").where("database!=''").where("tableName not like 'vw%'").where("database!=''").select(concat("database",lit("."),"tableName")).withColumnRenamed("concat(database, ., tableName)", "table_name").collect()
for row in stand_tbl_list:
    stand_in_tbl = row['table_name']
    #performing grain check for individual tables on hashed business key
    x = spark.sql("""select hashedbusinesskey, lakeValidFromTimestamp, count(1) 
                     from {} group by hashedbusinesskey, lakeValidFromTimestamp having count(1)>1 
                     UNION ALL 
                     select hashedbusinesskey, NULL AS lakeValidFromTimestamp, count(1) 
                     from {} where LakeIsActive = 1 group by hashedbusinesskey having count(1)>1""".format(stand_in_tbl,stand_in_tbl))
    if x.count()>1:
        print ('Grain check for table {} failed'.format(stand_in_tbl))

# COMMAND ----------

# DBTITLE 1,Count Comparison of standardised tables with previous run
stand_run_in_tbl = ''
in_stand_ver_num = ''
stand_tbl_list = spark.sql("SHOW TABLES IN standardised_intrali4444").where("database!=''").where("tableName not like 'vw%'").select(concat("database",lit("."),"tableName")).withColumnRenamed("concat(database, ., tableName)", "table_name").collect()
for row in stand_tbl_list:
    stand_run_in_tbl = row['table_name']
    #performing grain check for individual tables on hashed business key
    xx = spark.sql("DESCRIBE HISTORY {}".format(stand_run_in_tbl))
#     .where("operation == 'MERGE'")
    xx.createOrReplaceTempView('src')
    y = spark.sql("""select version as previous_version_number from 
                      (select version,row_number() over(order by version desc) as rnk from src where operation = 'MERGE') 
                      where rnk=2""")
    for row in y.collect():
        in_stand_ver_num=row['previous_version_number']
        if in_stand_ver_num is not None:
            z=spark.sql("select hashedbusinesskey from {}".format(stand_run_in_tbl))
            n=spark.sql("select hashedbusinesskey from {}@v{}".format(stand_run_in_tbl,in_stand_ver_num))
            x=(str(stand_run_in_tbl)+',' +' previous_run_count --->'+str(n.count())+','+'latest_run_count--->'+ str(z.count()) +','+'DELTA_RECORDS-->'+ str(z.count() - (n.count())))
            print(x)

# COMMAND ----------

displayHTML("""
<marquee direction="right" behavior="alternate" style="background:lightyellow;border:RED 3px SOLID"> 
Conformed Validation
</marquee>
""")

# COMMAND ----------

# DBTITLE 1,Record Count for Conformed Tables - should get no records in output
conf_test_result = []

if TableName != "":
    conf_tbl_list = ['conformed_intrali4444.'+TableName]
    print("::>>>>Conformed TableName provided by User")
else:
    print("::>>>>All Table included from conformed_intrali4444 as no input was provided by user ")
    conf_tbl_list = spark.sql("SHOW TABLES IN conformed_intrali4444").where("database!=''").where("tableName not like 'vw%'").select(concat("database",lit("."),"tableName")).rdd.flatMap(lambda x: x).collect()
for conf_tbl in range(len(conf_tbl_list)):
    if conf_tbl !=len(conf_tbl_list)-1:
        conf_test_result0 = "select '"+str(conf_tbl_list[conf_tbl])+"' as Table, count(1) as cnt from "+str(conf_tbl_list[conf_tbl])+" WHERE lakeIsActive = True and lakeDeletedTimestamp IS NULL UNION ALL"
        conf_test_result.append(conf_test_result0)
    else:
        conf_test_result0 = "select '"+str(conf_tbl_list[conf_tbl])+"' as Table, count(1) as cnt from "+str(conf_tbl_list[conf_tbl])+" WHERE lakeIsActive = True and lakeDeletedTimestamp IS NULL"
        conf_test_result.append(conf_test_result0)
                
# Converting list to string
conf_test_result_string = ' '.join(conf_test_result)

# Executing SQL and storing result in Pandas DF 
conf_finaldf = spark.sql(conf_test_result_string)
# .toPandas()

# Showing columns having null values
display(conf_finaldf.filter(conf_finaldf["cnt"] == 0))

# COMMAND ----------

# DBTITLE 1,Grain Check for conformed tables - should get no records in output
conf_in_tbl = ''
conf_tbl_list = spark.sql("SHOW TABLES IN conformed_intrali4444").where("database!=''").where("tableName not like 'vw%'").select(concat("database",lit("."),"tableName")).withColumnRenamed("concat(database, ., tableName)", "table_name").collect()
for row in conf_tbl_list:
    conf_in_tbl = row['table_name']
    #performing grain check for individual tables on hashed business key
    x=spark.sql("""select hashedbusinesskey, lakeValidFromTimestamp, count(1) 
                   from {} group by hashedbusinesskey, lakeValidFromTimestamp having count(1)>1 
                   UNION ALL 
                   select hashedbusinesskey, NULL AS lakeValidFromTimestamp, count(1) 
                   from {} where LakeIsActive = 1 group by hashedbusinesskey having count(1)>1""".format(conf_in_tbl,conf_in_tbl))
    if x.count()>1:
        print ('Grain check for table {} failed'.format(conf_in_tbl))

# COMMAND ----------

# DBTITLE 1,Count Comparison of conformed tables with previous run
conf_run_in_tbl=''
in_conf_ver_num=''
conf_tbl_list = spark.sql("SHOW TABLES IN conformed_intrali4444").where("database!=''").where("tableName not like 'vw%'").select(concat("database",lit("."),"tableName")).withColumnRenamed("concat(database, ., tableName)", "table_name").collect()
for row in conf_tbl_list:
    conf_run_in_tbl=row['table_name']
    #performing grain check for individual tables on hashed business key
    xx=spark.sql("DESCRIBE HISTORY {}".format(conf_run_in_tbl))
#     .where("operation == 'MERGE'")
    xx.createOrReplaceTempView('src')
    y=spark.sql("""select version as previous_version_number from 
                      (select version,row_number() over(order by version desc) as rnk from src where operation = 'MERGE') 
                      where rnk=2""")
    for row in y.collect():
        in_conf_ver_num=row['previous_version_number']
        if in_conf_ver_num is not None:
            z=spark.sql("select hashedbusinesskey from {}".format(conf_run_in_tbl))
            n=spark.sql("select hashedbusinesskey from {}@v{}".format(conf_run_in_tbl,in_conf_ver_num))
            x=(str(conf_run_in_tbl)+',' +' previous_run_count --->'+str(n.count())+', '+'latest_run_count--->'+ str(z.count()) +', '+'DELTA_RECORDS-->'+ str(z.count() - (n.count())))
            print(x)

# COMMAND ----------

# DBTITLE 1,Identify the column containing NULL & Blank values - Just for reference
# test_result = []
# cols_nt_req = ['HashedPartitionKey','HashedBusinessKey','sourceID','lakeCreatedTimeStamp','lakeCreatedBatchID','lakeLastUpdateDate',
#               'lakeLastUpdateTimeStamp','lakeLastUpdatedBatchID','HashValue','lakeCreatedDate','lakeValidFromTimestamp','lakeValidToTimestamp',
#               'lakeIsActive','lakeDeletedTimestamp','deletedTimestamp']

# if TableName != "":
#     tbl_list = ['conformed_intrali4444.'+TableName]
#     print("::>>>>Conformed TableName provided by User")
# else:
#     print("::>>>>All Table included from conformed_intrali4444 as no input was provided by user ")
#     tbl_list = spark.sql("SHOW TABLES IN conformed_intrali4444").where("database!=''").where("tableName not like 'vw%'").select(concat("database",lit("."),"tableName")).rdd.flatMap(lambda x: x).collect()
# for tbl in range(len(tbl_list)):
#     if tbl !=len(tbl_list)-1:
#         col_df = spark.sql("SHOW COLUMNS IN {}".format(tbl_list[tbl]))
#         col_list = col_df.filter(~col_df['col_name'].isin(cols_nt_req)).rdd.flatMap(lambda x: x).collect()
#         for col in range(len(col_list)):
#             test_result0 = "select '"+str(tbl_list[tbl])+"' as Table, '"+str(col_list[col])+"' as Column, count(1) as cnt from "+str(tbl_list[tbl])+" where lakeIsActive = 1 and ("+str(col_list[col])+" IS NULL OR "+str(col_list[col])+" = '') UNION ALL"
#             test_result.append(test_result0)
#     else:
#         col_df = spark.sql("SHOW COLUMNS IN {}".format(tbl_list[tbl]))
#         col_list = col_df.filter(~col_df['col_name'].isin(cols_nt_req)).rdd.flatMap(lambda x: x).collect()
#         for col in range(len(col_list)):
#             if col != len(col_list)-1:
#                 test_result0 = "select '"+str(tbl_list[tbl])+"' as Table, '"+str(col_list[col])+"' as Column, count(1) as cnt from "+str(tbl_list[tbl])+" where lakeIsActive = 1 and ("+str(col_list[col])+" IS NULL OR "+str(col_list[col])+" = '') UNION ALL"
#                 test_result.append(test_result0)
#             else:
#                 test_result0 = "select '"+str(tbl_list[tbl])+"' as Table, '"+str(col_list[col])+"' as Column, count(1) as cnt from "+str(tbl_list[tbl])+" where lakeIsActive = 1 and ("+str(col_list[col])+" IS NULL OR "+str(col_list[col])+" = '')"
#                 test_result.append(test_result0)
                
# # Converting list to string
# test_result_string = ' '.join(test_result)

# # Executing SQL and storing result in Pandas DF 
# finaldf = spark.sql(test_result_string)

# # Showing columns having null values
# display(finaldf.filter(finaldf["cnt"] > 0))

# COMMAND ----------

# DBTITLE 1,Identify the column containing Default values - Just for reference
# test_result = []
# cols_nt_req = ['HashedPartitionKey','HashedBusinessKey','sourceID','lakeCreatedTimeStamp','lakeCreatedBatchID','lakeLastUpdateDate',
#               'lakeLastUpdateTimeStamp','lakeLastUpdatedBatchID','HashValue','lakeCreatedDate','lakeValidFromTimestamp','lakeValidToTimestamp',
#               'lakeIsActive','lakeDeletedTimestamp','deletedTimestamp','LakeIsActive']

# if TableName != "":
#     tbl_list = ['conformed_intrali4444.'+TableName]
#     print("::>>>>Conformed TableName provided by User")
# else:
#     print("::>>>>All Table included from conformed_intrali4444 as no input was provided by user ")
#     tbl_list = spark.sql("SHOW TABLES IN conformed_intrali4444").where("database!=''").where("tableName not like 'vw%'").select(concat("database",lit("."),"tableName")).rdd.flatMap(lambda x: x).collect()
# for tbl in range(len(tbl_list)):
#     if tbl !=len(tbl_list)-1:
#         col_df = spark.sql("SHOW COLUMNS IN {}".format(tbl_list[tbl]))
#         col_list = col_df.filter(~col_df['col_name'].isin(cols_nt_req)).rdd.flatMap(lambda x: x).collect()
#         for col in range(len(col_list)):
#             test_result0 = "select '"+str(tbl_list[tbl])+"' as Table, '"+str(col_list[col])+"' as Column, count(1) as cnt from "+str(tbl_list[tbl])+" where lakeIsActive = 1 and "+str(col_list[col])+" IN ('UNMAPPED','UNKNWON','NOT AVAILABLE') UNION ALL"
#             test_result.append(test_result0)
#     else:
#         col_df = spark.sql("SHOW COLUMNS IN {}".format(tbl_list[tbl]))
#         col_list = col_df.filter(~col_df['col_name'].isin(cols_nt_req)).rdd.flatMap(lambda x: x).collect()
#         for col in range(len(col_list)):
#             if col != len(col_list)-1:
#                 test_result0 = "select '"+str(tbl_list[tbl])+"' as Table, '"+str(col_list[col])+"' as Column, count(1) as cnt from "+str(tbl_list[tbl])+" where lakeIsActive = 1 and "+str(col_list[col])+" IN ('UNMAPPED','UNKNWON','NOT AVAILABLE') UNION ALL"
#                 test_result.append(test_result0)
#             else:
#                 test_result0 = "select '"+str(tbl_list[tbl])+"' as Table, '"+str(col_list[col])+"' as Column, count(1) as cnt from "+str(tbl_list[tbl])+" where lakeIsActive = 1 and "+str(col_list[col])+" IN ('UNMAPPED','UNKNWON','NOT AVAILABLE')"
#                 test_result.append(test_result0)
                
# # Converting list to string
# test_result_string = ' '.join(test_result)

# # Executing SQL and storing result in Pandas DF 
# finaldf = spark.sql(test_result_string)

# # Showing columns having null values
# display(finaldf.filter(finaldf["cnt"] > 0))

# COMMAND ----------

# DBTITLE 1,BED Check for Conformed - should have distinct BEDs for all entities
conf_test_result_1 = []

if TableName != "":
    conf_tbl_list_1 = ['conformed_intrali4444.'+TableName]
    print("::>>>>Conformed TableName provided by User")
else:
    print("::>>>>All Table included from conformed_intrali4444 as no input was provided by user ")
    conf_tbl_list_1 = spark.sql("SHOW TABLES IN conformed_intrali4444").where("database!=''").where("tableName not like 'vw%'").select(concat("database",lit("."),"tableName")).rdd.flatMap(lambda x: x).collect()
for conf_tbl in range(len(conf_tbl_list_1)):
    if conf_tbl !=len(conf_tbl_list_1)-1:
        conf_test_result0 = "select distinct '"+str(conf_tbl_list_1[conf_tbl])+"' as Table, Lakevalidfromtimestamp from "+str(conf_tbl_list_1[conf_tbl])+" UNION ALL"
        conf_test_result_1.append(conf_test_result0)
    else:
        conf_test_result0 = "select distinct '"+str(conf_tbl_list_1[conf_tbl])+"' as Table, Lakevalidfromtimestamp from "+str(conf_tbl_list_1[conf_tbl])
        conf_test_result_1.append(conf_test_result0)
                
# Converting list to string
conf_test_result_string_1 = ' '.join(conf_test_result_1)

# print(conf_test_result_string)

# Executing SQL and storing result in Pandas DF 
conf_finaldf_1 = spark.sql(conf_test_result_string_1)
# .toPandas()

# Showing columns having null values
display(conf_finaldf_1)