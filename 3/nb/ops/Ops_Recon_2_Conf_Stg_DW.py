# Databricks notebook source
# MAGIC %md ## Notebook for reconciliation of Conformed, Staging and DW layer

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from pyspark.sql.window import *
import pyodbc
import numpy
import re
import pandas as pd

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
Staging Layer Validation Output
</marquee>
""")

# COMMAND ----------

# DBTITLE 1,Count Validation - Conformed v/s Staging - should get no records in output
########################Record Count of Conformed Tables########################
conf_test_result = []
conf_tbl_list_subs = spark.sql("show tables in conformed_subscribe").where("database != ''").where("tableName NOT IN ('claim_movement', 'party_claim_movement_role','party_relationship')").select(concat("database",lit("."),"tableName")).rdd.flatMap(lambda x: x).collect()
conf_tbl_list_mdm = spark.sql("show tables in conformed_mdm").where("database != ''").select(concat("database",lit("."),"tableName")).rdd.flatMap(lambda x: x).collect()
conf_tbl_list_bdx = spark.sql("show tables in conformed_intrali4444").where("database != ''").where("tableName NOT LIKE 'vw%'").select(concat("database",lit("."),"tableName")).rdd.flatMap(lambda x: x).collect()
conf_tbl_list = conf_tbl_list_subs + conf_tbl_list_mdm + conf_tbl_list_bdx
for conf_tbl in range(len(conf_tbl_list)):
    if conf_tbl !=len(conf_tbl_list)-1:
        conf_test_result0 = "select '"+str(conf_tbl_list[conf_tbl])+"' as tbl_nm, count(1) as tbl_cnt from "+str(conf_tbl_list[conf_tbl])+" group by 1 UNION ALL"
# and (lakevalidfromtimestamp = '2022-08-11T09:00:00.000+0000' OR lakevalidtotimestamp = '2022-08-11T09:00:00.000+0000' OR lakedeletedtimestamp = '2022-08-11T09:00:00.000+0000')
        conf_test_result.append(conf_test_result0)
    else:
        conf_test_result0 = "select '"+str(conf_tbl_list[conf_tbl])+"' as tbl_nm, count(1) as tbl_cnt from "+str(conf_tbl_list[conf_tbl])+" group by 1"
        conf_test_result.append(conf_test_result0)
                
# Converting list to string
conf_test_result_string = ' '.join(conf_test_result)

# Executing SQL and storing result in Pandas DF 
conf_finaldf = spark.sql(conf_test_result_string)
#creating dataframes
conf_finaldf.createOrReplaceTempView('adb_cnf_cnt')


########################Record Count of Staging Tables########################
sqlObject = """DECLARE @query nvarchar(max)
DECLARE @max nvarchar(max)
select @max = max(rnk)  from
(
    select row_number() over(order by a.name) as rnk, concat('select ''',a.name,''' as tbl_nm count(1) as tbl_cnt
    from staging.',a.name,' union all') as query
    from
    (
        select name, object_id from sys.tables where name not like 'Ext_%' 
        and schema_id = (select schema_id from sys.schemas where name = 'staging')
    )a
)x

select @query = STRING_AGG(CONVERT(NVARCHAR(max),query),'')
from
(
    select case when a.rnk=@max then replace(query,'union all', ' ') else query end as query
    from
    (
        select row_number() over(order by a.name) as rnk, concat('select ''',a.name,''' as tbl_nm, count(1) as tbl_cnt
        from staging.',a.name,' union all ') as query
        from
        (
        select name, object_id from sys.tables where name not like 'Ext_%' 
        and schema_id = (select schema_id from sys.schemas where name = 'staging' )
        )a
    )a
)x

exec(@query)"""
object = pd.read_sql(sqlObject, connDw)
dfObject = spark.createDataFrame(object)
dfObject.createOrReplaceTempView("sql_stg_cnt")


########################Record Count Comparison########################
cnf_stg_cnt_df = spark.sql("""
select replace(tbl_nm, 'conformed_','') as tbl_nm, cnf_tbl_cnt, stg_tbl_cnt,
CASE 
  WHEN stg_tbl_cnt = 0 THEN '(3) ISSUE - Zero records in Staging'
  WHEN cnf_tbl_cnt = 0 THEN '(2) ISSUE - Zero records in Conformed'
  WHEN cnf_tbl_cnt - coalesce(stg_tbl_cnt,0) <> 0 THEN '(1) ISSUE - Count Mismatch'
  ELSE '(4) Count Matching'
END AS Status
from
(
  select adb.src, adb.tbl_nm, stg.tbl_nm as stg_tbl_nm, adb.tbl_cnt as cnf_tbl_cnt, stg.src, stg.tbl_cnt as stg_tbl_cnt from
      (select 'Conformed' as src, tbl_nm, tbl_cnt from adb_cnf_cnt) adb
    left join
      (select 'Staging' as src, tbl_nm, tbl_cnt from sql_stg_cnt) stg
      on upper(substring_index(adb.tbl_nm,'.',-1)) = upper(stg.tbl_nm)
) cnt 
order by Status, tbl_nm
""")
display(cnf_stg_cnt_df.filter(cnf_stg_cnt_df['Status']!='(4) Count Matching'))

# COMMAND ----------

# DBTITLE 1,Staging Grain Check for Active Records - should get no records in output
sqlObject = """DECLARE @query nvarchar(max)
DECLARE @max nvarchar(max)
select @max = max(rnk)  from
(
    select row_number() over(order by name) as rnk, concat('select ','''',name,'''', ' as tbl, ', col, ' ,count(1) as count 
    from staging.', name,' where lakeisactive=1 group by ',col,' having count(1)>1 union all ') as query
    from
    (
        select y.name, string_agg(x.name,',') as col from
        (
            select name, object_id  from sys.tables 
            where schema_id = (select schema_id from sys.schemas where name = 'staging')
            and name not like 'Ext_%' AND name not in ('FactFx','FactFxPadded','red_fact_ctb_quarterly')
         )y
        inner join
        (
            select object_id, name from sys.columns where column_id in (2)
        ) x
    on y.object_id=x.object_id
    group by y.name
    )z
)x

select @query = concat('select distinct tbl , 
case
    when max(count)=1 then ', '''Grain check passed''','
    else ', '''Grain check failed''',' 
end as Msg from (',STRING_AGG(CONVERT(NVARCHAR(max),query),' ' ),')x group by tbl')
from
(
    select case when a.rnk=@max then replace(query,'union all', '') else query end as query
    from
    (
        select row_number() over(order by name) as rnk,concat('select ','''',name,'''', ' as tbl, ', col, ' ,count(1) as count 
        from staging.', name,' where lakeisactive=1 group by ',col,' having count(1)>=1 union all ') as query
        from
        (
            select y.name, string_agg('hashedbusinesskey',',') as col from
            (
                select name, object_id  from sys.tables 
                where schema_id = (select schema_id from sys.schemas where name = 'staging')
                and name not like 'Ext_%' AND name not in ('FactFx','FactFxPadded','red_fact_ctb_quarterly')
             )y
         group by y.name
        )z
    )a
)x
exec(@query)"""

object = pd.read_sql(sqlObject, connDw)
dfObject = spark.createDataFrame(object)
filter1 = dfObject["Msg"]=="Grain check failed"
x=dfObject.where(filter1)
display(x)

# COMMAND ----------

# DBTITLE 1,Staging Grain Check for All Records - should get no records in output
sqlObject = """DECLARE @query nvarchar(max)
DECLARE @max nvarchar(max)
select @max = max(rnk)  from
(
    select row_number() over(order by name) as rnk, concat('select ','''',name,'''', ' as tbl, ', col, ' ,count(1) as count 
    from staging.', name,' where lakeisactive=1 group by ',col,' having count(1)>1 union all ') as query
    from
    (
        select y.name, string_agg('hashedbusinesskey, lakevalidfromtimestamp',',') as col from
        (
            select name, object_id  from sys.tables 
            where schema_id = (select schema_id from sys.schemas where name = 'staging')
            and name not like 'Ext_%' AND name not in ('FactFx','FactFxPadded','red_fact_ctb_quarterly')
        )y
    group by y.name
    )z
)x

select @query = concat('select distinct tbl , 
case
    when max(count)=1 then ', '''Grain check passed''','
    else ', '''Grain check failed''',' 
end as Msg from (',STRING_AGG(CONVERT(NVARCHAR(max),query),' ' ),')x group by tbl') 
from
(
    select case when a.rnk=@max then replace(query,'union all', '') else query end as query
    from
    (
        select row_number() over(order by name) as rnk, concat('select ','''',name,'''', ' as tbl, ', col, ' ,count(1) as count 
        from staging.', name,'  group by ',col,' having count(1)>=1 union all ') as query
        from
        (
            select y.name, string_agg('hashedbusinesskey, lakevalidfromtimestamp',',') as col from
            (
                select name, object_id  from sys.tables 
                where schema_id = (select schema_id from sys.schemas where name = 'staging')
                and name not like 'Ext_%' AND name not in ('FactFx','FactFxPadded','red_fact_ctb_quarterly')
             )y
         group by y.name
        )z
    )a
)x
exec(@query)"""

object = pd.read_sql(sqlObject, connDw)
dfObject = spark.createDataFrame(object)
filter1 = dfObject["Msg"]=="Grain check failed"
z=dfObject.where(filter1)
display(z)

# COMMAND ----------

displayHTML("""
<marquee direction="right" behavior="alternate" style="background:lightyellow;border:RED 3px SOLID">  
DW Layer Validation Output
</marquee>
""")

# COMMAND ----------

# DBTITLE 1,Count Validation Staging v/s Dim - (PhaseA)
excluded_tbl_dict  = {'accounting_period_mapping':'dim_accounting_period','claim_component_characteristic':'dim_claim_characteristic','source':'dim_data_source','epi_transaction':'Dim_Estimated_Premium_Transaction','party_group_mapping':'dim_party_group','signing_message_transaction_narrative':'Dim_Signing_Message_Narrative','signing_message_treaty_section_amount':'dim_signing_message_treaty_detail'}

stg_tbls = '''
select lower(name) from sys.tables where schema_id = (select schema_id from sys.schemas where name = 'staging')
and name not like '%Ext%' and name not like '%fact%' and name not like '%fct%' and name not like '%Bordereaux%'
'''
stg_object = pd.read_sql(stg_tbls, connDw)
stg_objects = spark.createDataFrame(stg_object).rdd.flatMap(lambda x:x).collect()
stg_qry = ""
for i in range(0,len(stg_objects)):
    tbl = stg_objects[i]
    if tbl not in list(excluded_tbl_dict.keys()):
        if i != len(stg_objects)-1 :
            stg_qry = stg_qry +"select '{tbl}' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null UNION ALL ".format(tbl=tbl) 
        else :
            stg_qry = stg_qry +"select '{tbl}' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null ".format(tbl=tbl)
    else:
        if tbl == 'claim_component_characteristic':
            if i != len(stg_objects)-1 :
                stg_qry = stg_qry +"select 'claim_characteristic' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null UNION ALL ".format(tbl=tbl)
            else:
                stg_qry = stg_qry +"select 'claim_characteristic' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null".format(tbl=tbl)

        elif tbl == 'source':
            if i != len(stg_objects)-1 :
                stg_qry = stg_qry +"select 'data_source' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null UNION ALL ".format(tbl=tbl)
            else:
                stg_qry = stg_qry +"select 'data_source' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null ".format(tbl=tbl)

        elif tbl == 'epi_transaction':
            if i != len(stg_objects)-1 :
                stg_qry = stg_qry +"select 'estimated_premium_transaction' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null UNION ALL  ".format(tbl=tbl)
            else:
                stg_qry = stg_qry +"select 'estimated_premium_transaction' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null ".format(tbl=tbl)

        elif tbl == 'party_group_mapping':
            if i != len(stg_objects)-1 :
                stg_qry = stg_qry +"select 'party_group' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null UNION ALL ".format(tbl=tbl)
            else:
                stg_qry = stg_qry +"select 'party_group' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null ".format(tbl=tbl)
                
        elif tbl == 'signing_message_transaction_narrative':
            if i != len(stg_objects)-1 :
                stg_qry = stg_qry +"select 'Signing_Message_Narrative' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null UNION ALL ".format(tbl=tbl)
            else:
                stg_qry = stg_qry +"select 'Signing_Message_Narrative' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null ".format(tbl=tbl)

        elif tbl == 'signing_message_treaty_section_amount':
            if i != len(stg_objects)-1 :
                stg_qry = stg_qry +"select 'signing_message_treaty_detail' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null UNION ALL ".format(tbl=tbl)
            else:
                stg_qry = stg_qry +"select 'signing_message_treaty_detail' as tbl , count(1) as cnt from staging.{tbl} where lakeisactive =1 and lakedeletedtimestamp is null ".format(tbl=tbl)

stg_pddf = pd.read_sql(stg_qry, connDw)
stg_df = spark.createDataFrame(stg_pddf)

dwh_tbls = '''
select lower(name) from sys.tables where schema_id = (select schema_id from sys.schemas where name = 'dwh')
and (name like 'Dim_%' and name not like 'Dim_Bdx%') and name not like '%year%' and name not like '%development_period%' and name not like '%dim_date%' and name not like '%dim_lead_type%'
'''
dim_object = pd.read_sql(dwh_tbls, connDw)
dim_objects = spark.createDataFrame(dim_object).rdd.flatMap(lambda x:x).collect()
dim_qry = ""

for i in range(0,len(dim_objects)):
    tbl = dim_objects[i]
    if tbl not in list(excluded_tbl_dict.values()):
        if i != len(dim_objects)-1 :
            dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null UNION ALL ".format(tbl=tbl) 
        else :
            dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null ".format(tbl=tbl)
    else:
        if tbl == 'dim_claim_characteristic':
            if i != len(dim_objects)-1 :
                dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null UNION ALL ".format(tbl=tbl)
            else:
                dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null ".format(tbl=tbl)
        
        if tbl == 'dim_data_source':
            if i != len(dim_objects)-1 :
                dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null UNION ALL ".format(tbl=tbl)
            else:
                dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null ".format(tbl=tbl)
        
        if tbl == 'Dim_Estimated_Premium_Transaction':
            if i != len(dim_objects)-1 :
                dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null UNION ALL ".format(tbl=tbl)
            else:
                dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null ".format(tbl=tbl)
        
        if tbl == 'dim_party_group':
            if i != len(dim_objects)-1 :
                dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null UNION ALL ".format(tbl=tbl)
            else:
                dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null ".format(tbl=tbl)
        
        if tbl == 'Dim_Signing_Message_Narrative':
            if i != len(dim_objects)-1 :
                dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null UNION ALL ".format(tbl=tbl)
            else:
                dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null ".format(tbl=tbl)
        
        if tbl == 'dim_signing_message_treaty_detail':
            if i != len(dim_objects)-1 :
                dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null UNION ALL ".format(tbl=tbl)
            else:
                dim_qry = dim_qry +"select '{tbl}' as dim_tbl , count(1) - 3 as dim_cnt from dwh.{tbl} where dwh_valid_to is null ".format(tbl=tbl)

dim_pddf = pd.read_sql(dim_qry, connDw)
dim_df = spark.createDataFrame(dim_pddf)
dim_df = dim_df.withColumn('join_tbl',regexp_replace('dim_tbl', 'dim_', '')) #.withColumnRenamed('tbl','dim_tbl')

df = stg_df.join(dim_df,stg_df.tbl == dim_df.join_tbl ,'inner').drop(dim_df.join_tbl)
df = df.filter(df.tbl.isNotNull() & df.dim_tbl.isNotNull())
final_df =  df.withColumn('status',when(df.cnt == df.dim_cnt,lit('Matching')).otherwise(lit('Not Matching')))

display(final_df.filter(final_df.status=='Not Matching'))
# display(final_df)

# COMMAND ----------

# DBTITLE 1,Null check for Phase A Dims
sqlObject_dwh_dim = """
select distinct tbl_nm, col_nm, query from(
select a.name as tbl_nm, b.name as col_nm,
concat('select ''',a.name,''' as tbl_nm ,''', b.name,''' as col_nm, count(1) as tbl_cnt, count(case when ',b.name,' IS NULL THEN 1 END) as COL_NULL from dwh.',a.name,' union all') as query
from
(select name, object_id from sys.tables where schema_id  = (select schema_id from sys.schemas where name = 'dwh')
and (name like 'Dim_%' and name not like '%Bdx%' ))a
INNER JOIN 
(select name, object_id from sys.columns where column_id>10 and substring(name,1,3) !='dwh')b
on a.object_id =b.object_id)x
"""

object_dim = pd.read_sql(sqlObject_dwh_dim, connDw)
dfObject_dim = spark.createDataFrame(object_dim)
concat_query = dfObject_dim.select("query").rdd.flatMap(lambda x :x ).collect()
final_inner = ' '.join(concat_query).rsplit(' ',2)[0] # to remove last union all
object_dim1 = pd.read_sql(final_inner, connDw)
dfObject_dim1 = spark.createDataFrame(object_dim1)
dfObject_dim1.createOrReplaceTempView("t1_dim")

df = spark.sql("select tbl_nm, col_nm, 'All values are null for this column' as Comment from t1_dim where tbl_cnt=COL_NULL and tbl_cnt!=0 order by tbl_nm")
display(df)

# COMMAND ----------

# DBTITLE 1,Null check for Phase A Bridges
sqlObject_dwh_bridge = """ select tbl_nm, col_nm, query from(
select a.name as tbl_nm, b.name as col_nm,
concat('select ''',a.name,''' as tbl_nm ,''', b.name,''' as col_nm, count(1) as tbl_cnt, count(case when ',b.name,' IS NULL THEN 1 END)
as COL_NULL from dwh.',a.name,' union all') as query
from
(select name, object_id from sys.tables where schema_id = (select schema_id from sys.schemas where name = 'dwh') and substring(name,1,7) ='Bridge_')a
INNER JOIN 
(select name, object_id from sys.columns where column_id>10 and substring(name,1,3) !='dwh')b
on a.object_id =b.object_id)x """

object_bridge = pd.read_sql(sqlObject_dwh_bridge, connDw)
dfObject_bridge = spark.createDataFrame(object_bridge)
concat_query = dfObject_bridge.select("query").rdd.flatMap(lambda x :x ).collect()
final_inner = ' '.join(concat_query).rsplit(' ',2)[0] # to remove last union all
object_bridge = pd.read_sql(final_inner, connDw)
dfObject_bridge1 = spark.createDataFrame(object_bridge)
dfObject_bridge1.createOrReplaceTempView("t1_bridge")

df = spark.sql("select tbl_nm, col_nm, 'All values are null for this column' as Comment from t1_bridge where tbl_cnt=COL_NULL and tbl_cnt!=0 order by tbl_nm")
display(df)

# COMMAND ----------

# DBTITLE 1,Grain check for Dim/ Bridge Tables 

sqlObject_grain_dwh_dim = """
select distinct tbl_nm, query from(
select a.name as tbl_nm, b.name as col_nm,
concat('select ''',a.name,''' as tbl_nm ,''', b.name,''' as col_nm, count(1) as tbl_cnt from dwh.',a.name,' where dwh_valid_to is null group by ',b.name, ' having count(1)>1 union all') as query
from
(select name, object_id from sys.tables where schema_id =  (select schema_id from sys.schemas where name = 'dwh')
and name like 'Dim_%' )a
INNER JOIN 
(select * from sys.columns where substring(name,1,3) !='dwh' and name like '%_HBK')b
on a.object_id =b.object_id)x
"""

sqlObject_grain_dwh_bridge = """
select tbl_nm,  query from(
select a.name as tbl_nm,
concat('select ''',a.name,''' as tbl_nm ,''',a.name,'_HBK'' as col_nm, count(1) as tbl_cnt from dwh.',a.name,' where dwh_valid_to is null group by ',a.name, '_HBK having count(1)>1 union all') as query
from
(select name from sys.tables where schema_id = (select schema_id from sys.schemas where name = 'dwh') and substring(name,1,7) ='Bridge_' )a
)x
"""

object_grain_bridge = pd.read_sql(sqlObject_grain_dwh_bridge, connDw)
object_grain_dim = pd.read_sql(sqlObject_grain_dwh_dim, connDw)

dfObject_grain_bridge = spark.createDataFrame(object_grain_bridge)
dfObject_grain_dim = spark.createDataFrame(object_grain_dim)

dfObject_grain = dfObject_grain_bridge.unionByName(dfObject_grain_dim)
concat_grain_query = dfObject_grain.select("query").rdd.flatMap(lambda x :x ).collect()
final_grain_inner = ' '.join(concat_grain_query).rsplit(' ',2)[0] # to remove last union all
object_grain = pd.read_sql(final_grain_inner, connDw)
if object_grain.empty:
    print("Grain check is passing")
else:
    dfObject_grain1 = spark.createDataFrame(object_grain)
    dfObject_grain1.createOrReplaceTempView("t1_grain")
    df_grain = spark.sql("select tbl_nm as Grain_Check_Failing from t1_grain group by tbl_nm")
    display(df_grain)


# COMMAND ----------

# DBTITLE 1,Count check for all Dim/Bridge tables

sqlObject_cnt_dwh_bridge = """
select tbl_nm,  query from(
select a.name as tbl_nm, 
       concat('select ''',a.name,''' as tbl_nm, count(*) as cnt from dwh.',a.name,'  union all') as query
from (select name from sys.tables where schema_id = (select schema_id from sys.schemas where name = 'dwh') and substring(name,1,7) ='Bridge_' or 
name = 'Control_Reporting_Usage' )a
)x
"""

sqlObject_cnt_dwh_dim = """
select distinct tbl_nm, query from(
select a.name as tbl_nm,
       concat('select ''',a.name,''' as tbl_nm, count(*) as cnt from dwh.',a.name,' union all') as query
from
(select name, object_id from sys.tables where schema_id = (select schema_id from sys.schemas where name = 'dwh')
and name like 'Dim_%' )a)x
"""

object_dim_bridge = pd.read_sql(sqlObject_cnt_dwh_bridge, connDw)
object_dim = pd.read_sql(sqlObject_cnt_dwh_dim, connDw)

dfObject_bridge = spark.createDataFrame(object_dim_bridge)
dfObject_dim = spark.createDataFrame(object_dim)

dfObject = dfObject_bridge.unionByName(dfObject_dim)

concat_query = dfObject.select("query").rdd.flatMap(lambda x :x ).collect()
final_inner = ' '.join(concat_query).rsplit(' ',2)[0] # to remove last union all
object_dim = pd.read_sql(final_inner, connDw)
if object_dim.empty:
    print("There are no records in dim & Bridge tables")
else:
    dfObject_dim1 = spark.createDataFrame(object_dim)
    dfObject_dim1.createOrReplaceTempView("t1_cnt")
    df_cnt = spark.sql("select * from t1_cnt ")
    display(df_cnt.filter(df_cnt["cnt"] == 0))
    

# COMMAND ----------

# DBTITLE 1,Checking all BED from Dim/Bridge tables

sqlObject_BED_dwh_dim = """
select distinct tbl_nm, query from(
select a.name as tbl_nm, b.name as col_nm, 
       concat('select distinct''',a.name,''' as tbl_nm, dwh_valid_from as valid_from_timestamp from dwh.',a.name,' where dwh_valid_to is null union all') as query
from
(select name, object_id from sys.tables where schema_id = (select schema_id from sys.schemas where name = 'dwh')
and name like 'Dim_%' )a
INNER JOIN 
(select * from sys.columns where substring(name,1,3) !='dwh' and name like '%_HBK')b
on a.object_id =b.object_id)x
"""

sqlObject_BED_dwh_bridge = """
select tbl_nm,  query from(
select a.name as tbl_nm, concat('select distinct''',a.name,''' as tbl_nm, dwh_valid_from as valid_from_timestamp from dwh.',a.name,' where dwh_valid_to is null union all') as query
from (select name from sys.tables where schema_id = (select schema_id from sys.schemas where name = 'dwh') and substring(name,1,7) ='Bridge_' )a
)x
"""

object_BED_bridge = pd.read_sql(sqlObject_BED_dwh_bridge, connDw)
object_BED_dim = pd.read_sql(sqlObject_BED_dwh_dim, connDw)

dfObject_BED_bridge = spark.createDataFrame(object_BED_bridge)
dfObject_BED_dim = spark.createDataFrame(object_BED_dim)

dfObject_BED = dfObject_BED_dim.unionByName(dfObject_BED_bridge)
concat_BED_query = dfObject_BED.select("query").rdd.flatMap(lambda x :x ).collect()
final_BED_inner = ' '.join(concat_BED_query).rsplit(' ',2)[0] # to remove last union all
object_BED = pd.read_sql(final_BED_inner, connDw)
if object_BED.empty:
    print("***There are no records in Dim & Bridge tables***")
else:
    dfObject_BED1 = spark.createDataFrame(object_BED)
    dfObject_BED1.createOrReplaceTempView("t1_BED")
    df_BED = spark.sql("select * from t1_BED ")
    display(df_BED)

# COMMAND ----------

# DBTITLE 1,Count check b/w Staging & Dim for Bordereaux  
bdx_stg_dim_cnt_qry = """
select 'STAGING_Bordereaux' as src_trg , 'Dim_Bdx_Bordereaux' as tmp_col , count(*) as cnt  from (
select distinct BordereauxReference
FROM [Staging].[Bordereaux]
where lakedeletedtimestamp is null and lakevalidtotimestamp is null )Z
union all 
select 'Dim_Bdx_Bordereaux' as src_trg , 'Dim_Bdx_Bordereaux' as tmp_col , count(*)-3 as cnt  from dwh.Dim_Bdx_Bordereaux where dwh_Valid_to is null
UNION ALL 
select 'STAGING_Bordereaux_Claim_Movements' as src_trg , 'Dim_Bdx_Claim' as tmp_col , count(*) as cnt  from (
select distinct 
BinderReference,  DeclarationReference,  ClaimReference,  OriginalRowOrder
FROM [Staging].[Bordereaux_Claim_Movements] where lakedeletedtimestamp is null and lakevalidtotimestamp is null  )Z
union all 
select 'Dim_Bdx_Claim' as src_trg , 'Dim_Bdx_Claim' as tmp_col , count(*)-3 as cnt  from dwh.Dim_Bdx_Claim where dwh_Valid_to is null
UNION ALL 
select 'Staging_Bordereaux_Risk_Premium' as src_trg , 'Dim_Bdx_Declaration' as tmp_col , count(*) as cnt  from (
select distinct BinderReference,  DeclarationReference
FROM [Staging].[Bordereaux_Risk_Premium]
WHERE lakedeletedtimestamp is null and lakevalidtotimestamp is null
UNION
select distinct BinderReference,  DeclarationReference
FROM [Staging].Bordereaux_Claim_Movements
WHERE  lakedeletedtimestamp is null and lakevalidtotimestamp is null ) Z
union all
select 'Dim_Bdx_Declaration' as src_trg , 'Dim_Bdx_Declaration' as tmp_col , count(*)-3 as cnt  from DWH.Dim_Bdx_Declaration where dwh_Valid_to is null
UNION ALL 
select 'STAGING_Bordereaux_Risk_Premium' as src_trg , 'Dim_Bdx_Insurable_Interest_Cargo' as tmp_col , count(*) as cnt  from (
select distinct 
BinderReference,  DeclarationReference,  RiskReference,  OriginalRiskOrder
FROM [Staging].[Bordereaux_Risk_Premium] where lakedeletedtimestamp is null and lakevalidtotimestamp is null  )Z
union all 
select 'Dim_Bdx_Insurable_Interest_Cargo' as src_trg , 'Dim_Bdx_Insurable_Interest_Cargo' as tmp_col , count(*)-3 as cnt  from dwh.Dim_Bdx_Insurable_Interest_Cargo where dwh_Valid_to is null
UNION ALL 
select 'STAGING_Bordereaux_Risk_Premium' as src_trg , 'Dim_Bdx_Insurable_Interest_Characteristics' as tmp_col , count(*) as cnt  from (
select distinct 
BinderReference,  DeclarationReference,  RiskReference,  OriginalRiskOrder
FROM [Staging].[Bordereaux_Risk_Premium]  where lakedeletedtimestamp is null and lakevalidtotimestamp is null )Z
union all 
select 'Dim_Bdx_Insurable_Interest_Characteristics' as src_trg , 'Dim_Bdx_Insurable_Interest_Characteristics' as tmp_col , count(*)-3 as cnt  from dwh.Dim_Bdx_Insurable_Interest_Characteristics where dwh_Valid_to is null
UNION ALL 
select 'STAGING_Bordereaux_Risk_Premium' as src_trg , 'Dim_Bdx_Insurable_Interest_Classification' as tmp_col , count(*) as cnt  from (
select distinct 
BinderReference,  DeclarationReference,  RiskReference,  OriginalRiskOrder
FROM [Staging].[Bordereaux_Risk_Premium] where lakedeletedtimestamp is null and lakevalidtotimestamp is null  )Z
union all 
select 'Dim_Bdx_Insurable_Interest_Classification' as src_trg , 'Dim_Bdx_Insurable_Interest_Classification' as tmp_col , count(*)-3 as cnt  from dwh.Dim_Bdx_Insurable_Interest_Classification where dwh_Valid_to is null
UNION ALL 
select 'STAGING_Bordereaux_Risk_Premium' as src_trg , 'Dim_Bdx_Insurable_Interest_Liability' as tmp_col , count(*) as cnt  from (
select distinct BinderReference,  DeclarationReference,  RiskReference,  OriginalRiskOrder
FROM [Staging].[Bordereaux_Risk_Premium] where lakedeletedtimestamp is null and lakevalidtotimestamp is null  )Z
union all 
select 'Dim_Bdx_Insurable_Interest_Liability' as src_trg , 'Dim_Bdx_Insurable_Interest_Liability' as tmp_col , count(*)-3 as cnt  from dwh.Dim_Bdx_Insurable_Interest_Liability where dwh_Valid_to is null
UNION ALL 
select 'STAGING_Bordereaux_Risk_Premium' as src_trg , 'Dim_Bdx_Insurable_Interest_Limit' as tmp_col , count(*) as cnt  from (
select distinct 
BinderReference,  DeclarationReference,  RiskReference,  OriginalRiskOrder
FROM [Staging].[Bordereaux_Risk_Premium] where lakedeletedtimestamp is null and lakevalidtotimestamp is null )Z
union all 
select 'Dim_Bdx_Insurable_Interest_Limit' as src_trg , 'Dim_Bdx_Insurable_Interest_Limit' as tmp_col , count(*)-3 as cnt  from dwh.Dim_Bdx_Insurable_Interest_Limit where dwh_Valid_to is null
UNION ALL 
select 'STAGING_Bordereaux_Risk_Premium' as src_trg , 'Dim_Bdx_Insurable_Interest_Motor' as tmp_col , count(*) as cnt  from (
select distinct 
BinderReference,  DeclarationReference,  RiskReference,  OriginalRiskOrder
FROM [Staging].[Bordereaux_Risk_Premium] where lakedeletedtimestamp is null and lakevalidtotimestamp is null )Z
union all 
select 'Dim_Bdx_Insurable_Interest_Motor' as src_trg , 'Dim_Bdx_Insurable_Interest_Motor' as tmp_col , count(*)-3 as cnt  from dwh.Dim_Bdx_Insurable_Interest_Motor where dwh_Valid_to is null
UNION ALL 
select 'STAGING_Bordereaux_Risk_Premium' as src_trg , 'Dim_Bdx_Insurable_Interest_Property' as tmp_col , count(*) as cnt  from (
select distinct 
BinderReference,  DeclarationReference,  RiskReference,  OriginalRiskOrder
FROM [Staging].[Bordereaux_Risk_Premium] where lakedeletedtimestamp is null and lakevalidtotimestamp is null )Z
union all 
select 'Dim_Bdx_Insurable_Interest_Property' as src_trg , 'Dim_Bdx_Insurable_Interest_Property' as tmp_col , count(*)-3 as cnt  from dwh.Dim_Bdx_Insurable_Interest_Property where dwh_Valid_to is null
UNION ALL 
select 'STAGING_Bordereaux_Risk_Premium' as src_trg , 'Dim_Bdx_Insurable_Interest' as tmp_col , count(*) as cnt  from (
select distinct 
BinderReference,  DeclarationReference,  RiskReference,  OriginalRiskOrder
FROM [Staging].[Bordereaux_Risk_Premium] where lakedeletedtimestamp is null and lakevalidtotimestamp is null )Z
union all 
select 'Dim_Bdx_Insurable_Interest' as src_trg , 'Dim_Bdx_Insurable_Interest' as tmp_col , count(*)-3 as cnt  from dwh.Dim_Bdx_Insurable_Interest where dwh_Valid_to is null
UNION ALL 
select 'STAGING_Bordereaux_Risk_Premium' as src_trg , 'Dim_Bdx_Transaction_Type' as tmp_col , count(*) as cnt  from (
select distinct Transaction_TransactionType_OriginalPremiumEtc COLLATE SQL_Latin1_General_CP1_CS_AS AS Transaction_TransactionType_OriginalPremiumEtc
FROM [Staging].[Bordereaux_Risk_Premium] where lakedeletedtimestamp is null and lakevalidtotimestamp is null  )Z
union all 
select 'Dim_Bdx_Transaction_Type' as src_trg , 'Dim_Bdx_Transaction_Type' as tmp_col , count(*)-3 as cnt  from dwh.Dim_Bdx_Transaction_Type where dwh_Valid_to is null
"""
bdx_pddf = pd.read_sql(bdx_stg_dim_cnt_qry, connDw)
bdx_df = spark.createDataFrame(bdx_pddf)
# display(bdx_df)
w = Window.partitionBy('tmp_col').orderBy('cnt')
bdx_df2 = bdx_df.withColumn('rn',row_number().over(w)).withColumn("lead_cnt",lead("cnt",1).over(w))
# display(bdx_df2)
bdx_df3 = bdx_df2.filter(bdx_df2.rn ==1).withColumn('count_diff',bdx_df2.cnt - bdx_df2.lead_cnt)
# display(bdx_df3.filter(bdx_df3.count_diff !=0))
print("Stage and Dims counts are not matching for following Bdx Dims ---->\n" ,'\n'.join(bdx_df3.filter(bdx_df3.count_diff !=0).select("tmp_col").rdd.flatMap(lambda x:x).collect()))

# COMMAND ----------

print("-----------------------------------------------------------------------------------------------------------------------\n ------------------------------------UTs execution completed for Staging/Dims/Bridge------------------------------------")