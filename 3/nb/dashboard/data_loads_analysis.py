# Databricks notebook source
# DBTITLE 1,Setup Parameter
dbutils.widgets.removeAll()
dbutils.widgets.text("days_required","5","Enter the no. of days for which analysis is required : ")
days_required = dbutils.widgets.get("days_required")

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
import pyodbc
import numpy as np
import re
import pandas as pd

# COMMAND ----------

# DBTITLE 1,DB Connection
#ASQLDB Metadata DB
dbconnMeta = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')
connMeta = pyodbc.connect(dbconnMeta, autocommit = True)
cursorMeta = connMeta.cursor()

#Synapse Datawarehouse
#dbconnDw = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-syn-connection')
#connDw = pyodbc.connect(dbconnDw, autocommit = True)
#cursorDw = connDw.cursor()

# COMMAND ----------

# DBTITLE 1,Average Load Time across Phase for the given number of days
batchTaskQuery_trend = f"""
select temp.phase_name,CONVERT(date, temp.start_time, 101) AS 'Date',
DATEDIFF(minute,temp.start_time,temp.end_time) as time_taken_in_mins from 
(select bt.batch_id,p.phase_name,min(bt.batch_task_start_datetime) as start_time,max(bt.batch_task_end_datetime) as end_time
from config.tbl_source s, audit.tbl_batch_task bt,config.tbl_phase p,
(select distinct batch_id as max_bid,phase_id from audit.tbl_batch_task) temp1
where bt.batch_id = temp1.max_bid and bt.phase_id = temp1.phase_id and bt.batch_task_id = bt.batch_task_id
and bt.phase_id = p.phase_id and p.phase_name <> 'warehouse' and s.source_id = bt.source_id 
and lower(batch_task_status) = 'completed'
group by bt.batch_id,p.phase_name
UNION
select bt.batch_id,'warehouse_dims' as phase_name,min(bt.batch_task_start_datetime) as start_time,max(bt.batch_task_end_datetime) as end_time
from config.tbl_source s, audit.tbl_batch_task bt,config.tbl_phase p,
(select distinct batch_id as max_bid,phase_id from audit.tbl_batch_task ) temp1
where bt.batch_id = temp1.max_bid and bt.phase_id = temp1.phase_id and bt.batch_task_id = bt.batch_task_id
and bt.phase_id = p.phase_id and lower(bt.activity_name) like '%dim%' and s.source_id = bt.source_id 
and lower(batch_task_status) = 'completed'
group by bt.batch_id,p.phase_name
UNION
select bt.batch_id,'warehouse_bridges' as phase_name,min(bt.batch_task_start_datetime) as start_time,max(bt.batch_task_end_datetime) as end_time
from config.tbl_source s, audit.tbl_batch_task bt,config.tbl_phase p,
(select distinct batch_id as max_bid,phase_id from audit.tbl_batch_task ) temp1
where bt.batch_id = temp1.max_bid and bt.phase_id = temp1.phase_id and bt.batch_task_id = bt.batch_task_id
and bt.phase_id = p.phase_id and lower(bt.activity_name) like '%bridge%' and s.source_id = bt.source_id 
and lower(batch_task_status) = 'completed'
group by bt.batch_id,p.phase_name
UNION
select bt.batch_id,'warehouse_facts' as phase_name,min(bt.batch_task_start_datetime) as start_time,max(bt.batch_task_end_datetime) as end_time
from config.tbl_source s, audit.tbl_batch_task bt,config.tbl_phase p,
(select distinct batch_id as max_bid,phase_id from audit.tbl_batch_task ) temp1
where bt.batch_id = temp1.max_bid and bt.phase_id = temp1.phase_id and bt.batch_task_id = bt.batch_task_id
and bt.phase_id = p.phase_id and lower(bt.activity_name) like '%fact%' and s.source_id = bt.source_id 
and lower(batch_task_status) = 'completed'
group by bt.batch_id,p.phase_name) temp
where CONVERT(date, temp.start_time, 101) >= CONVERT(date, GETDATE()-{days_required}, 101)"""

pd_df = pd.read_sql_query(batchTaskQuery_trend,connMeta)

if pd_df.empty:
  batchTaskQuery_trend_new = """select top 1 'No Data' as phase_name , 'No Data' as Date , 'No Data' as time_taken_in_mins from config.tbl_source;"""
  pd_df = pd.read_sql_query(batchTaskQuery_trend_new,connMeta)
  display(pd_df)
  print('No Data to display !!')
else:
    pd.set_option('display.max_columns', None)
    display(pd_df)


# COMMAND ----------

# DBTITLE 1,Errors Count across Phase for the given number of days
batchTaskQuery_error_overall = f"""
select s.source_name,p.phase_name,count(el.error_log_id) as Count_of_Errors from audit.tbl_error_log el,config.tbl_source s, 
audit.tbl_batch_task bt,config.tbl_phase p,(select distinct batch_id as max_bid,phase_id from audit.tbl_batch_task where 
CONVERT(date, batch_task_start_datetime, 101) >= CONVERT(date, GETDATE()-{days_required}, 101) ) temp1
where el.batch_id = temp1.max_bid and bt.phase_id = temp1.phase_id and el.batch_task_id = bt.batch_task_id
and bt.phase_id = p.phase_id and p.phase_name <> 'warehouse' and s.source_id = bt.source_id
group by p.phase_name,s.source_name
union
select s.source_name,'warehouse_dims' as phase_name,count(el.error_log_id) as Count_of_Errors from audit.tbl_error_log el,
config.tbl_source s,audit.tbl_batch_task bt,config.tbl_phase p,
(select distinct batch_id as max_bid,phase_id from audit.tbl_batch_task where 
CONVERT(date, batch_task_start_datetime, 101) >= CONVERT(date, GETDATE()-{days_required}, 101) ) temp1
where lower(bt.activity_name) like '%dim%' and el.batch_id = temp1.max_bid and bt.phase_id = temp1.phase_id and 
el.batch_task_id = bt.batch_task_id and bt.phase_id = p.phase_id and s.source_id = bt.source_id
group by p.phase_name,s.source_name
union
select s.source_name,'warehouse_bridges' as phase_name,count(el.error_log_id) as Count_of_Errors from audit.tbl_error_log el,
config.tbl_source s,audit.tbl_batch_task bt,config.tbl_phase p,
(select distinct batch_id as max_bid,phase_id from audit.tbl_batch_task where 
CONVERT(date, batch_task_start_datetime, 101) >= CONVERT(date, GETDATE()-{days_required}, 101) ) temp1
where lower(bt.activity_name) like '%bridge%' and el.batch_id = temp1.max_bid and bt.phase_id = temp1.phase_id and 
el.batch_task_id = bt.batch_task_id and bt.phase_id = p.phase_id and s.source_id = bt.source_id
group by p.phase_name,s.source_name
union
select s.source_name,'warehouse_facts' as phase_name,count(el.error_log_id) as Count_of_Errors from audit.tbl_error_log el,
config.tbl_source s,audit.tbl_batch_task bt,config.tbl_phase p,
(select distinct batch_id as max_bid,phase_id from audit.tbl_batch_task where 
CONVERT(date, batch_task_start_datetime, 101) >= CONVERT(date, GETDATE()-{days_required}, 101) ) temp1
where lower(bt.activity_name) like '%dim%' and el.batch_id = temp1.max_bid and bt.phase_id = temp1.phase_id and 
el.batch_task_id = bt.batch_task_id and bt.phase_id = p.phase_id and s.source_id = bt.source_id
group by p.phase_name,s.source_name;"""

pd_df = pd.read_sql_query(batchTaskQuery_error_overall,connMeta)


if pd_df.empty:
  batchTaskQuery_error_overall_new = """select top 1 'No Data' as source_name , 'No Data' as phase_name , 'No Data' as Count_of_Errors from config.tbl_source;"""
  pd_df = pd.read_sql_query(batchTaskQuery_error_overall_new,connMeta)
  display(pd_df)
  print('No Data to display !!')
else:
    pd.set_option('display.max_columns', None)
    display(pd_df)

connMeta.close()