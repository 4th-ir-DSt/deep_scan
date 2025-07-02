# Databricks notebook source
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

# DBTITLE 1,Getting the datetime filter
datetime_filter = """
select end_datetime from (
select cast(tbl1.end_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time' as datetime) as end_datetime
,case when tbl2.count >0 and tbl1.Rn=1  then 'Yes'
     when tbl2.count =0 and tbl1.Rn=2  then 'Yes'
     else 'No'
end as datetime_filter
from 
(select batch_id,start_datetime,end_datetime,
ROW_NUMBER() OVER(ORDER BY batch_id DESC ) Rn
 from audit.tbl_batch where end_datetime is not null and status='completed' and schedule_id = 
(select schedule_id from config.tbl_schedule where schedule_reference = 'Conformed & DWH')
) tbl1
,
(select count(*) as count from audit.tbl_batch where start_datetime >
(select max(end_datetime) from audit.tbl_batch where status='completed' and schedule_id = 
(select schedule_id from config.tbl_schedule where schedule_reference = 'Conformed & DWH'))) tbl2
where tbl1.rn<3 ) tbl3 where datetime_filter ='Yes'"""

pd_datetime_filter1 = pd.read_sql_query(datetime_filter,connMeta)
pd_datetime_filter = pd_datetime_filter1.astype(str).end_datetime[0][0:19]
#print(pd_datetime_filter)


# COMMAND ----------

# DBTITLE 1,Last Load Run Timing across phase
batchTaskQuery_latest = f"""
select temp2.batch_id, 
    CASE WHEN ts.schedule_id is not null then replace(ts.schedule_reference, ' Raw & Standardised','')
        ELSE 'All Sources'
    END AS source_name,
    temp2.phase_name,
CASE WHEN temp2.end_time is  null then  COALESCE(b.status,'pending')
    WHEN temp2.end_time is  not null then 'completed'
    ELSE b.status 
end as status,
cast(temp2.start_time AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time' as datetime) as start_time
,CASE WHEN temp2.end_time is null then cast(b.end_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time' as datetime)
     else cast(temp2.end_time AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time' as datetime)
end as end_time,
CASE WHEN temp2.time_taken_in_mins is  null then CONVERT(CHAR(5), DATEADD(SECOND, 60 * DATEDIFF(minute,temp2.start_time,b.end_datetime),'00:00'), 8)
     ELSE CONVERT(CHAR(5), DATEADD(SECOND, 60 * temp2.time_taken_in_mins, '00:00'), 8)
END as time_taken_in_hh_mm,
    CASE WHEN temp2.time_taken_in_mins is  null then DATEDIFF(minute,temp2.start_time,b.end_datetime)
     else temp2.time_taken_in_mins
end as time_taken_in_mins
from audit.tbl_batch b right join
    (select main_tbl.batch_id, COALESCE(main_tbl.phase_name,dummy_tbl.phase_name) as phase_name,
        COALESCE(main_tbl.status,dummy_tbl.status) as status,
        main_tbl.start_time,
        main_tbl.end_time,
        main_tbl.time_taken_in_mins
    from (select temp1.batch_id, temp1.phase_name
 , temp1.status,
            min (temp1.start_time) as start_time , max(temp1.end_time) as end_time ,
            avg(temp1.time_taken_in_mins) as time_taken_in_mins
        from (select distinct s.source_name,
                case when adf.pipeline_parameters like '%|10' then 'warehouse_dims'
     when adf.pipeline_parameters like '%|20' then 'warehouse_bridges'
     when adf.pipeline_parameters like '%|30' then 'warehouse_facts'
     else p.phase_name
end as phase_name,
                adf.batch_id, adf.pipeline_execution_starttime as start_time,
                adf.pipeline_execution_endtime  as end_time, adf.pipeline_execution_status as status,
                DATEDIFF(minute,adf.pipeline_execution_starttime,adf.pipeline_execution_endtime) as time_taken_in_mins
            from config.tbl_source s, audit.tbl_batch_task bt, config.tbl_phase p,
                (select substring(pipeline_parameters,CHARINDEX('|',pipeline_parameters,2)+1,len(pipeline_parameters)) as new_param, batch_id, pipeline_parameters,
                    pipeline_execution_starttime, pipeline_execution_endtime, pipeline_execution_status
                from audit.tbl_log_adf_pipeline_execution
                where pipeline_execution_starttime >= '{pd_datetime_filter}'
                    and pipeline_parameters like '%|%') adf,
                (select max(bt.batch_id) max_bid, bt.phase_id, bt.source_id
                from audit.tbl_batch_task bt, config.tbl_source s
                where 
s.source_name <> 'BordereauxMDS' and s.source_id = bt.source_id and bt.batch_task_start_datetime >= '{pd_datetime_filter}'
                group by bt.phase_id ,bt.source_id ) temp1
            where bt.batch_id = temp1.max_bid and bt.phase_id = temp1.phase_id
                and temp1.phase_id = p.phase_id
                and s.source_id = temp1.source_id and adf.batch_id=temp1.max_bid
                and adf.pipeline_parameters like '%|%'
                and left(adf.new_param,CHARINDEX('|',adf.new_param,1)-1) = bt.phase_id) temp1
        group by temp1.phase_name,temp1.status,temp1.batch_id) main_tbl right join
        (                                                    select top 1
                'raw' as phase_name, 'pending' as status
            from config.tbl_source
        UNION
            select top 1
                'standardised' as phase_name, 'pending' as status
            from config.tbl_source
        UNION
            select top 1
                'conformed' as phase_name, 'pending' as status
            from config.tbl_source
        UNION
            select top 1
                'staging-warehouse' as phase_name, 'pending' as status
            from config.tbl_source
        UNION
            select top 1
                'warehouse_dims' as phase_name, 'pending' as status
            from config.tbl_source
        UNION
            select top 1
                'warehouse_bridges' as phase_name, 'pending' as status
            from config.tbl_source
        UNION
            select top 1
                'warehouse_facts' as phase_name, 'pending' as status
            from config.tbl_source
) dummy_tbl
        on 
main_tbl.phase_name = dummy_tbl.phase_name) temp2
    on b.batch_id=temp2.batch_id
    left join config.tbl_schedule ts on b.schedule_id=ts.schedule_id and schedule_reference like '%Raw & Standardised%'
order by COALESCE(temp2.batch_id, 999999999),5"""

pd_df = pd.read_sql_query(batchTaskQuery_latest,connMeta)
# set the max columns to none
pd.set_option('display.max_columns', None)

if pd_df.empty:
  batchTaskQuery_latest = """select top 1 'No Data' as phase_name, 'No Data' as source_name, 'No Data' as status,'No Data' as start_time,
  'No Data' as end_time, 'No Data' as time_taken_in_mins from config.tbl_source"""
  pd_df = pd.read_sql_query(batchTaskQuery_latest,connMeta)
  display(pd_df)
  print('No Data to display !!')
else:
    pd.set_option('display.max_columns', None)
    display(pd_df)

# COMMAND ----------

# DBTITLE 1,Tables Load Status across phase
batchTaskQuery_tbl_status = f"""
select main_data_tbl.current_batch_id, main_data_tbl.source_name,main_data_tbl.phase_name,
    case when main_data_tbl.phase_name like 'Warehouse_%'
     then left(substring(main_data_tbl.entity_name,CHARINDEX('_',main_data_tbl.entity_name,2)+10,len(main_data_tbl.entity_name)),len(substring(main_data_tbl.entity_name,CHARINDEX('_',main_data_tbl.entity_name,2)+10,len(main_data_tbl.entity_name)))-1)
     else main_data_tbl.entity_name 
end as entity_name,
    main_data_tbl.process, main_data_tbl.status,
    last_load_tbl.last_run_batch_id, last_load_tbl.last_run_status,
    cast(main_data_tbl.current_run_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time' as datetime) as current_run_start_time, 
    cast( main_data_tbl.current_run_end_time AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time' as datetime) as current_run_end_time,
    DATEDIFF(minute,main_data_tbl.current_run_start_time,main_data_tbl.current_run_end_time) as time_taken_in_mins,
    avg_time_tbl.historical_average_run_time_in_mins
from
    (select case 
    when lower(bt.activity_name) like '%dim%' then 'warehouse_dims'
    when lower(bt.activity_name) like '%bridge%' then 'warehouse_bridges'
    when lower(bt.activity_name) like '%fact%' then 'warehouse_facts'
    else p.phase_name
end phase_name,
        case 
    when e.entity_name like '%Entity that runs DWH SPs%' then bt.activity_name
    else e.entity_name
end entity_name,
        s.source_name,
        bt.activity_name as process,
        bt.batch_task_status as status,
        bt.batch_id as current_batch_id,
        batch_task_start_datetime as current_run_start_time,
        batch_task_end_datetime  as current_run_end_time
    from audit.tbl_batch_task bt, config.tbl_task t , config.tbl_entity e , config.tbl_phase p, config.tbl_source s,
        (select max(bt.batch_id) max_batch_id, bt.phase_id, bt.source_id
        from audit.tbl_batch_task bt, config.tbl_source s
        where 
s.source_name <> 'BordereauxMDS' and s.source_id = bt.source_id and bt.batch_task_start_datetime >= '{pd_datetime_filter}'
        group by bt.phase_id ,bt.source_id ) temp
    where batch_id=temp.max_batch_id and bt.task_id=t.task_id and t.entity_id=e.entity_id and
        bt.phase_id=temp.phase_id and bt.source_id=temp.source_id
        and p.phase_id = bt.phase_id and bt.source_id = s.source_id
        and batch_task_start_datetime >= '{pd_datetime_filter}' ) main_data_tbl ,
    (select phase_name, entity_name, process, avg(time_taken_in_mins) as historical_average_run_time_in_mins
    from (
select case 
    when lower(bt.activity_name) like '%dim%' then 'warehouse_dims'
    when lower(bt.activity_name) like '%bridge%' then 'warehouse_bridges'
    when lower(bt.activity_name) like '%fact%' then 'warehouse_facts'
    else p.phase_name
end phase_name,
            case 
    when e.entity_name like '%Entity that runs DWH SPs%' then bt.activity_name
    else e.entity_name
end entity_name,
            bt.activity_name as process,
            DATEDIFF(minute,batch_task_start_datetime,batch_task_end_datetime) as time_taken_in_mins
        from audit.tbl_batch_task bt, config.tbl_task t , config.tbl_entity e , config.tbl_phase p, config.tbl_source s,
            (select distinct batch_id, bt.phase_id, bt.source_id
            from audit.tbl_batch_task bt, config.tbl_source s
            where 
s.source_name <> 'BordereauxMDS' and s.source_id = bt.source_id
            group by bt.phase_id ,bt.source_id,bt.batch_id ) temp
        where bt.batch_id=temp.batch_id and bt.task_id=t.task_id and t.entity_id=e.entity_id and
            bt.phase_id=temp.phase_id and bt.source_id=temp.source_id
            and p.phase_id = bt.phase_id and bt.source_id = s.source_id and bt.batch_task_status = 'completed' and bt.activity_name <> 'load_na_dbdt_sqdw' 
) temp1
    group by phase_name,entity_name,process ) avg_time_tbl,
    (select case 
    when lower(bt.activity_name) like '%dim%' then 'warehouse_dims'
    when lower(bt.activity_name) like '%bridge%' then 'warehouse_bridges'
    when lower(bt.activity_name) like '%fact%' then 'warehouse_facts'
    else p.phase_name
end phase_name,
        case 
    when e.entity_name like '%Entity that runs DWH SPs%' then bt.activity_name
    else e.entity_name
end entity_name,
        bt.activity_name as process,
        bt.batch_task_status as last_run_status,
        bt.batch_id as last_run_batch_id
    from audit.tbl_batch_task bt, config.tbl_task t , config.tbl_entity e , config.tbl_phase p, config.tbl_source s,
        (select temp1.batch_id as max_batch_id, temp1.phase_id, temp1.source_id
        from (
select ROW_NUMBER() over (Partition BY temp.phase_id ,temp.source_id order by temp.batch_id desc) as 'rowNum',
                temp.batch_id, temp.phase_id, temp.source_id
            from
                (select distinct batch_id, bt.phase_id, bt.source_id
                from audit.tbl_batch_task bt, config.tbl_source s
                where s.source_name <> 'BordereauxMDS' and s.source_id = bt.source_id) temp) temp1
        where rowNum=2) temp
    where batch_id=temp.max_batch_id and bt.task_id=t.task_id and t.entity_id=e.entity_id and
        bt.phase_id=temp.phase_id and bt.source_id=temp.source_id
        and p.phase_id = bt.phase_id and bt.source_id = s.source_id) last_load_tbl
where avg_time_tbl.phase_name = main_data_tbl.phase_name and avg_time_tbl.entity_name = main_data_tbl.entity_name
    and avg_time_tbl.process=main_data_tbl.process and last_load_tbl.phase_name = main_data_tbl.phase_name and
    last_load_tbl.entity_name = main_data_tbl.entity_name
    and last_load_tbl.process=main_data_tbl.process
    and (DATEDIFF(minute,main_data_tbl.current_run_start_time,main_data_tbl.current_run_end_time) >= 0 or
    DATEDIFF(minute,main_data_tbl.current_run_start_time,main_data_tbl.current_run_end_time) is null )
order by 1,2,3"""


pd_df = pd.read_sql_query(batchTaskQuery_tbl_status,connMeta)
# set the max columns to none
pd.set_option('display.max_columns', None)

if pd_df.empty:
  batchTaskQuery_tbl_status = """select top 1 'No Data' as phase_name, 'No Data' as source_name,'No Data' as entity_name, 
'No Data' as process, 'No Data' as status, 'No Data' as current_batch_id, 'No Data' as last_run_batch_id,'No Data' as last_run_status, 
'No Data' as current_run_start_time, 'No Data' as current_run_end_time, 'No Data' as time_taken_in_mins, 
'No Data' as historical_average_run_time_in_mins from  config.tbl_source"""
  pd_df = pd.read_sql_query(batchTaskQuery_tbl_status,connMeta)
  display(pd_df)
  print('No Data to display !!')
else:
    pd.set_option('display.max_columns', None)
    display(pd_df)

# COMMAND ----------

# DBTITLE 1,Last Data Load Error Stats
batchTaskQuery_error_latest = f"""
select bt.batch_id as batch_id,
case 
    when lower(bt.activity_name) like '%dim%' then 'warehouse_dims'
    when lower(bt.activity_name) like '%bridge%' then 'warehouse_bridges'
    when lower(bt.activity_name) like '%fact%' then 'warehouse_facts'
    else p.phase_name
end phase_name,
s.source_name,
case 
    when e.entity_name like '%Entity that runs DWH SPs%' 
    then left(substring(bt.activity_name,CHARINDEX('_',bt.activity_name,2)+10,len(bt.activity_name)),len(substring(bt.activity_name,CHARINDEX('_',bt.activity_name,2)+10,len(bt.activity_name)))-1)
    else e.entity_name
end entity_name,
bt.activity_name as process,
el.error_message,
cast(el.error_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time' as datetime) as error_datetime
from audit.tbl_batch_task bt, config.tbl_task t , config.tbl_entity e , config.tbl_phase p, config.tbl_source s, audit.tbl_error_log el,
(select distinct bt.batch_id as max_batch_id,bt.source_id,bt.phase_id from audit.tbl_batch_task bt,config.tbl_source s where s.source_name <> 'BordereauxMDS' and 
s.source_id = bt.source_id and bt.batch_task_start_datetime >= '{pd_datetime_filter}') temp
where bt.batch_id=temp.max_batch_id and bt.task_id=t.task_id and t.entity_id=e.entity_id and 
bt.phase_id=temp.phase_id and bt.source_id=temp.source_id
and p.phase_id = bt.phase_id and bt.source_id = s.source_id and el.batch_id=temp.max_batch_id 
and bt.batch_task_id=el.batch_task_id and el.error_datetime >= '{pd_datetime_filter}'"""


pd_df = pd.read_sql_query(batchTaskQuery_error_latest,connMeta)

if pd_df.empty:
  batchTaskQuery_error_latest_new = """select top 1 'No Data' as batch_id , 'No Data' as phase_name , 'No Data' as source_name,'No Data' as entity_name,'No Data' as process,'No Data' as error_message  from config.tbl_source;"""
  pd_df = pd.read_sql_query(batchTaskQuery_error_latest_new,connMeta)
  display(pd_df)
  print('No Data to display !!')
else:
    pd.set_option('display.max_columns', None)
    display(pd_df)
    
connMeta.close()