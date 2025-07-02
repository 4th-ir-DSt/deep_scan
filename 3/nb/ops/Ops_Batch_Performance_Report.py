# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Performance Monitoring Report
# MAGIC 
# MAGIC 
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>Batch_Performance_Report</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Helps understand Batch performance by Phase and Object </td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Manmohan</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2023/02/01</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Parameter Name</th>
# MAGIC     <th>Parameter Description</th>
# MAGIC     <th>Example Parameter</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>Batch Id</td>
# MAGIC     <td>@batchId to retrieve batch details</td>
# MAGIC     <td>@batchId = 201</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchTaskId</td>
# MAGIC     <td>@batchTaskId to retrieve batchTask details</td>
# MAGIC     <td>@batchTaskId = 10</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>adfPipeLineName</td>
# MAGIC     <td>@adfPipeLineName to retrieve DataFactory pipelinename details</td>
# MAGIC     <td>@adfPipeLineName = 'testpipepline'</td>
# MAGIC   </tr>
# MAGIC      
# MAGIC   </table>
# MAGIC   
# MAGIC ## Change Log
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Date</th>
# MAGIC     <th>Changed By</th>
# MAGIC     <th>Change Description</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/12/20</td>
# MAGIC     <td>Manmohan</td>
# MAGIC     <td>Initial Version</td>
# MAGIC   </tr>  
# MAGIC </table>
# MAGIC 
# MAGIC ## How to use this report?
# MAGIC <ol>
# MAGIC   <li>Enter Batch Id and Phase Id you are interested in and hit Run-all </li> 
# MAGIC   <li>In case of an invalid BatchId-PhaseId inputs or when there aren't any valid logs for the input parameters, th report will fail to execute.</li>
# MAGIC   
# MAGIC </ol>

# COMMAND ----------

# DBTITLE 1,Setup Batch Parameters
dbutils.widgets.removeAll()
dbutils.widgets.text("phase_id","4","Phase Id:")
dbutils.widgets.text("batch_id","392","Batch Id:")

batch_id = dbutils.widgets.get("batch_id")
phase_id = dbutils.widgets.get("phase_id")

# COMMAND ----------

# DBTITLE 1,Chart Functions
# Updates and displays a chart object
def displayChart(figure):
    fig.update_layout(
        title_font_size=22,
        font_size=12,
        title_font_family='Arial'
        )
    fig.show()

# COMMAND ----------

# DBTITLE 1,Setup ODBC Connection
import pandas as pd
import plotly.express as px
import pyodbc
import plotly.figure_factory as ff
import plotly.graph_objs as go

# Define sqlDbConn
def sqlDbConn(dbconn):    
    try:
        conn = pyodbc.connect(dbconn, autocommit = True)
        cursor = conn.cursor()
        #print("Established Database connection in the notebook")
        return conn,cursor
    except Exception as e:
        errorMessage="unable to establish DB connection: " + str(e)
        assert False

# Setup params
dbconn = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')

#call function sqlDbConn to establish Database connection with given scope and key values
try:    
    conn,cursor = sqlDbConn(dbconn)
    print("Successfully Established SQL Connection")
except Exception as e:
    errorMessage="unable to establish DB connection: " + str(e)
    print(errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Get log data from Audit/log Database
try:
    #define SqlQuery to query log database
    batchTaskQuery = f"""
    SELECT
     bt.phase_id,p.phase_name,bt.batch_id,bt.batch_task_id
    ,Object_name
    ,LEFT(object_name,CHARINDEX('.',object_name,0)-1) AS DBSchema
    ,RIGHT(object_name,LEN(object_name)-CHARINDEX('.',object_name,0))  AS DBObject
    ,CONVERT(DATETIME2(0), MinTime)  AS StartTime
    ,CASE WHEN MaxTime <= MiNTime THEN GETDATE() 
			ELSE CONVERT(DATETIME2(0), MaxTime) 
	 END AS EndTime
    ,DATEDIFF(ss,MinTime,CASE WHEN MaxTime <= MiNTime THEN GETDATE() ELSE CONVERT(DATETIME2(0), MaxTime) END) AS Dur_ss
    ,STUFF(STUFF(CONVERT(VARCHAR(10),(CASE WHEN MaxTime <= MiNTime THEN GETDATE() ELSE CONVERT(datetime, MaxTime) END)-MinTime,108),6,1,':'),3,1,':') Dur_hms
    ,bt.batch_task_status
    ,bt.processing_group
    ,COALESCE(clu.cluster_name,'unknown') AS cluster_name
    FROM 
    (
      SELECT bt.batch_id,bt.batch_task_id, bt.task_id, bt.phase_id, bt.batch_task_status	  
	         ,COALESCE(bt.processing_group,-1) AS processing_group
			 ,COALESCE(bt.cluster_id,'unknown') AS cluster_id--, bt.activity_name
			 ,COALESCE(dob.object_name, bt.activity_name)AS object_name
			 ,MIN(bt.batch_task_start_datetime)MinTime, MAX(COALESCE(bt.batch_task_end_datetime, l.MaxErrorTime, GETDATE()))MaxTime
        FROM audit.tbl_batch_task bt

		--Get First and Last Error timestamps for eahc task within this batchId 
        LEFT 
		JOIN (SELECT batch_id, batch_task_id, MIN(error_datetime) MinErrorTime, MAX(error_datetime) MaxErrorTime 
				FROM [audit].[tbl_error_log]
			   WHERE batch_id = {batch_id} --607
			   GROUP 
			      BY batch_id, batch_task_id
             ) l 
		  ON bt.batch_task_id = l.batch_task_id 
		 AND bt.batch_id = l.batch_id

		 --Get Object name associated with each task
        LEFT 
		JOIN config.vw_task_object_map tom 
		  ON bt.task_id=tom.task_id 
		 AND tom.designation='destination'
        LEFT 
		JOIN config.tbl_object dob 
		  ON tom.object_id=dob.object_id
       WHERE bt.batch_id = {batch_id} --607
         AND bt.phase_id = {phase_id} --3
         AND batch_task_start_datetime IS NOT NULL
       GROUP 
	      BY bt.batch_id, bt.batch_task_id, bt.task_id, bt.phase_id, bt.batch_task_status, bt.processing_group, bt.cluster_id--, bt.activity_name
			,COALESCE(dob.object_name, bt.activity_name),LEFT(dob.object_name,CHARINDEX('.',dob.object_name,0)-1)

    ) bt 
    JOIN [config].tbl_task        ta          
	  ON ta.task_id           = bt.task_id
    JOIN [config].tbl_task_type   tt          
	  ON tt.task_type_id      = ta.task_type_id

    LEFT 
	JOIN [audit].tbl_batch b 
	  ON bt.batch_id = b.batch_id
    LEFT 
	JOIN [config].tbl_phase p 
	  ON p.phase_id = bt.phase_id
    LEFT 
	JOIN [config].tbl_databricks_cluster clu 
	  ON clu.cluster_id = bt.cluster_id

    ;
    """

    # print(batchTaskQuery)

    result = cursor.execute(batchTaskQuery)
    cursor.close()

    pd_df = pd.read_sql_query(batchTaskQuery,conn)
    sprk_df = spark.createDataFrame(pd_df)
    sprk_df.createOrReplaceTempView("results")
except Exception as e:
    errorMessage="Querying log database resulted in the error: " + str(e)
    print(errorMessage)
    assert False    

# COMMAND ----------

# DBTITLE 1,Display task logs in tabular format
# MAGIC %sql
# MAGIC -- SELECT * FROM results

# COMMAND ----------

# DBTITLE 1,Display batch tasks as Gantt Plot
# Assign Columns to variables
hover_name = pd_df['DBObject'].map(str) + " - "+ pd_df['batch_task_id'].map(str) 
start = pd_df['StartTime']
finish = pd_df['EndTime']
duration = pd_df['Dur_hms'].map(str)
dbSchema = pd_df['DBSchema']
taskStatus = pd_df['processing_group'].map(str)+"-"+pd_df['batch_task_status'] #pd_df['batch_task_status']
cluster_name = pd_df['cluster_name']
legend = dict(orientation="h"
              ,yanchor="bottom"
              ,y=1.0
              ,title="Processing group - Task Status"
              ,xanchor="center"
              ,x=0.5
              ,font=dict(family="Arial",size=14,color="darkgray"))
# Create Gantt Chart
fig = px.timeline(pd_df, x_start=start, x_end=finish, y=pd_df['DBObject'], 
                  title='Task Execution Timelines for <b>BatchId: '+ str(pd_df['batch_id'][0]) + ' | Phase Id: ' +str(pd_df['phase_id'][0]) +'</b> | Cluster: ' + cluster_name[0],
                  height=max(pd_df.batch_task_id.count()*24, 480),
                  #width=1024,
                  color=taskStatus, 
                  text = pd_df['DBObject'] +' : ' + duration,
                  hover_name=hover_name, #hovertemplate = "%{label}: <br>Table: %{task}", #hover_data=[dbSchema],
                  hover_data = [start, finish],
                  category_orders = duration,
                  opacity=0.7, 
                  range_x=None,
                  range_y=None,
                  template='simple_white' #'seaborn'
#                   ,rangeslider_visible=True
                 )

# Upade/Change Layout
fig.update_layout(showlegend=True, 
                  legend=legend
#                   ,color_discrete_map={"completed":"Green", "running":"Orange"}
                 )
displayChart(fig)
#fig.show()

# COMMAND ----------

# DBTITLE 1,Display task duration as Histogram
fig_pareto = px.histogram(pd_df, x=pd_df['Dur_ss'], nbins=15,                  
                  title='Task Duration Distribution for <b>BatchId: '+ str(pd_df['batch_id'][0]) + ' | Phase Id: ' +str(pd_df['phase_id'][0]) +'</b> | Cluster: ' + cluster_name[0],
                  height=420, #pd_df.batch_task_id.count()*24, 
                  width = 800,        
                  #color=taskStatus,
                  marginal = "rug",
                  text_auto = True,
#                   cumulative_enabled = True,
#                   hover_name=hover_name, #hovertemplate = "%{label}: <br>Table: %{task}", #hover_data=[dbSchema],
                  hover_data = [pd_df['Dur_hms'], pd_df['DBObject']],
#                   category_orders = duration,
                  opacity=0.7, 
                  range_x=None,
                  range_y=None,
                  template='simple_white' #'seaborn'
#                   ,rangeslider_visible=True
                 )#.update_xaxes(categoryorder='total ascending')
fig_pareto.show()