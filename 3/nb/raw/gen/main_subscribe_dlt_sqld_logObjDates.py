# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>main_subscribe_dlt_sqld_logObjDates</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading raw dlt record in tblObjectAvailability</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>RahulC</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/12/23</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC 
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Parameter Name</th>
# MAGIC     <th>Parameter Description</th>
# MAGIC     <th>Example Parameter</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchId</td>
# MAGIC     <td>@batchId to retrieve batch details</td>
# MAGIC     <td>@batchId = 7</td>
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
# MAGIC    <tr>
# MAGIC     <td>notebookName</td>
# MAGIC     <td>@notebookName to retrieve notebookname </td>
# MAGIC     <td>@notebookName = 'testnotebook'</td>
# MAGIC   </tr>
# MAGIC     <tr>
# MAGIC     <td>clusterId</td>
# MAGIC     <td>@clusterId to retrieve cluster details </td>
# MAGIC     <td>@clusterId = '12345'</td>
# MAGIC   </tr>
# MAGIC    <tr>
# MAGIC     <td>sourceId</td>
# MAGIC     <td>@sourceId to retrieve source details </td>
# MAGIC     <td>@sourceId = '1'</td>   
# MAGIC   </table>
# MAGIC   
# MAGIC ## Change Log
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Date</th>
# MAGIC     <th>Changed By</th>
# MAGIC     <th>Change Description</th>
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

# DBTITLE 1,Run log function notebook
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run standard function notebook
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run HVR functions notebook
# MAGIC %run ../../util/spe/hvr_functions

# COMMAND ----------

# DBTITLE 1,Import modules

try:
    import pandas as pd
    import pyodbc
    from datetime import datetime,timedelta
    from pyspark.sql.functions import lit,col,explode,concat_ws,create_map,struct,collect_list,coalesce,sequence 
    from functools import reduce
    from itertools import chain 
    from collections import defaultdict
    import json
    import sys
    from decimal import Decimal

    import numpy as np
    import re
    from pyspark.sql.functions import *
except Exception as e:
    errorMessage="Exception occurred while import modules " + str(e)
    assert False

# COMMAND ----------

# DBTITLE 1,Initialise variables
try:
    #varible for current_time
    currentTs=datetime.now()
  
    #get the last 3 positions of the microseconds as we need to remove them
    currentTsMicroseconds = int(str(currentTs.strftime('%f'))[3:6])
    #take away the last three microseconds
    currentTs = currentTs - timedelta(microseconds=currentTsMicroseconds)
    
    #get the date as an int format
    CreatedDate = int(currentTs.strftime('%Y%m%d'))
    createdDate=CreatedDate
    lastUpdateDate=createdDate
    #get the hour
    CreatedHour = currentTs.hour
    createdHour = CreatedHour
    createdTimestamp = currentTs
    lastUpdateTimestamp = currentTs
#   createdTimestamp
    lastUpdatedTimestamp = currentTs
    createdTime=currentTs
    #use the same time for the date also
    date=currentTs
#   createdBatchID=
    #PARAMETER FOR logError
    errorMessage = ''
    adfPipelineName = ''
    clusterId = ''
    notebookName = ''
    batchId = -1
    batchTaskId = -1
  
    #parameter for log_task_end
    batchTaskStatus = ''
    batchTaskSourceRows = 0
    batchTaskRowsLoaded = 0
    batchTaskRejectRows = 0
    batchTaskResult = ''
    batchTaskResultLocation = ''
    batchTaskProgressMessage = '' 
    
    #variable to retrieve the batch rows loaded or not
    retrieveBatchRowsLoaded = False
  
    deltaLiveSchema = "raw_subscribe"
    deltaLiveTable = "cdc_subscribe"
    
except Exception as e:
    errorMessage = "Exception occurred while variable initialisation :" + str(e)
    assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables and initialise error log location
try:
  #GET batchTaskId FROM WIDGETS   
    dbutils.widgets.text("batchTaskId","")
    batchTaskId = dbutils.widgets.get("batchTaskId")
  
    #GET batchId FROM WIDGETS
    dbutils.widgets.text("batchId","")
    batchId = dbutils.widgets.get("batchId")
    
    #GET sourceId  FROM WIDGETS   
    dbutils.widgets.text("sourceId","")
    sourceId  = dbutils.widgets.get("sourceId")
  
    #GET adfPipelineName  FROM WIDGETS
    dbutils.widgets.text("adfPipelineName","")
    adfPipelineName  = dbutils.widgets.get("adfPipelineName")
    
    #GET notebookName  FROM WIDGETS   
    dbutils.widgets.text("notebookName","")
    notebookName  = dbutils.widgets.get("notebookName")
  
    #GET clusterId  FROM WIDGETS
    dbutils.widgets.text("clusterId","")
    clusterId  = dbutils.widgets.get("clusterId")
  
  #assigned the source and batch to other variables that are referenced from the metadata
    SourceID=sourceId
    CreatedBatchID=batchId
    LastUpdatedBatchID=batchId
    lastUpdateBatchId=batchId
    createdBatchId=batchId
    validToTimestamp=None
    deletedTimestamp=None
    isActive=True
    sourceID=sourceId
    BatchID=batchId
  #call the get_logging_path function to create a log file path as a string and store it in a variable
    errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')  
except Exception as e:
    errorMessage = "Exception occurred while getting parameters and initiliasing error log location: " + str(e)
    assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn=dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')

#call function sqlDbConn to establish Database connection with given scope and key values
try:
  conn,cursor = sqlDbConn(dbconn,
                          batchTaskId,                          
                          adfPipelineName,
                          clusterId,
                          notebookName,
                          errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Successfully Established SQL Connection")
except Exception as e:
  errorMessage="unable to establish DB connection: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Check HVR Status with table_name/ Job name
# Check HVR Job status, if Refresh in progress or Job has errors recently then log and quit the notebook (gracefully or fail?)
try:
    hvrJobState = False

    #check if REFResh job is in progress
    hvrJobName ='subscribe-refr-hvrdevloc01-kafka' #'elgar-refr-elgarsubuat-kafka' #= f'{source_system}-refr-{source_location}-kafka'
    logTaskProgress(cursor,batchTaskId,f"Performing HVR API Checks for the Job [{hvrJobName}]")
    
    #get HVR job state
    hvrJobState,hvrJobHasErrors = getHvrRefreshStateByJobName(hvrJobName,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(hvrJobState,hvrJobHasErrors, sep="|")
    
    #Stop execution
    if hvrJobState != 'PENDING':
        #or not(hvrJobHasErrors):
        errorMessage=f"Exiting notebook execution, HVR job [{hvrJobName}] has State= {hvrJobState}, hvrJobHasErrors= {hvrJobHasErrors}" 
        logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation)
        print(errorMessage)
    #     dbutils.notebook.exit(errorMessage)  #Exit gracefully, this allows Standardsied to continue processing or cathup micro/threshold-batches
        assert False  #Fail the pipeline, Stops downstream steps
except Exception as e:
    errorMessage=f"Exception occured while performing HVR API Checks for the Job [{hvrJobName}] " + str(e)
    logToFile(errorLogFileLocation,errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Executing SP to get Maximum output datetime from tblObjectAvailability
# to get maximum output_datetime_to from tblObjectAvailability for rawdlt
try:
    query = f"exec [audit].[usp_get_rawdlt_availability_dates] {batchTaskId}"
    
    pd_df = pd.read_sql_query(query,conn)
    sprk_df = spark.createDataFrame(pd_df)
    destinationObjectId = sprk_df.select("destination_object").first()[0]
    sourceObjectId = sprk_df.select("source_object").first()[0]
    outputDatetimeTo = sprk_df.withColumn("output_datetime_to", to_timestamp(col("output_datetime_to"))).select("output_datetime_to").first()[0]
    
    maxDltDate = 0 if outputDatetimeTo is None else int(outputDatetimeTo.strftime("%Y%m%d"))
    maxDltDateHour = 0 if outputDatetimeTo is None else int(outputDatetimeTo.strftime("%Y%m%d%H"))
    logTaskProgress(cursor,batchTaskId,"Succesfully got the outputdatetime from tblObjectAvailability")
except Exception as e:
    errorMessage="Exception occured while fetching datetime from tblObjectAvailability " + str(e)
    logToFile(errorLogFileLocation,errorMessage)
    assert False

# COMMAND ----------

# display(sprk_df)

# COMMAND ----------

# DBTITLE 1,Get range for hvr_commit_timestamp from raw dlt table
# to get hvr_commit_timestamp from raw delta live table greater than timestamp in tblObjectAvailability
try:
    get_items_from_dlt_raw = f"""
    WITH A AS (
        SELECT MIN(hvr_commit_timestamp) AS min_hvr_commit_timestamp,
               MAX(hvr_commit_timestamp) AS max_hvr_commit_timestamp
         FROM {deltaLiveSchema}.{deltaLiveTable}
        WHERE (hvr_commit_date = {maxDltDate} AND hvr_commit_hour >= {maxDltDateHour}) 
           OR hvr_commit_date > {maxDltDate}
    )--Verify partitons are optimally used 
    SELECT
           MIN(min_hvr_commit_timestamp) AS MinDltDateTimeTo,
           MAX(max_hvr_commit_timestamp) AS MaxDltDateTimeTo
      FROM A
     WHERE max_hvr_commit_timestamp > '{outputDatetimeTo}'
    """
    
    df = spark.sql(get_items_from_dlt_raw)
    maxDltDateTime = df.withColumn("MaxDltDateTimeTo", to_timestamp(col("MaxDltDateTimeTo"))).select('MaxDltDateTimeTo').first()[0]
    minDltDateTime = df.withColumn("MinDltDateTimeTo", to_timestamp(col("MinDltDateTimeTo"))).select('MinDltDateTimeTo').first()[0]
    if maxDltDateTime and minDltDateTime is None:
        raise Exception("Raw Dlt is either empty or no new data in raw delta table")
    else:
        hvrCommitTimestampFrom = minDltDateTime
        hvrCommitTimestampTo = maxDltDateTime
    logTaskProgress(cursor,batchTaskId,"Got the range of hvr_commit_timestamp from raw_dlt table")
except Exception as e:
    errorMessage="Exception occured while getting timestamp from raw delta table " + str(e)
    logToFile(errorLogFileLocation,errorMessage)
    assert False
    

# COMMAND ----------

# DBTITLE 1,Upsert the raw_dlt details to tblObjectAvailability
# updating the tblObjectAvailability with latest hvr_commit_timestamp
try:
    if str(outputDatetimeTo) == '2000-01-01 00:00:01':
        execute_query = f"exec [audit].[usp_log_availability_dates_raw_dlt] {batchTaskId}, {sourceObjectId}, {destinationObjectId}, '{hvrCommitTimestampFrom}', '{hvrCommitTimestampTo}'"
    else:
        execute_query = f"exec [audit].[usp_log_availability_dates_raw_dlt] {batchTaskId}, {sourceObjectId}, {destinationObjectId}, '{outputDatetimeTo}', '{hvrCommitTimestampTo}'"
        
    cursor.execute(execute_query)
    logTaskProgress(cursor,batchTaskId,"Successfully Logged the raw_dlt entry in tblObjAvailability")
except Exception as e:
    errorMessage="Exception occured while inserting new datetime in tblObjectAvailability " + str(e)
    logToFile(errorLogFileLocation,errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Complete task and close connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
    taskEndAndCloseConn(cursor
                      ,conn
                      ,batchTaskId
                      ,batchTaskSourceRows
                      ,batchTaskRowsLoaded
                      ,batchTaskRejectRows
                      ,batchTaskResult
                      ,batchTaskResultLocation
                      ,adfPipelineName
                      ,clusterId
                      ,notebookName
                      ,errorLogFileLocation)
except:
    assert False