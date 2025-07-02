# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>dq_logOptimizationCheck_dbdt_gen2</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>check whether object is optimised or not</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2019/01/01</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC 
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Parameter Name</th>
# MAGIC     <th>Parameter Description</th>
# MAGIC     <th>Example Parameter</th>
# MAGIC   </tr>
# MAGIC   
# MAGIC    <tr>
# MAGIC     <td>sourceObjectName</td>
# MAGIC     <td>@sourceObjectName to retrieve source details </td>
# MAGIC     <td>@sourceObjectName = objectName</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>entityName</td>
# MAGIC     <td>@entityName to retrieve entity level details </td>
# MAGIC     <td>@entityName = entityName</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>testRunId</td>
# MAGIC     <td>@testRunId note book execution run id</td>
# MAGIC     <td>@testRunId</td>
# MAGIC   <tr>
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
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC   </tr>
# MAGIC   
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
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
  from datetime import datetime,timedelta
  from pyspark.sql.functions import lit,col,explode,concat_ws,create_map,struct,collect_list,coalesce,when
  from functools import reduce
  from itertools import chain 
  from collections import defaultdict
  import json
  import uuid
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
  createdDate = int(currentTs.strftime('%Y%m%d'))
  #get the hour
  createdHour = currentTs.hour
  CreatedTimestamp = currentTs
  LastUpdatedTimestamp = currentTs
  #use the same time for the date also
  date=currentTs
  
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
  
  targetLocation='/mnt/dataquality/logOptimizationCheck/'
  targetFormat='parquet'
#   testRunId=uuid.uuid4()
  
except Exception as e:
  errorMessage = "Exception occurred while variable initialisation :" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,remove widgets
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn=dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')
conn = pyodbc.connect(dbconn, autocommit = True)
cursor = conn.cursor()

# COMMAND ----------

# DBTITLE 1,SourceList
sourceDetails=pd.read_sql_query("select distinct source_name from config.tbl_source",conn).fillna('')
# sourceDetails

# COMMAND ----------

# DBTITLE 1,Create SourceWidget
dbutils.widgets.dropdown("1.sourceName", "Intrali4444", [str(x) for x in sourceDetails['source_name']])

# for x in selectedSource:
#   print(x)

# COMMAND ----------

# DBTITLE 1,Get SelectedSource
selectedSource = dbutils.widgets.get("1.sourceName")
# selectedSourceList="'"+selectedSource+"'"

# COMMAND ----------

# DBTITLE 1,Get ObjectDetails
objectDetails = pd.read_sql_query("exec [dq].[usp_get_sourceEntityObjectDetails] {}".format(selectedSource),conn)

# COMMAND ----------

# DBTITLE 1,Create Entity Widget
dbutils.widgets.multiselect("2.Entitiy", 'ALL', [str(x) for x in (objectDetails['entity_name'].append(pd.Series(['ALL']))).unique()])

# COMMAND ----------

# DBTITLE 1,Get Entity List
EntityList=dbutils.widgets.get('2.Entitiy')

# COMMAND ----------

# DBTITLE 1,TestRunID Widget
 #GET adfPipelineName  FROM WIDGETS
dbutils.widgets.text("3.TestRunId","")
testRunId  = dbutils.widgets.get("3.TestRunId")

# COMMAND ----------

# DBTITLE 1,Create spark Dataframe
# creaste spark dataframe 
objectDetails = spark.createDataFrame(objectDetails)
# display(objectDetails)

# COMMAND ----------

# DBTITLE 1,Read log optimise table
# get the optimise details from log optimise table based recent optimisation of the object
sourceDetails = pd.read_sql_query("select log_optimise_id, object_id, partition_date_optimised_to, partition_hour_optimised_to, optimise_state, optimise_start_datetime, optimise_end_datetime FROM (select log_optimise_id,  object_id, partition_date_optimised_to, partition_hour_optimised_to, optimise_state, optimise_start_datetime, optimise_end_datetime, RANK() OVER   (PARTITION BY object_id order by optimise_start_datetime desc ) as rank from audit.tbl_log_optimise) ab WHERE rank = 1",conn).fillna(0)
dataExists = False
if (sourceDetails.empty == False):
    # create spark dataframe
    sourceDetails = spark.createDataFrame(sourceDetails)
    dataExists = True
    # display(sourceDetails)

# COMMAND ----------

# DBTITLE 1,Validate optimise checks
if (dataExists == True):
    try:
      testScenario = 'logOptimisationCheck'
      for entity in EntityList.split(','):
        if entity=='ALL':
          filteredObjectDetails = objectDetails
        else:  
          filteredObjectDetails = objectDetails.filter(objectDetails["entity_name"]==entity)
      #     get the object details as rdd and assign to variables
        for row in filteredObjectDetails.select('source_name','entity_name','zone','object_name','object_attribute_name').distinct().rdd.collect():
          source_name = row['source_name']
          entity = row['entity_name']
          zone = row['zone']
          objectName = row['object_name']
      #     joining dataframes to get optimization details
          optimiseDetails =  filteredObjectDetails.join(sourceDetails, objectDetails.object_id == sourceDetails.object_id, how = 'left').select('object_name','source_name','entity_name','zone','optimise_state','partition_date_optimised_to','partition_hour_optimised_to','optimise_start_datetime','optimise_end_datetime')
      #   validate optimise state is completed or not
          optimiseState = optimiseDetails.withColumn("TestResult",when(optimiseDetails['optimise_state'] =='completed', 'Passed').otherwise('Failed'))
      #   validate partition date optimised and partition hour optimise is null or not
          optimiseDateHour = optimiseState.withColumn("TestResult", when(optimiseDetails['partition_date_optimised_to'] == 0, 'Failed').otherwise('Passed'))
      #   add the log columns to dataframe
      FinalDataFrame = optimiseDateHour.withColumn('attributeToCheck',lit('N/A')).withColumn('TestScenario',lit(testScenario)).withColumn('TestRunId',lit(testRunId)).withColumn('TestRunDate',lit(createdDate)).withColumn('TestRunHour',lit(createdHour)).drop_duplicates()
      # result dataframe
      FinalDataFrame = FinalDataFrame.select('source_name',col('entity_name').alias('entity'),col('zone').alias('Zone')
      ,col('object_name').alias('ObjectName'),'optimise_state','partition_date_optimised_to','partition_hour_optimised_to'                                     ,'optimise_start_datetime','optimise_end_datetime','TestScenario','attributeToCheck','TestRunId','TestResult','TestRunDate','TestRunHour')
    except Exception as e:
      errorMessage = "Exception occurred while checking whether object is optimised or not:" + str(e)
      assert False

# COMMAND ----------

# DBTITLE 1,Helper function
# Final output dataframe structure
def createSparkDataFrame(ResultSet):
  try:
    ResultDf=spark.createDataFrame(ResultSet,StructType([StructField('TestRunDate',StringType(),True)
                                                         ,StructField('TestRunHour',StringType(),True)
                                                         ,StructField('TestRunId',StringType(),True)
                                                         ,StructField('TestResult',StringType(),True)
                                                         ,StructField('source_name',StringType(),True)
                                                         ,StructField('entity_name',StringType(),True)
                                                         ,StructField('entity',StringType(),True)
                                                         ,StructField('Zone',StringType(),True)
                                                         ,StructField('ObjectName',StringType(),True)
                                                         ,StructField('optimise_state',StringType(),True)
                                                         ,StructField('partition_date_optimised_to',StringType(),True)
                                                         ,StructField('optimise_start_datetime',StringType(),True)
                                                         ,StructField('optimise_end_datetime',StringType(),True)
                                                         ,StructField('TestScenario',StringType(),True)
                                                         ,StructField('attributeToCheck',StringType(),True)
                                                    ]))
    return ResultDf
  except Exception as e:
    error_message = "Exception occurred while createSparkDataFrame :" + str(e)
    assert False

# COMMAND ----------

# DBTITLE 1,Write DataFrame
if (dataExists == False):
    resutSet=[]
    FinalDataFrame= createSparkDataFrame(resutSet)
    
(FinalDataFrame
     .write
     .partitionBy('TestRunDate','TestRunHour','source_name')
     .mode("append")
     .format(targetFormat)
     .save(targetLocation))

# COMMAND ----------

# DBTITLE 1,Read the output file
# logOptimisation = spark.read.format('parquet').load('/mnt/dataquality/logOptimizationCheck/').fillna('N/A')
# logOptimisation.select('source_name','entity','zone','objectName','optimise_state','partition_date_optimised_to','partition_hour_optimised_to','optimise_start_datetime','optimise_end_datetime','TestScenario','attributeToCheck','TestRunDate','TestRunHour','TestRunId').where("TestRunId = 'fcfbfea9-46ad-4cf2-9ef1-1a8c4771b9c8'").where("TestResult = 'Failed'").display()

# COMMAND ----------

# DBTITLE 1,complete task and close connection
conn.close()