# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>dq_ObjectRowCountCheck_dbdt_gen2</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>object row count check</td></tr>
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
# MAGIC   </tr>
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
  
  targetLocation='/mnt/dataquality/objectRowCountCheck/'
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

# DBTITLE 1,Test case for object row count check
# creaste spark dataframe 
objectDetails = spark.createDataFrame(objectDetails)
# display(objectDetails)

# COMMAND ----------

# DBTITLE 1,Store the valuse in list
try:
  objectRowCountList = []
  for entity in EntityList.split(','):
    if entity=='ALL':
      filteredObjectDetails = objectDetails
    else:  
      filteredObjectDetails = objectDetails.filter(objectDetails["entity_name"]==entity)
  #     fetching object details as rdd
    for row in filteredObjectDetails.select('source_name','entity_name','zone','object_name').distinct().rdd.collect():
  #     assigning object details to variables
      sourceName = row['source_name']
      entityName = row['entity_name']
      zone = row['zone']
      objectName = row['object_name']
  #     getting object count
      rowCount = spark.read.table(objectName).count()
  #   fetching data to list
      objectRowCountList.append(["rowCountCheck",sourceName,entityName,zone,objectName,rowCount])
except Exception as e:
  errorMessage = "Exception occurred while fetching object details and row count as list:" + str(e)
  assert False 

# COMMAND ----------

# DBTITLE 1,Create spark data frame to final result
try:
  # define schema
  schema = StructType([
    StructField('TestScenario', StringType(), True),
    StructField('source_name', StringType(), True),
    StructField('entity', StringType(), True),
    StructField('Zone', StringType(), True),
    StructField('ObjectName', StringType(), True),
    StructField('rowCount', StringType(), True)
  ])
  # Convert list to RDD
  rowCount = sc.parallelize(objectRowCountList)
  # Create data frame
  objectRowCnt = spark.createDataFrame(rowCount,schema)
  # validate the result and add log columns
  FinalDataFrame = objectRowCnt.withColumn('TestResult', when(objectRowCnt['rowCount'] == 0 , 'Failed')              .otherwise('Passed')).withColumn('TestRunDate',lit(createdDate)).withColumn('TestRunHour',lit(createdHour)).withColumn('TestRunId',lit(testRunId)).withColumn('attributeToCheck',lit('N/A')).drop_duplicates()
except Exception as e:
  errorMessage = "Exception occurred while fetching object retention details as list:" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Write DataFrame
# write result into target location
(FinalDataFrame
     .write
     .partitionBy('TestRunDate','TestRunHour','source_name')
     .mode("append")
     .format(targetFormat)
     .save(targetLocation))

# COMMAND ----------

# DBTITLE 1,Read the output file
# objCnt = spark.read.format(targetFormat).load('/mnt/dataquality/objectRowCountCheck/').fillna('N/A')
# objCnt.select('source_name','entity','ObjectName','TestScenario','attributeToCheck','TestRunId','TestRunDate','Zone','TestResult').where("TestRunId = 'ddfdb76d-1c75-4276-9459-325dc435e5aa'").display()


# COMMAND ----------

# DBTITLE 1,complete task and close connection
conn.close()