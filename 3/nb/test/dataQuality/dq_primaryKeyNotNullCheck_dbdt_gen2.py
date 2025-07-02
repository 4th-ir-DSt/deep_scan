# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>dq_primaryKeyNotNullCheck_dbdt_gen2</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>primary key not null</td></tr>
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
# MAGIC     <td>@sourceObjectName = sourceObjectName</td>
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
  
  targetLocation='/mnt/dataquality/primaryNotNullCheck/'
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

# DBTITLE 1,Test case for primary Key Null check
# creaste spark dataframe 
objectDetails = spark.createDataFrame(objectDetails)
# display(objectDetails)

# COMMAND ----------

# DBTITLE 1,Store the valuse in list
try:
  # Get the primary key column null count save as list
  pkColumnNullCntList = []
  objectPKColumns = objectDetails.filter(objectDetails["object_attribute_name"].like ("%PK"))
  for entity in EntityList.split(','):
    if entity=='ALL':
      filteredObjectDetails = objectPKColumns
    else:  
      filteredObjectDetails = objectPKColumns.filter(objectPKColumns["entity_name"]==entity)
      #     fetching object details as rdd
    for row in filteredObjectDetails.select('source_name','entity_name','zone','object_name','object_attribute_name').distinct().rdd.collect():
      #     assigning object details to variables
      sourceName = row['source_name']
      entityName = row['entity_name']
      zone = row['zone']
      objectName = row['object_name']
      pkColumn = row['object_attribute_name']
  #     get the primary key null count
      pkColumnNullCount = spark.sql("SELECT count(*) FROM " + objectName + 
                                    " WHERE " + pkColumn + " IS NULL").collect()[0].asDict()['count(1)']
      #   fetching data to list
      pkColumnNullCntList.append(["primaryKeyNullValidation",sourceName,entityName,zone,objectName,pkColumn,pkColumnNullCount])
except Exception as e:
  errorMessage = "Exception occurred while fetching object primary key coulmn null count as list:" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Create spark data frame to final result
try:
  schema = StructType([
    StructField('TestScenario', StringType(), True),
    StructField('source_name', StringType(), True),
    StructField('entity', StringType(), True),
    StructField('Zone', StringType(), True),
    StructField('ObjectName', StringType(), True),
    StructField('attributeToCheck', StringType(), True),
    StructField('attributenNullCount', StringType(), True)
  ])
  objectPKDetails = sc.parallelize(pkColumnNullCntList)
  # Create data frame
  primaryKeyNullCheck = spark.createDataFrame(objectPKDetails,schema)
  # validate the data and add log columns
  FinalDataFrame = primaryKeyNullCheck.withColumn('TestResult', when(primaryKeyNullCheck['attributenNullCount'] == 0 , 'Passed')                    .otherwise('Failed')).withColumn('TestRunDate',lit(createdDate)).withColumn('TestRunHour',lit(createdHour)).withColumn('TestRunId',lit(testRunId))
except Exception as e:
  errorMessage = "Exception occurred while fetching object retention details as list:" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Write DataFrame
(FinalDataFrame
     .write
     .partitionBy('TestRunDate','TestRunHour','source_name')
     .mode("append")
     .format(targetFormat)
     .save(targetLocation))

# COMMAND ----------

# DBTITLE 1,Read the output file
# pkNullCheck = spark.read.format('parquet').load('/mnt/dataquality/primaryNotNullCheck/').fillna('N/A')
# pkNullCheck.select('TestRunId','source_name','entity','zone','objectName','attributeToCheck','attributenNullCount','TestScenario','TestResult','TestRunDate','TestRunHour').where("TestRunId = 'fcfbfea9-46ad-4cf2-9ef1-1a8c4771b9c8'").where("TestResult = 'Failed'").display()


# COMMAND ----------

# DBTITLE 1,complete task and close connection
conn.close()