# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>dq_gdprRetentionCheck_dbdt_gen2</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>gdpr retention check</td></tr>
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
  
  targetLocation='/mnt/dataquality/gdprRetentionCheck/'
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
objectDetails = pd.read_sql_query("exec [dq].[usp_get_sourceDestinationGdprretentionObjects] {}".format(selectedSource),conn)

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

print(objectDetails.count())

# COMMAND ----------

# DBTITLE 1,Test case for gdpr retention check
# create spark dataframe 
if (objectDetails.empty == False):
    objectDetailsDf = spark.createDataFrame(objectDetails)
    #display(objectDetails)

# COMMAND ----------

# DBTITLE 1,Store the values in list
if (objectDetails.empty == False):
    try:
      # create empty list
      gdprretentionList = []
      # get the object details of person zone
      personObjectDetails = objectDetailsDf.filter(objectDetailsDf["zone"]=='person')
      # get object details of staging zone
      stageObjectDetails = objectDetailsDf.filter(objectDetailsDf["zone"]=='staging')
      for entity in EntityList.split(','):
        if entity=='ALL':
          filteredObjectDetails = stageObjectDetails
        else:  
          filteredObjectDetails = stageObjectDetails.filter(objectDetailsDf["entity_name"]==entity)
        for row in filteredObjectDetails.select('source_name','entity_name','zone','object_name').distinct().rdd.collect():
          source_name = row['source_name']
          entity = row['entity_name']
          zone = row['zone']
          objectName = row['object_name']
      #     get the stage table and reference table with key columns
          objectReferenceDetails = pd.read_sql_query("exec [dq].[usp_get_objectmappingDetailsGdprRetention] {}".format(objectName),conn).fillna('NA')
          if len(objectReferenceDetails)!=0:
      #       create spark dataframe
            objectReferenceDetails = spark.createDataFrame(objectReferenceDetails)
            for referenceDetails in objectReferenceDetails.select('object_name','object_attribute_name','reference_table_name','reference_attribute_name').distinct().rdd.collect():
      #       store the keys and referenece table name in variable
              stageObjectName = referenceDetails['object_name']
              stageAttributeName = referenceDetails['object_attribute_name']
              referenceObjectName = referenceDetails['reference_table_name']
              referenceAttributeName = referenceDetails['reference_attribute_name']
              gdprRetentionCount = spark.read.table(referenceObjectName).where(" Processed='true' and " + referenceAttributeName + " is null")
              if gdprRetentionCount.count() == 0:
                gdprRetentionCount = spark.sql("SELECT count( " + stageAttributeName + " ) as keyColumnCount FROM " + stageObjectName + " sob left join " + referenceObjectName + " rob on " + " sob." + stageAttributeName + " = rob." + referenceAttributeName + " where  rob.Processed=true and sob.PiiHash!='-1' and sob.PiiTraceabilityHash!='-1' and sob.PiiHashVersion!='-1'").collect()[0].asDict()['keyColumnCount']
                for personObjName in personObjectDetails.select('object_name').distinct().rdd.collect():
                  personObjectName = personObjName['object_name']
          #   person table retention count based on match piihash with stage table
                  personObjectRetentionCount = spark.sql("SELECT COUNT(PiiHash) FROM " + personObjectName + " WHERE PiiHash IN (SELECT PiiHash FROM " + stageObjectName + " where " + stageAttributeName + " in (SELECT " + referenceAttributeName + " FROM " + referenceObjectName + " where Processed = 'true'))").collect()[0].asDict()['count(PiiHash)']
      #   append values into list
                  gdprretentionList.append(["gdprRetentionCheck", source_name,entity,zone,stageObjectName, stageAttributeName,referenceObjectName, referenceAttributeName,personObjectName, gdprRetentionCount,personObjectRetentionCount])  
              else:
                gdprretentionList.append(["gdprRetentionCheck", source_name,entity,zone,stageObjectName, stageAttributeName,referenceObjectName, referenceAttributeName,'N/A', 'failed','failed'])   
    except Exception as e:
      errorMessage = "Exception occurred while get piihash from person table and append to list:" + str(e)
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
    StructField('referenceObjectName', StringType(), True),
    StructField('referenceAttributeName', StringType(), True),
    StructField('personObjectName', StringType(), True),
    StructField('stageTableRetentionCount', StringType(), True),
    StructField('personTableRetentionCount', StringType(), True)
  ])
  if (objectDetails.empty == False):  
    gdprRetentionDetails = sc.parallelize(gdprretentionList)
    # Create data frame
    gdprRetentionDetails = spark.createDataFrame(gdprRetentionDetails,schema)
    # validate the test result
    gdprRetentionDetails = gdprRetentionDetails.withColumn("TestResult", when(gdprRetentionDetails['stageTableRetentionCount'] == 0, 'Passed').when(gdprRetentionDetails['personTableRetentionCount'] == 0, 'Passed')
                                                           .otherwise('Failed'))
  else:
    gdprRetentionDetails = spark.createDataFrame(objectDetails,schema)
  # add log columns to final dataframe
  FinalDataFrame = gdprRetentionDetails.withColumn('TestRunId',lit(testRunId)).withColumn('TestRunDate',lit(createdDate)).withColumn('TestRunHour',lit(createdHour))
except Exception as e:
  errorMessage = "Exception occurred while validating test result:" + str(e)
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

# DBTITLE 1,Test case for check active PII keys
if (objectDetails.empty == False):
  try:
    activePiiList = []
    # get the object details of person zone
    personObjectDetails = objectDetailsDf.filter(objectDetailsDf["zone"]=='person')
    # get object details of staging zone
    stageObjectDetails = objectDetailsDf.filter(objectDetailsDf["zone"]=='staging')
    for entity in EntityList.split(','):
      if entity=='ALL':
        filteredObjectDetails = stageObjectDetails
      else:  
        filteredObjectDetails = stageObjectDetails.filter(objectDetailsDf["entity_name"]==entity)
      for row in filteredObjectDetails.select('source_name','entity_name','zone','object_name').distinct().rdd.collect():
        source_name = row['source_name']
        entity = row['entity_name']
        zone = row['zone']
        sourceObjectName = row['object_name']
    #     get the stage table and redaction state tables table with key columns
        objectReferenceDetails = pd.read_sql_query("exec [dq].[usp_get_objectmappingDetailsGdprRetention] {}".format(sourceObjectName),conn).fillna('NA')
        if len(objectReferenceDetails)!=0:
          objectReferenceDetails = spark.createDataFrame(objectReferenceDetails)
          for referenceDetails in objectReferenceDetails.select('reference_table_name').distinct().rdd.collect():
    #       get the values into variable
            objectName = referenceDetails['reference_table_name']
    #     get the reference tables details
            referenceTablesPD = pd.read_sql_query("exec [dq].[usp_get_gdpr_retention_reference_tables] {}".format(objectName),conn)
    #   create schema
            schema = StructType([StructField("object_name"          , StringType() , True)
                          ,StructField("object_attribute_name", StringType() , True)
                          ,StructField("where_clause"         , StringType() , True)
                          ,StructField("reference_object_name", StringType() , True)
                       ])
            referenceTablesDF = spark.createDataFrame(referenceTablesPD, schema)
            referenceTablesDic = referenceTablesDF.collect()[0].asDict()
            refTable =  referenceTablesDic['reference_object_name']
            refColumn = referenceTablesDic['object_attribute_name']
            refWhereClause =  referenceTablesDic['where_clause']
            refTableProcessed = referenceTablesDic['object_name']
    #           get the source id
            sourcesPd = pd.read_sql_query("exec [config].[usp_gdpr_get_sources] {}".format(refTableProcessed), conn)
            schema = StructType([StructField("SourceID", IntegerType() , True)])
            sourcesDF = spark.createDataFrame(sourcesPd, schema)
    #       get the expired keys to check whether these keys are active or not in redaction state table
            expiredKeys = (spark.sql("SELECT /*+ BROADCAST({}) */ r.*, CAST(NULL AS Timestamp) AS RedactedTimestamp, false AS Processed FROM {} r LEFT JOIN {} rp ON r.{}=rp.{} WHERE rp.{} IS NULL AND {}".format(refTable, refTable, refTableProcessed, refColumn, refColumn, refColumn, refWhereClause)))

    #         Add the SourceID
            expiredKeys = expiredKeys.crossJoin(sourcesDF)
  #     get the active key vales whichever rows stated as processed = 'false'
            activeKeys = spark.sql("select * from " + refTableProcessed + " where Processed= 'false' ")
  #   get the active keys from redaction state table whichever keys are expired in reference table
            activeKey = activeKeys.join(expiredKeys)
  #   get the activey key count
            keyCount = activeKey.count()
  #    if active key count !=0 then need to check in stage tables whether piihash values are acive or not if piihash values are active then  no need to do any action else  have check why piihash values are inactive(-1) in stage table
  # if active key count = 0 no need to look in staging tables then it will log into list with input values and count as 0 
            if keyCount != 0:
  #     get the stage tables which are mapped with redaction state tables
              sourceref = pd.read_sql_query("exec [dq].[usp_get_GdprRetentionObjects] {}".format(refTableProcessed), conn)
  #   convert spark datafram
              sourceref = spark.createDataFrame(sourceref)
  #   make it as list and values to variables
              for referenceDetails in sourceref.select('object_name','object_attribute_name','reference_attribute_name').distinct().rdd.collect():
                stageObjectName = referenceDetails['object_name']
                stageAttributeName = referenceDetails['object_attribute_name']
                referenceAttributeName = referenceDetails['reference_attribute_name']
  #           get the stage table data
                getStageKeys = spark.sql("select * from " + stageObjectName)
  #   join stage table with activekeys dataframe to get  piihash values in stage table
                activePiiKeys = getStageKeys.join(activeKey,  activeKey[refColumn] == getStageKeys[stageAttributeName ])
  #   get the inactive piihash count expecting 0 if count !=0 means need check why piihash values are inactive for active keys
                activePiiKeyCount = activePiiKeys.where("PiiHash' == '-1'").count()
  #   append all inputs and result to list
                activePiiList.append(["activePIICheck",source_name,entity,zone, stageObjectName, stageAttributeName, refTableProcessed, referenceAttributeName,'N/A', activePiiKeyCount,'N/A'])
            else:
  #       if active key count = 0 then won't check the stage tables directly logged into list with input values and count = 0
              activePiiList.append(["activePIICheck",source_name,entity,zone,refTableProcessed,refColumn,'N/A','N/A','N/A','0','N/A'])
  except Exception as e:
    errorMessage = "Exception occurred while get active pii keys and append to list:" + str(e)
    assert False

# COMMAND ----------

# DBTITLE 1,Create spark data frame to final result
if (objectDetails.empty == False):
  try:
    schema = StructType([
      StructField('TestScenario', StringType(), True),
      StructField('source_name', StringType(), True),
      StructField('entity', StringType(), True),
      StructField('Zone', StringType(), True),
      StructField('ObjectName', StringType(), True),
      StructField('attributeToCheck', StringType(), True),
      StructField('referenceObjectName', StringType(), True),
      StructField('referenceAttributeName', StringType(), True),
      StructField('personObjectName', StringType(), True),
      StructField('stageTableRetentionCount', StringType(), True),
      StructField('personTableRetentionCount', StringType(), True)
    ])
    activePiiList = sc.parallelize(activePiiList)
    # Create data frame
    activePiiList = spark.createDataFrame(activePiiList,schema)
    # validate the test result
    activePiiList = activePiiList.withColumn("TestResult", when(activePiiList['stageTableRetentionCount'] == 0, 'Passed').otherwise('Failed'))
    # add log columns to final dataframe
    FinalDataFrame = activePiiList.withColumn('TestRunId',lit(testRunId)).withColumn('TestRunDate',lit(createdDate)).withColumn('TestRunHour',lit(createdHour))
  except Exception as e:
    errorMessage = "Exception occurred while validating test result:" + str(e)
    assert False

# COMMAND ----------

# DBTITLE 1,Write DataFrame
if (objectDetails.empty == False):
  (FinalDataFrame
       .write
       .partitionBy('TestRunDate','TestRunHour','source_name')
       .mode("append")
       .format(targetFormat)
       .save(targetLocation))

# COMMAND ----------

# DBTITLE 1,Read the output file
#print(testRunId)
#gdprRetention = spark.read.format('parquet').load('/mnt/dataquality/gdprRetentionCheck/')
#gdprRetention.select('source_name','entity','ObjectName','TestScenario','attributeToCheck','TestRunId','TestRunDate','Zone','TestResult','TestRunId').where("TestRunId = '{}'").where("TestResult = 'Failed'").format(testRunId).display()


# COMMAND ----------

# DBTITLE 1,complete task and close connection
conn.close()