# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>dq_PreBatchChecks_dbdt_gen2</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Pre Batch checks for selected sources</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2019/01/01</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC 
# MAGIC ## Notebook Parameters
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Parameter Name</th>
# MAGIC     <th>Parameter Description</th>
# MAGIC     <th>Example Parameter</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>sourceName</td>
# MAGIC     <td>@sourceName to retrieve source details</td>
# MAGIC     <td>@sourceName = sourceName</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>EntityName</td>
# MAGIC     <td>@Entity to retrieve source details</td>
# MAGIC     <td>@Entity = entityName</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>TestRunId</td>
# MAGIC     <td>@TestRunId to retrieve test run details</td>
# MAGIC     <td>@TestRunId</td>
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

# DBTITLE 1,Install Expectations Library
#%sh
#pip install great_expectations

# COMMAND ----------

# DBTITLE 1,Import Modules
try:
  import pandas as pd
  import pyodbc
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DecimalType 
  from datetime import datetime,timedelta
  from pyspark.sql.functions import lit,col,explode,concat_ws,struct,collect_list,lower,split
  from functools import reduce
  from itertools import chain 
  import great_expectations as ge
  import json
except Exception as e:
  errorMessage="Exception occured while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialize variables
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
  createdTimestamp = currentTs
  lastUpdatedTimestamp = currentTs
  #use the same time for the date also
  date=currentTs
  
  #PARAMETER FOR LOG_ERROR
  errorLine = ''
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
  targetLocation='/mnt/dataquality/preBatchChecks'
  targetFormat='parquet'
  resultSet=[]
except Exception as e:
  error_message = "Exception occurred while variable initialisation :" + str(e)
  assert False

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Get SQL connection Details
try:
# import pandas as pd
  # import pyodbc
  dbconn=dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')
  conn = pyodbc.connect(dbconn, autocommit = True)
  cursor = conn.cursor()
except Exception as e:
  error_message = "Exception occurred while establishing dbconn :" + str(e)
  assert False


# COMMAND ----------

# DBTITLE 1,SourceList
try:
  sourceDetails=pd.read_sql_query("select distinct source_name from config.tbl_source",conn).fillna('')
except Exception as e:
  error_message = "Exception occurred while getting sourcelist :" + str(e)
  assert False
# sourceDetails

# COMMAND ----------

# DBTITLE 1,Create SourceWidget
dbutils.widgets.dropdown("1.sourceName", "Intrali4444", [str(x) for x in sourceDetails['source_name']])

# for x in selectedSource:
#   print(x)

# COMMAND ----------

# DBTITLE 1,Get SelectedSource
selectedSource=dbutils.widgets.get('1.sourceName')
# selectedSourceList="'"+selectedSource+"'"

# COMMAND ----------

# DBTITLE 1,Get Object Details
objectDetails=pd.read_sql_query("exec [dq].[usp_get_sourceEntityObjectsTaskDetails] {}".format(selectedSource),conn)
# objectDetailsDf=spark.createDataFrame(objectDetails)
# objectDetailsDf.display()
# objectDetails

# COMMAND ----------

# DBTITLE 1,Entity Widget
dbutils.widgets.multiselect("2.Entitiy", 'ALL', [str(x) for x in (objectDetails['entity_name'].append(pd.Series(['ALL']))).unique()])

# COMMAND ----------

# DBTITLE 1,get Entity Widget
EntityList=dbutils.widgets.get('2.Entitiy')

# COMMAND ----------

# DBTITLE 1,TestRunId Widget
 #GET adfPipelineName  FROM WIDGETS
dbutils.widgets.text("3.TestRunId","")
testRunId  = dbutils.widgets.get("3.TestRunId")

# COMMAND ----------

# DBTITLE 1,Test Case Functions
def ExternalTableCheck(object_name):

  try:
    Type=spark.sql("Describe Table  Extended {}".format(object_name)).select('data_type').where("col_name='Type'").collect()[0][0]
    if Type=='EXTERNAL':
      result='Passed'
    else:
      result='Failed'

    return result
  except Exception as e:
    error_message = "Exception occurred while ExternalTableCheck :" + str(e)
    result="Failed"
    return result
#   assert False

def TaskAssignedCheck(destinationTaskCount,MaintainanceCount):
  try:
    result='Failed'
    if destinationTaskCount>0 and MaintainanceCount>0:
      result ='Passed'
    else:
      result='Failed'
    return result
  except Exception as e:
    error_message = "Exception occurred while ExternalTableCheck :" + str(e)
    assert False

def CheckLocation(Location,Format):
  try:
    RowCount=spark.read.format(Format).load(Location).limit(1).count()
    if RowCount<1:
      Result='Failed'
    else :
      Result='Passed'
  except Exception as e:
    
    Result='Failed'
  return Result

def checkClusterAssigned(objectDetailsDf):
  try:
    NullCount=objectDetailsDf.where('databricks_cluster_id is null').count()
    if NullCount>0 :
      result='Failed'
    else:
      result='Passed'
    return result
  except Exception as e:
     error_message = "Exception occurred while checkClusterAssigned :" + str(e)
     assert False

# COMMAND ----------

# DBTITLE 1,Helper Functions
def checkDataFrameIsNotEmpty(padasdf):
  try:
    if padasdf.shape[0]>0:
      return True
    else:
      return False
  except Exception as e:
    error_message = "Exception occurred while checkDataFrameIsNotEmpty :" + str(e)
  assert False
  
  
def createSparkDataFrame(ResultSet):
  try:
#     TestCase,source_name,entity,zone,object_name,'N/A',result
    ResultDf=spark.createDataFrame(ResultSet,StructType([ StructField('TestScenario',StringType(),True)
                                                         ,StructField('source_name',StringType(),True)
                                                         ,StructField('entity',StringType(),True)
                                                         ,StructField('zone',StringType(),True)
                                                         ,StructField('ObjectName',StringType(),True)
                                                         ,StructField('AttributeToCheck',StringType(),True)
                                                         ,StructField('TestResult',StringType(),True)
                                                  
  #                                                   ,StructField('RowCounts',ArrayType(IntegerType()),True)
                                                    ]))
    return ResultDf
  except Exception as e:
    error_message = "Exception occurred while ApplyReferenceTest :" + str(e)
    assert False

# COMMAND ----------

# DBTITLE 1,Raw Source File Test Case
try:
  rawObjectDetails=pd.read_sql_query("exec [dq].[usp_get_RawsourceEntityObjects] {}".format(selectedSource),conn)
  CheckEmptyFlag=checkDataFrameIsNotEmpty(rawObjectDetails)

  # EntityList=dbutils.widgets.get('2.Entitiy')
  # print(CheckEmptyFlag)
  if  CheckEmptyFlag:
    records = rawObjectDetails.to_records(index=False)
    recordlist = list(records)
  #   print(recordlist)
    TestCase="RawSourceFilesCheck"
    for rawobjectDetails in recordlist:
      source_name=rawobjectDetails[0]
      phase_name=rawobjectDetails[3]
      zone=rawobjectDetails[4]
      entity=rawobjectDetails[1]
      object_name=rawobjectDetails[5]
      location=rawobjectDetails[6]
      format=rawobjectDetails[9]
      result=CheckLocation(location,format)
      resultSet.append((TestCase,source_name,entity,zone,object_name,'N/A',result))
except Exception as e:
  error_message = "Exception occurred while Raw Source File Test Case :" + str(e)
  assert False


# COMMAND ----------

# DBTITLE 1,Cluster Assigned Check
try:
  TestCase="ClusterAssignedCheck"
  clustersourceDetails=pd.read_sql_query("exec [dq].[usp_get_Source_Schedule_Cluster] {}".format(selectedSource),conn)
  CheckEmptyFlag=checkDataFrameIsNotEmpty(clustersourceDetails)
  # print(CheckEmptyFlag)
  if  CheckEmptyFlag:
    objectDetailsDf=spark.createDataFrame(clustersourceDetails)
    result=checkClusterAssigned(objectDetailsDf)
    resultSet.append((TestCase,selectedSource,'N/A','N/A','N/A','N/A',result))
except Exception as e:
  error_message = "Exception occurred while Cluster Assigned Check :" + str(e)
  assert False    


# COMMAND ----------

# DBTITLE 1,Object Level Test Cases
try:
  if EntityList.split(',')[0]=='ALL':
      filteredObjectDetails=objectDetails
  else:  

    filteredObjectDetails=objectDetails[objectDetails.entity_name.isin(EntityList.split(','))]
  records = filteredObjectDetails.to_records(index=False)
  TaskCheckTestcase="TasksAssignedCheck"
  ExternalCheckTestcase="ExternalTableCheck"
  recordlist = list(records)
  # print(recordlist)
  for object_Details in recordlist:
    source_name=object_Details[0]
    phase_name=object_Details[2]
    zone=object_Details[3]
    object_name=object_Details[4]
    entity_name=object_Details[1]
    destination_task_count=object_Details[7]
    maintainance_task_count=object_Details[8]
    resultTaskAssigned=TaskAssignedCheck(destination_task_count,maintainance_task_count)
#     print(object_name)
    resultExternalTableCheck=ExternalTableCheck(object_name)
#     print(object_name)
    resultSet.append((TaskCheckTestcase,source_name,entity_name,zone,object_name,'N/A',resultTaskAssigned))
    resultSet.append((ExternalCheckTestcase,source_name,entity_name,zone,object_name,'N/A',resultExternalTableCheck))
except Exception as e:
  error_message = "Exception occurred while Object Level Test Cases :" + str(e)
  assert False    
    


# COMMAND ----------

# DBTITLE 1,create Spark Dataframe
FinalDataFrame= createSparkDataFrame(resultSet) 

# COMMAND ----------

# DBTITLE 1,Add Log Columns
FinalDataFrame=FinalDataFrame.withColumn('TestRunDate',lit(createdDate)).withColumn('TestRunHour',lit(createdHour)).withColumn('TestRunId',lit(testRunId))

# COMMAND ----------

# DBTITLE 1,Write DataFrame
(FinalDataFrame
     .write
     .partitionBy('TestRunDate','TestRunHour','source_name')
     .mode("append")
     .format(targetFormat)
     .save(targetLocation))

# COMMAND ----------

# DBTITLE 1,Close Connection
conn.close()