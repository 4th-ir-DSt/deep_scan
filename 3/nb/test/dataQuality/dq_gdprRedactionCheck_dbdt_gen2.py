# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>dq_gdprRedactionCheck_dbdt_gen2</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>validate the Piihash and Piicolumns</td></tr>
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
# MAGIC     <td>@DestinationObject to retrieve source details</td>
# MAGIC     <td>@DestinationObject = objectName</td>
# MAGIC   </tr>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>testRunId</td>
# MAGIC     <td>@testRunId note book execution run id</td>
# MAGIC     <td>@testRunId</td>
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
  from pyspark.sql.functions import lit,col,explode,concat_ws,create_map,struct,collect_list,coalesce
  from functools import reduce
  from itertools import chain 
  from collections import defaultdict
  import json
  dbutils.widgets.removeAll()
except Exception as e:
  errorMessage="Exception occurred while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn=dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')
conn = pyodbc.connect(dbconn, autocommit = True)
cursor = conn.cursor()

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
  targetLocation='/mnt/dataquality/gdprRedactionCheck'
  targetFormat='parquet'
except Exception as e:
  error_message = "Exception occurred while variable initialisation :" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,SourceList
sourceDetails=pd.read_sql_query("select distinct source_name from config.tbl_source",conn).fillna('')
# sourceDetails

# COMMAND ----------

# DBTITLE 1,Create SourceWidget
dbutils.widgets.dropdown("1.sourceName", "Intrali4444", [str(x) for x in sourceDetails['source_name']])

# COMMAND ----------

# DBTITLE 1,Create SourceWidget
selectedSourceList=dbutils.widgets.get('1.sourceName')

# COMMAND ----------

# DBTITLE 1,Get ObjectDetails
objectDetails=pd.read_sql_query("exec [dq].[usp_get_sourceDestinationGdprredactionObjects] {}".format(selectedSourceList),conn)
dbutils.widgets.multiselect("2.Entitiy", 'ALL', [str(x) for x in (objectDetails['entity_name'].append(pd.Series(['ALL']))).unique()])
EntityList=dbutils.widgets.get('2.Entitiy')
# objectDetails

# COMMAND ----------

# DBTITLE 1,TestRunId Widget
 #GET adfPipelineName  FROM WIDGETS
dbutils.widgets.text("3.TestRunId","")
testRunId  = dbutils.widgets.get("3.TestRunId")

# COMMAND ----------

# DBTITLE 1,Mapping object 
# getting the object to redaction state table mapping details with pii_columns
def objectmappingDetailsFun(destinationObject):
  try:
      global objectmappingDetails
      objectmappingDetails=pd.read_sql_query("exec [dq].[usp_get_objectmappingDetailsGdprRedaction] {}".format(destinationObject),conn)
      return objectmappingDetails
  except Exception as e:
        error_message = f"Exception occurred while objectmappingDetails:{destinationObject}" + str(e)
        assert False

# COMMAND ----------

# DBTITLE 1,Pii_hash and Redacted column 
# Getting pii hash values redacted and redacted pii columns details
def piiHashRedacted(objectmappingDetails):
  try:
    objectName=objectmappingDetails["object_name"].iloc[0]
    refObjectName=objectmappingDetails["reference_table_name"].iloc[0]
    objectAttributeName=objectmappingDetails["object_attribute_name"].iloc[0]
    referenceAttributeName=objectmappingDetails["reference_attribute_name"].iloc[0]
    piiColumnsList=objectmappingDetails['pii_columns'].tolist()
    piiColumnsListWhere=[]
    for col in piiColumnsList:
      index=piiColumnsList.index(col)
      if index==0:
        out=col+" !='REDACTED' "
      else:
        out="and "+col+" !='REDACTED' "
      piiColumnsListWhere.append(out)
    whereClause=' '.join(piiColumnsListWhere)
    stateTable_NullCount=sqlContext.sql(f''' select count(Processed) as count_1 from {refObjectName} 
where {referenceAttributeName} is  null and Processed=true ''').collect()[0].asDict()['count_1']
    piiHash_Count=sqlContext.sql(f''' select count(rob.{referenceAttributeName}) as count_1 from {objectName} sob 
left join {refObjectName} rob on
sob.{objectAttributeName}=rob.{referenceAttributeName}
where rob.Processed=true and sob.PiiHash!='-1' and sob.PiiTraceabilityHash!='-1'
and sob.PiiHashVersion!='-1' and {whereClause} ''').collect()[0].asDict()['count_1']
#     piiRedacted_Count=sqlContext.sql(f''' select count({objectAttributeName}) as count_1 from {objectName} where {whereClause} ''').collect()[0].asDict()['count_1']
    return piiHash_Count,stateTable_NullCount
  except Exception as e:
      error_message = f"Exception occurred while sourceDFDestinationDF:{destinationObject}" + str(e)
      assert False

# COMMAND ----------

# DBTITLE 1,Result data frame
# Final output dataframe structure
def createSparkDataFrame(ResultSet):
  try:
    ResultDf=spark.createDataFrame(ResultSet,StructType([StructField('TestScenario',StringType(),True)
                                                         ,StructField('TestResult',StringType(),True)
                                                         ,StructField('source_name',StringType(),True)
                                                         ,StructField('phase_name',StringType(),True)
                                                         ,StructField('zone',StringType(),True)
                                                         ,StructField('ObjectName',StringType(),True)
                                                         ,StructField('AttributeToCheck',StringType(),True)
                                                         ,StructField('TestcaseRunBy',StringType(),True)
                                                         ,StructField('entity',StringType(),True)
                                                    ]))
    return ResultDf
  except Exception as e:
    error_message = "Exception occurred while createSparkDataFrame :" + str(e)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to test gdpr redaction for piihash and redacted columns
def gdprRedactionCheck():
  try:
    global resultSet,user
    resultSet=[]
    user=spark.sql("select current_user() as user").collect()[0]["user"]
    objectDetails_new=objectDetails[objectDetails['zone']=='staging']
    for entity in EntityList.split(','):
       if entity=='ALL':
         filteredObjectDetails=objectDetails_new
       else:  
          filteredObjectDetails=objectDetails_new[objectDetails_new['entity_name']==entity]
       destinationObject=filteredObjectDetails.filter(items=['object_name'])
       destinationObject=destinationObject['object_name'].tolist()
       for object_name in destinationObject:
            objectmapping_Details=objectmappingDetailsFun(object_name)
            piiHashCount,stateTableNullCount=piiHashRedacted(objectmapping_Details)
            filteredObjectDetails=objectDetails[objectDetails['object_name']==object_name]
            records = filteredObjectDetails.to_records(index=False)
            recordlist = list(records)
            for object_Details in recordlist:
              source_name=object_Details[0]
              entity_name=object_Details[1]
              phase_name=object_Details[2]
              zone=object_Details[3]              
              if piiHashCount<=0 and stateTableNullCount<=0:
                 result='Success'
              else:
                 result='Failed'
              resultSet.append(("gdprRedactionCheck",result,source_name,phase_name,zone,object_name,'N/A',user,entity_name))
  except Exception as e:
       errorMessage=f"Exception occured while checking expression fun " + str(e)
       assert False

# COMMAND ----------

# DBTITLE 1,main function
def main_gdprRedactionCheck():
  try:
      gdprRedactionCheck()
      FinalDataFrame=createSparkDataFrame(resultSet)                                            
      FinalDataFrame=(FinalDataFrame
                      .withColumn('TestRunDate',lit(createdDate))
                      .withColumn('TestRunHour',lit(createdHour))
                      .withColumn('TestRunId',lit(testRunId)))
      FinalDataFrame.write.partitionBy('TestRunDate','TestRunHour','source_name').mode("append").format(targetFormat).save(targetLocation)
      conn.close()
#       print("results saved")
  except:
    pass

# COMMAND ----------

# DBTITLE 1,calling main function
main_gdprRedactionCheck()