# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>dq_ExpressionCheck_dbdt_gen2</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>expression values checked for selected destination source attribute</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>FrameworkM</td></tr>
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
  #dbutils.library.installPyPI("great_expectations")
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
  targetLocation='/mnt/dataquality/expressionChecks'
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
objectDetails=pd.read_sql_query("exec [dq].[usp_get_sourceDestinationExpressionObjects] {}".format(selectedSourceList),conn)
dbutils.widgets.multiselect("2.Entitiy", 'ALL', [str(x) for x in (objectDetails['entity_name'].append(pd.Series(['ALL']))).unique()])
EntityList=dbutils.widgets.get('2.Entitiy')
# objectDetails

# COMMAND ----------

# DBTITLE 1,TestRunId Widget
 #GET adfPipelineName  FROM WIDGETS
dbutils.widgets.text("3.TestRunId","")
testRunId  = dbutils.widgets.get("3.TestRunId")

# COMMAND ----------

# DBTITLE 1,Source and destination object
# get the source object name 
def sourceDestinationObject(destinationObject):
  try:
      sourceObject=pd.read_sql_query(f'''select distinct so.object_name from config.vw_objectDefinition_to_objectDefinition_map om 
                                        left join config.tbl_object so on so.object_id=om.source_object_id 
                                        left join config.tbl_object do on do.object_id=om.destination_object_id
                                         where do.object_name='{destinationObject}' and om.source_object_id is not null''',conn)
      sourceObject=sourceObject.iloc[0,0]
      return sourceObject
  except Exception as e:
    error_message = f"Exception occurred while sourceDestinationObject :{destinationObject}" + str(e)
    assert False

# COMMAND ----------

# DBTITLE 1,Mapping object details
# getting the object to object mapping details for all expression checks
def objectmappingDetailsFun(destinationObject):
  try:
      objectmappingDetails=pd.read_sql_query("exec [dq].[usp_get_objectmappingDetailsExpression] {}".format(destinationObject),conn)
      destination_attribute_list=objectmappingDetails['destination_object_attribute_name'].tolist()
      return objectmappingDetails
  except Exception as e:
        error_message = f"Exception occurred while objectmappingDetails:{destinationObject}" + str(e)
        assert False

# COMMAND ----------

# DBTITLE 1,Source and destination object data frame
# Getting columns from source and destination table
def sourceDFDestinationDF(destinationObject,objectmappingDetails,sourceObject):
  try:
    expressionList=objectmappingDetails["destination_object_attribute_default_value_new"].tolist()
    columnList=objectmappingDetails["destination_object_attribute_name"].tolist()
    whereClause=objectmappingDetails["where_clause"].tolist()[0]
    selectString=','.join(expressionList)
    source_Df=sqlContext.sql(f''' select {selectString} from {sourceObject} where {whereClause}''')
    sourceColumns=','.join(list(source_Df.columns))
    destination_Df=sqlContext.sql(f'''select {sourceColumns} from {destinationObject}''')
    return source_Df,destination_Df
  except Exception as e:
      error_message = f"Exception occurred while sourceDFDestinationDF:{destinationObject}" + str(e)
      assert False

# COMMAND ----------

# DBTITLE 1,Result data frame
# Final output dataframe structure
def createSparkDataFrame(ResultSet):
  try:
    ResultDf=spark.createDataFrame(ResultSet,StructType([StructField('TestScenario',StringType(),True)
                                                        ,StructField('TestResult',StringType(),True)                                                                                                   ,StructField('source_name',StringType(),True)
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

# DBTITLE 1,Function to test expression values
def expressionCheck():
  try:
    global resultSet,user
    resultSet=[]
    user=spark.sql("select current_user() as user").collect()[0]["user"]
#     objectDetails=objectDetails[objectDetails['zone']=='staging']
    for entity in EntityList.split(','):
       if entity=='ALL':
         filteredObjectDetails=objectDetails
       else:  
          filteredObjectDetails=objectDetails[objectDetails['entity_name']==entity]
       destinationObject=filteredObjectDetails.filter(items=['object_name'])
       destinationObject=destinationObject['object_name'].tolist()
       for object_name in destinationObject:
            source_Object=sourceDestinationObject(object_name)
            objectmapping_Details=objectmappingDetailsFun(object_name)
            sourceDf,destinationDf=sourceDFDestinationDF(object_name,objectmapping_Details,source_Object)
            sourceDf.registerTempTable("sourceDfTable")
            destinationDf.registerTempTable("destinationDfTable")
            columnList=objectmapping_Details["destination_object_attribute_name"].tolist()
            filteredObjectDetails=objectDetails[objectDetails['object_name']==object_name]
            records = filteredObjectDetails.to_records(index=False)
            recordlist = list(records)
            for object_Details in recordlist:
              source_name=object_Details[0]
              entity_name=object_Details[1]
              phase_name=object_Details[2]
              zone=object_Details[3]
              for columnName in columnList:
                  source_max_value=spark.sql(f"SELECT MAX({columnName}) as maxval FROM sourceDfTable").collect()[0].asDict()['maxval']
                  source_min_value=spark.sql(f"SELECT MIN({columnName}) as minval FROM sourceDfTable").collect()[0].asDict()['minval']
                  source_null_value=spark.sql(f"SELECT count({columnName}) as nullcount FROM sourceDfTable where {columnName} is NULL").collect()[0].asDict()['nullcount']
                  des_max_value=spark.sql(f"SELECT MAX({columnName}) as maxval FROM destinationDfTable").collect()[0].asDict()['maxval']
                  des_min_value=spark.sql(f"SELECT MIN({columnName}) as minval FROM destinationDfTable").collect()[0].asDict()['minval']
                  des_null_value=spark.sql(f"SELECT count({columnName}) as nullcount FROM destinationDfTable where {columnName} is NULL").collect()[0].asDict()['nullcount']
                  if source_max_value==des_max_value and source_min_value==des_min_value and source_null_value==des_null_value:
                     result='Success'
                  else:
                     result='Failed'
                  resultSet.append(("expressionCheck",result,source_name,phase_name,zone,object_name,columnName,user,entity_name))
  except Exception as e:
       errorMessage=f"Exception occured while checking expression fun:{object_name} " + str(e)
       assert False

# COMMAND ----------

# DBTITLE 1,main function
def main_expressionCheck():
  try:
      expressionCheck()
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
main_expressionCheck()