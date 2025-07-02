# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>dq_ReferenceTableChecks_dbdt_gen2</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Reference\Currentstate table checks for selected sources</td></tr>
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
# MAGIC     <tr>
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
  targetLocation='/mnt/dataquality/ReferenceTableChecks'
  targetFormat='parquet'
except Exception as e:
  error_message = "Exception occurred while variable initialisation :" + str(e)
  assert False

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Get SQL connection Details
# import pandas as pd
# import pyodbc
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
selectedSource=dbutils.widgets.get('1.sourceName')
# selectedSourceList="'"+selectedSource+"'"

# COMMAND ----------

# DBTITLE 1,Get ObjectDetails
objectDetails=pd.read_sql_query("exec [dq].[usp_get_sourceEntityObjects] {}".format(selectedSource),conn)

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

# DBTITLE 1,Helper Functions For Reference Tests

#Update status if result count is 0 that means the table is empty 
def UpdateStatus(result):
  try:
#     print(type(result))
    result=result.to_json_dict()
#     result=json.loads(result)
    if 'element_count' in result['result'].keys():
      if result['result']['element_count']==0:
        result['success']=False
#         print(result['success'])
        
    return result
  except Exception as e:
    error_message = "Exception occurred while ApplyReferenceTest :" + str(e)
    assert False


def ApplyReferenceTest(object_name,DateField,primary_key,source_name,phase_name,zone,entity):
  try:
    
#     The logic is to read the table and convert any datefield to string as great expectations library expects the date fileds in string format
    DF=spark.read.table(object_name).withColumn(DateField,col(DateField).cast(StringType()))
    # DF.display()
    #Convert to great expectations dataset
    df_ge = ge.dataset.SparkDFDataset(DF)
    
    #Add in all the test cases
    result_notNull=df_ge.expect_column_values_to_not_be_null(primary_key)
    result_notNull=UpdateStatus(result_notNull)
    result_Unique=df_ge.expect_column_values_to_be_unique(primary_key)
    result_Unique=UpdateStatus(result_Unique)
    result_DateFormat=df_ge.expect_column_values_to_match_strftime_format(DateField,'%Y%m%d')
    result_DateFormat=UpdateStatus(result_DateFormat)
    result_rowCount=df_ge.expect_table_row_count_to_be_between(1)
    result_rowCount=UpdateStatus(result_rowCount)

    ResultSet=[]
    ResultSet.append(("NonNullCheck",object_name,primary_key,primary_key,result_notNull['result'],'Passed' if result_notNull['success'] else 'Failed',source_name,phase_name,zone,entity))
    ResultSet.append(("PKUniqueCheck",object_name,primary_key,primary_key,result_Unique['result'],'Passed' if result_Unique['success']  else 'Failed',source_name,phase_name,zone,entity))
    ResultSet.append(("DateFieldFormatCheck",object_name,primary_key,DateField,result_DateFormat['result'],'Passed' if result_DateFormat['success']  else 'Failed',source_name,phase_name,zone,entity))
    ResultSet.append(("rowCountCheck",object_name,primary_key,primary_key,result_rowCount['result'],'Passed' if result_rowCount['success']  else 'Failed',source_name,phase_name,zone,entity))
    return ResultSet
  except Exception as e:
          ResultSet.append(("NonNullCheck",object_name
                                        ,primary_key,primary_key
                                        ,NULL
                                        ,'Failed'
                                        ,source_name,phase_name,zone,entity))
          ResultSet.append(("PKUniqueCheck",object_name,primary_key,primary_key,NULL
                                        ,'Failed',source_name,phase_name,zone,entity))
          ResultSet.append(("DateFieldFormatCheck",object_name,primary_key,DateField,NULL
                                        ,'Failed',NULL
                                        ,'Failed',source_name,phase_name,zone,entity))
          ResultSet.append(("rowCountCheck",object_name,primary_key,primary_key,NULL
                                        ,'Failed',source_name,phase_name,zone,entity))
          return ResultSet
#     assert False

def createSparkDataFrame(ResultSet):
  try:
    ResultDf=spark.createDataFrame(ResultSet,StructType([StructField('TestScenario',StringType(),True)
                                                    ,StructField('ObjectName',StringType(),True)
                                                    ,StructField('PrimayrKey',StringType(),True)
                                                    ,StructField('AttributeToCheck',StringType(),True)
                                                    ,StructField('Result',
                                                                 StructType([
                                                                   StructField('element_count', IntegerType())
                                                                   , StructField('unexpected_count', IntegerType())        
                                                                     ,StructField('unexpected_percent', StringType() )
                                                                       , StructField('observed_value', IntegerType()) 

                                                                            ]))

                                                    ,StructField('TestResult',StringType(),True)
                                                         ,StructField('source_name',StringType(),True)
                                                         ,StructField('phase_name',StringType(),True)
                                                         ,StructField('zone',StringType(),True)
                                                         ,StructField('entity',StringType(),True)
  #                                                   ,StructField('RowCounts',ArrayType(IntegerType()),True)
                                                    ]))
    return ResultDf
  except Exception as e:
    error_message = "Exception occurred while ApplyReferenceTest :" + str(e)
    assert False


# COMMAND ----------

# DBTITLE 1,Create Test DataFrame
resutSet=[]
for entity in EntityList.split(','):
  if entity=='ALL':
    filteredObjectDetails=objectDetails
  else:  
    filteredObjectDetails=objectDetails[objectDetails['entity_name']==entity]
  records = filteredObjectDetails.to_records(index=False)
  recordlist = list(records)
  for object_Details in recordlist:
    source_name=object_Details[0]
    phase_name=object_Details[2]
    zone=object_Details[3]
    object_name=object_Details[4]
#     print(object_name)
    referenceDetails=pd.read_sql_query("exec [dq].[usp_get_primaryKeyDetails] '{}'".format(object_name ),conn) 
    if not(referenceDetails.empty):
      object_name=referenceDetails['object_name'][0]
      primary_key=referenceDetails['primary_key_fields'][0]
      DateField=referenceDetails['DateFiled'][0]
      resutSet=resutSet+ApplyReferenceTest(object_name,DateField,primary_key,source_name,phase_name,zone,entity)
FinalDataFrame= createSparkDataFrame(resutSet)   
#   print(filteredObjectDetails)

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