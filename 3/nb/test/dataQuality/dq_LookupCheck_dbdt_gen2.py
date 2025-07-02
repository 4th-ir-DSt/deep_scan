# Databricks notebook source
# DBTITLE 0,Untitled
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>dq_LookupCheck_dbdt_gen2</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>lookup values checked for selected destination source attribute</td></tr>
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

# DBTITLE 1,Import Modules
try:
  import pandas as pd
  import pyodbc
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
  from datetime import datetime,timedelta
  from pyspark.sql.functions import lit,col,explode,concat_ws,struct,collect_list,lower,split
  from functools import reduce
  from itertools import chain 
  #dbutils.library.installPyPI("great_expectations")
  import great_expectations as ge
  import json
  dbutils.widgets.removeAll()

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
  targetLocation='/mnt/dataquality/lookupChecks'
  targetFormat='parquet'
except Exception as e:
  error_message = "Exception occurred while variable initialisation :" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn=dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')
conn = pyodbc.connect(dbconn, autocommit = True)
cursor = conn.cursor()

# COMMAND ----------

# DBTITLE 1,SourceList
sourceDetails=pd.read_sql_query("select distinct source_name from config.tbl_source",conn).fillna('')
# print(sourceDetails)

# COMMAND ----------

# DBTITLE 1,Create SourceWidget
dbutils.widgets.dropdown("1.sourceName", "Intrali4444", [str(x) for x in sourceDetails['source_name']])

# COMMAND ----------

# DBTITLE 1,Create SourceWidget
selectedSourceList=dbutils.widgets.get('1.sourceName')

# COMMAND ----------

# DBTITLE 1,Get ObjectDetails
objectDetails=pd.read_sql_query("exec [dq].[usp_get_sourceDestinationlookupObjects] {}".format(selectedSourceList),conn)
# objectDetails

# COMMAND ----------

# DBTITLE 1,Create Entity Widget
dbutils.widgets.multiselect("2.Entitiy", 'ALL', [str(x) for x in (objectDetails['entity_name'].append(pd.Series(['ALL']))).unique()])
EntityList=dbutils.widgets.get('2.Entitiy')

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
                                         where do.object_name ='{destinationObject}' and om.source_object_id is not null ''',conn)
      source_Object=sourceObject.iloc[0,0]
      return source_Object
  except Exception as e:
    error_message = f"Exception occurred while sourceDestinationObject :{destinationObject}" + str(e)
    assert False

# COMMAND ----------

# DBTITLE 1,Mapping object details
# getting the object to object mapping details for all join checks
def objectmappingDetailsFun(destinationObject):
  try:
      objectmapping_Details=pd.read_sql_query("exec [dq].[usp_get_objectmappingDetailslookup] {}".format(destinationObject),conn)
      destination_attributelist=objectmapping_Details['destination_object_attribute_name'].tolist()
      return objectmapping_Details,destination_attributelist
  except Exception as e:
        error_message = f"Exception occurred while objectmappingDetails:{destinationObject}" + str(e)
        assert False

# COMMAND ----------

# DBTITLE 1,Lookup table and Join col value
# Joning the Reference table to get the Brand and saleschannel from lookup table

def LookupJoincol(destinationObject,objectmappingDetails,sourceObject):
  try:
      columnList1=objectmappingDetails["destination_object_attribute_default_value_new"].tolist()
      columnList1=columnList1[:2]
      columnList2=objectmappingDetails["join_col"].tolist()
      destination_object_attribute_name=objectmappingDetails["destination_object_attribute_name"].tolist()
      destination_object_attribute_name=','.join(destination_object_attribute_name)
      selectString1=','.join(columnList1)
      selectString2=','.join(columnList2)
      source_Df=sqlContext.sql(f''' select sd.IhpIntermediaryRef,sd.IhpSchemeRef,sd.UUID,{selectString2} from (select {selectString1},IHP_XML_OBJECT.OuterXML.Parent_UUID as UUID from {sourceObject}) sd left join ihprefdata.schemes ihs on ihs.Schema_Ref=sd.IhpSchemeRef left join ihprefdata.intermediary ii  on ii.Intermediary_Code=sd.IhpIntermediaryRef ''')
      destination_Df=sqlContext.sql(f'''select distinct UUID,{destination_object_attribute_name} from {destinationObject}''')  
      return source_Df,destination_Df
  except Exception as e:
        error_message = f"Exception occurred while LookupJoincol and sourceDFDestinationDF:{destinationObject}" + str(e)
        assert False

# COMMAND ----------

# DBTITLE 1,Result data frame
# Final output dataframe structure
def createSparkDataFrame(ResultSet):
  try:
    ResultDf=spark.createDataFrame(ResultSet,StructType([StructField('TestScenario',StringType(),True)
                                                        ,StructField('TestResult',StringType(),True)                                                                                                    ,StructField('source_name',StringType(),True)
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

# DBTITLE 1,Function to test lookup value
# checking null counts for the columns on source table and destination table
def lookupCheck():
    try:
      global resultSet,user
      resultSet=[] 
      user=spark.sql("select current_user() as user").collect()[0]["user"] 
      for entity in EntityList.split(','):
         if entity=='ALL':
           filteredObjectDetails=objectDetails
         else:  
            filteredObjectDetails=objectDetails[objectDetails['entity_name']==entity]
         destinationObject=filteredObjectDetails.filter(items=['object_name'])
         destinationObject=destinationObject['object_name'].tolist()            
         for object_name in destinationObject:
           sourceObject=sourceDestinationObject(object_name)
           objectmappingDetails,destination_attribute_list=objectmappingDetailsFun(object_name)
           sourceDf,destinationDf=LookupJoincol(object_name,objectmappingDetails,sourceObject)
           sourceDf.registerTempTable("sourceDfTable")
           destinationDf.registerTempTable("destinationDfTable")
           UUID_null_value=spark.sql(f"select count(t.dt_UUID) as count from ( select st.UUID as st_UUID,dt.UUID as dt_UUID from sourceDfTable st                             left join destinationDfTable dt on dt.UUID=st.UUID) t where t.dt_UUID is null").collect()[0].asDict()['count']
           filteredObjectDetails=objectDetails[objectDetails['object_name']==object_name]
           records = filteredObjectDetails.to_records(index=False)
           recordlist = list(records)
           for object_Details in recordlist:
             source_name=object_Details[0]
             entity_name=object_Details[1]
             phase_name=object_Details[2]
             zone=object_Details[3]      
             for column in destination_attribute_list:
               if UUID_null_value<=0:
                   result='Success'
               else:
                   result='Failed'
               resultSet.append(("Lookup_check",result,source_name,phase_name,zone,object_name,column,user,entity_name))           
    except Exception as e:
       errorMessage=f"Exception occured while checking expression:{object_name}" + str(e)
       assert False

# COMMAND ----------

# DBTITLE 1,main function
def main_lookupCheck():
  try:
    if selectedSourceList in ('CdlIhpQuote'):
      lookupCheck() 
      FinalDataFrame=createSparkDataFrame(resultSet)                                            
      FinalDataFrame=(FinalDataFrame
                      .withColumn('TestRunDate',lit(createdDate))
                      .withColumn('TestRunHour',lit(createdHour))
                      .withColumn('TestRunId',lit(testRunId)))
      FinalDataFrame.write.partitionBy('TestRunDate','TestRunHour','source_name').mode("append").format(targetFormat).save(targetLocation)
      conn.close()
#       print("results s-aved")
    else:
      pass
  except:
    pass

# COMMAND ----------

# DBTITLE 1,calling main function
main_lookupCheck()