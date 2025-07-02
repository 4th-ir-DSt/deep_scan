# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>main_unstructuredxmlSource_sqld_gen2_struct_dictt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Create struct and dictionary for unstructuredxmlSource data</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2020/01/30</td></tr>
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
# MAGIC     <td>2019/01/01</td>
# MAGIC     <td> Framework </td>
# MAGIC     <td> Updated the typecast function to return string for all 
# MAGIC          <br>Transfered code out into function calls to the unstructuredxmlSource_functions notebook. Also renamed variables throughout to camel case as general convention
# MAGIC          <br>Dev rework for function names, dates, sql db connection logging
# MAGIC          <br>Updated variable names for struct and dict to match camel case</td>
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

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DecimalType,DateType
  from datetime import datetime, timedelta
  import pyspark.sql.functions as F
  from pyspark.sql.functions import lit,udf, struct, col,split,posexplode,max,regexp_replace,coalesce
  from pyspark import SparkContext,SparkConf
  import xml.etree.ElementTree as ET
  from  itertools import chain
except Exception as e:
  errorMessage="Exception occured while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Run log function notebook
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run standard function notebook
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run unstructuredxmlSource functions notebook
# MAGIC %run ../../util/spe/unstructuredxmlSource_functions

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
  #get the hour
  CreatedHour = currentTs.hour
  CreatedTimestamp = currentTs
  LastUpdatedTimestamp = currentTs
  #use the same time for the date also
  date=currentTs

  #PARAMETER FOR LOG_ERROR
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
  batchTaskSourceRows=0
  
  #initialise Notebook variables
  xmlSchemaStruct=StructType()

  xmlSchemaStructDic={}
except Exception as e:
  errorMessage = "Exception occured while variable initialisation :" + str(e)
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
 
  #CALL THE GET_LOGGING_PATH FUNCTION TO CREATE A LOG FILE PATH AS A STRING AND STORE IT IN A VARIABLE
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occured while getting parameters and initiliasing error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  #access secret of database connection details from azure key vault
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  
  #call function sqlDbConn to establish Database connection with given scope and key values
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,' Established Database connection successfully')
except Exception as e:
  errorMessage="Exception occured while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False  

# COMMAND ----------

# DBTITLE 1,Get metadata - Execute unstructuredxmlSource stored procedures
# call sp_exec_unstructuredxmlSource_get_object_details function to retrieve values from store procedure and store in pandas dataframe                                         

try:        
  unstructuredxmlSourceMetadataDetails = spExecunstructuredxmlSourceGetObjectDetails(
                                                   batchTaskId,
                                                   conn,
                                                   cursor,
                                                   adfPipelineName,
                                                   clusterId,
                                                   notebookName,
                                                   errorLogFileLocation)
except Exception as e:
  errorMessage="Exception occured while execution of unstructuredxmlSource_get_object_details : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Transform metadata into spark dataframe
#call Transform_pd_df_to_Pyspark_df function to convert pandas to pyspark dataframe and store return values in variables
try:
  unstructuredxmlSourceMetadataDetailsDF = convertSinglePandasToSparkDf(unstructuredxmlSourceMetadataDetails,
                                                                  cursor,
                                                                  batchTaskId,
                                                                  adfPipelineName,
                                                                  clusterId,
                                                                  notebookName,
                                                                  errorLogFileLocation)
except Exception as e:
  ErrorMessage="Unable to convert pandas to spark dataframe: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Make function call to get initial lists and dictionaries
try:
  pathList, elementList, metaDataDic, dataTypeDic, xmlSchemaDic = getInitialListsAndDictionaries(unstructuredxmlSourceMetadataDetailsDF
                                                                                                 ,cursor
                                                                                                 ,batchTaskId
                                                                                                 ,adfPipelineName
                                                                                                 ,clusterId
                                                                                                 ,notebookName
                                                                                                 ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Made function call to getInitialListsAndDictionaries")
except Exception as e:
  errorMessage="Exception occured while making function call to getInitialListsAndDictionaries: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Make function call to get number of elements and the metadata dictionary
try:
  numberOfElements, unstructuredxmlSourceSplitMedataDic = getNoElementsAndMetadataDict(unstructuredxmlSourceMetadataDetailsDF
                                                                     ,cursor
                                                                     ,batchTaskId
                                                                     ,adfPipelineName
                                                                     ,clusterId
                                                                     ,notebookName
                                                                     ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Made function call to getNoElementsAndMetadataDict")
except Exception as e:
  errorMessage="Exception occured while making function call to getNoElementsAndMetadataDict: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Make function call to get the XML for the struct and add convert to dataframe
try:
  xmlSchemaStruct = getXMLSchemaStruct(unstructuredxmlSourceSplitMedataDic
                                      ,xmlSchemaStruct
                                      ,numberOfElements
                                      ,pathList
                                      ,metaDataDic
                                      ,dataTypeDic
                                      ,xmlSchemaStructDic 
                                      ,cursor
                                      ,batchTaskId
                                      ,adfPipelineName
                                      ,clusterId
                                      ,notebookName
                                      ,errorLogFileLocation)

  xmlSchemaDF = (spark.createDataFrame([(str(xmlSchemaDic)
                                         ,str(xmlSchemaStruct.simpleString())
                                        )
                                       ]
                                        ,StructType([
                                          StructField("xmlSchemaDic",StringType(),True)
                                          ,StructField("xmlSchemaStruct",StringType(),True)
                                        ]
                                        )
                                      )
                )
  logTaskProgress(cursor,batchTaskId,"Made function call to getXMLSchemaStruct and add convert to dataframe")
except Exception as e:
  errorMessage="Exception occured while making function call to getXMLSchemaStruct and add convert to dataframe: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Create unstructuredxmlSource Schema
try:
  DestinationDF=(xmlSchemaDF
                 .withColumn("CreatedBatchID",lit(batchId))
                 .withColumn("CreatedTimestamp ",lit(CreatedTimestamp))
                 .withColumn("LastUpdatedBatchID",lit(batchId))
                 .withColumn("LastUpdatedTimestamp", lit(LastUpdatedTimestamp))
                 )

   #checks if the target is a person table adds a new column which contains the source_table_name 
                                     
  logTaskProgress(cursor,batchTaskId,"Addition of date and batch columns in target")
except Exception as e:
  errorMessage="Exception occured while adding batch and date columns: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,write to target location
#call write_to_target function to write dataframe into destination location
try:    
  DestinationDF.write.format("csv")\
                         .mode("overwrite") \
                         .option("header", "true")\
                         .save("/mnt/raw/reference/unstructuredxmlSource/unstructuredxmlSourceShema_Def")
except Exception as e:
  errorMessage="Exception occured while loading into target location: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,complete task and close connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,
                          conn,
                          batchTaskId,
                          batchTaskSourceRows,
                          batchTaskRowsLoaded,
                          batchTaskRejectRows,
                          batchTaskResult,
                          batchTaskResultLocation,
                          adfPipelineName,
                          clusterId,
                          notebookName,
                          errorLogFileLocation)
except:
  assert False