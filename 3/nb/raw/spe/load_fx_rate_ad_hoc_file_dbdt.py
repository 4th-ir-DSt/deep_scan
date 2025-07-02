# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_fx_rate_ad_hoc_file_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>notebook to Load Csv Files for exchange Rate </td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/10/31</td></tr>
# MAGIC </table>
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
# MAGIC     <td>@batchId = 1</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchTaskId</td>
# MAGIC     <td>@batchTaskId to retrieve batchTask details</td>
# MAGIC     <td>@batchTaskId = 1</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>adfPipelineName</td>
# MAGIC     <td>@adfPipelineName to retrieve DataFactory pipelinename details</td>
# MAGIC     <td>@adfPipelineName = 'testpipepline'</td>
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
# MAGIC           <td>03/11/2022 </td>
# MAGIC     <td>Harish N </td>
# MAGIC     <td>Replaced Json Schema within the notebook</td></tr>
# MAGIC     <tr>
# MAGIC       <td>09/11/2022 </td>
# MAGIC     <td>Harish N </td>
# MAGIC     <td>Added Validations For CSV</td></tr>
# MAGIC     <tr>
# MAGIC       <td>07/12/2022 </td>
# MAGIC     <td>Harish N </td>
# MAGIC     <td>Added the tables for the reference currency type and exchange rate type dataframes and corrected Decimal datatype</td></tr>
# MAGIC       <tr>
# MAGIC       <td>07/12/2022 </td>
# MAGIC     <td>Harish N </td>
# MAGIC     <td>Included known issue currency List</td></tr>
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

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run Miscellaneous Function notebook
# MAGIC %run ../../util/spe/misc_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DecimalType,ShortType
  from datetime import datetime,timedelta
  from pyspark.sql.functions import lit,col,explode,concat_ws,create_map,struct,collect_list,coalesce,sequence 
  from functools import reduce
  from itertools import chain 
  from collections import defaultdict
  import json
  import sys
  from decimal import Decimal
  import great_expectations as ge
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
  CreatedDate = int(currentTs.strftime('%Y%m%d'))
  createdDate=CreatedDate
  lastUpdateDate=createdDate
  #get the hour
  CreatedHour = currentTs.hour
  createdHour = CreatedHour
  createdTimestamp = currentTs
  lastUpdateTimestamp = currentTs
#   createdTimestamp
  lastUpdatedTimestamp = currentTs
  createdTime=currentTs
  #use the same time for the date also
  date=currentTs
#   createdBatchID=
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
  
  #variable to retrieve the batch rows loaded or not
  retrieveBatchRowsLoaded = False
  
except Exception as e:
  errorMessage = "Exception occurred while variable initialisation :" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try: 
  
  #get batchId from widgets
  dbutils.widgets.text('batchId','')
  batchId = dbutils.widgets.get('batchId')
  
  #get batchTaskId from widgets
  dbutils.widgets.text('batchTaskId','')
  batchTaskId = dbutils.widgets.get('batchTaskId')
  
  #get sourceId from widgets
  dbutils.widgets.text('sourceId','')
  sourceId = dbutils.widgets.get('sourceId')
  
  #get adfPipelineName from widgets
  dbutils.widgets.text('adfPipelineName','')
  adfPipelineName = dbutils.widgets.get('adfPipelineName')
  
  #get notebookName from widgets
  dbutils.widgets.text('notebookName','')
  notebookName = dbutils.widgets.get('notebookName')
  
  #get clusterId from widgets
  dbutils.widgets.text('clusterId','')
  clusterId = dbutils.widgets.get('clusterId')
    #assigned the source and batch to other variables that are referenced from the metadata
  SourceID=sourceId
  CreatedBatchID=batchId
  LastUpdatedBatchID=batchId
  lastUpdateBatchId=batchId
  createdBatchId=batchId
  validToTimestamp=None
  deletedTimestamp=None
  isActive=True
  sourceID=sourceId
  BatchID=batchId
  createdYear=int(createdDate/10000)
  createdMonth=int(createdDate/100)
 
  
  
  #call the GET_LOGGING_PATH function to create a log file path as a string and store it in a variable 
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')

except Exception as e:
  errorMessage = 'Exception occurred while getting parameters and initialising error log location ' + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn=dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')

#call function sqlDbConn to establish Database connection with given scope and key values
try:
  conn,cursor = sqlDbConn(dbconn,
                          batchTaskId,                          
                          adfPipelineName,
                          clusterId,
                          notebookName,
                          errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Successfully Established SQL Connection")
except Exception as e:
  errorMessage="unable to establish DB connection: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Mark Source File Dates
try:
      populateFileObjectDateAvailability(
                                    cursor
                                    ,batchTaskId
                                    ,adfPipelineName
                                    ,clusterId
                                    ,notebookName
                                    ,errorLogFileLocation)

      logTaskProgress(cursor,batchTaskId,"Loaded dates are marked for source File object")
except Exception as e:
  errorMessage="Exception occurred while marking the file dates as loaded: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Get raw object information
                                        
#call spExecHighAndLowLevel function to retrieve values from store procedure and store in pandas dataframe                                         
try:        
  sourceDestinationTableDetails,sourceDestinationFieldsDetails = spExecHighAndLowLevel(conn
                                                                                       ,cursor
                                                                                       ,batchTaskId
                                                                                       ,adfPipelineName
                                                                                       ,clusterId
                                                                                       ,notebookName
                                                                                       ,errorLogFileLocation)

  #Replace the nulls with 'none' as this sp always returns single record no need to specify schema
  sourceDestinationTableDetails=sourceDestinationTableDetails.fillna('none')
  logTaskProgress(cursor,batchTaskId,"Executed high and low level store procedures")
except Exception as e:
  errorMessage="Exception occurred while execution of object high and low level stored procedures: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Get Struct For High And Low Level
#Schema for High level stor proc as pandas stores different null values for different types having an issue while conveting to spark df for infering types for null columns.
sourceDestinationFieldsDetailsSchema = getStructForHighAndLowLevelDf()
logTaskProgress(cursor,batchTaskId,"Got the schema for destination field details")

# COMMAND ----------

# DBTITLE 1,Convert Pandas Spark Df
#call Transform_pd_df_to_Pyspark_df function to convert pandas to pyspark dataframe and store return values in variables
try:
  srcAndDesTableDF,srcAndDesFieldsDF = convertPandasToSparkDf(sourceDestinationTableDetails,
                                                                  sourceDestinationFieldsDetails,
                                                                  sourceDestinationFieldsDetailsSchema,
                                                                  cursor,
                                                                  batchTaskId,
                                                                  adfPipelineName,
                                                                  clusterId,
                                                                  notebookName,
                                                                  errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Converted sourceDestinationTableDetails and sourceDestinationFieldsDetails pandas to spark dataframes")
except Exception as e:
  errorMessage="Unable to convert pandas to spark dataframe: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Initialize Variables 
try:
  #collect dataframe row as a dictionary
  srcDesDict = srcAndDesTableDF.collect()[0].asDict()
  #the list of source columns that are required for destination e.g. ["name as firstname", "dob as dateofbirth"]
  sourceColumns = srcAndDesFieldsDF.agg(collect_list(srcAndDesFieldsDF.source_as_destination)).collect()[0][0]
  #the id of the source object taken from teh procedure output
  sourceObjectId = srcDesDict['source_object_id']  
  #the name of the source taken from the procedure output
  sourceTableName = srcDesDict['source_object_name']
  #the location of the source taken from the procedure output
  sourceLocation = srcDesDict['source_location']
  #the format of the source taken from the procedure output
  sourceFormat = srcDesDict['source_format']
  #the header of the source taken from the procedure output
  sourceHeader=srcDesDict['source_header']
  #the calculated where expression to be applied in source dataframe
  whereExpression = srcDesDict['source_where_clause']
  #the xml start string to be applied when calculating xml fields
  jsonStartString = srcDesDict['source_XML_string_prefix']
  #the id of the target object taken from the procedure ouptut
  targetObjectId=srcDesDict['destination_object_id']
  #the name of the target table taken from the procedure output
  targetObjectName=srcDesDict['destination_object_name']
  #the name of the target location taken from the procedure output
  targetLocation=srcDesDict['destination_location']
  #the format of the target taken from the procedure output

  targetFormat='delta' if srcDesDict['destination_object_type']=='DeltaTable' else srcDesDict['destination_format']
  #Check Target object has Pii attributes
  calculatePiiHash=srcDesDict['calculate_pii_hash']
  primaryKeyFieldsList=srcAndDesFieldsDF.where("primary_key_order is not null").orderBy('primary_key_order').agg(collect_list(srcAndDesFieldsDF.destination_object_attribute_name)  ).collect()[0][0]
  type2FieldsList=srcAndDesFieldsDF.where("track_type_2_changes>0").orderBy('destination_object_attribute_order').agg(collect_list(srcAndDesFieldsDF.destination_object_attribute_name)).collect()[0][0]
  delimiter=srcDesDict['source_separator']
  inputFolderPath=f'{sourceLocation}Input/'
  failureFolderPath=f'{sourceLocation}Failure/'
  archiveFolderPath=f'{sourceLocation}Archive/'
  logTaskProgress(cursor,batchTaskId,"Got dataframe values into variables")
except Exception as e:
  errorMessage="Unable to retrieve the dataframe values as variables: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Directory Check For Files To Load
#call Get_Select_And_With_Column_Details to get Columnlist tuple
try:
  filesToLoad,filesList = checkDirectory(inputFolderPath
                                             ,cursor 
                                             ,batchTaskId
                                             ,adfPipelineName
                                             ,clusterId
                                             ,notebookName
                                             ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Checking InputFolder to load Files ")
except Exception as e:
  errorMessage="Unable to check Files In the inputDirectory: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Get Column List Details
#call Get_Select_And_With_Column_Details to get Columnlist tuple
try:
  if filesToLoad:
      columnList = getSelectAndWithColumnDetails(srcAndDesFieldsDF
                                                 ,cursor 
                                                 ,batchTaskId
                                                 ,adfPipelineName
                                                 ,clusterId
                                                 ,notebookName
                                                 ,errorLogFileLocation)
      logTaskProgress(cursor,batchTaskId,"column list tuples are created")
except Exception as e:
  errorMessage="Unable to retrieve he column Details for With Expression and select: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Get With And Select Column Expressions
try:
  if filesToLoad:
      #Get With Column Expression and Select Expression Details by calling the function
      withExpressionListFinal,columnListFinal=getWithExpressionsAndSelectList(columnList
                                                                              ,jsonStartString
                                                                              ,cursor
                                                                              ,batchTaskId
                                                                              ,adfPipelineName
                                                                              ,clusterId
                                                                              ,notebookName
                                                                              ,errorLogFileLocation)
      logTaskProgress(cursor,batchTaskId,"with column and select column expressions are created")
      #Prepare select expression
      selectExpression=[item[1] for item in columnListFinal]
      #Get join fileds list
      joinFieldsList=[c[1] for c in columnList  if c[2]=='join']
except Exception as e:
  errorMessage="Unable to retrieve with and selectedExpressions: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Read Schema
try:
#     with open(f'../../../schema/{targetObjectName}.json') as f:
    schema='''{"fields":[
                        {"metadata":{},"name":"Start_Date","nullable":true,"type":"string"}
                        ,{"metadata":{},"name":"End_Date","nullable":true,"type":"string"}
                        ,{"metadata":{},"name":"Source_Currency","nullable":true,"type":"string"}
                        ,{"metadata":{},"name":"Target_Currency","nullable":true,"type":"string"}
                        ,{"metadata":{},"name":"Rate_Type","nullable":true,"type":"string"}
                        ,{"metadata":{},"name":"Rate","nullable":true,"type":"decimal(22,8)"}
                        ],"type":"struct"
                        }'''
    customSchema = json.loads(schema)
    custom_schema = StructType.fromJson(customSchema)
    logTaskProgress(cursor,batchTaskId,"Custom Schema read cmpleted")

except Exception as e:
  errorMessage = "Exception occurred while reading custom schema from the source: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
  assert False
# print(data)

# COMMAND ----------

# DBTITLE 1,Selection of destination columns from source
# call select_dest_from_src function to select destination column from the source and store return values in a variable.
try:
  if filesToLoad:
      destinationDf = selectDestFromSrcWithDelimiter(sourceFormat,sourceHeader,inputFolderPath,sourceTableName,withExpressionListFinal,custom_schema,
                             selectExpression,whereExpression,delimiter,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      logTaskProgress(cursor,batchTaskId,"selectDestFromSrc function executed")

except Exception as e:
  errorMessage = "Exception occurred while reading destination columns from the source: " + str(e)
  dbutils.fs.cp(f'{inputFolderPath}',f'{failureFolderPath}/{createdYear}/{createdMonth}/{createdDate}',True)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
  assert False

# COMMAND ----------

# DBTITLE 1,CSV Validations
try:
  if filesToLoad:
        df=spark.read.table('Raw_MDM.mdm_Exchange_Rate_Type')
        cdf=spark.read.table('Raw_MDM.mdm_Currency')
        ExchangeRateTypes=df.select('Exchange_Rate_Type').agg(collect_list(df.Exchange_Rate_Type)).collect()[0][0]
        currencyCodeList=cdf.select('Currency_Code').agg(collect_list(cdf.Currency_Code)).collect()[0][0]
        known_issue_currency_List=['ITK'
                                    ,'IT2'
                                    ,'CLE'
                                    ,'TRK'
                                    ,'EC1'
                                    ,'MXF'
                                    ,'IT3'
                                    ,'BFP'
                                    ,'IRQ'
                                    ,'JPX'
                                    ,'JP1'
                                    ,'EQE'
                                    ,'NG2'
                                    ,'TR2'
                                    ,'BRZ'
                                    ,'TR6'
                                    ,'TR3'
                                    ,'KE1']
        currencyCodeList=currencyCodeList+known_issue_currency_List
        # pdViewDef = pd.read_sql_query("".format(batchTaskId),conn)
        gdf1=ge.dataset.SparkDFDataset(destinationDf)
        ex1=gdf1.expect_column_distinct_values_to_be_in_set('Rate_Type',ExchangeRateTypes)
        ex2=gdf1.expect_column_distinct_values_to_be_in_set('Source_Currency',currencyCodeList,catch_exceptions=True)
        ex3=gdf1.expect_column_distinct_values_to_be_in_set('Target_Currency',currencyCodeList)
        ex4=gdf1.expect_column_values_to_not_be_null('Rate')
        ex5=gdf1.expect_column_values_to_be_in_type_list('Rate',['DecimalType'])
        ValidationResult=ex1['success']&ex2['success']&ex3['success']&ex4['success']&ex5['success']
        logTaskProgress(cursor,batchTaskId,"Validations performed on csv ")
        if ValidationResult:
            logTaskProgress(cursor,batchTaskId,"Validations performed on csv RateTypeValidation:{ex1},Source_CurrencyValidation:{ex2},Target_CurrencyValidation:{ex3},RateValidation:{ex4}")
        else:
            assert False

except Exception as e:
  errorMessage = f"Validations performed on csv RateTypeValidation:{ex1['success']},Source_CurrencyValidation:{ex2['success']},Target_CurrencyValidation:{ex3['success']},RateValidation:{ex4['success']} "
  dbutils.fs.cp(f'{inputFolderPath}',f'{failureFolderPath}/{createdYear}/{createdMonth}/{createdDate}',True)
  list(map(lambda x: dbutils.fs.rm(x),filesList))
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
  assert False


# COMMAND ----------

# DBTITLE 1,Write To Target Table
#call writeToTargetTableWithAppend function to write dataframe into destination table with append
try: 
  if filesToLoad:
      writeToTargetTableWithAppend(destinationDf
                                  ,targetObjectName
                                  ,targetFormat
                                  ,cursor
                                  ,batchTaskId
                                  ,adfPipelineName
                                  ,clusterId
                                  ,notebookName
                                  ,errorLogFileLocation) 
      dbutils.fs.cp(f'{inputFolderPath}',f'{archiveFolderPath}/{createdYear}/{createdMonth}/{createdDate}',True)
      conn, cursor = logTaskProgressWithRetry(dbconn,conn,cursor,batchTaskId,"writeToTargetTableWithAppend function executed")
  
except Exception as e:
  errorMessage = "Exception occurred while loading into target table with append: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Mark Dates Loaded
try:
  if filesToLoad:
      conn, cursor = markDatesLoaded(targetObjectId
                                    ,sourceObjectId
                                    ,currentTs
                                    ,currentTs
                                    ,dbconn
                                    ,conn
                                    ,cursor
                                    ,batchTaskId
                                    ,adfPipelineName
                                    ,clusterId
                                    ,notebookName
                                    ,errorLogFileLocation)

      logTaskProgress(cursor,batchTaskId,"Loaded dates are marked for destination object")
except Exception as e:
  errorMessage="Exception occurred while marking the dates as loaded: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Complete task and close the Database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  if filesToLoad:
      list(map(lambda x: dbutils.fs.rm(x),filesList))
  taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,
                            adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False