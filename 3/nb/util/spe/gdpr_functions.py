# Databricks notebook source
# MAGIC %md
# MAGIC #gdpr_functions loaded

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>gdpr_functions</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Functions used on GDPR notebooks</td></tr>
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
# MAGIC     <td>Framework</td>
# MAGIC     <td>Changes in the function getPartitionWhereClause to fix empty string issues
# MAGIC         <br>Changes to use the hash attribute type instead of the attribute name
# MAGIC         <br>Added functions getGDPRdataRetentionSpExec,getReferenceAndStagingDataFrames,createSupersetPiiDf,createActivePiiDf for GDPR redaction keys
# MAGIC         <br>Added the PiiTraceabilityHash in the function createActivePiiDf
# MAGIC         <br>Changes to use the object name instead of the source id for the reference tables
# MAGIC         <br>Changes in the function getRetentionKeys to use the column RedactedTimestamp instead of ProcessedTimestamp
# MAGIC         <br>Changed getPiiDetailsWithStagingDF to return and empty dataframe case the table piiDetailsWithStagingTable doesn't exist
# MAGIC         <br>Changes to update LastUpdatedTimestamp and LastUpdatedBatchId in the redaction process
# MAGIC         <br>Changed createActivePiiDf and createPIItempTables to use the temp table temp_otherPIIHA
# MAGIC         <br>Changes to the temp deltables for gdpr retention
# MAGIC         <br>Added function getRetentionPiiDetailsWithStagingDF with changes to create a temp table by source
# MAGIC         <br>Changes to use the JoinKey in the retention redaction
# MAGIC         <br>Added functions retentionRedactionPiiInRefined, retentionRedactionPiiInPerson, retentionRedactionPiiInStaging. Changed function applyRetentionRedaction to do first refined, then person and finally staging tables. Changed function getPartitionWhereClause to deal with empty string.
# MAGIC         <br>Changed getRetentionPiiDetailsWithStagingDF join and added distinct to the person table. Removed the broadcast from gdprRetentionRedactionHashes.Changed updateReferenceProcessed and getRetentionKeys to use the sourceID.
# MAGIC         <br>Changed getRetentionKeys to use the usp_gdpr_get_sources.
# MAGIC         <br>Changed applyRetentionRedaction to use the merge source table with or without join keys. Changed createActivePiiDf to use the partition when writing to temp_otherPIIHA
# MAGIC         <br>Fixed applyRectification to return out of the for loop. Fixed logTaskProgress in gdprRectification to return the non processed tables. Changed logTaskProgress text in gdprRedaction and gdprRectification.
# MAGIC         <br>updated getRetentionKeys,updateReferenceProcessed to include the batchID parameter and bug fix for reference redact state table audit columns to source from current batch
# MAGIC         <br>Added fields PiiHash: bigint, PiiHashVersion: int into temp_otherPIIH table
# MAGIC         <br>UPdated getReferenceAndStagingDataFrames Function to cast join key as string as GDPR temp tables saving it as string
# MAGIC         <br>corrected pattern for looping for the following functions getPiiDetailsWithStagingDF, redactPiiInDownstreamAndStaging, applyRedaction, setNewPiiHashes, rectifyPiiInDownstreamAndStaging, applyRectification, gdprRectification, updateReferenceProcessed, getRetentionPiiDetailsWithStagingDF, retentionRedactionPiiInRefined, retentionRedactionPiiInPerson, applyRetentionRedaction
# MAGIC         <br>Added call to function to remove duplicate columns in getPiiDetailsWithStagingDF and getRetentionPiiDetailsWithStagingDF
# MAGIC         <br>createActivePiiDf - change supersetPiiDf and otherPiiDf to use the specific location and save to load/save to parquet, createPIItempTables - removed creation of the temp_supersetTempKeysPIIHA, temp_inUsePIIHA and temp_otherPIIHA delta tables. Added StagingTableName to the partitioning and optimizeWrite table property of temp_toRemovePIIHA in createActivePiiDf - Removed where clause to access the staging table as that is already in the specific path for the supersetPiiDf and otherPiiDf dataframes
# MAGIC         <br>Added usp_gdpr_get_tempview_recreate_object function</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC 
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
  import os 
  from datetime import datetime, timedelta
  from pyspark.sql.functions import lit,col
except Exception as e:
  errorMessage="Exception occured while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Function to add methods to class
from functools import wraps # This convenience func preserves name and docstring
def add_method(cls):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        setattr(cls, func.__name__, wrapper)
        # Note we are not binding func, but wrapper which accepts self but does exactly the same as func
        return func # returning func means func can still be used normally
    return decorator

# COMMAND ----------

# DBTITLE 1,Initialise the class
#initialise the class
class gdprFunctions:
  pass;

# COMMAND ----------

# DBTITLE 1,Function: Validate PII list
#Check if the inserted PII list is valid, returns a DF with the PII Hash Atributes case is valid
@add_method(gdprFunctions)
def validatePIIList(pii_values, errorLogFileLocation):
  try:
    pii_hash_attributes_schema = StructType([
                                        StructField("pii_hash", StringType(),True),
                                        StructField("pii_traceability_hash", StringType(),True),
                                        StructField("pii_hash_version", IntegerType(),True)
                                       ])
  
    piiHashAttributesDF = spark.createDataFrame(pii_values, pii_hash_attributes_schema)
    
    #return the spark dataframe and True for success
    return piiHashAttributesDF, True, ''
  
  except Exception as e:
    errorMessage = 'Exception occurred while validating the PII list: ' + str(e)
    print(errorMessage)
    return None, False, errorMessage

# COMMAND ----------

# DBTITLE 1,Function : Check the batch status
#this function check the status of the batch whether a batch is running or not.
@add_method(gdprFunctions)
def batchStatusCheck(conn, errorLogFileLocation):
  try:
    #check if any other batch is running
    existingBatchCount = pd.read_sql_query("SELECT * FROM audit.tbl_batch WHERE status = 'running'", conn)
    if existingBatchCount.shape[0] == 0:
      return True
    else:
      return False     
  except Exception as e:
    errorMessage = 'The batchStatusCheck function call failed'
    logToFile(errorLogFileLocation, errorMessage)
    sys.exit(errorMessage)


#Function to check if the running batch is the one with the batch id provided.
@add_method(gdprFunctions)
def currentBatchStatusCheck(batchId, conn, errorLogFileLocation):
  try:
    #check whether the generated new batch_id is running
    currentBatchCount = pd.read_sql_query("SELECT * FROM audit.tbl_batch WHERE status = 'running' AND batch_id = {}".format(batchId),conn)
    if currentBatchCount.shape[0] == 1:
      return True
    return False
  except Exception as e:
    errorMessage = 'currentBatchStatusCheck function call failed'
    sys.exit(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function: Start a new batch and validate that is running
#Starts a new batch and validates it is runnning
@add_method(gdprFunctions)
def batchStartAndValidation(conn, cursor, errorLogFileLocation, scheduleReference):
  try :
    #Check if there's any batch running
    checkBatchStatus = gdprFunctions.batchStatusCheck(conn, errorLogFileLocation)
    #No batch running
    if checkBatchStatus == True:
      #execute the usp_batch_start store procedure to start a new batch 
      cursor.execute('exec audit.usp_batch_start ?',scheduleReference)
      while cursor.nextset():
        x = 1
      #select the new batch_id and batch_task_id inserted 
      currentBatch = pd.read_sql_query("SELECT ba.batch_id ,tbt.batch_task_id FROM audit.tbl_batch  ba JOIN config.tbl_schedule sc ON ba.schedule_id = sc.schedule_id JOIN audit.tbl_batch_task tbt ON ba.batch_id = tbt.batch_id WHERE sc.schedule_reference = '{}' and ba.status = 'running'".format(scheduleReference), conn)
      currentBatchDF = spark.createDataFrame(currentBatch)
      currentBatchDic = currentBatchDF.collect()[0].asDict() 
      batchId = currentBatchDic['batch_id']
      batchTaskId = currentBatchDic['batch_task_id'] 
      #checkCurrentBatch checks if the batch is running for the batch id provided
      checkCurrentBatch = gdprFunctions.currentBatchStatusCheck(batchId, conn, errorLogFileLocation)
      if checkCurrentBatch == True:
        return batchId, batchTaskId, True
      else:
        return -1, -1, False
    
    else:
      return -1, -1, False
      
  except Exception as e:
    errorMessage = 'There is an issue with the batch, please contact the EDS team' + str(e)
    sys.exit(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function : Execute  usp_get_partition_schemes stored procedure 
# get the partition schemes and tables names using the usp_get_partition_schemes stored procedure 
@add_method(gdprFunctions)
def getPartitionSchemes(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
     #Call usp_get_persons_tables to get all the persons tables
    partitionSchemes = pd.read_sql_query('exec config.usp_get_partition_schemes',conn)
    schema = StructType([StructField("object_id"            , IntegerType(), True)
                        ,StructField("object_name"          , StringType() , True)
                        ,StructField("zone"                 , StringType() , True)
                        ,StructField("source_id"            , IntegerType(), True)
                        ,StructField("source_name"          , StringType() , True)
                        ,StructField("partition_scheme"     , StringType() , True)
                        ,StructField("all_partition_columns", StringType() , True)
                     ])
    partitionSchemesDF = spark.createDataFrame(partitionSchemes, schema)
    
    logTaskProgress(cursor,batchTaskId,'Executed usp_get_partition_schemes stored procedure successfully')
    return partitionSchemesDF
  except Exception as e:
    errorMessage = 'Exception occurred while executing usp_get_partition_schemes stored procedure: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Function : Get the Pii details with staging table name
# function to get a dataframe with all the staging tables related to the personsTablesNames by the pii_traceability_hash
@add_method(gdprFunctions)
def getPiiDetailsWithStagingDF(personsTablesNames, broadcastPiiHashAttributesDF, addPiiColumns, conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #Clean the temporary table before writing
    dbutils.fs.rm("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingTable" , True);
    PIIColumnsStr=''
    
    #get the distinct person table names and collect to list
    personTableList = personsTablesNames.select("object_name", "zone", "source_name", "partition_scheme", "all_partition_columns").distinct().collect()
    
    #For each person table, join with the inputed hashes to get the staging tables
    for personTableItem in personTableList:
      objectName = personTableItem['object_name']
      zone = personTableItem['zone']
      source_name = personTableItem['source_name']
      partition_scheme = personTableItem['partition_scheme']
      all_partition_columns = personTableItem['all_partition_columns']
      
      if(addPiiColumns == True):
        #Get the list with the pii columns for the current person table
        PIIColumnsStr = gdprFunctions.getPiiColumns(objectName, conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
        addPiiColumns = False
      
      #form the column list with possible duplicates
      columnsString = "PiiTraceabilityHash, SourceID, StagingTableName, StagingCreatedBatchId, {}{}".format(all_partition_columns, PIIColumnsStr)
      
      #call the function to remove duplicate columns
      distinctColumnList = removeDuplicateColumnsFromString(columnsString,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      
      #Get the person table with all the partitions and pii columns
      pTable = spark.sql("select {} from {}".format(distinctColumnList, objectName))
      
      #Join with the broadcast Pii attributes using the traceability hash
      joinCondition = "[broadcastPiiHashAttributesDF.pii_traceability_hash == pTable.PiiTraceabilityHash]"
      piiDetailsWithStagingDFAux = (broadcastPiiHashAttributesDF
                  .join(pTable 
                        ,eval(joinCondition)
                        ,how = 'inner')
                  .select(pTable["*"])
                  .withColumn('PersonTable', lit(objectName))
                  )
      #Keep appending the outuput until all the persons tables are done
      piiDetailsWithStagingDFAux.write.format("delta").mode("append").save("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingTable")
    
    #check if the table piiDetailsWithStagingTable exists
    piiDetailsWithStagingTableExists = os.path.exists('/dbfs/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingTable')
    
    #if the table doesn't exist return an empty dataframe
    if piiDetailsWithStagingTableExists == False:
      emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([]))
      return emptyDf
    
    #if the table exist proceed
    else:
      #Get the distinct rows from the previous join
      piiDetailsWithStagingDF = spark.read.format('delta').load("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingTable").distinct()

      #Clean the table location before writing
      dbutils.fs.rm("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingWithoutDuplicatesTable" , True);
      #Write the distinct rows to a temporary table
      (piiDetailsWithStagingDF.write
                              .format("delta")
                              .mode("append")
                              .save("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingWithoutDuplicatesTable"))

      piiDetailsWithStagingDF = (spark.read
                                      .format('delta')
                                      .load("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingWithoutDuplicatesTable"))

      logTaskProgress(cursor,batchTaskId,'Successfully created the Pii Details table')
      #return the pii Details With Staging Without Duplicates
      return  piiDetailsWithStagingDF
  except Exception as e:
    errorMessage = 'Exception occurred while getting the Pii details with staging: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Function : Get the string list of Piis to remove
# function to get the string list of Pii hashes to remove
@add_method(gdprFunctions)
def getPIIHAToRemove(piiDetailsWithStagingDF, broadcastPiiHashAttributesDF, sourceName, conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  try:
      #Set a dataFrame with the distinct pii_traceability_hash
      piiStagingTableDF = piiDetailsWithStagingDF.select("PiiTraceabilityHash").distinct()
      
      #get the list of pii hash to redact and convert the list to string
      PIIHAToRemove = piiStagingTableDF.select('PiiTraceabilityHash').rdd.flatMap(lambda x:x).collect()
      
      #get the list of inserted pii hashes not found in any persons table, convert the list to string and inform the user
      notFoundPIIHA = (broadcastPiiHashAttributesDF
                       .filter(~col('pii_traceability_hash')
                               .isin(PIIHAToRemove))
                       .select('pii_traceability_hash')
                       .rdd
                       .flatMap(lambda x:x)
                       .collect()
                      )
      
      if len(notFoundPIIHA) > 0 :
        print('For the source {}, the following PII hashes were not found in any persons table: {}'.format(sourceName, notFoundPIIHA))
        
      logTaskProgress(cursor,batchTaskId,'Successfully created the Pii hashes to remove list')
      
      return PIIHAToRemove
  
  except Exception as e:
    errorMessage = 'Exception occurred while getting pii string list of Pii to remove: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Function : Execute  usp_get_downstream_objects stored procedure 
# get the downstream information using the usp_get_downstream_objects stored procedure 
@add_method(gdprFunctions)
def getDownstreamObjects(stagingTableName,conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    downstreamTable = pd.read_sql_query('exec config.usp_get_downstream_objects {}'.format(stagingTableName),conn)
    schema = StructType([StructField("object_id"                   , IntegerType(), True)
                        ,StructField("object_name"                 , StringType() , True)
                        ,StructField("zone"                        , StringType() , True)
                     ])
    downstreamTableName = spark.createDataFrame(downstreamTable , schema)
    
    logTaskProgress(cursor,batchTaskId,'Successfully executed stored procedure usp_get_downstream_objects')

    return downstreamTableName
  except Exception as e:
    errorMessage = 'Exception occurred while executing the usp_get_downstream_objects stored procedure: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Function : Execute  usp_get_stagingtable_piifields_as_redacted stored procedure 
# get the pii columns for the staging table to be redacted
@add_method(gdprFunctions)
def getStagingtablePiiFieldsAsRedacted(stagingTableName,conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    piiFieldsToUpdate = pd.read_sql_query("exec [config].[usp_get_stagingtable_piifields_as_redacted] {}".format(stagingTableName),conn).at[0,'piifields_to_be_redacted']
    
    logTaskProgress(cursor,batchTaskId,'Succesfully executed the usp_get_stagingtable_piifields_as_redacted stored procedure')
    return piiFieldsToUpdate
  except Exception as e:
    errorMessage = 'Exception occurred while executing usp_get_stagingtable_piifields_as_redacted stored procedure: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Function : Gets the partitions where clause
#Gets the partitions where clause to apply to the object
@add_method(gdprFunctions)
def getPartitionWhereClause(objectName, partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, tablesNonProcessedList, tablesProcessedList, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  try :
    #Filter the specific object (table) for what the where clause will be produced
    objectDF = partitionSchemesDF.filter(col('object_name') == objectName)
    #Gets the partition scheme (and zone) for the object
    objectDic = objectDF.collect()[0].asDict()
    zone = objectDic['zone']
    partitionScheme = objectDic['partition_scheme']
    
    #Check if the table has a partition scheme
    if partitionScheme == '':
      if(zone == 'person'):
        #if the object is a person table filters for the specific table, this is only be used to check if the table exists on piiDetailsWithStagingDF
        partitionKeysDF = (piiDetailsWithStagingDF.filter(col('PersonTable') == objectName).limit(1))
      else:
        #otherwise we don't have the specific data for the table, but we can filter by the staging table name, this is only used to check if the table exists on piiDetailsWithStagingDF
        partitionKeysDF = (piiDetailsWithStagingDF.filter(col('StagingTableName') == stagingTableName).limit(1))
      
      #if the table not exists, nothing to do, so set the where clause to 1=0
      if len(partitionKeysDF.head(1)) == 0: 
        partitionsWhereClauseStr = "1=0"
        tablesNonProcessedList.append(objectName)
      #else, the table needs to be processed but there are no partitions, so set the where clause to 1=1 and apply the remaining conditions
      else:
        partitionsWhereClauseStr = "1=1"
        tablesProcessedList.append(objectName)
    
    else:
      #Gets the list with the partitions for the table we are processing. Ex: MessageType, QuoteDate, QuoteHour
      partitionSchemeList = partitionScheme.split(',')

      if(zone == 'person'):
        #if the object is a person table filters for the specific table
        partitionKeysDF = (piiDetailsWithStagingDF.filter(col('PersonTable') == objectName)
                                                  .select(partitionSchemeList)
                                                  .distinct()
                           )
      else:
        #otherwise we don't have the specific data for the table, but we can filter by the staging table name
        partitionKeysDF = (piiDetailsWithStagingDF.filter(col('StagingTableName') == stagingTableName)
                                                  .select(partitionSchemeList)
                                                  .distinct()
                          )

      if len(partitionKeysDF.head(1)) == 0: 
        partitionsWhereClauseStr = "1=0"
        tablesNonProcessedList.append(objectName)
      else:
        #At this stage the dataframe partitionKeysDF will contain the partition colunmns and respective values
        #Ex:
        #MessageType     QuoteDate     QuoteHour
        #Response        2020-04-20    5
        #Request         2020-04-21    21

        #Getting the columns data types
        #This will get the partion columns datatye in a dictionary. Ex: {MessageType:string, QuoteDate:date, QuoteHour:tinynt}
        partitionDataType = dict(partitionKeysDF.dtypes)

        #Going for each column on the partition scheme list to set the column as: columnName = cast('columnValue' as datatype)
        #This loop transforms the partitionKeysDF in something like this:
        #MessageType                                QuoteDate                                QuoteHour
        #t.MessageType=cast('Response' as string)   t.QuoteDate=cast('2020-04-20' as date)   t.QuoteHour=cast('5' as tinyint)
        #t.MessageType=cast('Request' as string)    t.QuoteDate=cast('2020-04-21' as date)   t.QuoteHour=cast('21' as tinyint) 
        #Adds a "t." before each column. Ex: t.QuoteDate

        for scheme in partitionSchemeList:     
          partitionKeysDF = (partitionKeysDF.withColumn(scheme, concat_ws(' = ', concat(lit("t."),lit(scheme)), 
                                                                               concat(lit("cast('"), 
                                                                                      scheme, 
                                                                                      lit("' as "), 
                                                                                      lit(partitionDataType.get(scheme)), 
                                                                                      lit(")")))))
        #Creating and extra column that concatenates all columns separated by ' AND ' and encapsulates them with '()'
        #Ex:
        #Columns  PartititionKeys
        #...      (t.MessageType=cast('Response' as string) AND t.QuoteDate=cast('2020-04-20' as date) AND t.QuoteHour=cast('5' as tinyint))
        #...      (t.MessageType=cast('Request' as string) AND t.QuoteDate=cast('2020-04-21' as date) AND t.QuoteHour=cast('21' as tinyint))
        partitionKeysDF = (partitionKeysDF.withColumn('PartitionKeys', concat(lit('('), concat_ws(' AND ', *partitionSchemeList), lit(')'))))

        #Getting the created PartitionKeys columns list and joining the elements with ' OR '
        #Ex:
        #((t.MessageType=cast('Response' as string) AND t.QuoteDate=cast('2020-04-20' as date) AND t.QuoteHour=cast('5' as tinyint)) OR 
        #(t.MessageType=cast('Request' as string) AND t.QuoteDate=cast('2020-04-21' as date) AND t.QuoteHour=cast('21' as tinyint)))  
        partitionsWhereClause = partitionKeysDF.select('PartitionKeys').rdd.flatMap(lambda x:x).collect()

        partitionsWhereClauseStr = "(" + " OR ".join(map(str,partitionsWhereClause)) + ")"  

        tablesProcessedList.append(objectName)
    
    logTaskProgress(cursor,batchTaskId,'Successfully created the partitions where clause for the table {}'.format(objectName))

    return partitionsWhereClauseStr, tablesNonProcessedList, tablesProcessedList
         
  except Exception as e:
    errorMessage = 'Exception occurred while creating the partition where clause: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Function : Update the staging and the downstream objects to redacted
#Update all the downstream table and the staging tables to redacted, if the downstream is from person zone then delete the record
@add_method(gdprFunctions)
def redactPiiInDownstreamAndStaging(partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, downstreamTableName, keyColumnName, keyToRemoveStr, PIIHAToRemoveStr, tablesNonProcessedList, tablesProcessedList, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  try :
    refinedDF = downstreamTableName.filter((col('zone') == lit('sensitive')) | (col('zone') == lit('refined')))
    personDF = downstreamTableName.filter(col('zone') == lit('person'))
    
    #get the distinct refined and sensitive table names and collect to list
    refinedTableList = refinedDF.select("object_name").distinct().collect()
    
    #Iterate through the downstream tables, updating the refined and sensitive tables first
    for refinedTableItem in refinedTableList:
      objectName = refinedTableItem['object_name']
      
      #get the where clause for the table partitions
      partitionWhereClause, tablesNonProcessedList, tablesProcessedList = gdprFunctions.getPartitionWhereClause(objectName, partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, tablesNonProcessedList, tablesProcessedList, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
        
      #Apply the update
      spark.sql("UPDATE {} t SET PiiHash = -1, PiiTraceabilityHash = -1, PiiHashVersion = -1, LastUpdatedBatchID={}, LastUpdatedTimestamp=current_timestamp() WHERE {} AND {} IN ({})".format(objectName,batchId,partitionWhereClause,keyColumnName,keyToRemoveStr))
      
    #Get the pii columns to redact on the staging table
    piiFieldsToUpdate = gdprFunctions.getStagingtablePiiFieldsAsRedacted(stagingTableName, conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
    
    #Get where clause for the staging table partitions
    partitionWhereClause, tablesNonProcessedList, tablesProcessedList = gdprFunctions.getPartitionWhereClause(stagingTableName, partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, tablesNonProcessedList, tablesProcessedList, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
    
    #Update the staging table
    spark.sql("UPDATE {} t SET PiiHash = -1, PiiTraceabilityHash = -1, PiiHashVersion = -1, LastUpdatedBatchID={}, LastUpdatedTimestamp=current_timestamp(),{} WHERE {} AND {} IN ({})".format(stagingTableName,batchId,piiFieldsToUpdate,partitionWhereClause,keyColumnName,keyToRemoveStr))
    
    #get the distinct person table names and collect to list
    personTableList = personDF.select("object_name").distinct().collect()
    
    #Iterate through the downstream tables, deleting the pii hash values in the person tables
    for personTableItem in personTableList:
      objectName = personTableItem['object_name']

      #get the where clause for the table partitions
      partitionWhereClause, tablesNonProcessedList, tablesProcessedList = gdprFunctions.getPartitionWhereClause(objectName, partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, tablesNonProcessedList, tablesProcessedList, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
      #Apply the delete
      spark.sql("DELETE FROM {} t WHERE {} AND PiiTraceabilityHash IN ({})".format(objectName,partitionWhereClause,PIIHAToRemoveStr))
     
    return tablesNonProcessedList, tablesProcessedList
  
  except Exception as e:
    errorMessage = 'Exception occurred while updating the staging and the downstream objects to redacted: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Function : Apply the redaction to all the staging and dowstream tables
# function to apply redaction to all the staging tables and the related downstream tables
@add_method(gdprFunctions)
def applyRedaction(PIIHAToRemove, partitionSchemesDF, piiDetailsWithStagingDF, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  try:
      #get the distinct staging tables
      stagingTablesDF = piiDetailsWithStagingDF.select('StagingTableName').distinct()
      
      #Initialize list of processed and non processed tables
      tablesNonProcessedList = []
      tablesProcessedList = []
      
      #get the distinct staging table names and collect to list
      stagingTableList = stagingTablesDF.select("StagingTableName").distinct().collect()
    
      #for each staging table apply the redaction
      for stagingTableItem in stagingTableList:
        stagingTableName = stagingTableItem['StagingTableName']
        
        #Get the dowstream tables for staging table being processed
        downstreamTableName = gdprFunctions.getDownstreamObjects(stagingTableName, conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
        
        PIIHAToRemoveStr = "'"+"','".join(map(str,PIIHAToRemove))+"'"
        keyColumnName = 'PiiTraceabilityHash'
        keyToRemoveStr = PIIHAToRemoveStr
        
        #Update all the downstream table and the staging tables
        tablesNonProcessedList, tablesProcessedList = gdprFunctions.redactPiiInDownstreamAndStaging(partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, downstreamTableName, keyColumnName, keyToRemoveStr, PIIHAToRemoveStr, tablesNonProcessedList, tablesProcessedList, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
        logTaskProgress(cursor,batchTaskId,'Redaction successfully applied to staging table {} and downstream tables'.format(stagingTableName))
        
      return tablesNonProcessedList, tablesProcessedList
    
  except Exception as e:
    errorMessage = 'Exception occurred while applying redaction hashes: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Function : Main gdpr redaction hashes function
# main function to do the gdpr redaction hashes
@add_method(gdprFunctions)
def gdprRedactionHashes(pii_values):
  try:
    #Initialise variables
    scheduleReference = 'GDPR_Redaction'
    adfPipelineName = 'Manual Notebook Execution'
    notebookName = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    clusterId = dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().get()
    errorLogFileLocation = ''
    cursor = ''
    #variable CURRENT_TIME
    currentTs = datetime.now() 
    #get the last 3 positions of the microseconds as we need to remove them
    currentTsMicroseconds = int(str(currentTs.strftime('%f'))[3:6]) 
    #take away the last three microseconds
    date = currentTs - timedelta(microseconds=currentTsMicroseconds)
    
    #Check if the inserted Pii Hashes are valid
    piiHashAttributesDF, isValid, errorMessage = gdprFunctions.validatePIIList(pii_values, errorLogFileLocation)
  
    if isValid == True:
      #Establish the SQL database connection
      dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
      conn = pyodbc.connect(dbconn, autocommit = True)
      cursor = conn.cursor()
      
      #Star a batch and validate that is running
      batchId, batchTaskId, isValid = gdprFunctions.batchStartAndValidation(conn, cursor, errorLogFileLocation, scheduleReference)
      
      if isValid == True:
        #call the GET_LOGGING_PATH function to create a log file path as a string and store it in a variable 
        errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')
      
        #call usp_log_task_start store procedure to update the batchTaskId to running
        cursor.execute("exec audit.usp_log_task_start ?",batchTaskId)
        while cursor.nextset():
          x = 1
        
        logTaskProgress(cursor,batchTaskId,'Successfully created the batch {} and batchTask {}'.format(batchId,batchTaskId))
        #broadcast the dataframe with the pii hashes
        broadcastPiiHashAttributesDF = broadcast(piiHashAttributesDF)
    
        #Get the partition schemes
        partitionSchemesDF = gdprFunctions.getPartitionSchemes(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
        #Get the distinct sources on the partition scheme
        sourcesDF = partitionSchemesDF.select('source_name').distinct()
    
        #Iterate through the sources, since the columns in the person tabes will be different for each source
        for source in range(0,sourcesDF.count()):
          sourceDic = sourcesDF.collect()[source].asDict()
          sourceName = sourceDic['source_name']
      
          #Filter partitionSchemesDF to get only the persons tables for the source we are dealing with
          personsTablesNames = partitionSchemesDF.filter((col('zone') == lit('person')) & (col('source_name') == sourceName))
          
          #We don't need pii columns since it's a redaction
          addPiiColumns = False
          #Get the staging tables related to the persons tables by calling getPiiDetailsWithStagingDF
          piiDetailsWithStagingDF = gdprFunctions.getPiiDetailsWithStagingDF(personsTablesNames, broadcastPiiHashAttributesDF, addPiiColumns, conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
          #Check if there are no rows, meaning the inserted Pii hashes aren't found anywhere
          if len(piiDetailsWithStagingDF.head(1)) == 0:
            print("The inserted PPI Hashes don't exist in any Person table for source {}.".format(sourceName))
   
          #If we are here means there are hashes to redact
          else:    
            #Get the string list of Pii hashes to redact
            PIIHAToRemove = gdprFunctions.getPIIHAToRemove(piiDetailsWithStagingDF, broadcastPiiHashAttributesDF, sourceName, conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
            
            #Apply redaction to the staging tables and related downstream tables based on the pii hashes
            tablesNonProcessedList, tablesProcessedList = gdprFunctions.applyRedaction(PIIHAToRemove, partitionSchemesDF, piiDetailsWithStagingDF, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
            
            logTaskProgress(cursor,batchTaskId,'Successfully scanned the following tables {} using the PiiTraceabilityHashes {} for the source {}. Where PiiTraceabilityHashes {} matched, the data has been redacted.'.format(tablesProcessedList, PIIHAToRemove, sourceName, PIIHAToRemove))
            
            #Delete the temporary tables  
            dbutils.fs.rm("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingTable" , True);
            dbutils.fs.rm("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingWithoutDuplicatesTable" , True);  
        
        #call usp_batch_complete store procedure to complete the batch
        cursor.execute("exec audit.usp_log_task_end  ?,?,?,?,?,?,?",batchTaskId,'completed','','','','','')
        while cursor.nextset():
          x = 1
      
        #call usp_batch_complete store procedure to complete the batch 
        cursor.execute('exec audit.usp_batch_complete ?',batchId)
        while cursor.nextset():
          x = 1
          
        #call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
        taskEndAndCloseConn(cursor,conn,batchTaskId,'','','', '','',adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      
        print("Redaction process concluded with success!")
      
      #There is another batch running
      else:
        print("Don't run the batch now because there is another batch running. Please contact the EDS team")
  
    #The inserted Pii hashes are not valid
    else:
      print(errorMessage)
    
  except Exception as e:
    errorMessage = 'Exception occurred while runnning gdpr redaction hashes: ' + str(e)
    if(errorLogFileLocation != '' and cursor != ''):
      logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      taskEndAndCloseConn(cursor,conn,batchTaskId,'','','', '','',adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function : Execute usp_get_all_pii_fields stored procedure
# Gets the list of ppi fields and the respective types
@add_method(gdprFunctions)
def getPiiFields():
  try:
    #Establish the SQL database connection
    dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
    conn = pyodbc.connect(dbconn, autocommit = True)
    cursor = conn.cursor()
    
    #Get the pii fields from the database
    allPiiFields = pd.read_sql_query('exec config.usp_get_all_pii_fields', conn)
    
    allPiiFieldsDF = spark.createDataFrame(allPiiFields)
    
    #Transform the dataframe into a list
    allPiiFieldsList = allPiiFieldsDF.select('hash_attribute_name').rdd.flatMap(lambda x:x).collect()
    print("List of pii fields where rectification can be applied: {}".format(allPiiFieldsList))
   
    return conn, cursor, allPiiFieldsDF
  except Exception as e:
    errorMessage = 'Exception occurred while executing the usp_get_all_pii_fields stored procedure: ' + str(e)   
    print(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function: Validates inserted Pii fields
# Validates the inserted pii fields
@add_method(gdprFunctions)
def validatePersonalPiiFields(personal_values, allPiiFieldsDF):
  try:
   
    #Get the inputed ppi fields list
    #Ex: If the inputed piis are {'LastName':'x', 'FirstName':'y'} it returns ['LastName', 'FirstName']
    inputedPiiList  = personal_values.keys()
    
    #Get the list of all existent pii fields in the metadata (for persons)
    #Ex: ['Age', 'Address', 'LastName', 'FirstName']
    piiFieldsList = allPiiFieldsDF.select('hash_attribute_name').rdd.flatMap(lambda x:x).collect()
    
    #Get the list with non existent inserted Pii fields but doing the difference between all the fields and the inserted ones
    #Ex: If the inserted list is ['LastName', 'SomeField'] and the piiFields list is ['Age', 'Address', 'LastName', 'FirstName'] it will
    #return ['SomeField'] since it doesnt exist in the piiFieldList
    notFoundFieldsList = list(set(inputedPiiList).difference(piiFieldsList))
    
    #If some of the inserted fields don't exist
    if(len(notFoundFieldsList) > 0):
      print("The following inserted Pii fields don't exist on the database: {}. Please insert only valid Pii fields.".format(notFoundFieldsList))
      return None, None, False, errorMessage
      
    #Build dataframe with the inserted fields
    else:  
      #Filter the list with all the Pii fields using the inputed ones
      #Ex: If the inputed Pii list is ['LastName', 'FirstName'] the piiFieldsDF will look like this
      # object_attribute_name   object_attribute_type
      # LastName                STRING
      # FirstName               STRING
      piiFieldsDF = allPiiFieldsDF.filter(col('hash_attribute_name').isin(*inputedPiiList))
      
      #Convert to pandas dataframe to map with the personal values dictionary with the inserted pii fields to add a column with the value
      #Ex: If the inputed values are {'LastName':'x', 'FirstName':'y'} the pandaspiiFieldsDF will be:
      # object_attribute_name   object_attribute_type    value
      # LastName                STRING                   x
      # FirstName               STRING                   y
      # Age                     INT                      30
      pandaspiiFieldsDF = piiFieldsDF.select("*").toPandas()
      pandaspiiFieldsDF['value'] = pandaspiiFieldsDF['hash_attribute_name'].map(personal_values)
      
      #Add the casts to the values and transform the attribute types to be used as a spark dataframe schema
      #Ex: Taking in account the previous example piiFieldsDF will be like:
      # object_attribute_name   object_attribute_type    value
      # LastName                StringType()             str('x')
      # FirstName               StringType()             str('y')
      # Age                     IntegerType()            int('30')
      piiFieldsDF = spark.createDataFrame(pandaspiiFieldsDF)
      piiFieldsDF = (piiFieldsDF.withColumn('value', 
                         when(col('object_attribute_type')==lit('BIGINT'), concat(lit("long('"),col('value'),lit("')")))
                        .when(col('object_attribute_type')==lit('DATE'), concat(lit("to_date('"),col('value'),lit("')")))
                        .when(col('object_attribute_type').contains(lit('DECIMAL')), concat(lit("Decimal('"),col('value'),lit("')")))
                        .when(col('object_attribute_type')==lit('INT'), concat(lit("int('"),col('value'),lit("')")))
                        .when(col('object_attribute_type')==lit('TIMESTAMP'), concat(lit("to_timestamp('"),col('value'),lit("')")))
                        .otherwise(concat(lit("str('"),col('value'),lit("')"))))
                                    
                        .withColumn('object_attribute_type', 
                                     when(col('object_attribute_type')==lit('BIGINT'), lit('LongType()'))
                                    .when(col('object_attribute_type')==lit('DATE'), lit('DateType()'))
                                    .when(col('object_attribute_type').contains(lit('DECIMAL')), lit('DecimalType()'))
                                    .when(col('object_attribute_type')==lit('INT'), lit('IntegerType()'))
                                    .when(col('object_attribute_type')==lit('TIMESTAMP'), lit('TimestampType()'))
                                    .otherwise(lit('StringType()'))))

      #Create the fields and schema to validate the dataframe
      attributeNamesTypes = piiFieldsDF.select('hash_attribute_type_code', 'object_attribute_type')
      #Create a list with the fields
      #Ex: [StructField(LastName, StringType(), True),  StructField(FirstName, StringType(), True)]
      fields = [StructField(aName, eval(aType), True) for aName, aType in attributeNamesTypes.rdd.collect()]
      #Create a schema with the previous fields
      #Ex: StructType(StructField(LastName, StringType(), True),  StructField(FirstName, StringType(), True))
      schema = StructType(fields)
    
      #Create the dataframe with the previous schema
      attributeNamesValues = piiFieldsDF.select('hash_attribute_type_code', 'value')
      #Create a dictionary with the inputed values casted to the respective data type
      #Ex:
      #{'LastName':eval(str('x')), 'FirstName':eval(str('y'))} 
      inputDic = { aName : eval(aValue) for aName, aValue in attributeNamesValues.rdd.collect()}
      #Try to create a dataframe with the dictionary and the created schema
      #Ex:  
      # LastName   FirstName
      # x          y
      insertedPersonalValuesDF = spark.createDataFrame([inputDic], schema)
      
      return inputDic, insertedPersonalValuesDF, True, ''
      
  except Exception as e:
    errorMessage = 'Exception occurred while validating inserted pii values to rectify: ' + str(e)
    return None, None, False, errorMessage

# COMMAND ----------

# DBTITLE 1,Function: Execute usp_get_table_pii_columns stored procedure 
# Get the pii columns for the specified table using the usp_get_table_pii_columns stored procedure 
@add_method(gdprFunctions)
def getPiiColumns(personTableName,conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    piiColumns = pd.read_sql_query('exec config.usp_get_table_pii_columns {}'.format(personTableName),conn)
    
    piiColumnsDF = spark.createDataFrame(piiColumns)
    
    #Get the list of pii hash to redact and convert the list to string
    PIIColumnsList = piiColumnsDF.select('object_attribute_name').rdd.flatMap(lambda x:x).collect()
    PIIColumnsStr = "," + ",".join(map(str,PIIColumnsList))
    
    logTaskProgress(cursor,batchTaskId,'Executed stored procedure usp_get_table_pii_columns for the table' + personTableName)
    return PIIColumnsStr
  
  except Exception as e:
    errorMessage = 'Exception occurred while executing the usp_get_table_pii_columns stored procedure, table {}. {}'.format(personTableName, str(e))
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    sys.exit(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function: Get Pii hashes variables
#Gets the pii hashes variables to apply the hashes
@add_method(gdprFunctions)
def getPiiHashesVariables(sourceObjectId, sourceId, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  try:
      #Execute stored procedure config.usp_get_object_pii_hash_details to get the object pii hash details
      objectPiiHashDetailsDf=pd.read_sql_query("exec config.usp_get_object_pii_hash_details_target_mapped {0},{1}".format(sourceObjectId,sourceId),conn)
      logTaskProgress(cursor,batchTaskId,"Executed get pii hash details store procedure")
      
      #Gets the pii hashes variables
      PiiHashVersion,PiiHashColumnFunctionList,PiiHashTraceabilityColumnFunctionList=piiGetHashVariables(objectPiiHashDetailsDf
                                                                                                                ,batchId
                                                                                                                ,cursor
                                                                                                                ,batchTaskId
                                                                                                                ,adfPipelineName
                                                                                                                ,clusterId
                                                                                                                ,notebookName
                                                                                                                ,errorLogFileLocation)
      
      logTaskProgress(cursor,batchTaskId,"Pii hash variables initialized for object id {}, source {}".format(sourceObjectId,sourceId))
      return PiiHashVersion,PiiHashColumnFunctionList,PiiHashTraceabilityColumnFunctionList
    
  except Exception as e:
    errorMessage="Unable to initialize the pii hashes variables: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    sys.exit(errorMessage)
      

# COMMAND ----------

# DBTITLE 1,Function: Sets the new pii hashes and the last updated batch id
# Function to set the new pii hashes and the last updated batch id
@add_method(gdprFunctions)
def setNewPiiHashes(personsTablesNames, piiDetailsWithStagingDF, insertedPersonalValuesDic, partitionSchemesDF, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  try:
    
    #Upates the pii fields in the dataframe
    #For each column inserted by the user
    #Ex: {'LastName':'X', 'FirstName':'A'}
    updatedPersonalValuesDic = copy.deepcopy(insertedPersonalValuesDic)
    #For the example it will pick up LastName, FirstName, Age
    for column in insertedPersonalValuesDic.keys():
      #If the dataframe contains the column 
      if(column in piiDetailsWithStagingDF.columns):
        #Update the column with the value the user inserted
        #Ex:
        #LastName    FirstName
        #X           A
        piiDetailsWithStagingDF = (piiDetailsWithStagingDF.withColumn(column, lit(insertedPersonalValuesDic.get(column))))
      else:
        #If the dataframe doesn't contain the column, remove it from the updated dictionary
        #Ex: If age doesn't exist on the dataframe, it will be removed from the updated dictionary
        #Ex: {'LastName':'X', 'FirstName':'A'}
        updatedPersonalValuesDic.pop(column)
    
    #Clean the table location before writing
    dbutils.fs.rm("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingWithNewHashes" , True);
    
    #Get the details of the person table
    personTableNameDic = personsTablesNames.collect()[0].asDict()
    sourceId = personTableNameDic['source_id']
  
    stagingTablesDF = piiDetailsWithStagingDF.select('StagingTableName').distinct()
    
    #get the distinct staging table names and collect to list
    stagingTableList = stagingTablesDF.collect()
    
    #for each staging table apply the redaction
    for stagingTableItem in stagingTableList:
      stagingTableName = stagingTableItem['StagingTableName']
      sourceObjectId = partitionSchemesDF.filter(col('object_name') == stagingTableName).collect()[0].asDict()['object_id']
      
      #Get the pii hashes variables
      PiiHashVersion,PiiHashColumnFunctionList,PiiHashTraceabilityColumnFunctionList = gdprFunctions.getPiiHashesVariables(sourceObjectId, sourceId, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
      
      #Set the new pii hashes and the new last updated batch id by adding new columns
      piiDetailsWithNewHashesDF = (
                          piiDetailsWithStagingDF
                          .filter(col('StagingTableName') == stagingTableName)
                          .withColumn('NewPiiHashVersion', lit(int(PiiHashVersion)))
                          .withColumn('NewPiiHash', computeHashValueUdf(concatenateFunc(PiiHashColumnFunctionList)))
                          .withColumn('NewPiiTraceabilityHash', computeHashValueUdf(concatenateFunc(PiiHashTraceabilityColumnFunctionList)))
                          .withColumn('NewLastUpdatedBatchID', lit(batchId)))
      
      #Write the table with the new hashes in a temporary table
      (piiDetailsWithNewHashesDF.write
                              .format("delta")
                              .mode("append")
                              .save("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingWithNewHashes"))
    
   
    #Reload dataframe to avoit any lazy evaluations because it is the one we will use going forward
    piiDetailsWithNewHashesDF = (spark.read
                                      .format('delta')
                                      .load("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingWithNewHashes"))
    
    logTaskProgress(cursor,batchTaskId,'New pii hashes and last updated batch id added')
    #return the pii details with the new hashes
    return  piiDetailsWithNewHashesDF, updatedPersonalValuesDic
  
  except Exception as e:
    errorMessage = 'Exception occurred while adding the new pii hashes and last updated batch id: ' + str(e)
    #logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    sys.exit(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function: Get the string list of pii fields to update
#Gets a string list of the pii fields to use in the update clause
@add_method(gdprFunctions)
def getPiiFieldsToRectify(insertedPersonalValuesDic, insertedPersonalValuesDF, tableName, conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  try :
    
    #Get the mappings between the hash attribute type code and the attribute name by calling the usp_get_mapping_object_attribute_names stored procedure
    mappedPiis = pd.read_sql_query('exec config.usp_get_mapping_object_attribute_names {}'.format(tableName),conn)
    mappedPiisDF = spark.createDataFrame(mappedPiis)
    #Filter the previous dataframe to get only the fields the user inserted
    mappedFieldsToUpdateDF = (mappedPiisDF.select('object_attribute_name', 'hash_attribute_type_code')
                                          .filter(col('hash_attribute_type_code').isin(*insertedPersonalValuesDic.keys())))
    
    if len(mappedFieldsToUpdateDF.head(1)) == 0:
      piiFieldsStr=""
    
    else:
      mappedFieldsToUpdateList = mappedFieldsToUpdateDF.select('hash_attribute_type_code').rdd.flatMap(lambda x:x).collect()
    
      #Get a dataframe with only the inserted pii values
      piiFieldsDF = (insertedPersonalValuesDF.select(*mappedFieldsToUpdateList))
    
      #At this stage the dataframe piiFieldsDF will contain the inserted pii colunmns and respective values
      #Ex:
      #LNAME     FNAME     
      #XYZ       ABC           
 
      #Getting the columns data types
      #This will get the partion columns datatye in a dictionary. Ex: {LNAME:string, FNAME:string}
      piiFieldsDataTypes = dict(piiFieldsDF.dtypes)
    
      #Going for each field in piiFieldsDataTypes to set the column as: columnName = cast('columnValue' as datatype)
      #This for loop transforms the piiFieldsDF in something like this:
      #LNAME                          FNAME                       
      #LNAME=cast('XYZ' as string)    FNAME=cast('ABC' as string)
      for piiField in piiFieldsDataTypes:     
        piiFieldsDF = (piiFieldsDF.withColumn(piiField, concat_ws(' = ',  lit(piiField), 
                                                                        concat(lit("cast('"), 
                                                                               piiField, 
                                                                               lit("' as "), 
                                                                               lit(piiFieldsDataTypes.get(piiField)), 
                                                                               lit(")")))))
      
      #Creating and extra column 'PiiFields' that concatenates all columns with ',' 
      #Ex:
      #Columns  PiiFields
      #...      (LNAME=cast('XYZ' as string),FNAME=cast('ABC' as string))
      piiFieldsDF = (piiFieldsDF.withColumn('PiiFields', concat_ws(',', *piiFieldsDF.columns)))

      #Getting the column 'PiiFields' as list, separated by commas
      piiFieldsList = piiFieldsDF.select('PiiFields').rdd.flatMap(lambda x:x).collect()
      piiFieldsStr = ",".join(map(str,piiFieldsList))
    
      #Replacing the hash attribute type code by the attribute name 
      #Ex: If piiFieldsStr is: LNAME = cast('X' as string),FNAME=cast('Y' as string)
      # It will replace LNAME for LastName and FNAME for FirstName, getting: LastName = cast('X' as string), FirstName=cast('Y' as string)
      for objectAttName, hashAttCode in mappedFieldsToUpdateDF.rdd.collect():
        piiFieldsStr = piiFieldsStr.replace(hashAttCode+" = ", objectAttName+" = ")
      
    logTaskProgress(cursor,batchTaskId,'Create the list of pii fields to use in the update')
    
    return piiFieldsStr
  
  except Exception as e:
    errorMessage = 'Exception creating the list of pii fields to use in the update: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    sys.exit(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function : Rectifies the staging and downstream objects
#Rectifies all the downstream and staging tables
@add_method(gdprFunctions)
def rectifyPiiInDownstreamAndStaging(partitionSchemesDF, ppiDetailsWithNewHashesDF, stagingTableName, downstreamTableName, constantFieldsToUpdate, insertedPersonalValuesDic, insertedPersonalValuesDF, tablesNonProcessedList, tablesProcessedList, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  #Update all the downstream tables and the staging table
  try :
    refinedDF = downstreamTableName.filter((col('zone') == lit('sensitive')) | (col('zone') == lit('refined')))
    personDF = downstreamTableName.filter(col('zone') == lit('person'))
      
    #get the distinct refined and sensitive table names and collect to list
    refinedTableList = refinedDF.select("object_name").distinct().collect()
    
    #Iterate through the downstream tables
    for refinedTableItem in refinedTableList:
      objectName = refinedTableItem['object_name']
      
      #Get the where clause for the table partitions
      partitionWhereClause, tablesNonProcessedList, tablesProcessedList = gdprFunctions.getPartitionWhereClause(objectName, partitionSchemesDF, ppiDetailsWithNewHashesDF, stagingTableName, tablesNonProcessedList, tablesProcessedList, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
      spark.sql("MERGE INTO {} t USING to_update s ON {} AND t.PiiTraceabilityHash = s.PiiTraceabilityHash AND t.PiiTraceabilityHash <> s.NewPiiTraceabilityHash WHEN MATCHED THEN UPDATE SET {}".format(objectName, partitionWhereClause, constantFieldsToUpdate))
    
    #Get the mapped pii columns to rectify on the staging table
    stagingPiiFieldsToUpdate = gdprFunctions.getPiiFieldsToRectify(insertedPersonalValuesDic, insertedPersonalValuesDF, stagingTableName, conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
    stagingPiiFieldsToUpdate = stagingPiiFieldsToUpdate + ',' if len(stagingPiiFieldsToUpdate)>0 else stagingPiiFieldsToUpdate
    
    #Get where clause for the staging table partitions
    partitionWhereClause, tablesNonProcessedList, tablesProcessedList = gdprFunctions.getPartitionWhereClause(stagingTableName, partitionSchemesDF, ppiDetailsWithNewHashesDF, stagingTableName, tablesNonProcessedList, tablesProcessedList, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
    
    #Update the staging table
    spark.sql("MERGE INTO {} t USING to_update s ON {} AND t.PiiTraceabilityHash = s.PiiTraceabilityHash AND t.PiiTraceabilityHash <> s.NewPiiTraceabilityHash WHEN MATCHED THEN UPDATE SET {}{}".format(stagingTableName, partitionWhereClause, stagingPiiFieldsToUpdate, constantFieldsToUpdate))
    
    #get the distinct person table names and collect to list
    personTableList = personDF.select("object_name").distinct().collect()
    
    #Iterate through the person downstream tables
    for personTableItem in personTableList:
      objectName = personTableItem['object_name']
      
      #Get the pii columns to update in the persons table
      piiFieldsToUpdate = gdprFunctions.getPiiFieldsToRectify(insertedPersonalValuesDic, insertedPersonalValuesDF, objectName, conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
      piiFieldsToUpdate = piiFieldsToUpdate + ',' if len(piiFieldsToUpdate)>0 else piiFieldsToUpdate

      #Get the where clause for the table partitions
      partitionWhereClause, tablesNonProcessedList, tablesProcessedList = gdprFunctions.getPartitionWhereClause(objectName, partitionSchemesDF, ppiDetailsWithNewHashesDF, stagingTableName, tablesNonProcessedList, tablesProcessedList, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
      #Update the persons table
      spark.sql("MERGE INTO {} t USING to_update s ON {} AND t.PiiTraceabilityHash = s.PiiTraceabilityHash WHEN MATCHED THEN UPDATE SET {}{}".format(objectName, partitionWhereClause, piiFieldsToUpdate, constantFieldsToUpdate))
      
    return tablesNonProcessedList, tablesProcessedList
  
  except Exception as e:
    errorMessage = 'Exception occurred while rectifying the staging and the downstream objects: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    sys.exit(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function : Applies the rectification to the person, staging and downstream tables
# function to apply rectification to the person, staging and related downstream tables
@add_method(gdprFunctions)
def applyRectification(partitionSchemesDF, ppiDetailsWithNewHashesDF, insertedPersonalValuesDic, insertedPersonalValuesDF, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  try:  
    #get the distinct staging tables
    stagingTablesDF = ppiDetailsWithNewHashesDF.select('StagingTableName').distinct()
      
    #variable CURRENT_TIME
    currentTs = datetime.now() 
    #get the last 3 positions of the microseconds as we need to remove them
    currentTsMicroseconds = int(str(currentTs.strftime('%f'))[3:6]) 
    #take away the last three microseconds
    date = currentTs - timedelta(microseconds=currentTsMicroseconds)
    
    #set the constant fields to update
    constantFieldsToUpdate = "t.PiiHash=s.NewPiiHash, t.PiiTraceabilityHash=s.NewPiiTraceabilityHash, t.PiiHashVersion=s.NewPiiHashVersion, t.LastUpdatedBatchID=s.NewLastUpdatedBatchID, t.LastUpdatedTimestamp=cast('{}' as timestamp)".format(date)
    
    #Set the columns to be used on the update
    columnsToSelect = ['PiiTraceabilityHash', 'NewPiiHash', 'NewPiiTraceabilityHash', 'NewPiiHashVersion', 'NewLastUpdatedBatchID']
    sourceDF = ppiDetailsWithNewHashesDF.select(columnsToSelect).distinct()
    #Create a temporary table with the previous columns
    sourceDF.createOrReplaceTempView("to_update")
    
    #Initialize list of processed and non processed tables
    tablesNonProcessedList = []
    tablesProcessedList = []
    
    #get the distinct staging table names and collect to list
    stagingTableList = stagingTablesDF.select("StagingTableName").distinct().collect()
    
    #for each staging table apply the redaction
    for stagingTableItem in stagingTableList:
      stagingTableName = stagingTableItem['StagingTableName']
        
      #Get the dowstream tables for staging table being processed
      downstreamTableName = gdprFunctions.getDownstreamObjects(stagingTableName, conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
      
      #Update all the downstream tables and the staging table
      gdprFunctions.rectifyPiiInDownstreamAndStaging(partitionSchemesDF, ppiDetailsWithNewHashesDF, stagingTableName, downstreamTableName, constantFieldsToUpdate, insertedPersonalValuesDic, insertedPersonalValuesDF, tablesNonProcessedList, tablesProcessedList, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
                                                    
      logTaskProgress(cursor,batchTaskId,'Rectification successfully applied to staging table {} and downstream tables'.format(stagingTableName))
      
    return tablesNonProcessedList, tablesProcessedList
      
  except Exception as e:
    errorMessage = 'Exception occurred while applying rectification: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    sys.exit(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function : Main gdpr rectification function
# main function to do the gdpr rectification
@add_method(gdprFunctions)
def gdprRectification(personal_values, allPiiFieldsDF, pii_values, conn, cursor):
  try:
    #Initialise variables
    scheduleReference = 'GDPR_Rectification'
    adfPipelineName = 'Manual Notebook Execution'
    notebookName = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    clusterId = dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().get()
    errorLogFileLocation = ''
    #variable CURRENT_TIME
    currentTs = datetime.now() 
    #get the last 3 positions of the microseconds as we need to remove them
    currentTsMicroseconds = int(str(currentTs.strftime('%f'))[3:6]) 
    #take away the last three microseconds
    date = currentTs - timedelta(microseconds=currentTsMicroseconds)
    
    #Check if the inserted personal Pii fields are valid
    insertedPersonalValuesDic, insertedPersonalValuesDF, isValid, errorMessage = gdprFunctions.validatePersonalPiiFields(personal_values, allPiiFieldsDF)
    
    if isValid == True:
      #Check if the inserted Pii Hashes are valid
      piiHashAttributesDF, isValid, errorMessage = gdprFunctions.validatePIIList(pii_values, errorLogFileLocation)
  
      if isValid == True:
        #Star a batch and validate that is running
        batchId, batchTaskId, isValid = gdprFunctions.batchStartAndValidation(conn, cursor, errorLogFileLocation, scheduleReference)
      
        if isValid == True:
          #call getLoggingPath function to create a log file path as a string and store it in a variable 
          errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')
      
          #call usp_log_task_start store procedure to update the batchTaskId to running
          cursor.execute("exec audit.usp_log_task_start ?",batchTaskId)
          while cursor.nextset():
            x = 1
        
          logTaskProgress(cursor,batchTaskId,'Successfully created the batch {} and batchTask {}'.format(batchId,batchTaskId))
          
          #broadcast the dataframe with the pii hashes
          broadcastPiiHashAttributesDF = broadcast(piiHashAttributesDF)
          
          #Get the partition schemes
          partitionSchemesDF = gdprFunctions.getPartitionSchemes(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
          #Get the distinct sources on the partition scheme and collect to list
          sourcesList = partitionSchemesDF.select('source_name').distinct().collect()
          
          #Register the computeHash function
          computeHashValueUdfRegistration()

          #Iterate through the sources, since the columns in the person tables will be different for each source
          for sourceItem in sourcesList:
            sourceName = sourceItem['source_name']
      
            #Filter partitionSchemesDF to get only the persons tables for the source we are dealing with
            personsTablesNames = partitionSchemesDF.filter((col('zone') == lit('person')) & (col('source_name') == sourceName))
            
            #We need pii columns since it's a rectification
            addPiiColumns = True
            #Get the staging tables related to the persons tables by calling getPiiDetailsWithStagingDF
            piiDetailsWithStagingDF = gdprFunctions.getPiiDetailsWithStagingDF(personsTablesNames, broadcastPiiHashAttributesDF, addPiiColumns, conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
              
            #Check if there are no rows, meaning the inserted Pii hashes aren't found anywhere
            if len(piiDetailsWithStagingDF.head(1)) == 0:
              print("The inserted PPI Hashes don't exist in any Person table for source {}.".format(sourceName))
            
            #If we are here means there are hashes to rectify
            else:    
              #Set new pii hashes
              ppiDetailsWithNewHashesDF, updatedPersonalValuesDic = gdprFunctions.setNewPiiHashes(personsTablesNames, piiDetailsWithStagingDF, insertedPersonalValuesDic, partitionSchemesDF, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
        
              #Apply the rectification to the person, staging and related downstream tables
              tablesNonProcessedList, tablesProcessedList = gdprFunctions.applyRectification(partitionSchemesDF, ppiDetailsWithNewHashesDF, updatedPersonalValuesDic, insertedPersonalValuesDF, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
            
              PIIHAToRectify = ppiDetailsWithNewHashesDF.select('PiiTraceabilityHash').distinct().rdd.flatMap(lambda x:x).collect()
              
              logTaskProgress(cursor,batchTaskId,'Successfully scanned the following tables {} using the PiiTraceabilityHashes {} for the source {}. Where PiiTraceabilityHashes {} matched, the data has been rectified.'.format(tablesProcessedList, PIIHAToRectify, sourceName, PIIHAToRectify))
          
              #Delete the temporary tables  
              dbutils.fs.rm("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingTable" , True);
              dbutils.fs.rm("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingWithoutDuplicatesTable" , True); 
         
          #call usp_batch_complete store procedure to complete the batch
          cursor.execute("exec audit.usp_log_task_end  ?,?,?,?,?,?,?",batchTaskId,'completed','','','','','')
          while cursor.nextset():
            x = 1
      
          #call usp_batch_complete store procedure to complete the batch 
          cursor.execute('exec audit.usp_batch_complete ?',batchId)
          while cursor.nextset():
            x = 1
          
          #call task_end_and_close_conn function to close the database connection and mark the end of task in batch_task_table
          taskEndAndCloseConn(cursor,conn,batchTaskId,'','','', '','',adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      
          print("Rectification process concluded with success!")
      
        #There is another batch running
        else:
          print("Don't run the batch now because there is another batch running. Please contact the EDS team")
  
      #The inserted Pii hashes are not valid
      else:
        print(errorMessage)
        
    #The inserted personal pii to rectify are not valid
    else:
      print(errorMessage)
      
  except Exception as e:
    errorMessage = 'Exception occurred while runnning gdpr redaction hashes: ' + str(e)
    if(errorLogFileLocation != '' and cursor != ''):
      logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      taskEndAndCloseConn(cursor,conn,batchTaskId,'','','', '','',adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function: Execute usp_get_gdpr_retention_reference_tables stored procedure 
# get reference tables for retention
@add_method(gdprFunctions)
def getRetentionReferenceTable(objectName,conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    referenceTablesPD = pd.read_sql_query("exec [config].[usp_get_gdpr_retention_reference_tables] {}".format(objectName),conn)
    
    schema = StructType([StructField("object_name"          , StringType() , True)
                        ,StructField("object_attribute_name", StringType() , True)
                        ,StructField("where_clause"         , StringType() , True)
                        ,StructField("reference_object_name", StringType() , True)
                     ])
    
    referenceTablesDF = spark.createDataFrame(referenceTablesPD, schema)
    
    logTaskProgress(cursor,batchTaskId,'Succesfully executed the usp_get_gdpr_retention_reference_tables stored procedure')
    return referenceTablesDF
  except Exception as e:
    errorMessage = 'Exception occurred while executing usp_get_gdpr_retention_reference_tables stored procedure: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: To get the reference retention keys
# get reference tables for retention
@add_method(gdprFunctions)
def getRetentionKeys(referenceTablesDF,conn,cursor,batchId,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    referenceTablesDic = referenceTablesDF.collect()[0].asDict()
    refTable = referenceTablesDic['reference_object_name']
    refColumn = referenceTablesDic['object_attribute_name']
    refWhereClause = referenceTablesDic['where_clause']
    refTableProcessed = referenceTablesDic['object_name']
    
    #Get the source ids for the reference redaction state table
    sourcesPd = pd.read_sql_query("exec [config].[usp_gdpr_get_sources] {}".format(refTableProcessed), conn)
    
    schema = StructType([StructField("SourceID", IntegerType() , True)])
    sourcesDF = spark.createDataFrame(sourcesPd, schema)
    currentTsRedactionState = datetime.now()
    #get the last 3 positions of the microseconds as we need to remove them
    currentTsRedactionStateMicroseconds = int(str(currentTsRedactionState.strftime('%f'))[3:6])
    #take away the last three microseconds
    currentTsRedactionState = currentTsRedactionState - timedelta(microseconds=currentTsRedactionStateMicroseconds) 
    #Apply the join between the reference and reference redaction state table to get the non processed rows
    toProcessDF = (spark.sql("SELECT /*+ BROADCAST({}) */ r.*, CAST(NULL AS Timestamp) AS RedactedTimestamp, false AS Processed FROM {} r LEFT JOIN {} rp ON r.{}=rp.{} WHERE rp.{} IS NULL AND {}".format(refTable, refTable, refTableProcessed, refColumn, refColumn, refColumn, refWhereClause))
                   .withColumn('CreatedBatchID',lit(int(batchId)))
                   .withColumn('CreatedTimestamp',lit(currentTsRedactionState))
                   .withColumn('LastUpdatedTimestamp', lit(currentTsRedactionState))
                   .withColumn('LastUpdatedBatchID',lit(int(batchId))))
    
    #Add the SourceID
    toProcessDF=toProcessDF.crossJoin(sourcesDF)
     
    #Write into the reference redaction state table
    toProcessDF.write.format("delta").mode("append").saveAsTable(refTableProcessed)

    logTaskProgress(cursor,batchTaskId,'Succesfully executed the getRetentionKeys function')
    
  except Exception as e:
    errorMessage = 'Exception occurred while executing getRetentionKeys function: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: To apply the retention redaction hashes
# function to do the gdpr retention redaction hashes
@add_method(gdprFunctions)
def gdprRetentionRedactionHashes(piiHashAttributesDF,sourceId,conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    piiHashAttributesDF = piiHashAttributesDF.select('PiiTraceabilityHash', 'JoinKey', 'InUse', 'StagingTableName').withColumn('pii_traceability_hash', col('PiiTraceabilityHash')).drop('PiiTraceabilityHash')
    
    #Get the partition schemes
    partitionSchemesDF = gdprFunctions.getPartitionSchemes(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
    #Get the distinct sources on the partition schemes
    sourcesDF = partitionSchemesDF.select('source_name').filter(col('source_id') == sourceId).distinct()
    
    #Iterate through the sources, since the columns in the person tabes will be different for each source
    
    sourceDic = sourcesDF.collect()[0].asDict()
    sourceName = sourceDic['source_name']

    #Filter partitionSchemesDF to get only the persons tables for the source we are dealing with
    personsTablesNames = partitionSchemesDF.filter((col('zone') == lit('person')) & (col('source_name') == sourceName))
    
    #Get the staging tables related to the persons tables by calling getRetentionPiiDetailsWithStagingDF
    piiDetailsWithStagingDF = gdprFunctions.getRetentionPiiDetailsWithStagingDF(personsTablesNames, piiHashAttributesDF, sourceName, conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

    #Check if there are no rows, meaning the inserted Pii hashes aren't found anywhere
    if len(piiDetailsWithStagingDF.head(1)) == 0:
      print("The inserted PPI Hashes don't exist in any Person table for source {}.".format(sourceName))

    #If we are here means there are hashes to redact
    else:    
      #Apply redaction to the staging tables and related downstream tables based on the pii hashes
      tablesNonProcessedList, tablesProcessedList = gdprFunctions.applyRetentionRedaction(partitionSchemesDF, piiDetailsWithStagingDF, sourceName, sourceId, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
      
      logTaskProgress(cursor,batchTaskId,'Successfully redacted the tables {} for the source {}. There was nothing to redact in the tables: {}'.format(tablesProcessedList, sourceName, tablesNonProcessedList))

      #Delete the temporary tables  
      dbutils.fs.rm("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingTable" , True);
      dbutils.fs.rm("/mnt/temp/gdprRedactionHashes/tables/piiDetailsWithStagingWithoutDuplicatesTable" , True);  
      
      print("Redaction process concluded with success!")
      
  except Exception as e:
    errorMessage = 'Exception occurred while runnning gdpr retention redaction hashes: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: Execute usp_get_gdpr_data_retention stored procedure 
@add_method(gdprFunctions)
# Function to execute config.usp_get_gdpr_data_retention store procedure and get reference and staging object details
def getGDPRdataRetentionSpExec(conn,cursor,sourceId,stagingObjectName,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    
    retentionObjectsDf = pd.read_sql_query("exec config.usp_get_gdpr_data_retention {0},{1}".format(sourceId,stagingObjectName), conn)
    logTaskProgress(cursor,batchTaskId,"executed usp_get_gdpr_data_retention stored procedures completed ")
    return retentionObjectsDf
  except Exception as e:
    errorMessage="Exception occurred while execution of usp_get_gdpr_data_retention procedure: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function:To get reference and staging table dataframes
@add_method(gdprFunctions)
def getReferenceAndStagingDataFrames(conn
                                     ,cursor
                                     ,referenceObjectName
                                     ,whereCondition
                                     ,referenceAttributeName
                                     ,stagingObjectName
                                     ,stagingObjectAttributeName
                                     ,piiColumnsList
                                     ,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #Read the reference table attribute name 
    refDf=spark.read.table(referenceObjectName).where(whereCondition).selectExpr("CAST({} AS STRING) AS JoinKey".format(referenceAttributeName))
    
    #select staging table records for pii and do distinct to remove duplicates
    stagingDf=spark.read.table(stagingObjectName).where("PiiTraceabilityHash is not null and PiiTraceabilityHash <> -1").selectExpr(*piiColumnsList+['CAST({} AS STRING) AS JoinKey'.format(stagingObjectAttributeName)]).distinct()
    return  refDf,stagingDf
  except Exception as e:
    errorMessage="Exception occurred while getting staging and referencedataframes: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function:To create superset pii
@add_method(gdprFunctions)
def createSupersetPiiDf(conn
                     ,cursor
                     ,referenceDf   
                     ,stagingDf
                     ,piiColumnsList        
                     ,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #Join on the key to get the the superset of piihashes
    supersetPiiDf=referenceDf.join(stagingDf,['JoinKey']).selectExpr(*piiColumnsList+['JoinKey'])
    
    
    return  supersetPiiDf
  except Exception as e:
    errorMessage="Exception occurred while createing supersetpii dataframe: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  

# COMMAND ----------

# DBTITLE 1,Function:To create activePii Dataframe
@add_method(gdprFunctions)
def createActivePiiDf(conn
                     ,cursor
                     ,referenceDf   
                     ,stagingDf
                     ,piiColumnsList 
                     ,supersetPiiObjectName
                     ,supersetPiiLocationSpecific #specific location including partition path
                     ,stagingObjectName
                     ,otherPiiObjectName
                     ,otherPiiObjectLocationSpecific #specific location including partition path
                     ,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #do the left anti join to get the keys which are not in reference df and save them in temp_otherPIIHA
    otherPiiDf=stagingDf.join(referenceDf,['JoinKey'],how='left_anti').selectExpr(*piiColumnsList+['JoinKey']).withColumn('StagingTableName',lit(stagingObjectName))
    
    #otherPiiDf.write.format("delta").mode("append").saveAsTable(otherPiiObjectName)
    #Use a specific write to location using parquet rather than a delta table
    otherPiiDf.where("StagingTableName='" + stagingObjectName + "'").write.format("parquet").save(otherPiiObjectLocationSpecific)
    
    #Read the superset pii table for the staging table. No need to filter for staging table name as that is in the specific path. Removed where clause to access the staging table as that is already in the specific path
    supersetPiiDf=spark.read.format('parquet').load(supersetPiiLocationSpecific).select('PiiTraceabilityHash').distinct()
    
    #Read the other pii table for the sataging table. No need to filter for staging table name as that is in the specific path. Removed where clause to access the staging table as that is already in the specific path
    otherPiiDf = spark.read.format('parquet').load(otherPiiObjectLocationSpecific).select('PiiTraceabilityHash').distinct()
    
    #join on the tracebility hash to get the active keys
    activePiiDf=supersetPiiDf.join(otherPiiDf.select('PiiTraceabilityHash'),['PiiTraceabilityHash']).select(supersetPiiDf['PiiTraceabilityHash'])
    
    return  activePiiDf
  except Exception as e:
    errorMessage="Exception occurred while createing activePii dataframe: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function:To create PII temp tables
@add_method(gdprFunctions)
def createPIItempTables(conn,cursor,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try: 
    #This function will create the temporary static tables required for gdpr redaction
    dropSupersetPIIHAsql = '''DROP TABLE IF EXISTS temp_supersetTempKeysPIIHA'''
    
    dropActivePIIHAsql = '''DROP TABLE IF EXISTS temp_inUsePIIHA'''
    
    dropOtherPIIHAsql = '''DROP TABLE IF EXISTS temp_otherPIIHA'''
    
    dropToRemovePIIHAsql = '''DROP TABLE IF EXISTS temp_toRemovePIIHA'''
    toRemovePIIHAsql='''CREATE TABLE IF NOT EXISTS temp_toRemovePIIHA(
                                                                              PiiTraceabilityHash bigint
                                                                             ,PiiHash bigint
                                                                             ,PiiHashVersion int
                                                                             ,JoinKey string
                                                                             ,ReferenceTableName string
                                                                             ,ReferenceTableAttributeName string
                                                                             ,SourceID int
                                                                             ,StagingTableName string
                                                                             ,InUse boolean)
                      USING Delta
                      PARTITIONED BY (SourceID, StagingTableName)
                      LOCATION "/mnt/temp/gdprRetention/tables/temp_toRemovePIIHA"
                      TBLPROPERTIES ("delta.autoOptimize.optimizeWrite"= "True")'''
    
    spark.sql(dropSupersetPIIHAsql)    
    dbutils.fs.rm("/mnt/temp/gdprRetention/tables/temp_supersetTempKeysPIIHA" , True);
    spark.sql(dropActivePIIHAsql)
    dbutils.fs.rm("/mnt/temp/gdprRetention/tables/temp_inUsePIIHA" , True);
    spark.sql(dropOtherPIIHAsql)
    dbutils.fs.rm("/mnt/temp/gdprRetention/tables/temp_otherPIIHA" , True);   
    spark.sql(dropToRemovePIIHAsql)
    dbutils.fs.rm("/mnt/temp/gdprRetention/tables/temp_toRemovePIIHA" , True);  

    spark.sql(toRemovePIIHAsql)
    
  except Exception as e:
    errorMessage="Exception occurred while createing PII Temp tables: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function: To update the processed reference tables 
# updated the reference tables redaction state as processed
@add_method(gdprFunctions)
def updateReferenceProcessed(referenceTablesDF,sourceId,conn,cursor,batchId,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try: 
    #get the distinct reference table names and collect to list
    referenceTableList = referenceTablesDF.select("ReferenceTableName").distinct().collect()
    
    for referenceTableItem in referenceTableList:
      refTableProcessed = referenceTableItem['ReferenceTableName']

      spark.sql("UPDATE {} SET Processed=true, RedactedTimestamp=current_timestamp(),LastUpdatedBatchID={},LastUpdatedTimestamp=current_timestamp() WHERE SourceID={} AND Processed=false".format(refTableProcessed,batchId, sourceId))

    logTaskProgress(cursor,batchTaskId,'Succesfully executed the updateReferenceProcessed function')
    
  except Exception as e:
    errorMessage = 'Exception occurred while executing updateReferenceProcessed function: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function : Get the Pii details with staging table name for retention
# function to get a dataframe with all the staging tables related to the personsTablesNames by the pii_traceability_hash
@add_method(gdprFunctions)
def getRetentionPiiDetailsWithStagingDF(personsTablesNames, broadcastPiiHashAttributesDF, sourceName, conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #Clean the temporary table before writing
    dbutils.fs.rm("/mnt/temp/gdprRetention/tables/piiDetailsWithStagingTable{}".format(sourceName) , True);
    
    #get the distinct person table names and collect to list
    personTableList = personsTablesNames.select("object_name","zone","source_name","partition_scheme","all_partition_columns").distinct().collect()
    
    #For each person table, join with the inputed hashes to get the staging table
    for personTableItem in personTableList:
      objectName = personTableItem['object_name']
      zone = personTableItem['zone']
      source_name = personTableItem['source_name']
      partition_scheme = personTableItem['partition_scheme']
      all_partition_columns = personTableItem['all_partition_columns']
      
      #form the column list with possible duplicates
      columnsString = "PiiTraceabilityHash, SourceID, StagingTableName, StagingCreatedBatchId, {}".format(all_partition_columns)
      
      #call the function to remove duplicate columns
      distinctColumnList = removeDuplicateColumnsFromString(columnsString,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      
      #Get the person table with all the partitions and pii columns
      pTable = spark.sql("select DISTINCT {} from {}".format(distinctColumnList, objectName))
      
      piiDetailsWithStagingDFAux = (broadcastPiiHashAttributesDF
                  .join(pTable 
                        ,(broadcastPiiHashAttributesDF.pii_traceability_hash == pTable.PiiTraceabilityHash) & (broadcastPiiHashAttributesDF.StagingTableName == pTable.StagingTableName) 
                        ,how = 'inner')
                  .select(pTable["*"],broadcastPiiHashAttributesDF['JoinKey'],broadcastPiiHashAttributesDF['InUse'])
                  .withColumn('PersonTable', lit(objectName))
                  )
      #Keep appending the outuput until all the persons tables are done
      piiDetailsWithStagingDFAux.write.format("delta").mode("append").save("/mnt/temp/gdprRetention/tables/piiDetailsWithStagingTable{}".format(sourceName))
      
    
    #check if the table piiDetailsWithStagingTable exists
    piiDetailsWithStagingTableExists = os.path.exists('/dbfs/mnt/temp/gdprRetention/tables/piiDetailsWithStagingTable{}'.format(sourceName))
    
    #if the table doesn't exist return an empty dataframe
    if piiDetailsWithStagingTableExists == False:
      emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([]))
      return emptyDf
    
    #if the table exist proceed
    else:
      #Get the distinct rows from the previous join
      piiDetailsWithStagingDF = spark.read.format('delta').load("/mnt/temp/gdprRetention/tables/piiDetailsWithStagingTable{}".format(sourceName)).distinct()

      #Clean the table location before writing
      dbutils.fs.rm("/mnt/temp/gdprRetention/tables/piiDetailsWithStagingWithoutDuplicatesTable{}".format(sourceName) , True);
      #Write the distinct rows to a temporary table
      (piiDetailsWithStagingDF.write
                              .format("delta")
                              .mode("append")
                              .save("/mnt/temp/gdprRetention/tables/piiDetailsWithStagingWithoutDuplicatesTable{}".format(sourceName)))

      piiDetailsWithStagingDF = (spark.read
                                      .format('delta')
                                      .load("/mnt/temp/gdprRetention/tables/piiDetailsWithStagingWithoutDuplicatesTable{}".format(sourceName)))
      
      logTaskProgress(cursor,batchTaskId,'Successfully created the Pii Details table')
      #return the pii Details With Staging Without Duplicates
      return  piiDetailsWithStagingDF
  except Exception as e:
    errorMessage = 'Exception occurred while getting the Pii details with staging: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Function : Retention redaction of the refined tables
#Applies retention redaction to the refined and sensitive tables linked to the stagingTableName
@add_method(gdprFunctions)
def retentionRedactionPiiInRefined(partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, downstreamTableName, tablesNonProcessedList, tablesProcessedList, sourceTable, joinColumnName, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  #Update all the downstream tables and the staging table
  try :
    refinedDF = downstreamTableName.filter((lower(col('zone')) == lit('sensitive')) | (lower(col('zone')) == lit('refined')))
    
    constantFieldsToUpdate = "t.PiiHash = -1, t.PiiTraceabilityHash = -1, t.PiiHashVersion = -1, t.LastUpdatedBatchID={}, t.LastUpdatedTimestamp=current_timestamp() ".format(batchId)
    
    #get the distinct refined and sensitive table names and collect to list
    refinedTableList = refinedDF.select("object_name").distinct().collect()
    
    #Iterate through the refined downstream tables
    for refinedTableItem in refinedTableList:
      objectName = refinedTableItem['object_name']
      
      #Get the where clause for the table partitions
      partitionWhereClause, tablesNonProcessedList, tablesProcessedList = gdprFunctions.getPartitionWhereClause(objectName, partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, tablesNonProcessedList, tablesProcessedList, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
      spark.sql("MERGE INTO {} t USING {} s ON {} AND t.PiiTraceabilityHash=s.PiiTraceabilityHash AND t.{}=s.JoinKey WHEN MATCHED THEN UPDATE SET {}".format(objectName, sourceTable, partitionWhereClause, joinColumnName, constantFieldsToUpdate))
      logTaskProgress(cursor,batchTaskId,'Performed Piihash retention redaction from table {} '.format(objectName))
    
    return tablesNonProcessedList, tablesProcessedList
  
  except Exception as e:
    errorMessage = 'Exception occurred while rectifying the staging and the downstream objects: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    sys.exit(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function : Retention redaction of the person tables
#Applies retention redaction to all the person tables 
@add_method(gdprFunctions)
def retentionRedactionPiiInPerson(partitionSchemesDF, piiDetailsWithStagingDF, tablesNonProcessedList, tablesProcessedList, sourceTable, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  #Update all the downstream tables and the staging table
  try :
    #The process will be accross all the staging tables
    stagingTableName=''
    #get the person tables that aren't in use, distinct and ollect to list
    personTableList = piiDetailsWithStagingDF.where("InUse=false").select('PersonTable').distinct().collect()
    
    #For each person table, join with the inputed hashes to get the staging table
    for personTableItem in personTableList:
      objectName = personTableItem['PersonTable']
      
      #Get the where clause for the table partitions
      partitionWhereClause, tablesNonProcessedList, tablesProcessedList = gdprFunctions.getPartitionWhereClause(objectName, partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, tablesNonProcessedList, tablesProcessedList, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)    
      #Delete from persons table
      spark.sql("MERGE INTO {} t USING {} s ON {} AND t.PiiTraceabilityHash=s.PiiTraceabilityHash AND s.InUse=false WHEN MATCHED THEN DELETE".format(objectName, sourceTable, partitionWhereClause))
      logTaskProgress(cursor,batchTaskId,'Performed Piihash retention deletes for table {} '.format(objectName))
      
    
    return tablesNonProcessedList, tablesProcessedList
  
  except Exception as e:
    errorMessage = 'Exception occurred while rectifying the staging and the downstream objects: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    sys.exit(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function : Retention redaction of the staging tables
#Applies retention redaction to all the staging tables 
@add_method(gdprFunctions)
def retentionRedactionPiiInStaging(partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, tablesNonProcessedList, tablesProcessedList, sourceTable, joinColumnName, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  #Update all the downstream tables and the staging table
  try :
    
    constantFieldsToUpdate = "t.PiiHash = -1, t.PiiTraceabilityHash = -1, t.PiiHashVersion = -1, t.LastUpdatedBatchID={}, t.LastUpdatedTimestamp=current_timestamp() ".format(batchId)
      
    #Get the pii columns to redact on the staging table
    piiFieldsToUpdate = gdprFunctions.getStagingtablePiiFieldsAsRedacted(stagingTableName, conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
    
    #Get where clause for the staging table partitions
    partitionWhereClause, tablesNonProcessedList, tablesProcessedList = gdprFunctions.getPartitionWhereClause(stagingTableName, partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, tablesNonProcessedList, tablesProcessedList, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
    
    #Update the staging table
    spark.sql("MERGE INTO {} t USING {} s ON {} AND t.PiiTraceabilityHash=s.PiiTraceabilityHash AND t.{}=s.JoinKey WHEN MATCHED THEN UPDATE SET {},{}".format(stagingTableName, sourceTable, partitionWhereClause, joinColumnName, piiFieldsToUpdate, constantFieldsToUpdate))
    
    logTaskProgress(cursor,batchTaskId,'Performed Piihash retention redaction from table {} '.format(stagingTableName))
    
    return tablesNonProcessedList, tablesProcessedList
  
  except Exception as e:
    errorMessage = 'Exception occurred while rectifying the staging and the downstream objects: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    sys.exit(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function : Applies the retention redaction to the person, staging and downstream tables
# function to apply retention redaction to the person, staging and related downstream tables
@add_method(gdprFunctions)
def applyRetentionRedaction(partitionSchemesDF, piiDetailsWithStagingDF, sourceName, sourceId, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  try:  
    #get the distinct staging tables
    stagingTablesDF = piiDetailsWithStagingDF.select('StagingTableName').distinct()
    
    #collect to list
    stagingTableList = stagingTablesDF.collect()
    
    #Set the columns to be used on the update with the JoinKey
    columnsToSelect = ['PiiTraceabilityHash', 'JoinKey', 'InUse']
    sourceDF = piiDetailsWithStagingDF.select(columnsToSelect).distinct()
    
    sourceTableWithJoinKey = "to_updateWithJoinKey{}".format(sourceName)
    #Create a temporary table with the previous columns
    sourceDF.createOrReplaceTempView(sourceTableWithJoinKey)
    
    #Set the columns to be used on the update without the JoinKey
    columnsToSelect = ['PiiTraceabilityHash', 'InUse']
    sourceDF = piiDetailsWithStagingDF.select(columnsToSelect).distinct()
    
    sourceTableWithoutJoinKey = "to_updateWithoutJoinKey{}".format(sourceName)
    #Create a temporary table with the previous columns
    sourceDF.createOrReplaceTempView(sourceTableWithoutJoinKey)
    
    #Initialize list of processed and non processed tables
    tablesNonProcessedList = []
    tablesProcessedList = []
    
    #for each staging table apply the redaction to the refined and sensitive tables
    for stagingTableItem in stagingTableList:
      stagingTableName = stagingTableItem['StagingTableName']
        
      #Get the dowstream tables for staging table being processed
      downstreamTableName = gdprFunctions.getDownstreamObjects(stagingTableName, conn, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
      
      #Get joinColumnName
      retentionObjectsDf = getGDPRdataRetentionSpExec(conn,cursor,sourceId,stagingTableName,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      joinColumnName = retentionObjectsDf['object_attribute_name'][0]
    
      #Update all the downstream refined and sensitive tables
      gdprFunctions.retentionRedactionPiiInRefined(partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, downstreamTableName, tablesNonProcessedList, tablesProcessedList, sourceTableWithJoinKey, joinColumnName, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
    
      logTaskProgress(cursor,batchTaskId,'Retention redaction successfully applied to refined and sensitive tables of the staging table {}'.format(stagingTableName))
      
    #apply the redaction for person tables
    retentionRedactionPiiInPerson(partitionSchemesDF, piiDetailsWithStagingDF, tablesNonProcessedList, tablesProcessedList, sourceTableWithoutJoinKey, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
    
    logTaskProgress(cursor,batchTaskId,'Retention redaction successfully applied to all the person tables for source '.format(sourceName))
    
     #for each staging table apply redaction
    for stagingTableItem in stagingTableList:
      stagingTableName = stagingTableItem['StagingTableName']
      
      #Get joinColumnName
      retentionObjectsDf = getGDPRdataRetentionSpExec(conn,cursor,sourceId,stagingTableName,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      joinColumnName = retentionObjectsDf['object_attribute_name'][0]
    
      #Update the staging table
      gdprFunctions.retentionRedactionPiiInStaging(partitionSchemesDF, piiDetailsWithStagingDF, stagingTableName, tablesNonProcessedList, tablesProcessedList, sourceTableWithJoinKey, joinColumnName, conn, cursor, batchId, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
    
    logTaskProgress(cursor,batchTaskId,'Retention redaction successfully applied to all the staging tables for source '.format(sourceName))
      
    return tablesNonProcessedList, tablesProcessedList
      
  except Exception as e:
    errorMessage = 'Exception occurred while applying retention redaction: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    sys.exit(errorMessage)

# COMMAND ----------

# DBTITLE 1,Function : Get temp views recreation Object
# get the reference object to recreate temp views
@add_method(gdprFunctions)
def usp_gdpr_get_tempview_recreate_object(batchId,conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    referenceObject = pd.read_sql_query("exec [config].[usp_gdpr_get_tempview_recreate_object] {}".format(batchId),conn).at[0,'gdpr_temp_view_recreate_object']
    
    logTaskProgress(cursor,batchTaskId,'Succesfully executed the usp_gdpr_get_tempview_recreate_object stored procedure')
    return referenceObject
  except Exception as e:
    errorMessage = 'Exception occurred while executing usp_gdpr_get_tempview_recreate_object stored procedure: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    print(errorMessage)
    assert False