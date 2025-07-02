# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>dataquality_function</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>DataQuality Functions to support data quality checks on data</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2019/01/01</td></tr>
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
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC   </tr>
# MAGIC </table>
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
# MAGIC     <td>Updated executeDataQualityTests function
# MAGIC         <br>Corrected name of destination_column from stored procedure
# MAGIC         <br>Incorporated function convertPandasToSparkDfWithSchema
# MAGIC         <br>Fix to convertPandasToSparkDfWithSchema to handle single row entries with null values</td>
# MAGIC </table>  
# MAGIC   
# MAGIC ## General Standards applying to each notebook
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

# DBTITLE 1,Function to Add methonds to class
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
class dataqualityFunctions:
  pass;

# COMMAND ----------

# DBTITLE 1,Function getDataQualityTests
#function to retrieve the data quality tasks by calling config.usp_get_data_quality_test
@add_method(dataqualityFunctions)
def getDataQualityTest( conn
                      , cursor
                      , batchTaskId
                      , adfPipelineName
                      , clusterId
                      , notebookName
                      , errorLogFileLocation):
  try:
    #Get the count of an item.  presen in the path
    dataqualityTests=pd.read_sql_query("exec [config].[usp_get_data_quality_test] {}".format(str(batchTaskId)),conn)
    
    #declare the struct for the procedure 
    dataqualityTestsSchema=StructType([
       StructField('data_quality_test_id'    ,IntegerType(),True)
      ,StructField('source_table'           ,StringType() ,True)
      ,StructField('destination_table'      ,StringType() ,True)
      ,StructField('source_column'          ,StringType() ,True)
      ,StructField('destination_column'     ,StringType() ,True)
      ,StructField('aggregation_function'   ,StringType() ,True)
      ,StructField('data_quality_test_type' ,StringType() ,True)
      ,StructField('source_where_clause'    ,StringType() ,True)
    ])
    
    #create the sparkDataFrame from the pandas dataframe
    dataqualityTestsDf=convertPandasToSparkDfWithSchema(dataqualityTests.fillna('NA'),dataqualityTestsSchema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
    logTaskProgress(cursor,batchTaskId,"Retrieved data quality tests from procedure and converted to spark dataframe")
    
    return dataqualityTestsDf

  except Exception as e:
    errorMessage="Error during execution of function getDataQualityTest: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function logDataqualityTestResult
#function to log the data quality result to the database using stored procedure [audit].[usp_log_data_quality_test_result]
@add_method(dataqualityFunctions)
def logDataqualityTestResult( dataqualityTestId
                            , dataqualityTestResult
                            , dataqualityTestMessage
                            , cursor
                            , batchTaskId
                            , adfPipelineName
                            , clusterId
                            , notebookName
                            , errorLogFileLocation):
  try:
    #log the dataquality test result
    cursor.execute("exec [audit].[usp_log_data_quality_test_result] @batch_task_id=?, @data_quality_test_id=?, @data_quality_test_result=?, @data_quality_test_message=?",batchTaskId, dataqualityTestId, dataqualityTestResult, dataqualityTestMessage)
    while cursor.nextset():
      x = 1
    
    #log task progress to say you have logged the test result
    logTaskProgress(cursor,batchTaskId,"logged restult of dataquality test {}".format(str(dataqualityTestId)))    
      
  except Exception as e:
    errorMessage="Exception during function call to logDataqualityTestResult: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False    

# COMMAND ----------

# DBTITLE 1,Function executeDataQualityTests
#Function that takes a dataframe containing the data quality tests and executes each of them by calling required function
@add_method(dataqualityFunctions)
def executeDataQualityTests( dataqualityTestsDf
                           , cursor
                           , batchTaskId
                           , adfPipelineName
                           , clusterId
                           , notebookName
                           , errorLogFileLocation):
  try:
    #code here to loop through the tests and run them 
    for dataqualityTest in dataqualityTestsDf.collect():
      #convert the row to dictionary to make easy to access
      dataqualityTestDict = dataqualityTest.asDict()
      
      #get values from the dictionary into variables
      dataqualityTestId = dataqualityTestDict["data_quality_test_id"]
      sourceTableName = dataqualityTestDict["source_table"]
      destinationTableName = dataqualityTestDict["destination_table"]
      sourceColumn = dataqualityTestDict["source_column"]
      destinationColumn = dataqualityTestDict["destination_column"]
      aggregationFunction = dataqualityTestDict["aggregation_function"]
      dataqualityTestType = dataqualityTestDict["data_quality_test_type"]
      sourceWhereClause = dataqualityTestDict["source_where_clause"]
      
      if dataqualityTestType == "AggregationValuesComparison":
        #call dataqualityFunctions.aggregationComparison
        dataqualityFunctions.aggregationComparison( dataqualityTestId
                                                  , destinationTableName
                                                  , sourceTableName
                                                  , destinationColumn
                                                  , sourceColumn
                                                  , sourceWhereClause
                                                  , aggregationFunction
                                                  , cursor
                                                  , batchTaskId
                                                  , adfPipelineName
                                                  , clusterId
                                                  , notebookName
                                                  , errorLogFileLocation)
        
      if dataqualityTestType == "NullValuesCheck":
        #call dataqualityFunctions.nullCountCheck
        dataqualityFunctions.nullCountCheck( dataqualityTestId
                                           , destinationTableName
                                           , destinationColumn
                                           , cursor
                                           , batchTaskId
                                           , adfPipelineName
                                           , clusterId
                                           , notebookName
                                           , errorLogFileLocation)
        
      if dataqualityTestType == "RowCountComparison":
        #call dataqualityFunctions.rowCountComparison
        dataqualityFunctions.rowCountComparison( dataqualityTestId
                                               , destinationTableName
                                               , sourceTableName
                                               , sourceWhereClause                  
                                               , cursor
                                               , batchTaskId
                                               , adfPipelineName
                                               , clusterId
                                               , notebookName
                                               , errorLogFileLocation)
      
  except Exception as e:
    errorMessage="Exception during execution of executeDataQualityTests function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  

# COMMAND ----------

# DBTITLE 1,Function rowCountComparison
#Function to perform a rowcount check comparison for a supplied source and destination table. The source where clause should be used when querying the source table to replicate any filters used when loading the data from the source to target. The possiblity of no rows from the source or destination table should be handled, and the result logged using logDataqualityTestResult function
@add_method(dataqualityFunctions)
def rowCountComparison( dataqualityTestId
                      , destinationTableName
                      , sourceTableName
                      , sourceWhereClause                  
                      , cursor
                      , batchTaskId
                      , adfPipelineName
                      , clusterId
                      , notebookName
                      , errorLogFileLocation):
  try:
    #set the source where clause if it is empty so we can always apply it
    sourceWhereClause = "1=1" if len(sourceWhereClause) == 0 else sourceWhereClause

    sourceRowCount = spark.read.table(sourceTableName).where(sourceWhereClause).count()
    destinationRowCount = spark.read.table(destinationTableName).count()

    rowCountMatched = 'succeeded' if sourceRowCount == destinationRowCount else 'failed'
    #construct the message
    dataqualityTestMessage = 'Row counts between tables {}. Source table {} has {} rows and Destination table {} has {} rows'.format(rowCountMatched, sourceTableName, str(sourceRowCount), destinationTableName, str(destinationRowCount))
    
    #log the test result to the database 
    dataqualityFunctions.logDataqualityTestResult( dataqualityTestId
                                                 , rowCountMatched
                                                 , dataqualityTestMessage
                                                 , cursor
                                                 , batchTaskId
                                                 , adfPipelineName
                                                 , clusterId
                                                 , notebookName
                                                 , errorLogFileLocation)

  except Exception as e:
    errorMessage="Execption during function rowCountComparison: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function aggregationComparison
#Function to perform an aggregation for a supplied column in a source and destination table. The source where clause should be used when querying the source table to replicate any filters used when loading the data from the source to target. The aggregation function is supplied and the appropriate pyspark.sql.function should be dynamically imported. The possiblity of no rows from the source or destination table should be handled, and the result logged using logDataqualityTestResult function
@add_method(dataqualityFunctions)
def aggregationComparison( dataqualityTestId
                         , destinationTableName
                         , sourceTableName
                         , aggregationDestinationColumn
                         , aggregationSourceColumn
                         , sourceWhereClause
                         , aggregationFunction
                         , cursor
                         , batchTaskId
                         , adfPipelineName
                         , clusterId
                         , notebookName
                         , errorLogFileLocation):
  try:   
    #dynamically import the module
    exec("from pyspark.sql.functions import {}".format(aggregationFunction.lower()))
    #set the source where clause to filter for nulls on the aggregation column
    sourceWhereClauseFilter = "{} IS NOT NULL".format(aggregationSourceColumn)
    sourceWhereClause = sourceWhereClauseFilter if len(sourceWhereClause) == 0 else sourceWhereClause + " AND " + sourceWhereClauseFilter

    #set the destination where clause to filter nulls
    destinationWhereClause = "{} IS NOT NULL".format(aggregationDestinationColumn)

    sourceAggregationDf = (spark
                           .read
                           .table(sourceTableName)
                           .where(sourceWhereClause)
                           .agg({aggregationSourceColumn:aggregationFunction})
                           .toDF("sourceAggregatedValue")
                          ).collect()

    destinationAggregationDf = (spark
                                .read
                                .table(destinationTableName)
                                .where(destinationWhereClause)
                                .agg({aggregationDestinationColumn:aggregationFunction})
                                .toDF("destinationAggregatedValue")
                               ).collect()
      
    sourceAggregationValue = 0 if len(sourceAggregationDf) == 0 else sourceAggregationDf[0]["sourceAggregatedValue"]
    destinationAggregationValue =  0 if len(destinationAggregationDf) == 0 else destinationAggregationDf[0]["destinationAggregatedValue"]

    aggregationValuesMatch = 'succeeded' if sourceAggregationValue == destinationAggregationValue else 'failed'

    #construct the message
    dataqualityTestMessage = 'Aggregated values between tables {} (column {}) and {} (column {}) using aggregation function {} {}. Source table value = {} and Destination table value {}'.format( sourceTableName
                                                                                                                                                                                                 , aggregationSourceColumn
                                                                                                                                                                                                 , destinationTableName
                                                                                                                                                                                                 , aggregationDestinationColumn
                                                                                                                                                                                                 , aggregationFunction
                                                                                                                                                                                                 , aggregationValuesMatch
                                                                                                                                                                                                 , str(sourceAggregationValue)
                                                                                                                                                                                                 , str(destinationAggregationValue)
                                                                                                                                                                                                 )
    #log the test result to the database 
    dataqualityFunctions.logDataqualityTestResult( dataqualityTestId
                                                 , aggregationValuesMatch
                                                 , dataqualityTestMessage
                                                 , cursor
                                                 , batchTaskId
                                                 , adfPipelineName
                                                 , clusterId
                                                 , notebookName
                                                 , errorLogFileLocation)    

  except Exception as e:
    errorMessage="Execption during function aggregationComparison: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function nullCountCheck
#Function to perform a count of null and not null for a supplied column in a target table. The possiblity of no rows from the destination table should be handled, and the result logged using logDataqualityTestResult function
@add_method(dataqualityFunctions)
def nullCountCheck( dataqualityTestId
                  , destinationTableName
                  , targetColumn
                  , cursor
                  , batchTaskId
                  , adfPipelineName
                  , clusterId
                  , notebookName
                  , errorLogFileLocation):
  try:
    #Get the count of an item.  presen in the path
    #null count #dataframe implementation    

    #set the aggregation function to sum as we want to aggregated the 1's
    aggregationFunction = 'sum'
    nullCountDf = (spark.read.table(destinationTableName)
                   .withColumn("nullCount", when(col(targetColumn).isNull()
                                                 , 1).otherwise(0))
                   .withColumn("notNullCount", when(~col(targetColumn).isNull()
                                                    , 1).otherwise(0)) 
                   .agg({"nullCount":aggregationFunction, "notNullCount":aggregationFunction})
                   .toDF("nullCount", "notNullCount")
                  ).collect()
    
    nullCount = 0 if len(nullCountDf) == 0 else nullCountDf[0]["nullCount"]
    notNullCount = 0 if len(nullCountDf) == 0 else nullCountDf[0]["notNullCount"]

    #if there are no nulls (probably the expectation)
    nullCountTestResult = 'succeeded' if nullCount == 0 else 'failed'

    #construct the message
    dataqualityTestMessage = 'Null counts check for table {} has {}. Table has {} NULLS and {} NOT NULL rows'.format(destinationTableName, nullCountTestResult, str(nullCount), str(notNullCount))

    #log the test result to the database 
    dataqualityFunctions.logDataqualityTestResult( dataqualityTestId
                                                 , nullCountTestResult
                                                 , dataqualityTestMessage
                                                 , cursor
                                                 , batchTaskId
                                                 , adfPipelineName
                                                 , clusterId
                                                 , notebookName
                                                 , errorLogFileLocation)
  except Exception as e:
    errorMessage="Exception during exectuion of function nullCountCheck: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False