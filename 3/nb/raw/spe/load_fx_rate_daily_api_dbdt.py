# Databricks notebook source
# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_fx_rate_daily_api_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Load daily exchange rates from Oanda API to Raw Delta table</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Manmohan</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/09/23</td></tr>
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
# MAGIC   <tr>
# MAGIC     <td>2022/09/23</td>
# MAGIC     <td>Manmohan </td>
# MAGIC     <td>Initial version</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/11/11</td>
# MAGIC     <td>Manmohan </td>
# MAGIC     <td>Moved Base Target currencies static list to SqlDb</td>
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

# DBTITLE 1,Logging functions
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Standard Functions
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Oanda Functions
# MAGIC %run ../../util/spe/oanda_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
    import json
    import requests
    from pyspark.sql.functions import from_json, explode, col, regexp_replace, create_map, lit, date_format, concat,concat_ws, udf, struct, collect_list, coalesce
    from pyspark.sql.types import *
    
    import pandas as pd
    import pyodbc
    import copy
    from datetime import datetime,timedelta

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
    createdTimestamp = currentTs
    lastUpdatedTimestamp = currentTs
    #use the same time for the date also
    date=currentTs
    
    #parameter for logError
    errorMessage = ''
    adfPipelineName = ''
    clusterId = ''
    notebookName = ''
    batchId = int(1)
    batchTaskId = int(1)
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
    clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    dbutils.widgets.text("clusterId",clusterId)
#     clusterId  =  dbutils.widgets.get("clusterId")
    
    #assign the source and batch to other variables that are referenced from the metadata
    sourceId=sourceId
    createdBatchId=batchId
    lastUpdateBatchId=batchId
    
    #call the get_logging_path function to create a log file path as a string and store it in a variable
    errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')  
except Exception as e:
    errorMessage = "Exception occured while getting parameters and initiliasing error log location: " + str(e)
    assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')

#call function sqlDbConn to establish Database connection with given scope and key values
try:
    conn,cursor = sqlDbConn(dbconn,
                          batchTaskId,                          
                          adfPipelineName,
                          clusterId,
                          notebookName,
                          errorLogFileLocation)
    logTaskProgress(cursor,batchTaskId,"Successfully established SQL connection")
except Exception as e:
    errorMessage="Unable to establish SQL connection: " + str(e)
    logToFile(errorLogFileLocation,errorMessage)
    assert False

# COMMAND ----------

# DBTITLE 1,Log Task Start
# Log start of the task
try:
    logTaskStart(cursor, batchTaskId)    
except Exception as e:
    errorMessage="Unable to log batch task as running: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)    
    assert False  

# COMMAND ----------

# DBTITLE 1,Get API key
#Get Oanda API key details with given scope and key values
try:
    apiKey = dbutils.secrets.get(scope = "lza-da-kv-001-d", key = "oanda-api-key")
    logTaskProgress(cursor,batchTaskId,"Successfully obtained API key from key vault")
except Exception as e:
    errorMessage="Unable to obtain API key from key vault: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)    
    assert False    

# COMMAND ----------

# DBTITLE 1,Get list of Base and Target currencies
#list of base and target currencies
try:
    baseCurrencies,targetCurrencies = getBaseTargetCurrencies(cursor,conn,batchTaskId, errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except Exception as e:
    errorMessage="Unable to obtain base and target currencies: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)    
    assert False

# COMMAND ----------

# DBTITLE 1,Setup API Parameters
#Number of decimal places in the exchange rate
decimalPlaces = "8"

#Base URL of the v1 API endpoint 
baseUrl = "https://exchange-rates-api.oanda.com/v1/"

#Exchange rate query parameter field for averages
fields = {"Averages"}

#query parameters for start and end date range, currently not being used 
start = ""
end = ""

prms = {}

#Credentials to be passed in the API request
credential = 'Bearer {}'.format(apiKey)

#API request headers
headers = {"Content-Type":"application/json"
           ,"User-Agent":"OANDAExchangeRates.Python/0.01"
           ,"Authorization":credential}

#Estimated number of quotes per quote date
estimatedQuotesPerDate = len(baseCurrencies) * len(targetCurrencies)

#Raw Delta table name
rawTargetObjectName = 'raw_fx_rate.exchange_rate_daily'

# COMMAND ----------

# DBTITLE 1,Process quote_dates
# Process for each date between last successful QuoteDate loaded to Yesterday 
try:
    #Get the last successful date quotes were loaded and increment by 1 day
    lastQuoteDateLoaded = getLastQuoteDateLoaded(cursor,conn,batchTaskId,rawTargetObjectName, errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    quoteDate = lastQuoteDateLoaded + timedelta(days=1)    
    
    logTaskProgress(cursor,batchTaskId,f"Starting to Process quote dates on and after : [{quoteDate}]")    
    #loop for each quote date to load until yesterday
    while quoteDate < date.date() :

        logTaskProgress(cursor,batchTaskId,f"Processing quote date [{quoteDate}]")
        
        #initialise empty df for base currency - quote date pair
        dfFinal = spark.createDataFrame(data = sc.emptyRDD(), schema = StructType([])) 
        dfBaseCurrencyQuoteDate = spark.createDataFrame(data = sc.emptyRDD(), schema = StructType([]))
        
        #check for enough quotes allowance limit
        if(hasQuotesAllowance(estimatedQuotesPerDate,baseUrl,headers ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)):           
            for baseCurrency in baseCurrencies:
                
                #logTaskProgress(cursor,batchTaskId,f"Requesting [{baseCurrency}] quote for [{quoteDate}]")
                dfBaseCurrencyQuoteDate = getQuotesPerBaseCurrencyAndQuoteDate(baseCurrency, quoteDate, dfBaseCurrencyQuoteDate ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)       
            
            #initilise dfFinal if empty
            if dfFinal.count()==0:
                dfFinal = dfBaseCurrencyQuoteDate 
                
            else:
                #union individual basecurrency Dfs
                dfFinal = dfFinal.union(dfBaseCurrencyQuoteDate)
        
        #write to delta table
        res = writeDfToDeltaTable(dfFinal,rawTargetObjectName,createdDate,createdTimestamp,batchId ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        
        #mark dates as loaded to object availability
        if(res):
            markQuoteDateAsLoaded(cursor,batchTaskId,rawTargetObjectName,quoteDate,createdTimestamp, adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        #increment quoteDate
        quoteDate = quoteDate + timedelta(days=1)
        
except Exception as e:
    errorMessage = "Exception occured while processing for quotes" + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)    
    assert False

# COMMAND ----------

# DBTITLE 1,Complete task and close the Database connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
    taskEndAndCloseConn(cursor,conn,batchTaskId,batchTaskSourceRows,
                            batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                            batchTaskResultLocation,
                            adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
    assert False