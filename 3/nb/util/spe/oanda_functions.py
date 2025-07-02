# Databricks notebook source
# MAGIC %md
# MAGIC #Oanda API functions

# COMMAND ----------

# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>oandaapi_functions</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Functions used for processing Oanda API requests</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/10/03</td></tr>
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
# MAGIC     <td>2022/10/03</td>
# MAGIC     <td>Manmohan</td>
# MAGIC     <td>Initial version
# MAGIC         </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/11/03</td>
# MAGIC     <td>Manmohan</td>
# MAGIC     <td>Converted Quote_Date from string to Date, lakeCreatedBatchID from string to BigInt.</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/11/11</td>
# MAGIC     <td>Manmohan</td>
# MAGIC     <td>Added new function getBaseTargetCurrencies to get currencies list from sqldb.</td>
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
    import json
    import requests
    from pyspark.sql.functions import from_json, explode, col, regexp_replace, create_map, lit, date_format, concat,concat_ws, udf, struct, collect_list, coalesce
    from pyspark.sql.types import *
    
    import pandas as pd
    import pyodbc
    from datetime import datetime,timedelta

except Exception as e:
    errorMessage="Exception occurred while import modules " + str(e)
    assert False

# COMMAND ----------

# DBTITLE 1,Standard Functions for Unstructured Sources
# MAGIC %run ../../util/spe/unstructuredSource_functions

# COMMAND ----------

# DBTITLE 1,Oanda Api functions
#initialise the class
class oandaApiFunctions:
    pass;

#function GetRemainingQuotes that retrieves remaining Quotes for the accounting period
@add_method(oandaApiFunctions)
def GetRemainingQuotes(baseUrl,headers):
    # set api end point 
    requestStr = 'remaining_quotes.json'
    url = f'{baseUrl}{requestStr}'
    
    #call the api
    response = requests.get(url, headers=headers)
    responseDict = json.loads(response.content)
    remaining_quotes = int(responseDict["remaining_quotes"])
    
    #print(query.text, remaining_quotes, sep="|")   
    return remaining_quotes

#function to transform response quotes json 
@add_method(oandaApiFunctions)    
def ConvertQuotesStructure(baseJson):
    #target currenceis as list
    targetCurrencyList = list(baseJson["quotes"])
    targetList2=[]
    for targetCurrency in targetCurrencyList:
          targetList2 = AssignTargetCurrency(targetCurrency,baseJson,targetList2)

    return targetList2    

#function to append target currency to the quotes list 
@add_method(oandaApiFunctions)    
def AssignTargetCurrency(targetCurrency,baseJson,targetList):
    baseJson["quotes"][targetCurrency]["target_Currency"]=targetCurrency
    targetList.append(baseJson["quotes"][targetCurrency])
    return targetList

#function to extract just the required attributes from meta json  
@add_method(oandaApiFunctions)    
def CleanseAndReStructure(baseJson):        
    #extract quote_date
    baseJson["quote_date"] = baseJson["meta"]["effective_params"]["date"]
    
    #cleanse meta
    meta = baseJson.pop("meta")
    newMeta={}    
    newMeta["request_time"] = meta["request_time"]
    newMeta["skipped_currencies"] = meta["skipped_currencies"]    
    baseJson["meta"] = newMeta
    
    return baseJson

#function GetRatesResponse that retrieves the query string to be added to the url, gets the url and makes the api call return the response in json
@add_method(oandaApiFunctions)
def GetRatesResponse(baseCurrency, quotes, fields, decimalPlaces, date, start, end, apiKey):

    #remove Base currency from target currencies set to avoid same currency quotes 
    quotes = copy.deepcopy(targetCurrencies)
    quotes.discard(baseCurrency)

    #get the query string
    queryString = RatesParamaterQueryString(quotes, fields, decimalPlaces, date, start, end)
    
    #create the final part of the query string
    requestStr = f"rates/{baseCurrency}.json{queryString}"
    
    #make the api call and return the response
    responseDict = PerformRequest(requestStr)    
    
    return responseDict

#function PerformRequest to form the final api call with the included headers. Convert result to json and return
@add_method(oandaApiFunctions)
def PerformRequest(requestStr):
    url = '{}{}'.format(baseUrl, requestStr)
    
    #perform request
    response = requests.get(url, headers=headers)

    responseDict = json.loads(response.content)
    
    #convert quotes to list
    responseDict["quotes_list"] = ConvertQuotesStructure(responseDict)

    #remove unwanted attributes
    responseDict = CleanseAndReStructure(responseDict)
    return responseDict

#function to create the api query string based on the quotes required, fields, decimal places, start and end date
@add_method(oandaApiFunctions)
def RatesParamaterQueryString(quotes, fields, decimalPlaces, date, start, end):
    response = ""
    nonDetaultParams = {}
    
    prms.update({"decimal_places": decimalPlaces})
    prms.update({"date": date})
    prms.update({"start": start})
    prms.update({"end": end})
    
    #initialise quote to empty list as all quotes will use the same key
    prms["quote"] = []
    
    for quote in quotes:
        prms["quote"].append(quote)

    for field in fields:
        prms.update({"fields": field.lower()}) # this is far more complex in the c# code
        
    for k,v in prms.items():
        if v != '':
            nonDetaultParams.update({k:v})
    
    #if it is the first time then the separator should be a ? as it represents the start of the query string
    first = True
    
    for k,v in nonDetaultParams.items():
        separator = '&'

        if first:
            separator = '?'
            first = False
        
        #if this is a list then we do an inner loop to iterate
        if isinstance(v, list):
            for q in v:
                response = "{}{}{}={}".format(response,separator, k, q)
        else:
            response = "{}{}{}={}".format(response,separator, k, v)
    return response

# COMMAND ----------

# DBTITLE 1,Check Quotes Allowance
#check if quotes allowance is greater than estimated quotes required, returns boolean
def hasQuotesAllowance(estimatedQuotesPerDate,baseUrl,headers
                       ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):                       
    res = False
    try:
        remaining_quotes = GetRemainingQuotes(baseUrl,headers)
        if remaining_quotes > estimatedQuotesPerDate:
            res = True
        else:
            errorMessage = f"Not enough quotes allowance, reqd: [{estimatedQuotesPerDate}], remaining allowance: [{remaining_quotes}]"
            logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation) 
            res = False
        return res
    except Exception as e:
        errorMessage = "Exception occured while checking for quotes allowance: " + str(e)
        print(errorMessage)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)    
        assert False

# COMMAND ----------

# DBTITLE 1,Function to get Quotes for a Base currency and Quote date
# Gets quotes for a given BaseCurrency-QuoteDate pair
def getQuotesPerBaseCurrencyAndQuoteDate(baseCurrency, quoteDate, dfBaseCurrencyQuoteDate
                                                         ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:

        # ApiRequest 
        responseDict = oandaApiFunctions.GetRatesResponse(baseCurrency, targetCurrencies, fields, decimalPlaces, quoteDate, start, end, apiKey)        
        logTaskProgress(cursor,batchTaskId,f"Response recieved for [{baseCurrency}] and [{quoteDate}]") 
        
        
        dfTemp = spark.read.json(sc.parallelize([responseDict]),multiLine=True)       
        # check if empty
        if dfBaseCurrencyQuoteDate.count()==0:
            dfBaseCurrencyQuoteDate = dfTemp        
        else:
            dfBaseCurrencyQuoteDate = dfBaseCurrencyQuoteDate.union(dfTemp)
            
        return dfBaseCurrencyQuoteDate
        
    except Exception as e:
        errorMessage = "Exception occured while obtaining quote for [{baseCurrency}] and date [{quoteDate}] " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)    
#         print(errorMessage)
        assert False

# COMMAND ----------

# DBTITLE 1,Write to Delta Table
# Function to write Quotes Dataframe to Delta Table
def writeDfToDeltaTable(df,rawTargetObjectName,createdDate,createdTimestamp,batchId
                        ,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):                        
    # logTaskProgress(cursor,batchTaskId,f"Writing quotes df to delta table")
    try:
        res = False

        #Add Audit data fields and prep reqd fields
        dfDelta = ( df.withColumn("exchange_rate_source", lit("Oanda Api"))
                    .withColumn("lakeCreatedDate",lit(createdDate))
                    .withColumn("lakeCreatedTimestamp",lit(createdTimestamp))
                    .withColumn("lakeCreatedBatchID",lit(batchId).cast(LongType()))                        
                    .select(col("quote_date").cast(DateType()).alias("quote_date")
                            ,"base_currency"
                            ,col("quotes_list").alias("quotes")
                            ,"meta"
                            ,"exchange_rate_source"
                            ,"lakeCreatedDate"
                            ,"lakeCreatedTimestamp"
                            ,"lakeCreatedBatchID"
                           )
                  )
        logTaskProgress(cursor,batchTaskId,f"Successfully added audit data to dataframe")

        #write to delta table
        dfDelta.write.format("delta").mode("append").saveAsTable(f"{rawTargetObjectName}")
        logTaskProgress(cursor,batchTaskId,f"Successfully written {dfDelta.count()} records to delta table ")

        res = True
        return res
    except Exception as e:
        errorMessage = "Exception occured while writing quotes df to delta table " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)    
        assert False

# COMMAND ----------

# DBTITLE 1,Mark dates as loaded
# function to log to object availability table
def markQuoteDateAsLoaded(cursor,batchTaskId,rawTargetObjectName,quoteDate,createdTimestamp, adfPipelineName,clusterId,notebookName,errorLogFileLocation):

    try:
        cursor.execute("exec audit.usp_log_availability_dates_exchangeratesdaily ?,?,?,?", batchTaskId, rawTargetObjectName, quoteDate, createdTimestamp )

        logTaskProgress(cursor,batchTaskId,"Loaded dates are marked for destination object")
    except Exception as e:
        errorMessage="Exception occured while marking the dates as loaded: " + str(e)
        print(errorMessage)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False

# COMMAND ----------

# DBTITLE 1,Get quote date to load from tblObjectAvailability
  # Get quote date to load from based on tblObjectAvailability
def getLastQuoteDateLoaded(cursor,conn,batchTaskId,rawTargetObjectName, errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:    
        lastQuoteDateLoaded_df = pd.read_sql_query(f"exec [config].[usp_get_last_quote_date_loaded] @rawTargetObjectName = '{rawTargetObjectName}' ",conn)
        lastQuoteDateLoaded = lastQuoteDateLoaded_df["Last_Date_Loaded"][0]
        logTaskProgress(cursor,batchTaskId,f"Recieved lastQuoteDateLoaded as {lastQuoteDateLoaded}")
        return lastQuoteDateLoaded
    except Exception as e:
        errorMessage="Exception occurred while execution lastQuoteDateLoaded query: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False
    
# lastQuoteDateLoaded

# COMMAND ----------

# DBTITLE 1,Get Base Target Currencies List
# Get BaseTargetCurrencies from sqlDb valid for this environment
def getBaseTargetCurrencies(cursor,conn,batchTaskId, errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:    

        #load data to pandas df
        pandas_df = pd.read_sql_query("exec [config].[usp_get_base_target_currencies] ",conn)
        
        #convert to Spark df
        baseTargetCurrencies_df = sparkDataframe=spark.createDataFrame(pandas_df)
        
        #extract base and target currencies to list
        baseCurrencies   = set([data[0] for data in baseTargetCurrencies_df.select('iso_3_currency_code').where("currency_usage_type='Base_Currency'").collect()])
        targetCurrencies = set([data[0] for data in baseTargetCurrencies_df.select('iso_3_currency_code').where("currency_usage_type='Target_Currency'").collect()])
        
        logTaskProgress(cursor,batchTaskId,f"Recieved Base and Target currencies from sqldb ")
        return baseCurrencies, targetCurrencies
    except Exception as e:
        errorMessage="Exception occurred while execution lastQuoteDateLoaded query: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False