# Databricks notebook source
# MAGIC %md
# MAGIC # HVR API Functions

# COMMAND ----------

# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>hvr_functions</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Functions used for processing HVR API requests</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/11/21</td></tr>
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
# MAGIC     <td>2022/11/21</td>
# MAGIC     <td>Manmohan</td>
# MAGIC     <td>Initial version
# MAGIC         </td>
# MAGIC   </tr>
# MAGIC   
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

# DBTITLE 1,Import libraries
try :    
    from itertools import chain
    from pyspark.sql.functions import from_json, explode, col, regexp_replace, create_map, lit, date_format, concat, udf, current_date, current_timestamp
    from pyspark.sql.types import ArrayType, StructType, StructField, StringType, TimestampType, IntegerType, MapType, BooleanType
    from datetime import datetime,timedelta
    
    import json
    import requests
    requests.packages.urllib3.disable_warnings()

except Exception as e:
    errorMessage="Exception occurred while import modules " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Get HVR secrets from Key Vault
try:
    secretScope = 'lza-da-kv-001-d'
    hvr_base_url = dbutils.secrets.get(scope = secretScope, key = 'hvr-api-base-url')
    hvr_channel = 'subscribe'#'hvrdevchnl1' # to be sourced from sqlDb
    hvr_hub = 'hvrhubdev'  ##Get this from KeyVault?
    hvr_user_name = dbutils.secrets.get(scope = secretScope, key = 'hvr-databricks-user-name')
    hvr_user_key  = dbutils.secrets.get(scope = secretScope, key = 'hvr-databricks-user-key')   

except Exception as e:
    errorMessage = "Exception occured while obtaining HVR key vault secrets " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)    
    assert False    

# COMMAND ----------

# DBTITLE 1,Set Certificate path
capubliccertificate = "hvr6.pem" #the public certificate
certificateBasePath = "/dbfs/mnt/raw/certificates/" #path to certificates 
hvr_cert = certificateBasePath + capubliccertificate

# COMMAND ----------

# DBTITLE 1,HVR Operation Types dictionary

# Commit_timestamp-->hvr_Seq--> [timestamp][Op_code]
#     Deletes(0,3) -->
#     Update/Insert (1,2)

# , '2':'After update'  --->UPSERT
# , '3':'Before key update'  --> Delete
# , '4':'Before non–key update'-->Ignore
    
##create the opertation type dictionary
op_type_dict = {  '0':'Delete'
                , '1':'Insert'
                , '2':'After update' 
                , '3':'Before key update'
                , '4':'Before non–key update'
                , '5':'Truncate table'
                , '6':'Verbose row-wise compare or refresh before insert and delete'                
                , '7':'UI row-wise compare or refresh'
                , '8':'Delete affecting multiple rows'
                ,'10':'In Doubt Delete - Resilient variant of 0'
                ,'11':'In Doubt Insert - Resilient variant of 1'
                ,'12':'In Doubt After update - Resilient variant of 2'
                ,'13':'In Doubt Before Key update - Resilient variant of 3'
                ,'20':'Poor delete, with missing values'
                ,'21':'Poor insert, with missing values'
                ,'22':'Poor key update with missing values'
                ,'30':'Poor delete, with missing values - Resilient variant of 20'
                ,'31':'Poor insert, with missing values - Resilient variant of 21'
                ,'32':'Poor key update with missing values - Resilient variant of 22'
                ,'41':'Poor insert, with missing values - Key update whose missing values have been augmented - Variant of 21 with augmented values'
                ,'42':'Poor key update with missing values - Key update whose missing values have been augmented - Variant of 22 with augmented values'
                ,'51':'Resilient variant of 41'
                ,'52':'Resilient variant of 42'}
##create the opertation type dictionary
op_type_dict_extended ={ 
     0:{'desc':'Delete' ,'simple_op_code':0, 'process':1},
     1:{'desc':'Insert' ,'simple_op_code':1, 'process':1},
     2:{'desc':'After update' ,'simple_op_code':2, 'process':1},
     3:{'desc':'Before key update' ,'simple_op_code':0, 'process':0},
     4:{'desc':'Before non-key update' ,'simple_op_code':4, 'process':0},
     5:{'desc':'Truncate table' ,'simple_op_code':5, 'process':0},
     6:{'desc':'Verbose row-wise compare or refresh before insert and delete' ,'simple_op_code':99, 'process':0},
     7:{'desc':'UI row-wise compare or refresh' ,'simple_op_code':99, 'process':0},
     8:{'desc':'Delete affecting multiple rows' ,'simple_op_code':0, 'process':1},
    10:{'desc':'Resilient variant of 0' ,'simple_op_code':0, 'process':1},
    11:{'desc':'Resilient variant of 1' ,'simple_op_code':1, 'process':1},
    12:{'desc':'Resilient variant of 2' ,'simple_op_code':2, 'process':1},
    13:{'desc':'Resilient variant of 3' ,'simple_op_code':3, 'process':0},
    20:{'desc':'Poor delete, with missing values' ,'simple_op_code':0, 'process':1},
    21:{'desc':'Poor insert, with missing values' ,'simple_op_code':1, 'process':0},
    22:{'desc':'Poor update, with missing values' ,'simple_op_code':2, 'process':0},
    30:{'desc':'Resilient variant of 20' ,'simple_op_code':0, 'process':1},
    31:{'desc':'Resilient variant of 21' ,'simple_op_code':1, 'process':0},
    32:{'desc':'Resilient variant of 22' ,'simple_op_code':2, 'process':0},
    41:{'desc':'Variant of 21 with augmented values' ,'simple_op_code':1, 'process':0},
    42:{'desc':'Variant of 22 with augmented values' ,'simple_op_code':2, 'process':0},
    51:{'desc':'Resilient variant of 41' ,'simple_op_code':1, 'process':0},
    52:{'desc':'Resilient variant of 42' ,'simple_op_code':2, 'process':0}
}

# COMMAND ----------

# for ele in op_type_dict:
#     if op_type_dict[ele]['process'] == 1:
#         print(ele, op_type_dict[ele]['simple_op_code'] , op_type_dict[ele]['desc'],sep= " | ")

# COMMAND ----------

# DBTITLE 1,Convert dictionary keys to lowercase
# create a very simple json struct so we can parse the outer part of the message.  The inner art will be dealt with using map type so we can manipulate the column names to be lower case.
initial_json_schema = ArrayType(StringType(), True)

#define function to convert the keys of a dictionary to lower case. This will be used and transient for a map type stirng to string. We will not need to specify full struct
def dict_keys_to_lowercase(dictIn):
    return dict(map(lambda item: (item[0].lower(), item[1]), dictIn.items()))

#create the udf based on the above function.
dict_keys_to_lowercase_Udf = udf(dict_keys_to_lowercase, MapType(StringType(), StringType()))

mapping_expr = create_map([lit(x) for x in chain(*op_type_dict.keys())])


# COMMAND ----------

# DBTITLE 1,Get HVR API access Token
#Function to obtaiin HVR API access token
#Returns access_token (string)
def getHvrApiAccesstoken(cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):  
    try:
        logTaskProgress(cursor,batchTaskId,'Attempting to get HVR API access token')
        
        #wrap username and password as payload
        payload = {"username": "" + hvr_user_name +"", "password": "" + hvr_user_key + ""}
        
        #set request headers as json
        headers = {"Content-Type":"application/json"}
        
        #API endpoint to obtain token 
        api_path = '/auth/v1/password'
        
        #build url
        url = hvr_base_url + api_path
        
        #perform request
        query = requests.post(url, json = payload, headers=headers, verify=hvr_cert)                    
        #queryStatusCode = query.status_code
        
        #parse resonse to json
        jsondata = json.loads(query.content)
        
        #extract token
        access_token = jsondata["access_token"]
        
        return "Bearer {}".format(access_token)        
    except Exception as e:
        errorMessage = "Exception occured while obtaining HVR api access token : " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)    
        assert False

# COMMAND ----------

# DBTITLE 1,Get HVR last refresh timestamp by Table name
#Function to get Get recent refresh timestamp for a given table
#Returns string
def getHvrLastRefreshTimestampByTableName(hvrTableName,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):  

    try:
        logTaskProgress(cursor,batchTaskId,f'Attempting to get last Refresh timestamp for the table: [{hvrTableName}]')
        
        #Get Auth bearer Token
        authToken = getHvrApiAccesstoken(cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        #build api end point url
        api_path = f'/api/latest/hubs/{hvr_hub}/channels/{hvr_channel}/refresh/tables_results_ids?table={hvrTableName}'

        #set request headers 
        headers = {"Content-Type":"application/json", "Authorization":"" + authToken + ""}

        url = hvr_base_url + api_path

        #perform request
        query = requests.get(url, headers=headers, verify=hvr_cert)
            
        #parse resonse to json    
        jsondata = json.loads(query.content)
        
        #get timestamp
        result = list(jsondata.keys())[0]

        return jsondata
        
    except Exception as e:
        errorMessage = f"Exception occured while Attempting to get last Refresh timestamp for table: [{hvrTableName}]: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)    
        assert False

# COMMAND ----------

# DBTITLE 1,Get HVR current job state by Job name
#Function to get HVR Refresh State by channel, hub
#Returns string
def getHvrRefreshStateByJobName(jobName,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):  

    try:
        logTaskProgress(cursor,batchTaskId,f'Attempting to get job status for the job: {jobName}')
        
        authToken = getHvrApiAccesstoken(cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        #api path
        apiPath = f'/api/latest/hubs/{hvr_hub}/jobs?job={jobName}'

        #set request headers 
        headers = {"Content-Type":"application/json", "Authorization":"" + authToken + ""}
        
        #build url
        url = hvr_base_url + apiPath

        #perform request
        query = requests.get(url, headers=headers, verify=hvr_cert)
    
        #parse json result
        jsondata = json.loads(query.content)

        #extract refresh state
        jobState = jsondata[jobName]["state"]
        
        #check if job has errors
        jobHasErrors = False 
        if "log_job_err_tstamp" in jsondata[jobName].keys():        
            jobHasErrors = True                    

        return jobState,jobHasErrors
        
    except Exception as e:
        errorMessage = f"Exception occured while obtaining job status by job name: {jobName}" + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)    
        assert False

# COMMAND ----------

#Function to get HVR Event details by jobName
#Returns string
def getHvrEventStateByJobName(jobName,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):  

    try:
        logTaskProgress(cursor,batchTaskId,f'Attempting to get event status for the job: {jobName}')
        
        authToken = getHvrApiAccesstoken(cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        #api path
        apiPath = (f'/api/latest/hubs/{hvr_hub}/jobs?job={jobName}'
                   +'&fetch_results=true'
                   +'&result_pattern=Table_Duration|Table_State|Table_Start_Time|Source_Rows_Used|Subtasks_Done|Subtasks_Total|Subtasks_Busy'
                   +'&type=Refresh'
                   +'&updated_begin=2023-01-29T17:41:30.533Z'
                  )

        #set request headers 
        headers = {"Content-Type":"application/json", "Authorization":"" + authToken + ""}
        
        #build url
        url = hvr_base_url + apiPath

        #perform request
        query = requests.get(url, headers=headers, verify=hvr_cert)
    
        #parse json result
        jsondata = json.loads(query.content)

        #extract refresh state
        jobState = jsondata[jobName]["state"]
        
        #check if job has errors
        jobHasErrors = False 
        if "log_job_err_tstamp" in jsondata[jobName].keys():        
            jobHasErrors = True                    

        return jsondata#jobState,jobHasErrors
        
    except Exception as e:
        errorMessage = f"Exception occured while obtaining job status by job name: {jobName}" + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)    
        assert False