# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>logging_functions</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Functions that logs the success and failure of batch task in MetaData table</td></tr>
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
# MAGIC     <td>Added function log_mark_dates_loaded. Also removed registering of functions as udf's</td>
# MAGIC          <br>Added function log_unit_test_status,added code makedir in log_to_file,removed a parameter from log_availability dates.
# MAGIC          <br>modified get_log_file_path function and added import os module.
# MAGIC          <br>modified functions, variables and parameters naming conventions to camelCase.
# MAGIC          <br>corrected number of passing parameter in logError function.
# MAGIC          <br>Added source object id to mark dates loaded.
# MAGIC          <br>Added additional functions logProgressWithRetry that will retry log progress with exponential back off and reestablish the databse connection.
# MAGIC          <br>Added additional functions logProgressWithRetry that will retry log progress with exponential back off and reestablish the databse connection.
# MAGIC          <br>updated sqlDbConn function call in logtaskProgresswithRetry with necessary parameters.
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
from datetime import datetime, timedelta
import os
import uuid
import time

# COMMAND ----------

# DBTITLE 1,Function get log file path
#this function generate a specific logging file location as a string
def getLoggingPath(batchId, batchTaskId, date, logType):
#path format looks similar to '/mnt/log/databricks/error/date=2020-01-20/batchId=1/batchTaskId=1/12:30:54/
  logFileLocation = '/dbfs/mnt/log/databricks/' + 'error' + '/date='+date.strftime("%Y-%m-%d")+'/batchId='+str(batchId)+'/batchTaskId='+str(batchTaskId)+'/'+date.strftime("%H:%M:%S")+'/'
  return logFileLocation 

# COMMAND ----------

# DBTITLE 1,Function create log file
#This function will take a logFileLocation as a parameter, checks if the file exists, and if not it creates and writes  message in the log file.
def logToFile(logFileLocation, logMessage):
  try: 
    #create a unique filename so that previous log errors are note overwritten
    uniqueFileName = str(uuid.uuid4()).replace("-","")[0:10]+".log"
    if not os.path.exists(logFileLocation):
      os.makedirs(logFileLocation)  
    f = open(logFileLocation + uniqueFileName,"w+")
    f.write(str(datetime.now()) + " : " + logMessage)
    f.close()
    return True 
  except Exception as e :
    return False

# COMMAND ----------

# DBTITLE 1,Function Log progress DB
# This function executes the log progress stored procedure to makes an entry about the success of the try block in log progress table
def logTaskProgress(cursor,batchTaskId,batchTaskProgressMessage):
  try:
    cursor.execute("exec audit.usp_log_task_progress ?,?",batchTaskId,batchTaskProgressMessage)
    while cursor.nextset():
      x = 1
    return True
  except Exception as e:
    #raise exception and caught in main catch block
    raise Exception("Unable to log progress in to DB", e)

# COMMAND ----------

# DBTITLE 1,Function Log progress DB with retry
# This function executes the log progress stored procedure to makes an entry about the success of the try block in log progress table
def logTaskProgressWithRetry(dbconn, conn, cursor, batchTaskId, batchTaskProgressMessage):
  try:
    #initialise variables for retry
    retryCount = 0
    maxRetries = 3
    backoffWaitTime = 10
    backoffFactor = 2 #we will multiply the value of backoffWaitTime by this value to wait a longer period of time
    haveDbConnection = True #initialise to True assuming that we have a connection
    
    while ( retryCount <= maxRetries ):
      try:
        if (retryCount > 0):
          time.sleep(backoffWaitTime) #wait for some seconds
          #increment the back off time for next time
          backoffWaitTime = backoffWaitTime * backoffFactor

        #if there isn't a db connection, then we need to recreate it
        if haveDbConnection == False:
          #call function sqlDbConn to establish Database connection with given scope and key values
          conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
          haveDbConnection = True

        cursor.execute("exec audit.usp_log_task_progress ?,?",batchTaskId,batchTaskProgressMessage)
        while cursor.nextset():
          x = 1
        #we can return from here as we now have completed the call to logMarkDatesLoaded
        return conn,cursor
      
      except Exception as e:
        haveDbConnection = False
        if ( retryCount == maxRetries ):
          #if it still hasn't succeded at this point and we have reached maximum retries then we just throw the exception
          raise e
        retryCount = retryCount + 1 #increment the retries   
        
  except Exception as e:
    #raise exception and caught in main catch block
    raise Exception("Unable to log progress in to DB", e)

# COMMAND ----------

# DBTITLE 1,Function Log task error
# This function is used in the main exception block to execute the logError stored procedure to makes an entry about the failure in log error table. also, if error is not captured in table, it writes into the file through logToFile(logFileLocation,errorMessage) function.
def logError(cursor,batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName,errorLogFileLocation): 
  try:
    cursor.execute("exec audit.usp_log_error ?,?,?,?,?",batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName)
    while cursor.nextset():
      x = 1
  except Exception as e:
# function call to  write into the log file
    logToFile(errorLogFileLocation,errorMessage) 

# COMMAND ----------

# DBTITLE 1,Function Log task end
#this function is used in last try block that marks the batchTaskStatus as completed on successful execution
def logTaskEnd(cursor,batchTaskId, batchTaskStatus, batchTaskSourceRows, batchTaskRowsLoaded, batchTaskRejectRows, batchTaskResult, batchTaskResultLocation):
  try:
    cursor.execute("exec audit.usp_log_task_end ?,?,?,?,?,?,?",batchTaskId,batchTaskStatus,batchTaskSourceRows,batchTaskRowsLoaded,batchTaskRejectRows,batchTaskResult,batchTaskResultLocation)
    while cursor.nextset():
      x = 1
    return True
  except Exception as e:
    #raise exception to outer except block
    raise Exception("Unable to log progress in to DB",e)
    return False

# COMMAND ----------

# DBTITLE 1,Function Log task start
def logTaskStart(cursor, batchTaskId):
  try:
    cursor.execute("exec audit.usp_log_task_start ?", batchTaskId)
    while cursor.nextset():
      x = 1
    return True
  except Exception as e:
    #raise exception to outer except block
    raise Exception("Unable to log task start",e)
    return False   

# COMMAND ----------

# DBTITLE 1,Function log availability dates
def logAvailabilityDates(cursor,batchTaskId, objectId, datetimeTo):
  try:
    cursor.execute("exec audit.usp_log_availability_dates ?,?,?", batchTaskId, objectId, datetimeTo)
    while cursor.nextset():
      x = 1
    return True
  except Exception as e:
    #raise exception to outer except block
    raise Exception("Unable to log availability dates",e)
    return False

# COMMAND ----------

# DBTITLE 1,Function log mark dates loaded
def logMarkDatesLoaded(cursor,batchTaskId, destinationObjectId, sourceObjectId, datetimeFrom, datetimeTo):
  try:
    cursor.execute("exec audit.usp_mark_dates_loaded ?,?,?,?,?", batchTaskId, destinationObjectId, sourceObjectId, datetimeFrom, datetimeTo)
    while cursor.nextset():
      x = 1
    return True
  except Exception as e:
    #raise exception to outer except block
    raise Exception("Unable to mark dates as loaded",e)
    return False

# COMMAND ----------

# DBTITLE 1,Function log unit test
def logUnitTestStatus(testObject,testObjectName,requiredInputParameter,testCaseScenario,executionOutputStatus,sampleOutputLocation,executionDateTime,testCaseStatus):
  try:
    cursor.execute("exec [audit].[usp_log_unit_test] ?,?,?,?,?,?,?,?",testObject,testObjectName,requiredInputParameter,testCaseScenario,
                 executionOutputStatus,sampleOutputLocation,executionDateTime,testCaseStatus)
    while cursor.nextset():
       x = 1
    return True
  except Exception as e:
    #raise exception to outer except block
    raise Exception("Unable to log the status into unit test table",e)
    return False