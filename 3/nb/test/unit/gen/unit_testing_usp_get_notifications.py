# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_usp_get_notifications</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>unit test on stored procedure usp_get_notifications</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework/td></tr>
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
# MAGIC     <td>adfPipeLineName</td>
# MAGIC     <td>@adfPipeLineName to retrieve DataFactory pipelinename details</td>
# MAGIC     <td>@adfPipeLineName = 'testpipepline'</td>
# MAGIC   </tr>
# MAGIC    <tr>
# MAGIC     <td>NotebookName</td>
# MAGIC     <td>@NotebookName to retrieve notebookname </td>
# MAGIC     <td>@NotebookName = 'testnotebook'</td>
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
# MAGIC     <td>25/06/2020 </td>
# MAGIC     <td>Praveen </td>
# MAGIC     <td>Updated testcases as per new changes to stored procedure </td>
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

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run Standard Function notebook
# MAGIC %run ../../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Import Module
try:
  import pandas as pd
  import pyodbc
  from datetime import datetime, timedelta
except Exception as e:
  assert False 

# COMMAND ----------

# DBTITLE 1,Initialise Variables
try:
  #VARIBLE FOR CURRENT_TIME
  currentTs = datetime.now()
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
  date = currentTs
  
  #PARAMETER FOR LOG_ERROR
  errorMessage = ''
  adfPipelineName = ''
  clusterId = ''
  notebookName = ''
  batchId = -1
  batchTaskId = -1 
  #parameter for log_task_end
  batchTaskSourceRows = ''
  batchTaskRowsLoaded = ''
  batchTaskRejectRows = '' 
  batchTaskResultLocation = ''
  batchTaskResult = '' 
  #Parameter for logging into tbl_unit_test_result
  testCaseScenario = ''
  sampleOutputLocation = ''
  testObject = 'stored procedure'
  testObjectName = 'uspGetNotifications'
  requiredInputParameter = 'batchId'
  #path for datasets used in stored procedure 
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
except Exception as e:
  assert False  

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables
try:   
  # Get input parameters for stored procedure
  dbutils.widgets.text("batchId","")
  batchId = dbutils.widgets.get("batchId")
  
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName = dbutils.widgets.get("adfPipelineName")
  
  dbutils.widgets.text("clusterId","")
  clusterId = dbutils.widgets.get("clusterId")
  
  dbutils.widgets.text("notebookName","")
  notebookName = dbutils.widgets.get("notebookName")
  
  dbutils.widgets.text("sourceId","")
  sourceId = dbutils.widgets.get("sourceId")
   
  #GET batchTaskId FROM WIDGETS
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")
 
  #Call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date,'error')

except Exception as e:
  errorMessage = "Exception occurred while getting parameters and initialising error log location " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn,cursor = sqlDbConn(dbconn,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,'Established Database connection successfully')
except Exception as e:
  errorMessage = "Exception occurred while connecting to database: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Perform cleanup from previous runs
#run cleanup script
try:
  cleanup = open("{}uspGetNotificationsCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage="Exception occurred while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Input parameter for stored procedure
#execute scripts to generate input parameter for stored procedure
try:  
  inputParameter = open("{}uspGetNotificationsInput.sql".format(testInputPath), "r").read()
  
  inputParameterResults = pd.read_sql_query(inputParameter,conn)
  
  #get the inputs parameters
  batchWithErrors = inputParameterResults.at[0,'batch_with_errors']
  batchWithoutErrors = inputParameterResults.at[0,'batch_without_errors']
  notificationInactive = inputParameterResults.at[0,'notification_inactive']
  scheduleDescription = inputParameterResults.at[0,'schedule_description']
  batchStatus = inputParameterResults.at[0,'batch_status_whithout_errors']
  inactiveGroup = inputParameterResults.at[0,'inactive_group_recipient']
  inactivePerson = inputParameterResults.at[0,'inactive_person_recipient']
  group1 = inputParameterResults.at[0,'group1_recipient']
  group2 = inputParameterResults.at[0,'group2_recipient']
  errorsGroup = inputParameterResults.at[0,'group_errors_recipient']
  
  logTaskProgress(cursor,batchTaskId,'Execution of script to generate input parameter for usp_get_notifications')  
except Exception as e:
  errorMessage = "Exception occurred while executing script to generate input parameter for usp_get_notifications " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Run stored procedure
#run the stored procedure 2 times: with a batch without errors and with a batch with errors and store the results to test
try:
  spOutputWithoutErrors = pd.read_sql_query("exec notification.usp_get_notifications {}".format(batchWithoutErrors),conn)
  spOutputWithErrors = pd.read_sql_query("exec notification.usp_get_notifications {}".format(batchWithErrors),conn)

  logTaskProgress(cursor,batchTaskId,'Run stored procedure usp_get_sources')  
except Exception as e:
  errorMessage="Exception occurred while running the stored procedure usp_get_notifications : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 1 - Check stored procedure output for batch without errors
#the stored procedure should return 3 rows as outupt since the created notification is connected to 4 groups but 1 is inactive
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check if stored procedure returned the expected result count for batch without errors'
  #read stored procedure result as pandas dataframe
  if spOutputWithoutErrors.shape[0] == 3:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'   
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 1 stored procedure usp_get_notifications')  
except Exception as e:
  errorMessage="Exception occurred while testing scenario 1 stored procedure usp_get_notifications : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 2 - Check if the inactive notification is not returned
#based on the notification_id, check if the inactive notification is not on the returned resultset
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check if inactive notification is not returned'
  #filter the pandas dataframe for the value we are not expecting to be included
  spOutputScenario2 = spOutputWithoutErrors[spOutputWithoutErrors['notification_log_id']== notificationInactive]

  if spOutputScenario2.shape[0] == 0:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'    
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 2 stored procedure usp_get_notifications')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 2 stored procedure usp_get_notifications : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 3 - Test email subject
#check if the e-mail subject contains the batch_id, schedule description and status
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check email subject'
  
  #get the subject
  spOutputWithoutErrorsDict = eval(spOutputWithoutErrors.at[0,'notification_body'])
  notificationSubject = spOutputWithoutErrorsDict['subject']
  
  #test the subject for batch_id, schedule description and status
  if (str(batchWithoutErrors) in notificationSubject and scheduleDescription in notificationSubject and batchStatus in notificationSubject):
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'   
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 3 stored procedure usp_get_notifications')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 3 stored procedure usp_get_notifications : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 4 - Test email body
#check if the e-mail body contains the batch_id, schedule description, status, duration and number of errors
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check email body'
  
  #get the body
  notificationBody = spOutputWithoutErrorsDict['content']
  #data to test 
  batchToTest = 'Batch Id: ' + str(batchWithoutErrors)
  scheduleToTest = 'Schedule: ' + scheduleDescription
  statusToTest = 'Status: ' + batchStatus
  durationToTest = 'Duration: 2 seconds'
  numbErrorsToTest = 'Number of errors: 0'
  
  #test the body for batch_id, schedule description, status, duration and number of errors
  if (batchToTest in str(notificationBody) and scheduleToTest in str(notificationBody) and statusToTest in str(notificationBody) and durationToTest in str(notificationBody) and numbErrorsToTest in str(notificationBody)):
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'   
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 4 stored procedure usp_get_notifications')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 4 stored procedure usp_get_notifications : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 5 - Test email recipients
#check if the inactive person is not in the recipients
#check if the inactibe group is not in the recipients
#check if there are notifications for group1 and group2
#check if the group only errors is not in the recipients
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check email recipients'
  #get the recipients for the 2 rows
  spOutputWithoutErrorsDict1 = eval(spOutputWithoutErrors.at[0,'notification_body'])
  recipients1 = spOutputWithoutErrorsDict1['personalizations'][0]['to'][0]['email']
  spOutputWithoutErrorsDict2 = eval(spOutputWithoutErrors.at[1,'notification_body'])
  recipients2 = spOutputWithoutErrorsDict2['personalizations'][0]['to'][0]['email']
  
  #test the recipients on both notification rows
  if (inactivePerson != recipients1 and inactivePerson != recipients2 and 
      inactiveGroup != recipients1 and inactiveGroup != recipients2 and (group1 == recipients2 or group2 == recipients2) and 
      errorsGroup != recipients1 and errorsGroup != recipients2):
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success' 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 5 stored procedure usp_get_notifications')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 5 stored procedure usp_get_notifications : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 6 - Check stored procedure output for batch with errors
#the stored procedure should return 4 rows as outupt since this time there are errors and there's one group that only receives in this case and the other 4 generic groups
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check if stored procedure returned the expected result count for batch with errors'
  #read stored procedure result as pandas dataframe
  if spOutputWithErrors.shape[0] == 4:
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success' 
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 6 stored procedure usp_get_notifications')  
except Exception as e:
  errorMessage="Exception occurred while testing scenario 6 stored procedure usp_get_notifications : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Scenario 7 - Test email body for number of errors
#check if the e-mail body contains the total number of errors -> 10
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check email body for total number of errors'
  
  #get the recipients for the 2 rows
  spOutputWithErrorsDict = eval(spOutputWithErrors.at[0,'notification_body'])
  notificationBodyWithErrors = spOutputWithErrorsDict['content']
  #data to test 
  numbErrorsToTest = 'Number of errors: 10'
  
  #test the body for batch_id, schedule description, status, duration and number of errors
  if (numbErrorsToTest in str(notificationBodyWithErrors)):
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'   
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 7 stored procedure usp_get_notifications')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 7 stored procedure usp_get_notifications : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 8 - Test body HTML for errors table
#check if the e-mail body contains the HTML tag </table> and if there are 6 rows tags </tr> (1 for the header and 5 for the top 5 errors) 
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check email body for html errors table'
  
  #data to test 
  tableTag = '</table>'
  rowTag = '</tr>'
  #test the body for batch_id, schedule description, status, duration and number of errors
  if (tableTag in str(notificationBodyWithErrors) and str(notificationBodyWithErrors).count(rowTag) == 6):
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'    
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 8 stored procedure usp_get_notifications')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 8 stored procedure usp_get_notifications : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario 9 - Check if the group only errors is part of the recipients
#check if the group only errors is in the recipients
try:
  executionOutputStatus = 'Mismatched'
  testCaseStatus = 'failed'
  testCaseScenario = 'Check if group only errors in in the email recipients'
  
  #get the recipients for the 3 rows
  spOutputWithErrorsDict1 = eval(spOutputWithErrors.at[0,'notification_body'])
  recipients1 = spOutputWithErrorsDict1['personalizations'][0]['to'][0]['email']
  spOutputWithErrorsDict2 = eval(spOutputWithErrors.at[1,'notification_body'])
  recipients2 = spOutputWithErrorsDict2['personalizations'][0]['to'][0]['email']
  spOutputWithErrorsDict3 = eval(spOutputWithErrors.at[2,'notification_body'])
  recipients3 = spOutputWithErrorsDict3['personalizations'][0]['to'][0]['email']
  
  #test the recipients on the 3 notification rows
  if (errorsGroup == recipients1 or errorsGroup == recipients2 or errorsGroup == recipients3):
    executionOutputStatus = 'As expected'
    testCaseStatus = 'success'    
  logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                       testCaseScenario,executionOutputStatus,sampleOutputLocation,
                       datetime.now(),testCaseStatus)
  logTaskProgress(cursor,batchTaskId,'Unit test performed on scenario 9 stored procedure usp_get_notifications')  
except Exception as e:
  errorMessage = "Exception occurred while testing scenario 9 stored procedure usp_get_notifications : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Metadata cleanup
#run cleanup script
try:
  cleanup = open("{}uspGetNotificationsCleanup.sql".format(testInputPath), "r").read()
  cursor.execute(cleanup)
  while cursor.nextset():
    x = 1
  logTaskProgress(cursor,batchTaskId,'Successfully executed clean up scripts')   
except Exception as e:
  errorMessage="Exception occurred while executing clean up scripts: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Close database connection
#call taskEndAndCloseConn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor,conn,batchTaskId , batchTaskSourceRows,
                          batchTaskRowsLoaded,batchTaskRejectRows, batchTaskResult,
                          batchTaskResultLocation,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
except:
  assert False

# COMMAND ----------

