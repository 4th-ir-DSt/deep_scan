# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Instructions for manual ADF Notebook testing
# MAGIC 
# MAGIC <b>Rules to run the notebook:</b>
# MAGIC <ul>
# MAGIC <li>The notebook is to test ADF pipeline run. Each cell has to be <b>run manually as per instructions.</b></li>
# MAGIC <li>Run each cell only once.</li>
# MAGIC <li>Incase of any error in the notebook, Run cell 19 "Metadata cleanup and close database connection" and contact EDS Team.</li>
# MAGIC <li><b>So avoid scrolling back to the instructions, you will find the instructions on this wiki page also (open in new window) 
# MAGIC </b></li></ul>
# MAGIC 
# MAGIC <b>Steps to run the notebook</b>
# MAGIC <ul>
# MAGIC <li>Step 1: Run the notebook from cell 3 "Import Module" to cell 9 "Metadata data cleanup and insertion" sequentially based on 'Run the next cell' message of each cell.</li>
# MAGIC <li>Step 2: Run cell 9: "Check if given ADFTest schedule is running"</li> 
# MAGIC   <ul>
# MAGIC     <li>On success: If cell prints the message 'Run the next cell'.Proceed with the run</li>
# MAGIC     <li>On failure: If cell prints the message 'Don't proceed with the run. Adjust the schedule start and end time to fall in current time window and update other schedule to inactive. Execute the query given in the wiki page <b>Adjust schedule to current time </b></li>
# MAGIC   </ul>  
# MAGIC <li>Step 3: Run cell 10 "Run ADF pipeline manually".when cell prints the message 'Now run the adf pipeline...'. Run the ADF pipeline (<b>follow below instruction to run ADF pipeline</b>) wait until successful completion of pipeline.</li>
# MAGIC 
# MAGIC <li>Step 4: Now run cell 11,12,13,14,15 sequentially based on 'Run the next cell' message of each cell.</li>
# MAGIC <li>Step 5: Run cell 16  "Check if given ADFTest schedule is running"</li> 
# MAGIC   <ul>
# MAGIC     <li>On success: If cell prints the message 'Run the next cell'.Proceed with the run</li>
# MAGIC     <li>On failure: If cell prints the message 'Don't proceed with the run. Adjust the schedule start and end time to fall in current time window and update other schedule to inactive. Execute the query given in the wiki page <b>Adjust schedule to current time </b></li>
# MAGIC   </ul> 
# MAGIC     
# MAGIC <li>Step 6: Run cell 17 "Run ADF pipeline manually".when cell prints the message 'Now run the adf pipeline...'. Run the ADF pipeline (<b>follow below instruction to run ADF pipeline</b>) wait until successful completion of pipeline.</li>  
# MAGIC <li>Step 7: Run the cell 18,19 and complete the ADF pipeline unit test.</li>
# MAGIC </ul>
# MAGIC 
# MAGIC <b>Steps to run the ADF pipeline:</b>
# MAGIC <ul>
# MAGIC <li>Step 1: Login to portal azure https://portal.azure.com/#home with your login credentials and select <b>data factories</b> icon from the Azure services or search Data Factories.Now select the data factory resource of your resource group. </li>
# MAGIC <li>Step 2: Select <b>Author and monitor option(A pencil icon).</b></li>
# MAGIC <li>Step 3: Click on the <b>Author tab(A pencil icon)</b> on the left pane.</li>
# MAGIC <li>Step 4: Now a new prompts ask to <b>create a new branch</b>. give a name and create a new branch.</li>
# MAGIC <li>Step 5: Now under <b>Factory Resources->Pipelines</b> select master pipeline folder ANY->GEN->MASTER->orch_napp_napp_master</li>
# MAGIC <li>Step 6: Now select the <b>ADD TRIGGER-->TRIGGER NOW</b> option at the top</li>
# MAGIC <li>Step 7: Now a new prompt "Pipeline run" will open,select the environment to run and click ok.</li>
# MAGIC <li>Step 8: Click on the monitor tab in the right corner to know the pipeline progress.wait until complete execution of all activities.</li>
# MAGIC </ul>
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>unit_testing_adf_pipeline_master</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Test on adf_pipeline_master</td></tr>
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
# MAGIC     <td> </td>
# MAGIC     <td> </td>
# MAGIC     <td> </td>
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
# MAGIC     <td> Framework</td>
# MAGIC     <td> Modified the test case as per new design of the ADF pipeline</td>
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

# DBTITLE 1,Import Module
try:
  import pandas as pd
  import pyodbc
  from datetime import datetime, timedelta
  from collections import Counter
  import json
except Exception as e:
  assert False

# COMMAND ----------

# DBTITLE 1,Run Logging Function notebook
# MAGIC %run ../../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Initialise variables
try :
  #varible for current_time
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
  
  # declare dummy parameters for creating log_file_path
  batchId = -1
  batchTaskId = -1
  
  #Call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')
  
  #Parameter for logging into tbl_unit_test_result  
  testObject = 'ADF Pipeline'
  testObjectName = 'masterAdfPipeline'  
  requiredInputParameter = 'Run Environment'
  
  sampleOutputLocation = 'NA'
  testInputPath = '/dbfs/mnt/dataquality/unit_tests/{}/input/'.format(testObjectName)
  testOutputPath = '/dbfs/mnt/dataquality/unit_tests/{}/output/'.format(testObjectName)
  testResultsPath = '/dbfs/mnt/dataquality/unit_tests/{}/testResults/'.format(testObjectName)
  
except Exception as e:
  errorMessage = "Exception occurred while variable declaration " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
try:
  dbconn = dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")
  conn = pyodbc.connect(dbconn, autocommit = True)
  cursor = conn.cursor()
except Exception as e:
  errorMessage = 'unable to establish SQL Database connection'
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Function for batch status check, list comparison,construct pipeline parameters
#this function check the status of the batch whether a batch is running or not.
def batchStatusCheck():
  try:
    #check if any other batch is running
    existingBatchCount = pd.read_sql_query("SELECT * FROM audit.tbl_batch WHERE status = 'running'",conn)
    if existingBatchCount.shape[0] == 0:
      return True
    else:
      return False     
  except Exception as e:
    errorMessage = 'batchStatusCheck function call failed'
    logToFile(errorLogFileLocation,errorMessage)
    assert False  

#function to compare two list
def compareList(executionOutputList,spOutputList):
  try:
    result  = Counter(executionOutputList) == Counter(spOutputList)
    return result
  except Exception as e:
    errorMessage = 'compareList function call failed'
    logToFile(errorLogFileLocation,errorMessage)
    assert False

#function to execute stored procedure and retrieve pipeline parameters in json format
def constructParameters(executedBatchId):
  try:
    #create empty list to append the executed parameters
    parameters = []
    #execute get_phase_sourcetype_taskpriority stored procedure and convert as a list
    spOutput = pd.read_sql_query("exec config.usp_get_phase_sourcetype_taskpriority {}".format(executedBatchId),conn)
    phaseId = spOutput['phase_id'].tolist()
    sourceTypeId = spOutput['source_type_id'].tolist()
    taskPriority = spOutput['task_priority'].tolist()
    
    for i in range(0,spOutput.shape[0]):
      phaseIdParam = phaseId[i]
      sourceTypeIdParam = sourceTypeId[i]
      taskPriorityParam = taskPriority[i]
      param = '{"batch_id": '+str(executedBatchId)+',"phase_id": '+str(phaseIdParam)+',"source_type_id":'+str(sourceTypeIdParam)+',"task_priority":'+str(taskPriorityParam)+'}'
      parameters.append(param)
  #select distinct list       
    return list(set(parameters))
  except Exception as e: 
    errorMessage = 'constructParameters function call failed'
    logToFile(errorLogFileLocation,errorMessage)
    assert False

#function to execute metadata cleanup scripts
def cleanup():
  try:
    cleanup = open("{}masterAdfPipelineCleanup.sql".format(testInputPath), "r").read()
    cursor.execute(cleanup)
    while cursor.nextset():
      x = 1 
  except Exception as e:
    print("cleanup scripts execution failed")
    assert False

# COMMAND ----------

# DBTITLE 1,Metadata data cleanup and insertion
#execute scripts to insert data into metadata table
try:  
  
  #check status of previous batch,if no batch is running then populate data in table
  batchStatus = batchStatusCheck()
  if batchStatus == True:
    
    #call the cleanup function to perform metadata cleanup before run
    cleanup()
    
    #retrieve current cluster information
    notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
    currentClusterId = notebook_info["tags"]["clusterId"] 
    workspaceURI = notebook_info["extraContext"]["api_url"] 

    #execute scripts to populate data
    inputParameter = open("{}masterAdfPipelineInput.sql".format(testInputPath), "r").read()
    inputParameter1 = inputParameter.replace("<cluster_id>",str(currentClusterId))
    inputParameter2 = inputParameter1.replace("<workspaceURI>",str(workspaceURI))
    inputParameterResults = pd.read_sql_query(inputParameter2,conn)

    #get the scheduleReference as input parameter for batch start
    inputScheduleReference = str(inputParameterResults.at[0,'scheduleReference'])
    print('Run the next cell')
  else:
    print("A batch is already running. Please contact EDS Team")
except Exception as e:
  errorMessage = 'Metadata cleanup or insertion failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Check if given ADFTest schedule is running 
try:
  #check if any other batch is running
  batchStatus = batchStatusCheck()
  if batchStatus == True:
    #retrieve the current schedule
    scheduleOutput = pd.read_sql_query("exec config.usp_get_schedule_reference",conn)
    currentSchedule = scheduleOutput.at[0,'schedule_reference']

    #check if the current schedule is adf test
    if currentSchedule == inputScheduleReference:
      print('Proceed the next cell')  
    else:
      #when the current schedule reference is not present in the result, then check the other schedule make it as inactive to run the pipeline or adjust your schedule time to make fall in current time window.
      # refer the sql scripts in the wiki page.
      print("Don't proceed with the run. Adjust the schedule start and end time to fall in current time window and update other schedule to inactive")
except Exception as e:
  cleanup()
  errorMessage = 'The adfTest schedule is failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False    

# COMMAND ----------

# DBTITLE 1,Run ADF pipeline manually 
print("Now run the adf pipeline (Follow the instruction mentioned at top of this notebook to run the ADF pipeline. Wait until successful completion and if the pipeline succeeds proceed with the next cell)")


# COMMAND ----------

# DBTITLE 1,Get the executed batch_id from the run
#after the adf pipeline run, retrieve the batch_id by querying the batch table, the batch_id is used for other test cases.
try:
  executedBatchId = pd.read_sql_query("SELECT TOP 1 bt.batch_id FROM audit.tbl_batch bt JOIN config.tbl_schedule se ON bt.schedule_id = se.schedule_id WHERE se.schedule_reference = 'adfMasterTestschedule' ORDER BY bt.batch_id DESC",conn).at[0,'batch_id']
  print('The executed batch_id for adf Testing is '+ str(executedBatchId))
except Exception as e:
  cleanup()
  errorMessage = 'Unit test on scenario 1 test failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False 
  

# COMMAND ----------

# DBTITLE 1,Scenario-1 Verify the task pipeline is executed and logged in adf_pipeline_execution table
# After pipeline run,take records from adf_pipeline_execution table with the executed batch_id.
#check if the inner task pipeline is executed.

try:
    executionOutputStatus = 'Mismatched'
    testCaseStatus = 'failed'
    testCaseScenario = 'Check if task pipelines has invoked'

    #Take the distinct pipeline_name
    pipelineOutput = pd.read_sql_query("SELECT DISTINCT(adf_pipeline_name) FROM audit.tbl_log_adf_pipeline_execution WHERE batch_id = {}".format(executedBatchId),conn)
   
    #compare the inner pipleine name inner
    if pipelineOutput.at[0,'adf_pipeline_name'] == 'orch_napp_napp_forEachTask':
      executionOutputStatus = 'As expected'
      testCaseStatus = 'success'    
      
    print("Run the next cell")     
    #function to log unit test results in the table
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                      testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)  
except Exception as e:
  cleanup()
  errorMessage = 'ADF Unit test on scenario 1 test failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-2 Compare the parameters executed
#compare the output of executed ADF pipeline parameters and stored procedure parameters
#take both parameters as list and compare using compareList function

try:
    executionOutputStatus = 'Mismatched'
    testCaseStatus = 'failed'
    testCaseScenario = 'Compare the executed pipeline parameter'

    # call the constructParameters functions to get stored procedure executed parameters as a list
    spOutputList = constructParameters(executedBatchId)

    #take the parameter from log_pipeline_execution table output
    executionOutputList = pd.read_sql_query("SELECT  DISTINCT(pipeline_parameters) FROM audit.tbl_log_adf_pipeline_execution WHERE batch_id = {} AND adf_pipeline_name = 'orch_napp_napp_forEachTask'".format(executedBatchId),conn)['pipeline_parameters'].tolist()
    
    # to get rid of '\n' from the executionOutputList result
    finalExecutionOutputList = []
    for jsonList in executionOutputList:
      newExecutionOutput = jsonList.replace("\n", "")
      finalExecutionOutputList.append(newExecutionOutput)
    
    #call the function to compare the parameters
    result = compareList(finalExecutionOutputList,spOutputList)

    #check the result if true,then the parameters are matching
    if result == True:
      executionOutputStatus = 'As expected'
      testCaseStatus = 'success'     
      
     #function to log unit test results in the table
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                      testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
    print("Run the next cell")  
except Exception as e:
  cleanup()
  errorMessage = 'ADF Unit test on scenario 2 failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False 

# COMMAND ----------

# DBTITLE 1,Scenario-3 Check if the progress of the pipeline is logged in to table
#check whether the pipeline logged its progress.
try:
    executionOutputStatus = 'Mismatched'
    testCaseStatus = 'failed'
    testCaseScenario = 'Check for logging'
    
    #check the logging entry in tbl_log_adf_pipeline_progress,tbl_log_adf_pipeline_execution by the pipeline.
    pipelineLogging = pd.read_sql_query("select * from audit.tbl_log_adf_pipeline_progress where batch_id = {}".format(executedBatchId),conn)
    pipelineExecution = pd.read_sql_query("select * from audit.tbl_log_adf_pipeline_execution where batch_id = {}".format(executedBatchId),conn)
    
    #check if the row count is greater than 1
    if pipelineLogging.shape[0] >= 1 and pipelineExecution.shape[0] >= 1:
      executionOutputStatus = 'As expected'
      testCaseStatus = 'success'     
      
     #function to log unit test results in the table
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                      testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
    print("Run the next cell")  
    
except Exception as e:
  cleanup()
  errorMessage = 'ADF Unit test on scenario 3 failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False     

# COMMAND ----------

# DBTITLE 1,Scenario-4 Data Preparation 
#execute scripts to insert data into metadata table
try:  
  
  #check status of previous batch,if no batch is running then populate data in table
  batchStatus = batchStatusCheck()
  if batchStatus == True:
    
    #call the cleanup function to perform metadata cleanup before run
    cleanup()
    
    #retrieve current cluster information
    notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
    currentClusterId = notebook_info["tags"]["clusterId"] 
    workspaceURI = notebook_info["extraContext"]["api_url"] 

    #execute scripts to populate data
    inputParameter = open("{}masterAdfPipelineInput.sql".format(testInputPath), "r").read()
    inputParameter1 = inputParameter.replace("<cluster_id>",str(currentClusterId))
    inputParameter2 = inputParameter1.replace("<workspaceURI>",str(workspaceURI))
    
    #updating the notebook_name as its not found in the location
    inputParameter3 = inputParameter2.replace("_run_notebook",'_error_notebook')
    inputParameterResults = pd.read_sql_query(inputParameter3,conn)
    
    #get the scheduleReference as input parameter for batch start
    inputScheduleReference = str(inputParameterResults.at[0,'scheduleReference'])
    print('Run the next cell')
  else:
    print("A batch is already running. Please contact EDS Team")
except Exception as e:
  errorMessage = 'ADF Metadata cleanup or insertion failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Check if the given ADFTest schedule is active
try:
  #check if any other batch is running
  batchStatus = batchStatusCheck()
  if batchStatus == True:
    #retrieve the current schedule
    scheduleOutput = pd.read_sql_query("exec config.usp_get_schedule_reference",conn)
    currentSchedule = scheduleOutput.at[0,'schedule_reference']

    #check if the current schedule is adf test
    if currentSchedule == inputScheduleReference:
      print('Proceed the next cell')  
    else:
      #when the current schedule_reference is not present in the result, then check the other schedule and wait until its completion or if no schedule is running adjust your schedule time to make fall in current time window.
      # refer the sql scripts in the wiki page.
      print("Don't proceed with the run. Adjust the schedule start and end time to fall in current time window. or if other schedule is running, wait until its completion or contact EDS team")
except Exception as e:
  cleanup()
  errorMessage = 'The adfTest schedule is failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False  

# COMMAND ----------

# DBTITLE 1,Scenario - 4 Run the ADF Pipeline
print("Now run the adf pipeline (Follow the instruction mentioned at top of this notebook to run the ADF pipeline. Wait until successful completion and if the pipeline succeeds proceed with the next cell)")

# COMMAND ----------

# DBTITLE 1,Scenario 4 - Negative test case
#The target notebook name is updated with a incorrect values by the scripts.so the pipeline logs into error table.
#this test case is to check the entry in the error_log table for the batch_id.
try:
    executionOutputStatus = 'Mismatched'
    testCaseStatus = 'failed'
    testCaseScenario = 'Check the entry in error_log table for the pipeline failure'
    
    #get the newly executed batch_id from the pipeline
    errorBatchId = pd.read_sql_query("SELECT TOP 1 bt.batch_id FROM audit.tbl_batch bt JOIN config.tbl_schedule se ON bt.schedule_id = se.schedule_id WHERE se.schedule_reference = 'adfMasterTestschedule' ORDER BY bt.batch_id DESC",conn).at[0,'batch_id']
    
    #check the entry in error_log_table
    errorEntry = pd.read_sql_query("select * from audit.tbl_error_log where batch_id = {}".format(errorBatchId),conn)

    #check the count of record is greater than 1.
    if errorEntry.shape[0] >= 1:
      executionOutputStatus = 'As expected'
      testCaseStatus = 'success'    

    #function to log unit test results in the table
    logUnitTestStatus(testObject,testObjectName,requiredInputParameter,
                      testCaseScenario,executionOutputStatus,sampleOutputLocation,
                      datetime.now(),testCaseStatus)
    print("Run the next cell")   
except Exception as e:
  cleanup()
  errorMessage = 'Unit test on Scenario 4 failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False 

# COMMAND ----------

# DBTITLE 1,Metadata cleanup and close database connection
#run cleanup script and close database connection
try:
  cleanup()
  conn.close()
except Exception as e:
  errorMessage = 'cleanup scripts execution failed'
  logToFile(errorLogFileLocation,errorMessage)
  assert False 