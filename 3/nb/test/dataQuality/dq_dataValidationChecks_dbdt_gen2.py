# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>dq_dataValidationChecks_dbdt_gen2</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>integration notebook for data checks</td></tr>
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
# MAGIC   <tr>
# MAGIC     <td>sourceName</td>
# MAGIC     <td>@sourceName to retrieve source details</td>
# MAGIC     <td>@sourceName = sourceName</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>EntityName</td>
# MAGIC     <td>@Entity to retrieve source details</td>
# MAGIC     <td>@Entity = objectName</td>
# MAGIC   </tr>
# MAGIC   
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
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC   </tr> 
# MAGIC    
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

# DBTITLE 1,Import Modules
try:
  import pandas as pd
  import pyodbc
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DecimalType 
  from datetime import datetime,timedelta
  from pyspark.sql.functions import lit,col,explode,concat_ws,struct,collect_list,lower,split
  from functools import reduce
  from itertools import chain 
#   import great_expectations as ge
  import json
  import uuid
except Exception as e:
  errorMessage="Exception occured while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialize variables
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
  
  #PARAMETER FOR LOG_ERROR
  errorLine = ''
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
  testRunId=uuid.uuid4()
  targetPath='/mnt/dataquality/dataValidationChecks'
  targetFormat='parquet'
except Exception as e:
  error_message = "Exception occurred while variable initialisation :" + str(e)
  assert False

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Get SQL connection Details
# import pandas as pd
# import pyodbc
#access secret of database connection details from azure key vault
dbconn=dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')
conn = pyodbc.connect(dbconn, autocommit = True)
cursor = conn.cursor()

# COMMAND ----------

# DBTITLE 1,SourceList
sourceDetails=pd.read_sql_query("select distinct source_name from config.tbl_source",conn).fillna('')
#sourceDetails

# COMMAND ----------

# DBTITLE 1,Create SourceWidget
dbutils.widgets.dropdown("1.sourceName", "Intrali4444", [str(x) for x in sourceDetails['source_name']])

#for x in selectedSource:
#  print(x)

# COMMAND ----------

# DBTITLE 1,Get SelectedSource
selectedSource=dbutils.widgets.get('1.sourceName')
# selectedSourceList="'"+selectedSource+"'"

# COMMAND ----------

# DBTITLE 1,Get ObjectDetails
objectDetails=pd.read_sql_query("exec [dq].[usp_get_sourceEntityObjects] {}".format(selectedSource),conn)

# COMMAND ----------

# DBTITLE 1,Create Entity Widget
dbutils.widgets.multiselect("2.Entitiy", 'ALL', [str(x) for x in (objectDetails['entity_name'].append(pd.Series(['ALL']))).unique()])

# COMMAND ----------

# DBTITLE 1,Get Entity List
EntityList=dbutils.widgets.get('2.Entitiy')

# COMMAND ----------

# DBTITLE 1,Get The Test suits to Run
testSuitDetails=pd.read_sql_query("select name,notebook_path,result_Location from [dq].[tbl_test_suite] where is_active=1",conn)

# COMMAND ----------

# DBTITLE 1,TestSuiteWidget
dbutils.widgets.multiselect("3.TestSuite", 'ALL', [str(x) for x in (testSuitDetails['name'].append(pd.Series(['ALL']))).unique()])

# COMMAND ----------

testSuiteList=dbutils.widgets.get('3.TestSuite')

# COMMAND ----------

# DBTITLE 1,Test Runs
#Run all thest cases if TestSuite is ALL
if testSuiteList=='ALL':
  filteredTestSuitDetails=testSuitDetails
else:
  filteredTestSuitDetails=testSuitDetails[testSuitDetails.name.isin(testSuiteList.split(','))]
  
records = filteredTestSuitDetails.to_records(index=False)
recordlist = list(records)
#print(len(records))
if (bool(recordlist) == True):
  for test_Details in recordlist: 
    testSuitName=test_Details[0]
    notebookPath=test_Details[1]
    resultLocation=test_Details[2]
    dbutils.notebook.run(notebookPath, 86400, {"1.sourceName": selectedSource, "2.Entitiy": EntityList,"3.TestRunId":str(testRunId)})
    #read file from folder
    resultCount = pd.read_parquet('/dbfs' + resultLocation)
    #check for empty
    if (resultCount.empty == False):
      #resultCount = pd.read_parquet('/dbfs' + resultLocation, columns=['TestRunId'], filters=[('TestRunId','=', str(testRunId))])
      #resultCount = resultCount[resultCount['TestRunId'] == str(testRunId)][['TestRunId']]
      resultCount = resultCount.loc[resultCount['TestRunId'] == str(testRunId), ['TestRunId']]
      if (resultCount['TestRunId'].count() > 0):
        #get result into sparkDF
        resultSet=(spark.read
                   .format('parquet')
                   .load(resultLocation)
                   .where("TestRunId='''{}'''".format(str(testRunId)))
                   .select('source_name','entity','ObjectName','TestScenario','attributeToCheck','TestRunDate','TestRunHour','Zone','TestResult'))
        try:
          if FinalResultSet.count()>=1:
            FinalResultSet=FinalResultSet.union(resultSet)
            outputGenerated = True
        except Exception as e:
           #print('Batch Running',str(e))
           FinalResultSet=resultSet

# COMMAND ----------

# DBTITLE 1,Write DataFrame
if (bool(recordlist) == True):
  (FinalResultSet
       .write
       .partitionBy('TestRunDate','TestRunHour','source_name','TestResult')
       .mode("append")
       .format(targetFormat)
       .save(targetPath))

# COMMAND ----------

# DBTITLE 1,Close Connection
conn.close()

# COMMAND ----------

# DBTITLE 1,Display Results Full
if (bool(recordlist) == True):
  display(FinalResultSet)

# COMMAND ----------

# DBTITLE 1,Display Results Failed
if (bool(recordlist) == True):
  failedRow = (FinalResultSet.where('TestResult == "Failed"').count())
  print(failedRow)
  if (failedRow > 0):
    display(FinalResultSet.where('TestResult == "Failed"'))
  else:
    print("No FAILED Rows")

# COMMAND ----------

print(testRunId)