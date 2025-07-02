# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>specific_notebook_unstructuredSource function</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Standard Function for unstructuredSource notebooks </td></tr>
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
# MAGIC     <td>Framework/td>
# MAGIC     <td>Added reBuildCurrentStateTable function and removed unstructuredSource object reference details function
# MAGIC          <br>Updated getTable function getting directly from message by loading to json</td>
# MAGIC   </tr> 
# MAGIC  
# MAGIC  
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

# DBTITLE 1,Function to Add methods to class
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

# DBTITLE 1,Initialise unstructuredSourceFunctions Class 
#initialise the class
class unstructuredSourceFunctions:
  pass;

# COMMAND ----------

# DBTITLE 1,Parse JSON from string
from pyspark.sql.functions import from_json, col
import json
#Function definition to extract json from string message
@add_method(unstructuredSourceFunctions)
def parseJSON(message):
  try: 
    #Add a root element outerJSON
    newMessage='{"OuterJSON":'+message+"}"
    finalJson=json.loads(newMessage)

  except:          
    finalJson = {"OuterJSON": {"message": {"headers": {"operation":"BadJSON"}}}}

  return finalJson

# COMMAND ----------

# DBTITLE 1,Find Data Quality
#Definition of Quality functions to find out message is properly json formated or not.
@add_method(unstructuredSourceFunctions)
def isFormatedData(message):   
  try:
    
    finalJson=json.loads(message)
    return True      
  except: 
    return False

# COMMAND ----------

# DBTITLE 1,Find Table Name
@add_method(unstructuredSourceFunctions)
def getTableName(data):
  try:
    #convert to json and get the TABLE value from it
    TableName=json.loads(data)['message']['data']['TABLE']
  except:
    TableName='Unknown'
  return TableName

# COMMAND ----------

# DBTITLE 1,Register JSON Parser Functions
@add_method(unstructuredSourceFunctions)
def getunstructuredSourceSchemaAndRegisterFunction(schemaLocation):
  #Create dataframe on unstructuredSource schema definition
  
  unstructuredSourceSchemaDefinitionDF=spark.read.load(schemaLocation, format="csv",header=True)
  #Get XML_schema_dic from dataframe
  unstructuredSourceSchemaDefinition=unstructuredSourceSchemaDefinitionDF.selectExpr('jsonSchemaDic', 'jsonSchemaStruct').collect()
  
  #declaring as global so parse XML function can access the dictionary
  unstructuredSourceJSONSchemaStruct = unstructuredSourceSchemaDefinition[0][1]
  
  #Declare the function as global so that it can be registered and accessed outside of the funciton
  global unstructuredSourceJSONParserUdf
  unstructuredSourceJSONParserUdf=udf(parseJSON,unstructuredSourceJSONSchemaStruct)
  global unstructuredSourceFindDataQualityUdf
  unstructuredSourceFindDataQualityUdf=udf(isFormatedData,BooleanType())
  global unstructuredSourceFindTableNameUdf
  unstructuredSourceFindTableNameUdf=udf(getTableName,StringType())
  

# COMMAND ----------

# DBTITLE 1,Function to build currentstate table
@add_method(unstructuredSourceFunctions)
def reBuildCurrentStateTable(sourceDF,targetObjectName,columnList,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
         try:
          #Get the details of the current state table for primary key and changeable fields
          referenceTableDetailsDf=getObjectReferenceDetails(targetObjectName
                                                            ,cursor
                                                            ,batchTaskId
                                                            ,adfPipelineName
                                                            ,clusterId
                                                            ,notebookName
                                                            ,errorLogFileLocation)
          #split and strip the changeable columns
          ChangeableColumns=[x.strip() for x in referenceTableDetailsDf['changeable_fields'][0].split(',')]

          #Get Primary key 
          primaryKey=[referenceTableDetailsDf['primary_key_fields'][0]]
          timeKey=referenceTableDetailsDf['time_key'][0]
          selectColumns=ChangeableColumns+primaryKey+[timeKey]
          #Get the audit columns for later use, These are the exclusions of selectcolumns from columnListfinal
          auditColumns=[c[1] for c in columnList if c[0] not in selectColumns ]

          #Read the target object 
          curstateDF=spark.read.table(targetObjectName)
          
          #Union source and target data
          combinedDF =  (curstateDF
                        .selectExpr(*selectColumns)
                        .union(sourceDF
                               .selectExpr(*selectColumns).distinct()
                              ))
          #Create partition window based on the primary key ,we want to filter out the maximum time stamp of primary key column
          partitionWindow = Window.partitionBy(*primaryKey)
          #create maxTimeKey on partition windo and filter rows having that time stamp.Include the audit columns in the end 
          currentVersionDF=(combinedDF
                           .withColumn('maxTimeKey', max(col(timeKey)).over(partitionWindow))
                           .where(col(timeKey) == col('maxTimeKey'))
                           .selectExpr(*(selectColumns+auditColumns)))
          return currentVersionDF
         except Exception as e:
          errorMessage="Unable to create current state table: " + str(e)
          logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
          assert False