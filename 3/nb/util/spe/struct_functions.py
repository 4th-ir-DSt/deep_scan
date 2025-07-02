# Databricks notebook source
# MAGIC %md
# MAGIC ###struct_functions notebook loaded

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>struct_function</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Struct Functions for struct and dictionary generation</td></tr>
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
# MAGIC     <td>removed redundant parameters from getInitialListsAndDictionaries
# MAGIC            <br>passed objectSchemaDic into getSchemaStruct
# MAGIC            <br>Incorporated function convertPandasToSparkDfWithSchema</td>
# MAGIC   </tr>   
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
class structFunctions:
  pass;

# COMMAND ----------

# DBTITLE 1,Function StructDictionary - to get attribute type
# DataType_Dic = {"STRING":"StringType()","INT":"IntegerType()"}

# Path_List sample ['PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs]

#MetaData_Dic sample {'CalculatedResult_CompulsoryXs': ['PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs', 'STRING']}

#Element_List Sample  ['PolMessage', ]

# Helper Function to Get the Type of the element. If an element has a childs(.i.e a.b.c.d  a,b,c reported as struct) in any of the path retrun as StructuType else return the type from above created dictionary
@add_method(structFunctions)
def findElementType( element
                   , pathList
                   , cursor
                   , batchTaskId
                   , adfPipelineName
                   , clusterId
                   , notebookName
                   , errorLogFileLocation):
    try:
        #Get the count of an item.  presen in the path
        Count  =  0
        for pathListItem in pathList:

            if element+'.' in pathListItem or element+'].' in pathListItem:
                Count = Count+1

        if Count > 0 :
            return "StructType()"

        else :
         #Get the type from the dictionary (changed defaults to Stirng Type if not found)
            return "StringType()"

    except Exception as e:
        errorMessage="Unable to FindElementType: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False

# COMMAND ----------

# DBTITLE 1,Function StructDictionary - to find attribute array or not
#Helper Function to find an element is an array or not , The path will have [] for array elements 
@add_method(structFunctions)
def findElementArrayStatus( element
                          , pathList
                          , cursor
                          , batchTaskId
                          , adfPipelineName
                          , clusterId
                          , notebookName
                          , errorLogFileLocation):

  try:
    arrayStatus = False
    
    for item in  pathList:
      if '['+element+']' in item:
          arrayStatus = True

    return arrayStatus

  except Exception as e:

    errorMessage="Unable to FindElementType: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function StructDictionary - to Generate attribute details dictionary
#Main function to generate the dictionary,
@add_method(structFunctions)
def generateElementDetailsDictironary( elementList
                                     , pathList
                                     , structWrapperType #'XML or JSON'
                                     , cursor
                                     , batchTaskId
                                     , adfPipelineName
                                     , clusterId
                                     , notebookName
                                     , errorLogFileLocation):

    try:
        elementDetailsDic = {}
        #   Loop through all the elements and find array or not and type of it using the above helper functions
        for elementListItem in elementList:

            elementDetailsDic.update({elementListItem:[structFunctions.findElementArrayStatus( elementListItem
                                                                                             , pathList
                                                                                             , cursor
                                                                                             , batchTaskId
                                                                                             , adfPipelineName
                                                                                             , clusterId
                                                                                             , notebookName
                                                                                             , errorLogFileLocation)
                                                          ,structFunctions.findElementType( elementListItem
                                                                                          , pathList
                                                                                          , cursor
                                                                                          , batchTaskId
                                                                                          , adfPipelineName
                                                                                          , clusterId
                                                                                          , notebookName
                                                                                          , errorLogFileLocation)]})
            #Add the Static element OuterXML or OuterJSON as top of the item.
       
        elementDetailsDic.update({"Outer{}".format(structWrapperType):[False,'StructType()']})
        
        return elementDetailsDic
    except Exception as e:
        errorMessage="Unable to FindElementType: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False        

# COMMAND ----------

# DBTITLE 1,Function StructDictionary - to get Initial Lists and Dictionaries for XML | JSON parsing
@add_method(structFunctions)
def getInitialListsAndDictionaries( metadataDetailsDF
                                  , structWrapperType #'XML or JSON'
                                  , cursor
                                  , batchTaskId
                                  , adfPipelineName
                                  , clusterId
                                  , notebookName
                                  , errorLogFileLocation):
  #The Logic is to Get the posintion of the element in for each path and get the position of the parent by -1 to index from array list.Loop it from the max position(these are simply not struct types) create struct and add it to dictionary.
  #DataType_Dic = {"STRING":"StringType()","INT":"IntegerType()"}

  #Path_List sample ['PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs]

  #MetaData_Dic sample {'CalculatedResult_CompulsoryXs': ['PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs', 'STRING']}

  #Element_List Sample  ['PolMessage', ]
  try:
    #Function to prepare metadata lists and dictionaries
    #def getInitialListsAndDictionaries(unstructuredxmlSource_metadata_DetailsDF)
    #returns pathList, elementList, metadataDic, dataTypeDic and objectSchemaDic

    #Get the Path and create the  element list from it by spliting by '.'
    pathList = list(metadataDetailsDF.select("Path").rdd.flatMap(lambda x: x).collect())

    #The dic.fromkeys used to remove duplicates
    elementList = list( dict.fromkeys(sc.parallelize(pathList).flatMap(lambda x: x.replace('[','').replace(']','').split('.')).collect()))


    ##Convert the dataframe to dictionary to use it flexible way in function rather than doing the filter everytime.
    metaDataDic = metadataDetailsDF.toPandas().set_index('Element').T.to_dict('list')

    #Static Dictionary to map the datatypes coming from metadata with pyspark sql types.
    dataTypeDic = {"STRING":"StringType()"}      
    #DataType_Dic = {"STRING":"StringType()","INT":"IntegerType()","DATE":"DateType()","DECIMAL(10,2)":"DecimalType(10,2)","DECIMAL(10,3)":"DecimalType(10,3)","DECIMAL(18,3)":"DecimalType(18,3)","DECIMAL(7,2)":"DecimalType(7,2)"}      

    #create  dictionary
    objectSchemaDic=structFunctions.generateElementDetailsDictironary( elementList
                                                                     , pathList
                                                                     , structWrapperType
                                                                     , cursor
                                                                     , batchTaskId
                                                                     , adfPipelineName
                                                                     , clusterId
                                                                     , notebookName
                                                                     , errorLogFileLocation)
                
    return pathList, elementList, objectSchemaDic
  except Exception as e:
    errorMessage="Unable to create Path And Element dictionaries in function structFunctions.getInitialListsAndDictionaries: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Function StructDictionary - to get Number of Elements and Metadata Dictionary
@add_method(structFunctions)
def getNoElementsAndMetadataDict( metadataDetailsDF
                                , structWrapperType #'XML or JSON'
                                , cursor
                                , batchTaskId
                                , adfPipelineName
                                , clusterId
                                , notebookName
                                , errorLogFileLocation):
  try: 
    
    splitMedataDF=(metadataDetailsDF
                   .withColumn("CleanPath"
                               ,regexp_replace(regexp_replace(col("Path"), "\\[", ""),"\\]", "")
                              )
                   .select(col("CleanPath").alias("Path")
                           ,split("CleanPath", "\\.").alias("ArrayList")
                           ,posexplode(split("CleanPath", "\\.")).alias("pos", "val"))
                  )

    #Get the parent of the element by going with pos-1
    splitMedataDF=splitMedataDF.select("Path"
                                       ,"pos"
                                       ,"val"
                                       ,coalesce(col("ArrayList")[col("pos")-1],lit("Outer{}".format(structWrapperType))).alias("Parent"))

    ##display(Split_unstructuredxmlSource_medataDF)
    #Get the maximum number of times we need to loop through
    numberOfElements=splitMedataDF.select([max("pos")]).collect()[0][0]

    #Multi index dictionary to be used in the next looping based on the posintion number.Effective rather than creating the filtered databframes every time
    splitMedataDic=splitMedataDF.select("val","Parent","pos").distinct().toPandas().groupby(['pos','val'])['Parent'].apply(list).reset_index(name="Parent").set_index(['pos','val']).T.to_dict() 

    return numberOfElements, splitMedataDic

  except Exception as e:
    errorMessage="Unable to create split dataframe and dictionary in function structFunctions.getNoElementsAndMetadataDict: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function StructDictionary - Get the Schema Struct
@add_method(structFunctions)
def getSchemaStruct( splitMedataDic
                   , objectSchemaStruct
                   , numberOfElements
                   , pathList
                   , objectSchemaStructDic
                   , structWrapperType #'XML or JSON'
                   , objectSchemaDic
                   , cursor
                   , batchTaskId
                   , adfPipelineName
                   , clusterId
                   , notebookName
                   , errorLogFileLocation):
  try:
  #Start from the bottom position which has no struct elements 
    for x in range(numberOfElements, -1, -1):
      
      #Filter the above created dictionaries for each position to get element and parent dictionary and parent list
      objectElementDic = { key[1]:value for (key,value) in splitMedataDic.items() if key[0] == x}
      
      objectParentList=[value.get('Parent') for (key,value) in splitMedataDic.items() if key[0] == x]

      #If mulitple child elemnt have same names the above parent list creates list of lists. Flaten that list
      objectParentList=list(dict.fromkeys(list(chain(*objectParentList))))

      for objectParentItem in objectParentList:
        objectParentStruct=StructType() # for every parent we construct sturct type and add to dictionary 
        for objectElementDicItem in dict(filter(lambda elem: objectParentItem in elem[1]["Parent"], objectElementDic.items())):

          #if an element is struct get the struct form dictionary which is updated for the previous x number  and check arrya or not
          if structFunctions.findElementType( objectElementDicItem
                                            , pathList
                                            , cursor
                                            , batchTaskId
                                            , adfPipelineName
                                            , clusterId
                                            , notebookName
                                            , errorLogFileLocation) == "StructType()": 

            if structFunctions.findElementArrayStatus( objectElementDicItem
                                                     , pathList
                                                     , cursor
                                                     , batchTaskId
                                                     , adfPipelineName
                                                     , clusterId
                                                     , notebookName
                                                     , errorLogFileLocation):
                  objectParentStruct.add(objectElementDicItem,ArrayType((objectSchemaStructDic.get(objectElementDicItem))))

            else:
                  objectParentStruct.add(objectElementDicItem,objectSchemaStructDic.get(objectElementDicItem))

          else :
            if structFunctions.findElementArrayStatus( objectElementDicItem
                                                     , pathList
                                                     , cursor
                                                     , batchTaskId
                                                     , adfPipelineName
                                                     , clusterId
                                                     , notebookName
                                                     , errorLogFileLocation): 
              objectParentStruct.add(objectElementDicItem,ArrayType(eval(objectSchemaDic.get(objectElementDicItem)[1])))

            else :
              objectParentStruct.add(objectElementDicItem,eval(objectSchemaDic.get(objectElementDicItem)[1]))

        objectSchemaStructDic.update({objectParentItem:objectParentStruct})

    objectSchemaStruct=StructType()
    objectSchemaStruct.add("Outer{}".format(structWrapperType),objectSchemaStructDic.get("Outer{}".format(structWrapperType))) #wrap outer XML or JSON struct  under single  root.
      
    return objectSchemaStruct
  except Exception as e:
    errorMessage="Unable to create structure object when running function structFunctions.getSchemaStruct: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

#function definition of [config].[usp_get_raw_object_details] that executes stored procedure and save the result as pandas dataframe
@add_method(structFunctions)
def spExecGetObjectDetails( batchTaskId
                          , source
                          , conn
                          , cursor
                          , adfPipelineName
                          , clusterId
                          , notebookName
                          , errorLogFileLocation):
  try:
    #execution of unstructuredxmlSource object details stored procedure
    metadataDetails=pd.read_sql_query("exec [config].[usp_get_raw_object_details] {}".format(source),conn)
    
    logTaskProgress(cursor,batchTaskId,"executed usp_get_raw_object_details stored procedure")
    return metadataDetails
  except Exception as e:
    errorMessage="Exception occured while execution of usp_get_raw_object_details stored procedures: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

@add_method(structFunctions)
def convertStructDictToDataFrame( objectSchemaDic
                                , objectSchemaStruct
                                , batchId
                                , CreatedTimestamp
                                , LastUpdatedTimestamp
                                , conn
                                , cursor
                                , adfPipelineName
                                , clusterId
                                , notebookName
                                , errorLogFileLocation ):
  try:
    objectSchemaDF = (convertPandasToSparkDfWithSchema([(str(objectSchemaDic),
                                              str(objectSchemaStruct.simpleString())
                                            )]
                                            ,StructType([
                                              StructField("{}SchemaDic".format(structWrapperType.lower()),StringType(),True),
                                              StructField("{}SchemaStruct".format(structWrapperType.lower()),StringType(),True)
                                            ]),cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation
                                           )
                      .withColumn("CreatedBatchID",lit(batchId))
                      .withColumn("CreatedTimestamp ",lit(CreatedTimestamp))
                      .withColumn("LastUpdatedBatchID",lit(batchId))
                      .withColumn("LastUpdatedTimestamp", lit(LastUpdatedTimestamp))
                     )

    logTaskProgress(cursor,batchTaskId,"Completed convertStructDictToDataFrame and created dataframe from dictionary and schema struct")
    return objectSchemaDF
  except Exception as e:
    errorMessage="Exception occured while execution of convertStructDictToDataFrame function: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False


# COMMAND ----------

@add_method(structFunctions)
def writeSchemaDictToStorage( superSource
                            , objectName
                            , destinationDF
                            , conn
                            , cursor
                            , adfPipelineName
                            , clusterId
                            , notebookName
                            , errorLogFileLocation ):
  #call write_to_target function to write dataframe into destination location
  try:    
    location = "/mnt/raw/reference/{}/{}Schema_Def".format(superSource, objectName)

    #print ("location is {}".format(location))
    (destinationDF.write.format("csv")
     .mode("overwrite")
     .option("header", "true")
     .save(location)
    )

    logTaskProgress(cursor,batchTaskId,"Completed function writeSchemaDictToStorage for object {} to location {}".format(objectName, location))

  except Exception as e:
    errorMessage="Exception occured while loading into target location: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False  