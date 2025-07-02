# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>misc_functions</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Miscellaneous Function for all notebook </td></tr>
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
# MAGIC     <td>Created new functions getTableHighAndLowLevelDF, performTableAlterationsAndReload, calculateTableDifferences<p>and altered functions tableStructureCheck, concatColumnDatatype, createTable to handle delta table changes
# MAGIC         <br>Tweaked filter in getTableHighAndLowLevelDF to exclude raw objects
# MAGIC         <br>Added functions deltaTableVacuum and deltaTableOptimize
# MAGIC         <br>Added additional logging to deltaTableOptimize. Removed the TestDataPrep function.
# MAGIC         <br>Added functions getMaxDateHour, getLastOptimise and markTableAsOptimised and changed optimise logic in functions deltaTableOptimiseVacuum and deltaTableOptimise
# MAGIC         <br>Changed getMaxDateHour function logic
# MAGIC         <br>Corrections to exclude hive partition when getting max date and hour
# MAGIC         <br>Changed getMaxDateHour and deltaTableOptimize function logic
# MAGIC         <br>Changed rawObjectLatestDateTime function to subtract the source time lag to the maxDatetime
# MAGIC         <br>Added changes to use the json extended properties and to create\alter raw tables
# MAGIC         <br>Correcte the select column creation to not use a , and insert a ¦ instead. This will produce a more reliable split afterwards
# MAGIC         <br>Checked that a correctly formed TablePartitionSchemeList was cretaed if there are no partitions for a table
# MAGIC         <br>Updated performTableAlterationsAndReload to include partitions in the table overwrite. Also, changed to saveAsTable
# MAGIC         <br>Moved position of alter columns when reloading table
# MAGIC         <br>Updated getObjectRetention function to include batch_task_id as an input parameter for the stored procedure
# MAGIC         <br>Added the functions getMinNonNullDatePath, getMinNonNullTimePath, getHistoricRawEventhubObjectLocations and rawObjectHistoricDateTime to load historic unstructuredxmlSource
# MAGIC         <br>Fixed max date to min date in function rawObjectHistoricDateTime
# MAGIC         <br>removed spaces from metadata database column data type in calculated table differences
# MAGIC         <br>Updated rawObjectHistoricDateTime function to incldue previous batch status check and folder exists check
# MAGIC         <br>update rawObjectHistoricDateTime to account for dev containing a time folder
# MAGIC         <br>Updated functions createTable, concatColumnDatatype, getCurrentTableDetails (new function to retrieve all current table details in a single function), calculateTableDifferences, performTableAlterationsAndReload and tableStructureCheck
# MAGIC         <br>Corrected caltulation of TableMatchBool in calculateTableDifferences
# MAGIC         <br>Added creattableonly mode for non etl workspaces
# MAGIC         <br>hanged getMaxDateHour function logic
# MAGIC         <br>Amended Pandas DF to allow upgrade of pandas / spark 3 cluster runtimes
# MAGIC         <br>Updated getCurrentTableDetails function to accommodate changes from spark 3 update
# MAGIC         <br>Added batchId to getRawEventhubObjectLocations function parameters
# MAGIC         <br>handle 'n/a' infile_datetime_column
# MAGIC         </td>
# MAGIC   </tr> 
# MAGIC   <tr>
# MAGIC     <td>2022/01/13</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>Added functions getActiveSynapseTablesSpExec, getSynapseTableDetailsDf, generateSynapseMergeSPs, generateSynapseTableScript, generateSynapseExternalTable</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/01/19</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>Added wrapper function generateSynapseScripts</td>
# MAGIC   </tr>
# MAGIC 
# MAGIC   <tr>
# MAGIC     <td>2022/02/15 </td>
# MAGIC     <td>Harish N </td>
# MAGIC     <td>Added  bug fixes for data load</td>
# MAGIC   </tr>
# MAGIC 
# MAGIC   <tr>
# MAGIC     <td>2022/02/06</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>Updated function generateSynapseMergeSPs to include transaction and try catch</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/03/07</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>Updated function generateSynapseMergeSPs to set objects availablity</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/03/24</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>audit.usp_populate_staging_dates_loaded added to merge SP generation</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/03/31</td>
# MAGIC     <td>Karthik</td>
# MAGIC     <td>lakeLastUpdateDate, lakeLastUpdateTimestamp, lakeLastUpdatedBatchID included to merge SP</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2022/04/12</td>
# MAGIC     <td>Akhilesh</td>
# MAGIC     <td>Add table alias "src" for sourceAttributes and create external table directory</td>
# MAGIC   <tr>
# MAGIC     <td>2022/08/12</td>
# MAGIC     <td>Manmohan</td>
# MAGIC     <td>Rewriting Staging Merge Statements as Update and Inserts due to performance issues, Added NOT NULL constraints to staging tables DDLs </td>
# MAGIC   </tr> 
# MAGIC   <tr>
# MAGIC     <td>2022/09/14</td>
# MAGIC     <td>Manmohan</td>
# MAGIC     <td>Added error logging step in UPSERT function</td>
# MAGIC   </tr> 
# MAGIC     <tr>
# MAGIC     <td>2022/11/21</td>
# MAGIC     <td>HARISH N</td>
# MAGIC     <td>Updates to remove the staging scripts to deal seperately on datamodel group</td>
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

# DBTITLE 1,Function to create table
#Function to create the table 
def createTable(tbName,schemaString,partitionScheme,location,tableDeltaExtendedProperties,objectColumn,schemaFromFile,createTableOnly,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    sqlCreateTable = ''
    #set the table properties
    tableProperties = ''
    #tableProperties = tableProperties + "delta.deletedFileRetentionDuration='interval {}'".format(metaDeleteFileRetentionProp) if len(metaDeleteFileRetentionProp) > 0 else tableProperties
    #tableProperties = tableProperties + ",delta.logRetentionDuration='interval {}'".format(metaLogRetentionProp) if len(metaLogRetentionProp) > 0 else tableProperties
    tableProperties =  ','.join(k + '=' + v for k,v in tableDeltaExtendedProperties.items()) 
    
    hasTableProperties = True if len(tableProperties) > 0 else False
    hasPartitionScheme = True if len(partitionScheme) > 0 else False    
    #print("We are in createTableOnly mode and location does not exist")
    if (hasPartitionScheme == False):
      if (hasTableProperties == False):
        sqlCreateTable = "create table {} ({}) using delta location '{}' ".format(tbName,schemaString,location)
        
      elif (hasTableProperties) :
        sqlCreateTable = "create table {} ({}) using delta TBLPROPERTIES({}) location '{}' ".format(tbName,schemaString,tableProperties,location)
        
    else:#we have a partition scheme if we are here
      if (hasTableProperties == False):
        sqlCreateTable = "create table {} ({}) using delta partitioned by ({}) location '{}' ".format(tbName,schemaString,partitionScheme,location)
        
      elif (hasTableProperties):
        sqlCreateTable = "create table {} ({}) using delta partitioned by ({}) TBLPROPERTIES({}) location '{}' ".format(tbName,schemaString,partitionScheme,tableProperties,location)
        
    try:
      if len(dbutils.fs.ls(location)) > 0:
        locationExists = True
      else:
        locationExists = False
    except:
      locationExists = False
    
    if ( (createTableOnly == False) or ( createTableOnly == True and locationExists == True ) ):
      #print(sqlCreateTable)
      spark.sql(sqlCreateTable)

#       if tbName.startswith("Raw_"):
#         uf_udf = udf(updateStruct, schemaFromFile)
#         if hasPartitionScheme == True:
#           tablePartitionSchemeString = partitionScheme.split(",")
#           spark.read.table(tbName).withColumn(objectColumn,uf_udf(col(objectColumn))).write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy(*tablePartitionSchemeString).save(location)
#         else:
#           spark.read.table(tbName).withColumn(objectColumn,uf_udf(col(objectColumn))).write.format("delta").mode("overwrite").option("overwriteSchema","true").save(location)
    else:
      #print("We are in createTableOnly mode and location does not exist")
      #print(sqlCreateTable) 
      spark.sql(sqlCreateTable)
    logTaskProgress(cursor,batchTaskId,"Table {} created.".format(tbName))
  except Exception as e:
    errorMessage="Exception occured while containating the string in createTable function : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to get the column select expression and alter columns
#Function takes in various table details and calculates any differences in columns with name and type. If the type is differenct it will cast to the new type. This is output in the form of a select expression
#Also, any column that need to be created, i.e. are new are added to a list
def concatColumnDatatype(tableName, tableMatchDetail,currentTableColumnTypePandasDF,colsInFinalTable,colsToAdd,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #tablesMatchBool, tableMatchDetail, currentTableColumnTypePandasDF, currentTablePropertiesDict, colsToAdd, colsInFinalTable
    #calculate the select expression to be used
    #get the columns and data types to load. This is all of the columns in the database
    selectExpressionStep1PandasDF = pd.merge(colsInFinalTable, currentTableColumnTypePandasDF, how="left", on="object_attribute_name").copy()
    #update column name to make easier
    selectExpressionStep1PandasDF.columns = ["meta_object_attribute_name", "meta_object_attribute_type", "meta_object_attribute_order", "current_object_attribute_type"]
    
    #add final column by assiging simple assignment when the types haven't changed, so a straight select
    selectExpressionStep1PandasDF["final_column"] = np.where(selectExpressionStep1PandasDF["meta_object_attribute_type"]==selectExpressionStep1PandasDF["current_object_attribute_type"], '"' + selectExpressionStep1PandasDF["meta_object_attribute_name"] + '"', "")
    
    #add column where data types are the same
    for index, row in selectExpressionStep1PandasDF.iterrows():
      #this will be 0 length if the data types weren't equal previously
      if len(row["final_column"]) == 0:
        #if there is a current type, then we know the column exists and the data types must be different. so we have to cast it
        if pd.isnull(row["current_object_attribute_type"]) == False:
          finalVal = "\"CAST({} AS {}) AS {}\"".format(row["meta_object_attribute_name"], row["meta_object_attribute_type"], row["meta_object_attribute_name"])
        #else, the column didn't exist and we must have added it as part of add columns. So we just select it
        else:
          finalVal = "\"" + row["meta_object_attribute_name"] + "\""
        selectExpressionStep1PandasDF.at[index, "final_column"] = finalVal
    
    #add a grouping column
    selectExpressionStep1PandasDF["grouping"] = 1
    selectExpressionColumns = selectExpressionStep1PandasDF.groupby(["grouping"])['final_column'].apply(','.join).reset_index()["final_column"].values[0]
    
    #add one to the order so we are able to lookup the previous field in preparation for the alter table add column
    colsInFinalTable["object_attribute_order"] += 1
    #initialise empty new columns to be added
    newColumnsToBeAdded = ''
  
    #check if there are columns to add
    if tableMatchDetail["colsToAddBool"] == True:
      #this is to convert the order to a number
      colsToAdd["object_attribute_order"] -=0
      colsToAdd = pd.merge(colsToAdd, colsInFinalTable, how='left', on="object_attribute_order").copy()
      colsToAdd.columns = ["object_attribute_name", "object_attribute_type", "object_attribute_order", "object_attribute_name_after", "object_attribute_type_after"]
      
      colsToAdd["alter_statement"] = colsToAdd["object_attribute_name"] + " " + colsToAdd["object_attribute_type"] + " AFTER " + colsToAdd["object_attribute_name_after"]
      #the first column won't find a mapping so we now replace the generated statement to add the alter column as first
      colsToAdd["alter_statement"].fillna(colsToAdd["object_attribute_name"] + " " + colsToAdd["object_attribute_type"] + " FIRST", inplace=True)
      colsToAdd["grouping"] = 1
      colsToAdd = colsToAdd.sort_values('object_attribute_order',ascending=True).reset_index(drop=True)
      # colsToAdd
      newColumnsToBeAdded = 'ALTER TABLE ' + tableName + ' ADD COLUMNS (' + colsToAdd.groupby(["grouping"])['alter_statement'].apply(','.join).reset_index()["alter_statement"].values[0] + ')'
      
    return selectExpressionColumns, newColumnsToBeAdded
  except Exception as e:
    errorMessage="Exception occured while containating the string in createTable function : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to get the current table details
def getCurrentTableDetails(tableName,objectName,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:    
    #initialise empty properties dictionary
    currentTablePropertiesDict = {}
    #initialise empty partitions dataframe
    currentPartitionsList = []

    currentTableDescribePandasDF = (spark.sql("DESCRIBE EXTENDED {}".format(tableName))).toPandas()

    #we need to get the columns, and these are found before the partition information, or if no partition information, before the Detail Table Information
    currentTablePartitionsIndex = 0
    if (currentTableDescribePandasDF.loc[:,"col_name"] == '# Partitioning').any():
      currentTablePartitionsIndex = currentTableDescribePandasDF.loc[currentTableDescribePandasDF.loc[:,'col_name']=='# Partitioning'].index[0]

    #get the detailed information index start
    currentTableDetailedTableInformationIndex = currentTableDescribePandasDF.loc[currentTableDescribePandasDF.loc[:,'col_name']=='# Detailed Table Information'].index[0]

    currentColsFilterIndex = currentTablePartitionsIndex -1 if currentTablePartitionsIndex > 0 else currentTableDetailedTableInformationIndex -1
    currentTableColumnTypePandasDF = currentTableDescribePandasDF.iloc[0:currentColsFilterIndex,:].copy()

    if currentTablePartitionsIndex > 0:
      #if the partitions are there then there will be an additional padding row of #col name and we need to skip this
      currentPartitionsPandasDF = currentTableDescribePandasDF.iloc[currentTablePartitionsIndex + 1:currentTableDetailedTableInformationIndex-1,:].copy()
      currentPartitionsList = currentPartitionsPandasDF["data_type"].tolist()
      if currentPartitionsList == ['']:
        currentPartitionsList = []
  #     currentPartitionsPandasDF.loc[:,"grouping"] = 1
  #     currentPartitions = currentPartitionsPandasDF.groupby(["grouping"])['col_name'].apply(','.join).reset_index()["col_name"].values[0]

    currentTableDetailsPandasDF = currentTableDescribePandasDF.iloc[currentTableDetailedTableInformationIndex+1:,:].copy()

    #get the table properties if there are any can convert to common dictionary structure for comparisson
    if (currentTableDetailsPandasDF.loc[:,"col_name"] == 'Table Properties').any():
      currentTablePropertiesPandasDF = currentTableDetailsPandasDF[currentTableDetailsPandasDF.loc[:,"col_name"]=='Table Properties']["data_type"].values[0].strip('][]').split(',').copy()
      if currentTablePropertiesPandasDF != ['']:
        currentTablePropertiesDict = dict(s.split("=") for s in currentTablePropertiesPandasDF)
        currentTablePropertiesDict = {k:v.lower() for k,v in currentTablePropertiesDict.items()}
      if "delta.minReaderVersion" in currentTablePropertiesDict:
        currentTablePropertiesDict.pop("delta.minReaderVersion")
      if "delta.minWriterVersion" in currentTablePropertiesDict:
        currentTablePropertiesDict.pop("delta.minWriterVersion")
  #currentTable properties are ready to compare

    #remove the comment column as we don't need it
    currentTableColumnTypePandasDF.drop("comment", axis=1, inplace=True)
    #rename columns and make the type upper in preparation
    currentTableColumnTypePandasDF.columns = ["object_attribute_name", "object_attribute_type"]
    currentTableColumnTypePandasDF["object_attribute_type"] = currentTableColumnTypePandasDF["object_attribute_type"].str.upper()

    #if it is a raw table here then, then if it contains a special column i.e. the nested type column than change the type to STRING
#     if tableName.startswith("Raw_"):
#       #check if the object column is present
#       if (currentTableColumnTypePandasDF.loc[:,"object_attribute_name"] == objectName).any():
#         #we know the row is there so we just find the first index
#         objectRowIndex = currentTableColumnTypePandasDF.loc[currentTableColumnTypePandasDF.loc[:,'object_attribute_name']==objectName].index[0]
#         #now update the attribute to just string
#         currentTableColumnTypePandasDF.at[objectRowIndex, "object_attribute_type"] = 'STRING'
  #current Table columnns and data types are ready to compare   

    return currentTableColumnTypePandasDF, currentTablePropertiesDict, currentPartitionsList
  
  except Exception as e:
    errorMessage="Exception occured while getting current table details in getCurrentTableDetails function : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function calculateTableDifferences - to understand differences between database and databricks table
#function that calculates the differences between the table described in the database and what is actually present in databricks.
#function that calculates the differences between the table described in the database and what is actually present in databricks.
def calculateTableDifferences(tableName,tablePartitionSchemeList,tableDeltaProperties
                              ,metaTableDetailsPandasDF,objectColumn,schemaFromFile
                              ,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #tableProperties = {}
    tableMatchDetail = {}
    tablesMatchBool = True
    
    #add the properties in the correct order into a list for later comparisson
    #if len(tableDeltaDeletedFileRetentionDuration) > 0: tableProperties.append(['delta.deletedFileRetentionDuration', 'interval ' + tableDeltaDeletedFileRetentionDuration])
    #if len(tableDeltaLogRetentionDuration) > 0: tableProperties.append(['delta.logRetentionDuration', 'interval ' + tableDeltaLogRetentionDuration])
    objectSchemaMatchesBool = True
    
    #get the schema if it is a raw table
#     if tableName.startswith("Raw_"):
#       schemaFromTable = spark.read.format("delta").table(tableName).select(objectColumn).limit(1).schema.simpleString()
#       schemaFromFile = "struct<{}:".format(objectColumn) + schemaFromFile + ">"
#       objectSchemaMatchesBool = True if schemaFromTable == schemaFromFile else False
#       tablesMatchBool = True if objectSchemaMatchesBool and tablesMatchBool else False
    
    tableMatchDetail.update({'objectSchemaMatchesBool':objectSchemaMatchesBool})
    
#get all the current table details
    #get all the tables for the current table into objects
    currentTableColumnTypePandasDF, currentTablePropertiesDict, currentTablePartitionsList = getCurrentTableDetails(tableName
                                                                                                                   ,objectColumn
                                                                                                                   ,cursor
                                                                                                                   ,batchTaskId
                                                                                                                   ,errorMessage
                                                                                                                   ,adfPipelineName
                                                                                                                   ,clusterId
                                                                                                                   ,notebookName
                                                                                                                   ,errorLogFileLocation)
#metadata table                  
    #remove spaces from the data types and make upper - easier comparison
    metaTableDetailsPandasDF.loc[:,"object_attribute_type"] = metaTableDetailsPandasDF["object_attribute_type"].str.replace(" ", "").str.upper()
    
#prepare for differences calculations    

    #this will be used for comparing the columns and data types order
    intersect = metaTableDetailsPandasDF[metaTableDetailsPandasDF["object_attribute_name"].isin(currentTableColumnTypePandasDF["object_attribute_name"])].sort_values('object_attribute_order',ascending=True).reset_index(drop=True)

    #get the list of columns that are new and therefore need to be added
    colsToAdd = metaTableDetailsPandasDF[~metaTableDetailsPandasDF["object_attribute_name"].isin(currentTableColumnTypePandasDF["object_attribute_name"])].copy()
    colsToAddBool = True if len(colsToAdd.index) > 0 else False
    tablesMatchBool = True if (colsToAddBool==False) and tablesMatchBool else False
    tableMatchDetail.update({'colsToAddBool':colsToAddBool})
    
    #get the list of columns that need to be dropped. Only the length of this will be useful really. So even if the intersect and the curMd match. if there are columns to drop then the table must still be reloaded
    colsToDropBool = True if len(currentTableColumnTypePandasDF[~currentTableColumnTypePandasDF["object_attribute_name"].isin(metaTableDetailsPandasDF["object_attribute_name"])].index) > 0 else False
    tablesMatchBool = True if (colsToDropBool==False) and tablesMatchBool else False
    tableMatchDetail.update({'colsToDropBool':colsToDropBool})
    
    
    #dbMeta contains all of the columns that we need to have in the final dataframe, so rename to make clear and sort
    colsInFinalTable = metaTableDetailsPandasDF.sort_values('object_attribute_order',ascending=True).reset_index(drop=True)
    
    logTaskProgress(cursor,batchTaskId,"Data preparation for calculate differences completed for table {}.".format(tableName))
    
#data types    
    #Understand if the columns, datatypes and order match. If this is true then we WILL need to reload the table
    #This includes dropping the attribute order column. If the intersected columns and data types don't match or tehre are columns to drop then this is false, otherwise true
    existingColumnsAndDataTypesMatchBool = True if intersect.drop("object_attribute_order", 1).equals(currentTableColumnTypePandasDF) and colsToDropBool == False else False
    
    tablesMatchBool = True if existingColumnsAndDataTypesMatchBool and tablesMatchBool else False
    tableMatchDetail.update({'existingColumnsAndDataTypesMatchBool':existingColumnsAndDataTypesMatchBool})
    
#partitions   
    partitionsMatchBool = True if tablePartitionSchemeList == currentTablePartitionsList else False
    tablesMatchBool = True if partitionsMatchBool and tablesMatchBool else False
    tableMatchDetail.update({'partitionsMatchBool':partitionsMatchBool})
    
#table properties
    
    #check the table properties - need to order property name
    #convert the metadata table properties into dictionary. 
    #convert everything to lower and sort on the key
    tablePropertiesToCompare = dict(sorted({k.lower().replace(" ", ""):str(eval(v)).lower().replace(" ", "") for k,v in tableDeltaProperties.items()}.items()))
    currentTablePropertiesDict = dict(sorted({k.lower().replace(" ", ""):v.lower().replace(" ", "") for k,v in currentTablePropertiesDict.items()}.items()))

    propertiesMatchBool = True if tablePropertiesToCompare == currentTablePropertiesDict else False

    tablesMatchBool = True if propertiesMatchBool and tablesMatchBool else False
    tableMatchDetail.update({'propertiesMatchBool':propertiesMatchBool})
    
    logTaskProgress(cursor,batchTaskId,"Calculated differences completed for table {}, with detail: {}".format(tableName, str(tableMatchDetail)))
    
#     tablesMatchBool - if this is true then the tables match completely (not including location)
    
#     tableMatchDetail - the following items are included in the list
#     objectSchemaMatchesBool - true if the object schema is correct (struct)
#     colsToAddBool - true if there are columns to add
#     colsToDropBool - true if there are columns to drop
#     ExistingColumnsAndDataTypesMatchBool - true if the columns from metadata databaes that intersect with the columns in the table match (inc data types) - and there are no columns to drop (i.e. colsToDropBool == False)
#     partitionsMatchBool - true if the partitions are correct
#     propertiesMatchBool - true if the properties are correct

#     currentTableColumnTypePandasDF - contains the current table structure with columns object_attribute_name and object_attribute_type
#     colsToAdd - these are the list of object_attribute_name, object_attribute_type and object_attribute_order
#     colsInFinalTable - these are all of the columns to be used in the final table object_attribute_name, object_attribute_type and object_attribute_order

    return tablesMatchBool, tableMatchDetail, currentTableColumnTypePandasDF, currentTablePropertiesDict, colsToAdd, colsInFinalTable
  except Exception as e:
    errorMessage="Exception occured in the calculateTableDifferences function : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function updateStruct - Dummy funtion to apply udf to a daframe column struct
def updateStruct(msg):
  return msg

# COMMAND ----------

# DBTITLE 1,Function performTableAlterationsAndReload - To bring a table up to date with the database definition
#function that performs table reload, adds and new columns to the table, unsets any incorrect properties and adds the correct ones
def performTableAlterationsAndReload(tableName,tableMatchDetail,currentTableColumnTypePandasDF,colsInFinalTable,colsToAdd,schemaFromFile,objectColumn,tablePartitionScheme,currentTablePropertiesDict,tableProperties
                                     ,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #if the tables don't match then we need to relaoad the tables.
    selectExpression, newColumnsToBeAdded = concatColumnDatatype(tableName
                                                                ,tableMatchDetail
                                                                ,currentTableColumnTypePandasDF
                                                                ,colsInFinalTable
                                                                ,colsToAdd
                                                                ,cursor
                                                                ,batchTaskId
                                                                ,errorMessage
                                                                ,adfPipelineName
                                                                ,clusterId
                                                                ,notebookName
                                                                ,errorLogFileLocation)

    if tableMatchDetail["colsToAddBool"] == True:
      #adding columns
#       print("altering table, adding columns using {}".format(newColumnsToBeAdded))
      spark.sql(newColumnsToBeAdded)
      
    #we need to reload the table if the columns and data types don't match or if there are partitions and they don't match
    if (tableMatchDetail["existingColumnsAndDataTypesMatchBool"] == False or tableMatchDetail["objectSchemaMatchesBool"] == False or tableMatchDetail["partitionsMatchBool"] == False):
      if len(selectExpression) > 0:
        sqlString = ''
        #SqlConcard function will return the sql to be exicuted to alter the table with new column ,renaming of column,Partationing of columns
        sqlString = "spark.read.table(\"{}\")".format(tableName)
        
        #check if table datatype and metadata datatype are same else alter the table 
        sqlString = sqlString + ".selectExpr({})".format(selectExpression)
        
#         if tableName.startswith("Raw_"):
#           uf_udf = udf(updateStruct, schemaFromFile)
#           sqlString = sqlString + '.withColumn("{}",uf_udf(col("{}")))'.format(objectColumn, objectColumn)  
        
        #add the remainder to perform the overwrite
        sqlString = sqlString + '.write.format("delta").mode("overwrite").option("overwriteSchema","true")'

        if (len(tablePartitionScheme) > 0):
          tablePartitionSchemeString = '\"' + '\",\"'.join(map(str, tablePartitionScheme.replace(" ", "").split(","))) + '\"'
          sqlString =  sqlString + '.partitionBy({})'.format(tablePartitionSchemeString)
        
        #as the table exists saveAsTable will be used
        sqlString = sqlString + ".saveAsTable(\"{}\")".format(tableName)
        
        #execute the sqlString
#         print('rewriting table with sqlString {}'.format(sqlString))
        eval(sqlString) 
        logTaskProgress(cursor,batchTaskId,"Columns updated for table {}.".format(tableName))

    if tableMatchDetail["propertiesMatchBool"] == False:
      if len(currentTablePropertiesDict) > 0:
        alterRemoveProperty = "ALTER TABLE {} UNSET TBLPROPERTIES IF EXISTS (".format(tableName) + ','.join(currentTablePropertiesDict.keys()) + ")" 
#         print("removing properties table with {}".format(alterRemoveProperty))
        spark.sql(alterRemoveProperty)

      if len(tableProperties) > 0:
        alterAddProperty = "ALTER TABLE {} SET TBLPROPERTIES (".format(tableName) + ','.join(k + '=' + v for k,v in tableProperties.items())   + ")" 
#         print("adding properties table with {}".format(alterAddProperty))
        spark.sql(alterAddProperty)
        
    logTaskProgress(cursor,batchTaskId,"Alterations completed for table {}.".format(tableName))
    #return true to signal success
    return True
  except Exception as e:
    errorMessage="Exception occured in the performTableAlterationsAndReload function : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to Execute usp_get_active_databricks_tables stored procedure
 # Function to execute usp_get_active_databricks_tables store procedure and get all the active databricks table details
def getActiveDatabricksTablesSpExec(conn,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
     # Execution of get the list of active table store procedures 
    sqlQuery = pd.read_sql_query("exec config.usp_get_active_databricks_tables", conn)
    logTaskProgress(cursor,batchTaskId,"executed usp_get_active_databricks_tables stored procedures completed ")
    return sqlQuery
  except Exception as e:
    errorMessage="Exception occurred while execution of usp_get_active_databricks_tables procedure: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to check table structure
# #Function to check the delta table structure and then alter if it is not matching with the metadata structure
def tableStructureCheck(tableHighDetailsDf,tableLowDetailsDf,createTableOnly,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    counter = 0
    currentTable = 'Unknown'
    #this variable is used to understand if everything was successful.  If anything fails this will be set to False
    allTablesSucceeded = True
    
    #convert to pandas, but really we should just use pandas from the start if possible - nice to have
    tableHighDetailsPandasDf = tableHighDetailsDf.toPandas()
    
    #get teh number of rows from the index lengeth
    totalTable = len(tableHighDetailsPandasDf.index)
    
    for index, tableRow in tableHighDetailsPandasDf.iterrows():
      try:
        counter = counter + 1
        currentTable = 'Unknown'

        #get the table high level details into variables for ease of referenceing
        tableName = tableRow["object_name"]
        
        print('Starting create/alter for: {}, table number {} or {}'.format(tableName, str(counter), str(totalTable)))
        
        currentTable = tableName
        tableLocation = tableRow["location"]
        tablePartitionScheme = tableRow["partition_scheme"]
        
        #assign an empty json string if there are no extended properties
        tableDeltaExtendedPropertiesJson = json.loads(tableRow["extended_properties"]) if len(tableRow["extended_properties"]) > 0 else json.loads('{}')
        
        if "delta_table_property" in tableDeltaExtendedPropertiesJson:
          tableDeltaExtendedPropertiesDict = tableDeltaExtendedPropertiesJson["delta_table_property"]
        else:
          tableDeltaExtendedPropertiesDict = {}
        
        if "schema" in tableDeltaExtendedPropertiesJson:
          objectSchemaLocation = tableDeltaExtendedPropertiesJson["schema"]  
        else:
          objectSchemaLocation = ''
       
        stringPrefix = tableRow["XML_string_prefix"]
        tableSchemaString = tableRow["schema_string"]
        
        objectColumn = ""
        schemaFromFile = ""
#         if tableName.startswith("Raw_"):
#           objectColumn = stringPrefix.split('.')[0][1:]
#           if "XML" in stringPrefix: 
#             structColumn = "xmlSchemaStruct"
#           else:
#             structColumn = "jsonSchemaStruct"
#           schemaFromFile = spark.read.format("csv").option("header","true").load(objectSchemaLocation).select(structColumn).collect()[0][0]  
      
        tableExists = tableRow["table_exists"]
        tablePartitionSchemeList = list(tablePartitionScheme.split(",")) if len(tablePartitionScheme) > 0 else list()
       
        #get the meta data table details, and only send required columns
        tableDetailsPandasDF = tableLowDetailsDf.filter(col("object_name") == tableName).select("object_attribute_name", "object_attribute_type", "object_attribute_order").toPandas() 
        
        if (tableExists == True and createTableOnly == False):
          #table exists and we CAN alter it as create table only is False
          tablesMatchBool, tableMatchDetail, currentTableColumnTypePandasDF, currentTablePropertiesDict, colsToAdd, colsInFinalTable = calculateTableDifferences( tableName
                                                                                                                                                                , tablePartitionSchemeList
                                                                                                                                                                , tableDeltaExtendedPropertiesDict
                                                                                                                                                                , tableDetailsPandasDF
                                                                                                                                                                , objectColumn
                                                                                                                                                                , schemaFromFile
                                                                                                                                                                , cursor
                                                                                                                                                                , batchTaskId
                                                                                                                                                                , errorMessage
                                                                                                                                                                , adfPipelineName
                                                                                                                                                                , clusterId
                                                                                                                                                                , notebookName
                                                                                                                                                                , errorLogFileLocation)
          
          if tablesMatchBool == False:
            tableAltered = performTableAlterationsAndReload( tableName
                                                           , tableMatchDetail
                                                           , currentTableColumnTypePandasDF
                                                           , colsInFinalTable
                                                           , colsToAdd
                                                           , schemaFromFile
                                                           , objectColumn
                                                           , tablePartitionScheme
                                                           , currentTablePropertiesDict
                                                           , tableDeltaExtendedPropertiesDict
                                                           , cursor
                                                           , batchTaskId
                                                           , errorMessage
                                                           , adfPipelineName
                                                           , clusterId
                                                           , notebookName
                                                           , errorLogFileLocation)

        elif (tableExists == True and createTableOnly == True):
          print("Table exists but we are in createTableOnly == True mode, so we will do nothing")
          
        elif (tableExists == False):
#           print("Table exists but we are in createTableOnly == False mode, so we will do nothing")
          #we can try and create the able as the table doesn't exist
          createTable(tableName,tableSchemaString,tablePartitionScheme,tableLocation
                        ,tableDeltaExtendedPropertiesDict,objectColumn,schemaFromFile,createTableOnly,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation) 
          print(tableSchemaString)
          logTaskProgress(cursor,batchTaskId,"successfully created the table:{}".format(tableName))
          
      except Exception as e:
        #allTablesSucceeded = False #set this to False as a table failed
        #attempt to log the exception for the current table
        errorMessage="Exception occured while created/altering table {}: ".format(currentTable) + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
       
      if allTablesSucceeded == False:
        print('Not all tables were successfully created/altered - check error log for specific error')
        raise Exception('Not all tables were successfully created/altered - check error log for specific error')
        
  except Exception as e:
    errorMessage="Exception occured while created/altered the table: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False     

# COMMAND ----------

# DBTITLE 1,Function definition to check if directory exist
#Function if the Folder exist
def dirCheck(directoryName):
  try:
    dirStatus = False
    dirStatus = bool(dbutils.fs.ls(directoryName))
  except Exception as e:
     if 'java.io.FileNotFoundException' in str(e):
        dirStatus = False
  return dirStatus

# COMMAND ----------

# DBTITLE 1,Function to Execute usp_get_active_location stored procedure
def getActiveLocation(conn
                      ,cursor
                      ,batchTaskId
                      ,errorMessage
                      ,adfPipelineName
                      ,clusterId
                      ,notebookName
                      ,errorLogFileLocation):
  try:
    #Execution of get active location stored procedure
    activeLocationsList = pd.read_sql_query("exec config.usp_get_active_location", conn)
    logTaskProgress(cursor,batchTaskId,"executed object to object high and low level stored procedures")
    return activeLocationsList
  except Exception as e:
    errorMessage = "Exception occured while execution of object high and low level stored procedures: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to transform metadata into pyspark dataframe for main_na_sqld_gen2
def convertPandasToSparkDfMainNaSqldGen2(activeLocationsList,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    schema = StructType([StructField("object_name", StringType(), True), StructField("location", StringType(), True)])
    #Convert pandas dataframe to spark dataframe
    activeLocations = convertPandasToSparkDfWithSchema(activeLocationsList,schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation) 
    logTaskProgress(cursor,batchTaskId,"Converted pandas dataframe as a spark dataframe")
    return activeLocations
  except Exception as e:
    errorMessage = "Exception occured while converting pandas dataframe to spark dataframe: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to create Folder if it does not exist
def createDirectoryIfNotExists(activeLocations,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  
  # collecting object names and locations as list
  try:

    for location in activeLocations.collect():
      locationExists = False
      #taking location from the list
      directoryName = location[1]  
      if dirCheck(directoryName) == True:
        continue
      else:
        dbutils.fs.mkdirs(directoryName)
    logTaskProgress(cursor,batchTaskId,"directories are created")
  except Exception as e:
    errorMessage = "Exception occured while creating directories in the respective locations: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function To execute  usp_get_historic_raw_eventhub_object_locations stored procedure
# get the object level information using the usp_get_historic_raw_eventhub_object_locations stored procedure 
def getHistoricRawEventhubObjectLocations(conn,cursor,batchId,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    rawLocation = pd.read_sql_query('exec config.usp_get_historic_raw_eventhub_object_locations {}'.format(batchId), conn)
    logTaskProgress(cursor,batchTaskId,'executed usp_get_historic_raw_eventhub_object_locations stored procedure successfully')
    return rawLocation
  except Exception as e:
    errorMessage = 'Exception occurred while executing usp_get_historic_raw_eventhub_object_locations stored procedure: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function To execute  get_raw_eventhub_object_locations stored procedures
# get the object level information using the usp_get_raw_eventhub_object_locations store procedure 
def getRawEventhubObjectLocations(conn,cursor,batchId,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    rawLocation = pd.read_sql_query('exec config.usp_get_raw_eventhub_object_locations ' + batchId, conn)
    logTaskProgress(cursor,batchTaskId,'executed usp_get_raw_eventhub_object_locations stored procedures successfully')
    return rawLocation
  except Exception as e:
    errorMessage = 'Exception occurred while executing usp_get_raw_eventhub_object_locations stored procedures: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to convert pandas to spark dataframe
#Convert the Pandas dataframe to Spark Dataframe 
def convertPandasToSparkDfLoadNaGen2Sqdb(rawLocation,cursor,batchTaskId,adfPipelineName,clusterId,
                                             notebookName,errorLogFileLocation): 
  try:
    schema = StructType([StructField("object_id"                   , IntegerType(), True), 
                         StructField("location"                    , StringType() , True),
                         StructField("format"                      , StringType() , True),
                         StructField("partition_date_time_columns" , StringType() , True),
                         StructField("infile_datetime_column"      , StringType() , True),
                         StructField("extended_properties"         , StringType() , True),
                         StructField("prev_batch_status"           , StringType() , True)
                        ])
    
    #creating a spark dataframe out of pandas dataframe        
    rawLocationDetails = convertPandasToSparkDfWithSchema(rawLocation,schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
    logTaskProgress(cursor,batchTaskId,'Converted pandas dataframe to spark dataframe')
    return rawLocationDetails 
  except Exception as e:
    errorMessage = 'Exception occurred while converting from pandas to spark dataframe: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to get list of directories
#Function gives the list of folder name in the given dir 
def getListOfDir(path,splitType):
  try :
    dirList = spark.createDataFrame(dbutils.fs.ls(path)).select(col('name')).rdd.flatMap(lambda x:x).collect()
    listOfFolder = [((splitType.split('=')[1].replace('/','')).zfill(6)) for splitType in dirList]
    return listOfFolder
  except Exception as e:
    errorMessage = 'Exception occurred while executing the getListOfDir function :' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to check folder for files
#Function return true if the given folder is having any files in it  
def checkFolderForFiles(dbfsPath):
  try :
    hasFiles = False
    if len(os.listdir(dbfsPath)) > 0:
      numberOfFiles = spark.createDataFrame(dbutils.fs.ls(dbfsPath.replace("/dbfs",""))).where("size > 0").count()
      if numberOfFiles > 0:
        hasFiles = True
    return hasFiles
  except Exception as e:
    errorMessage = 'Exception occurred while executing the checkFolderForFiles function :' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to get latest date 
#Get the latest date folder which is having files in it 
def getMaxNonNullDatePath(basePath, partitionScheme):
  try:
    foundDataInFolder = False
    date = 0
    
    pathList = partitionScheme.replace(' ', '').lower().split(',')
    #Get the List of Date folder name 
    listOfDateFolderName = getListOfDir(basePath ,pathList[0])
    #we can just sort on the first column as it is a date in INT format YYYYMMDD
    listOfDateFolderName.sort(reverse = True)

    #it looks like [20200911,20200918,20200912]
    for folderDate in listOfDateFolderName:
      latestDateFolder = '/dbfs{}/{}={}'.format(basePath, pathList[0], str(folderDate))
      # need to check here if the folder contains any file. if so return it, otherwise continue
      if checkFolderForFiles(latestDateFolder):
        foundDataInFolder = True
        date = folderDate
        return foundDataInFolder, date

    #if for loop exists, then no data found, so return False for found data and date as 0
    return foundDataInFolder, date
  except Exception as e:
    errorMessage = 'Exception occurred while executing the getMaxNonNullDatePath function : ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function To create Databases
def databaseCheck(tableHighDetailsDf,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    # Function To Create Databases if they present in metadata and not in spark
     
    existigdatabaseList=spark.catalog.listDatabases()
    existigdatabaseNameList=list(map(lambda x: x.name,existigdatabaseList))

    DatabaseName=tableHighDetailsDf.select(split(lower("object_name"), "\\.")[0].alias("DatabaseName"))
    metadataDatabaseList=DatabaseName.agg(collect_list(DatabaseName.DatabaseName)).collect()[0][0] 
    
    # print(metadataDatabaseList,existigdatabaseNameList)
    databaseToCreateList=list(set(metadataDatabaseList) - set(existigdatabaseNameList))
    if len(databaseToCreateList)>0:
        for database in databaseToCreateList:
            print("creating Database:{}".format(database))
            spark.sql("create database "+database)
    
   
  except Exception as e:
      errorMessage="unable to run databaseCheck Fuction: " + str(e)
      logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      assert False

# COMMAND ----------

# DBTITLE 1,Function to get earliest date 
#Get the earliest date folder which is having files in it 
def getMinNonNullDatePath(basePath, partitionScheme):
  try:
    foundDataInFolder = False
    date = 0
    
    pathList = partitionScheme.replace(' ', '').lower().split(',')
    #Get the List of Date folder name 
    listOfDateFolderName = getListOfDir(basePath ,pathList[0])
    #we can just sort on the first column as it is a date in INT format YYYYMMDD
    listOfDateFolderName.sort()

    #it looks like [20200911,20200918,20200912]
    for folderDate in listOfDateFolderName:
      earliestDateFolder = '/dbfs{}/{}={}'.format(basePath, pathList[0], str(folderDate))
      # need to check here if the folder contains any file. if so return it, otherwise continue
      if checkFolderForFiles(earliestDateFolder):
        foundDataInFolder = True
        date = folderDate
        return foundDataInFolder, date

    #if for loop exists, then no data found, so return False for found data and date as 0
    return foundDataInFolder, date
  except Exception as e:
    errorMessage = 'Exception occurred while executing the getMinNonNullDatePath function : ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to get latest date and time
#Get the latest date and time folder which is having files in it
def getMaxNonNullTimePath(basePath, partitionScheme):
  try : 
    foundDataInFolder = False
    date = 0
    time = 0
    pathList = partitionScheme.replace(' ', '').lower().split(',')
    
    #Get the List of Date folder name 
    listOfDateFolderName = getListOfDir(basePath, pathList[0])
    #we can just sort on the first column as it is a date in INT format YYYYMMDD
    listOfDateFolderName.sort(reverse = True)
    for folderDate in listOfDateFolderName:
      currentDateFolder = '/dbfs{}/{}={}'.format(basePath, pathList[0], str(folderDate))
      #does the currentDate folder contain any directories get the list of time folder from it
      if len(os.listdir(currentDateFolder)) > 0:
        #Get the List of Time folder name 
        listTimeFolderName = getListOfDir(currentDateFolder.replace('/dbfs',''), pathList[1])
        listTimeFolderName.sort(reverse = True)

        for folderTime in listTimeFolderName:
          currentTimeFolder = '/dbfs{}/{}={}'.format(currentDateFolder.replace('/dbfs',''), pathList[1], str(folderTime))

          #expecting files size >0
          if checkFolderForFiles(currentTimeFolder):
            foundDataInFolder = True
            date = folderDate
            time = folderTime
            return foundDataInFolder, date, time

    return foundDataInFolder, date, time
  except Exception as e:
    errorMessage = 'Exception occurred while executing the getMaxNonNullTimePath function : ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to get earliest date and time
#Get the earliest date and time folder which is having files in it
def getMinNonNullTimePath(basePath, partitionScheme):
  try : 
    foundDataInFolder = False
    date = 0
    time = 0
    pathList = partitionScheme.replace(' ', '').lower().split(',')
    
    #Get the List of Date folder name 
    listOfDateFolderName = getListOfDir(basePath, pathList[0])
    #we can just sort on the first column as it is a date in INT format YYYYMMDD
    listOfDateFolderName.sort()
    for folderDate in listOfDateFolderName:
      currentDateFolder = '/dbfs{}/{}={}'.format(basePath, pathList[0], str(folderDate))
      #does the currentDate folder contain any directories get the list of time folder from it
      if len(os.listdir(currentDateFolder)) > 0:
        #Get the List of Time folder name 
        listTimeFolderName = getListOfDir(currentDateFolder.replace('/dbfs',''), pathList[1])
        listTimeFolderName.sort()

        for folderTime in listTimeFolderName:
          currentTimeFolder = '/dbfs{}/{}={}'.format(currentDateFolder.replace('/dbfs',''), pathList[1], str(folderTime))

          #expecting files size >0
          if checkFolderForFiles(currentTimeFolder):
            foundDataInFolder = True
            date = folderDate
            time = folderTime
            return foundDataInFolder, date, time

    return foundDataInFolder, date, time
  except Exception as e:
    errorMessage = 'Exception occurred while executing the getMaxNonNullTimePath function : ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to get the max date and time for each object
# this function makes latest date time entry into the tbl_object_dates_availability table for the raw objects 
def rawObjectLatestDateTime(rawLocationDetails,cursor,batchTaskId
                            ,adfPipelineName,clusterId,notebookName,errorLogFileLocation): 
  try:
    maxDate = 0
    maxTime = 0
    # rawLocationDetails will have the list of objects 
    for i in range(0,rawLocationDetails.count()):
      #convert dataframe into dictionary
      rawLocationDetailsDic = rawLocationDetails.collect()[i].asDict()
      #get dictionary values into variables
      objectId = rawLocationDetailsDic['object_id']
      objLocation = rawLocationDetailsDic['location']
      objFormat = rawLocationDetailsDic['format']
      objPartitionScheme = rawLocationDetailsDic['partition_date_time_columns']
      objInfileDatetimeColumn = rawLocationDetailsDic['infile_datetime_column']
      objExtendedProperties = rawLocationDetailsDic['extended_properties']
      
      #get the json from the extended properties
      #CB: need to check if this is populated...
      objExtendedProperties = '{}' if len(objExtendedProperties) == 0 else objExtendedProperties
      objExtendedPropertiesJson = json.loads(objExtendedProperties) 
      
      #get the sourceTimeLag from the json ---> this may have to be changed to objExtendedPropertiesJson["general"]["sourceTimeLag"] if we move the sourceTimeLag to general
      if "sourceTimeLag" in objExtendedPropertiesJson:
        objSourceTimeLag = objExtendedPropertiesJson["sourceTimeLag"]
      else:
        objSourceTimeLag = {"minutes" : 0}
         
      #remove the trailing / from the object location
      objLocation = objLocation[0:-1] if objLocation[-1:] == "/" else objLocation
      
      #if the partition schema has date then get the max date 
      if (len(objPartitionScheme.replace(' ', '').lower().split(','))) == 1 :
        foundDataInFolder, maxDate = getMaxNonNullDatePath(objLocation, objPartitionScheme)
        #If there are no files in date directories make maxDatetime as default value
        if foundDataInFolder == False:
          maxDatetime = datetime.strptime(str("99990101"), '%Y%m%d')
        
        #If infile_datetime_column = ‘n/a’ then return the date from folder
        elif objInfileDatetimeColumn == 'n/a':
          maxDatetime = datetime.strptime(str(maxDate), '%Y%m%d')
        
        #If infile_datetime_column is null then return the date from folder else rturn the date from file
        elif objInfileDatetimeColumn is None:
          maxDatetime = datetime.strptime(str(maxDate), '%Y%m%d')
        else :
          maxDatetime = (spark.read.format('com.databricks.spark.avro')
                         .load(objLocation)
                         #get the infile date time column from the variable populated from metadata
                         .selectExpr("to_timestamp({},'M/d/y h:m:s a') AS InfileDateTime".format(objInfileDatetimeColumn))
                         .where("date="+str(maxDate))
                         .agg({"InfileDateTime": "max"})
                         .collect()[0][0]
                        )
        
       #if the partition schema has date then get the max dateTime 
      else :
        foundDataInFolder, maxDate, maxTime = getMaxNonNullTimePath(objLocation, objPartitionScheme)
        #If there are no files in date and time directories make maxDatetime as default value
        if foundDataInFolder == False:
          maxDatetime = datetime.strptime(str("99990101"), '%Y%m%d')
        else :
          maxDatetimeUnformatted = str(maxDate)+str(maxTime)
          #Formatted date and time
          maxDatetime = datetime.strptime(maxDatetimeUnformatted,"%Y%m%d%H%M%S")
          #Subtract the sourceTimeLag to the maxDatatime
          maxDatetime = maxDatetime - timedelta(**objSourceTimeLag)
              
      #update tbl_object_dates_availability with the latest date using the usp_log_availability_dates store procedure
      cursor.execute('exec  audit.usp_log_availability_dates ?, ?, ? ',(batchTaskId,objectId,maxDatetime))
      logTaskProgress(cursor,batchTaskId,'Succesfully fetched the max date and time for the object')
      
  except Exception as e:
    errorMessage = 'Exception occured while fetching the max date and time for the object: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to get the load date and time for historic objects
# this function makes latest date time entry into the tbl_object_dates_availability table for the raw objects 
def rawObjectHistoricDateTime(rawLocationDetails,cursor,batchTaskId
                            ,adfPipelineName,clusterId,notebookName,errorLogFileLocation): 
  try:
    maxDate = 0
    maxTime = 0
    minDate = 0
    minTime = 0
    FoundFolder=False
    # rawLocationDetails will have the list of objects 
    for i in range(0,rawLocationDetails.count()):
      #convert dataframe into dictionary
      rawLocationDetailsDic = rawLocationDetails.collect()[i].asDict()
      #get dictionary values into variables
      objectId = rawLocationDetailsDic['object_id']
      objLocation = rawLocationDetailsDic['location']
      objFormat = rawLocationDetailsDic['format']
      objPartitionScheme = rawLocationDetailsDic['partition_date_time_columns']
      objInfileDatetimeColumn = rawLocationDetailsDic['infile_datetime_column']
      objExtendedProperties = rawLocationDetailsDic['extended_properties']
      prevBatchStatus=rawLocationDetailsDic['prev_batch_status']

      #get the json from the extended properties
      objExtendedPropertiesJson = json.loads(objExtendedProperties) 

      #get the sourceTimeLag from the json ---> this may have to be changed to objExtendedPropertiesJson["general"]["sourceTimeLag"] if we move the sourceTimeLag to general
      if "sourceTimeLag" in objExtendedPropertiesJson:
        objSourceTimeLag = objExtendedPropertiesJson["sourceTimeLag"]
      else:
        objSourceTimeLag = {"minutes" : 0}

      #get the historicLoadTreshold from the json
      if "historicLoadTreshold" in objExtendedPropertiesJson:
        objHistoricLoadTreshold = objExtendedPropertiesJson["historicLoadTreshold"]
      else:
        objHistoricLoadTreshold = {"days" : 0}

      #remove the trailing / from the object location
      objLocation = objLocation[0:-1] if objLocation[-1:] == "/" else objLocation

      #if the partition schema has date then get the max and min date 
      if (len(objPartitionScheme.replace(' ', '').lower().split(','))) == 1 :
        foundDataInFolder, maxDate = getMaxNonNullDatePath(objLocation, objPartitionScheme)
        foundDataInFolder, minDate = getMinNonNullDatePath(objLocation, objPartitionScheme)
        #If there are no files in date directories make maxDatetime and minDateTime as default value
        if foundDataInFolder == False:
          maxDatetime = datetime.strptime(str("99990101"), '%Y%m%d')
          minDatetime = datetime.strptime(str("20000101"), '%Y%m%d')

        #If infile_datetime_column is null then return the date from folder else rturn the date from file
        elif objInfileDatetimeColumn is None:
          maxDatetime = datetime.strptime(str(maxDate), '%Y%m%d')
          minDatetime = datetime.strptime(str(minDate), '%Y%m%d')
        else :
          maxDatetime = (spark.read.format('com.databricks.spark.avro')
                         .load(objLocation)
                         #get the infile date time column from the variable populated from metadata
                         .selectExpr("to_timestamp({},'MM/dd/yyyy hh:mm:ss aa') AS InfileDateTime".format(objInfileDatetimeColumn))
                         .where("date="+str(maxDate))
                         .agg({"InfileDateTime": "max"})
                         .collect()[0][0]
                        )
          minDatetime = (spark.read.format('com.databricks.spark.avro')
                         .load(objLocation)
                         #get the infile date time column from the variable populated from metadata
                         .selectExpr("to_timestamp({},'MM/dd/yyyy hh:mm:ss aa') AS InfileDateTime".format(objInfileDatetimeColumn))
                         .where("date="+str(minDate))
                         .agg({"InfileDateTime": "min"})
                         .collect()[0][0]
                        )

       #if the partition schema has date then get the max and min dateTime 
      else :
        foundDataInFolder, maxDate, maxTime = getMaxNonNullTimePath(objLocation, objPartitionScheme)
        foundDataInFolder, minDate, minTime = getMinNonNullTimePath(objLocation, objPartitionScheme)

        #If there are no files in date and time directories make maxDatetime as default value
        if foundDataInFolder == False:
          maxDatetime = datetime.strptime(str("99990101"), '%Y%m%d')
          minDatetime = datetime.strptime(str("20000101"), '%Y%m%d')
        else :
          maxDatetimeUnformatted = str(maxDate)+str(maxTime)
          minDatetimeUnformatted = str(minDate)+str(minTime)
          #Formatted date and time
          maxDatetime = datetime.strptime(maxDatetimeUnformatted,"%Y%m%d%H%M%S")
          minDatetime = datetime.strptime(minDatetimeUnformatted,"%Y%m%d%H%M%S")
          #Subtract the sourceTimeLag to the maxDatatime
          maxDatetime = maxDatetime - timedelta(**objSourceTimeLag)

      #get the max outuput datetime to from the availability dates
      maxOutputDatetimeTo = getMaxOutputDatetimeTo(objectId,conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

      #case the maxOutputDatetimeTo is null (never loaded before) use the min date time from the files
      if maxOutputDatetimeTo == None:
        maxOutputDatetimeTo = minDatetime
      else:
        #add the treshold
        maxOutputDatetimeTo = maxOutputDatetimeTo + timedelta(**objHistoricLoadTreshold)

      if maxOutputDatetimeTo.date() <= maxDatetime.date():
        #case the maxOutputDatetimeTo  + the treshold is less then the max date time from the files, use the maxOutputDatetimeTo  + the treshold as max date time
        if maxOutputDatetimeTo <= maxDatetime:
          maxDatetime = maxOutputDatetimeTo

        #CB: assign object location = basePath
        basePath = objLocation
        #update tbl_object_dates_availability with the latest date using the usp_log_availability_dates store procedure
        #Update only if the previous batch is completed
        if prevBatchStatus.lower()=='completed':
          #Loop thorugh the folders untill folder exists by incrimenting treshold
          while FoundFolder==False:
            folderDate=int(maxDatetime.strftime('%Y%m%d'))
            pathList = objPartitionScheme.replace(' ', '').lower().split(',')

            if len(os.listdir('/dbfs{}'.format(basePath))) > 0:
              numberOfTimeFiles = spark.createDataFrame(dbutils.fs.ls('{}/{}={}'.format(basePath, pathList[0], str(folderDate)))).where("name like 'time%'").count()
              if numberOfTimeFiles > 0:
                timeFolder=spark.createDataFrame(dbutils.fs.ls('{}/{}={}'.format(basePath, pathList[0], str(folderDate)))).where("name like 'time%'").select("name").collect()[0][0]
                folderToCheck = '/dbfs{}/{}={}/{}'.format(basePath, pathList[0], str(folderDate), timeFolder.replace("/",""))
              else:
                folderToCheck = '/dbfs{}/{}={}'.format(basePath, pathList[0], str(folderDate))

            #Check the folder exists or not
            FoundFolder=checkFolderForFiles(folderToCheck)
            if FoundFolder == False:
              #Incrment the MaxDatetime by TresholdLimit
              maxDatetime = maxDatetime + timedelta(**objHistoricLoadTreshold)  
          cursor.execute('exec audit.usp_log_availability_dates ?, ?, ? ',(batchTaskId,objectId,maxDatetime))
          logTaskProgress(cursor,batchTaskId,'Succesfully fetched the max date and time for the object')
      else:
        logTaskProgress(cursor,batchTaskId,'No new data to load for the object')
      
  except Exception as e:
    errorMessage = 'Exception occured while fetching the max date and time for the object: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function To execute usp_get_max_input_datetime_to stored procedure
# get the max input datetime to from usp_get_max_output_datetime_to stored procedure 
def getMaxOutputDatetimeTo(objectId,conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    maxOutputDatetimeToPd = pd.read_sql_query("exec config.usp_get_max_output_datetime_to {}".format(objectId), conn)
    
    #fetch the input parameters
    maxOutputDatetimeTo = maxOutputDatetimeToPd.at[0,'max_output_datetime_to']
    
    return maxOutputDatetimeTo
  except Exception as e:
    errorMessage = 'Exception occurred while executing usp_get_max_output_datetime_to stored procedures ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function To execute get_object_retention stored procedures
# get the object delete statement from usp_get_object_retention store procedure 
def getObjectRetention(conn,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    retentionDetails = pd.read_sql_query("exec config.usp_get_object_retention {}".format(batchTaskId), conn)
    return retentionDetails
  except Exception as e:
    errorMessage = 'Exception occurred while executing usp_get_object_retention stored procedures: ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1, Function to execute the delete statement on each Delta table 
#retentionDeleteDetails dataframe will have the list of object and a delete statement to be executed in the delta table 
# this function will delete all the records applying where condition on a date or variant to remove records from an object
def deleteRetentionData(retentionDeleteDetails,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    for retentionDeleteDetailsRow in retentionDeleteDetails.collect():
      #convert dataframe into dictionary
      retentionDeleteDetailsDic = retentionDeleteDetailsRow.asDict()
      #get dictionary values into variables
      objectId = retentionDeleteDetailsDic['object_id']
      objName= retentionDeleteDetailsDic['object_name']
      objDefinitionId = retentionDeleteDetailsDic['object_definition_id']
      objAttributeName = retentionDeleteDetailsDic['object_attribute_name']
      objRetentionDuration = retentionDeleteDetailsDic['retention_duration_days']
      objDeleteStatement = retentionDeleteDetailsDic['delete_statement']

      #Log start of delete including the the delete statement that is being executed
      logTaskProgress(cursor,batchTaskId,'Execution of delete statement {} started'.format(objDeleteStatement))

      #Execute the delete statement 
      spark.sql(objDeleteStatement)

      #Log the completion of the delete
      logTaskProgress(cursor,batchTaskId,'Successfully executed the delete statement in the table '.format(objName))

  except Exception as e:
    errorMessage = 'Exception occurred while executing delete statement on the delta table ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Function to create date directories to test load_na_gen2_sqdb
def createDateDirectory(mainDirDate,commonDirDate,commonDirDateDate):
  try:
    for mDir in mainDirDate:
      for cDir in commonDirDate:
        for cDateDir in commonDirDateDate:
          dbutils.fs.mkdirs(mDir+'/'+cDir+'/'+cDateDir)
          pass
    logTaskProgress(cursor,batchTaskId,'Successfully created directories for date level')

  except Exception as e:
    errorMessage = 'Exception occurred while creating directories for date level ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Function to create date and time directories to test load_na_gen2_sqdb
def createDateTimeDirectory(mainDirTime,commonDirTime,commonDirDateTime,commonDirDateDateTime):
  try:
    for mDir in mainDirTime:
      for cDir in commonDirTime:
        for cDateDir in commonDirDateTime:
          for cTimeDir in commonDirDateDateTime:
            dbutils.fs.mkdirs(mDir+'/'+cDir+'/'+cDateDir+'/'+cTimeDir)
            pass
    logTaskProgress(cursor,batchTaskId,'Successfully created directories for date level')

  except Exception as e:
    errorMessage = 'Exception occurred while creating directories for date level ' + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Function to call stored procedure usp_get_maintenance_object
 # Function to execute usp_get_maintenance_object store procedure and get all the active databricks table details
def getMaintenanceObjectsSpExec(conn,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
     # Execution of get the list of active table store procedures 
    maintenanceObjectsDf = pd.read_sql_query("exec config.usp_get_maintenance_object {}".format(batchTaskId), conn)
    logTaskProgress(cursor,batchTaskId,"executed usp_get_maintenance_object stored procedures completed ")
    return maintenanceObjectsDf
  except Exception as e:
    errorMessage="Exception occurred while execution of usp_get_maintenance_object procedure: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to vacuum the delta table
def deltaTableVacuum(maintenanceObjectsDf,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #Get the object details from pandas dataframe
    objectId = maintenanceObjectsDf.at[0,'object_id']
    objectName = maintenanceObjectsDf.at[0,'object_name']
    objectLocation = maintenanceObjectsDf.at[0,'object_location']
    
    #Optimize the delta table
    spark.sql("VACUUM '{}'".format(objectLocation))
    logTaskProgress(cursor,batchTaskId,"successfully vacuumed the table:{}".format(objectName))
        
  except Exception as e:
    errorMessage = "Exception occured while vacuuming the table: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False          

# COMMAND ----------

# DBTITLE 1,Helper function to getMaxDateHour to create udf
def getEnding(pathValue, currentPartition):
  return pathValue[pathValue.rindex(currentPartition)+len(currentPartition)+1:]

# COMMAND ----------

# DBTITLE 1,Function to get max date and hour for optimise - getMaxDateHour
def getMaxDateHour(objectName,objectLocation,tablePartitionScheme,datetimePartitionColumns):
  try:
    #Append the location to '/dbfs'
    location = "/dbfs" + objectLocation
    wildcardAddition = '*,'
    #Get all partiton scheme without any spaces
    fullPartitionsList = tablePartitionScheme.replace(" ", "")

    #Get just the date and time partitions and remove spaces
    dateTimePartitions = datetimePartitionColumns.replace(" ", "")
    #Get partitons list upto date time
    fullPartitionsList = fullPartitionsList.split(dateTimePartitions)[0] + dateTimePartitions
    #Get date and time partitons list
    dateHourPartitionsList = dateTimePartitions.split(",")
    rootPartitionsToSkip = "," + fullPartitionsList.replace(dateTimePartitions, "").replace(",", wildcardAddition)
    #Get the partition to skip and by adding wildcardAddition
    #Verify if date is the first partiton in partition_scheme then skipping other partitions to search.Here searching date and time partitions only
    if "date" in fullPartitionsList.split(',')[0].lower():
      rootPartitionsToSkip = '/'  
    #Verify if dateHourPartitionsList list length > 1 then proceed to get Date an hour details
    if len(dateHourPartitionsList) >= 1:
      #Verify if Date is in partition list
      if "date" in dateHourPartitionsList[0].lower():
        #Construct the path to get all date level directories
        searchString = location + rootPartitionsToSkip + dateHourPartitionsList[0] + "*"
        searchStringJoin = searchString.split(",")        
        #This returns the list of all partition level directories
        dateDirectoryList = glob.glob(os.path.join(*searchStringJoin))
        #Create dataframe to get maxDate
        dateDF = (spark.createDataFrame(dateDirectoryList, StringType())
                  .withColumn("date"
                              , getEndingUdf(col("value"), lit(dateHourPartitionsList[0]))
                             )
                  .filter("date != '__HIVE_DEFAULT_PARTITION__'" )
                  .agg({"date":"max"}).alias("maxDate")
                  .toDF("maxDate")
            )
        #If maxDate count is 1 and value is not None then take that value as maxDate
        if (dateDF.count() == 1) and dateDF.collect()[0][0] is not None:
          maxDate = dateDF.collect()[0][0]
          # if list of partitions length is 2 then try for Hour partition
          if len(dateHourPartitionsList) == 2:
            if "hour" in  dateHourPartitionsList[1].lower() or "time" in  dateHourPartitionsList[1].lower():
              #Construct the hour path
              searchString = location + rootPartitionsToSkip + dateHourPartitionsList[0] + "=" + maxDate + "," + dateHourPartitionsList[1] + "*"
              searchStringJoin = searchString.split(",")
              #This returns the list of all partition level directories upto Hour
              hourDirectoryList = glob.glob(os.path.join(*searchStringJoin))
              #Create dataframe to get maxHour
              hourDF = (spark.createDataFrame(hourDirectoryList, StringType())
                        .withColumn("hour"
                                    , getEndingUdf(col("value"), lit(dateHourPartitionsList[1]))
                                   )
                        .filter("hour != '__HIVE_DEFAULT_PARTITION__'" )
                        .agg({"hour":"max"}).alias("maxHour")
                        .toDF("maxHour")
                  )
              #If maxHour count is 1 then take that value as maxHour
              if (hourDF.count() == 1):
                maxHour = hourDF.collect()[0][0]
          else:
            #If datetime partition list is only one then make maxHour as None
            maxHour = None
        else:
          #If maxDate is None then make maxHour as None and return both as None
          maxDate = dateDF.collect()[0][0]
          maxHour = None
            
    return maxDate,maxHour
    
  except Exception as e:
    errorMessage = "Exception occured while getting maxDate and maxTime: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False   
  

# COMMAND ----------

# DBTITLE 1,Function to optimize the delta table
def deltaTableOptimise(getLastOptimiseDf,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #Get the object details from pandas dataframe
    optimiseLogId = getLastOptimiseDf.at[0,'log_optimise_id']
    objectId = getLastOptimiseDf.at[0,'object_id']
    objectName = getLastOptimiseDf.at[0,'object_name']
    objectLocation = getLastOptimiseDf.at[0,'location']
    fullOrPartial = getLastOptimiseDf.at[0,'full_or_partial']
    whereCondition = getLastOptimiseDf.at[0,'where_clause']
    tablePartitionScheme = getLastOptimiseDf.at[0,'partition_scheme']
    datetimePartitionColumns = getLastOptimiseDf.at[0,'datetime_partition_columns']
    
    #Verify fullOrPartial if 'FULL' no need to apply where clause.If  'PARTIAL' need to apply where caluse to optmize the particular partition
    
    if fullOrPartial == 'FULL':    
      #Optimize the delta table
      spark.sql("OPTIMIZE '{}'".format(objectLocation))
      #For 'FULL' no values for date and hour
      partitionDateOptimisedTo = None
      partitionHourOptimisedTo = None
      
      #Mark table as optimised with latest date and hour in table tbl_log_optimise
      markTableAsOptimised(int(optimiseLogId), partitionDateOptimisedTo, partitionHourOptimisedTo, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
    
    if fullOrPartial == 'PARTIAL':    
      #Optimize the delta table
      spark.sql("OPTIMIZE '{}' {}".format(objectLocation,whereCondition))
      #Get latest date and hour from the getMaxDateHour function
      partitionDateOptimisedTo,partitionHourOptimisedTo = getMaxDateHour(objectName,objectLocation,tablePartitionScheme,datetimePartitionColumns)
      if partitionDateOptimisedTo is not None:
        #Mark table as optimised with latest date and hour in table tbl_log_optimise
        markTableAsOptimised(int(optimiseLogId), partitionDateOptimisedTo, partitionHourOptimisedTo, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation)    

    logTaskProgress(cursor,batchTaskId,"successfully Optimized the table:{}".format(objectName))
        
  except Exception as e:
    errorMessage = "Exception occured while optimizing the table: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False          

# COMMAND ----------

# DBTITLE 1,Function to call stored procedure usp_get_last_optimise
 # Function to execute usp_get_last_optimise store procedure and get the active databricks table details
def getLastOptimise(conn,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
     # Execution of get the list of active table store procedures 
    getLastOptimiseDf = pd.read_sql_query("exec audit.usp_get_last_optimise {}".format(batchTaskId), conn)
    logTaskProgress(cursor,batchTaskId,"executed usp_get_last_optimise stored procedures completed ")
    
    return getLastOptimiseDf
  except Exception as e:
    errorMessage="Exception occurred while execution of usp_get_last_optimise procedure: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to combine optimise and vacuum related functions
def deltaTableOptimiseVacuum(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #Call deltaTableOptimise and deltaTableVacuum functions to optimise and vacuum the delta tables for given batch_task_id

    #Optimize the delta table
    #Get last optimised details by running stored procedure function 
    getLastOptimiseDf = getLastOptimise(conn,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    #Pass getLastOptimiseDf to deltaTableOptimise function to optimise based on the conditions
    deltaTableOptimise(getLastOptimiseDf,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    
    logTaskProgress(cursor,batchTaskId,"successfully optimized the delta table for batch_task_id {}".format(batchTaskId))
    
    #Vacuum the delta table
    #Get maintenance object details by running stored procedure function
    maintenanceObjectsDf = getMaintenanceObjectsSpExec(conn,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    #Pass the maintenanceObjectsDf details to deltaTableVacuum function
    deltaTableVacuum(maintenanceObjectsDf,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

    logTaskProgress(cursor,batchTaskId,"successfully vacuumed the delta table for batch_task_id {}".format(batchTaskId))
        
  except Exception as e:
    errorMessage = "Exception occurred while optimizing and vacuuming the table: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False          

# COMMAND ----------

# DBTITLE 1,Function to call stored procedure usp_mark_table_as_optimised
def markTableAsOptimised(optimiseLogId, partitionDateOptimisedTo, partitionHourOptimisedTo, cursor, batchTaskId, adfPipelineName, clusterId, notebookName, errorLogFileLocation):
  try:
    cursor.execute("exec audit.usp_mark_table_as_optimised ?,?,?", optimiseLogId, partitionDateOptimisedTo, partitionHourOptimisedTo)
    while cursor.nextset():
      x = 1
    logTaskProgress(cursor,batchTaskId,"executed usp_mark_table_as_optimised stored procedures completed ")  
  except Exception as e:
    errorMessage="Exception occured while logging optimised details : " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to create master view in conformed layer
def createConformedMasterView(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:
        #execute the stored procedure and copy the data to dataframe ---- the procedure joins tbl_task, tbl_object, tbl_task_object_map, tbl_task_type and tbl_object_definition and provides mapping between master conforemd view and underlying objects
        config = pd.read_sql_query("exec config.usp_get_source_to_master_view_details",conn)
        dfConfig = spark.createDataFrame(config)
        logTaskProgress(cursor,batchTaskId,"execution of usp_get_source_to_master_view_details stored procedures completed ")
        
        #start a loop for each task_id and identify the object_id associated with it. Store the create script in "result" variable
        tmp_lstTaskId = dfConfig.select("task_id").distinct().collect()
        for taskId in range(dfConfig.select("task_id").distinct().count()):
            lstTaskId = tmp_lstTaskId[taskId][0]
            #print("task_id:"+str(lstTaskId))
            result = []
            result0 = "create or replace view "+str(dfConfig.where("task_id = '{}'".format(lstTaskId)).collect()[0][6])+" as"
            result.append(result0)
            #start a loop for each object id and create dataframes for source as well as target attributes
            for objectId in range(dfConfig.where("task_id = '{}'".format(lstTaskId)).select("src_obj_id").distinct().count()):
                #print(" object_counter:"+str(objectId))
                lstObjectId = (dfConfig.where("task_id = '{}'".format(lstTaskId)).select("src_obj_id")
                               .distinct().orderBy("src_obj_id").collect()[objectId][0])
                #print(" object_id:"+str(lstObjectId))
                dfSrcCol = (dfConfig.where("src_obj_id = '{}'".format(lstObjectId)).where("task_id = '{}'".format(lstTaskId))
                           .select("src_obj_id","src_obj_nm","src_attb_nm"))
                lstObjectNm = str(dfSrcCol.select("src_obj_nm").distinct().collect()[0][0])
                #print(" object_name:"+str(lstObjectNm))
                dfTgtCol = (dfConfig.where("task_id = '{}'".format(lstTaskId)).select("tgt_attb_nm","tgt_order").distinct())
                dfCol = dfSrcCol.join(dfTgtCol,dfSrcCol.src_attb_nm == dfTgtCol.tgt_attb_nm,"right").orderBy("tgt_order")
                #logic to add source name in column list
                #result1 = str("'"+'{}'.format(lstObjectNm)+"'")+" as source, "
                #print(result1)
                result1 = " select "
                result.append(result1)
                #generate source to target column level mapping for each object
                tmp_col = dfCol.collect()
                for columnId in range(dfCol.count()):
                    #print("  column_counter:"+str(objectId))
                    if columnId != dfCol.count()-1:
                        result2 = str(tmp_col[columnId][2])+" as "+str(tmp_col[columnId][3])+", "
                    else:
                        result2 = str(tmp_col[columnId][2])+" as "+str(tmp_col[columnId][3])+" "
                    #print("  column_name:"+result2)
                    result.append(result2.replace("None","NULL"))
                result3 = "from "
                result.append(result3+lstObjectNm)
                #add union all clause between each select statement, if there is no more object_id then union all clause is not required
                if objectId != dfConfig.select("src_obj_id").distinct().count()-1:
                    #result4 = " union all "
                    result4 = ""
                    result.append(result4)
            sqlCreateView = str(result).replace("', '","").replace("'","").replace("[","").replace("]","")
            #print(sqlCreateView)
            #execute the create view script
            spark.sql(sqlCreateView)
            logTaskProgress(cursor,batchTaskId,"Master View"+lstObjectNm+"created successfully")
            #print("MasterView "+str(dfTaskObj.collect()[taskId][3])+" created successfully")

        logTaskProgress(cursor,batchTaskId,"All Conformed Master Views created successfully")
      
    except Exception as e:
        errorMessage="Exception occured while importing backend config tables : " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False

# COMMAND ----------

# DBTITLE 1,Function to execute config.usp_get_active_synapse_tables
 # Function to execute usp_get_active_databricks_tables store procedure and get all the active databricks table details
def getActiveSynapseTablesSpExec(conn,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
     # Execution of get the list of active table store procedures 
    activeSynapseTablesPandasDF = pd.read_sql_query("exec config.usp_get_active_synapse_tables", conn)
    logTaskProgress(cursor,batchTaskId,"executed usp_get_active_databricks_tables stored procedures completed ")
    return activeSynapseTablesPandasDF
  except Exception as e:
    errorMessage="Exception occurred while execution of usp_get_active_databricks_tables procedure: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to get synapse table details in spark dataframe
#get active table details in the required format from pandas data frame
def getSynapseTableDetailsDf(activeSynapseTablesPandasDF,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #schema structre of the procudure result 
    schema = StructType([ StructField("object_name"                           , StringType()  , True)
                         ,StructField("object_attribute_name"                 , StringType()  , True)
                         ,StructField("object_attribute_type"                 , StringType()  , True)
                         ,StructField("object_attribute_order"                , IntegerType() , True)
                         ,StructField("hash_distribution_order"               , IntegerType()  , True)
                         ,StructField("schema_string"                         , StringType()  , True) 
                         ,StructField("data_model_group"                      , StringType()  , True) ])
    #converting pandas to spark data frame
    activeSynapseTableDf = convertPandasToSparkDfWithSchema(activeSynapseTablesPandasDF,schema,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation).na.fill('')
    
    #to split object_name to schema and table name
    activeSynapseTableDf = (activeSynapseTableDf.withColumn("schema_name", split(col("object_name"), '[.]',2).getItem(0))
                                                .withColumn("table_name", split(col("object_name"), '[.]',2).getItem(1)))
    
    #to get all hash distribution attributes as comma sperated values in hash_distirubiton_attributes
    #at the moment synapse only allows one distribution column per table so it is expected to have one hash distibution attribute per table
    tableHighDetailsDf = (activeSynapseTableDf.groupBy("object_name")
                             .agg(first("schema_name").alias("schema_name")
                                  ,first("table_name").alias("table_name")
                                  ,first("schema_string").alias("schema_string")
                                  ,first("data_model_group").alias("data_model_group")
                                  )) 
    
    #string aggregation of attribute names as comma seprated values
    tableLowLevelDetailsDf = (activeSynapseTableDf.groupBy("object_name")
                                                  .agg(concat_ws(", ", collect_list("object_attribute_name")).alias("object_attributes")))
    
    tableDetailsDf = tableHighDetailsDf.join(tableLowLevelDetailsDf, ['object_name'],"inner" )
    
    logTaskProgress(cursor,batchTaskId,"Got synapse table details in spark dataframe")
    
    return tableDetailsDf
  except Exception as e:
    errorMessage="Exception thrown while getting synapse table details in spark data frame: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to generate sql scripts for staging merge stored procedures
def generateSynapseMergeSPs(spObjectName,objectName,externalObjectName,targetAttributes,sourceAttributes,mergeSPFilePath,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:
        mergeSpScript = ("CREATE PROC {} @batchId INT,@batchTaskId INT AS\n ".format(spObjectName)
           +"DECLARE	@transtate BIT\n BEGIN \n IF @@TRANCOUNT = 0 \n BEGIN \n SET @transtate = 1 \n BEGIN TRANSACTION \n END \n BEGIN TRY \n"
           +"DECLARE @CurrentTimeStamp DATETIME2 = GETDATE()\n "
           +"MERGE {} as tgt\n USING {} as src \n ON tgt.HashedBusinessKey = src.HashedBusinessKey AND tgt.lakeValidFromTimestamp = src.lakeValidFromTimestamp\n".format(objectName,externalObjectName)
           +"WHEN MATCHED THEN\n UPDATE SET tgt.lakeValidToTimestamp = src.lakeValidToTimestamp, tgt.lakeIsActive = src.lakeIsActive, tgt.lakeLastUpdateDate = src.lakeLastUpdateDate, tgt.lakeLastUpdateTimestamp = src.lakeLastUpdateTimestamp, tgt.lakeLastUpdatedBatchID = src.lakeLastUpdatedBatchID, tgt.dwhLastUpdateTimestamp = @CurrentTimeStamp, tgt.dwhLastUpdatedBatchID = @batchId")

        if 'lakeDeletedTimestamp' in targetAttributes:
          mergeSpScript = mergeSpScript + ", tgt.lakeDeletedTimestamp = src.lakeDeletedTimestamp"

        mergeSpScript = (mergeSpScript + "\nWHEN NOT MATCHED THEN \n INSERT ({})\n VALUES ({});\n ".format(targetAttributes,sourceAttributes)
           +"EXEC audit.usp_populate_staging_dates_loaded @batchTaskId, '{}', '{}', @CurrentTimeStamp \n".format(objectName,externalObjectName)            
           +"IF @transtate = 1 COMMIT TRANSACTION \n END TRY \n BEGIN CATCH \n DECLARE @Error_Message VARCHAR(5000) = ERROR_MESSAGE() \n DECLARE @Error_Severity INT = ERROR_SEVERITY() \n DECLARE @Error_State INT = ERROR_STATE() \n IF @transtate = 1 \n AND XACT_STATE() <> 0 \n ROLLBACK TRANSACTION \n RAISERROR (@Error_Message, @Error_Severity, @Error_State) \n END CATCH \n END")

        dbutils.fs.put(mergeSPFilePath, mergeSpScript, True)
        logTaskProgress(cursor,batchTaskId,"Create proc script for {} generated successfully".format(spObjectName))
    except Exception as e:
        errorMessage="Unable to genrate create proc script: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False


# COMMAND ----------

# DBTITLE 1,Function to generate sql scripts for Staging UPSERT stored procedures
def generateSynapseUpsertSPs(spObjectName,objectName,externalObjectName,targetAttributes,sourceAttributes,upsertSPFilePath,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:
        upsertSpScript = (f"""
CREATE PROC {spObjectName} @batchId INT, @batchTaskId INT 
AS
/* **********************************************************************
Author:  Framework
Creation Date: Date 01/01/2021
One Liner: Procedure generated by notebook notebooks/dtp/nb/any/gen/main_na_sqldb_sqdw 
  to load from External parquet files to Synapse Staging tables  
************************************************************************
Parameters
@BatchId : Batch_Id this task is part of, INT
@BatchTaskId : Batch_Task_Id that uniquely identifies the staging load task for a staging table, INT

Sample
======
-- EXEC [Staging].[usp_merge_ext_XXXXXXX] @batchId = 1, @batchTaskId = 1
Change History
DATE		CHANGED BY			DESCRIPTION
30-08-2022	Manmohan			Initial version after rewriting MERGE with UP-SERT steps
XX-09-2022  Manmohan            Corrected logging to [audit].[usp_log_error] for logging errors
13-10-2022  Christian Bracchi   Added Additional join on sourceID from src (ext) to target (staging)

*/
SET NOCOUNT ON
DECLARE @transtate BIT
BEGIN
IF @@TRANCOUNT = 0 
BEGIN
    SET @transtate = 1
    BEGIN TRANSACTION 
END

BEGIN TRY 
    DECLARE @CurrentTimeStamp DATETIME2 = GETDATE()
    , @log_name VARCHAR(100)= '{spObjectName}'

	--log progress
	EXEC audit.usp_log_progress @batchTaskId, @log_name, 'Store Proc start'

    --Update Matching records
    UPDATE tgt 
       SET tgt.lakeValidToTimestamp = src.lakeValidToTimestamp, tgt.lakeIsActive = src.lakeIsActive, tgt.lakeLastUpdateDate = src.lakeLastUpdateDate, tgt.lakeLastUpdateTimestamp = src.lakeLastUpdateTimestamp, tgt.lakeLastUpdatedBatchID = src.lakeLastUpdatedBatchID, tgt.dwhLastUpdateTimestamp = @CurrentTimeStamp, tgt.dwhLastUpdatedBatchID = @batchId
                   """)

        #Update lakeDeletedTimestamp if applicable 
        if 'lakeDeletedTimestamp' in targetAttributes:
            upsertSpScript = upsertSpScript + ", tgt.lakeDeletedTimestamp = src.lakeDeletedTimestamp"

        upsertSpScript = upsertSpScript + (
            f""" 
      FROM {externalObjectName} src
     INNER 
      JOIN {objectName} tgt
        ON tgt.HashedBusinesskey      = src.HashedBusinesskey 
       AND tgt.lakeValidFromTimestamp = src.lakeValidFromTimestamp
       AND tgt.sourceID               = src.sourceID --CB: addition of join on sourceID
	OPTION (LABEL = '{spObjectName}|Updates')

	--log progress
	EXEC audit.usp_log_progress @batchTaskId, @log_name, 'Updates Completed'
    
    --Insert non-existing records
    INSERT 
      INTO {objectName}
          ({targetAttributes})
    SELECT {sourceAttributes}
      FROM {externalObjectName} AS src
     WHERE 
       NOT 
    EXISTS (SELECT NULL 
              FROM {objectName} 
             WHERE HashedBusinesskey      = src.HashedBusinesskey 
               AND lakeValidFromTimestamp = src.lakeValidFromTimestamp
               AND sourceID               = src.sourceID --CB: addition of join on sourceID
            )
	OPTION (LABEL = '{spObjectName}|Inserts')
     
	--log progress
	EXEC audit.usp_log_progress @batchTaskId, @log_name, 'Inserts Completed'

	--update tbl_object_availability
     EXEC audit.usp_populate_staging_dates_loaded @batchTaskId, '{objectName}', '{externalObjectName}', @CurrentTimeStamp 

    IF @transtate = 1
      COMMIT TRANSACTION

	--log progress
	EXEC audit.usp_log_progress @batchTaskId, @log_name, 'Store Proc End'
    
END TRY

BEGIN CATCH
    DECLARE @Error_Message VARCHAR(5000) = Error_message()
    , @Error_Severity INT = Error_severity()
    , @Error_State INT = Error_state()

    IF @transtate = 1 AND Xact_state() <> 0
        ROLLBACK TRANSACTION
    
    --log error
    EXEC [audit].[usp_log_error] @batchTaskId, @log_name, @Error_Message  
    RAISERROR (@Error_Message,@Error_Severity,@Error_State)
END CATCH
END
GO

""")
        dbutils.fs.put(upsertSPFilePath, upsertSpScript, True)
#         print(upsertSpScript)
        logTaskProgress(cursor,batchTaskId,"Create proc script for {} generated successfully".format(spObjectName))
    except Exception as e:
        errorMessage="Unable to genrate create proc script: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False

# COMMAND ----------

# DBTITLE 1,Function to generate sql script synapse staging tables
def generateSynapseTableScript(objectName,schemaString,hashDistColumns,stagingTablePath,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:
        
        distributionString = "HASH({})".format(hashDistColumns)        
        createTableScript = "CREATE TABLE {}\n ({})\n WITH (DISTRIBUTION = {})".format(objectName,schemaString,distributionString)        
#         print(createTableScript)
        dbutils.fs.put(stagingTablePath, createTableScript, True)
        logTaskProgress(cursor,batchTaskId,"Create table script for {} generated successfully".format(objectName))
    except Exception as e:
        errorMessage="Unable to genrate create table script: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False

# COMMAND ----------

# DBTITLE 1,Function to generate sql scripts for synapse external tables
def generateSynapseExternalTable(objectName,schemaString,dataSource,location,fileFormat,externalTableFilePath,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:
#         print(f"GO \nDROP EXTERNAL TABLE {objectName}\nGO \n")
        createExtTableScript = """CREATE EXTERNAL TABLE {}\n ({})\n WITH (DATA_SOURCE = {}, LOCATION = '{}', FILE_FORMAT = {} )""".format(objectName,schemaString,dataSource,location,fileFormat) 
#         print(createExtTableScript)
        dbutils.fs.put(externalTableFilePath, createExtTableScript, True)
        logTaskProgress(cursor,batchTaskId,"Create external table script for {} generated successfully".format(objectName))
    except Exception as e:
        errorMessage="Unable to genrate create table script: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False

# COMMAND ----------

# DBTITLE 1,Function wrapper to generate synapse scripts
def generateSynapseScripts(tableDetailsDf,scriptsBasePath,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
    try:
        for tableDetail in tableDetailsDf.rdd.toLocalIterator():
            objectName = tableDetail["object_name"]
            schemaName = tableDetail["schema_name"]
            dataModelGroup = tableDetail["data_model_group"]
            #line break added after each column
            #added NOT NULL constraints to HBK and lakeValidFromTimestamp
            schemaString = (tableDetail["schema_string"].replace("HashedBusinessKey BIGINT,","HashedBusinessKey BIGINT NOT NULL,")
                                                        .replace("lakeValidFromTimestamp DATETIME2(7),","lakeValidFromTimestamp DATETIME2(7) NOT NULL,")
                                                        .replace(', ','\n,')+', dwhCreatedTimestamp DATETIME2, dwhLastUpdateTimestamp DATETIME2, dwhCreatedBatchID INT, dwhLastUpdatedBatchID INT'
                           )
            hashDistColumns = "HashedBusinessKey"
            tableName = tableDetail["table_name"]
            targetAttributes = tableDetail["object_attributes"]+', dwhCreatedTimestamp, dwhLastUpdateTimestamp, dwhCreatedBatchID, dwhLastUpdatedBatchID'
            hashDistbType = "Replicated"
            #external table should have a prefix ext_
            externalTableName = "ext_"+tableName
            externalObjectName = objectName.replace(".", ".Ext_")
            externalTableLocation = objectName.replace(".","_")
            externalTableDir = '/mnt/extsynapse/' + externalTableLocation
            #merge stored procedure has usp_merge_ext
            spName = "usp_merge_ext_"+tableName
            spObjectName = objectName.replace(".", ".usp_merge_ext_")
            #to remove dwh specific fields from external table
            externalTblSchemaString = (schemaString.replace(', dwhCreatedTimestamp DATETIME2','')
                                                   .replace(', dwhLastUpdateTimestamp DATETIME2','')
                                                   .replace(', dwhCreatedBatchID INT','')
                                                   .replace(', dwhLastUpdatedBatchID INT','')
                                                   ) 

            stagingTableFilePath= scriptsBasePath+"/"+schemaName+"/StagingTables/"+tableName+".sql"
            externalTableFilePath = scriptsBasePath+"/"+schemaName+"/ExternalTables/"+externalTableName+".sql"
            mergeSPFilePath = scriptsBasePath+"/"+schemaName+"/MergeSPs/"+spName+".sql"

            #add alias src
            sourceAttributes = 'src.' + targetAttributes.replace(', ', ', src.')

            #to populate dwh specific audit columns
            sourceAttributes = (sourceAttributes.replace(', src.dwhCreatedTimestamp', ', @CurrentTimeStamp')
                                                .replace(', src.dwhLastUpdateTimestamp', ', @CurrentTimeStamp')
                                                .replace(', src.dwhCreatedBatchID', ', @batchId')
                                                .replace(', src.dwhLastUpdatedBatchID', ', @batchId'))
            
            #create external table directory
            if not dirCheck(externalTableDir):
              dbutils.fs.mkdirs(externalTableDir)

            generateSynapseTableScript(objectName,schemaString,hashDistColumns,stagingTableFilePath,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
            generateSynapseExternalTable(externalObjectName, externalTblSchemaString, dataSource, externalTableLocation, fileFormat, externalTableFilePath, cursor, batchTaskId, errorMessage, adfPipelineName, clusterId, notebookName, errorLogFileLocation)
            
            #Upsert statements for PhaseA Staging procs, Merge statements for existing Bdx
#             if dataModelGroup.lower() =='staging':
            generateSynapseUpsertSPs(spObjectName
                                            ,objectName
                                            ,externalObjectName
                                            ,targetAttributes
                                            ,sourceAttributes
                                            ,mergeSPFilePath
                                            ,cursor
                                            ,batchTaskId
                                            ,errorMessage
                                            ,adfPipelineName
                                            ,clusterId
                                            ,notebookName
                                            ,errorLogFileLocation)
#             else :
#                 generateSynapseMergeSPs(spObjectName,objectName,externalObjectName,targetAttributes,sourceAttributes,mergeSPFilePath,cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        logTaskProgress(cursor,batchTaskId,"Successfully generated sql scripts")
    except Exception as e:
      errorMessage="Exception occurred while generating sql scripts : " + str(e)
      logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
      assert False 