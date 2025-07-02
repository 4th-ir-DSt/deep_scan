# Databricks notebook source
import pyodbc, json
import pandas as pd

dbconn = dbutils.secrets.get(scope = 'data-scope-01', key = 'sql-dbrks-connection-01')
conn = pyodbc.connect(dbconn, autocommit = True)
cursor = conn.cursor()

sqlQuery = """
SELECT myJson
  FROM (SELECT DISTINCT 
               ph.phase_name
             , ph.phase_priority
             , se.entity_name AS entity_name
             , ss.source_name AS source_name
             , de.entity_name AS destination_entity_name
             , ds.source_name AS destination_source_name
             , tt.task_type_name
             , tt.task_super_type_name
             , dn.databricks_notebook_name
             , dn.databricks_notebook_location
             , ta.task_priority
          FROM config.tbl_task                     ta
          JOIN (SELECT DISTINCT task_id
                  FROM config.vw_task_object_map
                 UNION --changing to union to ensure that we don't get duplicate tasks id's
                SELECT DISTINCT task_id
                  FROM config.tbl_task      ta
                  JOIN config.tbl_task_type tt
                    ON tt.task_type_id    = ta.task_type_id
                 WHERE tt.task_type_name IN ('LoadPreparation', 'GdprRectification', 'GdprRedaction', 'GdprRetentionSource')) tob
            ON ta.task_id                         = tob.task_id
          JOIN config.tbl_phase                     ph
            ON ph.phase_id                        = ta.phase_id
          JOIN config.tbl_entity                    se
            ON se.entity_id                       = ta.entity_id
          JOIN config.tbl_source                    ss
            ON ss.source_id                       = se.source_id
          LEFT
          JOIN config.tbl_entity                    de
            ON de.entity_id                       = ta.destination_entity_id
          LEFT
          JOIN config.tbl_source                    ds
            ON ds.source_id                       = de.source_id
          JOIN config.tbl_task_type                 tt
            ON tt.task_type_id                    = ta.task_type_id
          JOIN config.tbl_databricks_notebook       dn
            ON dn.databricks_notebook_id          = ta.databricks_notebook_id
          FOR JSON PATH) x (myJson)"""

sqlQueryMapping = """SELECT myJson
  FROM (SELECT dob.object_name as destination_object_name
     , dde.object_attribute_name as destination_object_attribute_name
     , sob.object_name AS source_object_name
     , sde.object_attribute_name as source_object_attribute_name
     , REPLACE(ma.destination_object_attribute_default_value, '''', '!') AS destination_object_attribute_default_value
     , ty.object_attribute_computed_type
  FROM config.tbl_object                         dob
  JOIN config.tbl_object_definition              dde
    ON dde.object_id                           = dob.object_id
  JOIN config.tbl_object_to_object_map           ma
    ON ma.destination_object_definition_id     = dde.object_definition_id
  LEFT
  JOIN config.tbl_object_definition              sde
    ON sde.object_definition_id                = ma.source_object_definition_id
  LEFT
  JOIN config.tbl_object                         sob
    ON sob.object_id                           = sde.object_id
   AND sob.is_active                           = 1
  LEFT
  JOIN config.tbl_object_attribute_computed_type ty
    ON ty.object_attribute_computed_type_id    = ma.destination_object_attribute_computed_type_id
 WHERE dob.is_active                                    = 1

  AND ((sob.object_id IS NULL AND ma.destination_object_attribute_computed_type_id !=1) --To inlclude variable and expression types 
       OR  
       (sob.object_id IS NOT NULL)--To inlclude source definitions  
      )
      FOR JSON PATH) x (myJson)"""

sqlQueryObjectDefinition = """
SELECT myJson
  FROM (select Object.object_name
             , Object.object_type
             , attributeProperties.object_attribute_name
             , attributeProperties.object_attribute_type
             , Object.schema_string
             , Object.infile_datetime_column
             , attributeProperties.partition_order
             , attributeProperties.is_pii
             , attributeProperties.object_attribute_order
             , Object.partition_date_time_columns
             , Object.partition_scheme from config.tbl_object Object
          JOIN config.tbl_object_definition attributeProperties
            on Object.object_id=attributeProperties.object_id
         where Object.is_active=1  
          FOR JSON PATH) x (myJson)"""


# COMMAND ----------

# DBTITLE 1,Run on Environment that you expect to be correct - e.g. Dev\Test
#phase entity task
spOutput = pd.read_sql_query(sqlQuery,conn)

dfSpOutput = spark.createDataFrame(spOutput)

dfSpOutput.repartition(1).write.mode('overwrite').json('/mnt/temp/metadata/devTaskOutput.json')

#Attribute mappings
spOutput = pd.read_sql_query(sqlQueryMapping,conn)

dfSpOutput = spark.createDataFrame(spOutput)

dfSpOutput.repartition(1).write.mode('overwrite').json('/mnt/temp/metadata/devMappingOutput.json')

#object definition
spOutput = pd.read_sql_query(sqlQueryObjectDefinition,conn)

dfSpOutput = spark.createDataFrame(spOutput)

dfSpOutput.repartition(1).write.mode('overwrite').json('/mnt/temp/metadata/devObjectDefinitionOutput.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #Copy files from Dev/Test to the destination environment

# COMMAND ----------

# DBTITLE 1,Run this and below on destination environment after metadata deployment
jsonString = spark.read.json('/mnt/temp/metadata/devTaskOutput.json').collect()[0][0]

jsonMappingString = spark.read.json('/mnt/temp/metadata/devMappingOutput.json').collect()[0][0]

jsonObjectDefinitionString = spark.read.json('/mnt/temp/metadata/devObjectDefinitionOutput.json').collect()[0][0]

# COMMAND ----------

confirmationQuery = """DECLARE @p NVARCHAR(MAX) = '<jsonData>'

;WITH jsonData AS (SELECT *
  FROM OPENJSON(@p)
  WITH ( phase_name                   VARCHAR(255)
       , phase_priority               INT         
       , entity_name                  VARCHAR(255)  
       , source_name                  VARCHAR(255)  
       , destination_entity_name      VARCHAR(255)
       , destination_source_name      VARCHAR(255)
       , task_type_name               VARCHAR(255)
       , task_super_type_name         VARCHAR(255)
       , databricks_notebook_name     VARCHAR(255)
       , databricks_notebook_location VARCHAR(255)
       , task_priority                INT
       )
)
, currentData AS (SELECT DISTINCT 
               ph.phase_name
             , ph.phase_priority
             , se.entity_name AS entity_name
             , ss.source_name AS source_name
             , de.entity_name AS destination_entity_name
             , ds.source_name AS destination_source_name
             , tt.task_type_name
             , tt.task_super_type_name
             , dn.databricks_notebook_name
             , dn.databricks_notebook_location
             , ta.task_priority
          FROM config.tbl_task                     ta
          JOIN (SELECT DISTINCT task_id
                  FROM config.vw_task_object_map
                 UNION --changing to union to ensure that we don't get duplicate tasks id's
                SELECT DISTINCT task_id
                  FROM config.tbl_task      ta
                  JOIN config.tbl_task_type tt
                    ON tt.task_type_id    = ta.task_type_id
                 WHERE tt.task_type_name IN ('LoadPreparation', 'GdprRectification', 'GdprRedaction', 'GdprRetentionSource')) tob
            ON ta.task_id                         = tob.task_id
          JOIN config.tbl_phase                     ph
            ON ph.phase_id                        = ta.phase_id
          JOIN config.tbl_entity                    se
            ON se.entity_id                       = ta.entity_id
          JOIN config.tbl_source                    ss
            ON ss.source_id                       = se.source_id
          LEFT
          JOIN config.tbl_entity                    de
            ON de.entity_id                       = ta.destination_entity_id
          LEFT
          JOIN config.tbl_source                    ds
            ON ds.source_id                       = de.source_id
          JOIN config.tbl_task_type                 tt
            ON tt.task_type_id                    = ta.task_type_id
          JOIN config.tbl_databricks_notebook       dn
            ON dn.databricks_notebook_id          = ta.databricks_notebook_id) 
SELECT *
  FROM jsonData jd
  FULL OUTER
  -- LEFT
  JOIN currentData cd
    ON cd.phase_name                            = jd.phase_name                  
   AND cd.phase_priority                        = jd.phase_priority              
   AND cd.entity_name                           = jd.entity_name                 
   AND cd.source_name                           = jd.source_name                 
   AND COALESCE(cd.destination_entity_name, '') = COALESCE(jd.destination_entity_name, '')
   AND COALESCE(cd.destination_source_name, '') = COALESCE(jd.destination_source_name, '')
   AND cd.task_type_name                        = jd.task_type_name              
   AND cd.task_super_type_name                  = jd.task_super_type_name        
   AND cd.databricks_notebook_name              = jd.databricks_notebook_name    
   AND cd.databricks_notebook_location          = jd.databricks_notebook_location
   AND cd.task_priority                         = jd.task_priority
 WHERE cd.phase_name IS NULL 
    OR jd.phase_name IS NULL
 ORDER
    BY cd.phase_priority
     , cd.phase_name
     , cd.entity_name
     , cd.source_name
     , cd.task_type_name
     , cd.task_priority
     , jd.phase_priority
     , jd.phase_name
     , jd.entity_name
     , jd.source_name
     , jd.task_type_name
     , jd.task_priority"""

confirmationMappingQuery = """DECLARE @p NVARCHAR(MAX) = '<jsonData>'

;WITH jsonData AS (SELECT *
  FROM OPENJSON(@p)
  WITH ( destination_object_name                    VARCHAR(255)
       , destination_object_attribute_name          VARCHAR(255)      
       , source_object_name                         VARCHAR(255)  
       , source_object_attribute_name               VARCHAR(255)  
       , destination_object_attribute_default_value VARCHAR(1000)
       , object_attribute_computed_type             VARCHAR(255)
       )
)
, currentData AS (SELECT dob.object_name AS destination_object_name
     , dde.object_attribute_name         AS destination_object_attribute_name
     , sob.object_name                   AS source_object_name
     , sde.object_attribute_name         AS source_object_attribute_name
     , REPLACE(ma.destination_object_attribute_default_value, '''', '!') AS destination_object_attribute_default_value
     , ty.object_attribute_computed_type
  FROM config.tbl_object                         dob
  JOIN config.tbl_object_definition              dde
    ON dde.object_id                           = dob.object_id
  JOIN config.tbl_object_to_object_map           ma
    ON ma.destination_object_definition_id     = dde.object_definition_id
  LEFT
  JOIN config.tbl_object_definition              sde
    ON sde.object_definition_id                = ma.source_object_definition_id
  LEFT
  JOIN config.tbl_object                         sob
    ON sob.object_id                           = sde.object_id
   AND sob.is_active                           = 1
  LEFT
  JOIN config.tbl_object_attribute_computed_type ty
    ON ty.object_attribute_computed_type_id    = ma.destination_object_attribute_computed_type_id
 WHERE dob.is_active                                    = 1

  AND ((sob.object_id IS NULL AND ma.destination_object_attribute_computed_type_id !=1) --To inlclude variable and expression types 
       OR  
       (sob.object_id IS NOT NULL)--To inlclude source definitions  
      )) 
SELECT *
  FROM jsonData jd
  FULL OUTER
  -- LEFT
  JOIN currentData cd
    ON cd.destination_object_name                                  = jd.destination_object_name                  
   AND cd.destination_object_attribute_name                        = jd.destination_object_attribute_name                              
   AND COALESCE(cd.source_object_name, '')                         = COALESCE(jd.source_object_name, '')
   AND COALESCE(cd.source_object_attribute_name, '')               = COALESCE(jd.source_object_attribute_name, '')      
   AND COALESCE(cd.destination_object_attribute_default_value, '') = COALESCE(jd.destination_object_attribute_default_value, '')
   AND cd.object_attribute_computed_type                           = jd.object_attribute_computed_type              
 WHERE cd.destination_object_name IS NULL 
    OR jd.destination_object_name IS NULL
 ORDER
    BY cd.destination_object_name
     , cd.destination_object_attribute_name 
     , jd.destination_object_name
     , jd.destination_object_attribute_name"""

confirmationObjectDefinitionQuery = """DECLARE @p NVARCHAR(MAX) = '<jsonData>'

;WITH jsonData AS (SELECT DISTINCT *
  FROM OPENJSON(@p)
  WITH ( object_name                   VARCHAR(255)
       , object_type                  VARCHAR(255)        
       , object_attribute_name                  VARCHAR(255)  
       , object_attribute_type                VARCHAR(255)  
       , infile_datetime_column      VARCHAR(255)
       , partition_order             INT
       , is_pii          VARCHAR(255)
       , object_attribute_order     INT
       , partition_date_time_columns VARCHAR(255)
       , partition_scheme     VARCHAR(255)
       )
)
, currentData AS (select Object.object_name
,Object.object_type
,attributeProperties.object_attribute_name
,attributeProperties.object_attribute_type
,Object.infile_datetime_column
,attributeProperties.partition_order
,attributeProperties.is_pii
,attributeProperties.object_attribute_order
,Object.partition_date_time_columns
,Object.partition_scheme from config.tbl_object Object
JOIN config.tbl_object_definition attributeProperties
on Object.object_id=attributeProperties.object_id
where Object.is_active=1  ) 
SELECT *
  FROM jsonData jd
  FULL OUTER
  -- LEFT
  JOIN currentData cd
    ON cd.object_name                           = jd.object_name                 
   AND cd.object_type                       = jd.object_type             
   AND cd.object_attribute_name                          = jd.object_attribute_name               
   AND cd.object_attribute_type                          = jd.object_attribute_type                
   AND COALESCE( cd.infile_datetime_column,'') =COALESCE( jd.infile_datetime_column ,'')
   AND COALESCE(cd.partition_order  ,'' )                   = COALESCE(jd.partition_order       ,'' )       
   AND cd.is_pii                  = jd.is_pii       
   AND cd.object_attribute_order              = jd.object_attribute_order  
   AND COALESCE(cd.partition_date_time_columns, '' )           = COALESCE(jd.partition_date_time_columns,'' ) 
   AND COALESCE(cd.partition_scheme  ,   '' )                      = COALESCE(jd.partition_scheme,'' ) 
 WHERE cd.object_name IS NULL 
    OR jd.object_name IS NULL  
order by cd.object_name
       , cd.object_attribute_order 
       , jd.object_name
       , jd.object_attribute_order 
    """


# COMMAND ----------

pd.set_option("display.max_rows", None, "display.max_columns", None)

confirmationQuery = confirmationQuery.replace("<jsonData>", jsonString)

spOutputConfirmation = pd.read_sql_query(confirmationQuery,conn)

spOutputConfirmation

# COMMAND ----------

pd.set_option("display.max_rows", None, "display.max_columns", None)

confirmationMappingQuery = confirmationMappingQuery.replace("<jsonData>", jsonMappingString)

spOutputMappingConfirmation = pd.read_sql_query(confirmationMappingQuery,conn)

spOutputMappingConfirmation

# COMMAND ----------

pd.set_option("display.max_rows", None, "display.max_columns", None)

confirmationObjectDefinitionQuery = confirmationObjectDefinitionQuery.replace("<jsonData>", jsonObjectDefinitionString)

spOutputObjectDefinitionConfirmation = pd.read_sql_query(confirmationObjectDefinitionQuery,conn)

spOutputObjectDefinitionConfirmation