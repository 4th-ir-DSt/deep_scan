# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_na_subscribe_eventhb_dlt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>notebook to Load data from eventhub to delta live tables </td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2022/11/16</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC ## Change Log
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Date</th>
# MAGIC     <th>Changed By</th>
# MAGIC     <th>Change Description</th>
# MAGIC   </tr>
# MAGIC           <td>03/11/2022 </td>
# MAGIC     <td>Rahul C </td>
# MAGIC     <td>Created the notebook</td></tr>
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

# DBTITLE 1,Import Libraries
import dlt
import json
from itertools import chain
from pyspark.sql.functions import from_json, explode, col, regexp_replace, create_map, lit, date_format, concat, udf, current_date, current_timestamp, expr, hour
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, TimestampType, IntegerType, MapType, BooleanType
from datetime import datetime,timedelta

# COMMAND ----------

# DBTITLE 1,Functions
# create a very simple json struct so we can parse the outer part of the message.  The inner art will be dealt with using map type so we can manipulate the column names to be lower case.
initial_json_schema = ArrayType(StringType(), True)

#define function to convert the keys of a dictionary to lower case. This will be used and transient for a map type stirng to string. We will not need to specify full struct
def dict_keys_to_lowercase(dictIn):
    return dict(map(lambda item: (item[0].lower(), item[1]), dictIn.items()))

#create the udf based on the above function.
dict_keys_to_lowercase_Udf = udf(dict_keys_to_lowercase, MapType(StringType(), StringType()))


# COMMAND ----------

# DBTITLE 1,Parameters
hub_to_use = '2' #1 is standard, 2 is premium
dlt_table = 'cdc_subscribe'

readConnectionString = dbutils.secrets.get(scope = "lza-da-kv-001-d", key = "lza-da-ehn-002-d-primary-connectionstring")
TOPIC ="lza-da-evh-001-d-subscribe-hvrcdctest01"

BOOTSTRAP_SERVERS = "lza-da-ehn-00{}-d.servicebus.windows.net:9093".format(hub_to_use)
EH_SASL = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{readConnectionString}";'

kafka_options = {
       "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
       "kafka.security.protocol":"SASL_SSL",
       "kafka.sasl.mechanism":"PLAIN",
       "kafka.request.timeout.ms": "60000",
       "kafka.session.timeout.ms": "30000",
       "startingOffsets": "earliest",
       "kafka.sasl.jaas.config": EH_SASL,
       "subscribe": TOPIC,
       "failOnDataLoss": "false",
  }

# COMMAND ----------

# DBTITLE 1,Consume Events and create DLT table
try:

    @dlt.table(
        name="{}".format(dlt_table),
        spark_conf={"pipelines.trigger.interval": "10 seconds"},
        comment="Dev testing dlt being fed from event hub stream",
        partition_cols=["table_name", "lakeCreatedDate", "lakeCreatedHour"]
    )
    def eventHubStreamDLT():
        return (
            spark.readStream.format("kafka")
            .options(**kafka_options)
            .load()
            .withColumn("partition", col("partition").cast("int"))
            .withColumn("offset", col("offset").cast("bigint"))
            .withColumn("date", date_format("timestamp", "yyyyMMdd").cast("int"))  #hvr
            .withColumn("exploded_message", explode(from_json(col("value").cast("string"), initial_json_schema)))
            .withColumn("MessageMapType",from_json(col("exploded_message"), MapType(StringType(), StringType())))
            .withColumn("structured_message_map", dict_keys_to_lowercase_Udf(col("MessageMapType")))
            .withColumn("table_name", col("structured_message_map.hvr_tbl_name"))
            .withColumn("op_code", col("structured_message_map.op_code").cast("int"))
            .withColumn("simple_op_code", col("op_code") % 10)
            .withColumn("refresh", col("structured_message_map.hvr_refresh").cast("boolean"))
            .withColumn("lakeCreatedDate",expr('CAST(date_format(current_date(),"yyyyMMdd") AS INT)'))
            .withColumn("lakeCreatedHour",expr('CAST(hour(current_timestamp()) AS INT)'))
            .withColumn("lakeCreatedTimeStamp",current_timestamp())
            .select(
                "partition",
                "offset",
                "date",
                "timestamp",
                "structured_message_map",
                "table_name",
                "op_code",
                "simple_op_code",
                "refresh",
                "lakeCreatedDate",
                "lakeCreatedHour",
                "lakeCreatedTimeStamp"
            )
        )

except Exception as e:
    errorMessage = "Unable to load data in delta live tables: " + str(e)
    logError(errorMessage, notebookName, errorLogFileLocation)
    assert False
