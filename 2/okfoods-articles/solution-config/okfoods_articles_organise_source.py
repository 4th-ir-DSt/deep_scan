app_name = "okfoods_articles_organise_source"
##
###################################################
##  Author      : Waheeb Agherdien
##  Created     : 10 March 2021
##  Description : Load Text File
##
##-------------------------------------------------
##  Ammended by :
##  Date        :
##  Reason      :
###################################################
##
from pyspark.sql.functions import input_file_name
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, DateType, ShortType
from pyspark.sql import functions as sf
import argparse
import traceback

## Initialize:
spark = SparkSession.builder.appName(app_name).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
hiveContext= HiveContext(spark)

##  Manditory Logging Functions:
log_data = []
log_types = {
    'debug': 'DEBUG',
    'warn': 'WARN',
    'info': 'INFO',
    'success': 'SUCCESS',
    'error': 'ERROR'
}


def get_current_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def do_log (dl_appname, dl_logtype, dl_message):
### REMOVE
###     log_data.append([get_current_datetime(), dl_appname, dl_logtype, '{0} {1}'.format(dl_appname, dl_message)])
    print ('###Talendlog###', dl_logtype, get_current_datetime(), '{0} {1}'.format(dl_appname, dl_message))

def get_input():
    log_data.append([get_current_datetime(), app_name, log_types.get('info'), 'get required inputs to copy invoice header and line detail.'])
    parser = argparse.ArgumentParser(description='Copy source files from landing to source.')
    parser.add_argument(
        '--run_id', required=True, help=("Run id")
    )
    parser.add_argument(
        '--assumed_transaction_date', required=True, help=("Assumed transaction date")
    )
    parser.add_argument(
        '--workbucket', required=True, help=("Inventory working location")
    )
    parser.add_argument(
        '--filekey', required=True, help=("Assumed transaction date")
    )
    parser.add_argument(
        '--logbucket', required=True, help=("Inventory working location")
    )
    parser.add_argument(
        '--sourcebucket', required=True, help=("Inventory working location")
    )
    args = parser.parse_args()
    return args.run_id, args.assumed_transaction_date, args.workbucket, args.filekey, args.logbucket, args.sourcebucket

##  User Functions:

## MAIN Code
do_log(app_name, log_types.get('info'), 'Started')

try:
##  Schema
    okfdSchema = StructType([
                StructField("lookup_code", StringType(), True),
                StructField("description", StringType(), True),
                StructField("variant_code", StringType(), True),
                StructField("base_size_lookup_code", StringType(), True),
                StructField("inventory_sku_code", StringType(), True),
                StructField("department_code", StringType(), True),
                StructField("department_name", StringType(), True),
                StructField("manufacturer", StringType(), True),
                StructField("brand_name", StringType(), True),
                StructField("discontinued", StringType(), True),
                StructField("date_added", StringType(), True),
                StructField("date_deleted", StringType(), True),
                StructField("unit_of_measure", StringType(), True),
                StructField("unit_size", StringType(), True),
                StructField("house_brand", StringType(), True),
                StructField("height", StringType(), True),
                StructField("width", StringType(), True),
                StructField("depth", StringType(), True),
                StructField("weight", StringType(), True),
                StructField("pack_size", StringType(), True),
                StructField("article_code", StringType(), True)
       ])

## set veriables from input args ---
    run_id, assumed_transaction_date, workbucket, filekey, logbucket, sourcebucket = get_input()

## using input args to create file path ---
    inputfile = 's3://{0}/{1}{2}/{3}/'.format(workbucket, filekey, run_id, assumed_transaction_date)
    outputfile = 's3://{0}/{1}{2}/{3}/'.format(sourcebucket, filekey, run_id, assumed_transaction_date)

    okfd_df = spark.read.format('com.databricks.spark.csv').option('header','true').option('delimiter', '|').schema(okfdSchema).load(inputfile)

    do_log(app_name, log_types.get('info'), 'Add File name Column')
    okfd_df = okfd_df.withColumn("filename", input_file_name())

    do_log(app_name, log_types.get('info'), 'READ = s3://' + workbucket + "/" + filekey + run_id + "/" + assumed_transaction_date)
    okfd_df.coalesce(1).write.mode("overwrite").format("parquet").option("compression", "snappy").save(outputfile)
    do_log(app_name, log_types.get('info'), 'WRITE = s3://' + sourcebucket + "/" + filekey + run_id + "/" + assumed_transaction_date)

## save the log
    do_log(app_name, log_types.get('info'), 'Completed Successfull')


    solution = "okfoods_articles"
    outputfileland = 's3://{0}/{1}{2}/'.format(sourcebucket, filekey, run_id )

    hiveContext.sql("""DROP TABLE IF EXISTS landing. """ + solution  )

    hiveContext.sql("""
            CREATE EXTERNAL TABLE IF NOT EXISTS landing. """ + solution  +  """ (
                lookup_code String,
                description String,
                variant_code String,
                base_size_lookup_code String,
                inventory_sku_code String,
                department_code String,
                department_name String,
                manufacturer String,
                brand_name String,
                discontinued String,
                date_added String,
                date_deleted String,
                unit_of_measure String,
                unit_size String,
                house_brand String,
                height String,
                width String,
                depth String,
                weight String,
                pack_size String,
                article_code String,
                filename String )
            ROW FORMAT SERDE
              'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            STORED AS INPUTFORMAT
              'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
            OUTPUTFORMAT
              'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            LOCATION '""" + outputfileland + """'""")

except:
    errorout = traceback.format_exc()
    raise RuntimeError('###FATALERROR###', errorout)
