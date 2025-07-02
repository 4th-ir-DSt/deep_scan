app_name = "money_market_sales_organise_source"
##
###################################################
##  Author      : Waheeb Agherdien
##  Created     : 3 September 2022
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
import pyspark.sql.functions as sf
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

## set veriables from input args ---
    run_id, assumed_transaction_date, workbucket, filekey, logbucket, sourcebucket = get_input()

## using input args to create file path ---
    inputfile = 's3://{0}/{1}{2}/{3}/'.format(workbucket, filekey, run_id, assumed_transaction_date)
    outputfile = 's3://{0}/{1}{2}/{3}/'.format(sourcebucket, filekey, run_id, assumed_transaction_date)

    sourceSchema = StructType([
    StructField("date_from", StringType(), True),
    StructField("date_to", StringType(), True),
    StructField("product_description", StringType(), True),
    StructField("transaction_statuscode", StringType(), True),
    StructField("transaction_status_description", StringType(), True),
    StructField("time", StringType(), True),
    ])
    mm_source_df = spark.read.format('com.databricks.spark.csv').option('header', 'true').option('delimiter', '|').schema(sourceSchema).load(inputfile)

    do_log(app_name, log_types.get('info'), 'Add File name Column')
    mm_source_df = mm_source_df.withColumn("filename", input_file_name())

    do_log(app_name, log_types.get('info'), 'READ = s3://' + workbucket + "/" + filekey + run_id + "/" + assumed_transaction_date)
    mm_source_df.coalesce(1).write.mode("overwrite").format("parquet").option("compression", "snappy").save(outputfile)
    do_log(app_name, log_types.get('info'), 'WRITE = s3://' + sourcebucket + "/" + filekey + run_id + "/" + assumed_transaction_date)

## save the log
    do_log(app_name, log_types.get('info'), 'Completed Successfull')


    solution = "money_market_sales"
    outputfileland = 's3://{0}/{1}{2}/'.format(sourcebucket, filekey, run_id )

    hiveContext.sql("""DROP TABLE IF EXISTS landing. """ + solution  )

    hiveContext.sql("""
            CREATE EXTERNAL TABLE IF NOT EXISTS landing. """ + solution  +  """ (
                date_from String,
                date_to String,
                product_description String,
                transaction_statuscode String,
                transaction_status_description String,
                time String,
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
