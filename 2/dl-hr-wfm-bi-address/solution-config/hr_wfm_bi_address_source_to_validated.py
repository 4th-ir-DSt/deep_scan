app_name = "hr_wfm_bi_address_source_to_validated"
##
###################################################
##  Author      : Olatunde Ola
##  Created     : 30 June 2021
##  Description : Load Text File
##
##-------------------------------------------------
##  Ammended by :
##  Date        :
##  Reason      :
###################################################
##
from datetime import datetime
from pyspark import SparkContext, SparkConf, HiveContext, SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import argparse
import traceback

spark = SparkSession.builder.appName(app_name).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
hiveContext = HiveContext(spark)

##  Manditory Logging Functions
log_data = []
val_data = []
log_types = {
    'debug': 'DEBUG',
    'warn': 'WARN',
    'info': 'INFO',
    'success': 'SUCCESS',
    'error': 'ERROR'
}
validate_types = {
    'SCOUNT' : 'sourcecount',
    'FCOUNT' : 'validatedcount',
    'DEDUP'  : 'duplicates',
    'NONUM'  : 'notnumber',
    'NODATE' : 'notdate',
    'NOVAL'  : 'novalue'
}

data_validate_schema = StructType([
    StructField("runtime", StringType(), True),
    StructField("runid", StringType(), True),
    StructField("solution_name", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("rule", StringType(), True),
    StructField("effected_rows", StringType(), True)
])

def get_input():
    """Do arg parsing. Return generator for input lines."""
    parser = argparse.ArgumentParser(description='Copy source files from landing to validated.')
    parser.add_argument(
        '--run_id', required=True, help=("Run id")
    )
    parser.add_argument(
        '--assumed_transaction_date', required=True, help=("Assumed transaction date")
    )
    parser.add_argument(
        '--filekey', required=True, help=("source file s3 key")
    )
    parser.add_argument(
        '--logbucket', required=True, help=("log location")
    )
    parser.add_argument(
        '--sourcebucket', required=True, help=("source location")
    )
    parser.add_argument(
        '--validatebucket', required=True, help=("validate location")
    )
    parser.add_argument(
        '--solution', required=True, help=("solution name")
    )
    args = parser.parse_args()
    return args.run_id, args.assumed_transaction_date, args.filekey, args.logbucket, args.sourcebucket, args.validatebucket, args.solution

def get_current_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def do_log (dl_appname, dl_logtype, dl_message):
###  REMOVE
###    log_data.append([get_current_datetime(), dl_appname, dl_logtype, '{0} {1}'.format(dl_appname, dl_message)])
    print ('###Talendlog###', dl_logtype, get_current_datetime(), '{0} {1}'.format(dl_appname, dl_message))

def do_validate_log (dl_solution, dl_runid, dl_column, dl_rule, dl_rows):
    val_data.append([get_current_datetime(), dl_runid, dl_solution, dl_column, dl_rule, dl_rows ])

## MAIN Code
try:
    do_log(app_name, log_types.get('info'), 'Program Started!')

    run_id, assumed_transaction_date, filekey, logbucket, sourcebucket, validatebucket, solution = get_input()

    inputfile = 's3://' + sourcebucket + "/" + filekey + run_id + "/" + assumed_transaction_date + "/"
    outputfile = "s3://" + validatebucket + "/" + filekey
    logprefix = '/{0}{1}/{2}/{3}/'.format(filekey, app_name, run_id, assumed_transaction_date)
    vlogfile = "s3://" + logbucket + "/DQ/" + solution + "/" + run_id + "/"

    do_log(app_name, log_types.get('info'), 'Load Parquet File')
    df_source = spark.read.parquet(inputfile)

    do_log(app_name, log_types.get('info'), 'Start to Validate!')

## Log Source Row Count.
    rows = df_source.count()
    do_validate_log(solution, run_id, "-", validate_types.get('SCOUNT'), rows)

## Remove Duplicates
    do_log(app_name, log_types.get('info'), 'Remove Duplicates')
    df_source = df_source.dropDuplicates()
    rows = rows - df_source.count()
    do_validate_log(solution, run_id, "-", validate_types.get('DEDUP'), rows )

##  Add Run Info
    do_log(app_name, log_types.get('info'), 'Add runid to Data')
    df_source = df_source.withColumn("run_id", lit(run_id))
    df_source = df_source.withColumn("assumed_tran_date", lit(assumed_transaction_date))


## Log Row Count after validate.
    do_validate_log(solution , run_id, "-", validate_types.get('FCOUNT'), df_source.count())
    do_log(app_name, log_types.get('info'), 'End of Validation!')

    df_source.createOrReplaceTempView("df_source")

## Cater for Date if more thatn one days files are loaded
    file_count = spark.sql("""
                SELECT DISTINCT
                    filename AS dt
                FROM
                    df_source AS val
            """).count()

    date_key = """regexp_replace(RIGHT(regexp_replace(regexp_replace(filename,'.csv',''),'.gz',''), 10), '-', '')"""

## New Dataframe field date_key AKA load_date
    df_source = spark.sql("""
                SELECT
					CAST(address_id AS string),
					CAST(address_line_1 AS string),
					CAST(address_line_2 AS string),
					CAST(city AS string),
					CAST(state_code AS string),
					CAST(country_code AS string),
					CAST(postal_code AS string),
					CAST(last_modified_user_id AS string),
					CAST(last_modified_timestamp AS string),
					CAST(country_id AS string),
                    CAST(run_id AS string),
                    CAST(assumed_tran_date AS string),
                    CAST(""" + date_key + """ AS string) AS date_key,
					CAST(substring_index(filename, "/", -1) AS string) AS origin_file_name
                FROM
                    df_source
            """)

## Write Validated Source Data.
    df_source.coalesce(1).write.mode("append").format("parquet").option("compression", "snappy").partitionBy("run_id","assumed_tran_date").save(outputfile)
    do_log(app_name, log_types.get('info'), 'Done!')

## Create table on data
    hiveContext.sql(" drop table if exists my_table ")
    do_log(app_name, log_types.get('info'), 'Create External Table')
    hiveContext.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS validated. """ + solution  +  """ (
		address_id String,
		address_line_1 String,
		address_line_2 String,
		city String,
		state_code String,
		country_code String,
		postal_code String,
		last_modified_user_id String,
		last_modified_timestamp String,
		country_id String,
        date_key String,
		origin_file_name String
    )
    PARTITIONED BY ( run_id  String, assumed_tran_date String )
    STORED AS PARQUET
    LOCATION '""" + outputfile + """'
    tblproperties ("parquet.compress"="SNAPPY") """)

    hiveContext.sql("""MSCK REPAIR TABLE validated.""" + solution )

### Write Validation Data Quality Log File.
    vlogfile = "s3://" + logbucket + "/DQ/" + solution + "/" + run_id + "/"
    val_df = spark.createDataFrame(val_data, data_validate_schema)
    val_df.coalesce(1).write.mode("overwrite").format("parquet").option("compression", "snappy").save(vlogfile)

except:
    errorout = traceback.format_exc()
    raise RuntimeError('###FATALERROR###', errorout)
