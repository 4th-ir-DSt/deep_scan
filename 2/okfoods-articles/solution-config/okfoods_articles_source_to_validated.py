app_name = "okfoods_articles_source_to_validated"
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
### REMOVE
###    slogfile = "s3://{0}/{1}{2}.log".format(logbucket, logprefix, get_current_datetime())
    vlogfile = "s3://" + logbucket + "/DQ/" + solution + "/" + run_id + "/"

    do_log(app_name, log_types.get('info'), 'Load Parquet File')
    okfd_source = spark.read.parquet(inputfile)

    do_log(app_name, log_types.get('info'), 'Start to Validate!')

## Log Source Row Count.
    rows = okfd_source.count()
    do_validate_log(solution, run_id, "-", validate_types.get('SCOUNT'), rows)

## Remove Duplicates

##  Add Run Info
    do_log(app_name, log_types.get('info'), 'Add runid to Data')
    okfd_source = okfd_source.withColumn("run_id", lit(run_id))
    okfd_source = okfd_source.withColumn("assumed_tran_date", lit(assumed_transaction_date))

## Check for Invalid Barcode##
### REMOVE
###    row = okfd_source.select( f.when(f.col("Barcode").cast("long").isNull(), 1).otherwise(0).alias("name")).where("name == 1").count()
###    do_validate_log(solution, run_id, "Barcode", validate_types.get('NONUM'), row )

## Log Row Count after validate.
    do_validate_log(solution , run_id, "-", validate_types.get('FCOUNT'), okfd_source.count())
    do_log(app_name, log_types.get('info'), 'End of Validation!')

    okfd_source.createOrReplaceTempView("okfd_source")

## Cater for Date if more thatn one days files are loaded
    file_count = spark.sql("""
                SELECT DISTINCT
                    filename AS dt
                FROM
                    okfd_source AS val
            """).count()

## s3://prod-ire-integration/CloudBI/Inbound/SAP/Barcode/SAPRetail.BarcodeDetails.BI.20190724181758.txt.gz
##  if more than one file date found use file date, but add one day
    date_key = """LEFT(RIGHT(regexp_replace(regexp_replace(filename,'.csv',''),'.gz',''),11),8)"""

## New Dataframe fiel date_key AKA load_date
    okfd_source = spark.sql("""
                SELECT
                    CAST(lookup_code AS string),
                    CAST(description AS string),
                    CAST(variant_code AS string),
                    CAST(base_size_lookup_code AS string),
                    CAST(inventory_sku_code AS string),
                    CAST(department_code AS string),
                    CAST(department_name AS string),
                    CAST(manufacturer AS string),
                    CAST(brand_name AS string),
                    CAST(discontinued AS string),
                    CAST(date_added AS string),
                    CAST(date_deleted AS string),
                    CAST(unit_of_measure AS string),
                    CAST(unit_size AS string),
                    CAST(house_brand AS string),
                    CAST(height AS string),
                    CAST(width AS string),
                    CAST(depth AS string),
                    CAST(weight AS string),
                    CAST(pack_size AS string),
                    CAST(article_code AS string),
                    CAST(run_id AS string),
                    CAST(assumed_tran_date AS string),
                    CAST(""" + date_key + """ AS string) AS date_key
                FROM
                    okfd_source
            """)

## Write Validated Source Data.
    okfd_source.coalesce(1).write.mode("append").format("parquet").option("compression", "snappy").partitionBy("run_id",          "assumed_tran_date").save(outputfile)
    do_log(app_name, log_types.get('info'), 'Done!')

## Create table on data
    hiveContext.sql(" drop table if exists my_table ")
    do_log(app_name, log_types.get('info'), 'Create External Table')
    hiveContext.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS validated. """ + solution  +  """ (
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
        date_key String
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
