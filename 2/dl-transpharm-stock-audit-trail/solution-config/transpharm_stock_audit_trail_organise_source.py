app_name = "transpharm_stock_audit_trail_source"
##
###################################################
##  Author      : Vikas Singh
##  Created     : 13 July 2020
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

### REMOVE
### log_schema = StructType([
###     StructField("datetime", StringType(), True),
###     StructField("source", StringType(), True),
###     StructField("level", StringType(), True),
###     StructField("message", StringType(), True)
### ])

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
    transpharmstockaudittrailSchema = StructType([
StructField("sourceid", StringType(), True),
StructField("transactiontype", StringType(), True),
StructField("branchid", StringType(), True),
StructField("capturedate", StringType(), True),
StructField("itemid", StringType(), True),
StructField("description", StringType(), True),
StructField("accountid", StringType(), True),
StructField("supplierid", StringType(), True),
StructField("supplierinvoiceno", StringType(), True),
StructField("systemclaimno", StringType(), True),
StructField("suppliercreditnoteno", StringType(), True),
StructField("scriptno", StringType(), True),
StructField("cashdocketno", StringType(), True),
StructField("coadocketno", StringType(), True),
StructField("originalcoadocketno", StringType(), True),
StructField("coddocketno", StringType(), True),
StructField("ibtreferenceno", StringType(), True),
StructField("ibtbranchfrom", StringType(), True),
StructField("ibtbranchto", StringType(), True),
StructField("stockmovementreason", StringType(), True),
StructField("userid", StringType(), True),
StructField("quantity", StringType(), True),
StructField("quantitybonus", StringType(), True),
StructField("amountcostexcl", StringType(), True),
StructField("amountsellexcl", StringType(), True),
StructField("amountlogfeeexcl", StringType(), True),
StructField("amountswellallowanceexcl", StringType(), True),
StructField("amountwarehouseallowanceexcl", StringType(), True),
StructField("scheduleno", StringType(), True),
StructField("suppliertype", StringType(), True),
StructField("lossgain", StringType(), True),
StructField("stockmovementreasontype", StringType(), True),
StructField("stockmovementreasoncode", StringType(), True),
StructField("vatindicator", StringType(), True),
StructField("docgenerateddate", StringType(), True),
StructField("grvno", StringType(), True),
StructField("costincl", StringType(), True),
StructField("ibtstoreno", StringType(), True)
       ])

## set veriables from input args ---
    run_id, assumed_transaction_date, workbucket, filekey, logbucket, sourcebucket = get_input()

## using input args to create file path ---
    inputfile = 's3://{0}/{1}{2}/{3}/'.format(workbucket, filekey, run_id, assumed_transaction_date)
    outputfile = 's3://{0}/{1}{2}/{3}/'.format(sourcebucket, filekey, run_id, assumed_transaction_date)

    transpharmstockaudittrail_df = spark.read.format('com.databricks.spark.csv').option('delimiter', '|').option("header","true").schema(transpharmstockaudittrailSchema).load(inputfile)

    do_log(app_name, log_types.get('info'), 'Add File name Column')
    transpharmstockaudittrail_df = transpharmstockaudittrail_df.withColumn("filename", input_file_name())

    do_log(app_name, log_types.get('info'), 'READ = s3://' + workbucket + "/" + filekey + run_id + "/" + assumed_transaction_date)
    transpharmstockaudittrail_df.coalesce(1).write.mode("overwrite").format("parquet").option("compression", "snappy").save(outputfile)
    do_log(app_name, log_types.get('info'), 'WRITE = s3://' + sourcebucket + "/" + filekey + run_id + "/" + assumed_transaction_date)

## save the log
    do_log(app_name, log_types.get('info'), 'Completed Successfull')

### REMOVE
##    logprefix = 's3://{0}/{1}{2}/{3}/'.format(workbucket, filekey, run_id, assumed_transaction_date)
##    log_df = spark.createDataFrame(log_data, log_schema)
##    log_df.coalesce(1).write.format('json').mode('overwrite').save("s3://{0}/{1}{2}.log".format(logbucket, logprefix, get_current_datetime()))

    solution = "transpharm_stock_audit_trail"
    outputfileland = 's3://{0}/{1}{2}/'.format(sourcebucket, filekey, run_id )

    hiveContext.sql("""DROP TABLE IF EXISTS landing. """ + solution  )

    hiveContext.sql("""
            CREATE EXTERNAL TABLE IF NOT EXISTS landing. """ + solution  +  """ (
                SourceID String,
transactiontype String,
branchid String,
capturedate String,
itemid String,
description String,
accountid String,
supplierid String,
supplierinvoiceno String,
systemclaimno String,
suppliercreditnoteno String,
scriptno String,
cashdocketno String,
coadocketno String,
originalcoadocketno String,
coddocketno String,
ibtreferenceno String,
ibtbranchfrom String,
ibtbranchto String,
stockmovementreason String,
userid String,
quantity String,
quantitybonus String,
amountcostexcl String,
amountsellexcl String,
amountlogfeeexcl String,
amountswellallowanceexcl String,
amountwarehouseallowanceexcl String,
scheduleno String,
suppliertype String,
lossgain String,
stockmovementreasontype String,
stockmovementreasoncode String,
vatindicator String,
docgenerateddate String,
grvno String,
costincl String,
ibtstoreno String,
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
