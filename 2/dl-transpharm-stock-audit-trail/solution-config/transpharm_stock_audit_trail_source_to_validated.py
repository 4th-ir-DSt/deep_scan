app_name = "transpharm_stock_audit_trail_source_to_validated"
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

### REMOVE
### log_schema = StructType([
###     StructField("datetime", StringType(), True),
###    StructField("source", StringType(), True),
###     StructField("level", StringType(), True),
###     StructField("message", StringType(), True)
### ])

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
    se_source = spark.read.parquet(inputfile)

    do_log(app_name, log_types.get('info'), 'Start to Validate!')

## Log Source Row Count.
    rows = se_source.count()
    do_validate_log(solution, run_id, "-", validate_types.get('SCOUNT'), rows)

## Remove Duplicates
    do_log(app_name, log_types.get('info'), 'Remove Duplicates')
    se_source = se_source.dropDuplicates()
    rows = rows - se_source.count()
    do_validate_log(solution, run_id, "-", validate_types.get('DEDUP'), rows )

##  Add Run Info
    do_log(app_name, log_types.get('info'), 'Add runid to Data')
    se_source = se_source.withColumn("run_id", lit(run_id))
    se_source = se_source.withColumn("assumed_tran_date", lit(assumed_transaction_date))


## Log Row Count after validate.
    do_validate_log(solution , run_id, "-", validate_types.get('FCOUNT'), se_source.count())
    do_log(app_name, log_types.get('info'), 'End of Validation!')

    se_source.createOrReplaceTempView("se_source")

## Cater for Date if more thatn one days files are loaded
    file_count = spark.sql("""
                SELECT DISTINCT
                    filename AS dt
                FROM
                    se_source AS val
            """).count()

## s3://prod-ire-integration/CloudBI/Inbound/SAP/CARDNUMBER/SAPRetail.BarcodeDetails.BI.20190724181758.txt.gz
##  if more than one file date found use file date, but add one day
    date_key = """SUBSTRING(RIGHT(regexp_replace(filename,'.txt',''),13), 0, 8)"""
## New Dataframe file date_key AKA load_date
## CAST(CAST(DATE_ADD(from_unixtime(unix_timestamp(CaptureDate, 'dd/MM/yyyy'), 'yyyy-MM-dd HH:mm:ss'), 1) AS timestamp) AS string) AS CaptureDate,
    se_source = spark.sql("""
                    SELECT
                    CAST(sourceid AS string),
                    CAST(transactiontype AS string),
                    CAST(branchid AS string),
                    CAST(capturedate as string),
                    CAST(itemid AS string),
                    CAST(description AS string),
                    CAST(accountid AS string),
                    CAST(supplierid AS string),
                    CAST(supplierinvoiceno AS string),
                    CAST(systemclaimno AS string),
                    CAST(suppliercreditnoteno AS string),
                    CAST(scriptno AS string),
                    CAST(cashdocketno AS string),
                    CAST(coadocketno AS string),
                    CAST(originalcoadocketno AS string),
                    CAST(coddocketno AS string),
                    CAST(ibtreferenceno AS string),
                    CAST(ibtbranchfrom AS string),
                    CAST(ibtbranchto AS string),
                    CAST(stockmovementreason AS string),
                    CAST(userid AS string),
                    CAST(quantity AS string),
                    CAST(quantitybonus AS string),
                    CAST(amountcostexcl AS string),
                    CAST(amountsellexcl AS string),
                    CAST(amountlogfeeexcl AS string),
                    CAST(amountswellallowanceexcl AS string),
                    CAST(amountwarehouseallowanceexcl AS string),
                    CAST(scheduleno AS string),
                    CAST(suppliertype AS string),
                    CAST(lossgain AS string),
                    CAST(stockmovementreasontype AS string),
                    CAST(stockmovementreasoncode AS string),
                    CAST(vatindicator AS string),
                    CAST(docgenerateddate AS string),
                    CAST(grvno AS string),
                    CAST(costincl AS string),
                    CAST(ibtstoreno AS string),
                    CAST(run_id AS string),
                    CAST(assumed_tran_date AS string),
                    CAST(""" + date_key + """ AS string) AS date_key
                    FROM
                    se_source
            """)

## Write Validated Source Data.
    se_source.coalesce(1).write.mode("append").format("parquet").option("compression", "snappy").partitionBy("run_id",          "assumed_tran_date").save(outputfile)
    do_log(app_name, log_types.get('info'), 'Done!')

## Create table on data
    hiveContext.sql(" drop table if exists my_table ")
    do_log(app_name, log_types.get('info'), 'Create External Table')
    hiveContext.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS validated. """ + solution  +  """ (
        sourceid string,
        transactiontype string,
        branchid string,
        capturedate string,
        itemid string,
        description string,
        accountid string,
        supplierid string,
        supplierinvoiceno string,
        systemclaimno string,
        suppliercreditnoteno string,
        scriptno string,
        cashdocketno string,
        coadocketno string,
        originalcoadocketno string,
        coddocketno string,
        ibtreferenceno string,
        ibtbranchfrom string,
        ibtbranchto string,
        stockmovementreason string,
        userid string,
        quantity string,
        quantitybonus string,
        amountcostexcl string,
        amountsellexcl string,
        amountlogfeeexcl string,
        amountswellallowanceexcl string,
        amountwarehouseallowanceexcl string,
        scheduleno string,
        suppliertype string,
        lossgain string,
        stockmovementreasontype string,
        stockmovementreasoncode string,
        vatindicator string,
        docgenerateddate string,
        grvno string,
        costincl string,
        ibtstoreno string,
        date_key string
    )
    PARTITIONED BY ( run_id  string, assumed_tran_date string )
    STORED AS PARQUET
    LOCATION '""" + outputfile + """'
    tblproperties ("parquet.compress"="SNAPPY") """)

    hiveContext.sql("""MSCK REPAIR TABLE validated.""" + solution )

### Write Validation Data Quality Log File.
    vlogfile = "s3://" + logbucket + "/DQ/" + solution + "/" + run_id + "/"
    val_df = spark.createDataFrame(val_data, data_validate_schema)
    val_df.coalesce(1).write.mode("overwrite").format("parquet").option("compression", "snappy").save(vlogfile)

### REMOVE
##    log_df = spark.createDataFrame(log_data, log_schema)
##    log_df.coalesce(1).write.format('json').mode('overwrite').save("s3://{0}/{1}{2}.log".format(logbucket, logprefix, get_current_datetime()))

except:
    errorout = traceback.format_exc()
    raise RuntimeError('###FATALERROR###', errorout)
