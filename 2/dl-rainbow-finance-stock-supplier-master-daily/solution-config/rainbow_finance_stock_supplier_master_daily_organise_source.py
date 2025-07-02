app_name = "rainbow_finance_stock_supplier_master_daily_organise_source"
##
###################################################
##  Author      : Adiel Baker
##  Created     : 31 May 2022"
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
    StructField("branch", StringType(), True),
    StructField("fin_proc_julian", StringType(), True),
    StructField("fin_proc", StringType(), True),
    StructField("date_julian", StringType(), True),
    StructField("date", StringType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("branch_description", StringType(), True),
    StructField("chain", StringType(), True),
    StructField("chain_description", StringType(), True),
    StructField("supplier_code", StringType(), True),
    StructField("supplier_branch_code", StringType(), True),
    StructField("supplier_name", StringType(), True),
    StructField("supplier_postal_address_line_1", StringType(), True),
    StructField("supplier_postal_address_line_2", StringType(), True),
    StructField("supplier_postal_address_line_3", StringType(), True),
    StructField("supplier_postal_address_line_4", StringType(), True),
    StructField("supplier_postal_address_postal_code", StringType(), True),
    StructField("supplier_delivery_address_line_1", StringType(), True),
    StructField("supplier_delivery_address_line_2", StringType(), True),
    StructField("supplier_delivery_address_line_3", StringType(), True),
    StructField("supplier_delivery_address_line_4", StringType(), True),
    StructField("supplier_delivery_address_postal_code", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("fax_number", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("ap_cross_reference_number", StringType(), True),
    StructField("creditor_type", StringType(), True),
    StructField("authorize_all_orders", StringType(), True),
    StructField("contact_person", StringType(), True),
    StructField("date_created", StringType(), True),
    StructField("date_phased_out", StringType(), True),
    StructField("date_deleted", StringType(), True),
    StructField("date_of_last_order", StringType(), True),
    StructField("edi_active", StringType(), True),
    StructField("edi_location", StringType(), True),
    StructField("vat_number", StringType(), True),
    StructField("date_amended", StringType(), True),
    StructField("amended_by", StringType(), True),
    StructField("purchases_ltd", StringType(), True),
    StructField("purchases_ytd", StringType(), True),
    StructField("purchases_mtd", StringType(), True),
    StructField("company_group", StringType(), True),
    StructField("store_number", StringType(), True),
    StructField("telex_number", StringType(), True),
    StructField("process_type", StringType(), True),
    StructField("factor_number", StringType(), True),
    StructField("advance_payment", StringType(), True),
    StructField("hold_indicator", StringType(), True),
    StructField("payment_hold_indicator", StringType(), True),
    StructField("trading_type", StringType(), True),
    StructField("eft_bank_branch", StringType(), True),
    StructField("eft_bank_account", StringType(), True),
    StructField("eft_account_type", StringType(), True),
    StructField("pay_date_override_indicator", StringType(), True),
    StructField("cross_reference_account", StringType(), True),
    StructField("old_supplier_code", StringType(), True),
    StructField("supplier_country", StringType(), True),
    StructField("supplier_currency", StringType(), True),
    StructField("direct_delivery", StringType(), True),
    ])
    rainbow_finance_source_df = spark.read.format('com.databricks.spark.csv').option('header', 'true').option('delimiter', '|').schema(sourceSchema).load(inputfile)

    do_log(app_name, log_types.get('info'), 'Add File name Column')
    rainbow_finance_source_df = rainbow_finance_source_df.withColumn("filename", input_file_name())

    do_log(app_name, log_types.get('info'), 'READ = s3://' + workbucket + "/" + filekey + run_id + "/" + assumed_transaction_date)
    rainbow_finance_source_df.coalesce(1).write.mode("overwrite").format("parquet").option("compression", "snappy").save(outputfile)
    do_log(app_name, log_types.get('info'), 'WRITE = s3://' + sourcebucket + "/" + filekey + run_id + "/" + assumed_transaction_date)

## save the log
    do_log(app_name, log_types.get('info'), 'Completed Successfull')


    solution = "rainbow_finance_stock_supplier_master_daily"
    outputfileland = 's3://{0}/{1}{2}/'.format(sourcebucket, filekey, run_id )

    hiveContext.sql("""DROP TABLE IF EXISTS landing. """ + solution  )

    hiveContext.sql("""
            CREATE EXTERNAL TABLE IF NOT EXISTS landing. """ + solution  +  """ (
                branch String,
                fin_proc_julian String,
                fin_proc String,
                date_julian String,
                date String,
                day_of_week String,
                branch_description String,
                chain String,
                chain_description String,
                supplier_code String,
                supplier_branch_code String,
                supplier_name String,
                supplier_postal_address_line_1 String,
                supplier_postal_address_line_2 String,
                supplier_postal_address_line_3 String,
                supplier_postal_address_line_4 String,
                supplier_postal_address_postal_code String,
                supplier_delivery_address_line_1 String,
                supplier_delivery_address_line_2 String,
                supplier_delivery_address_line_3 String,
                supplier_delivery_address_line_4 String,
                supplier_delivery_address_postal_code String,
                phone_number String,
                fax_number String,
                email_address String,
                ap_cross_reference_number String,
                creditor_type String,
                authorize_all_orders String,
                contact_person String,
                date_created String,
                date_phased_out String,
                date_deleted String,
                date_of_last_order String,
                edi_active String,
                edi_location String,
                vat_number String,
                date_amended String,
                amended_by String,
                purchases_ltd String,
                purchases_ytd String,
                purchases_mtd String,
                company_group String,
                store_number String,
                telex_number String,
                process_type String,
                factor_number String,
                advance_payment String,
                hold_indicator String,
                payment_hold_indicator String,
                trading_type String,
                eft_bank_branch String,
                eft_bank_account String,
                eft_account_type String,
                pay_date_override_indicator String,
                cross_reference_account String,
                old_supplier_code String,
                supplier_country String,
                supplier_currency String,
                direct_delivery String,
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
