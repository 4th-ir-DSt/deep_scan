
import sys
import subprocess
ENV = subprocess.check_output('echo $ENV', shell=True);
sys.path.append('/opt/ildbld/'+ENV.decode().strip()+'/spark_frmwrk/scripts/')
from pyspark.sql import SparkSession
import datetime
import os
import glob
import lib_logging as log
import lib_utility as util
from config_reader import ConfigFactory as cf
import time

def dim_extract_load(provider_name, retailer_name, retailer_dir_path, logger, spark):

    logger=log.get_logger("Shoprite_cn_extract")
    start_time = datetime.datetime.now()

    cf_obj = cf()
    config_obj = cf_obj.create_obj(True, logger)
    config_op_obj = config_obj.obj.parse(retailer_dir_path)


    cn_mstr_table                   = config_op_obj["cn_dim"]["CN_MSTR_TABLE"]
    cn_mstr_key_lookup              = config_op_obj["cn_dim"]["CN_MSTR_KEY_LOOKUP"]
    cn_extract_table                = config_op_obj["cn_dim"]["CN_EXTRACT_TABLE"]
    extract_path                    = config_op_obj["cn_dim"]["CN_EXTRACT_PATH"]
    spec_name                       = config_op_obj["cn_dim"]["CN_SPEC_NAME"]
    dimkey_extractfile_path         = config_op_obj["cn_dim"]["CN_RADIX_PATH"]
    cn_summry_table                 = config_op_obj["cn_dim"]["CN_SUMMRY_TABLE"]
    cn_seg_mstr                    = config_op_obj["cn_dim"]["CDNA_MSTR_TABLE"]

    logger.info("\n\nParameter details are")
    logger.info("==========================================================")
    logger.info("cn_mstr_table             : "+cn_mstr_table)
    logger.info("cn_mstr_key_lookup        : "+cn_mstr_key_lookup)
    logger.info("cn_extract_table          : "+cn_extract_table)
    logger.info("extract_path              : "+extract_path)
    logger.info("spec_name                 : "+spec_name)
    logger.info("dimkey_extractfile_path   : "+dimkey_extractfile_path)
    logger.info("cn_summry_table           : "+cn_summry_table)
    logger.info("cn_seg_mstr               :"+cn_seg_mstr)
    logger.info("==========================================================\n")


    # creating consumer exrtrac tables process
    logger.info("Creating consumer exrtract table process started at {}".format(datetime.datetime.now().strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
    logger.info("============================================================================")

    try:
      sql_to_run=("drop table if exists {} purge".format(cn_extract_table))
      print('')
      print(sql_to_run)
      print('')
      spark.sql(sql_to_run)
      print('')

    except Exception as ex:
        logger.info("Error while dropping consumer exrtract table")
        logger.error('\n************************ ERROR ENCOUNTERED in %s ************************ os_error_exit')
        logger.error(str(ex))
        logger.error('********************************* PROCESS TERMINATED ************************************')
        raise Exception("Error while consumer exrtract table")

    try:
       print('')
       print('Creating consumer exrtract table: {}'.format(cn_extract_table))
       print("==================================================")
       print('')


       sql_to_run=("create table {0} as select distinct a.*,pmod(row_number() over (order by householdmemberidentifier),2000) as cn_hash_id  from ( select distinct  mkl.alt_key as householdmemberidentifier,nvl(b.personalidentificationnumberprefix,'') as personalidentificationnumberprefix,nvl(b.birthday,'') as birthday,nvl(b.email,'') as email,nvl(b.has_email,'') as has_email,nvl(b.mobilenumberprefix,'') as mobilenumberprefix,nvl(b.current_age,'') as current_age,nvl(b.age_range,'') as age_range,nvl(b.gender,'') as gender,case when ( mkl.alt_key not like '%_-SHOPRITE_%') or  (mkl.alt_key!=0)   then 'Y' else 'N' end as IS_MEMBER, case when householdmemberidentifier like '%_-SHOPRITE_%' then '' when a.activity is null or a.activity='Other' then 'Dormant' else a.activity end activity ,case when (b.householdmemberidentifier  is null and  b.retailbrand is null)  then 'Y' else 'N' end as IS_ORPHAN, CASE WHEN LENGTH(regexp_replace(mkl.alt_key,'[^_]',''))=3 THEN 'Y' ELSE 'N' END IS_MANAGER,rank() over (partition by mkl.alt_key order  by b.retailbrand)as rank  from {1} mkl  left outer join {2} b on mkl.alt_key = b.householdmemberidentifier  left outer join {3}  a on mkl.alt_key=a.consumer_alt_key_txt  where mkl.dimension='consumer' )  a where rank=1 ".format(cn_extract_table,cn_mstr_key_lookup,cn_mstr_table,cn_summry_table))
       print('')
       print(sql_to_run)
       print('')
       spark.sql(sql_to_run)
       print('\n consumer extract  {} table sucessfully created at {}\n'.format(cn_extract_table,datetime.datetime.now().strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
       print("==================================================")
       print('')
    except Exception as ex:
        logger.info("Error while creating consumer exrtract table")
        logger.error('\n************************ ERROR ENCOUNTERED in %s ************************ os_error_exit')
        logger.error(str(ex))
        logger.error('********************************* PROCESS TERMINATED ************************************')
        raise Exception("Error while creating consumer exrtract table")


    try:
       print('')
       print('Extracting data from {}'.format(cn_extract_table))
       print("==================================================")
       print('')
       datfilename=str(spec_name)+'.dat'
       print('data file name is {}\n'.format(datfilename))
       extractfile=str(extract_path)+'/'+str(datfilename)
       print('data file name with path is {}\n'.format(extractfile))
       print('')
       sql_ext=("select distinct householdmemberidentifier,personalidentificationnumberprefix,birthday,email,mobilenumberprefix,current_age,age_range,gender,cn_hash_id,is_member,activity,has_email  from {}".format(cn_extract_table))
       print('Command is :'+sql_ext)
       ext_df=spark.sql(sql_ext)
       ext_df.coalesce(1).write.format('csv').mode("overwrite").option("delimiter","|").option("emptyValue", None).option("nullValue",None).save("/tmp/"+spec_name, header='false')

       if os.path.exists("{}".format(extract_path)):
         if os.path.exists("{}".format(extractfile)):
           os.remove("{}".format(extractfile))
         else:
           print('extract file cleaned before creating file\n')
       else:
          hadoop_create_dir = "hadoop fs -mkdir -p " + extract_path
          print ("Command is : %s " %(hadoop_create_dir))
          print('')
          if (os.system(hadoop_create_dir) != 0):
            logger.error("Creation of the folder %s failed" %(extract_path))
            raise Exception("Creation of the folder %s failed" %(extract_path))
          else :
            print('')
            print('Input directory  {} Sucessfully Created '.format(extract_path))


       os.system('hadoop fs -copyToLocal /tmp/'+spec_name+'/*csv  {}'.format(extractfile))
       print('{} file  sucessfully extracted into {} path\n'.format(datfilename,extract_path))

       print('meta file extract process started ')
       metafilename=str(spec_name)+'.meta'
       print('data file name is {}\n'.format(metafilename))
       meta_extractfile=str(extract_path)+'/'+str(metafilename)
       print('data file name with path is {}\n'.format(meta_extractfile))



       meta_ext="householdmemberidentifier|VARCHAR(150)|1|householdmemberidentifier|1|0|1|22|295|1|Y|1\npersonalidentificationnumberprefix|VARCHAR(150)|2|personalidentificationnumberprefix|1|0|1|22|295|1|N|\nbirthday|VARCHAR(150)|3|birthday|1|0|1|22|295|1|N|\nemail|VARCHAR(150)|4|email|1|0|1|22|295|1|N|\nmobilenumberprefix|VARCHAR(150)|5|mobilenumberprefix|1|0|1|22|295|1|N|\ncurrent_age|VARCHAR(150)|6|current_age|1|0|1|22|295|1|N|\nage_range|VARCHAR(150)|7|age_range|1|0|1|22|295|1|N|\ngender|VARCHAR(150)|8|gender|1|0|1|22|295|1|N|\ncn_hash_id|VARCHAR(150)|9|cn_hash_id|1|0|1|22|295|1|N|\nis_member|VARCHAR(150)|10|is_member|1|0|1|22|295|1|N|\nactivity|VARCHAR(150)|11|activity|1|0|1|22|295|1|N|\nhas_email|VARCHAR(150)|12|has_email|1|0|1|22|295|1|N|"

       if os.path.exists(meta_extractfile):
          os.remove(meta_extractfile)
       else:
          print('extract file cleaned before creating file\n')


       fin = open(meta_extractfile, "wt")
       fin.write(meta_ext)
       fin.close()

       #logger.info('sleeping for 2 minutes')
       #time.sleep(120)
       #logger.info('sleep complete')

       print('{} file  sucessfully extracted into {} path'.format(metafilename,extract_path))
    except Exception as ex:
        logger.info("Error in  extracting .dat table")
        logger.error('\n************************ ERROR ENCOUNTERED in %s ************************ os_error_exit')
        logger.error(str(ex))
        logger.error('********************************* PROCESS TERMINATED ************************************')
        raise Exception("Error in extracting .dat data")


    print('\nchecking .dat and .meta file count:')
    print('=====================================\n')
    datcount=ext_df.count()
    final_df=spark.sql("select *  from {}".format(cn_extract_table))
    fin_tbl_count=final_df.count()


    print('{} file count is :{}'.format(datfilename,datcount))
    print('\n{} table count is:{}'.format(cn_extract_table,fin_tbl_count))
    print('\ncommparing {} and {} count :'.format(cn_extract_table,datfilename))
    if(fin_tbl_count==datcount):
      logger.info('Complete data available in dat file')
    else:
      logger.info('\nComplete data is not available in dat file')
      return 1


    print('\n{} file size checke :'. format(metafilename))
    if (os.stat(meta_extractfile).st_size >0):
      logger.info('{} file data is greater then 0 byte ,hence  data available in meta file'.format(metafilename))
    else:
      logger.info('\nComplete data is not available in meta file')
      return 1



#Main
##########################################################################################################################################################
# Description:      This is the main function for building the Statics table
# Input Parameters: argument_list: list of command line arguments supplied when executing the process
# Return Type:      None
##########################################################################################################################################################
def main(argument_list):

    logger=log.get_logger("Shoprite_cn_extract")
    start_time = datetime.datetime.now()

    logger.info('\n\n=====================================================')
    logger.info('SCRIPT NAME    : shoprite_consumer_extract.py')
    logger.info('START TIME     : {0}'.format(start_time.strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
    logger.info('=====================================================\n\n')

    return_value = log.log_on_execute_with_options(False, logger, util.validate_number_of_arguments, argument_list[1:], 3)

    if return_value[0] == util.FAILURE:
        raise Exception(return_value[1])

    provider_name                        = argument_list[1].lower().strip()
    retailer_name                        = argument_list[2].lower().strip()
    retailer_dir_path                    = argument_list[3].lower().strip()

    spark = SparkSession.builder.appName("Shoprite cn Table Load").enableHiveSupport().getOrCreate()

    try:
        dim_extract_load(provider_name,retailer_name,retailer_dir_path,logger,spark)

    except Exception as ex:
        logger.info("ERROR in loading consumer exrtract table")
        logger.error('\n************************ ERROR ENCOUNTERED in %s ************************'%__name__.upper())
        logger.error(str(ex))
        logger.error('********************************* PROCESS TERMINATED ************************************')
        raise
    finally:
        if spark is not None:
            spark.stop()

    log.log_end_time(logger, start_time, "Shoprite_cn_Load", "show_elapsed")

##########################################################################################################################################################
#GLOBAL SECTION
##########################################################################################################################################################
if __name__ == '__main__':
    main(sys.argv)
