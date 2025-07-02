
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
from glob import glob as getfiles
from sys import exit
from config_reader import ConfigFactory as cf

def load_master(provider_name, retailer_name, retailer_dir_path, logger, spark):

    logger=log.get_logger("Shoprite_Customerdnaattributes_Load")
    start_time = datetime.datetime.now()

    cf_obj = cf()
    config_obj = cf_obj.create_obj(True, logger)
    config_op_obj = config_obj.obj.parse(retailer_dir_path)

    sat_customerdnaattributes_raw_table           = config_op_obj["cn_dim"]["CDNA_SAT_RAW_TABLE"]
    hub_customerdnaattributes_raw_table           = config_op_obj["cn_dim"]["CDNA_HUB_RAW_TABLE"]
    customerdnaattributes_mstr_table              = config_op_obj["cn_dim"]["CDNA_MSTR_TABLE"]
    customerdnaattributes_mstr_table_path         = config_op_obj["cn_dim"]["CDNA_MSTR_TABLE_PATH"]
    customerdnaattributes_bkp_table_name          = config_op_obj["cn_dim"]["CDNA_BACKUP_TABLE"]
    customerdnaattributes_hub_sourcepath          = config_op_obj["cn_dim"]["CDNA_RAW_HUB_SOURCE_PATH"]
    customerdnaattributes_sat_sourcepath          = config_op_obj["cn_dim"]["CDNA_RAW_SAT_SOURCE_PATH"]


    logger.info("\n\nParameter details are")
    logger.info("==========================================================")
    logger.info("sat_customerdnaattributes_raw_table           : "+sat_customerdnaattributes_raw_table)
    logger.info("hub_customerdnaattributes_raw_table           : "+hub_customerdnaattributes_raw_table)
    logger.info("customerdnaattributes_mstr_table              : "+customerdnaattributes_mstr_table)
    logger.info("customerdnaattributes_mstr_table_path         : "+customerdnaattributes_mstr_table_path)
    logger.info("customerdnaattributes_bkp_table_name          : "+customerdnaattributes_bkp_table_name)
    logger.info("customerdnaattributes_hub_sourcepath          : "+customerdnaattributes_hub_sourcepath)
    logger.info("customerdnaattributes_sat_sourcepath          : "+customerdnaattributes_sat_sourcepath)
    logger.info("==========================================================\n")


    hub_delta_file_path   = customerdnaattributes_hub_sourcepath + "/*Delta*"
    hub_history_file_path = customerdnaattributes_hub_sourcepath + "/*History*"
    sat_delta_file_path   = customerdnaattributes_sat_sourcepath + "/*Delta*"
    sat_history_file_path = customerdnaattributes_sat_sourcepath + "/*History*"
    hub_file_path_done   = customerdnaattributes_hub_sourcepath + "/*Done"
    sat_file_path_done = customerdnaattributes_sat_sourcepath + "/*Done"	
	
	
	
    #get any files with the name Delta in the name
    hub_delta_files = getfiles(hub_delta_file_path)
    sat_delta_files = getfiles(sat_delta_file_path)
    hub_files_done = getfiles(hub_file_path_done)
    sat_files_done = getfiles(sat_file_path_done)		

    #get any files with the name History in the name
    hub_history_files = getfiles(hub_history_file_path)
    sat_history_files = getfiles(sat_history_file_path)
	
    if((len(hub_files_done)>=1) or (len(sat_files_done)>=1)):

        #Checking history files
        #if ("history" in files[0].lower()):
        print('')
        if((len(hub_history_files)>=1) and (len(sat_history_files)>=1) ):
        
          print ('history file present')
          #Checking is  master table Exists or not
          if (spark.catalog._jcatalog.tableExists(customerdnaattributes_mstr_table)== True):
           #Creating Backup table for customerdnaattributes master table
            print('Master table Exists taking a backup ')
            print('')
            print('Creating backup table for master table started at {} '.format(customerdnaattributes_mstr_table,datetime.datetime.now().strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
            print("==========================================================")
        
            try:
              sql_to_run=("drop table {}".format(customerdnaattributes_bkp_table_name))
              print('')
              print(sql_to_run)
              print('')
              spark.sql(sql_to_run)
              print('')
              print("{} existing todays backup table sucessfully dropped at {}".format(customerdnaattributes_bkp_table_name,datetime.datetime.now().strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
              print("==========================================================")
              print('')
            except Exception as ex:
              logger.info("Error while creating customerdnaattributes bckup table ")
        
        
            try:
              sql_to_run=("create table {}  as select * from {}".format(customerdnaattributes_bkp_table_name,customerdnaattributes_mstr_table))
              print('')
              print(sql_to_run)
              print('')
              spark.sql(sql_to_run)
              print('')
              print("{} backup table sucessfully created at {}".format(customerdnaattributes_bkp_table_name,datetime.datetime.now().strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
              print("==========================================================")
              print('')
            except Exception as ex:
              logger.info("Error while creating customerdnaattributes bckup table ")
              logger.error('\n======================== ERROR ENCOUNTERED in %s ======================== os_error_exit')
              logger.error(str(ex))
              logger.error('================================= PROCESS TERMINATED ====================================')
              raise Exception("Error while creating customerdnaattributes bckup table")
    
          print('Drop and Creating {} table process Started @ {}'.format(customerdnaattributes_mstr_table,datetime.datetime.now().strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'            +'%S'+' %Z')))
          print("==========================================================")
              #Droping customerdnaattributes master table
          try:
            sql_to_run=("drop table if exists {} purge".format(customerdnaattributes_mstr_table))
            print('')
            print(sql_to_run)
            print('')
            spark.sql(sql_to_run)
            print('')
            print("{} table sucessfully dropped ".format(customerdnaattributes_mstr_table))
        
          except Exception as ex:
            logger.info("Error while dropping sat master table ")
            logger.error('\n======================== ERROR ENCOUNTERED in %s ======================== os_error_exit')
            logger.error(str(ex))
            logger.error('================================= PROCESS TERMINATED ====================================')
            raise Exception("Error while dropping sat master table")
        
            #Creating customerdnaattributes master table
        
          try:
              print('')
              print('Creating customerdnaattributes master table {} '.format(customerdnaattributes_mstr_table))
              print('')
              sql_to_run=("create table {0} location '{1}' as (select dunnhumby_customerdnaattributes_hkey,custcode,storeformat,shabit,pricesenstivity,shoppingmission,customerprofitability from (select s.dunnhumby_customerdnaattributes_hkey,h.custcode,h.storeformat,s.shabit,s.pricesenstivity,s.shoppingmission,s.customerprofitability, rank() over (partition by s.dunnhumby_customerdnaattributes_hkey order by s.load_date desc) as rank from {2} h,{3} s where h.dunnhumby_customerdnaattributes_hkey =s.dunnhumby_customerdnaattributes_hkey) a where rank=1 )".format(customerdnaattributes_mstr_table,customerdnaattributes_mstr_table_path,hub_customerdnaattributes_raw_table,sat_customerdnaattributes_raw_table))
              print('')
              print(sql_to_run)
              print('')
              spark.sql(sql_to_run)
              print("customerdnaattributes master sucessfully created at {} ".format(datetime.datetime.now().strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
              print("==========================================================")
              print('')
          except Exception as ex:
              logger.info("Error in creating customerdnaattributes master table")
              logger.error('\n======================== ERROR ENCOUNTERED in %s ======================== os_error_exit')
              logger.error(str(ex))
              logger.error('================================= PROCESS TERMINATED ====================================')
              raise Exception("Error in creating customerdnaattributes master table")
        #if ("delta" in files[-1].lower()):
        else :
          print('')
          print(" processing the Delta file : ")
          print('')
          print('hub Delta files are :{}'.format(hub_delta_files))
          print('')
          print('sat Delta files are :{}'.format(sat_delta_files))
          print('')
          print("Creating customerdnaattributes master table process started at {} ".format(datetime.datetime.now().strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
          print ("==========================================================")
        
          if(spark.catalog._jcatalog.tableExists(customerdnaattributes_mstr_table)== False):
            print('Site master table not exists,hence creating')
            sql_to_run=("create table {0} as (select a.* from  {1} a  where 1=2)".format(customerdnaattributes_mstr_table,customerdnaattributes_mstr_table))
            try:
              print('')
              print(sql_to_run)
              print('')
              spark.sql(sql_to_run)
              print('{} table sucessfully created '.format(customerdnaattributes_mstr_table))
            except Exception as ex:
              logger.info("Error while creating customerdnaattributes master table ")
              
          print('Creating backup table for master table started at {} '.format(customerdnaattributes_mstr_table,datetime.datetime.now().strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
          print("==========================================================")
          try:
              sql_to_run=("drop table {}".format(customerdnaattributes_bkp_table_name))
              print('')
              print(sql_to_run)
              print('')
              spark.sql(sql_to_run)
              print('')
              print("{} existing todays backup table sucessfully dropped at {}".format(customerdnaattributes_bkp_table_name,datetime.datetime.now().strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
              print("==========================================================")
              print('')
          except Exception as ex:
              logger.info("Error while creating customerdnaattributes bckup table ")
          
          
          try:
              sql_to_run=("create table {}  as select * from {}".format(customerdnaattributes_bkp_table_name,customerdnaattributes_mstr_table))
              print('')
              print(sql_to_run)
              print('')
              spark.sql(sql_to_run)
              print('')
              print("{} backup table sucessfully created at {}".format(customerdnaattributes_bkp_table_name,datetime.datetime.now().strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
              print("==========================================================")
              print('')
          except Exception as ex:
              logger.info("Error while creating customerdnaattributes bckup table ")
              logger.error('\n======================== ERROR ENCOUNTERED in %s ======================== os_error_exit')
              logger.error(str(ex))
              logger.error('================================= PROCESS TERMINATED ====================================')
              raise Exception("Error while creating customerdnaattributes bckup table")
    
    
          sql_to_run=("drop table if exists {} purge".format(customerdnaattributes_mstr_table))
          print('')
          print(sql_to_run)
          print('')
          spark.sql(sql_to_run)
          print('')
          logger.info("{} table sucessfully dropped ".format(customerdnaattributes_mstr_table))
        
        
          #This is New master creation logic
          try:
             print('')
             print('Creating customerdnaattributes master table {}'.format(customerdnaattributes_mstr_table))
             print('')
             sql_to_run=("create table {0} location '{1}' as (select dunnhumby_customerdnaattributes_hkey,custcode,storeformat,shabit,pricesenstivity,shoppingmission,customerprofitability from (select s.dunnhumby_customerdnaattributes_hkey,h.custcode,h.storeformat,s.shabit,s.pricesenstivity,s.shoppingmission,s.customerprofitability, rank() over (partition by s.dunnhumby_customerdnaattributes_hkey order by s.load_date desc) as rank from {2} h,{3} s where h.dunnhumby_customerdnaattributes_hkey =s.dunnhumby_customerdnaattributes_hkey) a where rank=1 )".format(customerdnaattributes_mstr_table,customerdnaattributes_mstr_table_path,hub_customerdnaattributes_raw_table,sat_customerdnaattributes_raw_table))
             print('')
             print(sql_to_run)
             print('')
             spark.sql(sql_to_run)
             print("Master table sucessfully created at {} ".format(datetime.datetime.now().strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
             print('==========================================================')
             print('')
        
          except Exception as ex:
              logger.info("Error in creating Master table")
              logger.error('\n======================== ERROR ENCOUNTERED in %s ======================== os_error_exit')
              logger.error(str(ex))
              logger.error('================================= PROCESS TERMINATED ====================================')
              raise Exception("Error in creating master table")
          print('')
    else:
        print('')
        print('Done files are not available , hence retaining old master table ') 			  
        
#Main
##########################################################################################################################################################
# Description:      This is script will create Article  master table
# Input Parameters: argument_list: list of command line arguments supplied when executing the process
# Return Type:      None
##########################################################################################################################################################
def main(argument_list):

    logger=log.get_logger("Shoprite_Customerdnaattributes_Load")
    start_time = datetime.datetime.now()

    logger.info('\n\n=====================================================')
    logger.info('SCRIPT NAME    : shoprite_customerdnaattributes_master_table_load.py')
    logger.info('START TIME     : {0}'.format(start_time.strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
    logger.info('=====================================================\n\n')


    return_value = log.log_on_execute_with_options(False, logger, util.validate_number_of_arguments, argument_list[1:], 3)

    if return_value[0] == util.FAILURE:
        raise Exception(return_value[1])

    provider_name                        = argument_list[1].lower().strip()
    retailer_name                        = argument_list[2].lower().strip()
    retailer_dir_path                    = argument_list[3].lower().strip()

    spark = SparkSession.builder.appName("Shoprite customerdnaattributes Table Load").enableHiveSupport().getOrCreate()

    try:
        load_master(provider_name,retailer_name,retailer_dir_path,logger,spark)

    except Exception as ex:
        logger.info("ERROR in loading customerdnaattributes  master table")
        logger.error('\n======================== ERROR ENCOUNTERED in %s ========================'%__name__.upper())
        logger.error(str(ex))
        logger.error('================================= PROCESS TERMINATED ====================================')
        raise
    finally:
        if spark is not None:
            spark.stop()

    log.log_end_time(logger, start_time, "Shoprite_Customerdnaattributes_Load", "show_elapsed")

##########################################################################################################################################################
#GLOBAL SECTION
##########################################################################################################################################################
if __name__ == '__main__':
    main(sys.argv)

