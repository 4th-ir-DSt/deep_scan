
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
from glob import glob as getfiles
from re import findall as getdate
from shutil import rmtree as delete_folder
from zipfile import ZipFile
def load_ext(provider_name, retailer_name, retailer_dir_path, logger, spark):

    logger=log.get_logger("Shoprite_Customerdnaattributes_Load")
    start_time = datetime.datetime.now()

    cf_obj = cf()
    config_obj = cf_obj.create_obj(True, logger)
    config_op_obj = config_obj.obj.parse(retailer_dir_path)

    cdna_hub_raw_table                  = config_op_obj["cn_dim"]["CDNA_HUB_RAW_TABLE"]
    cdna_hub_raw_table_path             = config_op_obj["cn_dim"]["CDNA_HUB_RAW_TABLE_PATH"]
    cdna_hub_sourcepath                 = config_op_obj["cn_dim"]["CDNA_RAW_HUB_SOURCE_PATH"]
    cdna_hub_filename                   = config_op_obj["cn_dim"]["CDNA_RAW_HUB_FILENAME"]

    logger.info("\nParameter details are ")
    logger.info("=================================================================")
    logger.info('cdna_hub_raw_table      : '+cdna_hub_raw_table)
    logger.info('cdna_hub_raw_table_path : '+cdna_hub_raw_table_path)
    logger.info('cdna_hub_sourcepath     : '+cdna_hub_sourcepath)
    logger.info('cdna_hub_filename       : '+cdna_hub_filename)
    logger.info("=================================================================\n")


    logger.info("Source file availablity checking : ")
    logger.info("=================================\n")
    logger.info("Source file name is :{}".format(cdna_hub_filename))
    logger.info("Source file path is :{}\n".format(cdna_hub_sourcepath))



    fileNamePattern=str(cdna_hub_sourcepath)+str(cdna_hub_filename)+'*'
    latestfile = fileNamePattern
    files = glob.glob(fileNamePattern)



    #If no files found, we will rise Exception
    if (len(files) == 0):
        logger.error("no history or delta files present ")
        raise Exception("no history or delta files present")
    print('Source files are : {}'.format(files))
    print('=============================================\n')


   #Cheking is Customerdnaattributes external raw table exista or not if not exists creating table
    if (spark.catalog._jcatalog.tableExists(cdna_hub_raw_table)== False):

      try:
         print('')
         print('Creating  external raw table: {}'.format(cdna_hub_raw_table))
         print('=============================================\n')
         print('')
         sql_to_run=("create external table {0} (dunnhumby_customerdnaattributes_hkey string,storeformat string,custcode string,load_date string) partitioned by (segment int)  stored as parquet location '{1}' ".format(cdna_hub_raw_table,cdna_hub_raw_table_path))
         print('')
         print(sql_to_run)
         print('')
         spark.sql(sql_to_run)
         print('Sucessfully created external table {}'.format(cdna_hub_raw_table))
         print('=============================================\n')
      except Exception as ex:
          logger.info("Error in creating Customerdnaattributes HUB external table")
          logger.error('\n======================== ERROR ENCOUNTERED in %s ======================== os_error_exit')
          logger.error(str(ex))
          logger.error('================================= PROCESS TERMINATED ====================================')
          raise Exception("Error in creating Customerdnaattributes HUB external table")


    for i in range(len(files)):
        latestfile=files[i]
        file_name = os.path.basename(latestfile)
        file_date = getdate(r'\d+', file_name)
        segment_path = cdna_hub_raw_table_path + "segment=" + file_date[0].decode()+'/'

        if ("history" in file_name.lower()):
            logger.info("Will process the history file :%s " %(file_name))
            print('=============================================')
            if (os.path.exists(segment_path) == True):
              delete_folder(segment_path)

            hadoop_create_dir = "hadoop fs -mkdir -p " + segment_path
            print ("Creating segment folder if not exists in hadoop path :\n\nCommand is : %s " %(hadoop_create_dir))
            if (os.system(hadoop_create_dir) != 0):
                logger.error("Creation of the folder %s failed" %(segment_path))
                raise Exception("Creation of the folder %s failed" %(segment_path))
            else :
                print('')
                print('Segment directory {} Sucessfully created '.format(segment_path))

            print('')
            print("File Unzipping and Load process Started...")
            print('')
            command = "unzip "+ "-o " + latestfile  + " -d " + segment_path
            logger.info("command is : %s " %(command))
            if (os.system(command) != 0):
                logger.error("Failed to run the command : %s" %(command))
                raise Exception("unzip command execution failed")
            else :
              print('')
              print('File {} Sucessfully unzipped and loaded into {} path '.format(latestfile,segment_path))
              print('=============================================\n')
        elif ("delta" in file_name.lower()):
            #Will process the delta data, ex:CloudBI.DiscPromo.Delta.20210516.20210518140039.snappy.parquet
            logger.info("Will process the delta File : %s " %(file_name))
            print('=============================================\n')
            hadoop_create_dir = "hadoop fs -mkdir -p " + segment_path
            print ("Creating segment folder if not exists in hadoop path :\n\n Command is : %s " %(hadoop_create_dir))
            if (os.system(hadoop_create_dir) != 0):
              logger.error("Creation of the folder %s failed" %(segment_path))
              raise Exception("Creation of the folder %s failed" %(segment_path))
            else :
              print('')
              print('Segment folder sucessfully created {}'.format(segment_path))
              print('')


            command = "hadoop fs -copyFromLocal {0}{1} {2}".format(cdna_hub_sourcepath, file_name, segment_path)
            logger.info("command is :%s " %(command))
            if (os.system(command) != 0):
                logger.error("Failed to run the command : %s" %(command))
                raise Exception("Copy command execution failed")
            else :
              print('')
              print('File {} Sucessfully loaded  into {} path'.format(file_name,segment_path))
              print('=============================================\n')
        #out_file.close()

    try:

        sql_query =("MSCK REPAIR TABLE {}".format(cdna_hub_raw_table))
        print('')
        print("Add new partitions into history table :")
        print("Command is  : "+ sql_query)
        spark.sql(sql_query)
        print('')
        print("MSCK REPAIR TABLE {} Command Executed Sucessfully ".format(cdna_hub_raw_table))
    except Exception as ex:
        logger.error("SQL Query for MSCK REPAIR TABLE Failed")
        raise Exception(ex)




#Main
##########################################################################################################################################################
# Description:      This is the main function for building the Customerdnaattributes Hub stage  table
# Input Parameters: argument_list: list of command line arguments supplied when executing the process
# Return Type:      None
##########################################################################################################################################################
def main(argument_list):

    logger=log.get_logger("Shoprite_Customerdnaattributes_Load")
    start_time = datetime.datetime.now()

    logger.info('\n============================================================================')
    logger.info('SCRIPT NAME    : shoprite_customerdnaattributes_hub_raw_table_load.py')
    logger.info('START TIME     : {0}'.format(start_time.strftime('%d-'+'%b-'+'%Y'+' %H:'+'%M:'+'%S'+' %Z')))
    logger.info('==============================================================================\n')
    return_value = log.log_on_execute_with_options(False, logger, util.validate_number_of_arguments, argument_list[1:], 3)

    if return_value[0] == util.FAILURE:
        raise Exception(return_value[1])

    provider_name                        = argument_list[1].lower().strip()
    retailer_name                        = argument_list[2].lower().strip()
    retailer_dir_path                    = argument_list[3].lower().strip()

    spark = SparkSession.builder.appName("Shoprite Customerdnaattributes Table Load").enableHiveSupport().getOrCreate()

    try:
        load_ext(provider_name,retailer_name,retailer_dir_path,logger,spark)

    except Exception as ex:
        logger.info("ERROR in loading customerdnaattributes external table")
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

