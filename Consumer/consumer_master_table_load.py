
import sys
import subprocess

ENV = subprocess.check_output('echo $ENV', shell=True);
sys.path.append('/opt/ildbld/' + ENV.decode().strip() + '/spark_frmwrk/scripts/')
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
    logger = log.get_logger("Shoprite_consumer_Load")
    start_time = datetime.datetime.now()

    cf_obj = cf()
    config_obj = cf_obj.create_obj(True, logger)
    config_op_obj = config_obj.obj.parse(retailer_dir_path)

    sat_cn_raw_table = config_op_obj["cn_dim"]["CN_SAT_RAW_TABLE"]
    hub_cn_raw_table = config_op_obj["cn_dim"]["CN_HUB_RAW_TABLE"]
    cn_mstr_table = config_op_obj["cn_dim"]["CN_MSTR_TABLE"]
    cn_mstr_table_path = config_op_obj["cn_dim"]["CN_MSTR_TABLE_PATH"]
    cn_bkp_table_name = config_op_obj["cn_dim"]["CN_BACKUP_TABLE"]
    cn_mstr_new_table = config_op_obj["cn_dim"]["CN_DELTA_TMP_TABLE"]
    cn_mstr_new_table_path = config_op_obj["cn_dim"]["CN_DELTA_TMP_TABLE_PATH"]
    cn_mstr_new_table = config_op_obj["cn_dim"]["CN_MSTR_NEW_TABLE"]
    cn_mstr_new_table_path = config_op_obj["cn_dim"]["CN_MSTR_NEW_TABLE_PATH"]
    cn_hub_sourcepath = config_op_obj["cn_dim"]["CN_RAW_HUB_SOURCE_PATH"]
    cn_sat_sourcepath = config_op_obj["cn_dim"]["CN_RAW_SAT_SOURCE_PATH"]
    time_key_table = config_op_obj["cn_dim"]["TIME_DIM_DAY"]

    logger.info("\n\nParameter details are")
    logger.info("==========================================================")
    logger.info("sat_cn_raw_table           : " + sat_cn_raw_table)
    logger.info("hub_cn_raw_table           : " + hub_cn_raw_table)
    logger.info("cn_mstr_table              : " + cn_mstr_table)
    logger.info("cn_mstr_table_path         : " + cn_mstr_table_path)
    logger.info("cn_bkp_table_name          : " + cn_bkp_table_name)
    logger.info("cn_mstr_new_table          : " + cn_mstr_new_table)
    logger.info("cn_mstr_new_table_path     : " + cn_mstr_new_table_path)
    logger.info("cn_hub_sourcepath          : " + cn_hub_sourcepath)
    logger.info("cn_sat_sourcepath          : " + cn_sat_sourcepath)
    logger.info("==========================================================\n")

    hub_delta_file_path = cn_hub_sourcepath + "/*Delta*"
    hub_history_file_path = cn_hub_sourcepath + "/*History*"
    sat_delta_file_path = cn_sat_sourcepath + "/*Delta*"
    sat_history_file_path = cn_sat_sourcepath + "/*History*"

    hub_file_path_done = cn_hub_sourcepath + "/*Done"
    sat_file_path_done = cn_sat_sourcepath + "/*Done*"
    # get any files with the name Delta in the name
    hub_delta_files = getfiles(hub_delta_file_path)
    sat_delta_files = getfiles(sat_delta_file_path)

    # get any files with the name History in the name
    hub_history_files = getfiles(hub_history_file_path)
    sat_history_files = getfiles(sat_history_file_path)

    # get any files with the name Done in the name
    hub_files_done = getfiles(hub_file_path_done)
    sat_files_done = getfiles(sat_file_path_done)

    # Checking Done files
    if ((len(hub_files_done) >= 1) and (len(sat_files_done) >= 1)):
        # Checking history files
        if ((len(hub_history_files) >= 1) and (len(sat_history_files) >= 1)):
            print('Will process the history load')
            print("==========================================================")
            print('')
            # Checking is  master table Exists or not
            if (spark.catalog._jcatalog.tableExists(cn_mstr_table) == True):
                # Creating Backup table for consumer master table
                print('Master table Exists taking a backup ')
                print('')
                print('Creating backup table for master table started at {} '.format(cn_mstr_table,
                                                                                     datetime.datetime.now().strftime(
                                                                                         '%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
                print("==========================================================")

                try:
                    sql_to_run = ("drop table if exists {}".format(cn_bkp_table_name))
                    print('')
                    print(sql_to_run)
                    print('')
                    spark.sql(sql_to_run)
                    print('')
                    print("{} existing todays backup table successfully dropped at {}".format(cn_bkp_table_name,
                                                                                              datetime.datetime.now().strftime(
                                                                                                  '%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
                    print("==========================================================")
                    print('')
                except Exception as ex:
                    raise Exception("Error while droping consumer bckup table ")

                try:
                    sql_to_run = (
                        "create table if not exists {0}  as select * from {1}".format(cn_bkp_table_name, cn_mstr_table))
                    print('')
                    print(sql_to_run)
                    print('')
                    spark.sql(sql_to_run)
                    print('')
                    print("{} backup table successfully created at {}".format(cn_bkp_table_name,
                                                                              datetime.datetime.now().strftime(
                                                                                  '%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
                    print("==========================================================")
                    print('')
                except Exception as ex:
                    logger.info("Error while creating consumer bckup table ")
                    logger.error(
                        '\n======================== ERROR ENCOUNTERED in %s ======================== os_error_exit')
                    logger.error(str(ex))
                    logger.error(
                        '================================= PROCESS TERMINATED ====================================')
                    raise Exception("Error while creating consumer bckup table")
                    print('Drop and Creating {} table process Started @ {}'.format(cn_mstr_table,
                                                                                   datetime.datetime.now().strftime(
                                                                                       '%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
                    print("==========================================================")
                    # Droping consumer master table
                try:
                    sql_to_run = ("drop table if exists {} purge".format(cn_mstr_table))
                    print('')
                    print(sql_to_run)
                    print('')
                    spark.sql(sql_to_run)
                    print('')
                    print("{} table successfully dropped ".format(cn_mstr_table))

                except Exception as ex:
                    logger.info("Error while dropping consumer master table ")
                    logger.error(
                        '\n======================== ERROR ENCOUNTERED in %s ======================== os_error_exit')
                    logger.error(str(ex))
                    logger.error(
                        '================================= PROCESS TERMINATED ====================================')
                    raise Exception("Error while dropping consumer master table")

                # Creating consumer master table

                try:
                    print('')
                    print('Creating cn master table {} '.format(cn_mstr_table))

                    print('')

                    sql_to_run = (
                        "create table {0} location '{1}' as (select ab.*,loyaltycard_count from (select householdmemberidentifier,retailbrand,ciam_customerprofile_detail_hkey,householdidentifier,brandmemberstatus,prefferedstore,customercreationdate,customerupdateddate,loyaltycardnumber,cardcreatedate,cardupdatedate,personalidentificationnumberprefix,birthday,email,mobilenumberprefix,registrationchannel,countryofissue,registrationagent,case when current_age < 0 then '' else current_age end as current_age, CASE when current_age < 0 then '' WHEN current_age < 20  THEN '<20 Yrs' WHEN current_age  between 20 and 25 then '20-25 Yrs' WHEN current_age between 26 and 30 then '26-30 Yrs' WHEN current_age between 31 and 35 then '31-35 Yrs' WHEN current_age between 36 and 40  then '36-40 Yrs' WHEN current_age between 41 and 45 then  '41-45 Yrs' WHEN current_age between 46 and 50 then '46-50 Yrs' WHEN current_age between 51 and 55 then '51-55 Yrs' WHEN current_age between 56 and 59 then '56-59 Yrs' WHEN current_age >=60 then '>60 Yrs' end  AGE_RANGE ,CASE WHEN (SUBSTR(personalidentificationnumberprefix,7,8))  between 00 and 49  THEN 'Female' WHEN (SUBSTR(personalidentificationnumberprefix,7,8))  between 50 and 99  THEN 'Male' end  GENDER,case when   (email= '' or email='NULL' or email is null) then 'No' else 'Yes' end  HAS_EMAIL, pmod(row_number() over (order by householdidentifier),2000) as cn_hash_id ,t.tm_dim_key as cust_create_dt_key    from ( select h.householdmemberidentifier,h.retailbrand,s.*,case   when personalidentificationnumberprefix is null and birthday is not null then round(cast(((datediff(current_date,from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0) when personalidentificationnumberprefix is not null and cast(personalidentificationnumberprefix as int) is null then round(cast(((datediff(current_date,from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0) when personalidentificationnumberprefix is not null and cast(from_unixtime(unix_timestamp((SUBSTR(personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd') as date) is null then round(cast(((datediff(current_date, from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0)      when personalidentificationnumberprefix is not null and round(cast(((datediff(current_date, from_unixtime(unix_timestamp((SUBSTR(personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd')))/365.25) as int),0) < 50 then round(cast(((datediff(current_date, from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0)    when personalidentificationnumberprefix is not null and birthday is not null and round(cast(((datediff(current_date, from_unixtime(unix_timestamp((SUBSTR(personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd')))/365.25) as int),0) <> round(cast(((datediff(current_date, from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0) then round(cast(((datediff(current_date, from_unixtime(unix_timestamp((SUBSTR(personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd')))/365.25) as int),0) else      round(cast(((datediff(current_date, from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0) end current_age,rank() over (partition by s.ciam_customerprofile_detail_hkey order by s.load_date desc) as rank  from  {2} h, {3} s  where (h.ciam_customerprofile_detail_hkey =s.ciam_customerprofile_detail_hkey)) a inner join {4} t on (from_unixtime(unix_timestamp(a.customercreationdate,'yyyy/MM/dd'),'MM-dd-yy')=t.tm_end_date) where rank=1) ab left outer join (select householdmemberidentifier,h.retailbrand,count(distinct loyaltycardnumber) loyaltycard_count from {3} s join {2} h on s.ciam_customerprofile_detail_hkey=h.ciam_customerprofile_detail_hkey group by h.householdmemberidentifier,h.retailbrand) b on ab.householdmemberidentifier=b.householdmemberidentifier and ab.retailbrand=b.retailbrand)".format(
                            cn_mstr_table, cn_mstr_table_path, hub_cn_raw_table, sat_cn_raw_table, time_key_table))
                    print('')
                    print(sql_to_run)
                    print('')
                    spark.sql(sql_to_run)
                    print('inserting data from master table ,if  backup table data not there in master table')
                    print('')
                    sql_to_insert = (
                        "insert into {0} ( select c.* from {1} c  ANTI JOIN {0} d on (c.householdmemberidentifier=d.householdmemberidentifier and c.retailbrand=d.retailbrand) ) ".format(
                            cn_mstr_table, cn_bkp_table_name))
                    print(sql_to_insert)
                    print('')
                    spark.sql(sql_to_insert)
                    print('')

                    print("consumer master successfully created at {} ".format(
                        datetime.datetime.now().strftime('%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
                    print("==========================================================")
                    print('')
                    # reak
                except Exception as ex:
                    logger.info("Error in creating consumer  master table")
                    logger.error(
                        '\n======================== ERROR ENCOUNTERED in %s ======================== os_error_exit')
                    logger.error(str(ex))
                    logger.error(
                        '================================= PROCESS TERMINATED ====================================')
                    raise Exception("Error in creating consumer master table")
            else:
                try:
                    print('')
                    print('Creating cn master table {} Started at {} '.format(cn_mstr_table,
                                                                              datetime.datetime.now().strftime(
                                                                                  '%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
                    print("==========================================================")
                    print('')

                    sql_to_run = (
                        "create table {0} location '{1}' as (select ab.*,loyaltycard_count from (select householdmemberidentifier,retailbrand,ciam_customerprofile_detail_hkey,householdidentifier,brandmemberstatus,prefferedstore,customercreationdate,customerupdateddate,loyaltycardnumber,cardcreatedate,cardupdatedate,personalidentificationnumberprefix,birthday,email,mobilenumberprefix,registrationchannel,countryofissue,registrationagent,case when current_age < 0 then '' else current_age end as current_age,  CASE when current_age < 0 then '' WHEN current_age < 20  THEN '<20 Yrs' WHEN current_age  between 20 and 25 then '20-25 Yrs' WHEN current_age between 26 and 30 then '26-30 Yrs' WHEN current_age between 31 and 35 then '31-35 Yrs' WHEN current_age between 36 and 40  then '36-40 Yrs' WHEN current_age between 41 and 45 then  '41-45 Yrs' WHEN current_age between 46 and 50 then '46-50 Yrs' WHEN current_age between 51 and 55 then '51-55 Yrs' WHEN current_age between 56 and 59 then '56-59 Yrs' WHEN current_age >=60 then '>60 Yrs' end  AGE_RANGE ,CASE WHEN (SUBSTR(personalidentificationnumberprefix,7,8))  between 00 and 49  THEN 'Female' WHEN (SUBSTR(personalidentificationnumberprefix,7,8))  between 50 and 99  THEN 'Male' end  GENDER,case when   (email= '' or email='NULL' or email is null) then 'No' else 'Yes' end  HAS_EMAIL, pmod(row_number() over (order by householdidentifier),2000) as cn_hash_id ,t.tm_dim_key as cust_create_dt_key    from ( select h.householdmemberidentifier,h.retailbrand,s.*,case   when personalidentificationnumberprefix is null and birthday is not null then round(cast(((datediff(current_date,from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0) when personalidentificationnumberprefix is not null and cast(personalidentificationnumberprefix as int) is null then round(cast(((datediff(current_date,from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0) when personalidentificationnumberprefix is not null and cast(from_unixtime(unix_timestamp((SUBSTR(personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd') as date) is null then round(cast(((datediff(current_date, from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0)      when personalidentificationnumberprefix is not null and round(cast(((datediff(current_date, from_unixtime(unix_timestamp((SUBSTR(personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd')))/365.25) as int),0) < 50 then round(cast(((datediff(current_date, from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0)    when personalidentificationnumberprefix is not null and birthday is not null and round(cast(((datediff(current_date, from_unixtime(unix_timestamp((SUBSTR(personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd')))/365.25) as int),0) <> round(cast(((datediff(current_date, from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0) then round(cast(((datediff(current_date, from_unixtime(unix_timestamp((SUBSTR(personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd')))/365.25) as int),0) else      round(cast(((datediff(current_date, from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0) end current_age,rank() over (partition by s.ciam_customerprofile_detail_hkey order by s.load_date desc) as rank  from  {2} h, {3} s  where (h.ciam_customerprofile_detail_hkey =s.ciam_customerprofile_detail_hkey)) a inner join {4} t on (from_unixtime(unix_timestamp(a.customercreationdate,'yyyy/MM/dd'),'MM-dd-yy')=t.tm_end_date) where rank=1) ab left outer join (select householdmemberidentifier,h.retailbrand,count(distinct loyaltycardnumber) loyaltycard_count from {3} s join {2} h on s.ciam_customerprofile_detail_hkey=h.ciam_customerprofile_detail_hkey group by h.householdmemberidentifier,h.retailbrand) b on ab.householdmemberidentifier=b.householdmemberidentifier and ab.retailbrand=b.retailbrand)".format(
                            cn_mstr_table, cn_mstr_table_path, hub_cn_raw_table, sat_cn_raw_table, time_key_table))

                    print('')
                    print(sql_to_run)
                    print('')
                    spark.sql(sql_to_run)
                    print("consumer master successfully created at {} ".format(
                        datetime.datetime.now().strftime('%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
                    print("==========================================================")
                    print('')
                    # reak
                except Exception as ex:
                    logger.info("Error in creating consumer  master table")
                    logger.error(
                        '\n======================== ERROR ENCOUNTERED in %s ======================== os_error_exit')
                    logger.error(str(ex))
                    logger.error(
                        '================================= PROCESS TERMINATED ====================================')
                    raise Exception("Error in creating consumer master table")
        else:
            print('')
            print("Will process the Delta Load: ")
            print("==========================================================")
            print('')
            print("Creating consumer master table process started at {} ".format(
                datetime.datetime.now().strftime('%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
            print("==========================================================")

            if (spark.catalog._jcatalog.tableExists(cn_mstr_table) == True):
                # Creating Backup table for consumer master table

                # Creating Backup table for consumer master table
                print('Master table Exists taking a backup ')
                print('')
                print('Creating backup table for master table started at {} '.format(cn_mstr_table,
                                                                                     datetime.datetime.now().strftime(
                                                                                         '%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
                print("==========================================================")

                try:
                    sql_to_run = ("drop table if exists {}".format(cn_bkp_table_name))
                    print('')
                    print(sql_to_run)
                    print('')
                    spark.sql(sql_to_run)
                    print('')
                    print("{} existing todays backup table successfully dropped at {}".format(cn_bkp_table_name,
                                                                                              datetime.datetime.now().strftime(
                                                                                                  '%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
                    print("==========================================================")
                    print('')
                except Exception as ex:
                    raise Exception("Error while droping consumer bckup table ")

                try:
                    sql_to_run = (
                        "create table if not exists {0}  as select * from {1}".format(cn_bkp_table_name, cn_mstr_table))
                    print('')
                    print(sql_to_run)
                    print('')
                    spark.sql(sql_to_run)
                    print('')
                    print("{} backup table successfully created at {}".format(cn_bkp_table_name,
                                                                              datetime.datetime.now().strftime(
                                                                                  '%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
                    print("==========================================================")
                    print('')
                except Exception as ex:
                    logger.info("Error while creating consumer bckup table ")
                    logger.error(
                        '\n======================== ERROR ENCOUNTERED in %s ======================== os_error_exit')
                    logger.error(str(ex))
                    logger.error(
                        '================================= PROCESS TERMINATED ====================================')
                    raise Exception("Error while creating consumer bckup table")

            sql_to_run = ("drop table if exists {} purge".format(cn_mstr_table))
            print('')
            print(sql_to_run)
            print('')
            spark.sql(sql_to_run)
            print('')
            logger.info("{} table successfully dropped ".format(cn_mstr_table))

            # This is  master creation logic
            try:
                print('')
                print('Creating consumer master table {}'.format(cn_mstr_table))
                print('==========================================================')
                print('')

                sql_to_run = (
                    "create table {0} location '{1}' as (select ab.*,loyaltycard_count from (select householdmemberidentifier,retailbrand,ciam_customerprofile_detail_hkey,householdidentifier,brandmemberstatus,prefferedstore,customercreationdate,customerupdateddate,loyaltycardnumber,cardcreatedate,cardupdatedate,personalidentificationnumberprefix,birthday,email,mobilenumberprefix,registrationchannel,countryofissue,registrationagent,case when current_age < 0 then '' else current_age end current_age,  CASE when current_age < 0 then '' WHEN current_age < 20  THEN '<20 Yrs' WHEN current_age  between 20 and 25 then '20-25 Yrs' WHEN current_age between 26 and 30 then '26-30 Yrs' WHEN current_age between 31 and 35 then '31-35 Yrs' WHEN current_age between 36 and 40  then '36-40 Yrs' WHEN current_age between 41 and 45 then  '41-45 Yrs' WHEN current_age between 46 and 50 then '46-50 Yrs' WHEN current_age between 51 and 55 then '51-55 Yrs' WHEN current_age between 56 and 59 then '56-59 Yrs' WHEN current_age >=60 then '>60 Yrs' end  AGE_RANGE ,CASE WHEN (SUBSTR(personalidentificationnumberprefix,7,8))  between 00 and 49  THEN 'Female' WHEN (SUBSTR(personalidentificationnumberprefix,7,8))  between 50 and 99  THEN 'Male' end  GENDER,case when   (email= '' or email='NULL' or email is null) then 'No' else 'Yes' end  HAS_EMAIL, pmod(row_number() over (order by householdidentifier),2000) as cn_hash_id ,t.tm_dim_key as cust_create_dt_key    from ( select h.householdmemberidentifier,h.retailbrand,s.*,case   when personalidentificationnumberprefix is null and birthday is not null then round(cast(((datediff(current_date,from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0) when personalidentificationnumberprefix is not null and cast(personalidentificationnumberprefix as int) is null then round(cast(((datediff(current_date,from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0) when personalidentificationnumberprefix is not null and cast(from_unixtime(unix_timestamp((SUBSTR(personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd') as date) is null then round(cast(((datediff(current_date, from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0)      when personalidentificationnumberprefix is not null and round(cast(((datediff(current_date, from_unixtime(unix_timestamp((SUBSTR(personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd')))/365.25) as int),0) < 50 then round(cast(((datediff(current_date, from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0)    when personalidentificationnumberprefix is not null and birthday is not null and round(cast(((datediff(current_date, from_unixtime(unix_timestamp((SUBSTR(personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd')))/365.25) as int),0) <> round(cast(((datediff(current_date, from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0) then round(cast(((datediff(current_date, from_unixtime(unix_timestamp((SUBSTR(personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd')))/365.25) as int),0) else      round(cast(((datediff(current_date, from_unixtime(unix_timestamp(birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) as int),0) end current_age,rank() over (partition by s.ciam_customerprofile_detail_hkey order by s.load_date desc) as rank  from  {2} h, {3} s  where (h.ciam_customerprofile_detail_hkey =s.ciam_customerprofile_detail_hkey)) a inner join {4} t on (from_unixtime(unix_timestamp(a.customercreationdate,'yyyy/MM/dd'),'MM-dd-yy')=t.tm_end_date) where rank=1) ab left outer join (select householdmemberidentifier,h.retailbrand,count(distinct loyaltycardnumber) loyaltycard_count from {3} s join {2} h on s.ciam_customerprofile_detail_hkey=h.ciam_customerprofile_detail_hkey group by h.householdmemberidentifier,h.retailbrand) b on ab.householdmemberidentifier=b.householdmemberidentifier and ab.retailbrand=b.retailbrand)".format(
                        cn_mstr_table, cn_mstr_table_path, hub_cn_raw_table, sat_cn_raw_table, time_key_table))

                print('')
                print(sql_to_run)
                print('')
                spark.sql(sql_to_run)
                print('')
                print("delta table successfully created at {} ".format(
                    datetime.datetime.now().strftime('%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
                print('==========================================================')
                print('')

            except Exception as ex:
                logger.info("Error in creating table")
                logger.error(
                    '\n======================== ERROR ENCOUNTERED in %s ======================== os_error_exit')
                logger.error(str(ex))
                logger.error(
                    '================================= PROCESS TERMINATED ====================================')
                raise Exception("Error in creating table")
            print('\nchecking master file count:')
            print('=====================================\n')
            final_df = spark.sql("select *  from {}".format(cn_mstr_table))
            fin_tbl_count = final_df.count()
            print('\n{} table count is:{}'.format(cn_mstr_table, fin_tbl_count))
            if (fin_tbl_count > 0):
                logger.info('data available in master table')
            else:
                logger.info('\n data is not available in master table')
                exit - 1
    else:
        print('')
        print('Done files are not available , hence retaining old master table ')

    # Main


##########################################################################################################################################################
# Description:      This is script will create consumer master table
# Input Parameters: argument_list: list of command line arguments supplied when executing the process
# Return Type:      None
##########################################################################################################################################################
def main(argument_list):
    logger = log.get_logger("Shoprite_consumer_Load")
    start_time = datetime.datetime.now()

    logger.info('\n\n=====================================================')
    logger.info('SCRIPT NAME    : shoprite_consumer_master_table_load.py')
    logger.info(
        'START TIME     : {0}'.format(start_time.strftime('%d-' + '%b-' + '%Y' + ' %H:' + '%M:' + '%S' + ' %Z')))
    logger.info('=====================================================\n\n')

    return_value = log.log_on_execute_with_options(False, logger, util.validate_number_of_arguments, argument_list[1:],
                                                   3)

    if return_value[0] == util.FAILURE:
        raise Exception(return_value[1])

    provider_name = argument_list[1].lower().strip()
    retailer_name = argument_list[2].lower().strip()
    retailer_dir_path = argument_list[3].lower().strip()

    spark = SparkSession.builder.appName("Shoprite consumer Table Load").enableHiveSupport().getOrCreate()

    try:
        load_master(provider_name, retailer_name, retailer_dir_path, logger, spark)

    except Exception as ex:
        logger.info("ERROR in loading consumer  master table")
        logger.error('\n======================== ERROR ENCOUNTERED in %s ========================' % __name__.upper())
        logger.error(str(ex))
        logger.error('================================= PROCESS TERMINATED ====================================')
        raise
    finally:
        if spark is not None:
            spark.stop()

    log.log_end_time(logger, start_time, "Shoprite_consumer_Load", "show_elapsed")


##########################################################################################################################################################
# GLOBAL SECTION
##########################################################################################################################################################
if __name__ == '__main__':
    main(sys.argv)
