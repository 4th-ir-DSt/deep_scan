#!/usr/bin/python
# !/usr/bin/sh

########################################################################
###
###     MODULE     : data_movement_processor.py
###
###     DESCRIPTION:
###
###     PARAMETERS : None
###
###     NO_OF_STEPS: 01
###
###     HISTORY:
###
###     Date        Name          CRF Number  Description
###     ----------  ------------  ----------  ----------------------------
###     16/04/2018  ZS       None        Initial Revision.
########################################################################


import warnings
import sys
warnings.filterwarnings("ignore")
import os
import re
import fnmatch
import subprocess
from subprocess import Popen, call
import psycopg2
import logging
import time
import datetime
import traceback
from def_gen_func import commonMethods
import boto
from boto.s3.key import Key
from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.connection import Location
from control_file_generator import generate_control_file

try:
    log_file = sys.argv[6]
except:
    print(''' The input parameter is not given properly, Please follow the below pattern --
              Python data_movement_processor.py cycl_time_id scen_id data_movement_type data_movement_id load_option log_file
    ''')
    sys.exit('Given input parameter in trigger script is not correct')
FORMAT = '%(message)s'
logging.basicConfig(filename=log_file, level=logging.INFO, format=FORMAT)

'''
Additions to the def_gen_func :
1. EC-2_data_file_path
2. INFA S3 KEYS
3. SFTP connection
4. EC2 dump command
5. RDS dump command
6. INFA S3 bucket
'''

class DataMovementProcessor:
    '''
    '''

    def __init__(self, arg_list):
        '''
        '''
        self.cmnobj = commonMethods()
        self.data_dict = self.cmnobj.getEnvironVariable()
        input_parm_dict = {'Cycle time id ': arg_list[1], 'Scenario ID': arg_list[2], 'Data Movement Type': arg_list[3],
                           'Data Movement ID': arg_list[4], 'Load Option': arg_list[5]}
        output_parm_dict = {'Target Location': 'S3 & Target Data Base'}
        src_nm = 'Data Movement Script'
        process_id = os.getpid()
        process_nm = sys.argv[0]
        self.EC2_file_tmp_path = self.data_dict["EC-2_data_file_path"]
        self.rds_schema_nm = self.data_dict["rds_schema_nm"]
        #self.access_key = self.data_dict["access_key"]
        #self.secret_key = self.data_dict["secret_key"]
        #self.us_ops_schema = self.data_dict["us_ops_schema"]
        self.log_con = self.cmnobj.generate_log_head(src_nm, process_nm, process_id, input_parm_dict, output_parm_dict)
        #self.sftpPass = self.data_dict["GILEAD_SFTP_PWD"]
        #self.sftpUser = self.data_dict["GILEAD_SFTP_USER"]
        #self.sftpURL = self.data_dict["GILEAD_SFTP_URL"]
        try:
            self.cycl_time_id = arg_list[1]
            self.scen_id = arg_list[2]
            self.data_mv_optn = arg_list[3].upper()
            self.data_mv_id = arg_list[4]
            self.proc_optn = arg_list[5].upper()
        except:
            print(''' The input parameter is not given properly, Please follow the below pattern-
                              Python data_movement_processor.py cycl_time_id scen_id data_movement_type data_movement_id load_option log_file
                          ''')
        status, self.orch_rds_conn = self.cmnobj.getRDSConnection("US")
        if status:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Connection to RDS established Successfully')
        else:
            logging.info('RDS connection failed, Please check the RDS server status')
            sys.exit(self.orch_rds_conn)
        self.orch_rds_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    def process(self):
        '''
        '''
        ################################################################################################################
        ###Fetch configuration for the given data movement ID/group ID
        ################################################################################################################

        status, tbl_config_dict, src_db, trg_db = self.getConfigurationInfo()
        print(('src_db : ' + src_db))
        print(('trg__db : ' + trg_db))
        print("********table_config***********",tbl_config_dict)
        if src_db != 'S3' :
            status, src_db_conn = self.getDBConnection("US", 'src_db', src_db)
        else :
            #status, src_db_conn = self.getConnectionToCrossS3(src_db)
            status, src_db_conn=True, None
        if not status:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Error in connecting the source database %s' %
                                                            list(tbl_config_dict.values())[1])
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            exit(1)
                #here if the trg_db is INFA_S3 then we have to establish the boto connection and return that
        if trg_db != 'S3' and trg_db != 'SFTP' and trg_db != 'FTP':
            status, trg_db_conn = self.getDBConnection("US", 'trg_db', trg_db)
        elif trg_db == 'S3' :
            #status, trg_db_conn = self.getConnectionToCrossS3(trg_db)
            print("target is S3")
        elif trg_db == 'FTP':
            print("target is FTP")
        else :
            #connection to SFTP not needed to be built before and will be handled in the sshpass command only
            status = True
            trg_db_conn = None
        if not status:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Error in connecting the target database %s' %
                                                            list(tbl_config_dict.values())[2])
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            exit(1)
                #currently we are not using the option of SYNC
        if self.proc_optn == 'OUTFLOW' or self.proc_optn == 'SYNC':
            status, s3_file_path = self.unloadDataOnS3(tbl_config_dict, src_db_conn, src_db, trg_db)

        if self.proc_optn == 'INFLOW' or self.proc_optn == 'SYNC':
            status = self.copyDataFromS3(tbl_config_dict, src_db_conn, trg_db_conn, src_db, trg_db)
        if status :
            print('The process completed successfully')
        else :
            print('Some error occured in the process, unsuccessful execution')
        logging.info(self.log_con)
        logging.info('\nJob %s ended at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
        logging.info('\n################################################################################')
        return True

    def getConnectionToCrossS3(self, src_db):
        try :
            conn = S3Connection(self.access_key, self.secret_key)
            print(('src_db_conn returned to getConnectionToCrossS3 is : ' + str(conn)))
            return True, conn
        except:
            print('Cannot connect to the cross region S3 location')
            e = str(traceback.format_exc())
            print(e)
            return False, conn

    def updateInserLog(self, option, update_value, config, file_nm):
        '''
        '''
        if option == 'insert':
            check_log_sql = ''' select sta from %s.cntl_data_mv_log where cycl_time_id = %s and scen_id = %s and data_mv_id = %s;''' % (
                self.rds_schema_nm, self.cycl_time_id, self.scen_id, config[12])
            check_log_sql_obj = self.executeQuery(check_log_sql, self.orch_rds_conn)

            if len(check_log_sql_obj.fetchall()) > 0:
                sql_txt = '''update %s.cntl_data_mv_log set sta= %s, modf_dt = now() where cycl_time_id = %s and scen_id = %s
                            and data_mv_id = %s;''' % (self.rds_schema_nm, 2, self.cycl_time_id, self.scen_id, config[12])
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, '%s in log table for data movement id %s' % (
                option, config[12]))
            else:
                sql_txt = ''' INSERT INTO %s.cntl_data_mv_log (data_mv_id, tier_2_clfsn,sta,cycl_time_id,scen_id,unld_file_nm,inrt_dt,intr_by,modf_dt,modf_by)
                          VALUES (%s, '%s', 0, %s,%s,'%s',now(),'mdmetl01',now(),'mdmetl01');''' % (self.rds_schema_nm,
                config[12], config[2], self.cycl_time_id, self.scen_id, file_nm)
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, '%s in log table for data movement id %s' % (
                option, config[12]))

        elif option == 'update':
            sql_txt = '''update %s.cntl_data_mv_log set sta= %s, modf_dt = now() where cycl_time_id = %s and scen_id = %s
                          and data_mv_id = %s;''' % (self.rds_schema_nm, update_value, self.cycl_time_id, self.scen_id, config[12])

        elif option == 'update_trgt_tbl_cnt':
            sql_txt = '''update %s.cntl_data_mv_log set trgt_rec_cnt = %s, modf_dt = now() where cycl_time_id = %s and scen_id = %s
                          and data_mv_id = %s;''' % (self.rds_schema_nm, update_value, self.cycl_time_id, self.scen_id, config[12])

        elif option == 'update_src_tbl_cnt':
            sql_txt = '''update %s.cntl_data_mv_log set src_rec_cnt = %s, modf_dt = now() where cycl_time_id = %s and scen_id = %s
                          and data_mv_id = %s;''' % (self.rds_schema_nm, update_value, self.cycl_time_id, self.scen_id, config[12])


        status = self.executeQuery(sql_txt, self.orch_rds_conn)
        db_conn_cur = self.orch_rds_conn.cursor()
        db_conn_cur.execute('commit')
        return True

    def copyDataFromS3(self, tbl_config_dict, src_db_conn, trg_db_conn, src_db, trg_db):
        '''
        '''
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Inside the copy data from S3 function')
        if trg_db == 'RDS':
            for db_mv_id, config in tbl_config_dict.items():
                s3_path = config[5]
                print("************",s3_path)
                file_nm = s3_path.split('/')[-1]
                ec2_file_path = self.data_dict["EC-2_data_file_path"]+config[9] + str(self.cycl_time_id)+'/'+str(self.scen_id)+'/'
                #here we will use the function where the boto implementation of the file tarnsfer from the inforamtica s3 to ec2
                status = self.getFileFromCrossRegionS3ToEC2(config,src_db_conn ,ec2_file_path, file_nm , s3_path)
                #ec2_file_path, ec2_file_nm, s3_file = self.copyFileOnEC2(s3_file, src_db_conn, config, tab_nm)
                if not status :
                    sys.exit('The process faced some issue while fetching the file form cross region S3')
                status = self.copyDataFromEC2ToRDS(config, ec2_file_path , file_nm, trg_db_conn)
                if not status:
                    sys.exit('The data was not copied to the RDS table')
                #status, trgt_rec_cnt = self.getTblRecCnt('trgt_tbl_cnt', trg_db_conn, config)
                status = self.copyFileFromEC2ToS3 (config, ec2_file_path, file_nm)
                if not status :
                    sys.exit('The data was not backed up on S3, error occured')
                status = self.deleteEC_2File(ec2_file_path, file_nm, config)
                if not status :
                    sys.exit('The file deletion from EC2 caused an issue')
                status = self.updateInserLog('update', 1, config,file_nm)
                #status = self.updateInserLog('update_trgt_tbl_cnt', trgt_rec_cnt, config, '')
                #status = self.checkFinalCnt(config)

        #elif trg_db == 'RS_AMER_ETL_01':
        #    for db_mv_id, config in tbl_config_dict.iteritems():
        #        s3_file = self.getFileDetailFromLog(db_mv_id)
        #        log_con = self.copyDataInTable(config, src_db_conn, trg_db_conn, s3_file,src_db,trg_db)
        #        status, trgt_rec_cnt = self.getTblRecCnt('trgt_tbl_cnt', trg_db_conn, config)
        #        status = self.updateInserLog('update', 1, config, s3_file)
        #        status = self.updateInserLog('update_trgt_tbl_cnt', trgt_rec_cnt, config, '')
        #        status = self.checkFinalCnt(config)
        #
                #elif trg_db == 'RS_PII_AMER_ETL_01':
        #    for db_mv_id, config in tbl_config_dict.iteritems():
        #        s3_file = self.getFileDetailFromLog(db_mv_id)
        #        log_con = self.copyDataInTable(config, src_db_conn, trg_db_conn, s3_file,src_db,trg_db)
        #        status, trgt_rec_cnt = self.getTblRecCnt('trgt_tbl_cnt', trg_db_conn, config)
        #        status = self.updateInserLog('update', 1, config, s3_file)
        #        status = self.updateInserLog('update_trgt_tbl_cnt', trgt_rec_cnt, config, '')
        #        status = self.checkFinalCnt(config)

        return True

    def copyFileFromEC2ToS3(self, config, ec2_file_path, file_nm):
        ec2_file_path = ec2_file_path + file_nm
        s3_file_path = 's3://'+ config[8] + str(self.cycl_time_id) +'/'+str(self.scen_id) + '/'
        os.system('export TZ=Asia/Calcutta')
        # date = os.system('date +"%Y%m%d%H%M%S"')
        #s3_arc_file_nm = file_nm + '_'+ date
        now = datetime.datetime.today().strftime('%Y%m%d%H%M%S')
        s3_file_path_arc = 's3://' + config[8] + str(file_nm).replace('.txt', '') + '_' + str(now) + '.txt'
        # s3_file_path_arc = 's3://'+ config[8]
        cpy_cmd_arc = '''aws s3 cp %s %s'''%(ec2_file_path, s3_file_path_arc)
        subprocess.call(cpy_cmd_arc, shell=True)
        try :
            cpy_cmd = '''aws s3 cp %s %s'''%(ec2_file_path, s3_file_path)
            status = subprocess.call(cpy_cmd, shell=True)
            if status == 0 :
                print(('File has been archived to s3 location : '+s3_file_path))
                return True
            else :
                raise Exception('An error occured while copying data to AWS S3')
        except :
            e = str(traceback.format_exc())
            print(e)
            print('Some issue occured with the file archiving to S3')
            return False


    def copyDataFromEC2ToRDS(self, config, ec2_file_path, file_nm, trg_db_conn):
        '''
        '''

        #here the check for the population to the base table should be agnostic of cycl time id and scen id and the process would need some tweaks
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Inside the copy data from EC-2 to RDS function')
        tbl_nm = config[6] + '.' + config[7]  #target schema + target table
        ec2_file_nm = ec2_file_path + file_nm

        trunc_state = False
        print(('value of config 11 is : ',config[11]))
        if config[11] == 1:
            trun_sql = ''' truncate table %s''' % tbl_nm
            config_list_obj = self.executeQuery(trun_sql, trg_db_conn)

            #db_conn_cur = self.orch_rds_conn.cursor()
            #config_list_obj.fetchall()
            #print config_list_obj.rowcount
            config_list_obj.execute('commit')

            trunc_state = True
        else:
            check_cycl_time_id_data_sql = '''select count(1) from %s '''%(tbl_nm)
            #check_cycl_time_id_data_sql = '''select count(1) from %s where cycl_time_id = %s and scen_id = %s'''%(tbl_nm, self.cycl_time_id, self.scen_id)
            check_cycl_time_id_data_sql_obj = self.executeQuery(check_cycl_time_id_data_sql, trg_db_conn)
            data_cnt = check_cycl_time_id_data_sql_obj.fetchall()[0][0]
            data_cnt = 0 #data_cnt = 0 will need to be removed if we start to filter on cycl and scen_id in the target table
            trunc_state = True
            if data_cnt != 0:
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Unload data from EC-2 to S3 CMD failed') # what does this command mean ?? , and the error message shoud be different here
                logging.info(self.log_con)
                logging.info('\nJob %s failed at %s', sys.argv[0],
                             datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
                sys.exit('Total record count %s is already present in table %s for given cycle time id'%(data_cnt, tbl_nm))
        if trunc_state:
            try:
                #status, tmp_tbl_nm = self.createTempTable(trg_db_conn, config, tbl_nm)
                dump_data_rds_cmd = self.data_dict["RDS_DUMP_CMD_TRANS"] % (tbl_nm,ec2_file_nm)
                print(('dump_data_to RDS is ',dump_data_rds_cmd))
                self.log_con = self.cmnobj.generate_log_for_src_run(self.log_con, dump_data_rds_cmd)
                status = os.system(dump_data_rds_cmd)
                print(dump_data_rds_cmd)
                if status != 0 :
                    raise Exception('Copy to RDS from ec2 for file failed')
                #tbl_nm_obj = self.executeQuery('select count(1) from %s'%tbl_nm, trg_db_conn)
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Copy data from Ec-2 to RDS for table %s is complete'%tbl_nm)
                #status = self.loadDataMainTbl(config, tmp_tbl_nm, trg_db_conn)
                return True
            except:
                e = str(traceback.format_exc())
                print (e)
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Copy data from Ec-2 to RDS is failed, error %s'%e)
                logging.info(self.log_con)
                logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
                sys.exit('Copy data from Ec-2 to RDS is failed')

    def getFileSize(self, ec2_file_path, file_nm):
        ec2_file = ec2_file_path + file_nm
        print("Ec2 file name", ec2_file)
        cmd = "ls -ltr %s | awk '{print $5}' "%(ec2_file)
        print(cmd)
        try:
            status = subprocess.Popen(cmd ,stdout = subprocess.PIPE,stderr = subprocess.PIPE,  shell = 'TRUE')
            status_out = status.stdout.read()
            status_out = status_out.decode("UTF-8")
            status_err = status.stderr.read()
            status_err = status_err.decode("UTF-8")
            status.communicate()
            if 'not found' in status_err :
                raise Exception('File not present on EC2 location')
            size = status_out.split('\n')[0:-1]
            if len(size) >1 :
                raise Exception('same file present as duplicate on the ec2 location')
            return size[0]
        except :
            e = str(traceback.format_exc())
            print(e)
            sys.exit('Issue in getting size of ec2 file')
    def getRowCount(self,ec2_file_path, file_nm):
        full_file_nm = ec2_file_path + file_nm
        cmd = "wc -l %s "%(full_file_nm)
        print(cmd)
        try:
            status = subprocess.Popen(cmd ,stdout = subprocess.PIPE,stderr = subprocess.PIPE,  shell = 'TRUE')
            status_out = status.stdout.read()
            status_out = status_out.decode("UTF-8")
            status_err = status.stderr.read()
            status.communicate()
            rowcount = status_out.split(' ')[0]
            print(('row count is '+ rowcount))
            return rowcount
        except:
            e = str(traceback.format_exc())
            print('error in getting row count of file')
    def unloadDataOnS3(self, tbl_config_dict, src_db_conn, src_db, trg_db):
        '''
        '''
        #check how to handle the trg_db_conn when we have to transfer file from RDS to SFTP
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Inside the S3 unload function')
        s3_file_path = ''
        if src_db == 'RDS' and trg_db == 'S3':
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Input source DB %s and target is %s' % (src_db,trg_db))
            for db_mv_id, config in tbl_config_dict.items():
                if config[7].__contains__("$cycl_time_id$"):
                    file_nm = config[5] + "_" + str(self.cycl_time_id) + '.txt'  # src_obj_nm.txt , table_name.txt
                    status = self.updateInserLog('insert', 0, config, file_nm)
                    status, ec2_file_path, file_nm = self.dumpDataOnEC2(config, file_nm)
                else:
                    file_nm = config[7] + '.txt'  # src_obj_nm.txt , table_name.txt
                    status = self.updateInserLog('insert', 0, config, file_nm)
                    status, ec2_file_path, file_nm = self.dumpDataOnEC2(config, file_nm)
                if not status :
                    sys.exit('Error in dumping data from RDS to EC2, Exiting the process now')
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'EC-2 file path %s' %ec2_file_path)
                status = self.updateInserLog('update', 2, config, file_nm)
                #here we will add the function to move the file from ec2 to informatica S3 using boto
                print(ec2_file_path)
                print(file_nm)
                rows = self.getRowCount(ec2_file_path, file_nm)
                print('ROWS ISSSSSSSSSSSSSSSSSSSSSSS',rows)
                rowcount=int(rows)
                print('ROWCOUNT',rowcount,type(rowcount))
                if rowcount > 1:
                        size = self.getFileSize(ec2_file_path, file_nm)
                        print("Size is",size)
                        if int(size) < 5368709120:
                                print('Calling copy for small file')
                                status, s3_file_path = self.moveFromEC2ToCrossRegionS3(config, ec2_file_path, file_nm)
                        else :
                                print('Calling copy for larger file')
                                status , s3_file_path = self.moveLargerFileToCrossRegionS3(config, ec2_file_path, file_nm,trg_db_conn)
                        if not status :
                                sys.exit('The file could not be copied to cross region S3 successfully, please retry ')
                        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Informatica S3 file path %s' %s3_file_path)
                        status = self.deleteEC_2File(ec2_file_path, file_nm, config)
                        if not status :
                                sys.exit('The file deletion caused some error')
                        if self.proc_optn == 'OUTFLOW':
                                status = self.updateInserLog('update', 1, config, file_nm)
                else:
                        print('Exiting as file has only header')
                        status = self.updateInserLog('update', 1, config, file_nm)

        elif src_db == 'RDS ETL' and trg_db == 'SFTP' :
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Input source DB %s and target is %s' % (src_db,trg_db))
            for db_mv_id, config in tbl_config_dict.items():
                if config[7].__contains__("$cycl_time_id$"):
                    file_nm = config[5] + "_" + str(self.cycl_time_id) + '.csv'  # src_obj_nm.txt , table_name.txt
                    status = self.updateInserLog('insert', 0, config, file_nm)
                    status, ec2_file_path, file_nm = self.dumpDataOnEC2(config, file_nm)
                else:
                    file_nm = config[5] + '.csv'  # src_obj_nm.txt , table_name.txt
                    status = self.updateInserLog('insert', 0, config, file_nm)
                    status, ec2_file_path, file_nm = self.dumpDataOnEC2(config, file_nm)
                if not status :
                    sys.exit('Error in dumping data from RDS to EC2, exiting the process now')
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'EC-2 file path %s' %ec2_file_path)
                status = self.updateInserLog('update', 2, config, file_nm)
                status = self.moveFromEC2ToSFTP(config, ec2_file_path, file_nm)
                #status, s3_file_path = self.unloadFromEC2(config, ec2_file_path, file_nm, src_db_conn)
                if not status:
                    sys.exit('The file was not successfully backed up to SFTP, exiting process')
                status = self.copyFileFromEC2ToS3(config, ec2_file_path, file_nm)
                if not status :
                    sys.exit('The process faced some issue while archiving the file on S3')
                #status, src_rec_cnt = self.getTblRecCnt('src_tbl_cnt', src_db_conn, config)
                #status = self.updateInserLog('update_src_tbl_cnt', src_rec_cnt, config, '')
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Informatica S3 file path %s' %s3_file_path)
                status = self.deleteEC_2File(ec2_file_path, file_nm, config)
                if not status :
                    sys.exit('The file deletion caused some error')
                if self.proc_optn == 'OUTFLOW':
                    status = self.updateInserLog('update', 1, config, file_nm)
        elif src_db == 'S3' and trg_db == 'FTP':
            exitStatus = True
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Input source DB %s and target is %s' % (src_db,trg_db))
            for db_mv_id, config in tbl_config_dict.items():
                if config[7].__contains__("$cycl_time_id$"):
                    file_nm = config[5] + "_" + str(self.cycl_time_id) + '.csv.gz'
                    status = self.updateInserLog('insert', 0, config, file_nm)
                    status, ec2_file_path, file_nm = self.copyFileS3toEC2(config, file_nm)
                else:
                    file_nm = config[5] + '.csv.gz'
                    status = self.updateInserLog('insert', 0, config, file_nm)
                    status, ec2_file_path, file_nm = self.copyFileS3toEC2(config, file_nm)

                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Data File Name is :'+file_nm)
                if not status:
                    exitStatus = False
                    sys.exit('Error in dumping data from S3 to EC2, exiting the process now')
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'EC-2 file path %s' %ec2_file_path)
                status = self.updateInserLog('update', 2, config, file_nm)
                file_upload_status = self.moveFromEC2ToFTP(config, ec2_file_path, file_nm)
                if not file_upload_status:
                    exitStatus = False
                    status = self.updateInserLog('update', 3, config, file_nm)
                    self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Unable to upload file to FTP!')
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'File Uploaded to FTP successfully !!')
                # check if control file is required or not !!
                if config[16] is not None and config[16].strip() != '':
                    self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Generating control file')
                    cntl_file_name = generate_control_file(ec2_file_path, file_nm, file_upload_status, config, self.data_dict)
                    self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Control file generation completed')
                    cntl_file_status = self.moveFromEC2ToFTP(config, ec2_file_path, cntl_file_name)
                    if cntl_file_status:
                        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Control file moved to FTP successfully !')
                    else:
                        exitStatus = False
                        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Control file movement to FTP FAILED !')

                   # ctl_del_status = self.deleteEC_2File(ec2_file_path, cntl_file_name, config)
                    ctl_del_status = True
                    if not ctl_del_status:
                        exitStatus = False
                        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Unable to delete [ '+cntl_file_name+' ] from EC2 Machine')
                else:
                    self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Control file is not required')

                #file_del_status = self.deleteEC_2File(ec2_file_path, file_nm, config)
                file_del_status = True
                if not file_del_status:
                    exitStatus = False
                    self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Unable to delete [ '+file_nm+' ] from EC2 Machine')

                if not exitStatus:
                    logging.info(self.log_con)
                    sys.exit('There is some issue in uploading file to FTP server.')

        elif src_db == 'RS' and trg_db == 'S3':
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Input source DB %s and target is %s' % (src_db, trg_db))
            for db_mv_id, config in tbl_config_dict.items():
                if config[10]:  # getting the sql for the table to be unloaded on S3
                    sql_txt = self.getSQLQuery(config[10])
                    data_dump_sql = sql_txt.replace('$CYCL_TIME_ID$', self.cycl_time_id).replace('$cycl_time_id$',self.cycl_time_id)
                    data_dump_sql = data_dump_sql.replace('$SCEN_ID$', self.scen_id).replace('$scen_id$', self.scen_id)
                else:
                    self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'SQL ID is not present')
                    # status = self.sendFaliureEmail(config[5])
                    logging.info(self.log_con)
                    status = self.updateInserLog('update', 3, config, '')
                    sys.exit('SQL ID is not present')
                if config[7].__contains__("$cycl_time_id$"):
                    file_nm = config[5] + "_" + str(self.cycl_time_id) + '.txt'  # src_obj_nm.txt , table_name.txt
                    status = self.updateInserLog('insert', 0, config, file_nm)
                else:
                    file_nm = config[7] + '.txt'  # src_obj_nm.txt , table_name.txt
                    status = self.updateInserLog('insert', 0, config, file_nm)
                s3_path = config[8]
                print((config[7]))
                s3_path_file_nm = s3_path + file_nm
                from_s3 = 's3://' + s3_path_file_nm
                f_s3 = 's3://' +  s3_path

                #print(config[14])

                misc_copy_opt=config[14]
                if misc_copy_opt == 'NA' :
                        misc_copy_opt = ''

                print("88888888888888888888888888")
                print((config[19]))

                if config[19]is None or config[19].strip().lower() == 'null' or config[19].strip() == '':

                    if config[15] is None or config[15]=='NULL' or config[15]== '':
                        from_s3=from_s3.replace('txt','csv')
                        unload_cmd = ''' Unload ('%s') to '%s' CREDENTIALS 'aws_iam_role=%s' gzip ALLOWOVERWRITE %s ''' % (data_dump_sql, from_s3, self.data_dict['rs_rolename'],misc_copy_opt)
                    else:
                        #unload_cmd = ''' Unload ('%s') to '%s' CREDENTIALS 'aws_iam_role=%s' delimiter '%s' gzip ALLOWOVERWRITE ADDQUOTES ESCAPE %s ''' % (data_dump_sql, from_s3, self.data_dict['rs_rolename'], config[15], misc_copy_opt)
                        unload_cmd = ''' Unload ('%s') to '%s' CREDENTIALS 'aws_iam_role=%s' delimiter '%s' gzip ALLOWOVERWRITE ESCAPE %s ''' % (data_dump_sql, from_s3, self.data_dict['rs_rolename'], config[15], misc_copy_opt)
                    self.log_con = self.cmnobj.generate_log_for_src_run(self.log_con, unload_cmd)
                    status = self.updateInserLog('update', 2, config, file_nm)
                    tbl_nm = str(config[4])+'.'+str(config[5])
                    print(tbl_nm)
                    if config[4].lower()=='comm_stg_v' or config[4].lower()=='comm_dw' or config[4].lower()=='comm_dw_v' or config[5].lower().startswith('stg_'):
                            row_count_query = ("select count(1) from %s" %(tbl_nm))
                    else:
                            row_count_query = data_dump_sql.replace("''","'")
                            print(row_count_query)
                    rows = self.executeQuery(row_count_query, src_db_conn)
                    result=rows.fetchall()
                    if len(result) is 0:
                            rowcount = 0
                    else:
                            print(result)
                            rowcount = result[0][0]
                            rowcount = int(rowcount)
                            print('ROWCOUNT TYPECASTED TO INTEGER LINE 569')
                            
                    try:
                        if rowcount > 0:
                            config_list_obj = self.executeQuery(unload_cmd, src_db_conn)
                            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'File %s unload on S3 started' % s3_file_path)
                            status = self.updateInserLog('update', 1, config, file_nm)
                            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Unload file on S3 is complete')
                            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Now renaming the unloaded file')
                            try:
                                if misc_copy_opt.__contains__("PARALLEL OFF"):
                                    self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Renaming of file started')
                                    unload_file_nm=from_s3+'000.gz'
                                    rename_cmd = "aws s3 mv %s %s --sse" % (from_s3 + '000.gz', from_s3+'.gz')
                                    print(rename_cmd)
                                    status = subprocess.call(rename_cmd, shell=True)
                                    if status == 0:
                                        print(('File has been renamed to : ' + file_nm+'.gz'))
                                    else:
                                        raise Exception('An error occurred while renaming file on AWS S3')
                                else:
                                    print('Multipart Unload Done')
                                    self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Multipart Unload Done')
                            except:
                                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Some Isssue with renaming of file')
                                logging.info(self.log_con)
                                logging.info('\nJob %s failed at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
                                print('Rename file on S3 failed')
                                sys.exit('Rename file on S3 failed')
                        else:
                            print("Exiting as file has only header")
                            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'No records found in %s' % data_dump_sql)
                    except:
                        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Dump data from RS to EC-2 CMD failed')
                        logging.info(self.log_con)
                        status = self.updateInserLog('update', 3, config, '')
                        logging.info('\nJob %s failed at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
                        sys.exit('Unload data from RS to S3 failed')

                else:
                    publish_path = config[19]
                    publish_path_file_nm = publish_path + file_nm
                    pub_s3 = 's3://' + publish_path_file_nm
                    p_s3 =  's3://' + publish_path
                    print("5555555555555555555555555555555")

                    if config[15] is None or config[15]=='NULL' or config[15]== '':
                        from_s3=from_s3.replace('txt','csv')
                        unload_cmd = ''' Unload ('%s') to '%s' CREDENTIALS 'aws_iam_role=%s' gzip ALLOWOVERWRITE %s ''' % (data_dump_sql, from_s3, self.data_dict['rs_rolename'],misc_copy_opt)
                        #publish_cmd = ''' Unload ('%s') to '%s' CREDENTIALS 'aws_iam_role=%s' gzip ALLOWOVERWRITE %s ''' % (data_dump_sql, pub_s3, self.data_dict['rs_rolename'],misc_copy_opt)
                    else:
                        unload_cmd = ''' Unload ('%s') to '%s' CREDENTIALS 'aws_iam_role=%s' delimiter '%s' gzip ALLOWOVERWRITE ADDQUOTES ESCAPE %s ''' % (data_dump_sql, from_s3, self.data_dict['rs_rolename'], config[15], misc_copy_opt)
                        #publish_cmd = ''' Unload ('%s') to '%s' CREDENTIALS 'aws_iam_role=%s' delimiter '%s' gzip ALLOWOVERWRITE ADDQUOTES ESCAPE %s ''' % (data_dump_sql, pub_s3, self.data_dict['rs_rolename'], config[15], misc_copy_opt)
                    self.log_con = self.cmnobj.generate_log_for_src_run(self.log_con, unload_cmd)
                    status = self.updateInserLog('update', 2, config, file_nm)
                    tbl_nm = str(config[4])+'.'+str(config[5])
                    print(tbl_nm)
                    if config[4].lower()=='comm_stg_v' or config[4].lower()=='comm_dw' or config[4].lower()=='comm_dw_v' or config[5].lower().startswith('stg_'):
                            row_count_query = ("select count(1) from %s" %(tbl_nm))
                    else:
                            row_count_query = data_dump_sql.replace("''","'")
                            print(row_count_query)
                    rows = self.executeQuery(row_count_query, src_db_conn)
                    result=rows.fetchall()
                    if len(result) is 0:
                            rowcount = 0
                    else:
                            print(result)
                            rowcount = result[0][0]
                    try:
                        if rowcount > 0:
                            rm_path = 's3://' + config[19]
                            rm_cmd = '''aws s3 rm %s --recursive''' % (rm_path)
                            print(rm_cmd)
                            os.system(rm_cmd)
                            config_list_obj = self.executeQuery(unload_cmd, src_db_conn)
                            #config_list_obj_pub = self.executeQuery(publish_cmd, src_db_conn)
                            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'File %s unload on S3 started' % s3_file_path)
                            status = self.updateInserLog('update', 1, config, file_nm)
                            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Unload file on S3 is complete')
                            cpy_cmd_arc = '''aws s3 sync %s %s''' % (f_s3, p_s3)
                            subprocess.call(cpy_cmd_arc, shell=True)
                            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Now renaming the unloaded file')
                            try:
                                if misc_copy_opt.__contains__("PARALLEL OFF"):
                                    self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Renaming of file started')
                                    unload_file_nm=from_s3+'000.gz'
                                    rename_cmd = "aws s3 mv %s %s --sse" % (from_s3 + '000.gz', from_s3+'.gz')
                                    pub_rename_cmd = "aws s3 mv %s %s --sse" % (pub_s3 + '000.gz', pub_s3+'.gz')
                                    print(rename_cmd)
                                    status = subprocess.call(rename_cmd, shell=True)
                                    #subprocess.call(cpy_cmd_arc, shell=True)
                                    if status == 0:
                                        print(('Processed File has been renamed to : ' + file_nm+'.gz'))
                                    else:
                                        raise Exception('An error occurred while renaming processed file on AWS S3')
                                    sta = subprocess.call(pub_rename_cmd, shell=True)
                                    if sta == 0:
                                        print(('Published File has been renamed to : ' + file_nm+'.gz'))
                                    else:
                                        raise Exception('An error occurred while renaming published file on AWS S3')
                                else:
                                    print('Multipart Unload Done')
                                    self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Multipart Unload Done')
                            except:
                                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Some Isssue with renaming of file')
                                logging.info(self.log_con)
                                logging.info('\nJob %s failed at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
                                print('Rename file on S3 failed')
                                sys.exit('Rename file on S3 failed')
                        else:
                            print("Exiting as file has only header")
                            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'No records found in %s' % data_dump_sql)
                    except:
                        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Dump data from RS to EC-2 CMD failed')
                        logging.info(self.log_con)
                        status = self.updateInserLog('update', 3, config, '')
                        logging.info('\nJob %s failed at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
                        sys.exit('Unload data from RS to S3 failed')


        return True, s3_file_path

    def copyFileS3toEC2 (self, config, file_nm):
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Inside the dump data on ec2 from S3 function')
        print(('Config is:'+str(config)))
        #ec2_file_path = config[9] + str(self.cycl_time_id) + '/' + str(self.scen_id) + '/'
        ec2_file_path = config[9] if config[9].endswith('/') else config[9] + '/'
        if config[0] == 'S3':
            copy_cmd = 'aws s3 cp s3://' + config[8] + file_nm + ' ' + ec2_file_path
            self.log_con = self.cmnobj.generate_log_for_src_run(self.log_con, copy_cmd)
            try:
                print(('Executing copy command: '+copy_cmd))
                status = subprocess.call(copy_cmd, shell=True)
            except:
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Dump data from S3 to EC-2 CMD failed')
                status = self.updateInserLog('update', 3, config, '')
                logging.info('\nJob %s failed at %s', sys.argv[0],
                             datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
                sys.exit('Dump data from S3 to EC-2 CMD failed')
            if status == 0:
                msg_str = "Dump data from S3 to EC-2 is complete for table %s" % config[5]
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, msg_str)
                return True, ec2_file_path, file_nm
            else:
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Dump data from RDS to EC-2 CMD failed')
                logging.info(self.log_con)
                status = self.updateInserLog('update', 3, config, '')
                logging.info('\nJob %s failed at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
                sys.exit('Dump data from S3 to EC-2 CMD failed')
        else:
                sys.exit('source is not S3')

    def moveFromEC2ToFTP(self, config, ec2_file_path, file_nm):
        
        url = config[6]
        print(("------------",url))
        if file_nm.endswith('.ctl'):
            dstn_url = config[17]
            url = dstn_url.split('/')[2]
            cntl_destination = '/'+('/'.join(dstn_url.split('/')[-3:-1]))+'/'
            print(("$$$$$$$",cntl_destination))
            ec2_file_path = ec2_file_path + file_nm
            ftp_cmd = 'lftp -c  "open -u %s,%s %s; set ftp:ssl-force true;set ftp:ssl-protect-data true;set ssl:verify-certificate true;set ssl:ca-file $HOME/cacerts/ftps.veeva;put -O %s %s"'%(self.data_dict[config[18]+'_username'], self.data_dict[config[18]+'_password'],url,cntl_destination,ec2_file_path)
            #ftp_cmd="touch 1"
            print(('Executing FTP cmd [ '+ftp_cmd+' ]'))
        else:
            dstn_url = config[17]
            print(("#########",dstn_url))
            print(("*******",url))
            url_1 = dstn_url.split('/')[2]
            data_destination = '/'+url.split('/')[-2]+'/'
            #data_destination=('/'+('/'.join(dstn_url.split('/')[3::])))
            print(("%%%%%%%%%%%%%",data_destination))
            ec2_file_path = ec2_file_path + file_nm
            ftp_cmd = 'lftp -c "open -u %s,%s %s; set ftp:ssl-force true;set ftp:ssl-protect-data true;set ssl:verify-certificate true;set ssl:ca-file $HOME/cacerts/ftps.veeva.2;put -O %s %s"'%(self.data_dict[config[18]+'_username'], self.data_dict[config[18]+'_password'],url_1,data_destination,ec2_file_path)
            print(('Executing FTP cmd [ '+ftp_cmd+' ]'))

        retries = 0
        while retries < 3:
            try :
                status = subprocess.Popen(ftp_cmd,stdout = subprocess.PIPE,stderr = subprocess.PIPE, shell = 'TRUE')
                #handle the stderr part here
                std_out = status.stdout.read()
                std_err = status.stderr.read()
                out = std_out + std_err
                out = out.decode("UTF-8")
                status.communicate()
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'FTP result: '+out)
                if 'not' in out or 'No such' in out  or 'Access denied' in out:
                    self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'File not copied to FTP !')
                    retries = retries + 1
                    continue
                retries = 4
                return True
            except :
                e = str(traceback.format_exc())
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Process failed to transfer the file on FTP location'+str(e))
                retries = retries + 1
        if retries == 3:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Not able to move file to SFTP post retries also')
        return False

    '''def moveFromEC2ToSFTP(self, config, ec2_file_path, file_nm):
        file_complete_path_sftp = config[7].replace('$cycl_time_id$', str(self.cycl_time_id))
        ec2_file_path = ec2_file_path + file_nm
        retries = 0
        while retries < 3:
            try :
                status = subprocess.Popen("/usr/bin/sshpass -p %s sftp %s@%s <<< $'put %s %s'"%(self.sftpPass,self.sftpUser,self.sftpURL, ec2_file_path, file_complete_path_sftp),stdout = subprocess.PIPE,stderr = subprocess.PIPE, shell = 'TRUE')
                #handle the stderr part here
                std_out = status.stdout.read()
                std_err = status.stderr.read()
                status.communicate()
                print std_out
                print std_err
                if 'not' in std_err or 'No such' in std_err  or 'Permission denied' in std_err :
                    print('File not copied to SFTP')
                    retries =  retries + 1
                    continue
                retries = 4
                return True
            except :
                e = str(traceback.format_exc())
                print(e)
                print('Process failed to transfer the file on SFTP location')
                retries = retries + 1
        if retries == 3:
            print('Not able to move file to SFTP post retries also')
        return False'''

    def deleteEC_2File(self, ec2_file_path, file_nm, config):
        '''
        '''
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Inside the delete file from EC-2 function')
        #ec2_file = self.EC2_file_tmp_path + config[9] + config[7] + '/'
        f_path = ec2_file_path + file_nm
        if os.path.isdir(ec2_file_path):
            try:
                os.remove(f_path)
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Delete file %s from EC-2 is complete' % ec2_file_path)
                return True
            except:
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'File %s deleted from EC-2 is failed' % ec2_file_path)
                #status = self.sendFaliureEmail(config[5])
                status = self.updateInserLog('update', 3, config, file_nm)
                logging.info(self.log_con)
                logging.info('\nJob %s failed at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
                sys.exit('File deletion from EC-2 failed')
        return False


    def dumpDataOnEC2(self, config, file_nm):
        '''
        '''
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Inside the dump data on ec2 function')
        if config[10]: #getting the sql for the table to be unloaded on S3
            sql_txt = self.getSQLQuery(config[10])
            data_dump_sql = sql_txt.replace('$CYCL_TIME_ID$', self.cycl_time_id).replace('$cycl_time_id$', self.cycl_time_id)
            data_dump_sql = data_dump_sql.replace('$SCEN_ID$', self.scen_id).replace('$scen_id$', self.scen_id)
        else:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'SQL ID is not present')
            #status = self.sendFaliureEmail(config[5])
            logging.info(self.log_con)
            status = self.updateInserLog('update', 3, config, '')
            sys.exit('SQL ID is not present')
        #ec2_file_path = self.EC2_file_tmp_path + config[9] + str(self.cycl_time_id)+'/'+str(self.scen_id)+'/' #constant_path/ec2_path_in_table/CYCL/SCEN/
        ec2_file_path =  config[9] + str(self.cycl_time_id)+'/'+str(self.scen_id)+'/'
        status = self.checkDir(ec2_file_path)
        ec2_file_nm = ec2_file_path +  file_nm
        print(('ec2_file_path' + ec2_file_path))
        print(file_nm)
        print(config)
        print(('ec2_file_name is ' + ec2_file_nm))
        print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%')
        dum_cmd = self.data_dict["EC-2_DUMP_CMD_META"]%(data_dump_sql, ec2_file_nm)#we need to check the way we want to use the data delimiter here # look at the copy to command here and also the diff variable required for it or not
        self.log_con = self.cmnobj.generate_log_for_src_run(self.log_con, dum_cmd)
        print(dum_cmd)
        try:
            status = subprocess.call(dum_cmd, shell=True)
        except:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Dump data from RDS to EC-2 CMD failed')
            logging.info(self.log_con)
            status = self.updateInserLog('update', 3, config, '')
            logging.info('\nJob %s failed at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('Dump data from RDS to EC-2 CMD failed')

        if status == 0:
            msg_str = "Dump data from RDS to EC-2 is complete for table %s" % config[5]
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, msg_str)
            return True, ec2_file_path, file_nm
        else:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Dump data from RDS to EC-2 CMD failed')
            logging.info(self.log_con)
            status = self.updateInserLog('update', 3, config, '')
            logging.info('\nJob %s failed at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('Dump data from RDS to EC-2 CMD failed')

    def checkDir(self, ec2_file_path):
        '''
        '''
        dir_create_sql = '''mkdir -p %s'''%ec2_file_path
        #print dir_create_sql
        try:
            status = subprocess.call(dir_create_sql, shell=True)
            return True
        except:
            e = str(traceback.format_exc())
            logging.info('\nJob %s failed at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('directory creation is failed')

    def splitLargeFile(self, ec2_file, splits):
        cmd  = 'split -n %d -d %s %s'%(splits, ec2_file, ec2_file)
        status = subprocess.call(cmd , shell= True)
        if status != 0:
           raise Exception('Issue while splitting the file')
        else :
            file_list = []
            i = 0
            while i < splits :
                file_list.append(str(ec2_file)+'0'+str(i))
                i = i+1
        print(file_list)
        return file_list


    def moveLargerFileToCrossRegionS3(self , config, ec2_file_path, file_nm,trg_db_conn):
        #supposing that trg_db_conn is the boto object connected to the required
        #enter the value of the bucket in the def gen funci
        ec2_file_complete = ec2_file_path + file_nm
        splits =4
        i =1
        source_file_list = self.splitLargeFile(ec2_file_complete, splits)
        retries = 0
        while retries < 3 :
            try :
                bucket = trg_db_conn.get_bucket(self.data_dict['s3_bucket'])
                if config[19] is None or config[19].strip().lower() == 'null' or config[19].strip() == '':
                    s3_path = config[7]  #trg_obj_nm will be used which will  have the path leaving the bucket name on INFA S3
                    k = Key(bucket)
                    print('hooo haaa')
                    print((config[7]))
                    print((config[8].split('/')[0]))
                    bucket = str(config[8].split('/')[0])
                    s3_path_file_nm = s3_path + file_nm
                    print(s3_path)
                    print(file_nm)
                    print(s3_path_file_nm)
                    k.key = s3_path_file_nm
                    mp = bucket.initiate_multipart_upload(s3_path_file_nm)
                    for ec2_file in source_file_list :
                        if i <= splits:
                            try :
                                file = open(ec2_file, 'r')
                                #ec2_file =  ec2_file_path + file_nm
                                print(('Uploading part %d'%(i)))
                                mp.upload_part_from_file(file,i)
                                i = i+1
                                os.remove(ec2_file)
                                print(('File %d removed'%i))
                                #return True, s3_path_file_nm
                            except :
                                print ('Some issue with the file being transferred to Informatica S3')
                                e = str(traceback.format_exc())
                                print(e)
                                retries = retries + 1
                                continue
                    mp.complete_upload()
                    retries = 4

                else:
                    s3_path = config[7]
                    publish_path = config[19]
                    k = Key(bucket)
                    p = Key(bucket)
                    print('hooo haaa')
                    print((config[7]))
                    print((config[8].split('/')[0]))
                    bucket = str(config[8].split('/')[0])
                    s3_path_file_nm = s3_path + file_nm
                    publish_path_file_nm = publish_path + file_nm
                    s3_file_path_arc = 's3://' + config[8]
                    #publish_file_path_arc = 's3://' + config[19] + str(file_nm)
                    publish_file_path_arc = 's3://' + config[19]
                    print(s3_path)
                    print(file_nm)
                    print(s3_path_file_nm)
                    k.key = s3_path_file_nm
                    p.key = publish_path_file_nm
                    mp = bucket.initiate_multipart_upload(s3_path_file_nm)
                    #mpp = bucket.initiate_multipart_upload(publish_path_file_nm)
                    for ec2_file in source_file_list:
                        if i <= splits:
                            try:
                                file = open(ec2_file, 'r')
                                # ec2_file =  ec2_file_path + file_nm
                                print(('Uploading part %d' % (i)))
                                mp.upload_part_from_file(file, i)
                                #mpp.upload_part_from_file(file, i)
                                i = i + 1
                                os.remove(ec2_file)
                                print(('File %d removed' % i))
                                # return True, s3_path_file_nm
                            except:
                                print ('Some issue with the file being transferred to Informatica S3')
                                e = str(traceback.format_exc())
                                print(e)
                                retries = retries + 1
                                continue
                    mp.complete_upload()
                    cpy_cmd_arc = '''aws s3 sync %s %s''' % (s3_file_path_arc, publish_file_path_arc)
                    subprocess.call(cpy_cmd_arc, shell=True)
                    #mpp.complete_upload()
                    retries = 4
                return True, s3_path_file_nm
            except:
                retries = retries + 1
        return False , s3_path_file_nm



    def moveFromEC2ToCrossRegionS3(self , config, ec2_file_path, file_nm):
        #supposing that trg_db_conn is the boto object connected to the required
        #enter the value of the bucket in the def gen func
        retries = 0
        while retries < 3 :
            try :
                #bucket = trg_db_conn.get_bucket(self.data_dict['s3_bucket'])
                s3_path = config[8]

                #print(self.data_dict['s3_bucket'])
                #.replace("$cycl_time_id$", self.cycl_time_id)  #trg_obj_nm will be used which will  have the path leaving the bucket name on INFA S3
                #k = Key(bucket)
                print((config[7]))
                s3_path_file_nm = s3_path + file_nm
                from_s3 = 's3://' + s3_path_file_nm
                print(('s3 path is ' + s3_path + 'file_nm is ' + file_nm))
                print(('ec2 path is ' + ec2_file_path + ' file_nm is ' + file_nm))
                print(('moveFromEC2ToCrossRegionS3 file name is $$$$$$$$$$$$$$$$$$$$$$$$$$' + file_nm))
                print(s3_path_file_nm)
                #k.key = s3_path_file_nm
                ec2_file =  ec2_file_path + file_nm
                #k.set_contents_from_filename(ec2_file)
                #os.system('export TZ=Asia/Calcutta')
                                #td = '%Y%m%d%H%M%S'
                #cmd = 'date %s'%(td)
                #status = subprocess.call(cmd , shell = 'TRUE')
                #status_out = status.read()
                #now = datetime.datetime.today().strftime('%Y%m%d%H%M%S')
                #print(now)
                #print(status_out)
                s3_file_path_arc = 's3://' + config[8] + str(file_nm)
                s3_pth = 's3://' + config[8]
                # s3_file_path_arc = 's3://' + self.data_dict['s3_bucket'] + '/' + str(s3_path) + 'Archive/' + , str(file_nm).replace('.txt', '') + '_' + str(now) + '.txt'
                #s3_file_path_arc = 's3://'+ self.data_dict['s3_bucket'] + '/' + str(s3_path) +'Archive/' + str(now)+'/'+ str(file_nm).replace('.txt','') + '_'+ str(now) + '.txt'
                print(s3_file_path_arc)
                cpy_cmd_arc = '''aws s3 cp %s %s'''%(ec2_file, s3_file_path_arc)
                print(cpy_cmd_arc)
                os.system(cpy_cmd_arc)
                retries = 4

                if config[19] is not None and config[19].strip().lower() != 'null' and config[19].strip() != '':
                    publish_path = config[19]
                    publish_path_file_nm = publish_path + file_nm
                    publish_file_path_arc = 's3://' + config[19]
                    rm_path = 's3://' + config[19]
                    print(("*********** publish path ************",publish_file_path_arc))
                    rm_cmd = '''aws s3 rm %s --recursive''' % (rm_path)
                    cpy_cmd = '''aws s3 sync %s %s''' % (s3_pth, publish_file_path_arc)
                    print(cpy_cmd)
                    os.system(rm_cmd)
                    os.system(cpy_cmd)
                    retries = 4
                else:
                    print("Publish path not available")

                return True, s3_path_file_nm
            except :
                print ('Some issue with the file being transferred to Informatica S3')
                e = str(traceback.format_exc())
                print(e)
                retries = retries + 1
        if retries == 3 :
            print('Issue in downloading the file from INFA s3')
        return False , s3_path_file_nm

    def getFileFromCrossRegionS3ToEC2(self, config,src_db_conn, ec2_file_path, file_nm , s3_path):
        bucket = src_db_conn.get_bucket(self.data_dict['s3_bucket'])
        print(('value of bucket name is : ',bucket))
        s3_path_without_filename = ('/').join(s3_path.split('/')[:-1]) + '/'
        ec2_file_path_with_filename = ec2_file_path + file_nm
        print(('ec2_file_path : ',ec2_file_path))
        print(('file_name : ', file_nm))
        print(('s3 file path : ', s3_path))
        print(('value of s3_path_without_filename is : ' + s3_path_without_filename))
        self.checkDir(ec2_file_path)
        print(('value of ec2_file_path_with_filename is : ' + ec2_file_path_with_filename))
        s3_path = '/'.join(s3_path.split('/')[1:])
        print(('value of S3_PATH is : ', s3_path))
        if s3_path_without_filename == '/' or s3_path_without_filename == '':
            file_location_s3 = bucket.list()
            print(("!!!!!!!!!!",file_location_s3))
        else :
            s3_path_without_filename = '/'.join(s3_path_without_filename.split('/')[1:])
            print(("#$#$#$#$",s3_path_without_filename))
            file_location_s3 = bucket.list(s3_path_without_filename)
            print(('value of file_location_s3 is : ', file_location_s3))
            #print file_location_s3
        #print ('out of try')
        retries = 0
        while retries < 3 :
            try :
                print(('@@@@@@@@#$#@$',file_location_s3))
                for i in file_location_s3 :
                    if(i.name == s3_path) :
                        print(('name of the location of file : ',i.name))
                        print('file matches')
                        i.get_contents_to_filename(ec2_file_path_with_filename)
                        retries = 4
                        return True
                retries = retries + 1
            except :
                print ('There was some issue while copying the file from Informatica S3 to EC2')
                e = str(traceback.format_exc())
                print(e)
                retries =  retries + 1
        if retries == 3 :
            print('No file found on INFA S3 for the required name')
        return False
    def setFilePermission(self, ec2_file_path):
        '''
        '''
        pemission_cmd = '''chmod 775 %s''' % ec2_file_path
        self.log_con = self.cmnobj.generate_log_for_src_run(self.log_con, pemission_cmd)
        #print pemission_cmd
        try:
            status = subprocess.call(pemission_cmd, shell=True)
            return  True
        except:
            log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Permission command failed %s')
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('Permission command failed')

    def getSQLQuery(self, sql_id):
        '''
        '''
        etl_sql = ''' select sql_txt from %s.cntl_etl_sql where sql_id = %s''' % (self.rds_schema_nm, sql_id)
        etl_sql_obj = self.executeQuery(etl_sql, self.orch_rds_conn)
        try:
            sql_txt = etl_sql_obj.fetchall()[0][0]
        except:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'SQL txt is not present')
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('SQL ID is not present')
        return sql_txt


    def getDBConnection(self, ctry, db_type, db_nm):
        '''
        '''
        status = ''
        if db_type == 'src_db':
            if db_nm == 'RDS':
                status, db_conn = self.cmnobj.getRDSConnection("US")
            elif db_nm == 'RS':
                status, db_conn = self.cmnobj.getRedShiftConnection("US")
                db_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            elif db_nm == 'RS_PII_AMER_ETL_01':
                status, db_conn = self.cmnobj.getPIIRedShiftConnection("US")
                db_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        else :
        # db_type == 'trg_db':
            if db_nm == 'RDS':
                status, db_conn = self.cmnobj.getRDSConnection("US")
            elif db_nm == 'RS':
                status, db_conn = self.cmnobj.getRedShiftConnection("US")
                db_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            elif db_nm == 'RS_PII_AMER_ETL_01':
                status, db_conn = self.cmnobj.getPIIRedShiftConnection("US")
                db_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        if status:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Connection to DB %s established Successfully' % db_nm)
            return True, db_conn
        else:
            return False, ''

    def getConfigurationInfo(self):
        '''
        '''
        if self.data_mv_optn == 'GD':
            fetch_config_sql = '''select data_mv_id, trim(src_db_nm) as src_db_nm, trim(trgt_db_nm) as trgt_db_nm, trim(tier_2_clfsn) as tier_2_clfsn, seq_no,
                                 trim(src_schm_nm) as src_schm_nm, trim(src_tbl_nm) as src_tbl_nm, trim(trgt_schm_nm) as trgt_schm_nm, trim(trgt_tbl_nm) as trgt_tbl_nm, trim(s3_path) as s3_path, trim(ec2_path) as ec2_path,
                                  sql_id, s3_file_dlt_flag, trgt_tbl_trnct_flag, slpt_row_cnt,misc_options, delm, ctl_file_nm, ctl_file_trgt_path, cnnctn_key,trim(publish_path) from %s.cntl_data_mv_mast where actv_flag = 1 and grp_id = %s''' % (self.rds_schema_nm, self.data_mv_id)
            tbl_config_dict = self.prepareConfigDict(fetch_config_sql)

        elif self.data_mv_optn == 'DD':
            fetch_config_sql = '''select data_mv_id, trim(src_db_nm) as src_db_nm, trim(trgt_db_nm) as trgt_db_nm, trim(tier_2_clfsn) as tier_2_clfsn, seq_no,
                                 trim(src_schm_nm) as src_schm_nm, trim(src_obj_nm) as src_obj_nm, trim(trgt_schm_nm) as trgt_schm_nm, trim(trgt_obj_nm) as trgt_obj_nm, trim(s3_path) as s3_path, trim(ec2_path) as ec2_path,
                                  sql_id, s3_file_dlt_flag, trgt_tbl_trnct_flag, slpt_row_cnt,misc_options, delm, ctl_file_nm, ctl_file_trgt_path, cnnctn_key,trim(publish_path) from %s.cntl_data_mv_mast where actv_flag = 1 and data_mv_id = %s''' % (self.rds_schema_nm, self.data_mv_id)
            tbl_config_dict = self.prepareConfigDict(fetch_config_sql)
        print(("***********Config Dict ************",tbl_config_dict))
        if len(tbl_config_dict) > 0:
            src_db = list(tbl_config_dict.values())[0][0]
            trg_db = list(tbl_config_dict.values())[0][1]
            return True, tbl_config_dict, src_db.upper(), trg_db.upper()
        else:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'No configuration available for the input data movement ID/group ID')
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('No configuration available for the input data movement ID/group ID')

    def prepareConfigDict(self, fetch_config_sql):
        '''
        '''
        #0. data mv id , 1. src_db_nm , 2. trgt_db_nm, 3. tier_2_clsfn , 4. seq_no, 5. src_schm_nm , 6. src_obj_nm, 7. trgt_schm_nm , 8. trgt_obj_nm, 9. s3_path, 10. ec2_path , 11. sql_id, 12. file_dlt_flag, 13. trgt_tbl_trnt_flag, 14. slpt_row_cnt
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Inside preparing the configuration dictionary')
        tbl_config_dict = dict()
        print(tbl_config_dict)
        config_list_obj = self.executeQuery(fetch_config_sql, self.orch_rds_conn)
        config_list = config_list_obj.fetchall()
        print('Check for config list 9')
        #print(config_list)
        #print('config 9 is ')
        #print(config_list[9])
        if len(config_list) > 0:
            for config in config_list:
                s3_file_path = config[9].replace('$CYCL_TIME_ID$', self.cycl_time_id).replace('$cycl_time_id$',self.cycl_time_id).replace('$SCEN_ID$', self.scen_id).replace('$scen_id$', self.scen_id)
                print("(*_*)")
                tbl_config_dict[config[0]] = [config[1], config[2], config[3], config[4], config[5], config[6],
                                             config[7], config[8], s3_file_path, config[10], config[11], config[13], config[0], config[14], config[15], config[16], config[17], config[18], config[19],config[20]]

            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Configuration dictionary %s'%tbl_config_dict)
            return tbl_config_dict
        else:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'No configuration available for the input data movement ID/group ID')
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('No configuration available for the input data movement ID/group ID')

    def sendFaliureEmail(self, tbl_nm):
        '''
        '''
        email_cmd = '''python send_email.py 13 500 %s''' % tbl_nm
        try:
            status = subprocess.call(email_cmd, shell=True)
            return True
        except:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Email notification failed')
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('Email notification failed')

    def executeQuery(self, sql, db_obj):
        '''
        '''
        db_conn_cur = db_obj.cursor()
        print(sql)
        try:
            db_conn_cur.execute(sql)
            return db_conn_cur
        except:
            #print sql
            e = str(traceback.format_exc())
            logging.info(self.log_con)
            logging.info('sql query failed: %s ######### %s' %(sql, e))
            logging.info('\nJob %s failed at %s', sys.argv[0],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit(e)


if __name__ == '__main__':
    arg_list = sys.argv
    obj = DataMovementProcessor(arg_list)
    status = obj.process()





    """
    def checkFinalCnt(self, config):
        '''
        '''
        cnt_sql = '''select src_rec_cnt, trgt_rec_cnt from %s.cntl_data_mv_log where cycl_time_id = %s
                     and scen_id = %s and data_mv_id = %s'''%(self.rds_schema_nm, self.cycl_time_id, self.scen_id, config[12])
        count_sql_obj = self.executeQuery(cnt_sql, self.orch_rds_conn)
        tbl_count = count_sql_obj.fetchall()
        try:
            src_table_cnt = int(tbl_count[0][0])
            trgt_table_cnt = int(tbl_count[0][1])
        except:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'SQL txt is not present')
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('Count query failed while fetching the counts from data movement log table')

        if src_table_cnt == trgt_table_cnt:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                            'Source table %s and target table %s counts %s, %s matching'%(config[5], config[7], src_table_cnt, trgt_table_cnt))
        else:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Source table %s and target table %s counts %s, %s are not matching' %(config[5], config[7], src_table_cnt, trgt_table_cnt))
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('Source table count %s and target table counts %s not matching'%(src_table_cnt, trgt_table_cnt))

    """

    """
    def unloadFromEC2(self, config, ec2_file_path, file_nm, src_db_conn):
        '''
        '''
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Inside the function to load the file %s from EC-2 to S3'%file_nm)
        status = self.setFilePermission(ec2_file_path)
        if config[8]:
            s3_file_path = self.data_dict["load_s3_path"] + config[8] + config[5] + '/'

        split_nm = self.splitEC2File(ec2_file_path, config, file_nm, src_db_conn)

        unload_cmd = self.data_dict["unload_EC-2_CMD"] % (ec2_file_path, s3_file_path)
        unload_cmd = unload_cmd + ''' --exclude "*" --include "%s*" --recursive'''%split_nm
        self.log_con = self.cmnobj.generate_log_for_src_run(self.log_con, unload_cmd)
        #print unload_cmd
        try:
            status = subprocess.call(unload_cmd, shell=True)
        except:
            e = str(traceback.format_exc())
            status = self.sendFaliureEmail(config[5])
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Unload data from EC-2 to RDS failed %s' % e)
            status = self.updateInserLog('update', 3, config, '')
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('Unload data from RDS to EC-2 CMD failed')

        if status == 0:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Unload data from EC-2 to S3 is complete for table %s' % config[6])
            return True, s3_file_path
        else:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Unload data from EC-2 to S3 CMD failed')
            status = self.updateInserLog('update', 3, config, '')
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('Unload data from RDS to EC-2 CMD failed')

    def splitEC2File(self, ec2_file_path, config, file_nm, src_db_conn):
        '''
        '''
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                        'Inside the function to split EC-2 file %s' % file_nm)
        split_nm = ec2_file_path + config[5] + '_%s_%s'%(self.cycl_time_id, self.scen_id)
        ec2_file_nm = ec2_file_path + file_nm

        status, src_rec_cnt = self.getTblRecCnt('src_tbl_cnt', src_db_conn, config)

        if src_rec_cnt == 0:
            status = self.updateInserLog('update', 1, config, '')
            print 'data is not present in source table for given cycle time id % s' % self.cycl_time_id
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                            'data is not present in source table for given cycle time id % s' % self.cycl_time_id)
            sys.exit(0)

        try:
            split_cmd = '''split -l %s %s %s && gzip %s*''' % (int(config[13]), ec2_file_nm, split_nm, split_nm)
            #print split_cmd
            status = subprocess.call(split_cmd, shell=True)
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                            'File split is complete for with split file name %s' % split_nm)
            return split_nm
        except:
            e = str(traceback.format_exc())
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('Split command to split EC-2 file failed')
    """
    """
    def unloadFile(self, config, src_db_conn, trg_db_conn,src_db, trg_db):
        '''
        '''
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Inside unload file from RS to S3 function')
        #print 'Inside unload file from RS to S3 function'
        file_nm = config[5] + '.txt.gz'

        if config[8]:
            if src_db == 'RS_PII_AMER_ETL_01':
                s3_file_path = self.data_dict["pii_load_s3_path"] + config[8] + config[5] + '/'
            else:
                s3_file_path = self.data_dict["load_s3_path"] + config[8] + config[5] + '/'


        if config[9]:
            sql_txt = self.getSQLQuery(config[10])
            data_dump_sql = sql_txt.replace('$CYCL_TIME_ID$', self.cycl_time_id).replace('$cycl_time_id$', self.cycl_time_id)
            data_dump_sql = data_dump_sql.replace('$SCEN_ID$', self.scen_id).replace('$scen_id$', self.scen_id)
        else:
            status = self.sendFaliureEmail(config[5])
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'SQL ID is not present')
            status = self.updateInserLog('update', 3, config, file_nm)
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('SQL ID is not present')
        if src_db == 'RS_PII_AMER_ETL_01':
            unload_cmd = self.data_dict["pii_unload_RS_CMD"] % (data_dump_sql, s3_file_path, self.data_dict['pii_s3_iam_role'])
        else:
            unload_cmd = self.data_dict["unload_RS_CMD"] % (data_dump_sql, s3_file_path, self.data_dict['s3_iam_role'])
        #print unload_cmd
        self.log_con = self.cmnobj.generate_log_for_src_run(self.log_con, unload_cmd)
        config_list_obj = self.executeQuery(unload_cmd, src_db_conn)
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'File %s unload on S3 is complete' % s3_file_path)
        return True, s3_file_path
    """
    """
    def getTblRecCnt(self, option, db_conn, config):
        '''
        '''
        if config[10]:
            sql_txt = self.getSQLQuery(config[10])
            data_dump_sql = sql_txt.replace('$CYCL_TIME_ID$', self.cycl_time_id).replace('$cycl_time_id$', self.cycl_time_id)
            data_dump_sql = data_dump_sql.replace('$SCEN_ID$', self.scen_id).replace('$scen_id$', self.scen_id)
            filter_condition = re.compile('where', re.IGNORECASE).split(data_dump_sql,1)
        else:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'SQL ID is not present')
            status = self.sendFaliureEmail(config[5])
            logging.info(self.log_con)
            status = self.updateInserLog('update', 3, config, '')
            sys.exit('SQL ID is not present')
        src_tbl_nm = config[4].lower()+'.'+config[5].lower()
        trgt_tbl_nm = config[6].lower()+'.'+config[7].lower()
        pattern = re.compile(src_tbl_nm, re.IGNORECASE)
        if option == 'src_tbl_cnt':
            tbl_nm = config[4] + '.' + config[5]
        elif option == 'trgt_tbl_cnt':
            tbl_nm = config[6] + '.' + config[7]

        if len(filter_condition) > 1:
            if option == 'trgt_tbl_cnt' and src_tbl_nm != trgt_tbl_nm:
                filter_condition[1] = pattern.sub(trgt_tbl_nm,filter_condition[1])
            count_sql = '''select count(1) from %s where %s'''%(tbl_nm, filter_condition[1])
        else:
            count_sql = '''select count(1) from %s'''%tbl_nm
        count_sql_obj = self.executeQuery(count_sql, db_conn)
        try:
            table_cnt = count_sql_obj.fetchall()[0][0]
            return True, table_cnt
        except:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'SQL txt is not present')
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('SQL ID is not present')
    """
    """
    def loadDataMainTbl(self, config, tmp_tbl_nm, trg_db_conn):
        '''
        '''
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Inside loading the data in main table function')

        dump_data_main_tbl_sql = "SELECT 'insert into '|| nspname ||'.' || relname ||  ' select ' || substring( string_agg ( CASE WHEN typname = 'int2' OR typname = 'int4' THEN ',case when (' || attname||' = '''') then null else cast(' || attname || ' as integer) end as ' || attname WHEN typname = 'int8'    THEN ',case when (' || attname || ' = '''') then null else cast(' || attname || ' as bigint) end as ' || attname WHEN typname = 'numeric' THEN ',case when (' || attname || ' = '''') then null else cast(' || attname || ' as decimal(37,15)) end as ' || attname WHEN typname = 'bool' THEN ',case when (' || attname || ' = '''') then null else cast(cast(' || attname || ' as integer) as boolean) end as ' || attname WHEN typname = 'varchar' OR typname = 'text' THEN ',case when (' || attname || ' = '''') then null else trim(' || attname || ') end as ' || attname WHEN typname = 'bpchar' OR typname = 'text' THEN ',case when (' || attname || ' = '''') then null else trim(' || attname || ') end as ' || attname WHEN typname = 'date' THEN ',case when (' || attname || ' = '''') then null else cast(' || attname || ' as date) end as ' || attname WHEN typname = 'timestamp' /*then ',case when ('||attname||' = '''') then null else to_date('||attname||',''MM/DD/YYYYHHMISS'') end as '||attname*/ THEN ',case when (' || attname || ' = '''') then null else cast(trim(substring(' || attname || ',1,10)) ||'' ''|| trim(substring(' || attname || ',11,length(' || attname || '))) as timestamp) end as ' || attname          END  , '  ' ) from 2) || ' From %s ; ' from (   select * FROM pg_attribute a, pg_namespace n, pg_class c, pg_type t WHERE n.oid = c.relnamespace AND a.attrelid = c.oid AND t.oid = a.atttypid AND relname = lower('%s') AND nspname = '%s' AND attnum > 0 ORDER BY attnum ) a group by nspname,relname;"% (tmp_tbl_nm, config[7], config[6])
        dump_data_main_tbl_sql_obj = self.executeQuery(dump_data_main_tbl_sql, trg_db_conn)
        try:
            load_data_main_tbl_sql = dump_data_main_tbl_sql_obj.fetchall()[0][0]
            status = self.executeQuery(load_data_main_tbl_sql, trg_db_conn)
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                            'Data load in table %s is complete'%config[7])
            if status:
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                                'Mission to drop temp table %s......'%tmp_tbl_nm)
                # status = self.executeQuery('drop table if exists %s'%tmp_tbl_nm, trg_db_conn)
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                                'Temp table %s dropped successfully'%tmp_tbl_nm)
                return status
        except:
            e = str(traceback.format_exc())
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                            'Copy data temp table to main table failed %s'%e)
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('Copy data from Ec-2 to RDS is failed')

    def createTempTable(self, trg_db_conn, config, tbl_nm):
        '''
        '''
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                        'Inside creating the temp table in RDS function')
        tmp_tbl_nm = tbl_nm + '_temp_' + str(self.cycl_time_id) + '_' + str(self.scen_id)

        status = self.executeQuery('drop table if exists %s' % tmp_tbl_nm, trg_db_conn)

        crt_tbl_str = '''CREATE TABLE %s ('''%tmp_tbl_nm
        col_type = '''varchar(5000)'''
        col_nm = self.fetchColumnNm(trg_db_conn, tbl_nm)
        col_list = col_nm.split(',')

        for col in col_list:
            crt_tbl_str = crt_tbl_str + '\n' + col + ' ' +  col_type + ','
        crt_tbl_str = crt_tbl_str[:-1] + '\n );'

        status = self.executeQuery(crt_tbl_str, trg_db_conn)
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Temp table %s created successfully '%tmp_tbl_nm)
        return status, tmp_tbl_nm

    def copyFileOnEC2(self, s3_file, src_db_conn, config, tab_nm):
        '''
        '''
        #is it needed in Gilead ?? , we can parameterise it
                status, src_rec_cnt = self.getTblRecCnt('src_tbl_cnt', src_db_conn, config)

        if src_rec_cnt == 0:
            status = self.updateInserLog('update', 1, config, '')
            status = self.updateInserLog('update_src_tbl_cnt', src_rec_cnt, config, '')
            status = self.updateInserLog('update_trgt_tbl_cnt', src_rec_cnt, config, '')
            print 'data is not present in source table for given cycle time id % s' % self.cycl_time_id
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                            'data is not present in source table for given cycle time id % s' % self.cycl_time_id)
            sys.exit(0)

        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Inside the copy file on EC-2 from S3 function')
        status = self.updateInserLog('update', 2, config, s3_file)
        s3_file_path = self.data_dict["load_s3_path"] + config[8] + tab_nm + '/'
        ec2_file_path = self.EC2_file_tmp_path + config[9] + tab_nm + '/'
        copy_cmd = self.data_dict["copy_file_on_ec2_CMD"] % (s3_file_path, ec2_file_path)
        self.log_con = self.cmnobj.generate_log_for_src_run(self.log_con, copy_cmd)
        #print copy_cmd
        try:
            status = subprocess.call(copy_cmd, shell=True)
            ec2_file_nm = self.mergeDataSplit(ec2_file_path, config[7])  # config[7] is the target table name
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Copy file %s on EC-2 from S3 is complete'%ec2_file_nm)
            return ec2_file_path, ec2_file_nm, s3_file
        except:
            status = self.updateInserLog('update', 3, config, s3_file)
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Unload data from EC-2 to S3 CMD failed')
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('Copy file from S3 to ec-2 failed')

    def mergeDataSplit(self, ec2_file, tbl_nm):  # why are we exactly merging the data here ??
        '''
        '''
        ec2_file_nm = ec2_file + tbl_nm + '.txt'
        merge_cmd = self.data_dict['merge_file_cmd'] % (ec2_file, ec2_file_nm)
        #print merge_cmd
        try:
            status = subprocess.call(merge_cmd, shell=True)
            return ec2_file_nm
        except:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'merger data split cmd failed')
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('merger data split cmd failed')

    def getFileDetailFromLog(self, db_mv_id):
        '''
        '''
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Inside the get file information from data movement log table function')
        fetch_log_dtl_sql = ''' select unld_file_nm from %s.cntl_data_mv_log where cycl_time_id = %s and scen_id = %s and
                               data_mv_id = %s order by inrt_dt desc''' % (self.rds_schema_nm, self.cycl_time_id, self.scen_id, db_mv_id)

        config_list_obj = self.executeQuery(fetch_log_dtl_sql, self.orch_rds_conn)
        config_list = config_list_obj.fetchall()

        if len(config_list) > 0:
            unload_file = config_list[0][0]
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Copy in DB from S3 file name is %s'%unload_file)
            return unload_file
        else:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Error in connecting the target_db %s database' % db_mv_id)
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            exit(1)

    def copyDataInTable(self, config, src_db_conn, trg_db_conn, s3_file_nm,src_db,trg_db):
        '''
        '''


        status, src_rec_cnt = self.getTblRecCnt('src_tbl_cnt', src_db_conn, config)
        if src_rec_cnt == 0:
            status = self.updateInserLog('update', 1, config, '')
            status = self.updateInserLog('update_src_tbl_cnt', src_rec_cnt, config, '')
            status = self.updateInserLog('update_trgt_tbl_cnt', src_rec_cnt, config, '')
            print 'data is not present in source table for given cycle time id % s' % self.cycl_time_id
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                            'data is not present in source table for given cycle time id % s' % self.cycl_time_id)
            sys.exit(0)
        self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Inside the copy data from S3 to RS function')
        tbl_nm = config[6] + '.' + config[7]
        if src_db == 'RS_PII_AMER_ETL_01':
            s3_file_path = self.data_dict["pii_load_s3_path"] + config[8]
        else:
            s3_file_path = self.data_dict["load_s3_path"] + config[8]
        s3_file_path = s3_file_path[5:] + config[5] + '/'
        if config[11] == 1:
            trun_sql = ''' truncate table %s''' % tbl_nm
            config_list_obj = self.executeQuery(trun_sql, trg_db_conn)

        else:
            check_cycl_time_id_data_sql = '''select count(1) from %s where cycl_time_id = %s and scen_id = %s'''%(tbl_nm, self.cycl_time_id, self.scen_id)
            check_cycl_time_id_data_sql_obj = self.executeQuery(check_cycl_time_id_data_sql, trg_db_conn)
            data_cnt = check_cycl_time_id_data_sql_obj.fetchall()[0][0]

            if data_cnt != 0:
                status = self.sendFaliureEmail(config[5])
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,'Total record count %s is already present in table %s for given cycle time id' %(data_cnt, tbl_nm))
                logging.info(self.log_con)
                logging.info(self.log_con)
                logging.info('\nJob %s failed at %s', sys.argv[0],
                             datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
                sys.exit('Total record count %s is already present in table %s for given cycle time id' %(data_cnt, tbl_nm))

        col_nm = self.fetchColumnNm(trg_db_conn, tbl_nm)
        copy_obj = CopyToRedShift()
        if (trg_db == 'RS_PII_AMER_ETL_01' and src_db == 'RS_AMER_ETL_01') or (trg_db == 'RS_AMER_ETL_01' and src_db == 'RS_PII_AMER_ETL_01'):
            status, self.log_con = copy_obj.process(s3_file_path, tbl_nm, "\x01", "N", "H", "GZIP", "yyyy-mm-dd",'REMOVEQUOTES ESCAPE ', col_nm, trg_db_conn, self.log_con)
        else :
            status, self.log_con = copy_obj.process(s3_file_path, tbl_nm, "|", "N", "H", "GZIP", "yyyy-mm-dd",'REMOVEQUOTES ESCAPE ', col_nm, trg_db_conn, self.log_con)
        if status == 0:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Copy to RedShift Failed!')
            status = self.updateInserLog('update', 3, config, s3_file_nm)
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
        else:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Copy to RedShift completed')
            return

    def fetchColumnNm(self, trg_db_conn, tab_nm):
        '''
        '''
        col_nm = ""
        col_fetch_ct = 0
        sql_col = "select column_name from information_schema.columns where lower(table_schema) || '.' || lower(table_name) = lower('%s') order by ordinal_position asc;" % tab_nm
        while col_fetch_ct < 3:  #the significance of the condition ??
            try:
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                           'Fetching columns list for %s from information_schema.columns table' % (
                                                           tab_nm))
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, sql_col)

                config_list_obj = self.executeQuery(sql_col, trg_db_conn)
                #But we want the source data set to be S3 ,so how will that be handled
                colum_list = config_list_obj.fetchall()
                for record in colum_list:
                    col_nm = col_nm + record[0] + ","
                col_nm = col_nm[:-1] #why are these kind on computations even required ??
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                           'Successfully fetched columns list for %s from information_schema.columns table' % (
                                                           tab_nm))
                break
            except:
                e = str(traceback.format_exc())
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                           'Error occured while fetching columns for %s' % tab_nm)
                self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'Below is the Error %s' % e)
                col_nm = ""
                log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                           'Fetching columns list for %s from information_schema.columns table failed retrying ' % (
                                                           tab_nm))
                col_fetch_ct = col_fetch_ct + 1
                time.sleep(5)

        if col_nm == "" and col_fetch_ct >= 3:
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                       'Unable to fetch columns list for %s exiting now' % (tab_nm))
            self.log_con = self.cmnobj.generate_log_for_src_run(log_con, 'Copy to RedShift Wrapper Failed!')
            logging.info(self.log_con)
            sys.exit('Unable to fetch columns list')
        elif col_nm == "":
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con,
                                                       'Table %s is not present in information_schema.columns' % (
                                                       tab_nm))
            self.log_con = self.cmnobj.generate_log_with_dt(self.log_con, 'column fetch function failed')
            logging.info(self.log_con)
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
            sys.exit('Target table not present')
        return col_nm

    """

