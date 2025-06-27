#!/usr/bin/python
# -*- coding: utf-8 -*-

# !/usr/bin/sh

########################################################################
###
###     MODULE     : Data_Movement_Wrapper.py
###
###     AUTHOR     : zs associates
###
###     PARAMETERS : 5
###
###     NO_OF_STEPS: 01
###
###     HISTORY:
###
###     Date        Name          CRF Number  Description
###     ----------  ------------  ----------  ----------------------------
###     02/07/2018  ZS       None        Initial Revision.
########################################################################

import warnings
warnings.filterwarnings('ignore')
import sys
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
from s3_Redshift import CopyToRedShift
from s3_RDS import CopyToRds

try:
    log_file = sys.argv[5]
except:
    print(''' The input parameter is not given properly, Please follow the below pattern --
            python Data_Movement_Wrapper.py cycl_time_id scen_id dtst_id trg_db log_file
    ''')
    sys.exit('Given input parameter in trigger script is not correct')
FORMAT = '%(message)s'
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format=FORMAT)


class CopyWrapper:

    '''
    '''

    def __init__(self, arg_list):
        '''
        '''

        self.cmnobj = commonMethods()
        self.data_dict = self.cmnobj.getEnvironVariable()
        input_parm_dict = {'Cycle time id ': arg_list[1],
                           'Scenario ID': arg_list[2],
                           'Dataset ID': arg_list[3]}
        output_parm_dict = {'Target Location': 'S3 & Target Data Base'}
        src_nm = 'S3 to Target DB wrapper'
        process_id = os.getpid()
        process_nm = sys.argv[0]
        self.EC2_file_tmp_path = self.data_dict["EC-2_data_file_path"]
        self.log_con = self.cmnobj.generate_log_head(src_nm,process_nm, process_id, input_parm_dict, output_parm_dict)

        try:
            self.cycl_time_id = arg_list[1]
            self.scen_id = arg_list[2]
            self.dtst_id = arg_list[3]
            self.trgt_db = arg_list[4]
        except:
            print(''' The input parameter is not given properly, Please follow the below pattern-
                              python s3_stage_to_base.py cycl_time_id scen_id dtst_id trg_db log_file
                          ''')

        print('variable assignment completed')

    def process(self):
        '''
        '''

        # ###############################################################################################################
        # ##Fetch configuration for the given data movement ID/group ID
        # ###############################################################################################################


        trg_db = self.trgt_db
        status, self.orch_rds_conn = self.cmnobj.getRDSConnection("US")

        if status:
            self.log_con = \
                self.cmnobj.generate_log_with_dt(self.log_con,
                   'Connection to RDS established Successfully')
        else:
            logging.info('RDS connection failed, Please check the RDS server status'
                         )
            sys.exit(self.orch_rds_conn)

        status = self.checkStatusOfFiles(self.orch_rds_conn)
        if not status:
            print('Files of the required dtst_id ecist with status other than 50 in file_ftp_log for cycl_time_id : %d' \
                % int(self.cycl_time_id))
            sys.exit('Exiting process due to invalid entries in ftp log'
                     )
        (status, tbl_config_dict) = self.getConfigurationInfo()
        #status, self.orch_rds_conn = self.cmnobj.getRedShiftConnection("US")
        if status:
            self.log_con = \
               self.cmnobj.generate_log_with_dt(self.log_con,
                                               'Connection to Redshift established Successfully')
        else:
            logging.info('Redshift connection failed, Please check the Redshift server status'
                         )
            sys.exit(self.orch_rds_conn)

        if trg_db.upper() == 'RDS':
            print('Connecting to RDS to load data from S3 location')
            copy_obj = CopyToRds(arg_list,tbl_config_dict)
            (status, log_con) = copy_obj.process()
            logging.info(log_con)

        if trg_db.upper() == 'RS':
            print('Connecting to Redshift to load data from S3 location')
            copy_obj = CopyToRedShift(arg_list,tbl_config_dict)
            (status, log_con) = copy_obj.process()
            logging.info(log_con)

        logging.info(self.log_con)
        logging.info('\nJob %s ended at %s', sys.argv[0],
                     datetime.datetime.strftime(datetime.datetime.now(),
                     '%Y-%m-%d %H:%M:%S'))
        logging.info('\n################################################################################'
                     )
        return True

    def checkStatusOfFiles(self, conn):
        qry = \
            ''' select * from comm_ops.cntl_file_ftp_log where dtst_id =%d and file_sta <> 50
                and file_log_id in (select file_log_id from comm_ops.cntl_file_log where dtst_id =%d and cycl_time_id= %d and scen_id =%d)''' \
            % (int(self.dtst_id), int(self.dtst_id),
               int(self.cycl_time_id), int(self.scen_id))
        count_entries = self.executeQuery(qry, self.orch_rds_conn)
        if count_entries:
            if len(count_entries) > 0:
                return False
        else:
            return True
        return True

    def getS3SourceLocation(self, dtst_id):
        stg_path_qry = '''select bckt_nm,stg_path,delm from comm_ops.cntl_src_mast where dtst_id=%s''' % (dtst_id)
        print(stg_path_qry)
        stg_path_list = self.executeQuery(stg_path_qry, self.orch_rds_conn)
        try:
            if len(stg_path_list) > 0:
                stg_path = stg_path_list[0][0] + '/'.join(
                    stg_path_list[0][1].split('/')[0:-2]) + '/cycl_time_id=' + self.cycl_time_id + ''
                print(stg_path)
            else:
                raise Exception('Required data set ID is not present in the src_mast table')
            src_file_nm_qry = '''select case when SUBSTRING(src_file_actl_nm, length(src_file_actl_nm)-2, 3) = '.GZ' then SUBSTRING(src_file_actl_nm, 0, length(src_file_actl_nm)-1)||'gz' else src_file_actl_nm end from comm_ops.cntl_file_ftp_log where file_log_id in (select file_log_id from comm_ops.cntl_file_log where dtst_id=%s and cycl_time_id=%s and scen_id=%s)''' % (
            self.dtst_id, self.cycl_time_id, self.scen_id)
            print(src_file_nm_qry)
            src_file_nm_list = self.executeQuery(src_file_nm_qry, self.orch_rds_conn)
            print(src_file_nm_list)
            # if src_file_nm_list:
            src_file_nm = src_file_nm_list[0][0]
            # stg_path_with_file_name = 's3://' + stg_path + '/' + src_file_nm

            delm = (stg_path_list[0][2].strip('"')).strip("'")
            print(src_file_nm_list)
            return str(stg_path), src_file_nm_list,delm
        except:
            e = str(traceback.format_exc())
            print(e)
            sys.exit('Error in fetchng the S3 location')

    def getTargetTableName(self, dtst_id):
        target_table_qry = \
            '''select obj_nm from comm_ops.cntl_obj_metadata_mast where dtst_id=%s group by 1''' \
            % dtst_id
        print(target_table_qry)
        target_table_list = self.executeQuery(target_table_qry,self.orch_rds_conn)
        try:
            if target_table_list:
                if len(target_table_list) > 1:
                    raise Exception('Multiple entries for the same dtst_id in the obj_metadata_mast table')
                else:
                    target_table = target_table_list[0][0]
                    return str(target_table)
            else:
                raise Exception('No entry for the required dtst_id : '+ dtst_id+ ' in the obj_metadata_mast table')
        except:
            e = str(traceback.format_exc())
            print(e)
        return str(target_table_list[0][0])

    def getSourceFilePattern(self):
        src_file_pattern_qry = '''select distinct src_file_ptrn from comm_ops.cntl_file_ftp_log where file_log_id in (select file_log_id from comm_ops.cntl_file_log where dtst_id=%s and cycl_time_id=%s and scen_id=%s)''' % ( self.dtst_id, self.cycl_time_id, self.scen_id)
        src_file_pattern = self.executeQuery(src_file_pattern_qry, self.orch_rds_conn)
        print(('Source file pattern is:' + str(src_file_pattern)))
        if len(src_file_pattern) > 1:
            raise Exception('Multiple pattern found for single DTST id !!')
        return src_file_pattern[0][0]


    def getTableConfigDict(self, dtst_id):
        try:
            (stg_path, src_file_nm_list,delim) = \
                self.getS3SourceLocation(dtst_id)
            target_table = self.getTargetTableName(dtst_id)
            src_file_pattern = self.getSourceFilePattern()
        except:
            e = str(traceback.format_exc())
            print(e)
        print(target_table)
        (target_schema_nm, target_table_nm) = target_table.split('.')
        ec2_path = self.EC2_file_tmp_path  # from environmental file
        tab_nm = stg_path.split('/')[-1]
        print(stg_path + ' ' + target_table + ' ')
        table_config_dict = dict()
        table_config_dict[dtst_id] = dict()
        table_config_dict[dtst_id]['stg_path'] = stg_path
        table_config_dict[dtst_id]['target_table_nm'] = target_table_nm
        table_config_dict[dtst_id]['target_schema_nm'] = \
            target_schema_nm
        table_config_dict[dtst_id]['ec2_path'] = ec2_path
        table_config_dict[dtst_id]['tab_nm'] = tab_nm
        table_config_dict[dtst_id]['tbl_list'] = src_file_nm_list
        table_config_dict[dtst_id]['delimiter'] = delim
        table_config_dict[dtst_id]['src_file_pattern'] = src_file_pattern
        return table_config_dict

    def getConfigurationInfo(self):
        '''
        '''

        fetch_file_status_qry = \
            '''select file_sta from comm_ops.cntl_file_log where dtst_id=%s and cycl_time_id=%s and scen_id=%s''' \
            % (self.dtst_id, self.cycl_time_id, self.scen_id)
        print(fetch_file_status_qry)
        try:
            fetch_file_status_data_obj = \
                self.executeQuery(fetch_file_status_qry,
                                  self.orch_rds_conn)  # separate fuction to execute query in the target database connection
            fetch_file_status_data = fetch_file_status_data_obj
            if fetch_file_status_data:
                if fetch_file_status_data[0][0] == 1:

                    # get Configuration

                    table_config_dict = dict()
                    table_config_dict = \
                        self.getTableConfigDict(self.dtst_id)
                    return (True, table_config_dict)
                elif fetch_file_status_data[0][0] == 99:
                    raise Exception('The old incremental load for the specific file is pending'
                                    )
            else:

                self.log_con = \
                    self.cmnobj.generate_log_with_dt(self.log_con,
                        'No configuration available for the input data movement ID/group ID'
                        )  # ## generate log in common module
                logging.info(self.log_con)
                logging.info('\nJob %s failed at %s', sys.argv[0],
                             datetime.datetime.strftime(datetime.datetime.now(),
                             '%Y-%m-%d %H:%M:%S'))
                sys.exit('No configuration available for the input data movement ID/group ID'
                         )
        except:
            e = str(traceback.format_exc())
            print(e)
            return (False, '')

    def executeQuery(
        self,
        sql,
        db_obj,
        opt=None,
        ):
        '''
        '''

        db_conn_cur = db_obj.cursor()
        try:
            db_conn_cur.execute(sql)
            if opt is None:
                cur = db_conn_cur.fetchall()
                return cur
            else:
                db_conn_cur.execute('Commit')
                return 1
        except:
            e = str(traceback.format_exc())
            logging.info(self.log_con)
            logging.info('sql query failed: %s ######### %s' % (sql, e))
            logging.info('\nJob %s failed at %s', sys.argv[0],
                         datetime.datetime.strftime(datetime.datetime.now(),
                         '%Y-%m-%d %H:%M:%S'))
            sys.exit(e)






if __name__ == '__main__':
    arg_list = sys.argv
    obj = CopyWrapper(arg_list)
    status = obj.process()

