#!/usr/bin/python
#!/usr/bin/sh
########################################################################
###
###     MODULE     :
###
###     DESCRIPTION:
###
###     DOCUTEXT   :
###
###     PARAMETERS :
###
###     NO_OF_STEPS: 01
###
###     HISTORY:
###
###     Date        Name          CRF Number  Description
###     ----------  ------------  ----------  ----------------------------
###     20/06/2016  Achal         None        Initial Revision.
########################################################################
import warnings
warnings.filterwarnings("ignore")
import sys
import os
import psycopg2
import logging
import datetime
import math
import dateutil.relativedelta
import traceback
from def_gen_func import commonMethods



i_log_file = sys.argv[5]
FORMAT= '%(message)s'
logging.basicConfig(filename=i_log_file,level=logging.INFO, format=FORMAT)


class QeuryProcessor():
    '''
    '''
    def __init__(self):
        '''
        '''
        self.cmnobj = commonMethods()


    def process(self, cycl_time_id, scen_id, inrt_by ,rule_id):
        '''
        '''
        input_parm_dict = {'Cycle Time ID' : cycl_time_id, 'Scenario ID' : scen_id, 'Rule ID' : rule_id}
        output_parm_dict = {'Complete Execution Of Queries Log :  ' : i_log_file}
        src_nm = 'Query Executor Script'
        process_id = os.getpid()
        process_nm = sys.argv[0]
        log_con = self.cmnobj.generate_log_head(src_nm, process_nm, process_id, input_parm_dict, output_parm_dict)
        logging.info(log_con)
        log_con = ''
        self.data_dict = self.cmnobj.getEnvironVariable()
        run_id = 'PID-'+str(process_id)
        status, rs_conn = self.cmnobj.getRedShiftConnection("US")
        print ('entered the process')
        if status:
            rs_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            rs_cur = rs_conn.cursor()
            log_con = self.cmnobj.generate_log_with_dt(log_con,'Connection to Red-shift established Successfully')
        else:
            log_con = self.cmnobj.generate_log_with_dt(log_con,'Red-shift connect failed, Please check the Red-shift Server status')
            logging.info(log_con)
            sys.exit(mydb)


        status, rds_conn = self.cmnobj.getRDSConnection("US")
        if status:
            rds_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            rds_cur = rds_conn.cursor()
            log_con = self.cmnobj.generate_log_with_dt(log_con, 'Connection to RDS established Successfully')
        else:
            log_con = self.cmnobj.generate_log_with_dt(log_con, 'Unable to connect to RDS')
            logging.info(log_con)
            sys.exit(rds_conn)

        cursor = rds_cur
        status, rule_info = self.get_rule_info(rule_id, cursor)

        logging.info(log_con)
        log_con = ''
        if status:
            log_txt = 'Rule information :- %s'%rule_info
            print("rule_info>>>>>>>>>>", rule_info)
            log_con = self.cmnobj.generate_log_with_dt(log_con, log_txt)
            logging.info(log_con)
            log_con = ''
        else:
            log_txt = 'No information available for the Rule ID :- %s'%rule_id
            log_con = self.cmnobj.generate_log_with_dt(log_con, log_txt)
            log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
            logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
            logging.info('################################################################################')
            logging.info(log_con)
            sys.exit(log_txt)

        status, query_info_dict, order_list = self.get_query_info(rule_id, cursor)
        #print(' step 1')
        if status:
            #print('in if')
            log_txt = 'Query and Parameter information :- %s'%query_info_dict
            log_con = self.cmnobj.generate_log_with_dt(log_con, log_txt)
            logging.info(log_con)
            log_con = ''
        else:

            #print('in else')
            log_txt = 'No Query and Parameter information available for the Rule ID :- %s'%rule_id
            log_con = self.cmnobj.generate_log_with_dt(log_con, log_txt)
            logging.info(log_con)
            log_con = ''

        status, final_query_list = self.built_query(query_info_dict, order_list, cycl_time_id, scen_id, cursor, rds_cur, inrt_by)

        if rule_info[0][1] == 'Serial':
            #print('in serial')
            log_con = self.cmnobj.generate_log_with_dt(log_con,'Mission to execute queries serially')
            status, log_con = self.executeSerially(rule_id, final_query_list, cycl_time_id, scen_id, run_id, inrt_by, rds_cur, cursor, rs_cur, log_con)
            #print('step 2')
        elif rule_info[0][1] == 'Loop':
            log_con = self.cmnobj.generate_log_with_dt(log_con,'Mission to execute queries in looping format')
            status, log_con = self.executeInLoop(rule_id,final_query_list, cycl_time_id, scen_id, rule_info[0][2], rule_info[0][3], run_id, inrt_by, rds_cur, cursor, rs_cur, log_con)

        log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
        logging.info(log_con)
        logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
        logging.info('################################################################################')
        return status

    def get_rule_info(self, rule_id, cursor):
        '''
        '''
        rule_sql = 'select rule_nm, rule_type, loop_ctr_type, loop_ctr from %s.cntl_rule_type where rule_num = %s and actv_flg = 1'%(self.data_dict["rds_schema_nm"], rule_id)
        status, cursor = self.executeQuery(rule_sql, cursor)
        if status:
            rule_info = cursor.fetchall()
        else:
            sys.exit(cursor)
        return True, rule_info

    def get_query_info(self, rule_id, cursor):
        '''
        '''
        query_info_dict = dict()
        order_list = list()
        rule_sql = 'select qry_id, parm_nm, parm_val, pre_chk_qry_id, on_failr_chk_qry_id, seq from %s.cntl_rule_parm where rule_num = %s and actv_flg = 1 order by seq'%(self.data_dict["rds_schema_nm"], rule_id)

        status, cursor = self.executeQuery(rule_sql, cursor)
        if status:
            query_info_list = cursor.fetchall()
        else:
            sys.exit(cursor)
        for query_info in query_info_list:
            order_list.append((query_info[0], query_info[3], query_info[4], query_info[5]))

            if query_info[5] in query_info_dict:
                query_info_dict[query_info[5]][query_info[1]] = query_info[2]
            else:
                query_info_dict[query_info[5]] =  {query_info[1] : query_info[2], 'query_id':query_info[0], 'pre_chk_qry_id':query_info[3], 'on_failr_chk_qry_id':query_info[4]}


        temp_list = list(set(order_list))
        temp_list.sort()
        return True, query_info_dict, temp_list

    def built_query(self, query_info, order_list, cycl_time_id, scen_id, cursor, rds_cur, inrt_by):
        '''
        '''
        #print ('in query bulit')
        final_query_list = list()
        on_failr_chk_sql_txt = None
        pre_chk_qry_sql_txt = None

        #print query_info
        for seq, query_tup in query_info.items():
            #print '@@@@@@', (seq, query_tup)
            on_failr_chk_sql_txt = None
            pre_chk_qry_sql_txt = None
            status, final_sql = self.get_sql_txt(query_tup['query_id'], cursor, rds_cur, query_tup, cycl_time_id, scen_id, inrt_by)
            #print final_sql
            if query_tup['pre_chk_qry_id']:
                '''
                add exception that is pre-schek is defined but its not present in cntl_etl_Sql
                '''
                status, pre_chk_qry_sql_txt = self.get_sql_txt(query_tup['pre_chk_qry_id'], cursor, rds_cur, query_tup, cycl_time_id, scen_id, inrt_by)
            if query_tup['on_failr_chk_qry_id']:
                status, on_failr_chk_sql_txt = self.get_sql_txt(query_tup['on_failr_chk_qry_id'], cursor, rds_cur, query_tup, cycl_time_id, scen_id, inrt_by)

            final_query_list.append((query_tup['query_id'], final_sql, pre_chk_qry_sql_txt, on_failr_chk_sql_txt))
            #print 'final query list>>>>>>>>' , final_query_list
        #print '>>>>>>>>>>>>>>>Final query list', final_query_list
        return True, final_query_list


    def executeInLoop(self,rule_id, final_query_list, cycl_time_id, scen_id, loop_cnt_type, loop_cnt, run_id, inrt_by, rds_cur, cursor, rs_cur, log_con):
        '''
        '''
        print(" **********************")
        #print final_query_list
        print(loop_cnt_type)
        print(loop_cnt)
        #sys.exit(1)
        if loop_cnt_type.lower() == "constant":
            log_con = self.cmnobj.generate_log_with_dt(log_con,'Mission to execute queries in looping format for constant loop counter')
            status = self.executeInLoopForConst(rule_id,final_query_list,  cycl_time_id, scen_id, loop_cnt_type, loop_cnt,run_id, inrt_by, rds_cur, cursor, rs_cur, log_con)
        elif loop_cnt_type.lower() == "sql":
            log_con = self.cmnobj.generate_log_with_dt(log_con,'Mission to execute queries in looping format for SQL (Date) loop counter')
            status = self.executeInLoopForSQL(rule_id,final_query_list, cycl_time_id, scen_id, loop_cnt_type, loop_cnt,run_id, inrt_by, rds_cur,cursor, rs_cur, log_con)

        return status, log_con


    def executeInLoopForConst(self,rule_num,final_query_list, cycl_time_id, scen_id, loop_cnt_type, loop_cnt,run_id, inrt_by, rds_cur, cursor, rs_cur, log_con):
        '''
        '''
        txt_log = 'Constant loop counter values are %s'%loop_cnt.split(",")
        log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
        for const in loop_cnt.split(","):
            cursor.execute("Begin")
            txt_log = 'Counter value %s is running '%(const)
            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)

            for index, input_query in enumerate(final_query_list):
                sql_id = str(input_query[0])
                pre_sql_txt = input_query[2].replace("'","''") if input_query[2] else ''
                on_failr_sql_txt = input_query[3].replace("'","''") if input_query[3] else ''
                sql_txt = input_query[1].replace("$LOOP_CSTT_OUTP$", str(const)).replace("$loop_cstt_outp$", str(const)).replace("'","''") if input_query[1] else ''
                inrt_query = "insert into comm_ops.cntl_query_log (rule_num,sql_id,pre_sql_txt,sql_txt,on_failr_sql_txt,strt_dt,end_dt,sta,run_id,cycl_time_id,scen_id,cmt) " \
                        "values (%s,%s,'%s','%s','%s',now(),'9999-12-31',2,'%s',%s,%s,'');"%(str(rule_num),sql_id,pre_sql_txt,sql_txt,on_failr_sql_txt,run_id,str(cycl_time_id),str(scen_id))
                status,cur = self.executeQuery(inrt_query,rds_cur)


                txt_log = 'SQL ID : %s is running for above SQL loop count'%(sql_id)
                log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)

                if input_query[2]:
                    status, cursor = self.executeQuery(input_query[2], rs_cur)
                    if status:
                        pre_check_sta = cursor.fetchall()
                        pre_check_sta = pre_check_sta[0][0]
                        if pre_check_sta == 1:
                            sql_qry = input_query[1].replace("$LOOP_CSTT_OUTP$", str(const)).replace("$loop_cstt_outp$", str(const))
                            status, cursor_obj = self.executeQuery(sql_qry, rs_cur)
                            if status:
                                updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=1 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                status, cur = self.executeQuery(updt_stmt,rds_cur)
                                txt_log = "Execution of Query Id = %s is successful"%sql_id
                                log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                pass
                            else:
                                txt_log = 'Query Execution Failed for Query Id: %s '%sql_id
                                log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                if input_query[3]:
                                    txt_log = "Executing On Faliure Query : %s"%on_failr_sql_txt
                                    log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                    status,cursor_obj = self.executeQuery(input_query[3], rs_cur)
                                    #status = self.execute_on_failr(input_query[3],  log_con, cursor_obj)
                                    if status:
                                        txt_log = 'Execution of on failure query for Query Id = %s completed'%sql_id
                                        updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=3 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                        status, cur = self.executeQuery(updt_stmt,rds_cur)
                                        log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                        log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                                        logging.info(log_con)
                                        logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                                        logging.info('################################################################################')
                                        sys.exit(txt_log)
                                    else:
                                        txt_log = 'Execution of on failure query for Query Id = %s failed'%sql_id
                                        updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=4 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                        status, cur = self.executeQuery(updt_stmt,rds_cur)
                                        log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                        log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                                        logging.info(log_con)
                                        logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                                        logging.info('################################################################################')
                                        sys.exit(txt_log)
                                else:
                                    updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=3 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                    status, cur = self.executeQuery(updt_stmt,rds_cur)
                                    sys.exit(cursor_obj)
                        else:
                            '''
                            Out_val = 0 i.e. pre_check failed
                            '''
                            txt_log = "Pre sql text for Query id %s evaluated to 0."%sql_id
                            updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=0 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                            status, cur = self.executeQuery(updt_stmt,rds_cur)
                            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                            log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                            logging.info(log_con)
                            logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                            logging.info('################################################################################')
                            sys.exit(cursor)
                    else:
                        '''
                        Syntax error in pre_sql_txt
                        '''
                        txt_log = 'Execution of pre check query for Query Id = %s failed'%sql_id
                        updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=4 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(str(rule_num),sql_id,str(cycl_time_id),str(scen_id),run_id)
                        status, cur = self.executeQuery(updt_stmt,rds_cur)
                        log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                        log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                        logging.info(log_con)
                        logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                        logging.info('################################################################################')
                        sys.exit(cursor)
                else:
                    sql_qry = input_query[1].replace("$LOOP_CSTT_OUTP$", str(const)).replace("$loop_cstt_outp$", str(const))
                    status, cursor_obj = self.executeQuery(sql_qry, rs_cur)
                    if status:
                        updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=1 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                        status, cur = self.executeQuery(updt_stmt,rds_cur)
                        txt_log = "Execution of Query Id = %s is successful"%sql_id
                        log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                        pass
                    else:
                        #txt_log = 'Query Execution Failed for Query Id: %s '%sql_id
                        #updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=4 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                        #status, cur = self.executeQuery(updt_stmt,rds_cur)
                        #log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                        if input_query[3]:
                            txt_log = "Executing On Faliure Query : %s"%on_failr_sql_txt
                            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                            status,cursor_obj = self.executeQuery(input_query[3], rs_cur)
                            #status = self.execute_on_failr(input_query[3],  log_con, cursor_obj)
                            if status:
                                txt_log = 'Execution of on failure query for Query Id = %s completed'%sql_id
                                updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=3 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                status, cur = self.executeQuery(updt_stmt,rds_cur)
                                log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                                logging.info(log_con)
                                logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                                logging.info('################################################################################')
                                sys.exit(txt_log)
                            else:
                                txt_log = 'Execution of on failure query for Query Id = %s failed'%sql_id
                                updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=4 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                status, cur = self.executeQuery(updt_stmt,rds_cur)
                                log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                                logging.info(log_con)
                                logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                                logging.info('################################################################################')
                                sys.exit(txt_log)
                        else:
                             updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=3 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                             status, cur = self.executeQuery(updt_stmt,rds_cur)
                             txt_log = "Execution of Query Id = %s Failed"%sql_id
                             log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                             log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                             logging.info(log_con)
                             logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                             logging.info('################################################################################')
                             sys.exit(txt_log)
                             sys.exit(cursor_obj)
            rs_cur.execute("Commit")
        logging.info(log_con)
        return True


    def executeInLoopForSQL(self,rule_num, final_query_list, cycl_time_id, scen_id, loop_cnt_type, loop_cnt,run_id, inrt_by, rds_cur, cursor, rs_cur, log_con):
        '''
        '''
        txt_log = 'SQL for loop is : %s'%loop_cnt
        log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
        #cursor = mydb.cursor()
        status, rs_cur = self.executeQuery(loop_cnt, rs_cur)

        if status:
            sql_loop_cnt = rs_cur.fetchall()
            for const in sql_loop_cnt:
                cursor.execute("Begin")
                txt_log = 'Counter value %s is running '%(const)
                log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)


                for index, input_query in enumerate(final_query_list):
                    sql_id = str(input_query[0])
                    pre_sql_txt = input_query[2].replace("'","''") if input_query[2] else ''
                    on_failr_sql_txt = input_query[3].replace("'","''") if input_query[3] else ''
                    sql_txt = input_query[1].replace("$LOOP_CSTT_OUTP$", str(const[0])).replace("$loop_cstt_outp$", str(const[0])).replace("'","''") if input_query[1] else ''
                    inrt_query = "insert into comm_ops.cntl_query_log (rule_num,sql_id,pre_sql_txt,sql_txt,on_failr_sql_txt,strt_dt,end_dt,sta,run_id,cycl_time_id,scen_id,cmt) " \
                        "values (%s,%s,'%s','%s','%s',now(),'9999-12-31',2,'%s',%s,%s,'');"%(str(rule_num),sql_id,pre_sql_txt,sql_txt,on_failr_sql_txt,run_id,str(cycl_time_id),str(scen_id))
                    status,cur = self.executeQuery(inrt_query,rds_cur)

                    txt_log = 'SQL ID : %s is running for above SQL loop count'%(sql_id)
                    log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                    if input_query[2]:
                        status, cursor = self.executeQuery(input_query[2], rs_cur)
                        if status:
                            pre_check_sta = cursor.fetchall()
                            pre_check_sta = pre_check_sta[0][0]
                            if pre_check_sta == 1:
                                sql_qry = input_query[1].replace("$LOOP_CSTT_OUTP$", str(const[0])).replace("$loop_cstt_outp$", str(const[0]))
                                status, cursor = self.executeQuery(sql_qry, rs_cur)
                                if status:
                                    updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=1 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                    status, cur = self.executeQuery(updt_stmt,rds_cur)
                                    txt_log = "Execution of Query Id = %s is successful"%sql_id
                                    log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                    pass
                                else:
                                    txt_log = 'Query Execution Failed for Query Id: %s '%sql_id
                                    log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                    if input_query[3]:
                                        txt_log = "Executing On Faliure Query : %s"%on_failr_sql_txt
                                        log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                        status,cursor_obj = self.executeQuery(input_query[3], rs_cur)
                                        if status:
                                            txt_log = 'Execution of on failure query for Query Id = %s completed'%sql_id
                                            updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=3 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                            status, cur = self.executeQuery(updt_stmt,rds_cur)
                                            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                            log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                                            logging.info(log_con)
                                            logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                                            logging.info('################################################################################')
                                            sys.exit(txt_log)
                                        else:
                                            txt_log = 'Execution of on failure query for Query Id = %s failed'%sql_id
                                            updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=4 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                            status, cur = self.executeQuery(updt_stmt,rds_cur)
                                            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                            log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                                            logging.info(log_con)
                                            logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                                            logging.info('################################################################################')
                                            sys.exit(txt_log)
                                    else:
                                        updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=3 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                        status, cur = self.executeQuery(updt_stmt,rds_cur)
                                        sys.exit(cursor_obj)
                            else:
                                '''
                                Out_val = 0 i.e. pre_check failed
                                '''
                                txt_log = "Pre sql text for Query id %s evaluated to 0."%sql_id
                                updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=0 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                status, cur = self.executeQuery(updt_stmt,rds_cur)
                                log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                                logging.info(log_con)
                                logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                                logging.info('################################################################################')
                                sys.exit(cursor)
                        else:
                            '''
                            Syntax error in pre_sql_txt
                            '''
                            txt_log = 'Execution of pre check query for Query Id = %s failed'%sql_id
                            updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=4 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(str(rule_num),sql_id,str(cycl_time_id),str(scen_id),run_id)
                            status, cur = self.executeQuery(updt_stmt,rds_cur)
                            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                            log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                            logging.info(log_con)
                            logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                            logging.info('################################################################################')
                            sys.exit(cursor)
                    else:
                        sql_qry = input_query[1].replace("$LOOP_CSTT_OUTP$", str(const[0])).replace("$loop_cstt_outp$", str(const[0]))
                        status, cursor_obj = self.executeQuery(sql_qry, rs_cur)
                        if status:
                            updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=1 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                            status, cur = self.executeQuery(updt_stmt,rds_cur)
                            txt_log = "Execution of Query Id = %s is successful"%sql_id
                            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                            pass
                        else:
                            txt_log = 'Query Execution Failed for Query Id: %s '%sql_id
                            #updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=4 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                            #status, cur = self.executeQuery(updt_stmt,rds_cur)
                            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                            if input_query[3]:
                                txt_log = "Executing On Faliure Query : %s"%on_failr_sql_txt
                                log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                #status = self.execute_on_failr(input_query[3],  log_con, cursor_obj)
                                status,cursor_obj = self.executeQuery(input_query[3], rs_cur)
                                if status:
                                    txt_log = 'Execution of on failure query for Query Id = %s completed'%sql_id
                                    updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=3 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                    status, cur = self.executeQuery(updt_stmt,rds_cur)
                                    log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                    log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                                    logging.info(log_con)
                                    logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                                    logging.info('################################################################################')
                                    sys.exit(txt_log)
                                else:
                                    txt_log = 'Execution of on failure query for Query Id = %s failed'%sql_id
                                    updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=4 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                    status, cur = self.executeQuery(updt_stmt,rds_cur)
                                    log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                    log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                                    logging.info(log_con)
                                    logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                                    logging.info('################################################################################')
                                    sys.exit(txt_log)
                            else:
                                updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=3 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                status, cur = self.executeQuery(updt_stmt,rds_cur)
                                log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                                logging.info(log_con)
                                logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                                logging.info('################################################################################')
                                sys.exit(cursor_obj)

                rs_cur("Commit")
        else:
            txt_log = "please correct counter SQL query"
            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
            log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
            logging.info(log_con)
            logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
            logging.info('################################################################################')
            sys.exit(txt_log)
        logging.info(log_con)
        return True


    def executeSerially(self, rule_num, final_query_list, cycl_time_id, scen_id, run_id, inrt_by, rds_cur, cursor, rs_cur, log_con):
        '''
        '''
        print("**************************")
        #print final_query_list
        #cursor.execute("Begin")
        for input_query in final_query_list:
            sql_id = str(input_query[0])
            pre_sql_txt = input_query[2].replace("'","''") if input_query[2] else ''
            on_failr_sql_txt = input_query[3].replace("'","''") if input_query[3] else ''
            sql_txt = input_query[1].replace("'","''") if input_query[1] else ''
            inrt_query = "insert into comm_ops.cntl_query_log (rule_num,sql_id,pre_sql_txt,sql_txt,on_failr_sql_txt,strt_dt,end_dt,sta,run_id,cycl_time_id,scen_id,cmt) " \
                        "values (%s,%s,'%s','%s','%s',now(),'9999-12-31',2,'%s',%s,%s,'');"%(str(rule_num),sql_id,pre_sql_txt,sql_txt,on_failr_sql_txt,run_id,str(cycl_time_id),str(scen_id))
            status,cur = self.executeQuery(inrt_query,rds_cur)
            print('input_query--->', input_query)
            if input_query[2]:
                status, cursor_obj = self.executeQuery(input_query[2], rs_cur)
                if status:
                   pre_check_sta = cursor.fetchall()
                   pre_check_sta = pre_check_sta[0][0]
                   if pre_check_sta == 1:
                        status, cursor_obj = self.executeQuery(input_query[1], rs_cur)
                        if status:
                           updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=1 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                           status, cur = self.executeQuery(updt_stmt,rds_cur)
                           txt_log = "Execution of Query Id = %s is successful"%sql_id
                           log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                           pass
                        else:
                           txt_log = 'Query Execution Failed for Query Id: %s '%sql_id
                           log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                           if input_query[3]:
                               txt_log = "Executing On Faliure Query : %s"%on_failr_sql_txt
                               log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                               status,cursor_obj = self.executeQuery(input_query[3], rs_cur)
                               if status:
                                  txt_log = 'Execution of on failure query for Query Id = %s completed'%sql_id
                                  updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=3 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                  status, cur = self.executeQuery(updt_stmt,rds_cur)
                                  log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                  log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                                  logging.info(log_con)
                                  logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                                  logging.info('################################################################################')
                                  sys.exit(txt_log)
                               else:
                                  txt_log = 'Execution of on failure query for Query Id = %s failed'%sql_id
                                  updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=4 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                                  status, cur = self.executeQuery(updt_stmt,rds_cur)
                                  log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                                  log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                                  logging.info(log_con)
                                  logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                                  logging.info('################################################################################')
                                  sys.exit(txt_log)
                           else:
                               updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=3 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                               status, cur = self.executeQuery(updt_stmt,rds_cur)
                               sys.exit(cursor_obj)
                   else:
                        '''
                        Out_val = 0 i.e. pre_check failed
                        '''
                        txt_log = "Pre sql text for Query id %s evaluated to 0."%sql_id
                        updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=0 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                        status, cur = self.executeQuery(updt_stmt,rds_cur)
                        log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                        log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                        logging.info(log_con)
                        logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                        logging.info('################################################################################')
                        sys.exit(txt_log)
                else:
                   '''
                   Syntax error in pre_sql_txt
                   '''
                   txt_log = 'Execution of pre check query for Query Id = %s failed'%sql_id
                   updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=4 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(str(rule_num),sql_id,str(cycl_time_id),str(scen_id),run_id)
                   status, cur = self.executeQuery(updt_stmt,rds_cur)
                   log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                   log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                   logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                   logging.info('################################################################################')
                   sys.exit(cursor)
            else:
                status, cursor_obj = self.executeQuery(input_query[1], rs_cur)
                if status:
                    updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=1 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                    status, cur = self.executeQuery(updt_stmt,rds_cur)
                    txt_log = "Execution of Query Id = %s is successful"%sql_id
                    log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                    pass
                else:
                    txt_log = 'Query Execution Failed for Query Id : %s'%sql_id
                    updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=4 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                    status, cur = self.executeQuery(updt_stmt,rds_cur)
                    log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                    if input_query[3]:
                        txt_log = "Executing On Faliure Query : %s"%on_failr_sql_txt
                        log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                        #status = self.execute_on_failr(input_query[3], log_con, cursor_obj)
                        status,cursor_obj = self.executeQuery(input_query[3], rs_cur)
                        if status:
                            txt_log = 'Execution of on failure query for Query Id = %s completed'%sql_id
                            updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=3 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                            status, cur = self.executeQuery(updt_stmt,rds_cur)
                            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                            log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                            logging.info(log_con)
                            logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                            logging.info('################################################################################')
                            sys.exit(txt_log)
                        else:
                            txt_log = 'Execution of on failure query for Query Id = %s failed'%sql_id
                            updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=4 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                            status, cur = self.executeQuery(updt_stmt,rds_cur)
                            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
                            log_con = self.cmnobj.generate_log_with_dt(log_con, 'Job %s ended'%sys.argv[0])
                            logging.info(log_con)
                            logging.info('\n\n-----------------------------END OF PROCESSING----------------------------------')
                            logging.info('################################################################################')
                            sys.exit(txt_log)
                    else:
                        updt_stmt = "update comm_ops.cntl_query_log set end_dt='now()', sta=3 where rule_num=%s and sql_id=%s and cycl_time_id=%s and scen_id=%s and end_dt='9999-12-31 00:00:00' and run_id='%s';"%(rule_num,sql_id,str(cycl_time_id),str(scen_id),run_id)
                        status, cur = self.executeQuery(updt_stmt,rds_cur)
                        sys.exit(cursor_obj)
        rs_cur.execute("Commit")


        return status, log_con

    def execute_on_failr(self, input_query,  log_con, cursor_obj):
        '''
        '''
        status, cursor = self.executeQuery(input_query, cursor_obj)
        if status:
            txt_log = 'On failure query %s completed, sys.exiting scripts.'%input_query
            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
            logging.info(log_con)
            sys.exit(cursor_obj)
        else:
            txt_log = 'On failure query %s failed, sys.exiting scripts.'%input_query
            log_con = self.cmnobj.generate_log_with_dt(log_con, txt_log)
            logging.info(log_con)
            sys.exit(cursor)

    def get_sql_txt(self, sql_id, cursor, rds_cur, query_dict, cycl_time_id, scen_id, inrt_by):
        '''
        '''
        if query_dict:
            param_nm_list = list(query_dict.keys())
            print(param_nm_list)
            print("parm name list")
        cntl_etl_query = 'select sql_txt from %s.cntl_etl_sql where sql_id = %s' % (
        self.data_dict["rs_schema_nm"], sql_id)
        print (cntl_etl_query)
        status, cursor = self.executeQuery(cntl_etl_query, rds_cur)

        if status:
            print(status)
            query_info = cursor.fetchall()
        else:
            sys.exit(cursor)
        # print '@@@@@', query_info
        sql_txt = query_info[0][0]
        print(sql_txt)
        #if sql_txt and param_nm_list[0] is not None:
        if sql_txt and None not in param_nm_list:
            while any(s in sql_txt for s in param_nm_list):
                for k, v in query_dict.items():
                    sql_txt = sql_txt.replace(str(k), str(v))
            # sql_txt  = sql_txt.replace("$CYCL_TIME_ID$", cycl_time_id).replace("$SCEN_ID$", scen_id).replace("$INRT_BY$","'"+inrt_by+"'").replace("$cycl_time_id$", cycl_time_id).replace("$scen_id$", scen_id).replace("$inrt_by$","'"+inrt_by+"'")
        sql_txt = sql_txt.replace("$CYCL_TIME_ID$", cycl_time_id).replace("$SCEN_ID$", scen_id).replace("$INRT_BY$",inrt_by).replace("$cycl_time_id$", cycl_time_id).replace("$scen_id$", scen_id).replace("$inrt_by$", inrt_by)
        print(sql_txt)
        return True, sql_txt


    def executeQuery(self, sql, cursor):
        '''
        '''
        try:
            #print "sql----->" , sql
            cursor.execute(sql)
            return True, cursor
        except:
            e = str(traceback.format_exc())
            #print "test2", e
            logging.info('SQL Query Failed: %s'%sql)
            logging.error(e)
            return False, e


if __name__ == '__main__':
    cycl_time_id = sys.argv[1]
    scen_id = sys.argv[2]
    inrt_by = sys.argv[3]
    rule_id = sys.argv[4]
    obj = QeuryProcessor()
    status = obj.process(cycl_time_id, scen_id, inrt_by, rule_id)



