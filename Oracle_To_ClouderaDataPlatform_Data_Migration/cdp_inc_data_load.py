#!/usr/bin/python3

import datetime as dt
import subprocess as sp
import cx_Oracle as db
import pandas as pd
import impala.dbapi as impala
import config_cdp_inc_data_load as cfg
import logging, os, urllib, sys

## Variable Initialize
CURRENT_DATETIME = dt.datetime.now()
CURRENT_DATE = dt.date.today()
CURRENT_HOUR = CURRENT_DATETIME.strftime('%H%M')
#CURRENT_DIRECTORY = os.getcwd() + '/' /data9/scripts/cdp_data_migration_framework
CURRENT_DIRECTORY = r'/data9/scripts/cdp_data_migration_framework/'

## Log Variables
LOG_DIR = CURRENT_DIRECTORY + 'logs/'
LOG_FILE = LOG_DIR+'log_cdp_data_import_job_'+CURRENT_DATE.strftime('%Y%m%d')+'.txt'
JAVA_CLASS_DIR = CURRENT_DIRECTORY+'class_dir'

SCRIPT_RUNNING_FLAG = CURRENT_DIRECTORY+'data_import_job_running_flag'
FLAG_CREATE_TIME_THRESHOLD = 7 ## In Hours
PROCESS_START_TIME_THRESHOLD = 1 ## In Hours

logging.basicConfig(filename=LOG_FILE, filemode='a+', format='%(asctime)s|%(message)s', datefmt='%Y-%m-%d %H:%M:%S')


def shell_cmd_exec(cmd):
    try:
        process=sp.Popen(cmd,  shell=True, stderr=sp.PIPE, stdout=sp.PIPE)
        stdout_value, stderr_value= process.communicate()

        if process.returncode != 0:
            logging.warning(cmd+' - '+stderr_value)
        else:
            logging.warning(cmd+' - Successful')
        return stdout_value.strip()
    except Exception as e:
        handleException('Shell Command Execute Failed For command: '+cmd,e)


def get_db_conn():
    try:
        conn = db.connect(cfg.db_user, cfg.db_pass, cfg.db_conn_str)
        curs = conn.cursor()
        return curs, conn
    except Exception as e:
        handleException('Get Oracle Conn Error',e)

def get_cdp_conn():
    try:
        conn = impala.connect(host=cfg.cdp_db_host, port=cfg.cdp_db_port, auth_mechanism=cfg.cdp_auth_mechanism, use_ssl=cfg.cdp_ssl,\
        kerberos_service_name=cfg.cdp_kerberos_service_name, user=cfg.cdp_user)
        curs = conn.cursor()
        return curs, conn
    except Exception as e:
        handleException('Get CDP Conn Error',e)

def generate_tab_create_query(tab_info = {}, datadate = 0):
    if tab_info:
        ## Storing current table information into variables
        table_name = tab_info.get('SOURCE_TABLE')
        schema_name = tab_info.get('SOURCE_SCHEMA')
        partition_type = tab_info.get('PARTITION_TYPE')
        partition_prefix = tab_info.get('PARTITION_PREFIX')
        staging_table = tab_info.get('DWH_STAGING_TABLE')
        staging_schema = tab_info.get('DWH_STAGING_SCHEMA')
        partition_category = tab_info.get('PARTITION_CATEGORY')

        curs, conn = get_db_conn()
        try:
            ## Generating column list for staging table creation
            query_col_list = cfg.sql_tab_col_list.format(tab_name = table_name, tab_schema = schema_name)
            curs.execute(query_col_list)
            cols = curs.fetchall()[0][0]

        except db.DatabaseError as e:
            handleException("Column List Fetch Error For Table : {} - ".format(table_name),e)

        ## Check if partition type exists or not
        if partition_type:
            if partition_category == 'DAILY':
                query_partition = cfg.var_tab_create_footer.format(tab_part_type = partition_type, tab_part_prefix=partition_prefix, data_date = datadate)
            else:
                datadate = datadate[:6]
                query_partition = cfg.var_tab_create_footer.format(tab_part_type = partition_type, tab_part_prefix=partition_prefix, data_date = datadate)
        else:
            query_partition = ''

        query_header = cfg.var_tab_create_header.format(temp_tab_schema = staging_schema, temp_tab = staging_table)
        query_select = cfg.var_tab_create_select_query.format(tab_column_list = cols, source_schema = schema_name, tab_name = table_name)
        final_query = query_header + query_select + query_partition
        conn.close()

        return final_query
    else:
        logging.warning("Generate table create information is null")

def generate_insert_query(tab_info, datadate):
    if tab_info:
        curs, conn = get_db_conn()

        final_table_name = tab_info.get('CDP_FINAL_TABLE')
        final_table_db = tab_info.get('CDP_FINAL_DB')
        source_table_name = tab_info.get('SOURCE_TABLE')
        source_schema_name = tab_info.get('SOURCE_SCHEMA')
        partition_column = tab_info.get('PARTITION_COLUMN')
        cdp_landing_db = tab_info.get('CDP_LANDING_DB');
        cdp_landing_table = tab_info.get('CDP_LANDING_TABLE');
        is_partitioned_table = int(tab_info.get('IS_PARTITIONED'))
        try:
            ## Drop partition or truncate table befor loading data
            if is_partitioned_table == 1:
                cdp_table_truncate('final',tab_info,datadate)

                ## getting column list
                curs.execute(cfg.sql_cdp_column_list.format(table_name=source_table_name,schema_name=source_schema_name))
                cols = curs.fetchall()[0][0]

                query_header = cfg.var_tab_insert_header.format(db_name=final_table_db,table_name=final_table_name)
                query_select_block = cfg.var_tab_insert_partitioned_select.format(tab_column_list=cols,partition_column=partition_column, \
                    landing_db=cdp_landing_db,landing_table=cdp_landing_table)
            else:
                cdp_table_truncate('final',tab_info)

                ## getting column list
                curs.execute(cfg.sql_cdp_column_list.format(table_name=source_table_name,schema_name=source_schema_name))
                cols = curs.fetchall()[0][0]

                query_header = cfg.var_tab_insert_header.format(db_name=final_table_db,table_name=final_table_name)
                query_select_block = cfg.var_tab_insert_nonpartitioned_select.format(tab_column_list=cols, landing_db=cdp_landing_db, landing_table=cdp_landing_table)
        except Exception as e:
            handleException('Generate Insert Query Failed For Table {} - '.format(final_table_name),e)

        final_query = query_header + query_select_block
        conn.close()

        return final_query


def create_staging_table(tab_info,datadate):
    try:
        staging_table = tab_info.get('DWH_STAGING_TABLE')
        tab_create_query = generate_tab_create_query(tab_info,datadate)
        curs, conn = get_db_conn()
        curs.execute(cfg.sql_drop_table_query.format(drop_temp_tab=staging_table))
        curs.execute(tab_create_query)
        conn.close()
        logging.warning('Staging Table Create Successful: {} - '.format(staging_table))

    except db.DatabaseError as e:
        handleException('Staging Table Create Failed For Table: {} - '.format(staging_table),e)

def run_sqoop_job(tab_info = {}):
    if tab_info:
        staging_table = tab_info.get('DWH_STAGING_TABLE')
        staging_schema = tab_info.get('DWH_STAGING_SCHEMA')
        landing_db = tab_info.get('CDP_LANDING_DB')
        landing_table = tab_info.get('CDP_LANDING_TABLE')
        ## Truncate landing table before data insert
        cdp_table_truncate('landing',tab_info)

        ## Preparing sqoop command
        sqoop_command = cfg.var_sqoop_command.format(oracle_staging_db=staging_schema, oracle_staging_table = staging_table, \
            cdp_landing_db = landing_db, cdp_landing_table = landing_table)

        os.chdir(JAVA_CLASS_DIR)
        shell_cmd_exec(sqoop_command)
        os.chdir(CURRENT_DIRECTORY)

def run_beeline_query(query):
    try:
        shell_cmd_exec(cfg.beeline_command.format(sql_query=query))
    except Exception as e:
        handleException('Beeline Query Failed For Query: {} - '.format(query),e)

def tab_record_count_check(tab_type,tab_info,data_date=0):
    if tab_info:
        try:
            if tab_type == 'staging':
                staging_schema = tab_info.get('DWH_STAGING_SCHEMA')
                staging_table = tab_info.get('DWH_STAGING_TABLE')
                query = cfg.sql_tab_record_count.format(db_name = staging_schema, table_name = staging_table)
                curs, conn = get_db_conn()

                curs.execute(query)
                record_count = curs.fetchall()[0][0]
                conn.close()
                return record_count
            elif tab_type == 'landing':
                landing_db = tab_info.get('CDP_LANDING_DB');
                landing_table = tab_info.get('CDP_LANDING_TABLE');
                query = cfg.sql_tab_record_count.format(db_name = landing_db, table_name = landing_table)

                curs, conn = get_cdp_conn()
                curs.execute(query)
                record_count = curs.fetchall()[0][0]
                conn.close()
                return record_count
            elif tab_type == 'final':
                final_table_name = tab_info.get('CDP_FINAL_TABLE')
                final_table_db = tab_info.get('CDP_FINAL_DB')
                partition_column = tab_info.get('PARTITION_COLUMN')
                if partition_column:
                    query = cfg.sql_tab_record_count_partitioned.format(db_name = final_table_db, table_name = final_table_name, \
                        partition_col = partition_column, datadate=data_date)
                else:
                    query =  cfg.sql_tab_record_count.format(db_name = final_table_db, table_name = final_table_name)

                curs, conn = get_cdp_conn()
                curs.execute(query)
                record_count = curs.fetchall()[0][0]
                conn.close()
                return record_count
        except Exception as e:
            handleException('Table Record Count Check Failed For Table: {} - '.format(tab_type),e)
        else:
            return 0

def import_job_log_insert(tab_info, datadate, stg_rec, lnd_rec, fin_rec):
    if tab_info:
        try:
            table_name = tab_info.get('SOURCE_TABLE')
            schema_name = tab_info.get('SOURCE_SCHEMA')

            query = cfg.sql_import_log_insert.format(source_table=table_name, source_schema = schema_name, data_date = datadate, \
                staging_rec = stg_rec, landing_rec = lnd_rec, final_rec = fin_rec)

            curs, conn = get_db_conn()
            curs.execute(query)
            conn.commit()
            conn.close()
        except Exception as e:
           handleException('Insert Job Log Failed For Table: {} - '.format(table_name),e)

def cdp_table_truncate(tab_type, tab_info, datadate=0):
    if tab_info:
        final_table_name = tab_info.get('CDP_FINAL_TABLE')
        final_table_db = tab_info.get('CDP_FINAL_DB')
        partition_column = tab_info.get('PARTITION_COLUMN')
        cdp_landing_db = tab_info.get('CDP_LANDING_DB')
        cdp_landing_table = tab_info.get('CDP_LANDING_TABLE')
        is_partitioned_table = int(tab_info.get('IS_PARTITIONED'))
        try:
            if tab_type=='final':
                if is_partitioned_table == 1:
                    query = cfg.sql_cdp_drop_table_partition.format(db_name=final_table_db,table_name=final_table_name,\
                        partition_column=partition_column,partition_name=datadate)
                    run_beeline_query(query)
                else:
                    query = cfg.sql_truncate_table.format(db_name=final_table_db,table_name=final_table_name)
                    run_beeline_query(query)
            else:
                query = cfg.sql_truncate_table.format(db_name=cdp_landing_db,table_name=cdp_landing_table)
                run_beeline_query(query)
        except Exception as e:
            handleException('Truncate Table Failed For Table Type: {} - '.format(tab_type),e)

def send_sms_notification(message):
    api_endpoint = cfg.sms_api_dev
    contents = {'head': cfg.sms_header, 'msg':message}
    contents = urllib.parse.urlencode(contents,doseq=True)

    urllib.request.urlopen(api_endpoint+'?'+contents)


def load_data_into_cdp():
    cursor, conn = get_db_conn()
    ## Get table list for data migration -- condition current time > table load time and not yet processed
    target_tables = pd.read_sql(cfg.sql_table_list_select.format(curr_time = CURRENT_HOUR),con=conn)

    logging.warning('Total Count Of Tables For Current Job: '+str(len(target_tables.index)))
    if len(target_tables.index) > 0:
        ## Looping through all tables for data import
        for i in target_tables.index:
            ## storing current row in dictionary format for better usability
            curr_table = target_tables.loc[i].to_dict()

            if curr_table.get('PARTITION_CATEGORY') == 'MONTHLY' and int(CURRENT_DATE.strftime('%d')) != int(curr_table.get('DATA_UPDATE_INTERVAL')):
                continue ## Skipping Monthly table if contitions are True

            logging.warning('Current Table For Data Import Job: {} - '.format(curr_table.get('SOURCE_SCHEMA')+'.'+curr_table.get('SOURCE_TABLE')))
            ## Checking backlog CDR date
            if curr_table.get('BACKDATED_CDR_NUM_DAY') <=1 or curr_table.get('IS_PARTITIONED') == 0: ## if no backlog configured then proceed with normal execution

                data_update_day = int(curr_table.get('DATA_UPDATE_INTERVAL')) ## Data Update Day eg. sysdate or sysdate-1

                ## Checking if it is monthly table then data date will be current month - data update interval
                if curr_table.get('PARTITION_CATEGORY') == 'MONTHLY':
                    last_day_prev_mon = CURRENT_DATE.replace(day=1) - dt.timedelta(days=1)
                    datadate = last_day_prev_mon.strftime('%Y%m%d')
                else:
                    datadate = (CURRENT_DATE - dt.timedelta(days=data_update_day)).strftime('%Y%m%d')
                ## creating oracle staging table
                create_staging_table(curr_table,datadate)

                ## getting record count from staging table
                staging_tab_record_count = tab_record_count_check('staging',curr_table)

                if staging_tab_record_count > 0:

                    ## Running sqoop job for data migration to CDP
                    run_sqoop_job(curr_table)

                    ## landing table record count check
                    landing_tab_record_count = tab_record_count_check('landing',curr_table)

                    if int(staging_tab_record_count) != int(landing_tab_record_count):
                        message = 'Landing Table Record Count Mismatch For Table: {}'.format(curr_table.get('SOURCE_SCHEMA')+'.'+curr_table.get('SOURCE_TABLE'))
                        logging.warning(message)
                        send_sms_notification(message)
                        continue ## Skipping the job if record count doesn't match
                    else:
                        insert_query = generate_insert_query(curr_table,datadate)
                        run_beeline_query(insert_query)
                        ## Final Fact Table Record Count Check
                        final_tab_record_count = tab_record_count_check('final',curr_table, datadate)

                        ## Final table record count check
                        if int(landing_tab_record_count) != int(final_tab_record_count):
                            message = 'Final Table Record Count Mismatch For Table: {}'.format(curr_table.get('SOURCE_SCHEMA')+'.'+curr_table.get('SOURCE_TABLE'))
                            logging.warning(message)
                            send_sms_notification(message)
                            continue ## Skipping the job if record count doesn't match
                        else:
                            import_job_log_insert(curr_table,datadate,staging_tab_record_count,landing_tab_record_count,final_tab_record_count)
                            logging.warning('Import Job Successfull For Table: {}'.format(curr_table.get('SOURCE_SCHEMA')+'.'+curr_table.get('SOURCE_TABLE')))
                            flag_time_update()
                else:
                    logging.warning('No Record Found for Table: '+curr_table.get('SOURCE_TABLE')+' And DataDate: '+datadate)

            else:
                ## generate date range for refreshing backlog data
                data_update_day = int(curr_table.get('DATA_UPDATE_INTERVAL'))
                backlog_cdr_date = int(curr_table.get('BACKDATED_CDR_NUM_DAY'))
                start_date = CURRENT_DATE - dt.timedelta(days=backlog_cdr_date)
                end_date = CURRENT_DATE - dt.timedelta(days=data_update_day)
                data_date_list = pd.date_range(start=start_date,end=end_date).to_pydatetime().tolist()
                for date in data_date_list:
                    datadate = date.strftime('%Y%m%d')
                    ## creating oracle staging table
                    create_staging_table(curr_table,datadate)

                    ## getting record count from staging table
                    staging_tab_record_count = tab_record_count_check('staging',curr_table)

                    if staging_tab_record_count > 0:

                        ## Running sqoop job for data migration to CDP
                        run_sqoop_job(curr_table)

                        ## landing table record count check
                        landing_tab_record_count = tab_record_count_check('landing',curr_table)

                        if int(staging_tab_record_count) != int(landing_tab_record_count):
                            message = 'Landing Table Record Count Mismatch For Table: {}'.format(curr_table.get('SOURCE_SCHEMA')+'.'+curr_table.get('SOURCE_TABLE'))
                            logging.warning(message)
                            send_sms_notification(message)
                            continue ## Skipping the job if record count doesn't match
                        else:
                            insert_query = generate_insert_query(curr_table,datadate)
                            run_beeline_query(insert_query)
                            ## Final Fact Table Record Count Check
                            final_tab_record_count = tab_record_count_check('final',curr_table, datadate)

                            ## Final table record count check
                            if int(landing_tab_record_count) != int(final_tab_record_count):
                                message = 'Final Table Record Count Mismatch For Table: {}'.format(curr_table.get('SOURCE_SCHEMA')+'.'+curr_table.get('SOURCE_TABLE'))
                                logging.warning(message)
                                send_sms_notification(message)
                                continue ## Skipping the job if record count doesn't match
                            else:
                                import_job_log_insert(curr_table,datadate,staging_tab_record_count,landing_tab_record_count,final_tab_record_count)
                                logging.warning('Import Job Successfull For Table: {}'.format(curr_table.get('SOURCE_SCHEMA')+'.'+curr_table.get('SOURCE_TABLE')))
                                flag_time_update()
                    else:
                        logging.warning('No Record Found for Table: '+curr_table.get('SOURCE_TABLE')+' And DataDate: '+datadate)
    else:
       logging.warning('No Tables Found For Import Job')

def flag_time_update():
    flag_file = open(SCRIPT_RUNNING_FLAG,'w')
    flag_file.write(dt.datetime.now().strftime('%Y%m%d %H%M%S'))
    flag_file.close()

def handleException(message, e, sms_flag = 0, exit_flag = 0):
    logging.warning(message+' - '+str(e))

    if sms_flag == 1:
        send_sms_notification('CDP: '+message)

    if exit_flag == 1:
        sys.exit(1)

def housekeep_log_dir():
    os.chdir(LOG_DIR)
    log_list = os.listdir()
    log_delete_date = CURRENT_DATE - dt.timedelta(days=cfg.log_delete_date)
    for log_file in log_list:
        if os.path.isfile(log_file):
            try:
                log_date = dt.datetime.strptime(log_file.split('_')[-1].split('.')[0],'%Y%m%d').date()
                if  log_date <= log_delete_date:
                    os.remove(log_file)
            except Exception as e:
                handleException('Log File Remove Failed For File: {} - '.format(log_file),e)

if __name__ == '__main__':
    ## Running Flag File Check
    print('script execution started. - '+CURRENT_DATETIME.strftime('%Y%m%d %H%M%S'))
    if os.path.isfile(SCRIPT_RUNNING_FLAG) == False:
        flag_file = open(SCRIPT_RUNNING_FLAG,'w')
        flag_file.write(dt.datetime.now().strftime('%Y%m%d %H%M%S'))
        flag_file.close()
        ## Execute main process for data import
        try:
            load_data_into_cdp()
            housekeep_log_dir()
        except Exception as e:
            handleException('Process execution Failed',e,1,1)
        if os.path.isfile(SCRIPT_RUNNING_FLAG) == True:
            os.remove(SCRIPT_RUNNING_FLAG)
    else:
        ## Flag Creation Time Check
        flag_create_time = dt.datetime.fromtimestamp(os.stat(SCRIPT_RUNNING_FLAG).st_ctime)
        flag_time_threshold = dt.datetime.today() - dt.timedelta(hours=FLAG_CREATE_TIME_THRESHOLD)
        process_exec_theshold = dt.datetime.today() - dt.timedelta(hours=PROCESS_START_TIME_THRESHOLD)

        with open (SCRIPT_RUNNING_FLAG,'r') as flag_file:
            file_time = flag_file.readline()
            process_start_time =dt.datetime.strptime(file_time.strip(),'%Y%m%d %H%M%S')

        if (flag_create_time < flag_time_threshold) or (process_start_time < process_exec_theshold):
            handleException('Data Migration Process Flag Error.',None,1)

        print('Previous Process is still executing. - '+CURRENT_DATETIME.strftime('%Y%m%d %H%M%S'))
    print('script execution ended. - '+CURRENT_DATETIME.strftime('%Y%m%d %H%M%S'))[