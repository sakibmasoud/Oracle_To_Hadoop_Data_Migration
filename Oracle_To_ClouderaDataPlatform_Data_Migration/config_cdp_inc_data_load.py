## Oracle DB Connection Details
db_user = '####'
db_pass = '####'
db_conn_str = '####'

## CDP Connection Details
cdp_db_host ='####'
cdp_db_port=10001
cdp_auth_mechanism='####'
cdp_ssl='true'
cdp_kerberos_service_name='hive'
cdp_user='####'

## SQOOP Parameters
sqoop_connection_string = '####'
sqoop_data_format='stored as orc'
sqoop_mappers = '80'

## SMS API Config
sms_api_prod = '####'
sms_api_dev = '####'
sms_header = 'EDW'

## Housekeep Log
log_delete_date = 30


## Beeline Command
beeline_command = "beeline -u \"jdbc:hive2://host:10001/default;"\
        +"principal=hive/host@domain;ssl=true;"\
        +"sslTrustStore=/opt/cloudera/security/pki/truststore.jks;"\
        +"SSLTrustStorePwd=password\" " \
        + "-e \"set hive.exec.dynamic.partition.mode=nonstrict;"\
        +" set hive.execution.engine=tez; \n {sql_query} ;\""


## SQL Variables
sql_table_list_select="SELECT * FROM DIMENSION.TBL_DIM_CDP_DATA_MIG_TAB_LIST WHERE IS_ACTIVE = 1 AND POPULATION_TIME <= {curr_time}"\
        +" AND SOURCE_SCHEMA||'.'||SOURCE_TABLE NOT IN "\
        +"(SELECT DISTINCT SOURCE_SCHEMA||'.'||SOURCE_TABLE FROM DWADM.TBL_CDP_INC_DATA_MIG_LOG WHERE TRUNC(INSERT_DATETIME) = TRUNC(SYSDATE) AND STATUS = 1)"

sql_tab_col_list="SELECT GET_TAB_COL_LIST('{tab_name}','{tab_schema}') FROM DUAL"

sql_tab_record_count = "SELECT COUNT(*) FROM {db_name}.{table_name}"

sql_tab_record_count_partitioned = "SELECT COUNT(*) FROM {db_name}.{table_name} where {partition_col}='{datadate}'"

sql_drop_table_query = "begin \n DROP_TABLE_IFEXIST('{drop_temp_tab}'); end;"

sql_truncate_table = "truncate table {db_name}.{table_name}"

sql_cdp_column_list = "SELECT GET_CDP_TAB_COL_LIST('{table_name}','{schema_name}') FROM DUAL"

sql_cdp_drop_table_partition = "ALTER TABLE {db_name}.{table_name} DROP IF EXISTS PARTITION ({partition_column} = '{partition_name}')"

sql_import_log_insert = "INSERT INTO DWADM.TBL_CDP_INC_DATA_MIG_LOG "\
                                        + "SELECT (SELECT MAX(SL_NO) FROM DWADM.TBL_CDP_INC_DATA_MIG_LOG)+1, '{source_table}','{source_schema}', to_date({data_date},'YYYYMMDD') ," \
                                        + " 1, {staging_rec},{landing_rec},{final_rec}, SYSDATE FROM DUAL"


## Other Variables
var_tab_create_header="CREATE TABLE {temp_tab_schema}.{temp_tab} TABLESPACE FACT_TBS_DW NOLOGGING NOCOMPRESS PARALLEL 32 AS \n"

var_tab_create_select_query="SELECT /*+PARALLEL(32)*/ \n"\
                                                        +" {tab_column_list} "\
                                                        + " from {source_schema}.{tab_name} "

var_tab_create_footer=" {tab_part_type} ({tab_part_prefix}{data_date})"

var_tab_insert_header="insert into {db_name}.{table_name} \n"

var_tab_insert_partitioned_select = "SELECT \n"\
                                                        +" {tab_column_list}, "\
                                                        +" cast(from_unixtime(unix_timestamp(cast(current_timestamp() as string), 'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as string) insert_datetime,"\
                                                        +" {partition_column} "\
                                                        +" from {landing_db}.{landing_table}"

var_tab_insert_nonpartitioned_select = "SELECT \n"\
                                                        +" {tab_column_list}, "\
                                                        +" cast(from_unixtime(unix_timestamp(cast(current_timestamp() as string), 'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as string) insert_datetime"\
                                                        +" from {landing_db}.{landing_table} "

var_sqoop_command ="sqoop import \
-Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--connect \'"+sqoop_connection_string+ "\' \
--username "+ db_user +" \
--password \'" + db_pass + "\' \
--table {oracle_staging_db}.{oracle_staging_table} \
--hcatalog-database {cdp_landing_db} \
--hcatalog-table {cdp_landing_table} \
--hcatalog-storage-stanza \""+ sqoop_data_format +"\" \
--direct \
-m "+ sqoop_mappers;[