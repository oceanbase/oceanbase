/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
%define api.pure
%parse-param {ParseResult *result}
%name-prefix "obsql_mysql_yy"
%locations
%verbose
%error-verbose
%{
#include <stdint.h>
#define YYDEBUG 1
%}

%union {
  struct _ParseNode *node;
  const struct _NonReservedKeyword *non_reserved_keyword;
  const struct _NonReservedKeyword *reserved_keyword;
  int32_t ival[2]; //ival[0] means value,
                   //ival[1] means the number of constants that fast parse can recognize in the corresponding node and its child nodes,
                   //ival[2] for length_semantics
 }

%{
#include "../../../src/sql/parser/sql_parser_mysql_mode_lex.h"
#include "../../../src/sql/parser/sql_parser_base.h"
%}

%destructor {destroy_tree($$);}<node>
%destructor {oceanbase::common::ob_free($$);}<str_value_>

%token <node> NAME_OB
%token <node> STRING_VALUE
%token <node> INTNUM
%token <node> DATE_VALUE
%token <node> TIMESTAMP_VALUE
%token <node> HINT_VALUE
%token <node> BOOL_VALUE
%token <node> APPROXNUM
%token <node> DECIMAL_VAL
%token <node> NULLX
%token <node> QUESTIONMARK
%token <node> SYSTEM_VARIABLE
%token <node> USER_VARIABLE
%token <node> CLIENT_VERSION
%token <node> MYSQL_DRIVER
%token <node> HEX_STRING_VALUE
%token <node> OUTLINE_DEFAULT_TOKEN/*use for outline parser to just filter hint of query_sql*/

/*empty_query::
// (1) For query statements with only spaces or ";", will report an error: such as: " " or "  " or 
//     ";" or " ;" . both need to report an error: err_msg:Query was empty errno:1065
// (2) For query statements that only contain comments or spaces or ";", it needs to return success,
//     eg. "#fadfadf" or "/**\/" or "/**\/  ;" ==> return success
*/

/*
 * Lines beginning with %left define the associativity of operators
 * %Left indicates that the following operators follow the left associative;
 * %Right means right associativity,
 * %Nonassoc means that the following operators have no associativity.
 * The priority is implicit, the operator in the front row has a lower priority than the operator
 * in the latter row;
 * Operators arranged in the same line have the same priority, eg: '+' and'-' have the same priority
 * And their precedence is less than '*', all three operators are left associative
 */

/*
 * Unary operators are sometimes used in expressions, It may be the same symbol as a binary operator
 * For example, the unary operator minus sign "-" is the same as the binary operator minus sign '-'
 * The precedence of unary operators should be higher than the precedence of corresponding binary
 * operators, It should be at least the same as the priority of '*', which can be defined by the
 * %prec clause of yacc: '-' expr %prec '*'
 *  It indicates that the precedence of the rightmost operator or terminal in the grammar rule where
 * it is located is the same as the precedence of the symbol after %prec
 */

%nonassoc   KILL_EXPR
%nonassoc   CONNECTION QUERY
%nonassoc   LOWER_COMMA
%nonassoc   ','
%left	UNION EXCEPT MINUS
%left	INTERSECT
%left   JOIN CROSS LEFT FULL RIGHT INNER WINDOW
%left   SET_VAR
%left	OR OR_OP
%left	XOR
%left	AND AND_OP
%left   BETWEEN CASE WHEN THEN ELSE
%nonassoc LOWER_THAN_COMP
%left   COMP_EQ COMP_NSEQ COMP_GE COMP_GT COMP_LE COMP_LT COMP_NE IS LIKE IN REGEXP
%right  ESCAPE /*for conflict for escape*/
%left   '|'
%left   '&'
%left   SHIFT_LEFT SHIFT_RIGHT
%left   '+' '-'
%left   '*' '/' '%' MOD DIV POW
%left   '^'
%nonassoc LOWER_THAN_NEG /* for simple_expr conflict*/
%left CNNOP
%left   NEG '~'
%nonassoc LOWER_PARENS
%left   '(' ')'
%nonassoc HIGHER_PARENS TRANSACTION/*for simple_expr conflict*/
%left   '.'
%right  NOT NOT2
%right BINARY COLLATE
%left INTERVAL
%nonassoc LOWER_KEY /* for unique key and unique and key*/
%nonassoc KEY /* for unique key and unique and key*/
%nonassoc LOWER_ON /*on expr*/
%nonassoc ON /*on expr*/
%nonassoc LOWER_OVER
%nonassoc OVER
%nonassoc LOWER_INTO
%nonassoc INTO
%nonassoc LOWER_THAN_BY_ACCESS_SESSION

%token ERROR /*used internal*/
%token PARSER_SYNTAX_ERROR /*used internal*/

%token/*for hint*/

READ_STATIC INDEX_HINT USE_NL FROZEN_VERSION  TOPK QUERY_TIMEOUT READ_CONSISTENCY HOTSPOT LOG_LEVEL
LEADING_HINT ORDERED FULL_HINT USE_MERGE USE_HASH NO_USE_HASH USE_PLAN_CACHE USE_JIT NO_USE_JIT NO_USE_NL
NO_USE_MERGE NO_USE_BNL USE_NL_MATERIALIZATION NO_USE_NL_MATERIALIZATION NO_REWRITE TRACE_LOG USE_PX
QB_NAME USE_HASH_AGGREGATION NO_USE_HASH_AGGREGATION NEG_SIGN USE_LATE_MATERIALIZATION NO_USE_LATE_MATERIALIZATION
USE_BNL MAX_CONCURRENT PX_JOIN_FILTER NO_USE_PX PQ_DISTRIBUTE RANDOM_LOCAL BROADCAST TRACING
MERGE_HINT NO_MERGE_HINT NO_EXPAND USE_CONCAT UNNEST NO_UNNEST PLACE_GROUP_BY NO_PLACE_GROUP_BY NO_PRED_DEDUCE
TRANS_PARAM FORCE_REFRESH_LOCATION_CACHE LOAD_BATCH_SIZE NO_PX_JOIN_FILTER DISABLE_PARALLEL_DML PQ_MAP
ENABLE_PARALLEL_DML NO_PARALLEL

%token /*can not be relation name*/
_BINARY _UTF8 _UTF8MB4 _GBK _UTF16 _GB18030 CNNOP
SELECT_HINT_BEGIN UPDATE_HINT_BEGIN DELETE_HINT_BEGIN INSERT_HINT_BEGIN REPLACE_HINT_BEGIN HINT_HINT_BEGIN HINT_END
LOAD_DATA_HINT_BEGIN
END_P SET_VAR DELIMITER

/*reserved keyword*/
%token <reserved_keyword>
/*
 * MySQL 5.7 Reserved Keywords
 * https://dev.mysql.com/doc/refman/5.7/en/keywords.html
 * note! ! ! Non-special circumstances, it is forbidden to put keywords in this area
 * */
        ACCESSIBLE ADD ALL ALTER ANALYZE AND AS ASC ASENSITIVE
        BEFORE BETWEEN BIGINT BINARY BLOB BOTH BY
        CALL CASCADE CASE CHANGE CHAR CHARACTER CHECK COLLATE COLUMN CONDITION CONSTRAINT CONTINUE
        CONVERT CREATE CROSS CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER CURSOR
        DATABASE DATABASES DAY_HOUR DAY_MICROSECOND DAY_MINUTE DAY_SECOND /*DEC*/ DECLARE DECIMAL DEFAULT
        DELAYED DELETE DESC DESCRIBE DETERMINISTIC DISTINCT DISTINCTROW DIV DOUBLE DROP DUAL
        EACH ELSE ELSEIF ENCLOSED ESCAPED EXISTS EXIT EXPLAIN
        /*FALSE*/ FETCH FLOAT FLOAT4 FLOAT8 FOR FORCE FOREIGN FROM
        GENERATED GET GRANT GROUP
        HAVING HIGH_PRIORITY HOUR_MICROSECOND HOUR_MINUTE HOUR_SECOND
        IF IGNORE IN INDEX INFILE INNER INOUT INSENSITIVE INSERT INT INT1 INT2 INT3 INT4 INT8 INTEGER
        INTERVAL INTO IO_AFTER_GTIDS IO_BEFORE_GTIDS IS ITERATE
        JOIN
        KEY KEYS KILL
        LEADING LEAVE LEFT LIKE LIMIT LINEAR LINES LOAD LOCALTIME LOCALTIMESTAMP /*LOCK*/ LONG
        LONGBLOB LONGTEXT LOOP LOW_PRIORITY
        MASTER_BIND MASTER_SSL_VERIFY_SERVER_CERT MATCH MAXVALUE MEDIUMBLOB MEDIUMINT MEDIUMTEXT
        MIDDLEINT MINUTE_MICROSECOND MINUTE_SECOND MOD MODIFIES
        NATURAL NOT NO_WRITE_TO_BINLOG /*NULL*/ /*NUMERIC*/
        ON OPTIMIZE /*OPTIMIZER_COSTS*/ OPTION OPTIONALLY OR ORDER OUT OUTER OUTFILE
        PARTITION PRECISION PRIMARY PROCEDURE PURGE
        RANGE READ READS READ_WRITE REAL REFERENCES REGEXP RELEASE RENAME REPEAT REPLACE REQUIRE
        RESIGNAL RESTRICT RETURN REVOKE RIGHT RLIKE
        SCHEMA SCHEMAS SECOND_MICROSECOND SELECT SENSITIVE SEPARATOR SET SHOW SIGNAL SMALLINT SPATIAL
        SPECIFIC SQL SQLEXCEPTION SQLSTATE SQLWARNING SQL_BIG_RESULT SQL_CALC_FOUND_ROWS
        SQL_SMALL_RESULT SSL STARTING STORED STRAIGHT_JOIN
        TABLE TERMINATED THEN TINYBLOB TINYINT TINYTEXT TO TRAILING TRIGGER /*TRUE*/
        UNDO UNION UNIQUE UNLOCK UNSIGNED UPDATE USAGE USE USING UTC_DATE UTC_TIME UTC_TIMESTAMP
        VALUES VARBINARY VARCHAR VARCHARACTER VARYING VIRTUAL
        WHEN WHERE WHILE WITH WRITE
        XOR
        YEAR_MONTH
        ZEROFILL

/*OB-specific reserved keywords*/
        TABLEGROUP

%token <non_reserved_keyword>
/*Please add the newly added non-reserved keywords to the corresponding rows in alphabetical order, thanks! ! !*/
        ACCESS ACCOUNT ACTION ACTIVE ADDDATE AFTER AGAINST AGGREGATE ALGORITHM ALWAYS ANALYSE ANY
        APPROX_COUNT_DISTINCT APPROX_COUNT_DISTINCT_SYNOPSIS APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE
        ASCII AT AUTHORS AUTO AUTOEXTEND_SIZE AUTO_INCREMENT AVG AVG_ROW_LENGTH ACTIVATE AVAILABILITY
        ARCHIVELOG AUDIT

        BACKUP BALANCE BASE BASELINE BASELINE_ID BASIC BEGI BINDING BINLOG BIT BLOCK BLOCK_INDEX
        BLOCK_SIZE BLOOM_FILTER BOOL BOOLEAN BOOTSTRAP BTREE BYTE BREADTH BUCKETS BISON_LIST BACKUPSET

        CACHE CANCEL CASCADED CAST CATALOG_NAME CHAIN CHANGED CHARSET CHECKSUM CHECKPOINT CHUNK CIPHER
        CLASS_ORIGIN CLEAN CLEAR CLIENT CLOG CLOSE CLUSTER CLUSTER_ID CLUSTER_NAME COALESCE COLUMN_STAT
        CODE COLLATION COLUMN_FORMAT COLUMN_NAME COLUMNS COMMENT COMMIT COMMITTED COMPACT COMPLETION
        COMPRESSED COMPRESSION CONCURRENT CONNECTION CONSISTENT CONSISTENT_MODE CONSTRAINT_CATALOG
        CONSTRAINT_NAME CONSTRAINT_SCHEMA CONTAINS CONTEXT CONTRIBUTORS COPY COUNT CPU CREATE_TIMESTAMP
        CTX_ID CUBE CURDATE CURRENT CURTIME CURSOR_NAME CUME_DIST CYCLE

        DAG DATA DATAFILE DATA_TABLE_ID DATE DATE_ADD DATE_SUB DATETIME DAY DEALLOCATE DECRYPTION
        DEFAULT_AUTH DEFINER DELAY DELAY_KEY_WRITE DEPTH DES_KEY_FILE DENSE_RANK DESTINATION DIAGNOSTICS
        DIRECTORY DISABLE DISCARD DISK DISKGROUP DO DUMP DUMPFILE DUPLICATE DUPLICATE_SCOPE DYNAMIC
        DATABASE_ID DEFAULT_TABLEGROUP

        EFFECTIVE ENABLE ENCRYPTION END ENDS ENGINE_ ENGINES ENUM ENTITY ERROR_CODE ERROR_P ERRORS
        ESCAPE EVENT EVENTS EVERY EXCHANGE EXECUTE EXPANSION EXPIRE EXPIRE_INFO EXPORT OUTLINE EXTENDED
        EXTENDED_NOADDR EXTENT_SIZE EXTRACT EXCEPT EXPIRED

        FAILOVER FAST FAULTS FIELDS FILEX FINAL_COUNT FIRST FIRST_VALUE FIXED FLUSH FOLLOWER FORMAT
        FOUND FREEZE FREQUENCY FUNCTION FOLLOWING FLASHBACK FULL FROZEN FILE_ID

        GENERAL GEOMETRY GEOMETRYCOLLECTION GET_FORMAT GLOBAL GRANTS GROUP_CONCAT GROUPING GTS
        GLOBAL_NAME GLOBAL_ALIAS

        HANDLER HASH HELP HISTOGRAM HOST HOSTS HOUR

        ID IDC IDENTIFIED IGNORE_SERVER_IDS ILOG IMPORT INCR INDEXES INDEX_TABLE_ID INFO INITIAL_SIZE
        INNODB INSERT_METHOD INSTALL INSTANCE INVOKER IO IO_THREAD IPC ISOLATE ISOLATION ISSUER
        IS_TENANT_SYS_POOL INVISIBLE MERGE ISNULL INTERSECT INCREMENTAL INNER_PARSE ILOGCACHE INPUT

        JOB JSON

        KEY_BLOCK_SIZE KEY_VERSION KVCACHE

        LAG LANGUAGE LAST LAST_VALUE LEAD LEADER LEAVES LESS LEAK LEAK_MOD LEAK_RATE LINESTRING LIST_
        LISTAGG LOCAL LOCALITY LOCATION LOCKED LOCKS LOGFILE LOGONLY_REPLICA_NUM LOGS LOCK_ LOGICAL_READS
        LEVEL

        MAJOR MANUAL MASTER MASTER_AUTO_POSITION MASTER_CONNECT_RETRY MASTER_DELAY MASTER_HEARTBEAT_PERIOD
        MASTER_HOST MASTER_LOG_FILE MASTER_LOG_POS MASTER_PASSWORD MASTER_PORT MASTER_RETRY_COUNT
        MASTER_SERVER_ID MASTER_SSL MASTER_SSL_CA MASTER_SSL_CAPATH MASTER_SSL_CERT MASTER_SSL_CIPHER
        MASTER_SSL_CRL MASTER_SSL_CRLPATH MASTER_SSL_KEY MASTER_USER MAX MAX_CONNECTIONS_PER_HOUR MAX_CPU
        MAX_DISK_SIZE MAX_IOPS MAX_MEMORY MAX_QUERIES_PER_HOUR MAX_ROWS MAX_SESSION_NUM MAX_SIZE
        MAX_UPDATES_PER_HOUR MAX_USER_CONNECTIONS MEDIUM MEMORY MEMTABLE MESSAGE_TEXT META MICROSECOND
        MIGRATE MIN MIN_CPU MIN_IOPS MIN_MEMORY MINOR MIN_ROWS MINUS MINUTE MODE MODIFY MONTH MOVE
        MULTILINESTRING MULTIPOINT MULTIPOLYGON MUTEX MYSQL_ERRNO MIGRATION MAX_USED_PART_ID MAXIMIZE
        MATERIALIZED MEMSTORE_PERCENT

        NAME NAMES NATIONAL NCHAR NDB NDBCLUSTER NEW NEXT NO NOAUDIT NODEGROUP NONE NORMAL NOW NOWAIT
        NO_WAIT NULLS NUMBER NVARCHAR NTILE NTH_VALUE NOARCHIVELOG NETWORK NOPARALLEL

        OBSOLETE OCCUR OF OFF OFFSET OLD_PASSWORD ONE ONE_SHOT ONLY OPEN OPTIONS ORIG_DEFAULT OWNER OLD_KEY OVER

        PACK_KEYS PAGE PARALLEL PARAMETERS PARSER PARTIAL PARTITION_ID PARTITIONING PARTITIONS PASSWORD PAUSE
        PERCENT_RANK PHASE PLAN PHYSICAL PLANREGRESS PLUGIN PLUGIN_DIR PLUGINS POINT POLYGON PERFORMANCE
        PROTECTION PRIORITY PL POOL PORT POSITION PREPARE PRESERVE PREV PRIMARY_ZONE PRIVILEGES PROCESS
        PROCESSLIST PROFILE PROFILES PROXY PRECEDING PCTFREE P_ENTITY P_CHUNK PRIMARY_ROOTSERVICE_LIST
        PRIMARY_CLUSTER_ID PUBLIC PROGRESSIVE_MERGE_NUM PS

        QUARTER QUERY QUEUE_TIME QUICK

        REBUILD RECOVER RECYCLE REDO_BUFFER_SIZE REDOFILE REDUNDANT REFRESH REGION RELAY RELAYLOG
        RELAY_LOG_FILE RELAY_LOG_POS RELAY_THREAD RELOAD REMOVE REORGANIZE REPAIR REPEATABLE REPLICA
        REPLICA_NUM REPLICA_TYPE REPLICATION REPORT RESET RESOURCE RESOURCE_POOL_LIST RESPECT RESTART
        RESTORE RESUME RETURNED_SQLSTATE RETURNS REVERSE REWRITE_MERGE_VERSION ROLLBACK ROLLUP ROOT
        ROOTTABLE ROOTSERVICE ROOTSERVICE_LIST ROUTINE ROW ROLLING ROW_COUNT ROW_FORMAT ROWS RTREE RUN
        RECYCLEBIN ROTATE ROW_NUMBER RUDUNDANT RECURSIVE RANDOM REDO_TRANSPORT_OPTIONS REMOTE_OSS RT
        RANK READ_ONLY RECOVERY

        SAMPLE SAVEPOINT SCHEDULE SCHEMA_NAME SCOPE SECOND SECURITY SEED SERIAL SERIALIZABLE SERVER
        SERVER_IP SERVER_PORT SERVER_TYPE SESSION SESSION_USER SET_MASTER_CLUSTER SET_SLAVE_CLUSTER
        SET_TP SHARE SHUTDOWN SIGNED SIMPLE SLAVE SLOW SLOT_IDX SNAPSHOT SOCKET SOME SONAME SOUNDS
        SOURCE SPFILE SPLIT SQL_AFTER_GTIDS SQL_AFTER_MTS_GAPS SQL_BEFORE_GTIDS SQL_BUFFER_RESULT
        SQL_CACHE SQL_NO_CACHE SQL_ID SQL_THREAD SQL_TSI_DAY SQL_TSI_HOUR SQL_TSI_MINUTE SQL_TSI_MONTH
        SQL_TSI_QUARTER SQL_TSI_SECOND SQL_TSI_WEEK SQL_TSI_YEAR STANDBY STAT START STARTS STATS_AUTO_RECALC
        STATS_PERSISTENT STATS_SAMPLE_PAGES STATUS STATEMENTS STD STDDEV STDDEV_POP STDDEV_SAMP STRONG
        SYNCHRONIZATION STOP STORAGE STORAGE_FORMAT_VERSION STORAGE_FORMAT_WORK_VERSION STORING STRING
        SUBCLASS_ORIGIN SUBDATE SUBJECT SUBPARTITION SUBPARTITIONS SUBSTR SUBSTRING SUCCESSFUL SUM
        SUPER SUSPEND SWAPS SWITCH SWITCHES SWITCHOVER SYSTEM SYSTEM_USER SYSDATE SESSION_ALIAS SYNONYM
        SIZE

        TABLE_CHECKSUM TABLE_MODE TABLE_ID TABLE_NAME TABLEGROUPS TABLES TABLESPACE TABLET TABLET_MAX_SIZE
        TEMPLATE TEMPORARY TEMPTABLE TENANT TEXT THAN TIME TIMESTAMP TIMESTAMPADD TIMESTAMPDIFF TP_NO
        TP_NAME TRACE TRADITIONAL TRANSACTION TRIGGERS TRIM TRUNCATE TYPE TYPES TASK TABLET_SIZE
        TABLEGROUP_ID TENANT_ID THROTTLE TIME_ZONE_INFO

        UNCOMMITTED UNDEFINED UNDO_BUFFER_SIZE UNDOFILE UNICODE UNINSTALL UNIT UNIT_NUM UNLOCKED UNTIL
        UNUSUAL UPGRADE USE_BLOOM_FILTER UNKNOWN USE_FRM USER USER_RESOURCES UNBOUNDED

        VALID VALUE VARIANCE VARIABLES VERBOSE VERIFY VIEW VISIBLE VIRTUAL_COLUMN_ID VALIDATE VAR_POP
        VAR_SAMP

        WAIT WARNINGS WEEK WEIGHT_STRING WHENEVER WITH_ROWID WORK WRAPPER WINDOW WEAK

        X509 XA XML

        YEAR

        ZONE ZONE_LIST ZONE_TYPE

%type <node> sql_stmt stmt_list stmt opt_end_p
%type <node> select_stmt update_stmt delete_stmt
%type <node> insert_stmt single_table_insert values_clause dml_table_name
%type <node> create_table_stmt create_table_like_stmt opt_table_option_list table_option_list table_option table_option_list_space_seperated create_function_stmt drop_function_stmt parallel_option
%type <node> create_synonym_stmt drop_synonym_stmt opt_public opt_force synonym_name synonym_object opt_dlink
%type <node> create_database_stmt drop_database_stmt alter_database_stmt use_database_stmt
%type <node> opt_database_name database_option database_option_list opt_database_option_list database_factor
%type <node> create_tenant_stmt opt_tenant_option_list alter_tenant_stmt drop_tenant_stmt
%type <node> create_restore_point_stmt drop_restore_point_stmt
%type <node> create_resource_stmt drop_resource_stmt alter_resource_stmt
%type <node> cur_timestamp_func cur_time_func cur_date_func now_synonyms_func utc_timestamp_func sys_interval_func sysdate_func
%type <node> opt_create_resource_pool_option_list create_resource_pool_option alter_resource_pool_option_list alter_resource_pool_option
%type <node> opt_shrink_unit_option unit_id_list
%type <node> opt_resource_unit_option_list resource_unit_option
%type <node> tenant_option zone_list resource_pool_list
%type <node> opt_partition_option partition_option hash_partition_option key_partition_option opt_use_partition use_partition range_partition_option subpartition_option opt_range_partition_list opt_range_subpartition_list range_partition_list range_subpartition_list range_partition_element range_subpartition_element range_partition_expr range_expr_list range_expr opt_part_id sample_clause opt_block seed sample_percent opt_sample_scope modify_partition_info modify_tg_partition_info opt_partition_range_or_list column_partition_option opt_column_partition_option auto_partition_option auto_range_type partition_size auto_partition_type
%type <node> subpartition_template_option subpartition_individual_option opt_hash_partition_list hash_partition_list hash_partition_element opt_hash_subpartition_list hash_subpartition_list hash_subpartition_element opt_subpartition_list
%type <node> date_unit date_params timestamp_params
%type <node> drop_table_stmt table_list drop_view_stmt table_or_tables
%type <node> explain_stmt explainable_stmt format_name kill_stmt create_outline_stmt alter_outline_stmt drop_outline_stmt opt_outline_target
%type <node> expr_list expr expr_const conf_const simple_expr expr_or_default bit_expr bool_pri predicate explain_or_desc
%type <node> column_ref multi_delete_table
%type <node> case_expr func_expr in_expr sub_query_flag
%type <node> case_arg when_clause_list when_clause case_default
%type <node> window_function opt_partition_by generalized_window_clause win_rows_or_range win_preceding_or_following win_interval win_bounding win_window opt_win_window win_fun_lead_lag_params respect_or_ignore opt_respect_or_ignore_nulls win_fun_first_last_params first_or_last opt_from_first_or_last new_generalized_window_clause new_generalized_window_clause_with_blanket opt_named_windows named_windows named_window
%type <node> update_asgn_list update_asgn_factor
%type <node> table_element_list table_element column_definition column_definition_ref column_definition_list column_name_list aux_column_list vertical_column_name
%type <node> opt_generated_keyname opt_generated_column_attribute_list generated_column_attribute opt_storage_type
%type <node> data_type temporary_option opt_if_not_exists opt_if_exists opt_charset collation opt_collation cast_data_type
%type <node> replace_with_opt_hint insert_with_opt_hint column_list opt_on_duplicate_key_clause opt_into  opt_replace opt_materialized opt_materialized_or_temporary
%type <node> insert_vals_list insert_vals value_or_values
%type <node> select_with_parens select_no_parens select_clause select_into no_table_select_with_order_and_limit simple_select_with_order_and_limit select_with_parens_with_order_and_limit select_clause_set select_clause_set_left select_clause_set_right  select_clause_set_with_order_and_limit
%type <node> simple_select no_table_select limit_clause select_expr_list
%type <node> opt_where opt_hint_value opt_groupby opt_rollup opt_order_by order_by opt_having groupby_clause
%type <node> opt_limit_clause limit_expr opt_for_update opt_for_update_wait
%type <node> sort_list sort_key opt_asc_desc sort_list_for_group_by sort_key_for_group_by opt_asc_desc_for_group_by opt_column_id
%type <node> opt_query_expression_option_list query_expression_option_list query_expression_option opt_distinct opt_distinct_or_all opt_separator projection
%type <node> from_list table_references table_reference table_factor normal_relation_factor dot_relation_factor relation_factor
%type <node> relation_factor_in_hint relation_factor_in_hint_list relation_factor_in_leading_hint relation_factor_in_pq_hint relation_factor_in_leading_hint_list_entry relation_factor_in_use_join_hint_list
%type <node> relation_factor_in_leading_hint_list joined_table tbl_name table_subquery
%type <node> relation_factor_with_star relation_with_star_list opt_with_star
%type <node> index_hint_type key_or_index index_hint_scope index_element index_list opt_index_list
%type <node> index_hint_definition index_hint_list
%type <node> tracing_num_list
%type <node> qb_name_option
%type <node> join_condition inner_join_type opt_inner outer_join_type opt_outer natural_join_type
%type <ival> string_length_i opt_string_length_i opt_string_length_i_v2 opt_int_length_i opt_bit_length_i opt_datetime_fsp_i opt_unsigned_i opt_zerofill_i opt_year_i opt_time_func_fsp_i
%type <node> opt_float_precision opt_number_precision
%type <node> opt_equal_mark opt_default_mark read_only_or_write not not2 opt_disk_alias
%type <node> int_or_decimal
%type <node> opt_column_attribute_list column_attribute
%type <node> show_stmt from_or_in columns_or_fields database_or_schema index_or_indexes_or_keys opt_from_or_in_database_clause opt_show_condition opt_desc_column_option opt_status opt_storage
%type <node> prepare_stmt stmt_name preparable_stmt
%type <node> variable_set_stmt var_and_val_list var_and_val to_or_eq set_expr_or_default sys_var_and_val_list sys_var_and_val opt_set_sys_var opt_global_sys_vars_set
%type <node> execute_stmt argument_list argument opt_using_args
%type <node> deallocate_prepare_stmt deallocate_or_drop
%type <ival> opt_scope opt_drop_behavior opt_integer scope_or_scope_alias global_or_session_alias
%type <ival> int_type_i float_type_i datetime_type_i date_year_type_i cast_datetime_type_i text_type_i blob_type_i
%type <node> create_user_stmt user_specification user_specification_list user password opt_host_name user_with_host_name
%type <node> drop_user_stmt user_list
%type <node> set_password_stmt opt_for_user
%type <node> rename_user_stmt rename_info rename_list
%type <node> rename_table_stmt rename_table_actions rename_table_action
%type <node> truncate_table_stmt
%type <node> lock_user_stmt lock_spec_mysql57
%type <node> grant_stmt grant_privileges priv_type_list priv_type priv_level opt_privilege grant_options
%type <node> revoke_stmt
%type <node> opt_limit opt_for_grant_user
%type <node> parameterized_trim
%type <ival> opt_with_consistent_snapshot opt_config_scope opt_index_keyname opt_full opt_mode_flag
%type <node> opt_work begin_stmt commit_stmt rollback_stmt opt_ignore xa_begin_stmt xa_end_stmt xa_prepare_stmt xa_commit_stmt xa_rollback_stmt
%type <node> alter_table_stmt alter_table_actions alter_table_action alter_column_option alter_index_option alter_constraint_option alter_partition_option opt_to alter_tablegroup_option opt_table opt_tablegroup_option_list alter_tg_partition_option
%type <node> tablegroup_option_list tablegroup_option alter_tablegroup_actions alter_tablegroup_action tablegroup_option_list_space_seperated
%type <node> opt_tg_partition_option tg_hash_partition_option tg_key_partition_option tg_range_partition_option tg_subpartition_option tg_list_partition_option
%type <node> opt_column alter_column_behavior opt_set opt_position_column
%type <node> alter_system_stmt alter_system_set_parameter_actions alter_system_settp_actions settp_option alter_system_set_parameter_action server_info_list server_info
%type <node> opt_comment opt_as
%type <node> column_name relation_name function_name column_label var_name relation_name_or_string row_format_option
%type <node> opt_hint_list hint_option select_with_opt_hint update_with_opt_hint delete_with_opt_hint hint_list_with_end
%type <node> create_index_stmt index_name sort_column_list sort_column_key opt_index_option_list index_option opt_sort_column_key_length opt_index_using_algorithm index_using_algorithm visibility_option opt_constraint opt_constraint_name constraint_name
%type <node> opt_when
%type <non_reserved_keyword> unreserved_keyword unreserved_keyword_normal unreserved_keyword_special unreserved_keyword_extra
%type <reserved_keyword> mysql_reserved_keyword
%type <ival> set_type_other set_type_union
%type <ival> consistency_level use_plan_cache_type use_jit_type
%type <node> set_type set_expression_option
%type <node> drop_index_stmt hint_options opt_expr_as_list expr_as_list expr_with_opt_alias substr_params opt_comma substr_or_substring
%type <node> /*frozen_type*/ opt_binary
%type <node> ip_port
%type <node> create_view_stmt view_name opt_column_list opt_table_id view_select_stmt
%type <node> name_list
%type <node> partition_role zone_desc opt_zone_desc server_or_zone opt_server_or_zone opt_partitions opt_subpartitions add_or_alter_zone_options alter_or_change_or_modify
%type <node> partition_id_desc opt_tenant_list_or_partition_id_desc partition_id_or_server_or_zone opt_create_timestamp change_actions change_action add_or_alter_zone_option
%type <node> memstore_percent
%type <node> migrate_action replica_type suspend_or_resume tenant_name opt_tenant_name cache_name opt_cache_name file_id opt_file_id cancel_task_type
%type <node> sql_id_expr opt_sql_id baseline_id_expr opt_baseline_id baseline_asgn_factor
%type <node> server_action server_list opt_ignore_server_list opt_server_list
%type <node> zone_action upgrade_action
%type <node> opt_index_name opt_key_or_index opt_index_options opt_primary  opt_all
%type <node> charset_key database_key charset_name charset_name_or_default collation_name databases_or_schemas trans_param_name trans_param_value
%type <node> set_names_stmt set_charset_stmt
%type <node> charset_introducer opt_charset_introducer complex_string_literal literal number_literal now_or_signed_literal signed_literal
%type <node> create_tablegroup_stmt drop_tablegroup_stmt alter_tablegroup_stmt default_tablegroup
%type <node> set_transaction_stmt transaction_characteristics transaction_access_mode isolation_level
%type <node> lock_tables_stmt unlock_tables_stmt lock_type lock_table_list lock_table opt_local
%type <node> purge_stmt
%type <node> tenant_name_list opt_tenant_list tenant_list_tuple cache_type flush_scope opt_zone_list
%type <node> into_opt into_clause field_opt field_term field_term_list line_opt line_term line_term_list into_var_list into_var
%type <node> string_list text_string
%type <node> balance_task_type opt_balance_task_type
%type <node> list_expr list_partition_element list_partition_expr list_partition_list list_partition_option opt_list_partition_list opt_list_subpartition_list list_subpartition_list list_subpartition_element drop_partition_name_list
%type <node> primary_zone_name locality_name change_tenant_name_or_tenant_id distribute_method opt_distribute_method
%type <node> load_data_stmt opt_load_local opt_duplicate opt_load_charset opt_load_ignore_rows
%type <node> lines_or_rows opt_field_or_var_spec field_or_vars_list field_or_vars opt_load_set_spec
%type <node> load_set_list load_set_element load_data_with_opt_hint
%type <node> ret_type opt_agg
%type <node> opt_match_option
%type <ival> match_action
%type <node> opt_reference_option_list reference_option require_specification tls_option_list tls_option
%type <ival> reference_action
%type <node> alter_foreign_key_action
%type <node> optimize_stmt
%type <node> dump_memory_stmt
%type <node> create_savepoint_stmt rollback_savepoint_stmt release_savepoint_stmt
%type <node> opt_qb_name
%type <node> opt_force_purge
%type <node> opt_sql_throttle_for_priority opt_sql_throttle_using_cond sql_throttle_one_or_more_metrics sql_throttle_metric
%start sql_stmt
%%
////////////////////////////////////////////////////////////////
sql_stmt:
stmt_list
{
  merge_nodes($$, result, T_STMT_LIST, $1);
  result->result_tree_ = $$;
  YYACCEPT;
}
;

stmt_list:
END_P
{
  malloc_terminal_node($$, result->malloc_pool_, T_EMPTY_QUERY);
  $$->value_ = result->has_encount_comment_;
}
| DELIMITER
{
  malloc_terminal_node($$, result->malloc_pool_, T_EMPTY_QUERY);
  $$->value_ = result->has_encount_comment_;
}
| stmt END_P
{
  $$ = (NULL != $1) ? $1 : NULL;
}
| stmt DELIMITER opt_end_p
{
  (void)($3);
  $$ = (NULL != $1) ? $1 : NULL;
}
;

opt_end_p:
/*empty*/ {$$ = NULL;}
| END_P { $$ = NULL;}
;

/* only preparable statement will set question mark size */
stmt:
    select_stmt             { $$ = $1; question_mark_issue($$, result); }
  | insert_stmt             { $$ = $1; question_mark_issue($$, result); }
  | create_table_stmt       { $$ = $1; check_question_mark($$, result); }
  | create_function_stmt    { $$ = $1; check_question_mark($$, result); }
  | drop_function_stmt      { $$ = $1; check_question_mark($$, result); }
  | create_table_like_stmt  { $$ = $1; check_question_mark($$, result); }
  | create_database_stmt    { $$ = $1; check_question_mark($$, result); }
  | drop_database_stmt      { $$ = $1; check_question_mark($$, result); }
  | alter_database_stmt     { $$ = $1; check_question_mark($$, result); }
  | use_database_stmt       { $$ = $1; check_question_mark($$, result); }
  | update_stmt             { $$ = $1; question_mark_issue($$, result); }
  | delete_stmt             { $$ = $1; question_mark_issue($$, result); }
  | drop_table_stmt         { $$ = $1; check_question_mark($$, result); }
  | drop_view_stmt          { $$ = $1; check_question_mark($$, result); }
  | explain_stmt            { $$ = $1; question_mark_issue($$, result); }
  | create_outline_stmt     { $$ = $1; question_mark_issue($$, result); }
  | alter_outline_stmt      { $$ = $1; question_mark_issue($$, result); }
  | drop_outline_stmt       { $$ = $1; check_question_mark($$, result); }
  | show_stmt               { $$ = $1; check_question_mark($$, result); }
  | prepare_stmt            { $$ = $1; question_mark_issue($$, result); }
  | variable_set_stmt       { $$ = $1; question_mark_issue($$, result); }
  | execute_stmt            { $$ = $1; check_question_mark($$, result); }
  | alter_table_stmt        { $$ = $1; check_question_mark($$, result); }
  | alter_system_stmt       { $$ = $1; check_question_mark($$, result); }
  | deallocate_prepare_stmt { $$ = $1; check_question_mark($$, result); }
  | create_user_stmt        { $$ = $1; check_question_mark($$, result); }
  | drop_user_stmt          { $$ = $1; check_question_mark($$, result); }
  | set_password_stmt       { $$ = $1; check_question_mark($$, result); }
  | rename_user_stmt        { $$ = $1; check_question_mark($$, result); }
  | lock_user_stmt          { $$ = $1; check_question_mark($$, result); }
  | grant_stmt              { $$ = $1; check_question_mark($$, result); }
  | revoke_stmt             { $$ = $1; check_question_mark($$, result); }
  | begin_stmt              { $$ = $1; check_question_mark($$, result); }
  | commit_stmt             { $$ = $1; check_question_mark($$, result); }
  | rollback_stmt           { $$ = $1; check_question_mark($$, result); }
  | create_index_stmt       { $$ = $1; check_question_mark($$, result); }
  | drop_index_stmt         { $$ = $1; check_question_mark($$, result); }
  | kill_stmt               { $$ = $1; check_question_mark($$, result); }
  | create_view_stmt        { $$ = $1; check_question_mark($$, result); }
  | create_tenant_stmt      { $$ = $1; check_question_mark($$, result); }
  | alter_tenant_stmt       { $$ = $1; check_question_mark($$, result); }
  | drop_tenant_stmt        { $$ = $1; check_question_mark($$, result); }
  | create_restore_point_stmt { $$ = $1; check_question_mark($$, result); }
  | drop_restore_point_stmt   { $$ = $1; check_question_mark($$, result); }
  | create_resource_stmt    { $$ = $1; check_question_mark($$, result); }
  | alter_resource_stmt     { $$ = $1; check_question_mark($$, result); }
  | drop_resource_stmt      { $$ = $1; check_question_mark($$, result); }
  | set_names_stmt          { $$ = $1; check_question_mark($$, result); }
  | set_charset_stmt        { $$ = $1; check_question_mark($$, result); }
  | create_tablegroup_stmt  { $$ = $1; check_question_mark($$, result); }
  | drop_tablegroup_stmt    { $$ = $1; check_question_mark($$, result); }
  | alter_tablegroup_stmt   { $$ = $1; check_question_mark($$, result); }
  | rename_table_stmt       { $$ = $1; check_question_mark($$, result); }
  | truncate_table_stmt     { $$ = $1; check_question_mark($$, result); }
  | set_transaction_stmt    { $$ = $1; check_question_mark($$, result); }
  | create_synonym_stmt     { $$ = $1; check_question_mark($$, result); }
  | drop_synonym_stmt       { $$ = $1; check_question_mark($$, result); }
  | create_savepoint_stmt   { $$ = $1; check_question_mark($$, result); }
  | rollback_savepoint_stmt { $$ = $1; check_question_mark($$, result); }
  | release_savepoint_stmt  { $$ = $1; check_question_mark($$, result); }
  | lock_tables_stmt
  { $$ = $1; check_question_mark($$, result); $$->value_ = 1; }
  | unlock_tables_stmt
  { $$ = $1; check_question_mark($$, result); $$->value_ = 1; }
  | purge_stmt              { $$ = $1; check_question_mark($$, result); }
  | load_data_stmt          { $$ = $1; check_question_mark($$, result); }
  | xa_begin_stmt           { $$ = $1; check_question_mark($$, result); }
  | xa_end_stmt             { $$ = $1; check_question_mark($$, result); }
  | xa_prepare_stmt         { $$ = $1; check_question_mark($$, result); }
  | xa_commit_stmt          { $$ = $1; check_question_mark($$, result); }
  | xa_rollback_stmt        { $$ = $1; check_question_mark($$, result); }
  | optimize_stmt     { $$ = $1; check_question_mark($$, result); }
  | dump_memory_stmt  { $$ = $1; check_question_mark($$, result); }
  ;

/*****************************************************************************
*
*	expression grammar
*
*****************************************************************************/

expr_list:
expr %prec LOWER_COMMA
{
  $$ = $1;
  /* every mysql's item(same as ob's expr) has its own name */
  if (OB_UNLIKELY((NULL == $$->str_value_)) && $$->type_ != T_VARCHAR) {
    dup_string($$, result, @1.first_column, @1.last_column);
  }
}
| expr_list ',' expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

expr_as_list:
expr_with_opt_alias
{
  $$ = $1;
}
| expr_as_list  ',' expr_with_opt_alias
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}

expr_with_opt_alias:
expr
{
  $$ = $1;
  /* every mysql's item(same as ob's expr) has its own name */
  if (OB_UNLIKELY((NULL == $$->str_value_)) && $$->type_ != T_VARCHAR) {
    dup_string($$, result, @1.first_column, @1.last_column);
  }
}
|
expr opt_as column_label
{
  (void)($2);
  ParseNode *alias_node = NULL;
  if (OB_UNLIKELY((NULL == $1))) {
    yyerror(NULL, result, "alias expr can not be NULL\n");
    YYABORT_UNEXPECTED;
  } else if (OB_UNLIKELY(T_COLUMN_REF == $1->type_ && NULL != $1->children_ && NULL != $1->children_[1]
             && NULL != $1->children_[2] && T_STAR == $1->children_[2]->type_)) {
    yyerror(&@2, result, "table.* as label is invalid\n");
    YYERROR;
  } else {
    malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, $3);
    malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_WITH_ALIAS, 1, alias_node);
    dup_expr_string($$, result, @3.first_column, @3.last_column);
    dup_node_string($3, alias_node, result->malloc_pool_);
    alias_node->param_num_ = 0;
  }
}
| expr opt_as STRING_VALUE
{
  (void)($2);
  if (OB_UNLIKELY(NULL == $1)) {
    yyerror(NULL, result, "alias expr can not be NULL\n");
    YYABORT_UNEXPECTED;
  } else if (OB_UNLIKELY(T_COLUMN_REF == $1->type_ && NULL !=$1->children_ && NULL != $1->children_[1]
             && NULL != $1->children_[2] && T_STAR == $1->children_[2]->type_)) {
    yyerror(&@2, result, "table.* as label is invalid\n");
    YYERROR;
  } else {
    ParseNode *alias_node = NULL;
    ParseNode *alias_name_node = NULL;
    malloc_terminal_node(alias_name_node, result->malloc_pool_, T_IDENT);
    dup_node_string($3, alias_name_node, result->malloc_pool_);

    malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, alias_name_node);
    malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_WITH_ALIAS, 1, alias_node);
    dup_expr_string($$, result, @3.first_column, @3.last_column);
    dup_node_string($3, alias_node, result->malloc_pool_);
    alias_node->param_num_ = 1;
  }
}


column_ref:
column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, NULL, $1);
  dup_node_string($1, $$, result->malloc_pool_);
#ifndef SQL_PARSER_COMPILATION
#endif
}
| relation_name '.' column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, $1, $3);
  dup_node_string($3, $$, result->malloc_pool_);
}
| relation_name '.' mysql_reserved_keyword
{
  ParseNode *col_name = NULL;
  get_non_reserved_node(col_name, result->malloc_pool_, @3.first_column, @3.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, $1, col_name);
  dup_node_string(col_name, $$, result->malloc_pool_);
}
| mysql_reserved_keyword '.' mysql_reserved_keyword
{
  ParseNode *col_name = NULL;
  ParseNode *table_name = NULL;
  get_non_reserved_node(table_name, result->malloc_pool_, @1.first_column, @1.last_column);
  get_non_reserved_node(col_name, result->malloc_pool_, @3.first_column, @3.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, table_name, col_name);
  dup_node_string(col_name, $$, result->malloc_pool_);
}
| relation_name '.' '*'
{
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, $1, node);
  $$->value_ = 0;
}
| relation_name '.' relation_name '.' column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, $1, $3, $5);
  dup_node_string($5, $$, result->malloc_pool_);
}
| relation_name '.' relation_name '.' mysql_reserved_keyword
{
  ParseNode *col_name = NULL;
  get_non_reserved_node(col_name, result->malloc_pool_, @5.first_column, @5.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, $1, $3, col_name);
  dup_node_string(col_name, $$, result->malloc_pool_);
}
| relation_name '.' mysql_reserved_keyword '.' mysql_reserved_keyword
{
  ParseNode *col_name = NULL;
  ParseNode *table_name = NULL;
  get_non_reserved_node(table_name, result->malloc_pool_, @3.first_column, @3.last_column);
  get_non_reserved_node(col_name, result->malloc_pool_, @5.first_column, @5.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, $1, table_name, col_name);
  dup_node_string(col_name, $$, result->malloc_pool_);
}
| relation_name '.' relation_name '.' '*'
{
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, $1, $3, node);
  $$->value_ = 0;
}
| '.' relation_name '.' column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, $2, $4);
  dup_node_string($4, $$, result->malloc_pool_);
}
| '.' relation_name '.' mysql_reserved_keyword
{
  ParseNode *col_name = NULL;
  get_non_reserved_node(col_name, result->malloc_pool_, @4.first_column, @4.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, $2, col_name);
  dup_node_string(col_name, $$, result->malloc_pool_);
}
| '.' mysql_reserved_keyword '.' mysql_reserved_keyword
{
  ParseNode *col_name = NULL;
  ParseNode *table_name = NULL;
  get_non_reserved_node(table_name, result->malloc_pool_, @2.first_column, @2.last_column);
  get_non_reserved_node(col_name, result->malloc_pool_, @4.first_column, @4.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, table_name, col_name);
  dup_node_string(col_name, $$, result->malloc_pool_);
}
;

opt_charset_introducer:
/*empty*/
{
  $$ = NULL;
}
| charset_introducer
{
  $$ = $1;
};

/* literal string with  */
complex_string_literal:
opt_charset_introducer STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 2, $1, $2);
  $$->str_value_ = $2->str_value_;
  $$->str_len_ = $2->str_len_;
  $$->raw_text_ = $2->raw_text_;
  $$->text_len_ = $2->text_len_;
  if (NULL == $1)
  {
    @$.first_column = @2.first_column;
    @$.last_column = @2.last_column;
  }
}
| charset_introducer HEX_STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 1, $1);
  $$->str_value_ = $2->str_value_;
  $$->str_len_ = $2->str_len_;
  $$->raw_text_ = $2->raw_text_;
  $$->text_len_ = $2->text_len_;
}
;

charset_introducer:
_UTF8
{
  malloc_terminal_node($$, result->malloc_pool_, T_CHARSET);
  $$->str_value_ = parse_strdup("utf8", result->malloc_pool_, &($$->str_len_));
  if (OB_UNLIKELY(NULL == $$->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
}
| _UTF8MB4
{
  malloc_terminal_node($$, result->malloc_pool_, T_CHARSET);
  $$->str_value_ = parse_strdup("utf8mb4", result->malloc_pool_, &($$->str_len_));
  if (OB_UNLIKELY(NULL == $$->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
}
| _BINARY
{
  malloc_terminal_node($$, result->malloc_pool_, T_CHARSET);
  $$->str_value_ = parse_strdup("binary", result->malloc_pool_, &($$->str_len_));
  if (OB_UNLIKELY(NULL == $$->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
}
| _GBK
{
  malloc_terminal_node($$, result->malloc_pool_, T_CHARSET);
  $$->str_value_ = parse_strdup("gbk", result->malloc_pool_, &($$->str_len_));
  if (OB_UNLIKELY(NULL == $$->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string");
    YYABORT_NO_MEMORY;
  }
}
| _GB18030
{
  malloc_terminal_node($$, result->malloc_pool_, T_CHARSET);
  $$->str_value_ = parse_strdup("gb18030", result->malloc_pool_, &($$->str_len_));
  if (OB_UNLIKELY(NULL == $$->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string");
    YYABORT_NO_MEMORY;
  }
}
| _UTF16
{
  malloc_terminal_node($$, result->malloc_pool_, T_CHARSET);
  $$->str_value_ = parse_strdup("utf16", result->malloc_pool_, &($$->str_len_));
  if (OB_UNLIKELY(NULL == $$->str_value_)) {
    yyerror(NULL, result, "no more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
}
;

literal:
complex_string_literal { $$ = $1; }
| DATE_VALUE { $$ = $1; }
| TIMESTAMP_VALUE { $$ = $1; }
| INTNUM { $$ = $1; }
| APPROXNUM { $$ = $1; }
| DECIMAL_VAL { $$ = $1; }
| BOOL_VALUE { $$ = $1; }
| NULLX { $$ = $1; }
| HEX_STRING_VALUE
{
  $$ = $1;
  $$->type_ = T_HEX_STRING;
}
;

number_literal:
INTNUM { $$ = $1; $$->param_num_ = 1;}
| DECIMAL_VAL { $$ = $1; $$->param_num_ = 1;}
;

expr_const:
literal { $$ = $1; }
| SYSTEM_VARIABLE { $$ = $1; }
| QUESTIONMARK { $$ = $1; }
| global_or_session_alias '.' column_name
{
  $3->type_ = T_SYSTEM_VARIABLE;
  $3->value_ = $1[0];
  $$ = $3;
}
;

conf_const:
STRING_VALUE
| DATE_VALUE
| TIMESTAMP_VALUE
| INTNUM
| APPROXNUM
| DECIMAL_VAL
| BOOL_VALUE
| NULLX
| SYSTEM_VARIABLE
| global_or_session_alias '.' column_name
{
  $3->type_ = T_SYSTEM_VARIABLE;
  $3->value_ = $1[0];
  $$ = $3;
}
| '-' INTNUM
{
  $2->value_ = -$2->value_;
  $$ = $2;
}
| '-' DECIMAL_VAL
{
  int32_t len = $2->str_len_ + 2;
  char *str_value = (char *)parse_malloc(len, result->malloc_pool_);
  if (OB_LIKELY(NULL != str_value)) {
    snprintf(str_value, len, "-%.*s", (int32_t)($2->str_len_), $2->str_value_);
    $$ = $2;
    $$->str_value_ = str_value;
    $$->str_len_ = $2->str_len_ + 1;
  } else {
    yyerror(NULL, result, "No more space for copying expression string\n");
    YYABORT_NO_MEMORY;
  }
}
;

global_or_session_alias:
GLOBAL_ALIAS { $$[0] = 1; }
| SESSION_ALIAS { $$[0] = 2; }
;

bool_pri:
bool_pri IS NULLX %prec IS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bool_pri IS not NULLX %prec IS
{
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bool_pri COMP_LE predicate
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LE, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bool_pri COMP_LE sub_query_flag '(' select_no_parens ')'
{
	ParseNode *sub_query = NULL;
	malloc_non_terminal_node(sub_query, result->malloc_pool_, $3->type_, 1, $5);
	malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LE, 2, $1, sub_query);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @6.last_column),
            &@1, result);
}
| bool_pri COMP_LT predicate
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LT, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bool_pri COMP_LT sub_query_flag '(' select_no_parens ')'
{
  ParseNode *sub_query = NULL;
  malloc_non_terminal_node(sub_query, result->malloc_pool_, $3->type_, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LT, 2, $1, sub_query);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @6.last_column),
            &@1, result);
}
| bool_pri COMP_EQ predicate %prec COMP_EQ
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bool_pri COMP_EQ sub_query_flag '(' select_no_parens ')' %prec COMP_EQ
{
  ParseNode *sub_query = NULL;
  malloc_non_terminal_node(sub_query, result->malloc_pool_, $3->type_, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, sub_query);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @6.last_column),
            &@1, result);
}
| bool_pri COMP_NSEQ predicate %prec COMP_NSEQ
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NSEQ, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bool_pri COMP_GE predicate %prec COMP_GE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GE, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bool_pri COMP_GE sub_query_flag '(' select_no_parens ')'  %prec COMP_GE
{
  ParseNode *sub_query = NULL;
  malloc_non_terminal_node(sub_query, result->malloc_pool_, $3->type_, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GE, 2, $1, sub_query);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @6.last_column),
            &@1, result);
}
| bool_pri COMP_GT predicate %prec COMP_GT
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GT, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bool_pri COMP_GT sub_query_flag '(' select_no_parens ')' %prec COMP_GT
{
  ParseNode *sub_query = NULL;
  malloc_non_terminal_node(sub_query, result->malloc_pool_, $3->type_, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GT, 2, $1, sub_query);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @6.last_column),
            &@1, result);
}
| bool_pri COMP_NE predicate %prec COMP_NE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bool_pri COMP_NE sub_query_flag '(' select_no_parens ')' %prec COMP_NE
{
  ParseNode *sub_query = NULL;
  malloc_non_terminal_node(sub_query, result->malloc_pool_, $3->type_, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, sub_query);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @6.last_column),
            &@1, result);
}
| predicate {
  $$ = $1;
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @1.last_column),
            &@1, result);
}
;

predicate:
bit_expr IN in_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IN, 2, $1, $3);
}
| bit_expr not IN in_expr
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_IN, 2, $1, $4);
}
| bit_expr not BETWEEN bit_expr AND predicate  %prec BETWEEN
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_BTW, 3, $1, $4, $6);
}
| bit_expr BETWEEN bit_expr AND predicate %prec BETWEEN
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_BTW, 3, $1, $3, $5);
}
| bit_expr LIKE simple_expr
{
  //In the resolver, if only two children are found, the escape parameter will be set to '\'
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LIKE, 2, $1, $3);
}
| bit_expr LIKE simple_expr ESCAPE simple_expr  %prec LIKE
{
  // If escape is an empty string '', the default value'\' is used
  if (OB_UNLIKELY(T_VARCHAR == $5->type_ && 0 == $5->str_len_)) {
    ParseNode *node = NULL;
    malloc_terminal_node(node, result->malloc_pool_, T_VARCHAR);
    node->str_value_ = "\\";
    node->str_len_ = 1;
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LIKE, 3, $1, $3, node);
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LIKE, 3, $1, $3, $5);
  }
}
| bit_expr not LIKE simple_expr
{
  (void)($2);
  //In the resolver, if only two children are found, the escape parameter will be set to '\'
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_LIKE, 2, $1, $4);
}
| bit_expr not LIKE simple_expr ESCAPE simple_expr %prec LIKE
{
  (void)($2);
  // If escape is an empty string '', the default value'\' is used
  if (OB_UNLIKELY(T_VARCHAR == $6->type_ && 0 == $6->str_len_)) {
    ParseNode *node = NULL;
    malloc_terminal_node(node, result->malloc_pool_, T_VARCHAR);
    node->str_value_ = "\\";
    node->str_len_ = 1;
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_LIKE, 3, $1, $4, node);
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_LIKE, 3, $1, $4, $6);
  }
}
| bit_expr REGEXP bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_REGEXP, 2, $1, $3);
}
| bit_expr not REGEXP bit_expr
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_REGEXP, 2, $1, $4);
}
| bit_expr %prec LOWER_THAN_COMP
{ $$ = $1; }
;

bit_expr:
bit_expr '|' bit_expr %prec '|'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_BIT_OR, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr '&' bit_expr %prec '&'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_BIT_AND, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr SHIFT_LEFT bit_expr %prec SHIFT_LEFT
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_BIT_LEFT_SHIFT, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr SHIFT_RIGHT bit_expr %prec SHIFT_RIGHT
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_BIT_RIGHT_SHIFT, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr '+' bit_expr %prec '+'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_ADD, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr '-' bit_expr %prec '-'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MINUS, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr '+' INTERVAL expr date_unit %prec '+'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 3, $1, $4, $5);
  make_name_node($$, result->malloc_pool_, "date_add");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr '-' INTERVAL expr date_unit %prec '-'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 3, $1, $4, $5);
  make_name_node($$, result->malloc_pool_, "date_sub");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr '*' bit_expr %prec '*'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MUL, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr '/' bit_expr %prec '/'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_DIV, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr '%' bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MOD, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr MOD bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MOD, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr DIV bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_INT_DIV, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr '^' bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_BIT_XOR, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| simple_expr %prec LOWER_THAN_NEG
{
  $$ = $1;
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @1.last_column),
            &@1, result);
}
;

simple_expr:
simple_expr collation %prec NEG
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $1, $2);
  make_name_node($$, result->malloc_pool_, "set_collation");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| BINARY simple_expr %prec NEG
{
  ParseNode *cast_type = NULL;
  malloc_terminal_node(cast_type, result->malloc_pool_, T_CAST_ARGUMENT);
  cast_type->value_ = 0;
  cast_type->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_VARCHAR; /* data type */
  cast_type->int16_values_[OB_NODE_CAST_COLL_IDX] = BINARY_COLLATION; /* is binary */
  cast_type->int32_values_[OB_NODE_CAST_C_LEN_IDX] = DEFAULT_STR_LENGTH; /* precision */
  cast_type->param_num_ = 0;

  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $2, cast_type);
  make_name_node($$, result->malloc_pool_, "cast");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| column_ref { $$ = $1; }
| expr_const { $$ = $1; }
| simple_expr CNNOP simple_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_CNN, 2, $1, $3);
}
| '+' simple_expr %prec NEG
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_POS, 1, $2);
}
| '-' simple_expr %prec NEG
{
  if (T_UINT64 == $2->type_ && 0 == $2->is_assigned_from_child_) {
    uint64_t value = $2->value_;
    int64_t pos = 0;
    for (; pos < $2->str_len_ && ISSPACE($2->str_value_[pos]); pos++);
    int64_t num_len = $2->str_len_ - pos;
    if (INT64_MAX == value - 1) {
      char *new_str_value = (char *)parse_malloc((int32_t)num_len + 2, result->malloc_pool_);
      if (NULL == new_str_value) {
        yyerror(NULL, result, "No more space for copy str");
        YYABORT_NO_MEMORY;
      } else {
        new_str_value[0] = '-';
        memmove(new_str_value + 1, $2->str_value_ + pos, num_len);
        new_str_value[num_len + 1] = '\0';
        $2->str_value_ = new_str_value;
        $2->str_len_ = num_len + 1;
        $2->value_ = INT64_MIN;
        $2->type_ = T_INT;
        $$ = $2;
      }
    } else {
      char *new_str_value = (char *)parse_malloc((int32_t)num_len + 1, result->malloc_pool_);
      if (NULL == new_str_value) {
        yyerror(NULL, result, "No more space for copy str");
        YYABORT_NO_MEMORY;
      } else {
        memmove(new_str_value, $2->str_value_ + pos, num_len);
        new_str_value[num_len] = '\0';
        $2->str_value_ = new_str_value;
        $2->str_len_ = num_len;
        $2->type_ = T_NUMBER;
        malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NEG, 1, $2);
      }
    }
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NEG, 1, $2);
  }
}
| '~' simple_expr %prec NEG
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_BIT_NEG, 1, $2);
}
| not2 simple_expr  %prec NEG
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT, 1, $2);
}
|  select_with_parens %prec NEG
{ $$ = $1; }
| '(' expr ')'
{ $$ = $2; $$->is_assigned_from_child_ = 1; }
| '(' expr_list ',' expr ')'
{
  ParseNode *node = NULL;
  malloc_non_terminal_node(node, result->malloc_pool_, T_LINK_NODE, 2, $2, $4);
  merge_nodes($$, result, T_EXPR_LIST, node);
}
| ROW '(' expr_list ',' expr ')'
{
  ParseNode *node = NULL;
  malloc_non_terminal_node(node, result->malloc_pool_, T_LINK_NODE, 2, $3, $5);
  merge_nodes($$, result, T_EXPR_LIST, node);
}
| EXISTS select_with_parens
{
  if (NULL == $2->children_[PARSE_SELECT_FROM]) {
    $2->value_ = 2;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EXISTS, 1, $2);
}
| MATCH '(' column_list ')' AGAINST '(' STRING_VALUE opt_mode_flag ')'
{
  ParseNode *node = NULL;
  merge_nodes(node, result, T_MATCH_COLUMN_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MATCH_AGAINST, 2, node, $7);
  $$->value_ = $8[0];
}
| case_expr
{
  $$ = $1;
}
| func_expr %prec LOWER_OVER
{
  $$ = $1;
}
| window_function
{
  $$ = $1;
}
/*
  | '{' ident expr '}'
*/
| USER_VARIABLE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GET_USER_VAR, 1, $1);
}
;

opt_mode_flag:
IN NATURAL LANGUAGE MODE
{
  $$[0] = 0;
}
| IN BOOLEAN MODE
{
  $$[0] = 1;
}
| /*empty*/
{
  $$[0] = 0;
};

expr:
expr AND expr %prec AND
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_AND, 2, $1, $3);
}
| expr AND_OP expr %prec AND
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_AND, 2, $1, $3);
}
| expr OR expr %prec OR
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_OR, 2, $1, $3);
}
| expr OR_OP expr %prec OR
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_OR, 2, $1, $3);
}
| expr XOR expr %prec XOR
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_XOR, 2, $1, $3);
}
| NOT expr %prec NOT
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT, 1, $2);
}
| bool_pri IS BOOL_VALUE %prec IS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS, 2, $1, $3);
}
| bool_pri IS not BOOL_VALUE %prec IS
{
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4);
}
| bool_pri IS UNKNOWN %prec IS
{
  /* Unknown is can only appears in grammer 'bool_pri is unknown'
   * and it is equal to NULL semanticly
   * so we set its value to to NULL directly
   */
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_DEFAULT_NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS, 2, $1, node);
}
| bool_pri IS not UNKNOWN %prec IS
{
  (void)($3);
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_DEFAULT_NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS_NOT, 2, $1, node);
}
| bool_pri %prec LOWER_THAN_COMP
{ $$ = $1;}
| USER_VARIABLE SET_VAR expr
{
  $1->type_ = T_LEFT_VALUE;
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_ASSIGN, 2, $1, $3);
}
;

not:
NOT { $$ = NULL;}
| NOT2 { $$ = NULL;}
;

not2:
'!' { $$ = NULL;}
| NOT2 { $$ = NULL;}
;


sub_query_flag:
ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_ALL);
}
| ANY
{
  malloc_terminal_node($$, result->malloc_pool_, T_ANY);
}
| SOME
{
  malloc_terminal_node($$, result->malloc_pool_, T_ANY);
}


in_expr:
select_with_parens %prec NEG
{
  $$ = $1;
}
| '(' expr_list ')'
{ merge_nodes($$, result, T_EXPR_LIST, $2); }
;

case_expr:
CASE case_arg when_clause_list case_default END
{
  merge_nodes($$, result, T_WHEN_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CASE, 3, $2, $$, $4);
}
;

window_function:
COUNT '(' opt_all '*' ')' OVER new_generalized_window_clause
{
  (void)($3);
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 1, node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| COUNT '(' opt_all expr ')' OVER new_generalized_window_clause
{
  malloc_terminal_node($3, result->malloc_pool_, T_ALL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| COUNT '(' DISTINCT expr_list ')' OVER new_generalized_window_clause
{
  ParseNode *distinct = NULL;
  malloc_terminal_node(distinct, result->malloc_pool_, T_DISTINCT);
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 2, distinct, expr_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| APPROX_COUNT_DISTINCT '(' expr_list ')' OVER new_generalized_window_clause
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_APPROX_COUNT_DISTINCT, 1, expr_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $6);
}
| APPROX_COUNT_DISTINCT_SYNOPSIS '(' expr_list ')' OVER new_generalized_window_clause
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS, 1, expr_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $6);
}
| APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE '(' expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE, 1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $6);
}
| SUM '(' opt_distinct_or_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SUM, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| MAX '(' opt_distinct_or_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MAX, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| MIN '(' opt_distinct_or_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MIN, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| AVG '(' opt_distinct_or_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_AVG, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| STD '(' opt_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| STDDEV '(' opt_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| VARIANCE '(' opt_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_VARIANCE, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| STDDEV_POP '(' opt_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV_POP, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| STDDEV_SAMP '(' opt_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV_SAMP, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| VAR_POP '(' opt_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_VAR_POP, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| VAR_SAMP '(' opt_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_VAR_SAMP, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| GROUP_CONCAT '(' opt_distinct expr_list opt_order_by opt_separator ')' OVER new_generalized_window_clause
{
  ParseNode *group_concat_exprs = NULL;
  merge_nodes(group_concat_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_CONCAT, 4, $3, group_concat_exprs, $5, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $9);
}
| LISTAGG '(' opt_distinct expr_list opt_order_by opt_separator ')' OVER new_generalized_window_clause
{
  ParseNode *group_concat_exprs = NULL;
  merge_nodes(group_concat_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_CONCAT, 4, $3, group_concat_exprs, $5, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $9);
}
| RANK '(' ')' OVER new_generalized_window_clause
{
  malloc_terminal_node($$, result->malloc_pool_, T_WIN_FUN_RANK);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $5);
}
| DENSE_RANK '(' ')' OVER new_generalized_window_clause
{
  malloc_terminal_node($$, result->malloc_pool_, T_WIN_FUN_DENSE_RANK);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $5);
}
| PERCENT_RANK '(' ')' OVER new_generalized_window_clause
{
  malloc_terminal_node($$, result->malloc_pool_, T_WIN_FUN_PERCENT_RANK);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $5);
}
| ROW_NUMBER '(' ')' OVER new_generalized_window_clause
{
  malloc_terminal_node($$, result->malloc_pool_, T_WIN_FUN_ROW_NUMBER);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $5);
}
| NTILE '(' expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_FUN_NTILE, 1, $3 );
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $6);
}
| CUME_DIST '(' ')' OVER new_generalized_window_clause
{
  malloc_terminal_node($$, result->malloc_pool_, T_WIN_FUN_CUME_DIST);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $5);
}
| FIRST_VALUE win_fun_first_last_params OVER new_generalized_window_clause
{
  $$ = $2;
  $$->type_ = T_WIN_FUN_FIRST_VALUE;
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $4);
}
| LAST_VALUE win_fun_first_last_params OVER new_generalized_window_clause
{
  $$ = $2;
  $$->type_ = T_WIN_FUN_LAST_VALUE;
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $4);
}
| LEAD win_fun_lead_lag_params OVER new_generalized_window_clause
{
  $$ = $2;
  $$->type_ = T_WIN_FUN_LEAD;
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $4);
}
| LAG win_fun_lead_lag_params OVER new_generalized_window_clause
{
  $$ = $2;
  $$->type_ = T_WIN_FUN_LAG;
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $4);
}
| NTH_VALUE '(' expr ',' expr ')' opt_from_first_or_last opt_respect_or_ignore_nulls OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_FUN_NTH_VALUE, 4, $3, $5, $7, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
;

first_or_last:
FIRST
{
  malloc_terminal_node($$, result->malloc_pool_, T_FIRST);
}
|
LAST
{
  malloc_terminal_node($$, result->malloc_pool_, T_LAST);
}
;

opt_from_first_or_last:
FROM first_or_last
{
  $$ = $2;
}
|
/* empty */
{ $$ = NULL;}
;

respect_or_ignore:
RESPECT
{
  malloc_terminal_node($$, result->malloc_pool_, T_RESPECT);
}
|
IGNORE
{
  malloc_terminal_node($$, result->malloc_pool_, T_IGNORE);
}
;

opt_respect_or_ignore_nulls:
respect_or_ignore NULLS
{
  (void)($2) ; /* make bison mute */
  $$ = $1;
}
|
/* empty */
{ $$ = NULL;}
;

win_fun_first_last_params:
'(' expr respect_or_ignore NULLS ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INVALID, 2, $2, $3);
}
| '(' expr ')' opt_respect_or_ignore_nulls
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INVALID, 2, $2, $4);
}
;

win_fun_lead_lag_params:
'(' expr respect_or_ignore NULLS ')'
{
  ParseNode *params_node = NULL;
  malloc_non_terminal_node(params_node, result->malloc_pool_, T_LINK_NODE, 2, $2, NULL);
  merge_nodes(params_node, result, T_EXPR_LIST, params_node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INVALID, 2, params_node, $3);
}
|
'(' expr respect_or_ignore NULLS ',' expr_list ')'
{
  ParseNode *params_node = NULL;
  malloc_non_terminal_node(params_node, result->malloc_pool_, T_LINK_NODE, 2, $2, $6);
  merge_nodes(params_node, result, T_EXPR_LIST, params_node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INVALID, 2, params_node, $3);
}
| '(' expr_list ')' opt_respect_or_ignore_nulls
{
  ParseNode *params_node = NULL;
  merge_nodes(params_node, result, T_EXPR_LIST, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INVALID, 2, params_node, $4);
}
;

new_generalized_window_clause:
NAME_OB
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_NEW_GENERALIZED_WINDOW, 2, $1, NULL);
}
|
new_generalized_window_clause_with_blanket
{
  $$ = $1;
};

new_generalized_window_clause_with_blanket:
'(' NAME_OB generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_NEW_GENERALIZED_WINDOW, 2, $2, $3);
}
|
'(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_NEW_GENERALIZED_WINDOW, 2, NULL, $2);
}
;

opt_named_windows:
/* EMPTY */
{ $$ = NULL; }
| WINDOW named_windows
{
  merge_nodes($$, result, T_WIN_NAMED_WINDOWS, $2);
}
;

named_windows:
named_window
{
  $$ = $1;
}
| named_windows ',' named_window
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

named_window:
NAME_OB AS new_generalized_window_clause_with_blanket
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_NAMED_WINDOW, 2, $1, $3);
  dup_string($3, result, @3.first_column, @3.last_column);
}
;

generalized_window_clause:
opt_partition_by opt_order_by opt_win_window
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_GENERALIZED_WINDOW, 3, $1, $2, $3);
}
;

opt_partition_by:
/* EMPTY */
{ $$ = NULL;}
| PARTITION BY expr_list
{
  merge_nodes($$, result, T_EXPR_LIST, $3);
};

win_rows_or_range:
ROWS
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->is_hidden_const_ = 1;
  $$->value_ = 1;
}
| RANGE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->is_hidden_const_ = 1;
  $$->value_ = 2;
};

win_preceding_or_following:
PRECEDING
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->is_hidden_const_ = 1;
  $$->value_ = 1;
}
| FOLLOWING
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->is_hidden_const_ = 1;
  $$->value_ = 2;
}
;

win_interval:
expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_INTERVAL, 1, $1);
  $$->value_ = 1;
}
| INTERVAL expr date_unit
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_INTERVAL, 2, $2, $3);
  $$->value_ = 2;
};

win_bounding:
CURRENT ROW
{
  malloc_terminal_node($$, result->malloc_pool_, T_WIN_BOUND);
  $$->value_ = 1;
}
| win_interval win_preceding_or_following
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_BOUND, 2, $1, $2);
  dup_string($1, result, @1.first_column, @1.last_column);
  $$->value_ = 2;
};

win_window:
win_rows_or_range BETWEEN win_bounding AND win_bounding
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_WINDOW, 3, $1, $3, $5);
  $$->value_ = 1;
}
| win_rows_or_range win_bounding
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_WINDOW, 2, $1, $2);
  $$->value_ = 2;
}
;

opt_win_window:
/* EMPTY */
{ $$ = NULL;}
| win_window
{
  $$ = $1;
}
;

case_arg:
expr                  { $$ = $1; }
| /*EMPTY*/             { $$ = NULL; }
;

when_clause_list:
when_clause { $$ = $1; }
| when_clause_list when_clause
{ malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2); }
;

when_clause:
WHEN expr THEN expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WHEN, 2, $2, $4);
}
;

case_default:
ELSE expr   { $$ = $2; }
| /*EMPTY*/ { malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_NULL); }
;

opt_all:
ALL {
  $$ = NULL;
}
| /*empty*/{ $$ = NULL; }
;

func_expr:
MOD '(' expr ',' expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MOD, 2, $3, $5);
}
| COUNT '(' opt_all '*' ')'
{
  (void)($3);
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 1, node);
}
| COUNT '(' opt_all expr ')'
{
  malloc_terminal_node($3, result->malloc_pool_, T_ALL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 2, $3, $4);
}
| COUNT '(' DISTINCT expr_list ')'
{
  ParseNode *distinct = NULL;
  malloc_terminal_node(distinct, result->malloc_pool_, T_DISTINCT);
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 2, distinct, expr_list);
}
| COUNT '(' UNIQUE expr_list ')'
{
  ParseNode *distinct = NULL;
  malloc_terminal_node(distinct, result->malloc_pool_, T_DISTINCT);
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 2, distinct, expr_list);
}
| APPROX_COUNT_DISTINCT '(' expr_list ')'
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_APPROX_COUNT_DISTINCT, 1, expr_list);
}
| APPROX_COUNT_DISTINCT_SYNOPSIS '(' expr_list ')'
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS, 1, expr_list);
}
| APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE '(' expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE, 1, $3);
}
| SUM '(' opt_distinct_or_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SUM, 2, $3, $4);
}
| MAX '(' opt_distinct_or_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MAX, 2, $3, $4);
}
| MIN '(' opt_distinct_or_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MIN, 2, $3, $4);
}
| AVG '(' opt_distinct_or_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_AVG, 2, $3, $4);
}
| STD '(' opt_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV, 2, $3, $4);
}
| STDDEV '(' opt_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV, 2, $3, $4);
}
| VARIANCE '(' opt_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_VARIANCE, 2, $3, $4);
}
| STDDEV_POP '(' opt_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV_POP, 2, $3, $4);
}
| STDDEV_SAMP '(' opt_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV_SAMP, 2, $3, $4);
}
| VAR_POP '(' opt_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_VAR_POP, 2, $3, $4);
}
| VAR_SAMP '(' opt_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_VAR_SAMP, 2, $3, $4);
}
| GROUPING '(' expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUPING, 1, $3);
}
| GROUP_CONCAT '(' opt_distinct expr_list opt_order_by opt_separator ')'
{
  ParseNode *group_concat_exprs = NULL;
  merge_nodes(group_concat_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_CONCAT, 4, $3, group_concat_exprs, $5, $6);
}
| IF '(' expr ',' expr ',' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 3, $3, $5, $7);
  make_name_node($$, result->malloc_pool_, "if");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_IF, 2, $$, params);
}
| ISNULL '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "isnull");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_ISNULL, 2, $$, params);
}
| cur_timestamp_func
{
  $$ = $1;
}
| sysdate_func
{
  $$ = $1;
}
| cur_time_func
{
  $$ = $1;
}
| cur_date_func
{
  $$ = $1;
}
| utc_timestamp_func
{
  $$ = $1;
}
| CAST '(' expr AS cast_data_type ')'
{
  //cast_data_type is a T_CAST_ARGUMENT rather than a T_INT to avoid being parameterized automatically
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "cast");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| INSERT '(' expr ',' expr ',' expr ',' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 4, $3, $5, $7, $9);
  make_name_node($$, result->malloc_pool_, "insert");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| LEFT '(' expr ',' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "left");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| CONVERT '(' expr ',' cast_data_type ')'
{
  //same as CAST
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "cast");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| CONVERT '(' expr USING charset_name ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "convert");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| POSITION '(' bit_expr IN expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "position");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| substr_or_substring '(' substr_params ')'
{
  (void)($1);               /* unused */
  make_name_node($$, result->malloc_pool_, "substr");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, $3);
}
| TRIM '(' parameterized_trim ')'
{
  make_name_node($$, result->malloc_pool_, "trim");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, $3);
}
| DATE '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "date");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| YEAR '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "year");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| TIME '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "time");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| MONTH '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "month");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| WEEK '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "week");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| WEEK '(' expr ',' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "week");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| QUARTER '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "quarter");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| SECOND '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "second");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| MINUTE '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "minute");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| MICROSECOND '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "microsecond");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| HOUR '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "hour");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| DATE_ADD '(' date_params ')'
{
  make_name_node($$, result->malloc_pool_, "date_add");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, $3);
}
| DATE_SUB '(' date_params ')'
{
  make_name_node($$, result->malloc_pool_, "date_sub");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, $3);
}
| ADDDATE '(' date_params ')'
{
  make_name_node($$, result->malloc_pool_, "date_add");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, $3);
}
| SUBDATE '(' date_params ')'
{
  make_name_node($$, result->malloc_pool_, "date_sub");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, $3);
}
| ADDDATE '(' expr ',' expr ')'
{
  ParseNode *param = NULL;
  ParseNode *interval = NULL;

  malloc_terminal_node(interval, result->malloc_pool_, T_INT);
  interval->value_ = DATE_UNIT_DAY;
  interval->is_hidden_const_ = 1;

  malloc_non_terminal_node(param, result->malloc_pool_, T_EXPR_LIST, 3, $3, $5, interval);

  make_name_node($$, result->malloc_pool_, "date_add");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, param);
}
| SUBDATE '(' expr ',' expr ')'
{
  ParseNode *param = NULL;
  ParseNode *interval = NULL;

  malloc_terminal_node(interval, result->malloc_pool_, T_INT);
  interval->value_ = DATE_UNIT_DAY;
  interval->is_hidden_const_ = 1;

  malloc_non_terminal_node(param, result->malloc_pool_, T_EXPR_LIST, 3, $3, $5, interval);

  make_name_node($$, result->malloc_pool_, "date_sub");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, param);
}
| TIMESTAMPDIFF '(' timestamp_params ')'
{
  make_name_node($$, result->malloc_pool_, "timestampdiff");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, $3);
}
| TIMESTAMPADD '(' timestamp_params ')'
{
  make_name_node($$, result->malloc_pool_, "timestampadd");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, $3);
}
| EXTRACT '(' date_unit FROM expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "extract");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| ASCII '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "ascii");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| DEFAULT '(' column_definition_ref ')'
{
  ParseNode *node = NULL;
  ParseNode *null_node1 = NULL;
  ParseNode *null_node2 = NULL;
  ParseNode *null_node3 = NULL;
  ParseNode *null_node4 = NULL;
  malloc_terminal_node(null_node1, result->malloc_pool_, T_NULL);
  null_node1->is_hidden_const_ = 1;
  malloc_terminal_node(null_node2, result->malloc_pool_, T_NULL);
  null_node2->is_hidden_const_ = 1;
  malloc_terminal_node(null_node3, result->malloc_pool_, T_NULL);
  null_node3->is_hidden_const_ = 1;
  malloc_terminal_node(null_node4, result->malloc_pool_, T_NULL);
  null_node4->is_hidden_const_ = 1;
  malloc_non_terminal_node(node, result->malloc_pool_, T_EXPR_LIST, 5, $3, null_node1,
                           null_node2, null_node3, null_node4);
  make_name_node($$, result->malloc_pool_, "default");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, node);

}
| VALUES '(' column_definition_ref ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $3);
  make_name_node($$, result->malloc_pool_, "values");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| CHARACTER '(' expr_list ')'
{
  //default using binary
  ParseNode *charset_node = NULL;
  malloc_terminal_node(charset_node, result->malloc_pool_, T_CHAR_CHARSET);
  charset_node->str_value_ = parse_strdup("binary", result->malloc_pool_, &(charset_node->str_len_));
  if (OB_UNLIKELY(NULL == charset_node->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }

  ParseNode *params_node = NULL;
  malloc_non_terminal_node(params_node, result->malloc_pool_, T_LINK_NODE, 2, charset_node, $3);
  merge_nodes(params_node, result, T_EXPR_LIST, params_node);

  make_name_node($$, result->malloc_pool_, "char");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params_node);

}
| CHARACTER '(' expr_list USING charset_name')'
{
  ParseNode *params_node = NULL;
  $5->type_ = T_CHAR_CHARSET;
  malloc_non_terminal_node(params_node, result->malloc_pool_, T_LINK_NODE, 2, $5, $3);
  merge_nodes(params_node, result, T_EXPR_LIST, params_node);

  make_name_node($$, result->malloc_pool_, "char");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params_node);
}
| function_name '(' opt_expr_as_list ')'
{
  if (NULL != $3)
  {
    ParseNode *params = NULL;
    merge_nodes(params, result, T_EXPR_LIST, $3);
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $1, params);
    store_pl_ref_object_symbol($$, result, REF_FUNC);
  }
  else
  {
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $1);
    store_pl_ref_object_symbol($$, result, REF_FUNC);
  }
}
| relation_name '.' function_name '(' opt_expr_as_list ')'
{
  if (NULL != $5)
  {
    ParseNode *params = NULL;
    merge_nodes(params, result, T_EXPR_LIST, $5);
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_UDF, 4, $3, params, $1, NULL);
    store_pl_ref_object_symbol($$, result, REF_FUNC);
  }
  else
  {
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_UDF, 4, $3, NULL, $1, NULL);
    store_pl_ref_object_symbol($$, result, REF_FUNC);
  }
}
| sys_interval_func
{
  $$ = $1;
}
;

sys_interval_func:
INTERVAL '(' expr ',' expr ')'
{
  make_name_node($$, result->malloc_pool_, "interval");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_INTERVAL, 2, $3, $5);
}
| INTERVAL '(' expr ',' expr ',' expr_list ')'
{
  ParseNode *params = NULL;
  ParseNode *params_node = NULL;
  make_name_node($$, result->malloc_pool_, "interval");
  malloc_non_terminal_node(params, result->malloc_pool_, T_LINK_NODE, 2, $5, $7);
  merge_nodes(params_node, result, T_EXPR_LIST, params);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_INTERVAL, 2, $3, params_node);
}
;

utc_timestamp_func:
UTC_TIMESTAMP '(' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_UTC_TIMESTAMP, 1, NULL);
}
| UTC_TIMESTAMP '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_UTC_TIMESTAMP, 1, $3);
}
;

sysdate_func:
SYSDATE '(' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_SYSDATE, 1, NULL);
}
| SYSDATE '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_SYSDATE, 1, $3);
}
;

cur_timestamp_func:
NOW '(' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIMESTAMP, 1, NULL);
}
| NOW '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIMESTAMP, 1, $3);
}
| now_synonyms_func opt_time_func_fsp_i
{
  (void)($1);
  if (0 != $2[1])
  {
    ParseNode *params = NULL;
    malloc_terminal_node(params, result->malloc_pool_, T_INT);
    params->value_ = $2[0];
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIMESTAMP, 1, params);
  }
  else
  {
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIMESTAMP, 1, NULL);
  }

}
;

now_synonyms_func:
CURRENT_TIMESTAMP {$$ = NULL;}
| LOCALTIME {$$ = NULL;}
| LOCALTIMESTAMP {$$ = NULL;}
;

cur_time_func:
CURTIME  '(' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIME, 1, NULL);
}
| CURTIME  '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIME, 1, $3);
}
| CURRENT_TIME opt_time_func_fsp_i
{
  if (0 != $2[1])
  {
    ParseNode *params = NULL;
    malloc_terminal_node(params, result->malloc_pool_, T_INT);
    params->value_ = $2[0];
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIME, 1, params);
  }
  else
  {
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIME, 1, NULL);
  }
}
;

cur_date_func:
CURDATE '(' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_DATE, 1, NULL);
}
| CURRENT_DATE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_DATE, 1, NULL);
}
| CURRENT_DATE '(' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_DATE, 1, NULL);
}
;

opt_time_func_fsp_i:
'(' INTNUM ')'  { $$[0] = $2->value_; $$[1] = 1; }
| '('  ')'        { $$[0] = 0; $$[1] = 0; }
| /*EMPTY*/       { $$[0] = 0; $$[1] = 0; }
;



substr_or_substring:
SUBSTR
{
  $$ = NULL;
}
| SUBSTRING
{
  $$ = NULL;
}
;


substr_params:
expr ',' expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 2, $1, $3);
}
| expr ',' expr ',' expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, $1, $3, $5);
}
| expr FROM expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 2, $1, $3);
}
| expr FROM expr FOR expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, $1, $3, $5);
}
;

date_params:
expr ',' INTERVAL expr date_unit
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, $1, $4, $5);
}
;

timestamp_params:
date_unit ',' expr ',' expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, $1, $3, $5);
}
;

opt_expr_as_list:
/* EMPTY */
{ $$ = NULL; }
| expr_as_list
{ $$ = $1; }
;

opt_distinct:
/* EMPTY */
{
  $$ = NULL;
}
| DISTINCT
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTINCT);
}
| UNIQUE
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTINCT);
}
;

opt_distinct_or_all:
/* EMPTY */
{
  $$ = NULL;
}
| ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_ALL);
}
| DISTINCT
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTINCT);
}
| UNIQUE
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTINCT);
}
;

opt_separator:
/* EMPTY */
{
  $$ = NULL;
}
| SEPARATOR STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SEPARATOR_CLAUSE, 1, $2);
}
;

/*****************************************************************************
 *
 *	delete grammar
 *
 *****************************************************************************/
delete_stmt:
delete_with_opt_hint FROM tbl_name opt_where opt_order_by opt_limit_clause
{
  ParseNode *from_list = NULL;
  ParseNode *delete_table_node = NULL;
  merge_nodes(from_list, result, T_TABLE_REFERENCES, $3);
  malloc_non_terminal_node(delete_table_node, result->malloc_pool_, T_DELETE_TABLE_NODE, 2,
                           NULL, /*0. delete list*/
                           from_list);    /*1. from list*/
  malloc_non_terminal_node($$, result->malloc_pool_, T_DELETE, 7,
                           delete_table_node,   /* 0. table_node */
                           $4,      /* 1. where      */
                           $5,      /* 2. order by   */
                           $6,      /* 3. limit      */
                           NULL,      /* 4. when       */
                           $1,      /* 5. hint       */
                           NULL      /* 6. returning, unused in mysql  */
                           );

}
| delete_with_opt_hint multi_delete_table opt_where
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DELETE, 7,
                           $2,   /* 0. table_node */
                           $3,        /* 1. where      */
                           NULL,      /* 2. order by   */
                           NULL,      /* 3. limit      */
                           NULL,      /* 4. when       */
                           $1,        /* 5. hint       */
                           NULL       /* 6. returning, unused in mysql  */
                           );
}
;

multi_delete_table:
relation_with_star_list FROM table_references
{
  ParseNode *delete_list = NULL;
  ParseNode *from_list = NULL;
  merge_nodes(delete_list, result, T_TABLE_REFERENCES, $1);
  merge_nodes(from_list, result, T_TABLE_REFERENCES, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DELETE_TABLE_NODE, 2,
                           delete_list,   /*0. delete list*/
                           from_list);    /*1. from list*/
}
| FROM relation_with_star_list USING table_references
{
  ParseNode *delete_list = NULL;
  ParseNode *from_list = NULL;
  merge_nodes(delete_list, result, T_TABLE_REFERENCES, $2);
  merge_nodes(from_list, result, T_TABLE_REFERENCES, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DELETE_TABLE_NODE, 2,
                           delete_list,   /*0. delete list*/
                           from_list);    /*1. from list*/
}
;

/*****************************************************************************
 *
 *	update grammar
 *
 *****************************************************************************/

update_stmt:
update_with_opt_hint opt_ignore table_references SET update_asgn_list opt_where opt_order_by opt_limit_clause
{
  ParseNode *from_list = NULL;
  ParseNode *assign_list = NULL;
  merge_nodes(from_list, result, T_TABLE_REFERENCES, $3);
  merge_nodes(assign_list, result, T_ASSIGN_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_UPDATE, 9,
                           from_list,     /* 0. table node */
                           assign_list,   /* 1. update list */
                           $6,            /* 2. where node */
                           $7,            /* 3. order by node */
                           $8,            /* 4. limit node */
                           NULL,            /* 5. when node */
                           $1,            /* 6. hint node */
                           $2,            /* 7. ignore */
                           NULL            /* 8. returning, unused in mysql */
                           );

}
;

update_asgn_list:
update_asgn_factor
{
  $$ = $1;
}
| update_asgn_list ',' update_asgn_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

update_asgn_factor:
column_definition_ref COMP_EQ expr_or_default
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ASSIGN_ITEM, 2, $1, $3);
}
;
/*****************************************************************************
 *
 *	resource unit & resource pool grammar
 *
 *****************************************************************************/

create_resource_stmt:
CREATE RESOURCE UNIT opt_if_not_exists relation_name opt_resource_unit_option_list
{
  ParseNode *resource_options = NULL;
  merge_nodes(resource_options, result, T_RESOURCE_UNIT_OPTION_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_RESOURCE_UNIT, 3,
                           $4,
                           $5,                     /* resource unit name */
                           resource_options);      /* resource opt */
}
| CREATE RESOURCE POOL opt_if_not_exists relation_name opt_create_resource_pool_option_list
{
  ParseNode *resource_options = NULL;
  merge_nodes(resource_options, result, T_RESOURCE_POOL_OPTION_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_RESOURCE_POOL, 3,
                           $4,
                           $5,                     // resource pool name
                           resource_options);      // resource opt
}
;
opt_resource_unit_option_list:
resource_unit_option
{
  $$ = $1;
}
| opt_resource_unit_option_list ',' resource_unit_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

resource_unit_option:
MIN_CPU opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MIN_CPU, 1, $3);
}
| MIN_IOPS opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MIN_IOPS, 1, $3);
}
| MIN_MEMORY opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MIN_MEMORY, 1, $3);
}
| MAX_CPU opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_CPU, 1, $3);
}
| MAX_MEMORY opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_MEMORY, 1, $3);
}
| MAX_IOPS opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_IOPS, 1, $3);
}
| MAX_DISK_SIZE opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_DISK_SIZE, 1, $3);
}
| MAX_SESSION_NUM opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_SESSION_NUM, 1, $3);
}
;

opt_create_resource_pool_option_list:
create_resource_pool_option
{
  $$ = $1;
}
| opt_create_resource_pool_option_list ',' create_resource_pool_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

create_resource_pool_option:
UNIT opt_equal_mark relation_name_or_string
{
  (void)($2); /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_UNIT, 1, $3);
}
| UNIT_NUM opt_equal_mark INTNUM
{
  (void)($2); /*make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_UNIT_NUM, 1, $3);
}
| ZONE_LIST opt_equal_mark '(' zone_list ')'
{
  (void)($2); /* make bison mute */
  merge_nodes($$, result, T_ZONE_LIST, $4);
}
| REPLICA_TYPE opt_equal_mark STRING_VALUE
{
  (void)($2); /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_REPLICA_TYPE, 1, $3);
}
;

alter_resource_pool_option_list:
alter_resource_pool_option
{
  $$ = $1;
}
| alter_resource_pool_option_list ',' alter_resource_pool_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

unit_id_list:
INTNUM
{
  $$ = $1;
}
|
unit_id_list ',' INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

opt_shrink_unit_option:
DELETE UNIT opt_equal_mark '(' unit_id_list ')'
{
  (void)($3); /* make bison mute */
  merge_nodes($$, result, T_UNIT_ID_LIST, $5);
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

alter_resource_pool_option:
UNIT opt_equal_mark relation_name_or_string
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_UNIT, 1, $3);
}
| UNIT_NUM opt_equal_mark INTNUM opt_shrink_unit_option
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_UNIT_NUM, 2, $3, $4);
}
| ZONE_LIST opt_equal_mark '(' zone_list ')'
{
  (void)($2) ; /* make bison mute */
  merge_nodes($$, result, T_ZONE_LIST, $4);
}
;

alter_resource_stmt:
ALTER RESOURCE UNIT relation_name opt_resource_unit_option_list
{
  ParseNode *resource_options = NULL;
  merge_nodes(resource_options, result, T_RESOURCE_UNIT_OPTION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_RESOURCE_UNIT, 2,
                           $4,                     /* resource unit name */
                           resource_options);      /* resource opt */
}
| ALTER RESOURCE POOL relation_name alter_resource_pool_option_list
{
  ParseNode *resource_pool_options = NULL;
  merge_nodes(resource_pool_options, result, T_RESOURCE_POOL_OPTION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_RESOURCE_POOL, 2,
                           $4,                          /* resource_pool name */
                           resource_pool_options);      /* resource_pool opt */
}
| ALTER RESOURCE POOL relation_name SPLIT INTO '(' resource_pool_list ')' ON '(' zone_list ')'
{
  ParseNode *resource_pool_list = NULL;
  ParseNode *zone_list = NULL;
  merge_nodes(resource_pool_list, result, T_RESOURCE_POOL_LIST, $8);
  merge_nodes(zone_list, result, T_ZONE_LIST, $12);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SPLIT_RESOURCE_POOL, 3,
                           $4,                     /* resource pool name */
                           resource_pool_list,     /* new pool names */
                           zone_list);             /* corresponding zones */
}
| ALTER RESOURCE POOL MERGE '(' resource_pool_list ')' INTO '(' resource_pool_list ')'
{
  ParseNode *old_resource_pool_list = NULL;
  ParseNode *new_resource_pool_list = NULL;
  merge_nodes(old_resource_pool_list, result, T_RESOURCE_POOL_LIST, $6);
  merge_nodes(new_resource_pool_list, result, T_RESOURCE_POOL_LIST, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE_RESOURCE_POOL, 2,
                           old_resource_pool_list,                       /* to be merged*/
                           new_resource_pool_list);                      /* finish merge*/
}
;

drop_resource_stmt:
DROP RESOURCE UNIT opt_if_exists relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_RESOURCE_UNIT, 2, $4, $5);
}
| DROP RESOURCE POOL opt_if_exists relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_RESOURCE_POOL, 2, $4, $5);
}
;

/*****************************************************************************
 *
 *	tenant grammar
 *
 *****************************************************************************/
create_tenant_stmt:
CREATE TENANT opt_if_not_exists relation_name
opt_tenant_option_list opt_set_sys_var
{
  ParseNode *tenant_options = NULL;
  merge_nodes(tenant_options, result, T_TENANT_OPTION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TENANT, 4,
                           $3,                   /* if not exists */
                           $4,                   /* tenant name */
                           tenant_options,      /* tenant opt */
                           $6);      /* system variable set opt */
};

opt_tenant_option_list:
tenant_option
{
  $$ = $1;
}
| opt_tenant_option_list ',' tenant_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

tenant_option:
LOGONLY_REPLICA_NUM opt_equal_mark INTNUM
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOGONLY_REPLICA_NUM, 1, $3);
}
| LOCALITY opt_equal_mark STRING_VALUE opt_force
{
  (void)($2) ;
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOCALITY, 2, $3, $4);
}
| REPLICA_NUM opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_REPLICA_NUM, 1, $3);
}
| REWRITE_MERGE_VERSION opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_REWRITE_MERGE_VERSION, 1, $3);
}
| STORAGE_FORMAT_VERSION opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_STORAGE_FORMAT_VERSION, 1, $3);
}
| STORAGE_FORMAT_WORK_VERSION opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_STORAGE_FORMAT_WORK_VERSION, 1, $3);
}
| PRIMARY_ZONE opt_equal_mark primary_zone_name
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_ZONE, 1, $3);
}
| RESOURCE_POOL_LIST opt_equal_mark '(' resource_pool_list ')'
{
  (void)($2) ; /* make bison mute */
  merge_nodes($$, result, T_TENANT_RESOURCE_POOL_LIST, $4);
}
| ZONE_LIST opt_equal_mark '(' zone_list ')'
{
  (void)($2) ; /* make bison mute */
  merge_nodes($$, result, T_ZONE_LIST, $4);
}
| charset_key opt_equal_mark charset_name
{
  (void)($1);
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_CHARSET);
  $$->str_value_ = $3->str_value_;
  $$->str_len_ = $3->str_len_;
}
| COLLATE opt_equal_mark collation_name
{
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_COLLATION);
  $$->str_value_ = $3->str_value_;
  $$->str_len_ = $3->str_len_;
  $$->param_num_ = $3->param_num_;
}
| read_only_or_write
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_READ_ONLY, 1, $1);
}
| COMMENT opt_equal_mark STRING_VALUE
{
  (void)($2); /*  make bison mute*/
  malloc_non_terminal_node($$, result->malloc_pool_, T_COMMENT, 1, $3);
}
| default_tablegroup
{
  $$ = $1;
}
| PROGRESSIVE_MERGE_NUM opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_PROGRESSIVE_MERGE_NUM, 1, $3);
}
;

opt_set_sys_var:
SET sys_var_and_val_list
{
  merge_nodes($$, result, T_VARIABLE_SET, $2);
}
| SET VARIABLES sys_var_and_val_list
{
  merge_nodes($$, result, T_VARIABLE_SET, $3);
}
| VARIABLES sys_var_and_val_list
{
  merge_nodes($$, result, T_VARIABLE_SET, $2);
}
| /*empty*/
{
  $$ = NULL;
};

opt_global_sys_vars_set:
VARIABLES sys_var_and_val_list
{
  merge_nodes($$, result, T_VARIABLE_SET, $2);
}
| /*empty*/
{
  $$ = NULL;
};



/* TODO: () refactor after requirement determined */
zone_list:
STRING_VALUE
{ $$ = $1; }
| zone_list opt_comma STRING_VALUE
{ (void)($2); malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3); }
;

resource_pool_list:
STRING_VALUE
{ $$ = $1; }
| resource_pool_list ',' STRING_VALUE
{ malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3); }
;

alter_tenant_stmt:
ALTER TENANT relation_name opt_set opt_tenant_option_list opt_global_sys_vars_set
{
  (void)$4;
  ParseNode *tenant_options = NULL;
  merge_nodes(tenant_options, result, T_TENANT_OPTION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_MODIFY_TENANT, 4,
                           $3,                   /* tenant name */
                           tenant_options,       /* tenant opt */
                           $6,                   /* global sys vars set opt */
                           NULL);                /* new tenant name */
}
| ALTER TENANT ALL opt_set opt_tenant_option_list opt_global_sys_vars_set
{
  (void)$4;
  ParseNode *tenant_options = NULL;
  merge_nodes(tenant_options, result, T_TENANT_OPTION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_MODIFY_TENANT, 4,
                           NULL,                 /* tenant name */
                           tenant_options,       /* tenant opt */
                           $6,                   /* global sys vars set opt */
                           NULL);                /* new tenant name */
}
| ALTER TENANT relation_name RENAME GLOBAL_NAME TO relation_name // add by xiaonfeng
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MODIFY_TENANT, 4,
                           $3,                   /* tenant name */
                           NULL,                 /* tenant opt */
                           NULL,                 /* global sys vars set opt */
                           $7);                  /* new tenant name */
}
| ALTER TENANT relation_name lock_spec_mysql57
{
  /*ParseNode *tenant_options = NULL;*/
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOCK_TENANT, 2,
                           $3,                   /* tenant name */
                           $4);                  /* lock opt */
}
;

drop_tenant_stmt:
DROP TENANT opt_if_exists relation_name opt_force_purge
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_TENANT, 3, $3, $4, $5);
}
;

create_restore_point_stmt:
CREATE RESTORE POINT relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_RESTORE_POINT, 1, $4);
}
;
drop_restore_point_stmt:
DROP RESTORE POINT relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_RESTORE_POINT, 1, $4);
}
;

/*****************************************************************************
 *
 *	create database grammar
 *
 *****************************************************************************/

create_database_stmt:
CREATE database_key opt_if_not_exists database_factor opt_database_option_list
{
  (void)($2);
  ParseNode *database_option = NULL;
  merge_nodes(database_option, result, T_DATABASE_OPTION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_DATABASE, 3, $3, $4, database_option);
}
;

database_key:
DATABASE
{
  $$ = NULL;
}
| SCHEMA
{
  $$ = NULL;
}

database_factor:
relation_name
{
  $$ = $1;
}
;

opt_database_option_list:
database_option_list
{
  $$ = $1;
}
|
{
  $$ = NULL;
}
;

database_option_list:
database_option
{
  $$ = $1;
}
| database_option_list  database_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

charset_key:
CHARSET
{
  $$ = NULL;
}
| CHARACTER SET
{
  $$ = NULL;
}

database_option:
opt_default_mark charset_key opt_equal_mark charset_name
{
  (void)($1);
  (void)($2);
  (void)($3);
  malloc_terminal_node($$, result->malloc_pool_, T_CHARSET);
  $$->str_value_ = $4->str_value_;
  $$->str_len_ = $4->str_len_;
}
| opt_default_mark COLLATE opt_equal_mark collation_name
{
  (void)($1);
  (void)($3);
  malloc_terminal_node($$, result->malloc_pool_, T_COLLATION);
  $$->str_value_ = $4->str_value_;
  $$->str_len_ = $4->str_len_;
  $$->param_num_ = $4->param_num_;
}
| REPLICA_NUM opt_equal_mark INTNUM
{
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_REPLICA_NUM);
  $$->value_ = $3->value_;
}
| PRIMARY_ZONE opt_equal_mark primary_zone_name
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_ZONE, 1, $3);
}
| read_only_or_write
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_READ_ONLY, 1, $1);
}
| default_tablegroup
{
  $$ = $1;
}
| DATABASE_ID opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_DATABASE_ID, 1, $3);
}
;

read_only_or_write:
READ ONLY { malloc_terminal_node($$, result->malloc_pool_, T_ON); }
|
READ WRITE { malloc_terminal_node($$, result->malloc_pool_, T_OFF); }
;

/*****************************************************************************
 *
 *	drop database grammar
 *
 *****************************************************************************/
drop_database_stmt:
DROP database_key opt_if_exists database_factor
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_DATABASE, 2, $3, $4);
}
;

/*****************************************************************************
 *
 *	alter database grammar
 *
 *****************************************************************************/
alter_database_stmt:
ALTER database_key opt_database_name opt_set database_option_list
{
  (void)($2);
  (void)($4);
  ParseNode *database_option = NULL;
  merge_nodes(database_option, result, T_DATABASE_OPTION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_DATABASE, 2, $3, database_option);
}
;

opt_database_name:
NAME_OB
{
  $$ = $1;
}
|
{
  $$ = NULL;
}
;
/*****************************************************************************
 *
 *	load data grammar
 *
 *****************************************************************************/
load_data_stmt:
load_data_with_opt_hint opt_load_local INFILE STRING_VALUE opt_duplicate INTO TABLE
relation_factor opt_use_partition opt_load_charset field_opt line_opt opt_load_ignore_rows
opt_field_or_var_spec opt_load_set_spec
{
  (void) $9;
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOAD_DATA, 11,
                           $2,            /* 0. local */
                           $4,            /* 1. filename */
                           $5,            /* 2. duplicate  */
                           $8,            /* 3. table */
                           $10,           /* 4. charset */
                           $11,           /* 5. field */
                           $12,           /* 6. line */
                           $13,           /* 7. ignore rows */
                           $14,           /* 8. field or vars */
                           $15,           /* 9. set field  */
                           $1             /* 10. hint */
                           );
}
;

load_data_with_opt_hint:
LOAD DATA {$$ = NULL;}
| LOAD_DATA_HINT_BEGIN hint_list_with_end
{$$ = $2;}
;

opt_load_local:
/* empty */
{
  $$= NULL;
}
| LOCAL
{
  malloc_terminal_node($$, result->malloc_pool_, T_LOCAL);
}
| REMOTE_OSS
{
  malloc_terminal_node($$, result->malloc_pool_, T_REMOTE_OSS);
}
;

opt_duplicate:
/* empty */     { $$= NULL; }
| IGNORE        { malloc_terminal_node($$, result->malloc_pool_, T_IGNORE); }
| REPLACE       { malloc_terminal_node($$, result->malloc_pool_, T_REPLACE); }
;

opt_load_charset:
/* empty */     { $$= NULL; }
| CHARACTER SET charset_name_or_default
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_CHARSET, 1, $3);
}
;

opt_load_ignore_rows:
/* empty */     { $$= NULL; }
| IGNORE INTNUM lines_or_rows
{
  (void) $3;
  malloc_non_terminal_node($$, result->malloc_pool_, T_IGNORE_ROWS, 1, $2);
}
| GENERATED INTNUM lines_or_rows
{
  (void) $3;
  malloc_non_terminal_node($$, result->malloc_pool_, T_GEN_ROWS, 1, $2);
}
;

lines_or_rows:
LINES           { $$= NULL; }
| ROWS          { $$= NULL; }
;

opt_field_or_var_spec:
/* empty */     { $$= NULL; }
| '(' ')'       { $$= NULL; }
| '(' field_or_vars_list ')'
{
  merge_nodes($$, result, T_COLUMN_LIST, $2);
}
;

field_or_vars_list:
field_or_vars
{
  $$ = $1;
}
| field_or_vars_list ',' field_or_vars
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

field_or_vars:
column_definition_ref
{
  $$ = $1;
}
| USER_VARIABLE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USER_VARIABLE_IDENTIFIER, 1, $1);
  dup_node_string($1, $$, result->malloc_pool_);
}
;

opt_load_set_spec:
/* empty */     { $$= NULL; }
| SET load_set_list
{
  merge_nodes($$, result, T_VALUE_LIST, $2);
}
;

load_set_list:
load_set_element
{
  $$ = $1;
}
| load_set_list ',' load_set_element
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

load_set_element:
column_definition_ref COMP_EQ expr_or_default
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ASSIGN_ITEM, 2, $1, $3);
}
;

/*****************************************************************************
 *
 *	use grammar
 *
 *****************************************************************************/
use_database_stmt:
USE database_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_DATABASE, 1, $2);
}
;

/*****************************************************************************
 *
 *	create synonym grammar
 *
 *****************************************************************************/

create_synonym_stmt:
CREATE opt_replace opt_public SYNONYM  synonym_name FOR synonym_object opt_dlink
{
  malloc_non_terminal_node($$,
                           result->malloc_pool_,
                           T_CREATE_SYNONYM,
                           7,
                           $2,                   /*or replace*/
                           $3,                   /* public */
                           NULL,                 /* opt schema name */
                           $5,                   /* synonym name */
                           NULL,                   /* opt schema name */
                           $7,                   /* synonym object */
                           $8);                  /* partition optition */
}
;

| CREATE opt_replace opt_public SYNONYM database_factor '.' synonym_name FOR synonym_object opt_dlink
{
  malloc_non_terminal_node($$,
                           result->malloc_pool_,
                           T_CREATE_SYNONYM,
                           7,
                           $2,                   /*or replace*/
                           $3,                   /* public */
                           $5,                   /* opt schema name */
                           $7,                   /* synonym name */
                           NULL,                   /* opt schema name */
                           $9,                   /* synonym object */
                           $10);                  /* partition optition */
}
;

| CREATE opt_replace opt_public SYNONYM  synonym_name FOR database_factor '.' synonym_object opt_dlink
{
  malloc_non_terminal_node($$,
                           result->malloc_pool_,
                           T_CREATE_SYNONYM,
                           7,
                           $2,                   /*or replace*/
                           $3,                   /* public */
                           NULL,                 /* opt schema name */
                           $5,                   /* synonym name */
                           $7,                   /* opt schema name */
                           $9,                   /* synonym object */
                           $10);                  /* partition optition */
}
;
| CREATE opt_replace opt_public SYNONYM  database_factor '.' synonym_name FOR database_factor '.' synonym_object opt_dlink
{
  malloc_non_terminal_node($$,
                           result->malloc_pool_,
                           T_CREATE_SYNONYM,
                           7,
                           $2,                   /*or replace*/
                           $3,                   /* public */
                           $5,                 /* opt schema name */
                           $7,                   /* synonym name */
                           $9,                   /* opt schema name */
                           $11,                   /* synonym object */
                           $12);                  /* partition optition */
}
;

opt_public:
PUBLIC
{
  malloc_terminal_node($$, result->malloc_pool_, T_PUBLIC); }
| /* EMPTY */
{ $$ = NULL; }
;


synonym_name:
NAME_OB
{ $$ = $1; }
| unreserved_keyword
{
  get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

opt_dlink:
'@' ip_port
{
  $$ = $2;}
| /* EMPTY */
{ $$ = NULL; }
;

synonym_object:
NAME_OB
{ $$ = $1; }
| unreserved_keyword
{
  get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

/*****************************************************************************
 *
 *      DROP SYNONYM grammar
 *
 *****************************************************************************/
drop_synonym_stmt:
DROP opt_public SYNONYM synonym_name opt_force
{
  malloc_non_terminal_node($$,
                           result->malloc_pool_,
                           T_DROP_SYNONYM,
                           4,
                           $2,                   /*opt public*/
                           NULL,                   /* opt schema name */
                           $4,                   /* synonym name */
                           $5);                  /* opt force */
}
;
| DROP opt_public SYNONYM database_factor '.' synonym_name opt_force
{
  malloc_non_terminal_node($$,
                           result->malloc_pool_,
                           T_DROP_SYNONYM,
                           4,
                           $2,                   /*opt public*/
                           $4,                   /* opt schema name */
                           $6,                   /* synonym name */
                           $7);                  /* opt force */
}
;

opt_force:
FORCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_FORCE); }
| /* EMPTY */
{ $$ = NULL; }
;

opt_force_purge:
FORCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_FORCE); }
| PURGE
{
  malloc_terminal_node($$, result->malloc_pool_, T_PURGE); }
| /* EMPTY */
{ $$ = NULL; }
;

temporary_option:
TEMPORARY
{
  malloc_terminal_node($$, result->malloc_pool_, T_TEMPORARY); }
| /* EMPTY */
{ $$ = NULL; }
;

/*****************************************************************************
 *
 *	CREATE TABLE LIKE grammar
 *
 *****************************************************************************/

create_table_like_stmt:
CREATE temporary_option TABLE opt_if_not_exists relation_factor LIKE relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE_LIKE, 4, $2, $4, $5, $7);
}
| CREATE temporary_option TABLE opt_if_not_exists relation_factor '(' LIKE relation_factor ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE_LIKE, 4, $2, $4, $5, $8);
}
;

/*****************************************************************************
 *
 *	create table grammar
 *
 *****************************************************************************/

create_table_stmt:
CREATE temporary_option TABLE opt_if_not_exists relation_factor '(' table_element_list ')'
opt_table_option_list opt_partition_option
{
  ParseNode *table_elements = NULL;
  ParseNode *table_options = NULL;
  merge_nodes(table_elements, result, T_TABLE_ELEMENT_LIST, $7);
  merge_nodes(table_options, result, T_TABLE_OPTION_LIST, $9);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 7,
                           $2,                   /* temporary option */
                           $4,                   /* if not exists */
                           $5,                   /* table name */
                           table_elements,       /* columns or primary key */
                           table_options,        /* table option(s) */
                           $10,                 /* partition optition */
                           NULL);               /* The on commit option for storing temporary tables
                                                   in oracle compatibility mode */
}
| CREATE temporary_option TABLE opt_if_not_exists relation_factor '(' table_element_list ')'
 opt_table_option_list opt_partition_option opt_as select_stmt
{
  (void)$11;
  ParseNode *table_elements = NULL;
  ParseNode *table_options = NULL;
  merge_nodes(table_elements, result, T_TABLE_ELEMENT_LIST, $7);
  merge_nodes(table_options, result, T_TABLE_OPTION_LIST, $9);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 8,
                           $2,                   /* temporary option */
                           $4,                   /* if not exists */
                           $5,                   /* table name */
                           table_elements,       /* columns or primary key */
                           table_options,        /* table option(s) */
                           $10,                  /* partition optition */
                           NULL,                 /* The on commit option for storing temporary tables
                                                    in oracle compatibility mode */
                           $12);                 /* select_stmt */
}
| CREATE temporary_option TABLE opt_if_not_exists relation_factor table_option_list opt_partition_option opt_as select_stmt
{
  (void)$8;
  ParseNode *table_options = NULL;
  merge_nodes(table_options, result, T_TABLE_OPTION_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 8,
                           $2,                   /* temporary option */
                           $4,                   /* if not exists */
                           $5,                   /* table name */
                           NULL,                 /* columns or primary key */
                           table_options,        /* table option(s) */
                           $7,                   /* partition optition */
                           NULL,                 /* The on commit option for storing temporary tables
                                                   in oracle compatibility mode */
                           $9);                  /* select_stmt */
}
| CREATE temporary_option TABLE opt_if_not_exists relation_factor partition_option opt_as select_stmt
{
  (void)$7;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 8,
                           $2,                   /* temporary option */
                           $4,                   /* if not exists */
                           $5,                   /* table name */
                           NULL,                 /* columns or primary key */
                           NULL,                 /* table option(s) */
                           $6,                   /* partition optition */
                           NULL,                 /* The on commit option for storing temporary tables
                                                   in oracle compatibility mode */
                           $8);                  /* select_stmt */
}
| CREATE temporary_option TABLE opt_if_not_exists relation_factor select_stmt
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 8,
                           $2,                   /* temporary option */
                           $4,                   /* if not exists */
                           $5,                   /* table name */
                           NULL,                 /* columns or primary key */
                           NULL,                 /* table option(s) */
                           NULL,                 /* partition optition */
                           NULL,                 /* The on commit option for storing temporary tables
                                                   in oracle compatibility mode */
                           $6);                  /* select_stmt */
}
| CREATE temporary_option TABLE opt_if_not_exists relation_factor AS select_stmt
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 8,
                           $2,                   /* temporary option */
                           $4,                   /* if not exists */
                           $5,                   /* table name */
                           NULL,                 /* columns or primary key */
                           NULL,                 /* table option(s) */
                           NULL,                 /* partition optition */
                           NULL,                 /* The on commit option for storing temporary tables
                                                   in oracle compatibility mode */
                           $7);                  /* select_stmt */
}
;

opt_agg:
AGGREGATE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
| /* EMPTY */
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
;

ret_type:
STRING
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
|
INTEGER
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
|
REAL
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 3;
}
|
DECIMAL
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 4;
}
;

create_function_stmt:
CREATE opt_agg FUNCTION NAME_OB RETURNS ret_type SONAME STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_FUNC, 4, $2, $4, $6, $8);
}
;

drop_function_stmt:
DROP FUNCTION opt_if_exists NAME_OB
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_FUNC, 2, $3, $4);
}
;

opt_if_not_exists:
IF not EXISTS
{
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_IF_NOT_EXISTS); }
| /* EMPTY */
{ $$ = NULL; }
;

table_element_list:
table_element
{
  $$ = $1;
}
| table_element_list ',' table_element
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

table_element:
column_definition
{
  $$ = $1;
}
| opt_constraint PRIMARY KEY opt_index_using_algorithm '(' column_name_list ')' opt_index_using_algorithm opt_comment
{
  (void)($1);
  ParseNode *col_list= NULL;
  merge_nodes(col_list, result, T_COLUMN_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_KEY, 3, col_list, NULL != $8 ? $8 : $4, $9);
}
| key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list
{
  (void)($1);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $5);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX, 4, $2, col_list, index_option, $3);
  $$->value_ = 0;
}
| UNIQUE opt_key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list
{
  (void)($2);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $6);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX, 4, $3, col_list, index_option, $4);
  $$->value_ = 1;
}
| CONSTRAINT opt_constraint_name UNIQUE opt_key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list
{
  (void)($4);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $8);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX, 4, $5 ? $5 : $2, col_list, index_option, $6);
  $$->value_ = 1;
}
| CONSTRAINT constraint_name CHECK '(' expr ')'
{
  dup_expr_string($5, result, @5.first_column, @5.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 2, $2, $5);
  $$->value_ = 1;
}
| opt_constraint FOREIGN KEY opt_index_name '(' column_name_list ')' REFERENCES relation_factor '(' column_name_list ')' opt_match_option opt_reference_option_list
{
  ParseNode *child_col_list= NULL;
  ParseNode *parent_col_list= NULL;
  ParseNode *reference_option_list = NULL;
  merge_nodes(child_col_list, result, T_COLUMN_LIST, $6);
  merge_nodes(parent_col_list, result, T_COLUMN_LIST, $11);
  merge_nodes(reference_option_list, result, T_REFERENCE_OPTION_LIST, $14);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOREIGN_KEY, 7, child_col_list, $9, parent_col_list, reference_option_list, $1, $4, $13);
}
;

opt_reference_option_list:
opt_reference_option_list reference_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| /*empty*/
{
  $$ = NULL;
}
;

reference_option:
ON UPDATE reference_action
{
  malloc_terminal_node($$, result->malloc_pool_, T_REFERENCE_OPTION);
  $$->int32_values_[0] = T_UPDATE;
  $$->int32_values_[1] = $3[0];
}
| ON DELETE reference_action
{
  malloc_terminal_node($$, result->malloc_pool_, T_REFERENCE_OPTION);
  $$->int32_values_[0] = T_DELETE;
  $$->int32_values_[1] = $3[0];
}
;

reference_action:
RESTRICT
{
  $$[0] = T_RESTRICT;
}
| CASCADE
{
  $$[0] = T_CASCADE;
}
| SET NULLX
{
  (void)($2);
  $$[0] = T_SET_NULL;
}
| NO ACTION
{
  $$[0] = T_NO_ACTION;
}
| SET DEFAULT
{
  $$[0] = T_SET_DEFAULT;
}
;

opt_match_option:
MATCH match_action
{
  malloc_terminal_node($$, result->malloc_pool_, T_FOREIGN_KEY_MATCH);
  $$->int32_values_[0] = $2[0];
}
| /*empty*/
{
  $$ = NULL;
}
;

match_action:
SIMPLE
{
  $$[0] = T_SIMPLE;
}
| FULL
{
  $$[0] = T_FULL;
}
| PARTIAL
{
  $$[0] = T_PARTIAL;
}
;

column_definition:
//column_name data_type opt_column_attribute_list
column_definition_ref data_type opt_column_attribute_list opt_position_column
{
  ParseNode *attributes = NULL;
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 4, $1, $2, attributes, $4);
}
| column_definition_ref data_type opt_generated_keyname AS '(' expr ')' opt_storage_type opt_generated_column_attribute_list opt_position_column
{
  (void)($3);
  ParseNode *attributes = NULL;
  dup_expr_string($6, result, @6.first_column, @6.last_column);
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $9);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 6, $1, $2, attributes, $6, $8, $10);
}
;

opt_generated_keyname:
GENERATED ALWAYS
{
  $$ = NULL;
}
| /*empty*/
{
  $$ = NULL;
}
;

opt_generated_column_attribute_list:
opt_generated_column_attribute_list generated_column_attribute
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| /*empty*/
{
  $$ = NULL;
}

generated_column_attribute:
NOT NULLX
{
  (void)($2) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NOT_NULL);
}
| NULLX
{
  (void)($1) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NULL);
}
| UNIQUE KEY
{
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_UNIQUE_KEY);
}
| opt_primary KEY
{
  (void)($1);
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_PRIMARY_KEY);
}
| UNIQUE %prec LOWER_KEY
{
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_UNIQUE_KEY);
}
| COMMENT STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COMMENT, 1, $2);
}
| ID INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ID, 1, $2);
}
;

opt_storage_type:
VIRTUAL
{
  malloc_terminal_node($$, result->malloc_pool_, T_VIRTUAL_COLUMN);
}
| STORED
{
  malloc_terminal_node($$, result->malloc_pool_, T_STORED_COLUMN);
}
| /*empty*/
{
  $$ = NULL;
}
;

column_definition_ref:
column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, NULL, $1);
  dup_node_string($1, $$, result->malloc_pool_);
}
| relation_name '.' column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, $1, $3);
  dup_node_string($3, $$, result->malloc_pool_);
}
| relation_name '.' relation_name '.' column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, $1, $3, $5);
  dup_node_string($5, $$, result->malloc_pool_);
}
;

column_definition_list:
/*column_definition,column_definition...*/
column_definition
{
  $$ = $1;
}
| column_definition_list ',' column_definition
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

cast_data_type:
BINARY opt_string_length_i_v2
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_CHAR; /* data type */
  $$->int16_values_[OB_NODE_CAST_COLL_IDX] = BINARY_COLLATION; /* is binary */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = $2[0];        /* length */
  $$->param_num_ = $2[1];
}
| CHARACTER opt_string_length_i_v2 opt_binary
{
  (void)($3);
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_CHAR;//to keep consitent with mysql
  $$->int16_values_[OB_NODE_CAST_COLL_IDX] = INVALID_COLLATION;        /* is char */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = $2[0];        /* length */
  $$->param_num_ = $2[1];
}
| cast_datetime_type_i opt_datetime_fsp_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = $1[0];
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2[0];
  $$->param_num_ = $1[1] + $2[1];
}
| NUMBER opt_number_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NUMBER;
  if (NULL != $2) {
    $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = $2->int16_values_[0];    /* precision */
    $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2->int16_values_[1];    /* scale */
    $$->param_num_ = $2->param_num_;
  }
}
| DECIMAL opt_number_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NUMBER;
  if (NULL != $2) {
    $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = $2->int16_values_[0];    /* precision */
    $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2->int16_values_[1];    /* scale */
    $$->param_num_ = $2->param_num_;
  }
}
| SIGNED opt_integer
{
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_INT;
  $$->param_num_ = $2[1];
}
| UNSIGNED opt_integer
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_UINT64;
  $$->param_num_ = $2[1];
}
| DOUBLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_DOUBLE;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = -1;    /* precision */
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = -1;    /* scale */
  $$->param_num_ = 0;
}
;

opt_integer:
INTEGER  { $$[0] = 0; $$[1] = 0;}
| /*empty*/ { $$[0] = 0; $$[1] = 0;}
;

cast_datetime_type_i:
DATETIME    { $$[0] = T_DATETIME; $$[1] = 0; }
| DATE        { $$[0] = T_DATE; $$[1] = 0; }
| TIME        { $$[0] = T_TIME; $$[1] = 0; }
;

data_type:
int_type_i opt_int_length_i opt_unsigned_i opt_zerofill_i
{
  malloc_terminal_node($$, result->malloc_pool_, ($3[0] || $4[0]) ? $1[0] + (T_UTINYINT - T_TINYINT) : $1[0]);
  $$->int16_values_[0] = $2[0];
  $$->int16_values_[2] = $4[0];   /* 2 is the same index as float or number. */
}
| float_type_i opt_float_precision opt_unsigned_i opt_zerofill_i
{
  malloc_terminal_node($$, result->malloc_pool_, ($3[0] || $4[0]) ? $1[0] + (T_UFLOAT - T_FLOAT) : $1[0]);
  if (NULL != $2) {
    $$->int16_values_[0] = $2->int16_values_[0];
    $$->int16_values_[1] = $2->int16_values_[1];
  }
  /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
  $$->int16_values_[2] = $4[0];
}
| NUMBER opt_number_precision opt_unsigned_i opt_zerofill_i
{
  malloc_terminal_node($$, result->malloc_pool_, ($3[0] || $4[0]) ? T_UNUMBER : T_NUMBER);
  if (NULL != $2) {
    $$->int16_values_[0] = $2->int16_values_[0];
    $$->int16_values_[1] = $2->int16_values_[1];
  }
  /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
  $$->int16_values_[2] = $4[0];
}
| DECIMAL opt_number_precision opt_unsigned_i opt_zerofill_i
{
  malloc_terminal_node($$, result->malloc_pool_, ($3[0] || $4[0]) ? T_UNUMBER : T_NUMBER);
  if (NULL != $2) {
    $$->int16_values_[0] = $2->int16_values_[0];
    $$->int16_values_[1] = $2->int16_values_[1];
  }
  /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
  $$->int16_values_[2] = $4[0];
}
| BOOL
{
  malloc_terminal_node($$, result->malloc_pool_, T_TINYINT);
  $$->int16_values_[0] = 1;
  $$->int16_values_[2] = 0;  // zerofill always false
}
| BOOLEAN
{
  malloc_terminal_node($$, result->malloc_pool_, T_TINYINT);
  $$->int16_values_[0] = 1;
  $$->int16_values_[2] = 0; // zerofill always false
}
| datetime_type_i opt_datetime_fsp_i
{
  malloc_terminal_node($$, result->malloc_pool_, $1[0]);
  $$->int16_values_[1] = $2[0];
}
| date_year_type_i
{
  malloc_terminal_node($$, result->malloc_pool_, $1[0]);
}
| CHARACTER opt_string_length_i opt_binary opt_charset opt_collation
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHAR, 3, $4, $5, $3);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 0; /* is char */
}
/*  | TEXT opt_binary opt_charset opt_collation
//  {
//    (void)($2);
//    malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 3, $3, $4, $2);
//    $$->int32_values_[0] = 256;
//    $$->int32_values_[1] = 0; /* is char */
/*  }*/
| VARCHAR string_length_i opt_binary opt_charset opt_collation
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 3, $4, $5, $3);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 0; /* is char */
}
| blob_type_i opt_string_length_i_v2
{
  malloc_terminal_node($$, result->malloc_pool_, $1[0]);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 1; /* is binary */
}
| text_type_i opt_string_length_i_v2 opt_binary opt_charset opt_collation
{
  malloc_non_terminal_node($$, result->malloc_pool_, $1[0], 3, $4, $5, $3);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 0; /* is text */
}
| BINARY opt_string_length_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_CHAR);
  if ($2[0] < 0) {
    $2[0] = 1;
  }
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 1; /* is binary */
}
| VARBINARY string_length_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 1; /* is binary */
}
| STRING_VALUE /* wrong or unsupported data type */
{
  malloc_terminal_node($$, result->malloc_pool_, T_INVALID);
  $$->str_value_ = $1->str_value_;
  $$->str_len_ = $1->str_len_;
}
| BIT opt_bit_length_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_BIT);
  $$->int16_values_[0] = $2[0];
}
| ENUM '(' string_list ')' opt_binary opt_charset opt_collation
{
  ParseNode *string_list_node = NULL;
  merge_nodes(string_list_node, result, T_STRING_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ENUM, 4, $6, $7, $5, string_list_node);
  $$->int32_values_[0] = 0;//not used so far
  $$->int32_values_[1] = 0; /* is char */
}
| SET '(' string_list ')' opt_binary opt_charset opt_collation
{
  ParseNode *string_list_node = NULL;
  merge_nodes(string_list_node, result, T_STRING_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET, 4, $6, $7, $5, string_list_node);
  $$->int32_values_[0] = 0;//not used so far
  $$->int32_values_[1] = 0; /* is char */
}
;

string_list:
text_string
{
 $$ = $1;
}
| string_list ',' text_string
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
};

text_string:
STRING_VALUE
{
  $$ = $1;
}
| HEX_STRING_VALUE
{
  $$ = $1;
};


int_type_i:
TINYINT     { $$[0] = T_TINYINT; }
| SMALLINT    { $$[0] = T_SMALLINT; }
| MEDIUMINT   { $$[0] = T_MEDIUMINT; }
| INTEGER     { $$[0] = T_INT32; }
| BIGINT      { $$[0] = T_INT; }
;

float_type_i:
FLOAT              { $$[0] = T_FLOAT; }
| DOUBLE             { $$[0] = T_DOUBLE; }
| REAL             { $$[0] = T_DOUBLE; }
| DOUBLE PRECISION   { $$[0] = T_DOUBLE; }
| REAL PRECISION   { $$[0] = T_DOUBLE; }
;

datetime_type_i:
DATETIME      { $$[0] = T_DATETIME; }
| TIMESTAMP   { $$[0] = T_TIMESTAMP; }
| TIME        { $$[0] = T_TIME; }
;

date_year_type_i:
DATE        { $$[0] = T_DATE; }
| YEAR opt_year_i { $$[0] = T_YEAR; }
;

text_type_i:
TINYTEXT     { $$[0] = T_TINYTEXT; }
| TEXT   { $$[0] = T_TEXT; }
| MEDIUMTEXT   { $$[0] = T_MEDIUMTEXT; }
| LONGTEXT   { $$[0] = T_LONGTEXT;  }
;

blob_type_i:
TINYBLOB     { $$[0] = T_TINYTEXT; }
| BLOB   { $$[0] = T_TEXT; }
| MEDIUMBLOB   { $$[0] = T_MEDIUMTEXT; }
| LONGBLOB   { $$[0] = T_LONGTEXT;  }
;

opt_int_length_i:
'(' INTNUM ')'  { $$[0] = $2->value_; }
| /*EMPTY*/       { $$[0] = -1; }
;

opt_bit_length_i:
'(' INTNUM ')'  { $$[0] = $2->value_; }
| /*EMPTY*/       { $$[0] = 1; }
;

opt_float_precision:
'(' INTNUM ',' INTNUM ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  $$->int16_values_[0] = $2->value_;
  $$->int16_values_[1] = $4->value_;
}
| '(' INTNUM ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  $$->int16_values_[0] = $2->value_;
  $$->int16_values_[1] = -1;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

opt_number_precision:
'(' INTNUM ',' INTNUM ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  if($2->value_ > OB_MAX_PARSER_INT16_VALUE) {
    $$->int16_values_[0] = OB_MAX_PARSER_INT16_VALUE;
  } else {
    $$->int16_values_[0] = $2->value_;
  }
  if($4->value_ > OB_MAX_PARSER_INT16_VALUE) {
    $$->int16_values_[1] = OB_MAX_PARSER_INT16_VALUE;
  } else {
    $$->int16_values_[1] = $4->value_;
  }
  $$->param_num_ = 2;
}
| '(' INTNUM ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  if($2->value_ > OB_MAX_PARSER_INT16_VALUE) {
    $$->int16_values_[0] = OB_MAX_PARSER_INT16_VALUE;
  } else {
    $$->int16_values_[0] = $2->value_;
  }
  $$->int16_values_[1] = 0;
  $$->param_num_ = 1;
}
| /*EMPTY*/
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  $$->int16_values_[0] = 10;
  $$->int16_values_[1] = 0;
  $$->param_num_ = 0;
}
;

opt_year_i:
'(' INTNUM  ')' { $$[0] = $2->value_; }
| /*EMPTY*/ { $$[0] = 0; }
;

opt_datetime_fsp_i:
'(' INTNUM ')'  { $$[0] = $2->value_; $$[1] = 1; }
| /*EMPTY*/       { $$[0] = 0; $$[1] = 0;}
;

string_length_i:
'(' number_literal ')'
{
  // Report a syntax error at `*`
  // select cast('' as BINARY(-1));
  //                          *
  // select cast('' as CHARACTER(-1));
  //                             *
  int64_t val = 0;
  if (T_NUMBER == $2->type_) {
    errno = 0;
    val = strtoll($2->str_value_, NULL, 10);
    if (ERANGE == errno) {
      $$[0] = OUT_OF_STR_LEN;// out of str_max_len
    } else if (val < 0) {
      yyerror(&@2, result, "length cannot < 0\n");
      YYABORT_UNEXPECTED;
    } else if (val > UINT32_MAX) {
      $$[0] = OUT_OF_STR_LEN;// out of str_max_len
    } else if (val > INT32_MAX) {
      $$[0] = DEFAULT_STR_LENGTH;
    } else {
      $$[0] = val;
    }
  } else if ($2->value_ < 0) {
    yyerror(&@2, result, "length cannot < 0\n");
    YYABORT_UNEXPECTED;
  } else if ($2->value_ > UINT32_MAX) {
    $$[0] = OUT_OF_STR_LEN;;
  } else if ($2->value_ > INT32_MAX) {
    $$[0] = DEFAULT_STR_LENGTH;
  } else {
    $$[0] = $2->value_;
  }
  $$[1] = $2->param_num_;
}
;

opt_string_length_i:
string_length_i { $$[0] = $1[0]; $$[1] = $1[1];}
| /*EMPTY*/       { $$[0] = 1; $$[1] = 0;}
;

opt_string_length_i_v2:
string_length_i { $$[0] = $1[0]; $$[1] = $1[1]; }
| /*EMPTY*/       { $$[0] = DEFAULT_STR_LENGTH; $$[1] = 0; } // undefined str_len , -1.


opt_unsigned_i:
UNSIGNED    { $$[0] = 1; }
| SIGNED      { $$[0] = 0; }
| /*EMPTY*/   { $$[0] = 0; }
;

opt_zerofill_i:
ZEROFILL    { $$[0] = 1; }
| /*EMPTY*/   { $$[0] = 0; }
;

opt_binary:
BINARY
{
  malloc_terminal_node($$, result->malloc_pool_, T_BINARY);
  $$->value_ = 1;
}
| /*EMPTY*/ {$$ = 0; }
;

collation_name:
NAME_OB
{
  $$ = $1;
  $$->type_ = T_VARCHAR;
  $$->param_num_ = 0;
}
| STRING_VALUE
{
  $$ = $1;
  $$->param_num_ = 1;
}
;

trans_param_name:
'\'' STRING_VALUE '\''
{
  $$ = $2;
  $$->type_ = T_VARCHAR;
  $$->param_num_ = 1;
  $$->is_hidden_const_ = 0;
}
;

trans_param_value:
'\'' STRING_VALUE '\''
{
  $$ = $2;
  $$->type_ = T_VARCHAR;
  $$->param_num_ = 1;
}
| INTNUM
{
  $$ = $1;
  $$->type_ = T_INT;
  $$->param_num_ = 1;
}

charset_name:
NAME_OB
{
  $$ = $1;
  $$->type_ = T_VARCHAR;
  $$->param_num_ = 0;
  $$->is_hidden_const_ = 1;
}
| STRING_VALUE
{
  $$ = $1;
  $$->param_num_ = 1;
  $$->is_hidden_const_ = 0;
}
| BINARY
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = parse_strdup("binary", result->malloc_pool_, &($$->str_len_));
  if (OB_UNLIKELY(NULL == $$->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
  $$->param_num_ = 0;
  $$->is_hidden_const_ = 1;
}
;

charset_name_or_default:
charset_name
{
  $$ = $1;
}
| DEFAULT
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT);
}
;

opt_charset:
charset_key charset_name
{
  (void)($1);
  malloc_terminal_node($$, result->malloc_pool_, T_CHARSET);
  $$->str_value_ = $2->str_value_;
  $$->str_len_ = $2->str_len_;
}
| /*EMPTY*/
{ $$ = NULL; }
;

collation:
COLLATE collation_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_COLLATION);
  $$->str_value_ = $2->str_value_;
  $$->str_len_ = $2->str_len_;
  $$->param_num_ = $2->param_num_;
};

opt_collation:
collation
{
  $$ = $1;
}
| /*EMPTY*/
{ $$ = NULL; }
;

opt_column_attribute_list:
opt_column_attribute_list column_attribute
{ malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2); }
| /*EMPTY*/
{ $$ = NULL; }
;

column_attribute:
not NULLX
{
  (void)($1) ;
  (void)($2) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NOT_NULL);
}
| NULLX
{
  (void)($1) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NULL);
}
| DEFAULT now_or_signed_literal
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_DEFAULT, 1, $2);
}
| ORIG_DEFAULT now_or_signed_literal
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_ORIG_DEFAULT, 1, $2);
}
| AUTO_INCREMENT
{
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_AUTO_INCREMENT);
}
| UNIQUE KEY
{
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_UNIQUE_KEY);
}
| opt_primary KEY

{
  (void)($1);
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_PRIMARY_KEY);
}
| UNIQUE %prec LOWER_KEY
{
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_UNIQUE_KEY);
}
| COMMENT STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COMMENT, 1, $2);
}
| ON UPDATE cur_timestamp_func
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ON_UPDATE, 1, $3);
}
| ID INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ID, 1, $2);
}
;

now_or_signed_literal:
cur_timestamp_func
{
  $$ = $1;
}
| signed_literal
{
  $$ = $1;
}
;

signed_literal:
literal
{ $$ = $1;}
| '+' number_literal
{ $$ = $2; }
| '-' number_literal
{
  if (T_UINT64 == $2->type_) {
    uint64_t value = $2->value_;
    if (INT64_MAX == value - 1) {
      $2->value_ = INT64_MIN;
      $2->type_ = T_INT;
    } else {
      $2->value_ = -$2->value_;
      $2->type_ = T_NUMBER;
    }
  } else {
    $2->value_ = -$2->value_;
  }
  int32_t len = $2->str_len_ + 2;
  char *str_value = (char*)parse_malloc(len, result->malloc_pool_);
  if (OB_LIKELY(NULL != str_value)) {
    snprintf(str_value, len, "-%.*s", (int32_t)($2->str_len_), $2->str_value_);
    $$ = $2;
    $$->str_value_ = str_value;
    $$->str_len_ = $2->str_len_ + 1;
  } else {
    yyerror(NULL, result, "No more space for copying expression string\n");
    YYABORT_NO_MEMORY;
  }
}
;

opt_primary:
/*EMPTY*/
{
  $$ = NULL;
}
| PRIMARY
{
  $$ = NULL;
}
;

opt_comma:
','
{
  $$ = NULL;
}
|/*empty*/
{
  $$ = NULL;
}
;

opt_table_option_list:
table_option_list
{
  $$ = $1;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

table_option_list_space_seperated:
table_option
{
  $$ = $1;
}
| table_option  table_option_list_space_seperated
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

table_option_list:
table_option_list_space_seperated
{
  $$ = $1;
}
| table_option ',' table_option_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

primary_zone_name:
DEFAULT
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT);
}
| RANDOM
{
  malloc_terminal_node($$, result->malloc_pool_, T_RANDOM);
}
| USER_VARIABLE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GET_USER_VAR, 1, $1);
}
| relation_name_or_string
{
  $$ = $1;
}
;

locality_name:
STRING_VALUE
{
  $$ = $1;
}
| DEFAULT
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT);
}
;

table_option:
TABLE_MODE opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_MODE, 1, $3);
}
| DUPLICATE_SCOPE opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DUPLICATE_SCOPE, 1, $3);
}
| LOCALITY opt_equal_mark locality_name opt_force
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOCALITY, 2, $3, $4);
}
| EXPIRE_INFO opt_equal_mark '(' expr ')'
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPIRE_INFO, 1, $4);
  dup_expr_string($$, result, @4.first_column, @4.last_column);
}
| PROGRESSIVE_MERGE_NUM opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_PROGRESSIVE_MERGE_NUM, 1, $3);
}
| BLOCK_SIZE opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_BLOCK_SIZE, 1, $3);
}
| TABLE_ID opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_ID, 1, $3);
}
| REPLICA_NUM opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_REPLICA_NUM, 1, $3);
}
| COMPRESSION opt_equal_mark STRING_VALUE
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_COMPRESSION, 1, $3);
}
| ROW_FORMAT opt_equal_mark row_format_option
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_STORE_FORMAT, 1, $3);
}
| STORAGE_FORMAT_VERSION opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_STORAGE_FORMAT_VERSION, 1, $3);
}
| USE_BLOOM_FILTER opt_equal_mark BOOL_VALUE
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_BLOOM_FILTER, 1, $3);
}
| opt_default_mark charset_key opt_equal_mark charset_name
{
  (void)($1) ; /* make bison mute */
  (void)($2) ; /* make bison mute */
  (void)($3) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHARSET, 1, $4);
}
| opt_default_mark COLLATE opt_equal_mark collation_name
{
  (void)($1);
  (void)($3);
  malloc_terminal_node($$, result->malloc_pool_, T_COLLATION);
  $$->str_value_ = $4->str_value_;
  $$->str_len_ = $4->str_len_;
  $$->param_num_ = $4->param_num_;
}
| COMMENT opt_equal_mark STRING_VALUE
{
  (void)($2); /*  make bison mute*/
  malloc_non_terminal_node($$, result->malloc_pool_, T_COMMENT, 1, $3);
}
| PRIMARY_ZONE opt_equal_mark primary_zone_name
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_ZONE, 1, $3);
}
| TABLEGROUP opt_equal_mark relation_name_or_string
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLEGROUP, 1, $3);
}
| AUTO_INCREMENT opt_equal_mark int_or_decimal
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_AUTO_INCREMENT, 1, $3);
}
| read_only_or_write
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_READ_ONLY, 1, $1);
}
| ENGINE_ opt_equal_mark relation_name_or_string
{
  (void)($2) ;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ENGINE, 1, $3);
}
| TABLET_SIZE opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLET_SIZE, 1, $3);
}
| PCTFREE opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_PCTFREE, 1, $3);
}
| MAX_USED_PART_ID opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_USED_PART_ID, 1, $3);
}
| parallel_option
{
  $$ = $1;
}
;

parallel_option:
PARALLEL opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  if (OB_UNLIKELY($3->value_ < 1)) {
    yyerror(&@1, result, "value for PARALLEL or DEGREE must be greater than 0!\n");
    YYERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARALLEL, 1, $3);
}
| NOPARALLEL
{
  ParseNode *int_node = NULL;
  malloc_terminal_node(int_node, result->malloc_pool_, T_INT);
  int_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARALLEL, 1, int_node);
}
;

relation_name_or_string:
relation_name
{ $$ = $1; $$->type_ = T_VARCHAR;}
| STRING_VALUE {$$ = $1;}
| ALL
{
  make_name_node($$, result->malloc_pool_, "all");
}
;


opt_equal_mark:
COMP_EQ     { $$ = NULL; }
| /*EMPTY*/   { $$ = NULL; }
;

opt_default_mark:
DEFAULT     { $$ = NULL; }
| /*EMPTY*/   { $$ = NULL; }
;

partition_option:
hash_partition_option
{
  $$ = $1;
}
| key_partition_option
{
  $$ = $1;
}
| range_partition_option
{
  $$ = $1;
}
| list_partition_option
{
  $$ = $1;
}
;

opt_partition_option:
partition_option opt_column_partition_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_OPTION, 2, $1, $2);
}
| opt_column_partition_option
{
  $$ = $1;
}
| auto_partition_option
{
  $$ = $1;
}
;
auto_partition_option:
auto_partition_type PARTITION SIZE partition_size PARTITIONS AUTO
{
 malloc_non_terminal_node($$, result->malloc_pool_, T_AUTO_PARTITION, 2, $1, $4);
}
;

partition_size:
conf_const
{
  $$ = $1;
}
| AUTO
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUTO);
}
;

auto_partition_type:
auto_range_type
{
  $$ = $1;
}
;

auto_range_type:
PARTITION BY RANGE '('')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_PARTITION, 1, params);
}
| PARTITION BY RANGE '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_PARTITION, 1, params);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| PARTITION BY RANGE COLUMNS'(' column_name_list ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_COLUMNS_PARTITION, 1, params);
  dup_expr_string($$, result, @6.first_column, @6.last_column);
};

hash_partition_option:
PARTITION BY HASH '(' expr ')' subpartition_option opt_partitions %prec LOWER_PARENS
{
  ParseNode *params = NULL;
  ParseNode *hash_func = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_hash");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, params);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 6, $$, $8, NULL, $7, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| PARTITION BY HASH '(' expr ')' subpartition_option opt_partitions opt_hash_partition_list
{
  ParseNode *params = NULL;
  ParseNode *hash_func = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_hash");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, params);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 6, $$, $8, $9, $7, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
;

list_partition_option:
PARTITION BY BISON_LIST '(' expr ')' subpartition_option opt_partitions opt_list_partition_list
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_PARTITION, 5, params, $9, $7, $8, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| PARTITION BY BISON_LIST COLUMNS '(' column_name_list ')' subpartition_option opt_partitions opt_list_partition_list
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_COLUMNS_PARTITION, 5, params, $10, $8, $9, NULL);
  dup_expr_string($$, result, @6.first_column, @6.last_column);
}
;

key_partition_option:
PARTITION BY KEY '(' column_name_list ')' subpartition_option opt_partitions %prec LOWER_PARENS
{
  ParseNode *column_name_list = NULL;
  ParseNode *hash_func = NULL;
  merge_nodes(column_name_list, result, T_EXPR_LIST, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_key_v2");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, column_name_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_KEY_PARTITION, 6, $$, $8, NULL, $7, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| PARTITION BY KEY '(' column_name_list ')' subpartition_option opt_partitions opt_hash_partition_list
{
  ParseNode *column_name_list = NULL;
  ParseNode *hash_func = NULL;
  merge_nodes(column_name_list, result, T_EXPR_LIST, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_key_v2");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, column_name_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_KEY_PARTITION, 6, $$, $8, $9, $7, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| PARTITION BY KEY '(' ')' subpartition_option opt_partitions %prec LOWER_PARENS
{
  ParseNode *hash_func = NULL;
  ParseNode *column_name_list = NULL;
  make_name_node(hash_func, result->malloc_pool_, "partition_key_v2");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, column_name_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_KEY_PARTITION, 6, $$, $7, NULL, $6, NULL, NULL);
}
| PARTITION BY KEY '(' ')' subpartition_option opt_partitions opt_hash_partition_list
{
  ParseNode *hash_func = NULL;
  ParseNode *column_name_list = NULL;
  make_name_node(hash_func, result->malloc_pool_, "partition_key_v2");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, column_name_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_KEY_PARTITION, 6, $$, $7, $8, $6, NULL, NULL);
}
;

range_partition_option:
PARTITION BY RANGE '(' expr ')' subpartition_option opt_partitions opt_range_partition_list
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_PARTITION, 5, params, $9, $7, $8, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| PARTITION BY RANGE COLUMNS '(' column_name_list ')' subpartition_option opt_partitions opt_range_partition_list
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_COLUMNS_PARTITION, 5, params, $10, $8, $9, NULL);
  dup_expr_string($$, result, @6.first_column, @6.last_column);
}
;

opt_column_partition_option:
{ $$ = NULL; }
| column_partition_option
{ $$ =$1; }
;

column_partition_option:
PARTITION BY COLUMN '(' vertical_column_name ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_VERTICAL_COLUMNS_PARTITION, 2, $5, NULL /*aux_column_list*/);
}
| PARTITION BY COLUMN '(' vertical_column_name ',' aux_column_list')'
{
  ParseNode *aux_column_list= NULL;
  merge_nodes(aux_column_list, result, T_COLUMN_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_VERTICAL_COLUMNS_PARTITION, 2, $5, aux_column_list);
}
;

aux_column_list:
vertical_column_name
{
  $$ = $1;
}
| aux_column_list ',' vertical_column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

vertical_column_name:
column_name
{
  $$ = $1;
  $$->value_ = 1;
}
| '('column_name_list')'
{
  merge_nodes($$, result, T_COLUMN_LIST, $2);
  $$->value_ = 2;
}
;


column_name_list:
column_name
{
  $$ = $1;
}
| column_name_list ',' column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

subpartition_option:
subpartition_template_option
{
  $$ = $1;
}
| subpartition_individual_option
{
  $$ = $1;
}

subpartition_template_option:
SUBPARTITION BY RANGE '(' expr ')' SUBPARTITION TEMPLATE opt_range_subpartition_list
{
  ParseNode *params = NULL;
  ParseNode *template_mark = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $5);
  make_name_node(template_mark, result->malloc_pool_, "template_mark");
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_PARTITION, 5, params, $9, NULL, NULL, template_mark);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| SUBPARTITION BY RANGE COLUMNS '(' column_name_list ')' SUBPARTITION TEMPLATE opt_range_subpartition_list
{
  ParseNode *params = NULL;
  ParseNode *template_mark = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $6);
  make_name_node(template_mark, result->malloc_pool_, "template_mark");
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_COLUMNS_PARTITION, 5, params, $10, NULL, NULL, template_mark);
  dup_expr_string($$, result, @6.first_column, @6.last_column);
}
| SUBPARTITION BY HASH '(' expr ')' SUBPARTITION TEMPLATE opt_hash_subpartition_list
{
  ParseNode *params = NULL;
  ParseNode *hash_func = NULL;
  ParseNode *template_mark = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_hash");
  make_name_node(template_mark, result->malloc_pool_, "template_mark");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, params);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 6, $$, NULL, $9, NULL, template_mark, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| SUBPARTITION BY BISON_LIST '(' expr ')' SUBPARTITION TEMPLATE opt_list_subpartition_list
{
  ParseNode *params = NULL;
  ParseNode *template_mark = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $5);
  make_name_node(template_mark, result->malloc_pool_, "template_mark");
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_PARTITION, 5, params, $9, NULL, NULL, template_mark);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| SUBPARTITION BY BISON_LIST COLUMNS '(' column_name_list ')' SUBPARTITION TEMPLATE opt_list_subpartition_list
{
  ParseNode *params = NULL;
  ParseNode *template_mark = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $6);
  make_name_node(template_mark, result->malloc_pool_, "template_mark");
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_COLUMNS_PARTITION, 5, params, $10, NULL, NULL, template_mark);
  dup_expr_string($$, result, @6.first_column, @6.last_column);
}
| SUBPARTITION BY KEY '(' column_name_list ')' SUBPARTITION TEMPLATE opt_hash_subpartition_list
{
  ParseNode *column_name_list = NULL;
  ParseNode *hash_func = NULL;
  ParseNode *template_mark = NULL;
  merge_nodes(column_name_list, result, T_EXPR_LIST, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_key_v2");
  make_name_node(template_mark, result->malloc_pool_, "template_mark");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, column_name_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_KEY_PARTITION, 6, $$, NULL, $9, NULL, template_mark, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
|
{
  $$ = NULL;
}
;

subpartition_individual_option:
SUBPARTITION BY RANGE '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_PARTITION, 5, params, NULL, NULL, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| SUBPARTITION BY RANGE COLUMNS '(' column_name_list ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_COLUMNS_PARTITION, 5, params, NULL, NULL, NULL, NULL);
  dup_expr_string($$, result, @6.first_column, @6.last_column);
}
| SUBPARTITION BY HASH '(' expr ')' opt_subpartitions
{
  ParseNode *params = NULL;
  ParseNode *hash_func = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_hash");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, params);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 6, $$, $7, NULL, NULL, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| SUBPARTITION BY BISON_LIST '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_PARTITION, 5, params, NULL, NULL, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| SUBPARTITION BY BISON_LIST COLUMNS '(' column_name_list ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_COLUMNS_PARTITION, 5, params, NULL, NULL, NULL, NULL);
  dup_expr_string($$, result, @6.first_column, @6.last_column);
}
| SUBPARTITION BY KEY '(' column_name_list ')' opt_subpartitions
{
  ParseNode *column_name_list = NULL;
  ParseNode *hash_func = NULL;
  merge_nodes(column_name_list, result, T_EXPR_LIST, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_key_v2");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, column_name_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_KEY_PARTITION, 6, $$, $7, NULL, NULL, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
;

opt_hash_partition_list:
'(' hash_partition_list')'
{
  merge_nodes($$, result, T_PARTITION_LIST, $2);
}

hash_partition_list:
hash_partition_element
{
  $$ = $1;
}
| hash_partition_list ',' hash_partition_element
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

hash_partition_element:
PARTITION relation_factor opt_part_id opt_subpartition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, $3, NULL, $4);
}
;

opt_range_partition_list:
'(' range_partition_list ')'
{
  merge_nodes($$, result, T_PARTITION_LIST, $2);
}
;

range_partition_list:
range_partition_element
{
  $$ = $1;
}
| range_partition_list ',' range_partition_element
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;
range_partition_element:
PARTITION relation_factor VALUES LESS THAN range_partition_expr opt_part_id opt_subpartition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, $6, $7, NULL, $8);
}
;

opt_list_partition_list:
'(' list_partition_list ')'
{
  merge_nodes($$, result, T_PARTITION_LIST, $2);
}
;

list_partition_list:
list_partition_element
{
  $$ = $1;
}
| list_partition_list ',' list_partition_element
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

list_partition_element:
PARTITION relation_factor VALUES IN list_partition_expr opt_part_id opt_subpartition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, $5, $6, NULL, $7);
}
;

opt_subpartition_list:
opt_hash_subpartition_list
{
  $$ = $1;
}
| opt_range_subpartition_list
{
  $$ = $1;
}
| opt_list_subpartition_list
{
  $$ = $1;
}
|
{
  $$ = NULL;
}

opt_hash_subpartition_list:
'(' hash_subpartition_list ')'
{
  merge_nodes($$, result, T_HASH_SUBPARTITION_LIST, $2);
}

hash_subpartition_list:
hash_subpartition_element
{
  $$ = $1;
}
| hash_subpartition_list ',' hash_subpartition_element
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}

hash_subpartition_element:
SUBPARTITION relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, NULL, NULL, NULL);
}
;

opt_range_subpartition_list:
'(' range_subpartition_list ')'
{
  merge_nodes($$, result, T_RANGE_SUBPARTITION_LIST, $2);
}
;

range_subpartition_list:
range_subpartition_element
{
  $$ = $1;
}
| range_subpartition_list ',' range_subpartition_element
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

range_subpartition_element:
SUBPARTITION relation_factor VALUES LESS THAN range_partition_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, $6, NULL, NULL, NULL);
}
;

opt_list_subpartition_list:
'(' list_subpartition_list ')'
{
  merge_nodes($$, result, T_LIST_SUBPARTITION_LIST, $2);
}
;

list_subpartition_list:
list_subpartition_element
{
  $$ = $1;
}
| list_subpartition_list ',' list_subpartition_element
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

list_subpartition_element:
SUBPARTITION relation_factor VALUES IN list_partition_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, $5, NULL, NULL, NULL);
}
;

list_partition_expr:
'(' list_expr ')' {
  merge_nodes($$, result, T_EXPR_LIST, $2);
}
|
'(' DEFAULT ')' {
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT);
}
;

list_expr:
expr {
  $$ = $1;
}
|
list_expr ',' expr {
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

range_partition_expr:
'(' range_expr_list ')'
{
  merge_nodes($$, result, T_EXPR_LIST, $2);
}
| MAXVALUE
{
  ParseNode *max_node = NULL;
  malloc_terminal_node(max_node, result->malloc_pool_, T_MAXVALUE);
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 1, max_node);
}
;

opt_part_id:
/* EMPTY */
{ $$ = NULL; }
| ID INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PART_ID, 1, $2);
}
;

range_expr_list:
range_expr
{
  $$ = $1;
}
| range_expr_list ',' range_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

range_expr:
expr
{
  $$ = $1;
}
| MAXVALUE
{
  malloc_terminal_node($$, result->malloc_pool_, T_MAXVALUE);
}
;

//opt_partition_column_list:
//    column_list
//    {
//      merge_nodes($$, result->malloc_pool_, T_COLUMN_LIST, $1);
//    }
//  | /*EMPTY*/
//    { $$ = NULL; }
//

opt_partitions:
PARTITIONS INTNUM
{
  $$ = $2;
}
|
{
  $$ = NULL;
}
;

opt_subpartitions:
SUBPARTITIONS INTNUM
{
  $$ = $2;
}
|
{
  $$ = NULL;
}
;

int_or_decimal:
INTNUM { $$ = $1; }
| DECIMAL_VAL { $$ = $1; }

/*tablegroup partition option*/
opt_tg_partition_option:
tg_hash_partition_option
{
  $$ = $1;
}
| tg_key_partition_option
{
  $$ = $1;
}
| tg_range_partition_option
{
  $$ = $1;
}
| tg_list_partition_option
{
  $$ = $1;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

tg_hash_partition_option:
PARTITION BY HASH tg_subpartition_option opt_partitions
{
  ParseNode *hash_func = NULL;
  make_name_node(hash_func, result->malloc_pool_, "partition_hash");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 3, $$, $5, $4);
}
;

tg_key_partition_option:
PARTITION BY KEY INTNUM tg_subpartition_option opt_partitions
{
  ParseNode *hash_func = NULL;
  make_name_node(hash_func, result->malloc_pool_, "partition_key_v2");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_KEY_PARTITION, 3, $$, $6, $5);
}
;
tg_range_partition_option:
PARTITION BY RANGE tg_subpartition_option opt_partitions opt_range_partition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_PARTITION, 4, NULL, $6, $4, $5);
}
| PARTITION BY RANGE COLUMNS INTNUM tg_subpartition_option opt_partitions opt_range_partition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_COLUMNS_PARTITION, 4, $5, $8, $6, $7);
}
;

tg_list_partition_option:
PARTITION BY BISON_LIST tg_subpartition_option opt_partitions opt_list_partition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_PARTITION, 4, NULL, $6, $4, $5);
}
| PARTITION BY BISON_LIST COLUMNS INTNUM tg_subpartition_option opt_partitions opt_list_partition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_COLUMNS_PARTITION, 4, $5, $8, $6, $7);
}
;

tg_subpartition_option:
SUBPARTITION BY RANGE SUBPARTITION TEMPLATE opt_range_subpartition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_PARTITION, 4, NULL, $6, NULL, NULL);
}
| SUBPARTITION BY RANGE COLUMNS INTNUM SUBPARTITION TEMPLATE opt_range_subpartition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_COLUMNS_PARTITION, 4, $5, $8, NULL, NULL);
}
| SUBPARTITION BY HASH opt_subpartitions
{
  ParseNode *hash_func = NULL;
  make_name_node(hash_func, result->malloc_pool_, "partition_hash");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 3, $$, $4, NULL);
}
| SUBPARTITION BY KEY INTNUM opt_subpartitions
{
  ParseNode *hash_func = NULL;
  make_name_node(hash_func, result->malloc_pool_, "partition_key_v2");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_KEY_PARTITION, 3, $$, $5, NULL);
}
| SUBPARTITION BY BISON_LIST SUBPARTITION TEMPLATE opt_list_subpartition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_PARTITION, 4, NULL, $6, NULL, NULL);
}
| SUBPARTITION BY BISON_LIST COLUMNS INTNUM SUBPARTITION TEMPLATE opt_list_subpartition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_COLUMNS_PARTITION, 4, $5, $8, NULL, NULL);
}
|
{
  $$ = NULL;
}
;

row_format_option:
REDUNDANT
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
| COMPACT
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
| DYNAMIC
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 3;
}
| COMPRESSED
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 4;
}
| DEFAULT
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 3;
}
;
/*****************************************************************************
 *
 * create tablegroup
 *
 *****************************************************************************/
create_tablegroup_stmt:
CREATE TABLEGROUP opt_if_not_exists relation_name opt_tablegroup_option_list opt_tg_partition_option
{
  ParseNode *tablegroup_options = NULL;
  merge_nodes(tablegroup_options, result, T_TABLEGROUP_OPTION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLEGROUP, 4, $3, $4, tablegroup_options, $6);
}
;

/*****************************************************************************
 *
 * drop tablegroup
 *
 *****************************************************************************/
drop_tablegroup_stmt:
DROP TABLEGROUP opt_if_exists relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_TABLEGROUP, 2, $3, $4);
}
;

/*****************************************************************************
 *
 * alter tablegroup
 *
 *****************************************************************************/
alter_tablegroup_stmt:
ALTER TABLEGROUP relation_name ADD opt_table table_list
{
  (void)($5);
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_TABLE_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLEGROUP, 2, $3, table_list);
}
| ALTER TABLEGROUP relation_name alter_tablegroup_actions
{
  ParseNode *tablegroup_actions = NULL;
  merge_nodes(tablegroup_actions, result, T_ALTER_TABLEGROUP_ACTION_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLEGROUP, 2, $3, tablegroup_actions);
}
| ALTER TABLEGROUP relation_name alter_tg_partition_option
{
  ParseNode *partition_options = NULL;
  malloc_non_terminal_node(partition_options, result->malloc_pool_, T_ALTER_PARTITION_OPTION, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLEGROUP, 2, $3, partition_options);
}
;

opt_tablegroup_option_list:
tablegroup_option_list
{
  $$ = $1;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;


tablegroup_option_list_space_seperated:
tablegroup_option
{
  $$ = $1;
}
| tablegroup_option  tablegroup_option_list_space_seperated
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

tablegroup_option_list:
tablegroup_option_list_space_seperated
{
  $$ = $1;
}
| tablegroup_option ',' tablegroup_option_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

tablegroup_option:
LOCALITY opt_equal_mark locality_name opt_force
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOCALITY, 2, $3, $4);
}
| PRIMARY_ZONE opt_equal_mark primary_zone_name
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_ZONE, 1, $3);
}
| TABLEGROUP_ID opt_equal_mark INTNUM
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLEGROUP_ID, 1, $3);
}
| BINDING opt_equal_mark BOOL_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLEGROUP_BINDING, 1, $3);
}
| MAX_USED_PART_ID opt_equal_mark INTNUM
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_USED_PART_ID, 1, $3);
}
;


alter_tablegroup_actions:
alter_tablegroup_action
{
  $$ = $1;
}
| alter_tablegroup_actions ',' alter_tablegroup_action
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

alter_tablegroup_action:
opt_set tablegroup_option_list_space_seperated
{
  (void)$1;
  $$ = $2;
}
;

/*****************************************************************************
 *
 * default tablegroup
 *
 *****************************************************************************/
default_tablegroup:
opt_default_mark TABLEGROUP opt_equal_mark relation_name
{
  (void)($1) ; /* make bison mute */
  (void)($3) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_TABLEGROUP);
  $$->str_value_ = $4->str_value_;
  $$->str_len_ = $4->str_len_;
}
| opt_default_mark TABLEGROUP opt_equal_mark NULLX
{
  (void)($1) ; /* make bison mute */
  (void)($3) ; /* make bison mute */
  (void)($4) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_TABLEGROUP);
}
;


opt_table:
TABLE { $$ = NULL; }
| /*EMPTY*/ {$$ = NULL;}
;

/*****************************************************************************
 *
 * create view
 *
 *****************************************************************************/
create_view_stmt:
CREATE opt_replace opt_materialized VIEW view_name opt_column_list opt_table_id AS view_select_stmt
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_VIEW, 7,
                           $3,    /* opt_materialized */
                           $5,    /* view name */
                           $6,    /* column list */
                           $7,    /* table_id */
                           $9,    /* select_stmt */
                           $2,
						   NULL   /* with option */
						   );
  dup_expr_string($9, result, @9.first_column, @9.last_column);
}
;

view_select_stmt:
select_stmt
{
  $$ = $1;
}
;

opt_replace:
OR REPLACE
{ malloc_terminal_node($$, result->malloc_pool_, T_IF_NOT_EXISTS); }
| /* EMPTY */
{ $$ = NULL; }
;

opt_materialized:
MATERIALIZED
{ malloc_terminal_node($$, result->malloc_pool_, T_MATERIALIZED); }
| /* EMPTY */
{ $$ = NULL; }
;

view_name:
relation_factor
{ $$ = $1; }
;

opt_column_list:
'(' column_name_list ')'
{
  merge_nodes($$, result, T_COLUMN_LIST, $2);
}
| /*EMPTY*/ { $$ = NULL; }
;

opt_table_id:
TABLE_ID COMP_EQ INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_ID, 1, $3);
}
| /*EMPTY*/ { $$ = NULL; }
;
/*****************************************************************************
 *
 *	create index
 *
 *****************************************************************************/

create_index_stmt:
CREATE opt_index_keyname INDEX opt_if_not_exists normal_relation_factor opt_index_using_algorithm ON relation_factor '(' sort_column_list ')'
opt_index_option_list opt_partition_option
{
  ParseNode *idx_columns = NULL;
  ParseNode *index_options = NULL;
  merge_nodes(idx_columns, result, T_INDEX_COLUMN_LIST, $10);
  merge_nodes(index_options, result, T_TABLE_OPTION_LIST, $12);
  $5->value_ = $2[0]; /* index prefix keyname */
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_INDEX, 7,
                           $5,                   /* index name */
                           $8,                   /* table name */
                           idx_columns,          /* index columns */
                           index_options,        /* index option(s) */
                           $6,                   /* index method */
                           $13,                  /* partition method*/
                           $4);                  /* if not exists*/
};

opt_index_keyname:
UNIQUE { $$[0] = 1; }
| /*EMPTY*/ { $$[0] = 0; }
;


opt_index_name:
index_name
{
  $$ = $1;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

index_name:
relation_name {$$ = $1;}
;

opt_constraint:
CONSTRAINT opt_constraint_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 1, $2);
}
|
{
  $$ = NULL;
}
;

opt_constraint_name:
constraint_name
{
  $$ = $1;
}
|
{
  $$ = NULL;
}
;

constraint_name:
relation_name {$$ = $1;}
;

sort_column_list:
sort_column_key
{ $$ = $1; }
| sort_column_list ',' sort_column_key
{ malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3); }
;

sort_column_key:
column_name opt_sort_column_key_length opt_asc_desc opt_column_id
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SORT_COLUMN_KEY, 4, $1, $2, $3, $4);
}
/*
  'split key' is abandoned from 1.0

  | SPLIT KEY '(' column_list ')'
  {
  ParseNode *col_list= NULL;
  merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SPLIT_KEY, 1, col_list);
  }
*/
;
opt_column_id:
/* EMPTY */
{ $$ = 0; }
| ID INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ID, 1, $2);
}
;
opt_sort_column_key_length:
'('  INTNUM ')'
{
  $$ = $2;
}
| /*empty*/
{
  $$ = NULL;
}
;

opt_index_option_list:
/*EMPTY*/
{
  $$ = NULL;
}
| opt_index_options
{
  $$ = $1;
}
;

opt_index_options:
index_option
{
  $$ = $1;
}
| opt_index_options index_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

index_option:
GLOBAL
{
  ParseNode *default_operand = NULL;
  malloc_terminal_node(default_operand, result->malloc_pool_, T_VARCHAR);
  int64_t len = strlen("GLOBAL");
  default_operand->str_value_ = parse_strndup("GLOBAL", len, result->malloc_pool_);
  if (OB_UNLIKELY(NULL == default_operand->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
  default_operand->str_len_ = len;

  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
  default_type->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_SCOPE, 2, default_type, default_operand);
}
| LOCAL
{
  ParseNode *default_operand = NULL;
  malloc_terminal_node(default_operand, result->malloc_pool_, T_VARCHAR);
  int64_t len = strlen("LOCAL");
  default_operand->str_value_ = parse_strndup("LOCAL", len, result->malloc_pool_);
  if (OB_UNLIKELY(NULL == default_operand->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
  default_operand->str_len_ = len;

  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
  default_type->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_SCOPE, 2, default_type, default_operand);
}
| BLOCK_SIZE opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_BLOCK_SIZE, 1, $3);
}
| COMMENT STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COMMENT, 1, $2);
}
| STORING '(' column_name_list ')'
{
  merge_nodes($$, result, T_STORING_COLUMN_LIST, $3);
}
| WITH_ROWID
{
  malloc_terminal_node($$, result->malloc_pool_, T_WITH_ROWID);
}
| WITH PARSER STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARSER_NAME, 1, $3);
}
| index_using_algorithm
{
  $$ = $1;
}
| visibility_option
{
  $$ = $1;
}
| DATA_TABLE_ID opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_DATA_TABLE_ID, 1, $3);
}
| INDEX_TABLE_ID opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_TABLE_ID, 1, $3);
}
| VIRTUAL_COLUMN_ID opt_equal_mark INTNUM
{
  (void)($2) ;
  malloc_non_terminal_node($$, result->malloc_pool_, T_VIRTUAL_COLUMN_ID, 1, $3);
}
| MAX_USED_PART_ID opt_equal_mark INTNUM
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_USED_PART_ID, 1, $3);
}
| parallel_option
{
  $$ = $1;
}
;

opt_index_using_algorithm:
/* EMPTY */
{
  $$ = NULL;
}
| index_using_algorithm
{
  $$ = $1;
}
;

index_using_algorithm:
USING BTREE
{
  malloc_terminal_node($$, result->malloc_pool_, T_USING_BTREE);
}
| USING HASH
{
  malloc_terminal_node($$, result->malloc_pool_, T_USING_HASH);
}
;

opt_materialized_or_temporary:
TEMPORARY
{ malloc_terminal_node($$, result->malloc_pool_, T_TEMPORARY); }
|
MATERIALIZED
{ malloc_terminal_node($$, result->malloc_pool_, T_MATERIALIZED); }
| /* EMPTY */
{ $$ = NULL; }
;
/*****************************************************************************
 *
 *	drop table grammar
 *
 *****************************************************************************/

drop_table_stmt:
DROP opt_materialized_or_temporary table_or_tables opt_if_exists table_list opt_drop_behavior
{
  (void)($3);
  ParseNode *tables = NULL;
  merge_nodes(tables, result, T_TABLE_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_TABLE, 3, $2, $4, tables);
}
;

table_or_tables:
TABLE
{
  $$ = NULL;
}
| TABLES
{
  $$ = NULL;
}
;

drop_view_stmt:
DROP opt_materialized VIEW opt_if_exists table_list opt_drop_behavior
{
  ParseNode *views = NULL;
  merge_nodes(views, result, T_VIEW_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_VIEW, 3, $2, $4, views);
}
;

opt_if_exists:
/* EMPTY */
{ $$ = NULL; }
| IF EXISTS
{ malloc_terminal_node($$, result->malloc_pool_, T_IF_EXISTS); }
;

table_list:
relation_factor
{
  $$ = $1;
}
| table_list ',' relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;


/*****************************************************************************
 *
 *	drop index grammar
 *
 *****************************************************************************/

drop_index_stmt:
DROP INDEX relation_name ON relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_INDEX, 2, $3, $5);
}
;


/*****************************************************************************
 *
 *	insert grammar
 *
 *****************************************************************************/
insert_stmt:
insert_with_opt_hint opt_ignore opt_into single_table_insert opt_on_duplicate_key_clause
{
  (void)($3);
  if (NULL == $4) {
    yyerror(NULL, result, "invalid single table insert node\n");
    YYABORT_UNEXPECTED;
  }
  $4->children_[2] = $5; /*duplicate key node*/
  malloc_non_terminal_node($$, result->malloc_pool_, T_INSERT, 4,
                           $4, /*single or multi table insert node*/
                           $1->children_[0], /* is replacement */
                           $1->children_[1], /* hint */
                           $2 /*ignore node*/);
}
| replace_with_opt_hint opt_ignore opt_into single_table_insert
{
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INSERT, 4,
                           $4, /*single or multi table insert node*/
                           $1->children_[0], /* is replacement */
                           $1->children_[1], /* hint */
                           $2 /*ignore node*/);
}
;

single_table_insert:
dml_table_name values_clause
{
  ParseNode *into_node = NULL;
  malloc_non_terminal_node(into_node, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 2, $1, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_TABLE_INSERT, 3,
                           into_node, /*insert_into_clause*/
                           $2, /*values_clause*/
                           NULL /*duplicate key node*/);
}
| dml_table_name '(' ')' values_clause
{
  ParseNode *into_node = NULL;
  malloc_non_terminal_node(into_node, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 2, $1, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_TABLE_INSERT, 3,
                           into_node, /*insert_into_clause*/
                           $4, /*values_clause*/
                           NULL /*duplicate key node*/);
}
| dml_table_name '(' column_list ')' values_clause
{
  ParseNode *into_node = NULL;
  ParseNode *column_list = NULL;
  merge_nodes(column_list, result, T_COLUMN_LIST, $3);
  malloc_non_terminal_node(into_node, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 2, $1, column_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_TABLE_INSERT, 3,
                           into_node, /*insert_into_clause*/
                           $5, /*values_clause*/
                           NULL /*duplicate key node*/);
}
| dml_table_name SET update_asgn_list
{
  ParseNode *val_list = NULL;
  ParseNode *into_node = NULL;
  merge_nodes(val_list, result, T_ASSIGN_LIST, $3);
  malloc_non_terminal_node(into_node, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 1, $1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_TABLE_INSERT, 3,
                           into_node, /*insert_into_clause*/
                           val_list, /*values_list*/
                           NULL /*duplicate key node*/);
}
;

values_clause:
value_or_values insert_vals_list
{
  (void)($1);
  merge_nodes($$, result, T_VALUE_LIST, $2);
}
| select_stmt
{
  $$ = $1;
}
;

value_or_values:
VALUE
{
  $$ = NULL;
}
| VALUES
{
  $$ = NULL;
}
;
opt_into:
INTO
{
  $$ = NULL;
}
/* empty */
| {
  $$ = NULL;
}
;
opt_ignore:
IGNORE
{
  malloc_terminal_node($$, result->malloc_pool_, T_IGNORE);
}
/* empty */
| {
  $$ = NULL;
}
;

opt_on_duplicate_key_clause:
ON DUPLICATE KEY UPDATE update_asgn_list
{
  ParseNode *assign_list = NULL;
  merge_nodes(assign_list, result, T_ASSIGN_LIST, $5);
  $$ = assign_list;
}
|/* EMPTY */
{ $$ = NULL; }
;

opt_when:
/* EMPTY */
{ $$ = NULL; }

replace_with_opt_hint:
REPLACE
{
  ParseNode *op_node = NULL;
  malloc_terminal_node(op_node, result->malloc_pool_, T_REPLACE);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, op_node, NULL);
}
| REPLACE_HINT_BEGIN hint_list_with_end
{
  ParseNode *op_node = NULL;
  malloc_terminal_node(op_node, result->malloc_pool_, T_REPLACE);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, op_node, $2);
}
;

insert_with_opt_hint:
INSERT
{
  ParseNode *op_node = NULL;
  malloc_terminal_node(op_node, result->malloc_pool_, T_INSERT);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, op_node, NULL);
}
| INSERT_HINT_BEGIN hint_list_with_end
{
  ParseNode *op_node = NULL;
  malloc_terminal_node(op_node, result->malloc_pool_, T_INSERT);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, op_node, $2);
}
;

column_list:
column_definition_ref {$$ = $1; }
| column_list ',' column_definition_ref
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

insert_vals_list:
'(' insert_vals ')'
{
  merge_nodes($$, result, T_VALUE_VECTOR, $2);
}
| insert_vals_list ',' '(' insert_vals ')'
{
  merge_nodes($4, result, T_VALUE_VECTOR, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $4);
}
;

insert_vals:
expr_or_default { $$ = $1; }
| insert_vals ',' expr_or_default
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| /* EMPTY */
{
  malloc_terminal_node($$, result->malloc_pool_, T_EMPTY);
}
;
expr_or_default:
expr { $$ = $1;}
| DEFAULT
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT);
}
;

/*****************************************************************************
 *
 *	select grammar
 *
 *****************************************************************************/

select_stmt:
select_no_parens opt_when
{
  $$ = $1;
  $$->children_[PARSE_SELECT_WHEN] = $2;
  if (NULL == $$->children_[PARSE_SELECT_FOR_UPD] && NULL != $2)
  {
    malloc_terminal_node($$->children_[PARSE_SELECT_FOR_UPD], result->malloc_pool_, T_INT);
    $$->children_[PARSE_SELECT_FOR_UPD]->value_ = -1;
  }
}
| select_with_parens
{
  $$ = $1;
}
| select_into
{
  $$ = $1;
}
;

// for select_into
//select ..from/.. into ..
select_into:
select_no_parens into_clause
{
  $$ = $1;
  if ($2 != NULL) {
    if ($$->children_[PARSE_SELECT_INTO] != NULL) {
      yyerror(&@2, result, "");
      YYERROR;
    } else {
      $$->children_[PARSE_SELECT_INTO_EXTRA] = $2;
    }
  }
}
;

select_with_parens:
'(' select_no_parens ')'      { $$ = $2; }
| '(' select_with_parens ')'  { $$ = $2; }
;

select_no_parens:
select_clause opt_for_update
{
  $$ = $1;
  $$->children_[PARSE_SELECT_FOR_UPD] = $2;
}
| select_clause_set opt_for_update
{
  $$ = $1;
  $$->children_[PARSE_SELECT_FOR_UPD] = $2;
}
| select_clause_set_with_order_and_limit opt_for_update
{
  $$ = $1;
  $$->children_[PARSE_SELECT_FOR_UPD] = $2;
}
;

no_table_select:
select_with_opt_hint opt_query_expression_option_list select_expr_list into_opt
{
  ParseNode *project_list = NULL;
  merge_nodes(project_list, result, T_PROJECT_LIST, $3);

  ParseNode *select_node = NULL;
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_DISTINCT] = $2;
  select_node->children_[PARSE_SELECT_SELECT] = project_list;
  select_node->children_[PARSE_SELECT_HINTS] = $1;
  select_node->children_[PARSE_SELECT_INTO] = $4;
  $$ = select_node;

  setup_token_pos_info(select_node, @1.first_column - 1, 6);
}
| select_with_opt_hint opt_query_expression_option_list select_expr_list into_opt
FROM DUAL opt_where opt_named_windows
{
  ParseNode *project_list = NULL;
  merge_nodes(project_list, result, T_PROJECT_LIST, $3);

  ParseNode *select_node = NULL;
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_DISTINCT] = $2;
  select_node->children_[PARSE_SELECT_SELECT] = project_list;
  select_node->children_[PARSE_SELECT_WHERE] = $7;
  select_node->children_[PARSE_SELECT_HINTS] = $1;
  select_node->children_[PARSE_SELECT_INTO] = $4;
  select_node->children_[PARSE_SELECT_NAMED_WINDOWS] = $8;
  $$ = select_node;

  setup_token_pos_info(select_node, @1.first_column - 1, 6);
}
;

select_clause:
no_table_select
{
  $$ = $1;
}
| no_table_select_with_order_and_limit
{
  $$ = $1;
}
| simple_select
{
  $$ = $1;
  }
| simple_select_with_order_and_limit
{
  $$ = $1;
}
| select_with_parens_with_order_and_limit
{
  $$ = $1;
}
;

select_clause_set_with_order_and_limit:
select_clause_set order_by
{
  $$ = $1;
  $$->children_[PARSE_SELECT_ORDER] = $2;
}
| select_clause_set opt_order_by limit_clause
{
  $$ = $1;
  $$->children_[PARSE_SELECT_ORDER] = $2;
  $$->children_[PARSE_SELECT_LIMIT] = $3;
}
;

select_clause_set:
select_clause_set set_type select_clause_set_right
{
  ParseNode *select_node = NULL;
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_SET] = $2;
  select_node->children_[PARSE_SELECT_FORMER] = $1;
  select_node->children_[PARSE_SELECT_LATER] = $3;
  $$ = select_node;
}
| select_clause_set_with_order_and_limit set_type select_clause_set_right {
  ParseNode *select_node = NULL;
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_SET] = $2;
  select_node->children_[PARSE_SELECT_FORMER] = $1;
  select_node->children_[PARSE_SELECT_LATER] = $3;
  $$ = select_node;
}
| select_clause_set_left set_type select_clause_set_right {
  ParseNode *select_node = NULL;
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_SET] = $2;
  select_node->children_[PARSE_SELECT_FORMER] = $1;
  select_node->children_[PARSE_SELECT_LATER] = $3;
  $$ = select_node;
}
;

select_clause_set_right:
no_table_select
{
  $$ = $1;
}
| simple_select
{
  $$ = $1;
}
| select_with_parens %prec LOWER_PARENS
{
  $$ = $1;
}
;

select_clause_set_left:
no_table_select_with_order_and_limit
{
  $$ = $1;
}
| simple_select_with_order_and_limit
{
  $$ = $1;
}
| select_clause_set_right
{
  $$ = $1;
}
;

no_table_select_with_order_and_limit:
no_table_select order_by
{
  $$ = $1;
  $$->children_[PARSE_SELECT_ORDER] = $2;
}
| no_table_select opt_order_by limit_clause
{
  $$ = $1;
  $$->children_[PARSE_SELECT_ORDER] = $2;
  $$->children_[PARSE_SELECT_LIMIT] = $3;
}
;

simple_select_with_order_and_limit:
simple_select order_by
{
  $$ = $1;
  $$->children_[PARSE_SELECT_ORDER] = $2;
}
| simple_select opt_order_by limit_clause
{
  $$ = $1;
  $$->children_[PARSE_SELECT_ORDER] = $2;
  $$->children_[PARSE_SELECT_LIMIT] = $3;
}
;

select_with_parens_with_order_and_limit:
select_with_parens order_by
{
  // select_list
  ParseNode *project_list = NULL;

  ParseNode *star_node = NULL;
  malloc_terminal_node(star_node, result->malloc_pool_, T_STAR);

  ParseNode *upper_node = NULL;
  malloc_non_terminal_node(upper_node, result->malloc_pool_, T_PROJECT_STRING, 1, star_node);
  dup_string_to_node(upper_node, result->malloc_pool_, "*");

  merge_nodes(project_list, result, T_PROJECT_LIST, upper_node);

  // from_list
  ParseNode *alias_node = NULL;
  make_name_node(alias_node, result->malloc_pool_, "");
  malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, alias_node);

  ParseNode *from_list = NULL;
  merge_nodes(from_list, result, T_FROM_LIST, alias_node);
  // upper stmt

  ParseNode *select_node = NULL;
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_SELECT] = project_list;
  select_node->children_[PARSE_SELECT_FROM] = from_list;
  select_node->children_[PARSE_SELECT_ORDER] = $2;
  $$ = select_node;
}
| select_with_parens opt_order_by limit_clause
{
  // select_list
  ParseNode *project_list = NULL;

  ParseNode *star_node = NULL;
  malloc_terminal_node(star_node, result->malloc_pool_, T_STAR);

  ParseNode *upper_node = NULL;
  malloc_non_terminal_node(upper_node, result->malloc_pool_, T_PROJECT_STRING, 1, star_node);
  dup_string_to_node(upper_node, result->malloc_pool_, "*");

  merge_nodes(project_list, result, T_PROJECT_LIST, upper_node);

  // from_list
  ParseNode *alias_node = NULL;
  make_name_node(alias_node, result->malloc_pool_, "");
  malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, alias_node);

  ParseNode *from_list = NULL;
  merge_nodes(from_list, result, T_FROM_LIST, alias_node);
  // upper stmt

  ParseNode *select_node = NULL;
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_SELECT] = project_list;
  select_node->children_[PARSE_SELECT_FROM] = from_list;
  select_node->children_[PARSE_SELECT_ORDER] = $2;
  select_node->children_[PARSE_SELECT_LIMIT] = $3;
  $$ = select_node;

}
;

select_with_opt_hint:
SELECT {$$ = NULL;}
| SELECT_HINT_BEGIN hint_list_with_end
{
  $$ = $2;
#ifdef SQL_PARSER_COMPILATION
  if (NULL != $$) {
    if (OB_UNLIKELY(NULL == $2)) {
      yyerror(NULL, result, "hint_list_with_end not cannot be NULL here");
      YYABORT;
    } else {
      // select /*+ no_rewrite */ 1 from dual;
      // @1.first_column: pos of 'select' (1)
      // @1.last_column: pos of '+' (10)
      // @2.first_column: start pos of 'no_rewrite' (12)
      // @2.last_column: end pos of '*/' (24)
      setup_token_pos_info($$, @1.last_column - 1, @2.last_column - @1.last_column + 1);
      result->stop_add_comment_ = true;
    }
  }
#endif
}

update_with_opt_hint:
UPDATE {$$ = NULL;}
| UPDATE_HINT_BEGIN hint_list_with_end
{$$ = $2;}
;

delete_with_opt_hint:
DELETE {$$ = NULL;}
| DELETE_HINT_BEGIN hint_list_with_end
{$$ = $2;}
;

simple_select:
select_with_opt_hint opt_query_expression_option_list select_expr_list into_opt
FROM from_list
opt_where opt_groupby opt_having opt_named_windows
{
  ParseNode *project_list = NULL;
  ParseNode *from_list = NULL;
  merge_nodes(project_list, result, T_PROJECT_LIST, $3);
  merge_nodes(from_list, result, T_FROM_LIST, $6);

  ParseNode *select_node = NULL;
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_DISTINCT] = $2;
  select_node->children_[PARSE_SELECT_SELECT] = project_list;
  select_node->children_[PARSE_SELECT_FROM] = from_list;
  select_node->children_[PARSE_SELECT_WHERE] = $7;
  select_node->children_[PARSE_SELECT_GROUP] = $8;
  select_node->children_[PARSE_SELECT_HAVING] = $9;
  select_node->children_[PARSE_SELECT_HINTS] = $1;
  select_node->children_[PARSE_SELECT_INTO] = $4;
  select_node->children_[PARSE_SELECT_NAMED_WINDOWS] = $10;
  $$ = select_node;

  setup_token_pos_info(from_list, @5.first_column - 1, 4);
  setup_token_pos_info(select_node, @1.first_column - 1, 6);
}
;

set_type_union:
UNION     { $$[0] = T_SET_UNION; }
;

set_type_other:
INTERSECT { $$[0] = T_SET_INTERSECT; }
| EXCEPT    { $$[0] = T_SET_EXCEPT; }
| MINUS    { $$[0] = T_SET_EXCEPT; }
;

set_type:
set_type_union set_expression_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, $1[0], 1, $2);
}
| set_type_other
{
  malloc_non_terminal_node($$, result->malloc_pool_, $1[0], 1, NULL);
}

set_expression_option:
/* EMPTY */
{ $$ = NULL; }
| ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_ALL);
}
| DISTINCT
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTINCT);
}
| UNIQUE
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTINCT);
}
;

opt_where:
/* EMPTY */
{$$ = NULL;}
| WHERE opt_hint_value expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WHERE_CLAUSE, 2, $3, $2);
  setup_token_pos_info($$, @1.first_column - 1, 5);
}
;

opt_hint_value:
/* EMPTY */
{$$ = NULL;}
| HINT_VALUE
{
  $$ = $1;
}
;

limit_clause:
LIMIT limit_expr OFFSET limit_expr
{
  if ($2 != NULL) {
    if (T_INT == $2->type_) {
      $2->type_ = T_LIMIT_INT;
    } else if (T_UINT64 == $2->type_) {
      $2->type_ = T_LIMIT_UINT;
    }
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIMIT_CLAUSE, 2, $2, $4);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| LIMIT limit_expr
{
  if ($2 != NULL) {
    if (T_INT == $2->type_) {
      $2->type_ = T_LIMIT_INT;
    } else if (T_UINT64 == $2->type_) {
      $2->type_ = T_LIMIT_UINT;
    }
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIMIT_CLAUSE, 2, $2, NULL);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @2.last_column),
            &@1, result);
}
| LIMIT limit_expr ',' limit_expr
{
  if ($4 != NULL) {
    if (T_INT == $4->type_) {
      $4->type_ = T_LIMIT_INT;
    } else if (T_UINT64 == $4->type_) {
      $4->type_ = T_LIMIT_UINT;
    }
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_COMMA_LIMIT_CLAUSE, 2, $2, $4);
  // setup_token_pos_info($$, @1.first_column - 1, @4.last_column - @1.first_column + 1);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
;

into_clause:
INTO OUTFILE STRING_VALUE opt_charset field_opt line_opt
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INTO_OUTFILE, 4, $3, $4, $5, $6);
}
| INTO DUMPFILE STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INTO_DUMPFILE, 1, $3);
}
| INTO into_var_list
{
  ParseNode *vars_list = NULL;
  merge_nodes(vars_list, result, T_INTO_VARS_LIST, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INTO_VARIABLES, 1, vars_list);
  copy_and_skip_symbol(result, @1.first_column, @2.last_column);
}
;

into_opt:
%prec LOWER_INTO
{
  $$ = NULL;
}
| into_clause
{
  $$ = $1;
}
;

into_var_list:
into_var_list ',' into_var
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| into_var
{
  $$ = $1;
}
;

into_var:
USER_VARIABLE
{
  $$ = $1;
}
|
var_name
{
  $$ = $1;
}
;

field_opt:
columns_or_fields field_term_list
{
  (void)$1;
  merge_nodes($$, result, T_INTO_FIELD_LIST, $2);
}
| /*empty*/
{
  $$ = NULL;
}
;

field_term_list:
field_term_list field_term
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| field_term
{
  $$ = $1;
}
;

field_term:
TERMINATED BY STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FIELD_TERMINATED_STR, 1, $3);
}
| OPTIONALLY ENCLOSED BY STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OPTIONALLY_CLOSED_STR, 1, $4);
}
| ENCLOSED BY STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CLOSED_STR, 1, $3);
}
| ESCAPED BY STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ESCAPED_STR, 1, $3);
}
;

line_opt:
LINES line_term_list
{
  merge_nodes($$, result, T_INTO_LINE_LIST, $2);
}
| /* empty */
{
  $$ = NULL;
}
;

line_term_list:
line_term_list line_term
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| line_term
{
  $$ = $1;
}
;

line_term:
TERMINATED BY STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINE_TERMINATED_STR, 1, $3);
}
| STARTING BY STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINE_START_STR, 1, $3);
}
;

hint_list_with_end:
opt_hint_list HINT_END
{
  if (NULL != $1) {
    merge_nodes($$, result, T_HINT_OPTION_LIST, $1);
  } else {
    $$ = NULL;
  }
}
;

opt_hint_list:
hint_options
{
  $$ = $1;
}
| opt_hint_list ',' hint_options
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

hint_options:
hint_option
{
  $$ = $1;
}
| hint_options hint_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

name_list:
NAME_OB
{
  $$ = $1;
}
| name_list NAME_OB %prec COMMA
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| name_list ',' NAME_OB %prec COMMA
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

hint_option:
NO_REWRITE
{
  malloc_terminal_node($$, result->malloc_pool_, T_NO_REWRITE);
}
| READ_CONSISTENCY '(' consistency_level ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_READ_CONSISTENCY);
  $$->value_ = $3[0];
}
| INDEX_HINT '(' qb_name_option relation_factor_in_hint NAME_OB ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX, 3, $3, $4, $5);
}
| QUERY_TIMEOUT '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_QUERY_TIMEOUT, 1, $3);
}
| FROZEN_VERSION '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FROZEN_VERSION, 1, $3);
}
| TOPK '(' INTNUM INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TOPK, 2, $3, $4);
}
| HOTSPOT
{
  malloc_terminal_node($$, result->malloc_pool_, T_HOTSPOT);
}
| LOG_LEVEL '(' NAME_OB ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOG_LEVEL, 1, $3);
}
| LOG_LEVEL '(' '\'' STRING_VALUE '\'' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOG_LEVEL, 1, $4);
}
| LEADING_HINT '(' qb_name_option relation_factor_in_leading_hint_list_entry ')'
{
  ParseNode *link_node = NULL;
  malloc_non_terminal_node(link_node, result->malloc_pool_, T_LINK_NODE, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LEADING, 2, $3, link_node);
}
| LEADING_HINT '(' qb_name_option relation_factor_in_hint_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LEADING, 2, $3, $4);
}
| ORDERED
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORDERED);
}
| FULL_HINT '(' qb_name_option relation_factor_in_hint ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FULL, 2, $3, $4);
}
| USE_PLAN_CACHE '(' use_plan_cache_type ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_USE_PLAN_CACHE);
  $$->value_ = $3[0];
}
| USE_MERGE '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_MERGE, 2, $3, table_list);
}
| NO_USE_MERGE '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_MERGE, 2, $3, table_list);
}
| USE_HASH '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_HASH, 2, $3, table_list);
}
| NO_USE_HASH '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_HASH, 2, $3, table_list);
}
| USE_NL '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_NL, 2, $3, table_list);
}
| NO_USE_NL '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_NL, 2, $3, table_list);
}
| USE_BNL '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_BNL, 2, $3, table_list);
}
| NO_USE_BNL '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_BNL, 2, $3, table_list);
}
| USE_NL_MATERIALIZATION '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_NL_MATERIALIZATION, 2, $3, table_list);
}
| NO_USE_NL_MATERIALIZATION '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_NL_MATERIALIZATION, 2, $3, table_list);
}
| USE_HASH_AGGREGATION
{
  malloc_terminal_node($$, result->malloc_pool_, T_USE_HASH_AGGREGATE);
}
| NO_USE_HASH_AGGREGATION
{
  malloc_terminal_node($$, result->malloc_pool_, T_NO_USE_HASH_AGGREGATE);
}
| MERGE_HINT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE_HINT, 1, $2);
}
| NO_MERGE_HINT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_MERGE_HINT, 1, $2);
}
| NO_EXPAND opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_EXPAND, 1, $2);
}
| USE_CONCAT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_CONCAT, 1, $2);
}
| UNNEST opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_UNNEST, 1, $2);
}
| NO_UNNEST opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_UNNEST, 1, $2);
}
| PLACE_GROUP_BY opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PLACE_GROUP_BY, 1, $2);
}
| NO_PLACE_GROUP_BY opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PLACE_GROUP_BY, 1, $2);
}
| NO_PRED_DEDUCE opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PRED_DEDUCE, 1, $2);
}
| USE_JIT '(' use_jit_type ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_USE_JIT);
  $$->value_ = $3[0];
}
| NO_USE_JIT
{
  malloc_terminal_node($$, result->malloc_pool_, T_NO_USE_JIT);
}
| USE_LATE_MATERIALIZATION
{
  malloc_terminal_node($$, result->malloc_pool_, T_USE_LATE_MATERIALIZATION);
}
| NO_USE_LATE_MATERIALIZATION
{
  malloc_terminal_node($$, result->malloc_pool_, T_NO_USE_LATE_MATERIALIZATION);
}
| TRACE_LOG
{
  malloc_terminal_node($$, result->malloc_pool_, T_TRACE_LOG);
}
| STAT '(' tracing_num_list ')'
{
  ParseNode *tracing_nums = NULL;
  merge_nodes(tracing_nums, result, T_STAT, $3);
  $$=tracing_nums;
}
| TRACING '(' tracing_num_list ')'
{
  ParseNode *tracing_nums = NULL;
  merge_nodes(tracing_nums, result, T_TRACING, $3);
  $$=tracing_nums;
}
| USE_PX
{
  malloc_terminal_node($$, result->malloc_pool_, T_USE_PX);
}
| NO_USE_PX
{
  malloc_terminal_node($$, result->malloc_pool_, T_NO_USE_PX);
}
| TRANS_PARAM '(' trans_param_name opt_comma trans_param_value ')'
{
  (void) $4;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANS_PARAM, 2, $3, $5);
}
| PX_JOIN_FILTER '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PX_JOIN_FILTER, 2, $3, table_list);
}
| NO_PX_JOIN_FILTER '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PX_JOIN_FILTER, 2, $3, table_list);
}
| FORCE_REFRESH_LOCATION_CACHE
{
  malloc_terminal_node($$, result->malloc_pool_, T_FORCE_REFRESH_LOCATION_CACHE);
}
| QB_NAME '(' NAME_OB ')'
{
malloc_non_terminal_node($$, result->malloc_pool_, T_QB_NAME, 1, $3);
}
| MAX_CONCURRENT '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_CONCURRENT, 1, $3);
}
| PARALLEL '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARALLEL, 1, $3);
}
| NO_PARALLEL
{
  malloc_terminal_node($$, result->malloc_pool_, T_NO_PARALLEL);
}
| PQ_DISTRIBUTE '(' qb_name_option relation_factor_in_pq_hint opt_comma distribute_method opt_distribute_method ')'
{
  (void) $5;
  malloc_non_terminal_node($$, result->malloc_pool_, T_PQ_DISTRIBUTE, 4, $3, $4, $6, $7);
}
| LOAD_BATCH_SIZE '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOAD_BATCH_SIZE, 1, $3);
}
| PQ_MAP '(' qb_name_option relation_factor_in_hint ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PQ_MAP, 2, $3, $4);
}
| NAME_OB
{
  destroy_tree($1);
  $$ = NULL;
}
| END_P
{
  $$ = NULL;
  yyerror(&@1, result, "unterminated hint string\n");
  YYABORT;
}
| PARSER_SYNTAX_ERROR
{
  $$ = NULL;
  yyerror(&@1, result, "unterminated hint string\n");
  YYABORT;
}
| error
{
  $$ = NULL;
}
| ENABLE_PARALLEL_DML
{
  malloc_terminal_node($$, result->malloc_pool_, T_ENABLE_PARALLEL_DML);
}
| DISABLE_PARALLEL_DML
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISABLE_PARALLEL_DML);
}
;

opt_qb_name:
'(' qb_name_option ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OPT_QB_NAME, 1, $2);
}
| /*empty*/
{
  $$ = NULL;
}
;

consistency_level:
WEAK
{
  $$[0] = 3;
}
| STRONG
{
  $$[0] = 4;
}
| FROZEN
{
  $$[0] = 2;
}
;

use_plan_cache_type:
NONE
{
  $$[0] = 1;
}
| DEFAULT
{
  $$[0] = 2;
};

use_jit_type:
AUTO
{
  $$[0] = 1;
}
| FORCE
{
  $$[0] = 2;
};

distribute_method:
NONE
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_NONE);
}
| PARTITION
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_PARTITION);
}
| RANDOM
{
  malloc_terminal_node($$, result->malloc_pool_, T_RANDOM);
}
| RANDOM_LOCAL
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_RANDOM_LOCAL);
}
| HASH
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_HASH);
}
| BROADCAST
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_BROADCAST);
};

opt_distribute_method:
opt_comma distribute_method
{
  (void) $1;
  $$ = $2;
}
| /* EMPTY */
{
  $$ = NULL;
};

limit_expr:
INTNUM
{ $$ = $1; }
| QUESTIONMARK
{ $$ = $1; }
;

opt_limit_clause:
/* EMPTY */
{ $$ = NULL; }
| limit_clause
{ $$ = $1; }
;

opt_for_update:
/* EMPTY */
{ $$ = NULL; }
| FOR UPDATE opt_for_update_wait
{
  $$ = $3;
}
;

opt_for_update_wait:
/* EMPTY */
{
  /* USE T_SFU_XXX to avoid being parsed by plan cache as template var */
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = -1;
}
| WAIT DECIMAL_VAL
{
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_DECIMAL);
  $$->str_value_ = $2->str_value_;
  $$->str_len_ = $2->str_len_;
}
| WAIT INTNUM
{
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = $2->value_;
}
| NOWAIT
{
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = 0;
}
| NO_WAIT
{
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = 0;
};

parameterized_trim:
expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 1, $1);
}
| expr FROM expr
{
  ParseNode *default_type = NULL;
  //avoid parameterized, so use T_DEFAULT_INT
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, $1, $3);
}
| BOTH expr FROM expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, $2, $4);
}
| LEADING expr FROM expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, $2, $4);
}
| TRAILING expr FROM expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, $2, $4);
}
| BOTH FROM expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 2, default_type, $3);
}
| LEADING FROM expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 2, default_type, $3);
}
| TRAILING FROM expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 2, default_type, $3);
}
;

opt_groupby:
/* EMPTY */
{ $$ = NULL; }
| GROUP BY groupby_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_GROUPBY_CLAUSE, 1, $3);
  setup_token_pos_info($$, @1.first_column - 1, 8);
}
;

groupby_clause:
sort_list_for_group_by opt_rollup
{
  ParseNode *group_exprs = NULL;
  merge_nodes(group_exprs, result, T_SORT_LIST, $1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WITH_ROLLUP_CLAUSE, 2, $2, group_exprs);
}
;

sort_list_for_group_by:
sort_key_for_group_by
{ $$ = $1; }
| sort_list_for_group_by ',' sort_key_for_group_by
{ malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3); }
;

sort_key_for_group_by:
expr opt_asc_desc_for_group_by
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SORT_KEY, 2, $1, $2); }
;

opt_asc_desc_for_group_by:
/* EMPTY */
{ $$ = NULL;}
| ASC
{ malloc_terminal_node($$, result->malloc_pool_, T_SORT_ASC); }
| DESC
{ malloc_terminal_node($$, result->malloc_pool_, T_SORT_DESC); }
;

opt_rollup:
/* EMPTY */
{ $$ = NULL;}
| WITH ROLLUP
{ malloc_terminal_node($$, result->malloc_pool_, T_ROLLUP); }
;


opt_order_by:
order_by	              { $$ = $1;}
| /*EMPTY*/             { $$ = NULL; }
;

order_by:
ORDER BY sort_list
{
  ParseNode *sort_list = NULL;
  ParseNode *opt_siblings = NULL;
  merge_nodes(sort_list, result, T_SORT_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORDER_BY, 2, sort_list, opt_siblings);
  setup_token_pos_info($$, @1.first_column - 1, 8);
}
;

sort_list:
sort_key
{ $$ = $1; }
| sort_list ',' sort_key
{ malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3); }
;

sort_key:
expr opt_asc_desc
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SORT_KEY, 2, $1, $2);
  if (NULL == $1->str_value_) {
    dup_string($1, result, @1.first_column, @1.last_column);
  }
}
;




opt_asc_desc:
/* EMPTY */
{ malloc_terminal_node($$, result->malloc_pool_, T_SORT_ASC); $$->value_ = 2; }
| ASC
{ malloc_terminal_node($$, result->malloc_pool_, T_SORT_ASC); $$->value_ = 2; }
| DESC
{ malloc_terminal_node($$, result->malloc_pool_, T_SORT_DESC); $$->value_ = 2; }
;

opt_having:
/* EMPTY */
{ $$ = 0; }
| HAVING expr
{
  $$ = $2;
  setup_token_pos_info($$, @1.first_column - 1, 6);
}
;

opt_query_expression_option_list:
query_expression_option_list
{
  merge_nodes($$, result, T_QEURY_EXPRESSION_LIST, $1);
}
|
{
  $$ = NULL;
}
;

query_expression_option_list:
query_expression_option
{
  $$ = $1;
}
| query_expression_option  query_expression_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

query_expression_option:
ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_ALL);
}
| DISTINCT
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTINCT);
}
| UNIQUE
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTINCT);
}
| SQL_CALC_FOUND_ROWS
{
  malloc_terminal_node($$, result->malloc_pool_, T_FOUND_ROWS);
}
;
projection:
expr %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, $1);
  if (T_VARCHAR == $1->type_) {
    if (2 == $1->num_child_ && 1 == $1->children_[1]->num_child_)
    {
      $$->str_value_ = $1->children_[1]->children_[0]->str_value_;
      $$->str_len_ = $1->children_[1]->children_[0]->str_len_;
    }
    else
    {
      if (1 == @1.last_column - @1.first_column) {
        $$->str_value_ = $1->str_value_;
        $$->str_len_ = $1->str_len_;
      } else {
        dup_node_string($1, $$, result->malloc_pool_);
      }
    }
  }
  else
  {
    dup_expr_string($$, result, @1.first_column, @1.last_column);
  }
  $$->raw_sql_offset_ = @1.first_column - 1;
}
| expr column_label
{
  ParseNode *alias_node = NULL;
  if (OB_UNLIKELY((NULL == $1))) {
    yyerror(NULL, result, "alias expr can not be NULL\n");
    YYABORT_UNEXPECTED;
  } else if (OB_UNLIKELY(T_COLUMN_REF == $1->type_ && NULL != $1->children_ && NULL != $1->children_[1]
             && NULL != $1->children_[2] && T_STAR == $1->children_[2]->type_)) {
    yyerror(&@2, result, "select table.* as label is invalid\n");
    YYERROR;
  } else {
    malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, $2);
    malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
    dup_expr_string($$, result, @1.first_column, @1.last_column);
    dup_node_string($2, alias_node, result->malloc_pool_);
    alias_node->param_num_ = 0;
  }
}
| expr AS column_label
{
  ParseNode *alias_node = NULL;
  if (OB_UNLIKELY((NULL == $1))) {
    yyerror(NULL, result, "alias expr can not be NULL\n");
    YYABORT_UNEXPECTED;
  } else if (OB_UNLIKELY(T_COLUMN_REF == $1->type_ && NULL != $1->children_ && NULL != $1->children_[1]
             && NULL != $1->children_[2] && T_STAR == $1->children_[2]->type_)) {
    yyerror(&@2, result, "select table.* as label is invalid\n");
    YYERROR;
  } else {
    malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, $3);
    malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
    dup_expr_string($$, result, @1.first_column, @1.last_column);
    dup_node_string($3, alias_node, result->malloc_pool_);
    alias_node->param_num_ = 0;
  }
}
| expr opt_as STRING_VALUE
{
  (void)($2);
  if (OB_UNLIKELY(NULL == $1)) {
    yyerror(NULL, result, "alias expr can not be NULL\n");
    YYABORT_UNEXPECTED;
  } else if (OB_UNLIKELY(T_COLUMN_REF == $1->type_ && NULL !=$1->children_ && NULL != $1->children_[1]
             && NULL != $1->children_[2] && T_STAR == $1->children_[2]->type_)) {
    yyerror(&@2, result, "select table.* as label is invalid\n");
    YYERROR;
  } else {
    ParseNode *alias_node = NULL;
    ParseNode *alias_name_node = NULL;
    malloc_terminal_node(alias_name_node, result->malloc_pool_, T_IDENT);
    if (NULL == $3->str_value_) {
      alias_name_node->str_value_ = NULL;
      alias_name_node->str_len_ = 0;
    } else {
      dup_node_string($3, alias_name_node, result->malloc_pool_);
    }
    malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, alias_name_node);
    malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
    dup_expr_string($$, result, @1.first_column, @1.last_column);
    if (NULL == $3->str_value_) {
      alias_node->str_value_ = NULL;
      alias_node->str_len_ = 0;
    } else {
      dup_node_string($3, alias_node, result->malloc_pool_);
    }
    alias_node->param_num_ = 1;
  }
}
| '*'
{
  ParseNode *star_node = NULL;
  malloc_terminal_node(star_node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, star_node);
  dup_expr_string($$, result, @1.first_column, @1.last_column);
  setup_token_pos_info(star_node, @1.first_column - 1, 1);
}
;

opt_as:
AS {$$ = NULL;}
| /*emtpy*/ {$$ = NULL;}
;

select_expr_list:
projection
{
  $$ = $1;
}
| select_expr_list ',' projection
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

// @ TODO not support  like 'select * from (t1,t2) join (t3,t4)'
from_list:
table_references
{
  $$ = $1;
}
;

table_references:
table_reference
{
  $$ = $1;
}
| table_references ',' table_reference
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

table_reference:
table_factor
{
  $$ = $1;
}
| joined_table
{
  $$ = $1;
}
;

table_factor:
tbl_name
{
  $$ = $1;
}
| table_subquery
{
  $$ = $1;
}
| select_with_parens %prec LOWER_PARENS
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, unname_node);
}
| '(' table_references ')'
{
  $$ = $2;
}
;

tbl_name:
relation_factor %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, NULL, NULL, NULL);
}
| relation_factor use_partition %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, NULL, $2, NULL);
}
| relation_factor use_partition index_hint_list %prec LOWER_PARENS
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, $$, $2, NULL);
}
| relation_factor use_partition sample_clause %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, NULL, $2, $3);
}
| relation_factor use_partition sample_clause seed %prec LOWER_PARENS
{
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, NULL, $2, $3);
}
| relation_factor use_partition sample_clause index_hint_list %prec LOWER_PARENS
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, $$, $2, $3);
}
| relation_factor use_partition sample_clause seed index_hint_list %prec LOWER_PARENS
{
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
  merge_nodes($$, result, T_INDEX_HINT_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, $$, $2, $3);
}
| relation_factor sample_clause %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, NULL, NULL, $2);
}
| relation_factor sample_clause seed %prec LOWER_PARENS
{
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, NULL, NULL, $2);
}
| relation_factor sample_clause index_hint_list %prec LOWER_PARENS
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, $$, NULL, $2);
}
| relation_factor sample_clause seed index_hint_list %prec LOWER_PARENS
{
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
  merge_nodes($$, result, T_INDEX_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, $$, NULL, $2);
}
| relation_factor index_hint_list %prec LOWER_PARENS
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, $$, NULL, NULL);
}
| relation_factor AS relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, NULL, NULL, NULL);
}
| relation_factor use_partition AS relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, NULL, $2, NULL);
}
| relation_factor sample_clause AS relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, NULL, NULL, $2);
}
| relation_factor sample_clause seed AS relation_name
{
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $5, NULL, NULL, $2);
}
| relation_factor use_partition sample_clause AS relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $5, NULL, $2, $3);
}
| relation_factor use_partition sample_clause seed AS relation_name
{
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $6, NULL, $2, $3);
}
| relation_factor AS relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, $$, NULL, NULL);
}
| relation_factor use_partition AS relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, $$, $2, NULL);
}

| relation_factor sample_clause AS relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, $$, NULL, $2);
}
| relation_factor sample_clause seed AS relation_name index_hint_list
{
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
  merge_nodes($$, result, T_INDEX_HINT_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $5, $$, NULL, $2);
}
| relation_factor use_partition sample_clause AS relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $5, $$, $2, $3);
}
| relation_factor use_partition sample_clause seed AS relation_name index_hint_list
{
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
  merge_nodes($$, result, T_INDEX_HINT_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $6, $$, $2, $3);
}
| relation_factor relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $2, NULL, NULL, NULL);
}
| relation_factor use_partition relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, NULL, $2, NULL);
}
| relation_factor relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $2, $$, NULL, NULL);
}
| relation_factor use_partition relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, $$, $2, NULL);
}
| relation_factor sample_clause seed relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, NULL, NULL, $2);
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
}
| relation_factor use_partition sample_clause seed relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $5, NULL, $2, $3);
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
}
| relation_factor sample_clause seed relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, $$, NULL, $2);
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
}
| relation_factor use_partition sample_clause seed relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $5, $$, $2, $3);
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
}
| relation_factor sample_clause relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, NULL, NULL, $2);
}
| relation_factor use_partition sample_clause relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, NULL, $2, $3);
}
| relation_factor sample_clause relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, $$, NULL, $2);
}
| relation_factor use_partition sample_clause relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, $$, $2, $3);
}

dml_table_name:
relation_factor opt_use_partition
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 3, $1, NULL, $2);
}

seed:
SEED '(' INTNUM ')'
{
  /* USE T_SFU_XXX to avoid being parsed by plan cache as template var */
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = $3->value_;
};

sample_percent:
INTNUM
{
  /* USE T_SFU_XXX to avoid being parsed by plan cache as template var */
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = $1->value_;
}
|
DECIMAL_VAL
{
  /* USE T_SFU_XXX to avoid being parsed by plan cache as template var */
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_DECIMAL);
  $$->str_len_ = $1->str_len_;
  $$->str_value_ = $1->str_value_;
};

opt_sample_scope:
/* EMPTY */
{
  malloc_terminal_node($$, result->malloc_pool_, T_ALL);
}
| ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_ALL);
}
| BASE
{
  malloc_terminal_node($$, result->malloc_pool_, T_BASE);
}
| INCR
{
  malloc_terminal_node($$, result->malloc_pool_, T_INCR);
};

sample_clause:
SAMPLE opt_block opt_sample_scope '(' sample_percent ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SAMPLE_SCAN, 4, $2, $5, NULL, $3);
}
;

opt_block:
BLOCK
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_INT);
  $$->value_ = 2;
}
| /* empty */
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_INT);
  $$->value_ = 1;
};


table_subquery:
select_with_parens relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, $2);
}
| select_with_parens AS relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, $3);
}
;

/*
  table PARTITION(list of partitions)
*/

opt_use_partition:
use_partition
{
  $$ = $1;
}
/* empty */
| {
  $$ = NULL;
}
;

use_partition:
PARTITION '(' name_list ')'
{
  ParseNode *name_list = NULL;
  merge_nodes(name_list, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_PARTITION, 1, name_list);
}

index_hint_type:
FORCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_FORCE);
}
| IGNORE
{
  malloc_terminal_node($$, result->malloc_pool_, T_IGNORE);
}
;

opt_key_or_index:
key_or_index
{
  (void) $1;
  $$ = NULL;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

key_or_index:
KEY
{
  $$ = NULL;
}
| INDEX
{
  $$ = NULL;
}
;

/*index hint scope is unused*/
index_hint_scope:
/*empty*/
{
  $$ = NULL;
}
| FOR JOIN
{
  $$ = NULL;
}
| FOR ORDER BY
{
  $$ = NULL;
}
| FOR GROUP BY
{
  $$ = NULL;
}
;

index_element:
NAME_OB
{
  $$ = $1;
}
| PRIMARY
{
  malloc_terminal_node($$, result->malloc_pool_, T_IDENT);
  int64_t len = strlen("PRIAMRY");
  $$->str_value_ = parse_strndup("PRIMARY", len, result->malloc_pool_);
  if (OB_UNLIKELY(NULL == $$->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
  $$->str_len_ = len;
}
;

index_list:
index_element
{
  $$ = $1;
}
| index_list ',' index_element
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

opt_index_list:
index_list
{
  $$ = $1;
}
|  /*empty*/
{
  $$ = NULL;
}
;

index_hint_definition:
USE key_or_index index_hint_scope '(' opt_index_list ')'
{
  ParseNode *use_node = NULL;
  malloc_terminal_node(use_node, result->malloc_pool_, T_USE);
  (void) $2;
  (void) $3;
  ParseNode *index_list = NULL;
  merge_nodes(index_list, result, T_NAME_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_HINT_DEF, 2, use_node, index_list);
}
|
index_hint_type key_or_index index_hint_scope '(' index_list ')'
{
  (void) $2;
  (void) $3;
  ParseNode *index_list = NULL;
  merge_nodes(index_list, result, T_NAME_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_HINT_DEF, 2, $1, index_list);
}
;

index_hint_list:
index_hint_definition
{
  $$ = $1;
}
| index_hint_definition index_hint_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

/* diff normal_relation_facotr and dot_relation_factor for relation_factor_in_hint*/
relation_factor:
normal_relation_factor
{
  $$ = $1;
  store_pl_ref_object_symbol($$, result, REF_REL);
}
| dot_relation_factor
{
  $$ = $1;
  store_pl_ref_object_symbol($$, result, REF_REL);
}
;

relation_with_star_list:
relation_factor_with_star
{
  $$ = $1;
}
| relation_with_star_list ',' relation_factor_with_star
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

relation_factor_with_star:
relation_name opt_with_star
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 2, NULL, $1);
  dup_node_string($1, $$, result->malloc_pool_);
}
| relation_name '.' relation_name opt_with_star
{
  (void)($4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 2, $1, $3);
  dup_node_string($3, $$, result->malloc_pool_);
}
;

opt_with_star:
'.' '*'
{
  $$ = NULL;
}
| /*Empty*/
{
  $$ = NULL;
}
;

normal_relation_factor:
relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 2, NULL, $1);
  dup_node_string($1, $$, result->malloc_pool_);
}
| relation_name '.' relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 2, $1, $3);
  dup_node_string($3, $$, result->malloc_pool_);
}
| relation_name '.' mysql_reserved_keyword
{
  ParseNode *table_name = NULL;
  get_non_reserved_node(table_name, result->malloc_pool_, @3.first_column, @3.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 2, $1, table_name);
  dup_node_string(table_name, $$, result->malloc_pool_);
}
;

dot_relation_factor:
'.' relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 2, NULL, $2);
  dup_node_string($2, $$, result->malloc_pool_);
}
| '.' mysql_reserved_keyword
{
  ParseNode *table_name = NULL;
  get_non_reserved_node(table_name, result->malloc_pool_, @2.first_column, @2.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 2, NULL, table_name);
  dup_node_string(table_name, $$, result->malloc_pool_);
}
;

relation_factor_in_hint:
normal_relation_factor qb_name_option
{
malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR_IN_HINT, 2, $1, $2);
}
;

qb_name_option:
'@' NAME_OB
{ $$ = $2; }
| //empty
{ $$ = NULL; }
;

relation_factor_in_hint_list:
relation_factor_in_hint
{
  $$ = $1;
}
| relation_factor_in_hint_list relation_sep_option relation_factor_in_hint
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

relation_sep_option:
','
{}
| /* empty */
{}
;

relation_factor_in_pq_hint:
relation_factor_in_hint
{
  $$ = $1;
}
|
'(' relation_factor_in_hint_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR_IN_HINT_LIST, 1, $2);
}

relation_factor_in_leading_hint:
'(' relation_factor_in_hint_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR_IN_HINT_LIST, 1, $2);
}

relation_factor_in_leading_hint_list:
relation_factor_in_leading_hint
{
  $$ = $1;
}
| relation_factor_in_leading_hint_list relation_sep_option relation_factor_in_leading_hint
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| relation_factor_in_leading_hint_list relation_sep_option relation_factor_in_hint
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| '(' relation_factor_in_leading_hint_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR_IN_HINT_LIST, 1, $2);
}
| '(' relation_factor_in_hint_list relation_sep_option relation_factor_in_leading_hint_list ')'
{
  ParseNode *link_node = NULL;
  malloc_non_terminal_node(link_node, result->malloc_pool_, T_LINK_NODE, 2, $2, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR_IN_HINT_LIST, 1, link_node);
}
| relation_factor_in_leading_hint_list relation_sep_option '(' relation_factor_in_hint_list relation_sep_option relation_factor_in_leading_hint_list ')'
{
  ParseNode *link_node = NULL;
  ParseNode *link_node2 = NULL;
  malloc_non_terminal_node(link_node, result->malloc_pool_, T_LINK_NODE, 2, $4, $6);
  malloc_non_terminal_node(link_node2, result->malloc_pool_, T_RELATION_FACTOR_IN_HINT_LIST, 1, link_node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, link_node2);
}
;

relation_factor_in_leading_hint_list_entry:
relation_factor_in_leading_hint_list
{
  $$ = $1;
}
| relation_factor_in_hint_list relation_sep_option relation_factor_in_leading_hint_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

relation_factor_in_use_join_hint_list:
relation_factor_in_hint
{
  $$ = $1;
}
| '(' relation_factor_in_hint_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR_IN_HINT_LIST, 1, $2);
}
| relation_factor_in_use_join_hint_list relation_sep_option relation_factor_in_hint
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| relation_factor_in_use_join_hint_list relation_sep_option '(' relation_factor_in_hint_list ')'
{
  ParseNode *link_node = NULL;
  malloc_non_terminal_node(link_node, result->malloc_pool_, T_RELATION_FACTOR_IN_HINT_LIST, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, link_node);
}
;

tracing_num_list:
INTNUM relation_sep_option tracing_num_list
{
  ParseNode *link_node = NULL;
  malloc_non_terminal_node(link_node, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, link_node);
}
| INTNUM
{
  $$=$1;
};

join_condition:
ON expr
{
  $$ = $2;
}
| USING '(' column_list ')'
{
  merge_nodes($$, result, T_COLUMN_LIST, $3);
}
;

joined_table:
/**
 * ref: https://dev.mysql.com/doc/refman/8.0/en/join.html
 */
table_reference inner_join_type table_factor %prec LOWER_ON
{
  JOIN_MERGE_NODES($1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $2, $1, $3, NULL, NULL);
}
| table_reference inner_join_type table_factor ON expr
{
  JOIN_MERGE_NODES($1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $2, $1, $3, $5, NULL);
}
| table_reference inner_join_type table_factor USING '(' column_list ')'
{
  JOIN_MERGE_NODES($1, $3);
  ParseNode *condition_node = NULL;
  merge_nodes(condition_node, result, T_COLUMN_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $2, $1, $3, condition_node, NULL);
}
| table_reference outer_join_type table_factor join_condition
{
  JOIN_MERGE_NODES($1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $2, $1, $3, $4, NULL);
}
| table_reference natural_join_type table_factor
{
  JOIN_MERGE_NODES($1, $3);

  ParseNode *join_attr = NULL;
  malloc_terminal_node(join_attr, result->malloc_pool_, T_NATURAL_JOIN);

  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $2, $1, $3, NULL, join_attr);
}
;

natural_join_type:
NATURAL outer_join_type
{
  $$ = $2
}
| NATURAL opt_inner JOIN
{
  (void)$2;
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_INNER);
}
;

inner_join_type:
JOIN
{
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_INNER);
}
| INNER JOIN
{
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_INNER);
}
| CROSS JOIN
{
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_INNER);
}
;

opt_inner:
INNER { $$ = NULL; }
| /* EMPTY */ { $$ = NULL; }
;

outer_join_type:
FULL opt_outer JOIN
{
  /* make bison mute */
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_FULL);
}
| LEFT opt_outer JOIN
{
  /* make bison mute */
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_LEFT);
}
| RIGHT opt_outer JOIN
{
  /* make bison mute */
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_RIGHT);
}
;

opt_outer:
OUTER                    { $$ = NULL; }
| /* EMPTY */               { $$ = NULL; }
;

/*****************************************************************************
 *
 *      CREATE OUTLINE grammar
 *
 *****************************************************************************/
create_outline_stmt:
CREATE opt_replace OUTLINE relation_name ON explainable_stmt opt_outline_target
{
  ParseNode *name_node = NULL;
  ParseNode *flag_node = new_terminal_node(result->malloc_pool_, T_DEFAULT);
  flag_node->value_ = 1;

  malloc_non_terminal_node(name_node, result->malloc_pool_, T_RELATION_FACTOR, 2, NULL, $4);
  dup_node_string($4, name_node, result->malloc_pool_);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_OUTLINE, 5, $2, name_node, flag_node, $6, $7);
  dup_expr_string($6, result, @6.first_column, @6.last_column);
}
|
CREATE opt_replace OUTLINE relation_name ON STRING_VALUE USING HINT_HINT_BEGIN hint_list_with_end
{
  ParseNode *name_node = NULL;
  malloc_non_terminal_node(name_node, result->malloc_pool_, T_RELATION_FACTOR, 2, NULL, $4);
  ParseNode *flag_node = new_terminal_node(result->malloc_pool_, T_DEFAULT);
  flag_node->value_ = 2;

  if ($9 != NULL) {
    dup_expr_string($9, result, @9.first_column, @9.last_column);
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_OUTLINE, 5, $2, name_node, flag_node, $9, $6);
}
;

/*****************************************************************************
 *
 *      ALTER OUTLINE grammar
 *
 *****************************************************************************/
alter_outline_stmt:
ALTER OUTLINE relation_name ADD explainable_stmt opt_outline_target
{
  ParseNode *name_node = NULL;
  malloc_non_terminal_node(name_node, result->malloc_pool_, T_RELATION_FACTOR, 2, NULL, $3);
  dup_node_string($3, name_node, result->malloc_pool_);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_OUTLINE, 3, name_node, $5, $6);
  dup_expr_string($5, result, @5.first_column, @5.last_column);
}
;

/*****************************************************************************
 *
 *      DROP OUTLINE grammar
 *
 *****************************************************************************/
drop_outline_stmt:
DROP OUTLINE relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_OUTLINE, 1, $3);
}
;

opt_outline_target:
TO explainable_stmt
{
  $$ = $2;
  dup_expr_string($$, result, @2.first_column, @2.last_column);
}
| /*empty*/
{
$$ = NULL;
};

/*****************************************************************************
 *
 *	explain grammar
 *
 *****************************************************************************/
explain_stmt:
explain_or_desc relation_factor opt_desc_column_option
{
  (void)($1);
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_COLUMNS, 4, $$, $2, NULL, $3);
}
| explain_or_desc explainable_stmt
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPLAIN, 2, NULL, $2);
}
| explain_or_desc BASIC explainable_stmt
{
  (void)($1);
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_BASIC);
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPLAIN, 2, type_node, $3);
}
| explain_or_desc OUTLINE explainable_stmt
{
  (void)($1);
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_OUTLINE);
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPLAIN, 2, type_node, $3);
}
| explain_or_desc EXTENDED explainable_stmt
{
  (void)($1);
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_EXTENDED);
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPLAIN, 2, type_node, $3);
}
| explain_or_desc EXTENDED_NOADDR explainable_stmt
{
  (void)($1);
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_EXTENDED_NOADDR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPLAIN, 2, type_node, $3);
}
| explain_or_desc PLANREGRESS explainable_stmt
{
  (void)($1);
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_PLANREGRESS);
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPLAIN, 2, type_node, $3);
}
| explain_or_desc PARTITIONS explainable_stmt
{
  (void)($1);
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_PARTITIONS);
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPLAIN, 2, type_node, $3);
}
| explain_or_desc FORMAT COMP_EQ format_name explainable_stmt
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPLAIN, 2, $4, $5);
}
;

explain_or_desc:
EXPLAIN {$$ = NULL;}
| DESCRIBE {$$ = NULL;}
| DESC  {$$ = NULL;}
;

explainable_stmt:
select_stmt         { $$ = $1; }
| delete_stmt         { $$ = $1; }
| insert_stmt         { $$ = $1; }
| update_stmt         { $$ = $1; }
;

format_name:
TRADITIONAL
{ malloc_terminal_node($$, result->malloc_pool_, T_TRADITIONAL); }
| JSON
{ malloc_terminal_node($$, result->malloc_pool_, T_JSON); }
;


/*****************************************************************************
 *
 *	show grammar
 *
 *****************************************************************************/
show_stmt:
SHOW opt_full TABLES opt_from_or_in_database_clause opt_show_condition
{
  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = $2[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_TABLES, 3, $4, $5, value);
}
| SHOW databases_or_schemas opt_status opt_show_condition
{
  (void)$2;
  //(void)$3;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_DATABASES, 2, $4, $3);
}
| SHOW opt_full columns_or_fields from_or_in relation_factor opt_from_or_in_database_clause opt_show_condition
{
  (void)$3;
  (void)$4;
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = $2[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_COLUMNS, 4, $$, $5, $6, $7);
}
| SHOW TABLE STATUS opt_from_or_in_database_clause opt_show_condition
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_TABLE_STATUS, 2, $4, $5); }
| SHOW PROCEDURE STATUS opt_from_or_in_database_clause opt_show_condition
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_PROCEDURE_STATUS, 2, $4, $5); }
| SHOW FUNCTION STATUS opt_from_or_in_database_clause opt_show_condition
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_FUNCTION_STATUS, 2, $4, $5); }
| SHOW TRIGGERS opt_from_or_in_database_clause opt_show_condition
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_TRIGGERS, 2, $3, $4); }
| SHOW SERVER STATUS opt_show_condition
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_SERVER_STATUS, 1, $4); }
| SHOW opt_scope VARIABLES opt_show_condition
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = $2[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_VARIABLES, 2, $$, $4);
}
| SHOW SCHEMA
{ malloc_terminal_node($$, result->malloc_pool_, T_SHOW_SCHEMA); }
| SHOW CREATE database_or_schema opt_if_not_exists database_factor
{
  (void)$3;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_DATABASE, 2, $4, $5);
}
| SHOW CREATE TABLE relation_factor
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_TABLE, 1, $4); }
| SHOW CREATE VIEW relation_factor
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_VIEW, 1, $4); }
| SHOW CREATE PROCEDURE relation_factor
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_PROCEDURE, 1, $4); }
| SHOW CREATE FUNCTION relation_factor
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_FUNCTION, 1, $4); }
| SHOW WARNINGS opt_limit
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_WARNINGS, 1, $3);
}
| SHOW ERRORS opt_limit
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_ERRORS, 1, $3);
}
| SHOW COUNT '(' '*' ')' WARNINGS
{
  ParseNode *fun = NULL;
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node(fun, result->malloc_pool_, T_FUN_COUNT, 1, node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_WARNINGS, 1, fun);
}
| SHOW  COUNT '(' '*' ')' ERRORS
{
  ParseNode *fun = NULL;
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node(fun, result->malloc_pool_, T_FUN_COUNT, 1, node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_ERRORS, 1, fun);
}
| SHOW GRANTS opt_for_grant_user
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_GRANTS, 1, $3);
}
| SHOW charset_key opt_show_condition
{
  (void)$2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CHARSET, 1, $3);
}
| SHOW TRACE opt_show_condition
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_TRACE, 1, $3); }
| SHOW COLLATION opt_show_condition
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_COLLATION, 1, $3); }
/*
  | SHOW opt_all PARAMETERS opt_show_condition
  {
  if ($2 == 0)
  {
  ParseNode *column = NULL;
  ParseNode *value = NULL;
  ParseNode *sub_where = NULL;
  make_name_node(column, result->malloc_pool_, "visible_level");
  malloc_terminal_node(value, result->malloc_pool_, T_VARCHAR);
  value->str_value_ = parse_strndup("SYS", strlen("SYS") + 1, result->malloc_pool_);
  if (NULL == value->str_value_)
  {
  yyerror(NULL, result, "No more space for mallocing string\n");
  YYABORT;
  }
  value->str_len_ = strlen("SYS");
  malloc_non_terminal_node(sub_where, result->malloc_pool_, T_OP_NE, 2, column, value);
  if ($4 == NULL)
  {
  malloc_non_terminal_node($4, result->malloc_pool_, T_WHERE_CLAUSE, 1,sub_where);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_PARAMETERS, 1, $4);
  }
  else if ($4->type_ == T_WHERE_CLAUSE)
  {
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_AND, 2, $4->children_[0], sub_where);
  malloc_non_terminal_node($4, result->malloc_pool_, T_WHERE_CLAUSE, 1, $$);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_PARAMETERS, 1, $4);
  }
  else if ($4->type_ == T_LIKE_CLAUSE)
  {
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_PARAMETERS, 2, $4, sub_where);
  } else {
  yyerror(NULL, result, "invalid show parameter clause\n");
  YYABORT;
  }
  }
  else
  {
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_PARAMETERS, 1, $4);
  }
  }
*/
| SHOW PARAMETERS opt_show_condition opt_tenant_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_PARAMETERS, 2, $3, $4);
}
| SHOW index_or_indexes_or_keys from_or_in relation_factor opt_from_or_in_database_clause opt_where
{
  (void)$2;//useless
  (void)$3;//useless
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_INDEXES, 3, $4, $5, $6);
}
| SHOW opt_full PROCESSLIST
{
  ParseNode *full_node = NULL;
  malloc_terminal_node(full_node, result->malloc_pool_, T_INT);
  full_node->value_ = $2[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_PROCESSLIST, 1, full_node);
}
| SHOW TABLEGROUPS opt_show_condition
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_TABLEGROUPS, 1, $3); }
| SHOW opt_scope STATUS opt_show_condition
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = $2[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_STATUS, 2, $$, $4);
}
| SHOW TENANT opt_status
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_TENANT, 1, $3);
}
| SHOW CREATE TENANT relation_name
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_TENANT, 1, $4); }
| SHOW opt_storage ENGINES
{
  (void)$2;
  malloc_terminal_node($$, result->malloc_pool_, T_SHOW_ENGINES);
}
| SHOW PRIVILEGES
{
  malloc_terminal_node($$, result->malloc_pool_, T_SHOW_PRIVILEGES);
}
| SHOW RECYCLEBIN
{
  malloc_terminal_node($$, result->malloc_pool_, T_SHOW_RECYCLEBIN);
}
| SHOW CREATE TABLEGROUP relation_name
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_TABLEGROUP, 1, $4); }
;

databases_or_schemas:
DATABASES {$$ = NULL;}
| SCHEMAS {$$ = NULL;}
;

opt_limit:
LIMIT INTNUM ',' INTNUM
{
  if (OB_UNLIKELY($2->value_ < 0 || $4->value_ < 0)) {
    yyerror(&@1, result, "OFFSET/COUNT must not be less than 0!\n");
    YYERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_LIMIT, 2, $2, $4);
}
| LIMIT INTNUM
{
  if (OB_UNLIKELY($2->value_ < 0)) {
    yyerror(&@1, result, "COUNT must not be less than 0!\n");
    YYERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_LIMIT, 2, NULL, $2);
}
| /* EMPTY */
{ $$ = NULL; }
;

opt_for_grant_user:
opt_for_user
{ $$ = $1; }
| FOR CURRENT_USER
{ $$ = NULL; }
| FOR CURRENT_USER '(' ')'
{ $$ = NULL; }
;

opt_status:
STATUS { malloc_terminal_node($$, result->malloc_pool_, T_SHOW_STATUS);  }
| /* EMPTY */ { $$ = NULL; }
;

opt_storage:
/* EMPTY */
{ $$ = NULL; }
| STORAGE
{ $$ = NULL; }
;

opt_scope:
GLOBAL      { $$[0] = 1; }
| SESSION     { $$[0] = 2; }
| LOCAL       { $$[0] = 2; }
| /* EMPTY */ { $$[0] = 0; }
;

columns_or_fields:
COLUMNS
{ $$ = NULL; }
| FIELDS
{ $$ = NULL; }
;

database_or_schema:
DATABASE
{ $$ = NULL; }
| SCHEMA
{ $$ = NULL; }
;

index_or_indexes_or_keys:
INDEX
{ $$ = NULL; }
| INDEXES
{ $$ = NULL; }
| KEYS
{ $$ = NULL; }
;

from_or_in:
FROM
{ $$ = NULL; }
| IN
{ $$ = NULL; }
;

opt_from_or_in_database_clause:
/* EMPTY */
{ $$ = NULL; }
| from_or_in database_factor
{
  (void)$1;//useless
  malloc_non_terminal_node($$, result->malloc_pool_, T_FROM_LIST, 1, $2);
}
;

opt_show_condition:
/* EMPTY */
{ $$ = NULL; }
| LIKE STRING_VALUE
{
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_VARCHAR);
  node->str_value_ = "\\";
  node->str_len_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIKE_CLAUSE, 2, $2, node);
}
| LIKE STRING_VALUE ESCAPE STRING_VALUE
{
  if (OB_UNLIKELY(1 != $4->str_len_)) {
    yyerror(&@1, result, "Incorrect arguments to ESCAPE\n");
    YYABORT_PARSE_SQL_ERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIKE_CLAUSE, 2, $2, $4);
}
| WHERE expr
{ malloc_non_terminal_node($$, result->malloc_pool_, T_WHERE_CLAUSE, 1, $2); }
;

opt_desc_column_option:
/* EMPTY */
{ $$ = NULL; }
| STRING_VALUE
{
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_VARCHAR);
  node->str_value_ = "\\";
  node->str_len_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIKE_CLAUSE, 2, $1, node);
}
| column_name
{
  ParseNode *pattern_node = NULL;
  malloc_terminal_node(pattern_node, result->malloc_pool_, T_VARCHAR);
  dup_node_string($1, pattern_node, result->malloc_pool_);
  ParseNode *escape_node = NULL;
  malloc_terminal_node(escape_node, result->malloc_pool_, T_VARCHAR);
  escape_node->str_value_ = "\\";
  escape_node->str_len_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIKE_CLAUSE, 2, pattern_node, escape_node);
}
;

/*****************************************************************************
 *
 *	create user grammar
 *
 *****************************************************************************/
create_user_stmt:
CREATE USER opt_if_not_exists user_specification_list
{
  ParseNode *users_node = NULL;
  merge_nodes(users_node, result, T_USERS, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER, 3, $3, users_node, NULL);
}
| CREATE USER opt_if_not_exists user_specification_list require_specification
{
  ParseNode *users_node = NULL;
  merge_nodes(users_node, result, T_USERS, $4);
  ParseNode *require_node = NULL;
  merge_nodes(require_node, result, T_TLS_OPTIONS, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER, 3, $3, users_node, require_node);
}
;

user_specification_list:
user_specification
{
  $$ = $1;
}
| user_specification_list ',' user_specification
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

user_specification:
user opt_host_name
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER_SPEC, 4, $1, NULL, need_enc_node, $2);
}
| user opt_host_name IDENTIFIED BY password
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER_SPEC, 4, $1, $5, need_enc_node, $2);
}
| user opt_host_name IDENTIFIED BY PASSWORD password
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER_SPEC, 4, $1, $6, need_enc_node, $2);
}
;

require_specification:
REQUIRE NONE
{
  malloc_terminal_node($$, result->malloc_pool_, T_TLS_NONE);
}
| REQUIRE SSL
{
  malloc_terminal_node($$, result->malloc_pool_, T_TLS_SSL);
}
| REQUIRE X509
{
  malloc_terminal_node($$, result->malloc_pool_, T_TLS_XFZN);
}
| REQUIRE tls_option_list
{
  ParseNode *specified_node = NULL;
  merge_nodes(specified_node, result, T_TLS_SPECIFIED, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_TLS_SPECIFIED, 1, specified_node);
}
;

tls_option_list:
tls_option
{
  $$ = $1;
}
| tls_option_list tls_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| tls_option_list AND tls_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

tls_option:
CIPHER STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TLS_CIPHER, 1, $2);
}
| ISSUER STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TLS_ISSUER, 1, $2);
}
| SUBJECT STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TLS_SUBJECT, 1, $2);
}
;


user:
STRING_VALUE
{
  $$ = $1;
}
| NAME_OB
{
  $$ = $1;
}
| unreserved_keyword
{
  get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

opt_host_name:
USER_VARIABLE
{
  $$ = $1;
}
|
{
  $$ = NULL;
}
;

user_with_host_name:
user opt_host_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USER_WITH_HOST_NAME, 2, $1, $2);
}
;

password:
STRING_VALUE
{
  $$ = $1;
  $$->stmt_loc_.first_column_ = @1.first_column - 1;
  $$->stmt_loc_.last_column_ = @1.last_column - 1;
}
;

/*****************************************************************************
 *
 *	drop user grammar
 *
 *****************************************************************************/
drop_user_stmt:
DROP USER user_list
{
  merge_nodes($$, result, T_DROP_USER, $3);
}
;

user_list:
user_with_host_name
{
  $$ = $1;
}
| user_list ',' user_with_host_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

/*****************************************************************************
 *
 *	set password grammar
 *
 *****************************************************************************/
set_password_stmt:
SET PASSWORD opt_for_user COMP_EQ STRING_VALUE
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 4, $3, $5, need_enc_node, NULL);
}
| SET PASSWORD opt_for_user COMP_EQ PASSWORD '(' password ')'
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 4, $3, $7, need_enc_node, NULL);
}
| ALTER USER user_with_host_name IDENTIFIED BY password
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 4, $3, $6, need_enc_node, NULL);
}
| ALTER USER user_with_host_name require_specification
{
  ParseNode *require_node = NULL;
  merge_nodes(require_node, result, T_TLS_OPTIONS, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 4, $3, NULL, NULL, require_node);
}
;

opt_for_user:
FOR user opt_host_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USER_WITH_HOST_NAME, 2, $2, $3);

}
| /**/
{
  $$ = NULL;
}
;
/*****************************************************************************
 *
 *	rename user grammar
 *
 *****************************************************************************/
rename_user_stmt:
RENAME USER rename_list
{
  merge_nodes($$, result, T_RENAME_USER, $3);
}
;

rename_info:
user opt_host_name TO user opt_host_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RENAME_INFO, 4, $1, $2, $4, $5);
}
;

rename_list:
rename_info
{
  $$ = $1;
}
| rename_list ',' rename_info
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

/*****************************************************************************
 *
 *	lock user grammar
 *
 *****************************************************************************/
lock_user_stmt:
ALTER USER user_list ACCOUNT lock_spec_mysql57
{
  ParseNode *users_node = NULL;
  merge_nodes(users_node, result, T_USERS, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOCK_USER, 2, users_node, $5);
}
;

lock_spec_mysql57:
LOCK_
{
  malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
  $$->value_ = 1;
}
| UNLOCK
{
  malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
  $$->value_ = 0;
}
;

/*****************************************************************************
 *
 *	lock tables grammar
 *
 *****************************************************************************/


lock_tables_stmt:
LOCK_ table_or_tables lock_table_list
{
  (void)$2;
  (void)$3;
  malloc_terminal_node($$, result->malloc_pool_, T_EMPTY_QUERY);
}
;

unlock_tables_stmt:
UNLOCK TABLES
{
  malloc_terminal_node($$, result->malloc_pool_, T_EMPTY_QUERY);
}
;

lock_table_list:
lock_table
{
  (void)$$;
  (void)$1;
}
| lock_table_list ',' lock_table
{
  (void)$$;
  (void)$1;
  (void)$3;
}
;

lock_table:
relation_factor lock_type
{
  (void)$$;
  (void)$1;
  (void)$2;
}
|
relation_factor opt_as relation_name lock_type
{
  (void)$$;
  (void)$1;
  (void)$2;
  (void)$3;
  (void)$4;
}
;

lock_type:
READ opt_local
{
  (void)$$;
  (void)$2;
}
|
WRITE
{
  (void)$$;
}
|
LOW_PRIORITY WRITE
{
  (void)$$;
}
;

opt_local:
LOCAL {$$ = NULL;}
| {$$ = NULL;} /*emtpy*/
;

/*****************************************************************************
 *
 *  begin/start transaction grammer
 *
 ******************************************************************************/

opt_work:
WORK
{
  (void)$$;
}
| /*empty*/
{
  (void)$$;
}
;

opt_with_consistent_snapshot:
WITH CONSISTENT SNAPSHOT
{
  $$[0] = OB_WITH_CONSTISTENT_SNAPSHOT;
}
| transaction_access_mode
{
  $$[0] = $1->value_;
}
| WITH CONSISTENT SNAPSHOT ',' transaction_access_mode
{
  $$[0] = OB_WITH_CONSTISTENT_SNAPSHOT | $5->value_;
}
| transaction_access_mode ',' WITH CONSISTENT SNAPSHOT
{
  $$[0] = OB_WITH_CONSTISTENT_SNAPSHOT | $1->value_;
}
| /*empty*/
    {
      $$[0] = 0;
    }
;

begin_stmt:
BEGI opt_work
{
  (void)$2;
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BEGIN, 1, $$);
}
| START TRANSACTION opt_with_consistent_snapshot
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = $3[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_BEGIN, 1, $$);
}
;

/*****************************************************************************
 *
 *  xa transaction grammar
 *
 ******************************************************************************/

xa_begin_stmt:
XA START STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_XA_START, 1, $3);
}
| XA BEGI STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_XA_START, 1, $3);
}
;

xa_end_stmt:
XA END STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_XA_END, 1, $3);
}
;

xa_prepare_stmt:
XA PREPARE STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_XA_PREPARE, 1, $3);
}
;

xa_commit_stmt:
XA COMMIT STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_XA_COMMIT, 1, $3);
}
;

xa_rollback_stmt:
XA ROLLBACK STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_XA_ROLLBACK, 1, $3);
}
;

/*****************************************************************************
 *
 *  commit grammer
 *
 ******************************************************************************/
commit_stmt:
COMMIT opt_work
{
  (void)$2;
  malloc_terminal_node($$, result->malloc_pool_, T_COMMIT);
}
;

/*****************************************************************************
 *
 *  rollback grammer
 *
 ******************************************************************************/
rollback_stmt:
ROLLBACK opt_work
{
  (void)$2;
  malloc_terminal_node($$, result->malloc_pool_, T_ROLLBACK);
}
;

/*****************************************************************************
 *
 *  kill grammer
 *
 ******************************************************************************/
kill_stmt:
KILL expr
{
  ParseNode *opt_node = NULL;
  malloc_terminal_node(opt_node, result->malloc_pool_, T_BOOL);
  opt_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_KILL, 2, opt_node, $2);
}
|
KILL CONNECTION expr
{
  ParseNode *opt_node = NULL;
  malloc_terminal_node(opt_node, result->malloc_pool_, T_BOOL);
  opt_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_KILL, 2, opt_node, $3);
}
|
KILL QUERY expr
{
  ParseNode *opt_node = NULL;
  malloc_terminal_node(opt_node, result->malloc_pool_, T_BOOL);
  opt_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_KILL, 2, opt_node, $3);
}
;

/*****************************************************************************
 *
 *	grant grammar
 *
 *****************************************************************************/
grant_stmt:
GRANT grant_privileges ON priv_level TO user_specification_list grant_options
{
  ParseNode *privileges_list_node = NULL;
  ParseNode *privileges_node = NULL;
  ParseNode *users_node = NULL;
  malloc_non_terminal_node(privileges_list_node, result->malloc_pool_,
                           T_LINK_NODE, 2, $2, $7);
  merge_nodes(privileges_node, result, T_PRIVILEGES, privileges_list_node);
  merge_nodes(users_node, result, T_USERS, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_GRANT,
                           3, privileges_node, $4, users_node);
}
;

grant_privileges:
priv_type_list
{
  $$ = $1;
}
| ALL opt_privilege
{
  (void)$2;                 /* useless */
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_ALL;
}
;

priv_type_list:
priv_type
{
  $$ = $1;
}
| priv_type_list ',' priv_type
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

priv_type:
ALTER
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_ALTER;
}
| CREATE
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE;
}
| CREATE USER
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE_USER;
}
| DELETE
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_DELETE;
}
| DROP
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_DROP;
}
| GRANT OPTION
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_GRANT;
}
| INSERT
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_INSERT;
}
| UPDATE
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_UPDATE;
}
| SELECT
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_SELECT;
}
| INDEX
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_INDEX;
}
| CREATE VIEW
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE_VIEW;
}
| SHOW VIEW
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_SHOW_VIEW;
}
| SHOW DATABASES
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_SHOW_DB;
}
| SUPER
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_SUPER;
}
| PROCESS
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_PROCESS;
}
| USAGE
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = 0;
}
| CREATE SYNONYM
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE_SYNONYM;
}
| FILEX
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_FILE;
}
| ALTER TENANT
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_ALTER_TENANT;
}
| ALTER SYSTEM
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_ALTER_SYSTEM;
}
| CREATE RESOURCE POOL
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE_RESOURCE_POOL;
}
| CREATE RESOURCE UNIT
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE_RESOURCE_UNIT;
}


;

opt_privilege:
PRIVILEGES
{
  $$ = NULL;
}
| /*empty*/
{
  $$ = NULL;
}
;

priv_level:
'*'
{
  /* means global priv_level */
  malloc_terminal_node($$, result->malloc_pool_, T_STAR);
}
| '*' '.' '*'
{
  ParseNode *first_node = NULL;
  ParseNode *snd_node = NULL;
  malloc_terminal_node(first_node, result->malloc_pool_, T_STAR);
  malloc_terminal_node(snd_node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIV_LEVEL, 2, first_node, snd_node);
}
| relation_name '.' '*'
{
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIV_LEVEL, 2, $1, node);
}
| relation_name
{
  $$ = $1;
}
| relation_name '.' relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIV_LEVEL, 2, $1, $3);
}
;

grant_options:
WITH GRANT OPTION
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_GRANT;
}
| /*empty*/
{
  $$ = NULL;
}
;

/*****************************************************************************
 *
 *	revoke grammar
 *
 *****************************************************************************/
revoke_stmt:
REVOKE grant_privileges ON priv_level FROM user_list
{
  ParseNode *privileges_node = NULL;
  ParseNode *users_node = NULL;
  merge_nodes(privileges_node, result, T_PRIVILEGES, $2);
  merge_nodes(users_node, result, T_USERS, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_REVOKE,
                           3, privileges_node, $4, users_node);
}
| REVOKE ALL opt_privilege ',' GRANT OPTION FROM user_list
{
  (void)$3;//useless
  ParseNode *users_node = NULL;
  merge_nodes(users_node, result, T_USERS, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_REVOKE_ALL,
                           1, users_node);
}
;

/*****************************************************************************
 *
 *	prepare grammar
 *
 *****************************************************************************/
prepare_stmt:
/* PREPARE stmt_name FROM '"' preparable_stmt '"' */
PREPARE stmt_name FROM preparable_stmt
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PREPARE, 2, $2, $4);
//  dup_expr_string($4, result, @4.first_column, @4.last_column);
}
;

stmt_name:
column_label
{ $$ = $1; }
;

preparable_stmt:
text_string
{
  $$ = $1;
}
| USER_VARIABLE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GET_USER_VAR, 1, $1);
}
;


/*****************************************************************************
 *
 *	set grammar
 *
 *****************************************************************************/
variable_set_stmt:
SET var_and_val_list
{
  merge_nodes($$, result, T_VARIABLE_SET, $2);
  //$$->value_ = 2; //useless
}
;

sys_var_and_val_list:
sys_var_and_val
{
  $1->value_ = 1;//set global
  $$ = $1;
}
| sys_var_and_val_list ',' sys_var_and_val
{
  $3->value_ = 1;//set global
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
};

var_and_val_list:
var_and_val
{
  $$ = $1;
}
| var_and_val_list ',' var_and_val
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

set_expr_or_default:
expr
{
  if ($1 != NULL && $1->type_ == T_COLUMN_REF && $1->num_child_ == 3 &&
      $1->children_[0] == NULL && $1->children_[1] == NULL && $1->children_[2] != NULL) {
      ParseNode *obj_node = $1->children_[2];
    if (nodename_equal(obj_node, "OFF", 3)) {
      malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
      $$->str_value_ = "OFF";
      $$->str_len_ = 3;
    } else {
      $$ = $1;
    }
  } else {
    $$ = $1;
  }
}
| ON
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "ON";
  $$->str_len_ = 2;
}
/*
| OFF
  {
    malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
    $$->str_value_ = "OFF";
    $$->str_len_ = 3;
  }
*/
| BINARY
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "BINARY";
  $$->str_len_ = 6;
}
| DEFAULT
{
  //$$ = NULL;
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT);
}
;

var_and_val:
USER_VARIABLE to_or_eq expr
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
  $$->value_ = 2;
}
| USER_VARIABLE SET_VAR expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
  $$->value_ = 2;
}
| sys_var_and_val
{
  $$ = $1;
}
| scope_or_scope_alias column_name to_or_eq set_expr_or_default
{
  (void)($3);
  $2->type_ = T_SYSTEM_VARIABLE;
  if (NULL != $4 && T_COLUMN_REF == $4->type_ && NULL != $4->children_ && NULL == $4->children_[0] && NULL == $4->children_[1] && NULL != $4->children_[2]) {
    $4->type_ = T_VARCHAR;
    $4->str_value_ = $4->children_[2]->str_value_;
    $4->str_len_ = $4->children_[2]->str_len_;
    $4->num_child_ = 0;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $2, $4);
  $$->value_ = $1[0];
}
| SYSTEM_VARIABLE to_or_eq set_expr_or_default
{
  (void)($2);
  if (NULL != $3 && T_COLUMN_REF == $3->type_  && NULL != $3->children_ && NULL == $3->children_[0] && NULL == $3->children_[1] && NULL != $3->children_[2]) {
    $3->type_ = T_VARCHAR;
    $3->str_value_ = $3->children_[2]->str_value_;
    $3->str_len_ = $3->children_[2]->str_len_;
    $3->num_child_ = 0;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
  $$->value_ = 2;
}
;

sys_var_and_val:
var_name to_or_eq set_expr_or_default
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
  $$->value_ = 2;
}
| var_name SET_VAR set_expr_or_default
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
  $$->value_ = 2;
}
;

scope_or_scope_alias:
GLOBAL %prec LOWER_PARENS      { $$[0] = 1; }
| SESSION  %prec LOWER_PARENS  { $$[0] = 2; }
| GLOBAL_ALIAS '.'  { $$[0] = 1; }
| SESSION_ALIAS '.' { $$[0] = 2; }
;

to_or_eq:
TO      { $$ = NULL; }
| COMP_EQ { $$ = NULL; }
;

/*****************************************************************************
 *
 *	execute grammar
 *
 *****************************************************************************/
execute_stmt:
EXECUTE stmt_name opt_using_args
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXECUTE, 2, $2, $3);
}
;

opt_using_args:
USING argument_list
{
  merge_nodes($$, result, T_ARGUMENT_LIST, $2);
}
| /*empty*/
{
  $$ = NULL;
}
;

argument_list:
argument
{
  $$ = $1;
}
| argument_list ',' argument
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

argument:
USER_VARIABLE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GET_USER_VAR, 1, $1);
}
;

/*****************************************************************************
 *
 *	DEALLOCATE grammar
 *
 *****************************************************************************/
deallocate_prepare_stmt:
deallocate_or_drop PREPARE stmt_name
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DEALLOCATE, 1, $3);
}
;

deallocate_or_drop:
DEALLOCATE
{ $$ = NULL; }
| DROP
{ $$ = NULL; }
;

/*****************************************************************************
 *
 *	TRUNCATE TABLE grammar
 *
 *****************************************************************************/

truncate_table_stmt:
TRUNCATE opt_table relation_factor
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRUNCATE_TABLE, 1, $3);
}
;

/*****************************************************************************
 *
 *	RENAME TABLE grammar
 *
 *****************************************************************************/

rename_table_stmt:
RENAME TABLE rename_table_actions
{
  //ParseNode *rename_table_actions = NULL;
  merge_nodes($$, result, T_RENAME_TABLE, $3);
  //malloc_non_terminal_node($$, result->malloc_pool_, T_RENAME_TABLE, 1, rename_table_actions);
}
;

rename_table_actions:
rename_table_action
{
  $$ = $1;
}
|  rename_table_actions ',' rename_table_action
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

rename_table_action:
relation_factor TO relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RENAME_TABLE_ACTION, 2, $1, $3);
}
;

/*****************************************************************************
 *
 *	ALTER TABLE grammar
 *
 *****************************************************************************/
alter_table_stmt:
ALTER TABLE relation_factor alter_table_actions
{
  ParseNode *table_actions = NULL;
  merge_nodes(table_actions, result, T_ALTER_TABLE_ACTION_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLE, 2, $3, table_actions);
  $$->value_ = 0;
}
;

alter_table_actions:
alter_table_action
{
  $$ = $1;
}
| alter_table_actions ',' alter_table_action
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
|
{ $$ = NULL; }
;

alter_table_action:
opt_set table_option_list_space_seperated
{
  (void)$1;
  merge_nodes($$, result, T_TABLE_OPTION_LIST, $2);
}
| alter_column_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_COLUMN_OPTION, 1, $1);
}
| alter_tablegroup_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLEGROUP_OPTION, 1, $1);
}
| RENAME opt_to relation_factor
{
  (void)($2);
  ParseNode *rename_node = NULL;
  malloc_non_terminal_node(rename_node, result->malloc_pool_, T_TABLE_RENAME, 1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLE_OPTION, 1, rename_node);
}
| alter_index_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_INDEX_OPTION, 1, $1);
}
| alter_partition_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_OPTION, 1, $1);
}
| alter_constraint_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_CHECK_CONSTRAINT_OPTION, 1, $1);
}
| alter_foreign_key_action
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_FOREIGN_KEY_OPTION, 1, $1);
}
/*  | ORDER BY column_list
//    {
//      ParseNode *col_list = NULL;
//      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, $3);
//      malloc_non_terminal_node($$, result->malloc_pool_, T_ORDER_BY, 1, col_list);
//    }*/
;

alter_constraint_option:
DROP CONSTRAINT '(' name_list ')'
{
  merge_nodes($$, result, T_NAME_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 1, $$);
  $$->value_ = 0;
}
|
ADD CONSTRAINT constraint_name CHECK '(' expr ')'
{
  dup_expr_string($6, result, @6.first_column, @6.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 2, $3, $6);
  $$->value_ = 1;
}
;

alter_partition_option:
DROP PARTITION drop_partition_name_list
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_DROP, 2, $$, NULL);
}
|
DROP SUBPARTITION drop_partition_name_list
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SUBPARTITION_DROP, 2, $$, NULL);
}
|
ADD PARTITION opt_partition_range_or_list
{
  merge_nodes($$, result, T_PARTITION_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_ADD, 1, $$);
  dup_string($$, result, @3.first_column, @3.last_column);
}
| modify_partition_info
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_PARTITIONED, 1, $1);
}
| REORGANIZE PARTITION name_list INTO opt_partition_range_or_list
{
  ParseNode *partition_names = NULL;
  merge_nodes(partition_names, result, T_NAME_LIST, $3);
  ParseNode *partition_node = NULL;
  merge_nodes(partition_node, result, T_PARTITION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_REORGANIZE, 2, partition_node, partition_names);
}
| TRUNCATE PARTITION name_list %prec LOWER_COMMA
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_TRUNCATE, 2, $$, NULL);
}
| TRUNCATE SUBPARTITION name_list %prec LOWER_COMMA
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SUBPARTITION_TRUNCATE, 2, $$, NULL);
}
;

opt_partition_range_or_list:
opt_range_partition_list
{
  $$ = $1;
}
|
opt_list_partition_list
{
  $$ = $1;
}
;
alter_tg_partition_option:
DROP PARTITION drop_partition_name_list
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_DROP, 1, $$);
}
|
ADD PARTITION opt_partition_range_or_list
{
  merge_nodes($$, result, T_PARTITION_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_ADD, 1, $$);
  dup_string($$, result, @3.first_column, @3.last_column);
}
| modify_tg_partition_info
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_PARTITIONED, 1, $1);
}
| REORGANIZE PARTITION name_list INTO opt_partition_range_or_list
{
  ParseNode *partition_names = NULL;
  merge_nodes(partition_names, result, T_NAME_LIST, $3);
  ParseNode *partition_node = NULL;
  merge_nodes(partition_node, result, T_PARTITION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_REORGANIZE, 2, partition_node, partition_names);
}
| TRUNCATE PARTITION name_list %prec LOWER_COMMA
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_TRUNCATE, 1, $$);
}
;

drop_partition_name_list:
name_list %prec LOWER_COMMA
{
  $$ = $1;
}
|
'(' name_list ')'
{
  $$ = $2;
}
;

modify_partition_info:
hash_partition_option
{
  $$ = $1;
}
| key_partition_option
{
  $$ = $1;
}
| range_partition_option
{
  $$ = $1;
}
| list_partition_option
{
  $$ = $1;
}
;

modify_tg_partition_info:
tg_hash_partition_option
{
  $$ = $1;
}
| tg_key_partition_option
{
  $$ = $1;
}
| tg_range_partition_option
{
  $$ = $1;
}
| tg_list_partition_option
{
  $$ = $1;
}
;


alter_index_option:
ADD key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list
{
  (void)($2);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $6);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_ADD, 4, $3, col_list, index_option, $4);
  $$->value_ = 0;
}
| ADD UNIQUE opt_key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list
{
  (void)($3);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $7);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $9);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_ADD, 4, $4, col_list, index_option, $5);
  $$->value_ = 1;
}
| ADD CONSTRAINT opt_constraint_name UNIQUE opt_key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list
{
  (void)($5);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $9);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $11);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_ADD, 4, $6 ? $6 : $3, col_list, index_option, $7);
  $$->value_ = 1;
}
| DROP key_or_index index_name
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_DROP, 1, $3);
}
| ADD opt_constraint PRIMARY KEY '(' column_name_list ')' opt_index_option_list
{
  (void)($2);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_COLUMN_LIST, $6);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_KEY, 2, col_list, index_option);
}
| ALTER INDEX index_name visibility_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_ALTER, 2, $3, $4);
}
| RENAME key_or_index index_name TO index_name
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_RENAME, 2, $3, $5);
}
| ALTER INDEX index_name parallel_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_ALTER_PARALLEL, 2, $3, $4);
}
;

alter_foreign_key_action:
DROP FOREIGN KEY index_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOREIGN_KEY_DROP, 1, $4);
}
| ADD opt_constraint FOREIGN KEY opt_index_name '(' column_name_list ')' REFERENCES relation_factor '(' column_name_list ')' opt_match_option opt_reference_option_list
{
  ParseNode *child_col_list= NULL;
  ParseNode *parent_col_list= NULL;
  ParseNode *reference_option_list = NULL;
  merge_nodes(child_col_list, result, T_COLUMN_LIST, $7);
  merge_nodes(parent_col_list, result, T_COLUMN_LIST, $12);
  merge_nodes(reference_option_list, result, T_REFERENCE_OPTION_LIST, $15);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOREIGN_KEY, 7, child_col_list, $10, parent_col_list, reference_option_list, $2, $5, $14);
}
;

visibility_option:
VISIBLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_VISIBLE);
}
| INVISIBLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INVISIBLE);
}
;

alter_column_option:
ADD opt_column column_definition
{
  (void)($2); /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ADD, 1, $3);
}
| ADD opt_column '(' column_definition_list ')'
{
  (void)($2); /* make bison mute */
  merge_nodes($$, result, T_COLUMN_ADD, $4);
}
| DROP column_definition_ref opt_drop_behavior
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DROP, 1, $2);
  $$->value_ = $3[0];
}
| DROP COLUMN column_definition_ref opt_drop_behavior
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DROP, 1, $3);
  $$->value_ = $4[0];
}
| ALTER opt_column column_definition_ref alter_column_behavior
{
  (void)($2); /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ALTER, 2, $3, $4);
}
| CHANGE opt_column column_definition_ref column_definition
{
  (void)($2); /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_CHANGE, 2, $3, $4 );
}
| MODIFY opt_column column_definition
{
  (void)($2); /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_MODIFY, 1, $3);
}
/* we don't have table constraint, so ignore it */
;

opt_position_column:
{
  $$ = NULL;
}
| FIRST
{
  malloc_terminal_node($$, result->malloc_pool_, T_COLUMN_ADD_FIRST);
}
| BEFORE column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ADD_BEFORE, 1, $2);
}
| AFTER column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ADD_AFTER, 1, $2);
}
;

alter_tablegroup_option:
DROP TABLEGROUP
{
  malloc_terminal_node($$, result->malloc_pool_, T_TABLEGROUP_DROP);
}
;

opt_column:
COLUMN        { $$ = NULL; }
| /*EMPTY*/   { $$ = NULL; }
;

opt_to:
TO           { $$ = NULL; }
| /*EMPTY*/  { $$ = NULL; }
;

opt_set:
SET          { $$ = NULL; }
| /*EMPTY*/  { $$ = NULL; }
;

opt_drop_behavior:
CASCADE     { $$[0] = 2; }
| RESTRICT  { $$[0] = 1; }
| /*EMPTY*/ { $$[0] = 0; }
;

alter_column_behavior:
/*
  SET NOT NULLX
  {
  (void)($3); // make bison mute
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NOT_NULL);
  }
  | DROP NOT NULLX
  {
  (void)($3); // make bison mute
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NULL);
  }*/
SET DEFAULT signed_literal
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_DEFAULT, 1, $3);
}
| DROP DEFAULT
{
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NULL);
}
;

/*****************************************************************************
 *
 *	RECYCLE grammar
 *
 *****************************************************************************/

purge_stmt:
PURGE TABLE relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PURGE_TABLE, 1, $3);
}
|
PURGE INDEX relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PURGE_INDEX, 1, $3);
}
|
PURGE database_key database_factor
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PURGE_DATABASE, 1, $3);
}
|
PURGE TENANT relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PURGE_TENANT, 1, $3);
}
|
PURGE RECYCLEBIN
{
  malloc_terminal_node($$, result->malloc_pool_, T_PURGE_RECYCLEBIN);
}


optimize_stmt:
OPTIMIZE TABLE table_list
{
  ParseNode *tables = NULL;
  merge_nodes(tables, result, T_TABLE_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OPTIMIZE_TABLE, 1, tables);
}
|
OPTIMIZE TENANT relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OPTIMIZE_TENANT, 1, $3);
}
|
OPTIMIZE TENANT ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_OPTIMIZE_ALL);
}

dump_memory_stmt:
DUMP ENTITY ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_TEMPORARY);
  $$->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_DUMP_MEMORY, 1, $$);
}
|
DUMP ENTITY P_ENTITY COMP_EQ STRING_VALUE ',' SLOT_IDX COMP_EQ INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TEMPORARY, 2, $5, $9);
  $$->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_DUMP_MEMORY, 1, $$);
}
|
DUMP CHUNK ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_TEMPORARY);
  $$->value_ = 2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_DUMP_MEMORY, 1, $$);
}
|
DUMP CHUNK TENANT_ID COMP_EQ INTNUM ',' CTX_ID COMP_EQ INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TEMPORARY, 2, $5, $9);
  $$->value_ = 3;
  malloc_non_terminal_node($$, result->malloc_pool_, T_DUMP_MEMORY, 1, $$);
}
|
DUMP CHUNK P_CHUNK COMP_EQ STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TEMPORARY, 1, $5);
  $$->value_ = 4;
  malloc_non_terminal_node($$, result->malloc_pool_, T_DUMP_MEMORY, 1, $$);
}
|
SET OPTION LEAK_MOD COMP_EQ STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TEMPORARY, 1, $5);
  $$->value_ = 5;
  malloc_non_terminal_node($$, result->malloc_pool_, T_DUMP_MEMORY, 1, $$);
}
|
SET OPTION LEAK_RATE COMP_EQ INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TEMPORARY, 1, $5);
  $$->value_ = 6;
  malloc_non_terminal_node($$, result->malloc_pool_, T_DUMP_MEMORY, 1, $$);
}
|
DUMP MEMORY LEAK
{
  malloc_terminal_node($$, result->malloc_pool_, T_TEMPORARY);
  $$->value_ = 7;
  malloc_non_terminal_node($$, result->malloc_pool_, T_DUMP_MEMORY, 1, $$);
}

/*****************************************************************************
 *
 *	ALTER SYSTEM grammar
 *
 *****************************************************************************/
alter_system_stmt:
ALTER SYSTEM BOOTSTRAP server_info_list
{
  ParseNode *server_list = NULL;
  merge_nodes(server_list, result, T_SERVER_INFO_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_BOOTSTRAP, 3, server_list, NULL, NULL);
}
|
ALTER SYSTEM FLUSH cache_type CACHE opt_tenant_list flush_scope
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FLUSH_CACHE, 3, $4, $6, $7);
}
|
ALTER SYSTEM FLUSH SQL cache_type opt_tenant_list flush_scope
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FLUSH_CACHE, 3, $5, $6, $7);
}
|
ALTER SYSTEM FLUSH KVCACHE opt_tenant_name opt_cache_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FLUSH_KVCACHE, 2, $5, $6);
}
|
ALTER SYSTEM FLUSH DAG WARNINGS
{
  malloc_terminal_node($$, result->malloc_pool_, T_FLUSH_DAG_WARNINGS);
}
|
ALTER SYSTEM FLUSH ILOGCACHE opt_file_id
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FLUSH_ILOGCACHE, 1, $5);
}
|
ALTER SYSTEM ALTER PLAN BASELINE opt_tenant_name opt_sql_id opt_baseline_id SET baseline_asgn_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_BASELINE, 4, $6, $7, $8, $10);
}
|
ALTER SYSTEM LOAD PLAN BASELINE FROM PLAN CACHE opt_tenant_list opt_sql_id
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOAD_BASELINE, 2, $9, $10);
}
|
ALTER SYSTEM SWITCH REPLICA partition_role partition_id_or_server_or_zone
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SWITCH_REPLICA_ROLE, 2, $5, $6);
}
|
ALTER SYSTEM SWITCH ROOTSERVICE partition_role server_or_zone
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SWITCH_RS_ROLE, 2, $5, $6);
}
|
ALTER SYSTEM alter_or_change_or_modify REPLICA partition_id_desc ip_port alter_or_change_or_modify change_actions opt_force
{
  (void)($3);
  (void)($7);
  ParseNode *change_actions = NULL;
  merge_nodes(change_actions, result, T_CHANGE_LIST, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHANGE_REPLICA, 4, $5, $6, change_actions, $9);
}
|
ALTER SYSTEM DROP REPLICA partition_id_desc ip_port opt_create_timestamp opt_zone_desc opt_force
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_REPLICA, 5, $5, $6, $7, $8, $9);
}
|
ALTER SYSTEM migrate_action REPLICA partition_id_desc SOURCE opt_equal_mark STRING_VALUE DESTINATION opt_equal_mark STRING_VALUE opt_force
{
  (void)($7);
  (void)($10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_MIGRATE_REPLICA, 5, $3, $5, $8, $11, $12);
}
| ALTER SYSTEM REPORT REPLICA opt_server_or_zone
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_REPORT_REPLICA, 1, $5);
}
|
ALTER SYSTEM RECYCLE REPLICA opt_server_or_zone
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RECYCLE_REPLICA, 1, $5);
}
|
ALTER SYSTEM START MERGE zone_desc
{
  ParseNode *start = NULL;
  malloc_terminal_node(start, result->malloc_pool_, T_INT);
  start->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE_CONTROL, 2, start, $5);
}
|
ALTER SYSTEM suspend_or_resume MERGE opt_zone_desc
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE_CONTROL, 2, $3, $5);
}
|
ALTER SYSTEM suspend_or_resume RECOVERY opt_zone_desc
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RECOVERY_CONTROL, 2, $3, $5);
}
|
ALTER SYSTEM CLEAR MERGE ERROR_P
{
  malloc_terminal_node($$, result->malloc_pool_, T_CLEAR_MERGE_ERROR);
}
|
ALTER SYSTEM CANCEL cancel_task_type TASK STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CANCEL_TASK, 2, $4, $6);
}
|
ALTER SYSTEM MAJOR FREEZE opt_ignore_server_list
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_FREEZE, 2, type, $5);
}
|
ALTER SYSTEM CHECKPOINT
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_FREEZE, 2, type, NULL);
}
|
ALTER SYSTEM MINOR FREEZE opt_tenant_list_or_partition_id_desc opt_server_list opt_zone_desc
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_FREEZE, 4, type, $5, $6, $7);
}
|
ALTER SYSTEM CLEAR ROOTTABLE opt_tenant_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CLEAR_ROOT_TABLE, 1, $5);
}
|
ALTER SYSTEM server_action SERVER server_list opt_zone_desc
{
  ParseNode *server_list = NULL;
  merge_nodes(server_list, result, T_SERVER_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ADMIN_SERVER, 3, $3, server_list, $6);
}
|
ALTER SYSTEM ADD ZONE relation_name_or_string add_or_alter_zone_options
{
  ParseNode *zone_action = NULL;
  malloc_terminal_node(zone_action, result->malloc_pool_, T_INT);
  ParseNode *zone_options = NULL;
  merge_nodes(zone_options, result, T_LINK_NODE, $6);
  zone_action->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ADMIN_ZONE, 3, zone_action, $5, zone_options);
}
|
ALTER SYSTEM zone_action ZONE relation_name_or_string
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ADMIN_ZONE, 3, $3, $5, NULL);
}
|
ALTER SYSTEM alter_or_change_or_modify ZONE relation_name_or_string opt_set add_or_alter_zone_options
{
  (void)($3);
  (void)($6);
  ParseNode *zone_action = NULL;
  malloc_terminal_node(zone_action, result->malloc_pool_, T_INT);
  ParseNode *zone_options = NULL;
  merge_nodes(zone_options, result, T_LINK_NODE, $7);
  zone_action->value_ = 5;      /* 1:add,2:delete,3:start,4:stop,5:modify,6:force stop */
  malloc_non_terminal_node($$, result->malloc_pool_, T_ADMIN_ZONE, 3, zone_action, $5, zone_options);
}
|
ALTER SYSTEM REFRESH SCHEMA opt_server_or_zone
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_REFRESH_SCHEMA, 1, $5);
}
|
ALTER SYSTEM REFRESH MEMORY STAT opt_server_or_zone
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_REFRESH_MEMORY_STAT, 1, $6);
}
|
ALTER SYSTEM opt_set alter_system_set_parameter_actions
{
  (void)$3;
  merge_nodes($$, result, T_SYTEM_ACTION_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SYSTEM_SET_PARAMETER, 1, $$);
}
|
ALTER SYSTEM SET_TP alter_system_settp_actions
{
  merge_nodes($$, result, T_SYTEM_SETTP_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SYSTEM_SETTP, 1, $$);
}
|
ALTER SYSTEM CLEAR LOCATION CACHE opt_server_or_zone
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CLEAR_LOCATION_CACHE, 1, $6);
}
|
ALTER SYSTEM REMOVE BALANCE TASK opt_tenant_list opt_zone_list opt_balance_task_type
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CLEAR_BALANCE_TASK, 3, $6, $7, $8);
}
|
ALTER SYSTEM RELOAD GTS
{
  malloc_terminal_node($$, result->malloc_pool_, T_RELOAD_GTS);
}
|
ALTER SYSTEM RELOAD UNIT
{
  malloc_terminal_node($$, result->malloc_pool_, T_RELOAD_UNIT);
}
|
ALTER SYSTEM RELOAD SERVER
{
  malloc_terminal_node($$, result->malloc_pool_, T_RELOAD_SERVER);
}
|
ALTER SYSTEM RELOAD ZONE
{
  malloc_terminal_node($$, result->malloc_pool_, T_RELOAD_ZONE);
}
|
ALTER SYSTEM MIGRATE UNIT opt_equal_mark INTNUM DESTINATION opt_equal_mark STRING_VALUE
{
  (void)($5);
  (void)($8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_MIGRATE_UNIT, 2, $6, $9);
}
|
ALTER SYSTEM CANCEL MIGRATE UNIT INTNUM
{
   malloc_non_terminal_node($$, result->malloc_pool_, T_MIGRATE_UNIT, 2, $6, NULL);
}
|
ALTER SYSTEM UPGRADE VIRTUAL SCHEMA
{
  malloc_terminal_node($$, result->malloc_pool_, T_UPGRADE_VIRTUAL_SCHEMA);
}
|
ALTER SYSTEM RUN JOB STRING_VALUE opt_server_or_zone
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RUN_JOB, 2, $5, $6);
}
|
ALTER SYSTEM upgrade_action UPGRADE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ADMIN_UPGRADE_CMD, 1, $3);
}
|
ALTER SYSTEM RUN UPGRADE JOB STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ADMIN_RUN_UPGRADE_JOB, 1, $6);
}
|
ALTER SYSTEM STOP UPGRADE JOB
{
  malloc_terminal_node($$, result->malloc_pool_, T_ADMIN_STOP_UPGRADE_JOB);
}
|
ALTER SYSTEM upgrade_action ROLLING UPGRADE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ADMIN_ROLLING_UPGRADE_CMD, 1, $3);
}
|
ALTER SYSTEM REFRESH TIME_ZONE_INFO
{
  malloc_terminal_node($$, result->malloc_pool_, T_REFRESH_TIME_ZONE_INFO);
}
|
ALTER SYSTEM ENABLE SQL THROTTLE opt_sql_throttle_for_priority opt_sql_throttle_using_cond
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ENABLE_SQL_THROTTLE, 2, $6, $7);
}
|
ALTER SYSTEM DISABLE SQL THROTTLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISABLE_SQL_THROTTLE);
}
|
ALTER SYSTEM SET DISK VALID ip_port
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_DISK_VALID, 1, $6);
}
|
ALTER SYSTEM RESTORE tenant_name FROM STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RESTORE_TENANT, 2, $4, $6);
}
|
ALTER SYSTEM RESTORE relation_name FROM relation_name AT STRING_VALUE UNTIL STRING_VALUE WITH STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PHYSICAL_RESTORE_TENANT, 5, $4, $6, $8, $10, $12);
}
|
ALTER SYSTEM CHANGE TENANT change_tenant_name_or_tenant_id
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHANGE_TENANT, 1, $5);
}
|
ALTER SYSTEM DROP TABLES IN SESSION INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SYSTEM_DROP_TEMP_TABLE, 1, $7);
}
|
ALTER SYSTEM REFRESH TABLES IN SESSION INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SYSTEM_REFRESH_TEMP_TABLE, 1, $7);
}
|
ALTER DISKGROUP relation_name ADD DISK STRING_VALUE opt_disk_alias ip_port opt_zone_desc
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_DISKGROUP_ADD_DISK, 5, $3, $6, $7, $8, $9);
}
|
ALTER DISKGROUP relation_name DROP DISK STRING_VALUE ip_port opt_zone_desc
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_DISKGROUP_DROP_DISK, 4, $3, $6, $7, $8);
}
|
ALTER SYSTEM ARCHIVELOG
{
  ParseNode *enable = NULL;
  malloc_terminal_node(enable, result->malloc_pool_, T_INT);
  enable->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ARCHIVE_LOG, 1, enable);
}
|
ALTER SYSTEM NOARCHIVELOG
{
  ParseNode *enable = NULL;
  malloc_terminal_node(enable, result->malloc_pool_, T_INT);
  enable->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ARCHIVE_LOG, 1, enable);
}
|
ALTER SYSTEM BACKUP DATABASE
{
  ParseNode *incremental = NULL;
  malloc_terminal_node(incremental, result->malloc_pool_, T_INT);
  incremental->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_DATABASE, 1, incremental);
}
|
ALTER SYSTEM BACKUP INCREMENTAL DATABASE
{
  ParseNode *incremental = NULL;
  malloc_terminal_node(incremental, result->malloc_pool_, T_INT);
  incremental->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_DATABASE, 1, incremental);
}
|
ALTER SYSTEM CANCEL BACKUP
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 0;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
ALTER SYSTEM SUSPEND BACKUP
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 1;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
ALTER SYSTEM RESUME BACKUP
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 2;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
ALTER SYSTEM DELETE EXPIRED BACKUP
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 3;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
ALTER SYSTEM DELETE BACKUPSET INTNUM
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 4;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = $5->value_;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
ALTER SYSTEM VALIDATE DATABASE
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 5;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
ALTER SYSTEM VALIDATE BACKUPSET INTNUM
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 6;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = $5->value_;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
ALTER SYSTEM CANCEL VALIDATE INTNUM
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 7;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = $5->value_;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
ALTER SYSTEM DELETE OBSOLETE BACKUP
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 8;
  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
ALTER SYSTEM CANCEL DELETE BACKUP
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 10;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
SET ENCRYPTION ON IDENTIFIED BY STRING_VALUE ONLY
{
  ParseNode *mode = NULL;
  malloc_terminal_node(mode, result->malloc_pool_, T_INT);
  mode->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_SET_ENCRYPTION, 2, mode, $6);
}
|
SET DECRYPTION IDENTIFIED BY string_list
{
  ParseNode *string_list_node = NULL;
  merge_nodes(string_list_node, result, T_STRING_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_SET_DECRYPTION, 1, string_list_node);
}
;

opt_sql_throttle_for_priority:
FOR PRIORITY COMP_LE INTNUM
{
  $$ = $4;
}
|
{

  $$ = NULL;
}
;

opt_sql_throttle_using_cond:
USING sql_throttle_one_or_more_metrics
{
  merge_nodes($$, result, T_SQL_THROTTLE_METRICS, $2);
}
;

sql_throttle_one_or_more_metrics:
sql_throttle_metric sql_throttle_one_or_more_metrics
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| sql_throttle_metric
{
  $$ = $1;
}
;

sql_throttle_metric:
RT COMP_EQ int_or_decimal
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RT, 1, $3);
}
| CPU COMP_EQ int_or_decimal
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CPU, 1, $3);
}
| IO COMP_EQ INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_IO, 1, $3);
}
| NETWORK COMP_EQ int_or_decimal
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NETWORK, 1, $3);
}
| LOGICAL_READS COMP_EQ INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOGICAL_READS, 1, $3);
}
| QUEUE_TIME COMP_EQ int_or_decimal
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_QUEUE_TIME, 1, $3);
}
;

opt_disk_alias:
NAME opt_equal_mark relation_name_or_string
{
  (void)($2);
  $$ = $3;
}
| /*empty*/ {$$ = NULL;}
;

change_tenant_name_or_tenant_id:
relation_name_or_string
{
  malloc_terminal_node($$, result->malloc_pool_, T_TENANT_NAME);
  $$->str_value_ = $1->str_value_;
  $$->str_len_ = $1->str_len_;
}
| TENANT_ID opt_equal_mark INTNUM
{
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_TENANT_ID);
  $$->value_ = $3->value_;
}
;

cache_type:
ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_ALL;
}
| LOCATION
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_LOCATION;
}
| CLOG
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_CLOG;
}
| ILOG
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_ILOG;
}
| COLUMN_STAT
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_COLUMN_STAT;
}
| BLOCK_INDEX
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_BLOCK_INDEX;
}
| BLOCK
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_BLOCK;
}
| ROW
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_ROW;
}
| BLOOM_FILTER
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_BLOOM_FILTER;
}
| SCHEMA
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_SCHEMA;
}
| PLAN
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_PLAN;
}
| AUDIT
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_SQL_AUDIT;
}
| PL
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_PL_OBJ;
}
| PS
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_PS_OBJ;
}
;

balance_task_type:
AUTO
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
}
|
MANUAL
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
|
ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
;

opt_balance_task_type:
TYPE opt_equal_mark balance_task_type
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_BALANCE_TASK_TYPE, 1, $3);
}
| /*empty*/ {$$ = NULL;}

opt_tenant_list:
TENANT COMP_EQ tenant_name_list
{
  merge_nodes($$, result, T_TENANT_LIST, $3);
}
| /*empty*/ {$$ = NULL;}

tenant_list_tuple:
TENANT opt_equal_mark '(' tenant_name_list ')'
{
  (void)($2) ; /* make bison mute */
  merge_nodes($$, result, T_TENANT_LIST, $4);
}
;

tenant_name_list:
relation_name_or_string
{
  $$ = $1;
}
| tenant_name_list ',' relation_name_or_string
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}

flush_scope:
GLOBAL
{
  malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
  $$->value_ = 1;
}
| /* empty */
{
  malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
  $$->value_ = 0;
}

opt_zone_list:
ZONE COMP_EQ zone_list
{
  merge_nodes($$, result, T_ZONE_LIST, $3);
}
| /*empty*/ {$$ = NULL;}

server_info_list:
server_info
{
  $$ = $1;
}
| server_info_list ',' server_info
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

server_info:
REGION opt_equal_mark relation_name_or_string ZONE opt_equal_mark relation_name_or_string SERVER opt_equal_mark STRING_VALUE
{
  (void)($2);
  (void)($5);
  (void)($8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SERVER_INFO, 3, $3, $6, $9);
}
| ZONE opt_equal_mark relation_name_or_string SERVER opt_equal_mark STRING_VALUE
{
  (void)($2);
  (void)($5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SERVER_INFO, 3, NULL, $3, $6);
}
;

server_action:
ADD
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
| DELETE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
| CANCEL DELETE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 3;
}
| START
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 4;
}
| STOP
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 5;
}
| FORCE STOP
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 6;
}
| ISOLATE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 7;
}
;

server_list:
STRING_VALUE
{
  $$ = $1;
}
| server_list ',' STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

opt_server_list:
/*empty*/
{
  $$ = NULL;
}
| SERVER opt_equal_mark '(' server_list ')'
{
  (void)($2) ; /* make bison mute */
  merge_nodes($$, result, T_SERVER_LIST, $4);
}
;

opt_ignore_server_list:
/*empty*/
{
  $$ = NULL;
}
| IGNORE server_list
{
  ParseNode *server_list = NULL;
  merge_nodes(server_list, result, T_SERVER_LIST, $2);
  $$ = server_list;
};

zone_action:
DELETE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
| START
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 3;
}
| STOP
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 4;
}
| FORCE STOP
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 6;
}
| ISOLATE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 7;
}
;

ip_port:
SERVER opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_IP_PORT, 1, $3);
}
;

zone_desc :
ZONE opt_equal_mark relation_name_or_string
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ZONE, 1, $3);
}
;

opt_zone_desc:
zone_desc
{
  $$ = $1;
}
| /* EMPTY */
{
  $$ = NULL;
}
;

opt_create_timestamp:
CREATE_TIMESTAMP opt_equal_mark INTNUM
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TIMESTAMP, 1, $3);
}
| /* EMPTY */
{
  $$ = NULL;
}
;

server_or_zone:
ip_port
{
  $$ = $1;
}
| zone_desc
{
  $$ = $1;
}
;

opt_server_or_zone:
server_or_zone
{
  $$ = $1;
}
| /* EMPTY */
{
  $$ = NULL;
}
;

add_or_alter_zone_option:
REGION opt_equal_mark relation_name_or_string
{
  (void)($2);
  $$ = $3;
  $$->type_ = T_REGION;
}
| IDC opt_equal_mark relation_name_or_string
{
  (void)($2);
  $$ = $3;
  $$->type_ = T_IDC;
}
| ZONE_TYPE opt_equal_mark relation_name_or_string
{
  (void)($2);
  $$ = $3;
  $$->type_ = T_ZONE_TYPE;
}
;

add_or_alter_zone_options:
add_or_alter_zone_option
{
  $$ = $1;
}
| add_or_alter_zone_options ',' add_or_alter_zone_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

alter_or_change_or_modify:
ALTER
{
  $$ = NULL;
}
| CHANGE
{
  $$ = NULL;
}
| MODIFY
{
  $$ = NULL;
}
;

partition_id_desc:
PARTITION_ID opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ID_DESC, 1, $3);
}
;

opt_tenant_list_or_partition_id_desc:
tenant_list_tuple
{
  $$ = $1;
}
| partition_id_desc
{
  $$ = $1;
}
| /* EMPTY */
{
  $$ = NULL;
}
;

partition_id_or_server_or_zone:
partition_id_desc ip_port
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ID_SERVER, 2, $1, $2);
}
| ip_port opt_tenant_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SERVER_TENANT, 2, $1, $2);
}
| zone_desc opt_tenant_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ZONE_TENANT, 2, $1, $2);
}
;

migrate_action:
MOVE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
| COPY
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
;

change_actions:
change_action
{
  $$ = $1;
}
| change_action change_actions
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

change_action:
replica_type
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_REPLICA_TYPE, 1, $1);
}
| memstore_percent
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MEMSTORE_PERCENT, 1, $1);
}
;

replica_type:
REPLICA_TYPE opt_equal_mark STRING_VALUE
{
  (void)($2);
  $$ = $3;
}
;

memstore_percent:
MEMSTORE_PERCENT opt_equal_mark INTNUM
{
  (void)($2);
  $$ = $3;
}
;

suspend_or_resume:
SUSPEND
{
  // START is 1
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
| RESUME
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 3;
}
;

baseline_id_expr:
BASELINE_ID opt_equal_mark INTNUM
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_BASELINE_ID, 1, $3);
}
;

opt_baseline_id:
baseline_id_expr
{
  $$ = $1;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;


sql_id_expr:
SQL_ID opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SQL_ID, 1, $3);
}
;

opt_sql_id:
sql_id_expr
{
  $$ = $1;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

baseline_asgn_factor :
column_name COMP_EQ literal
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ASSIGN_ITEM, 2, $1, $3);
}


tenant_name :
TENANT opt_equal_mark relation_name_or_string
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_TENANT_NAME, 1, $3);
}
;

opt_tenant_name:
tenant_name
{
  $$ = $1;
}
| /* EMPTY */
{
  $$ = NULL;
}
;

cache_name :
CACHE opt_equal_mark relation_name_or_string
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CACHE_NAME, 1, $3);
}
;

opt_cache_name:
cache_name
{
  $$ = $1;
}
| /* EMPTY */
{
  $$ = NULL;
}
;

file_id:
FILE_ID opt_equal_mark INTNUM
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FILE_ID, 1, $3);
}
;

opt_file_id:
file_id
{
  $$ = $1
}
|
{
  $$ = NULL;
}
;

cancel_task_type:
PARTITION MIGRATION
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
| /* empty */
{
  $$ = NULL;
}
;

alter_system_set_parameter_actions:
alter_system_set_parameter_action
{
  $$ = $1;
}
| alter_system_set_parameter_actions ',' alter_system_set_parameter_action
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

alter_system_set_parameter_action:
NAME_OB COMP_EQ conf_const opt_comment opt_config_scope
opt_server_or_zone opt_tenant_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SYSTEM_ACTION, 5,
                           $1,    /* param_name */
                           $3,    /* param_value */
                           $4,    /* comment */
                           $6,    /* zone or server */
                           $7     /* tenant */
                           );
  $$->value_ = $5[0];             /* scope */
}
|
TABLET_SIZE COMP_EQ conf_const opt_comment opt_config_scope
opt_server_or_zone opt_tenant_name
{
  ParseNode *tablet_size = NULL;
  make_name_node(tablet_size, result->malloc_pool_, "tablet_size");
  malloc_non_terminal_node($$, result->malloc_pool_, T_SYSTEM_ACTION, 5,
                           tablet_size,    /* param_name */
                           $3,    /* param_value */
                           $4,    /* comment */
                           $6,    /* zone or server */
                           $7     /* tenant */
                           );
  $$->value_ = $5[0];             /* scope */
}
|
CLUSTER_ID COMP_EQ conf_const opt_comment opt_config_scope
opt_server_or_zone opt_tenant_name
{
  ParseNode *cluster_id = NULL;
  make_name_node(cluster_id, result->malloc_pool_, "cluster_id");
  malloc_non_terminal_node($$, result->malloc_pool_, T_SYSTEM_ACTION, 5,
                           cluster_id,    /* param_name */
                           $3,    /* param_value */
                           $4,    /* comment */
                           $6,    /* zone or server */
                           $7     /* tenant */
                           );
  $$->value_ = $5[0];                /* scope */
}
|
ROOTSERVICE_LIST COMP_EQ STRING_VALUE opt_comment opt_config_scope
opt_server_or_zone opt_tenant_name
{
  ParseNode *rootservice_list = NULL;
  make_name_node(rootservice_list, result->malloc_pool_, "rootservice_list");
  malloc_non_terminal_node($$, result->malloc_pool_, T_SYSTEM_ACTION, 5,
                           rootservice_list,    /* param_name */
                           $3,    /* param_value */
                           $4,    /* comment */
                           $6,    /* zone or server */
                           $7     /* tenant */
                           );
  $$->value_ = $5[0];                /* scope */
}
;

opt_comment:
COMMENT STRING_VALUE
{ $$ = $2; }
| /* EMPTY */
{ $$ = NULL; }
;

alter_system_settp_actions:
settp_option
{
  $$ = $1;
}
| alter_system_settp_actions ',' settp_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

settp_option:
TP_NO opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_TP_NO, 1, $3);
}
| TP_NAME opt_equal_mark relation_name_or_string
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_TP_NAME, 1, $3);
}
| OCCUR opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_OCCUR, 1, $3);
}
| FREQUENCY opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRIGGER_MODE, 1, $3);
}
| ERROR_CODE opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_ERROR_CODE, 1, $3);
}
;

opt_full:
FULL
{$$[0]=1;}
| /* EMPTY */
{$$[0]=0;}
;

opt_config_scope:
SCOPE COMP_EQ MEMORY
{ $$[0] = 0; }   /* same as ObConfigType */
| SCOPE COMP_EQ SPFILE
{ $$[0] = 1; }   /* same as ObConfigType */
| SCOPE COMP_EQ BOTH
{ $$[0] = 2; }   /* same as ObConfigType */
| /* EMPTY */
{ $$[0] = 2; }   /* same as ObConfigType */
;

/*
frozen_type:
MINOR FREEZE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
| MAJOR FREEZE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
;
*/

partition_role:
LEADER
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
}
| FOLLOWER
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
;

upgrade_action:
BEGI
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
| END
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
;

////////////////////////////////////////////////////////////////
/* SET NAMES 'charset_name' [COLLATE 'collation_name'] */
set_names_stmt:
SET NAMES charset_name_or_default opt_collation
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_NAMES, 2, $3, $4);
};

////////////////////////////////////////////////////////////////
/* SET CHARACTER SET charset_name */
set_charset_stmt:
SET charset_key charset_name_or_default
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_CHARSET, 1, $3);
};

//////////////////////////////
set_transaction_stmt:
SET TRANSACTION transaction_characteristics
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANSACTION, 2, $$, $3);
};
| SET GLOBAL TRANSACTION transaction_characteristics
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANSACTION, 2, $$, $4);
};
| SET SESSION TRANSACTION transaction_characteristics
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANSACTION, 2, $$, $4);
};
| SET LOCAL TRANSACTION transaction_characteristics
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANSACTION, 2, $$, $4);
};

transaction_characteristics:
transaction_access_mode
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANSACTION_CHARACTERISTICS, 2, $1, NULL);
}
| ISOLATION LEVEL isolation_level
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANSACTION_CHARACTERISTICS, 2, NULL, $3);
}
| transaction_access_mode ',' ISOLATION LEVEL isolation_level
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANSACTION_CHARACTERISTICS, 2, $1, $5);
}
| ISOLATION LEVEL isolation_level ',' transaction_access_mode
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANSACTION_CHARACTERISTICS, 2, $5, $3);
};

transaction_access_mode:
READ ONLY
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = OB_TRANS_READ_ONLY;
}
| READ WRITE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = OB_TRANS_READ_WRITE;
};

isolation_level:
 READ UNCOMMITTED
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
}
| READ COMMITTED
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
| REPEATABLE READ
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
| SERIALIZABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 3;
};


/*===========================================================
 * savepoint
 *===========================================================*/
create_savepoint_stmt:
SAVEPOINT var_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_CREATE_SAVEPOINT);
  $$->str_value_ = $2->str_value_;
  $$->str_len_ = $2->str_len_;
}
;
rollback_savepoint_stmt:
ROLLBACK TO var_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_ROLLBACK_SAVEPOINT);
  $$->str_value_ = $3->str_value_;
  $$->str_len_ = $3->str_len_;
}
| ROLLBACK WORK TO var_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_ROLLBACK_SAVEPOINT);
  $$->str_value_ = $4->str_value_;
  $$->str_len_ = $4->str_len_;
}
| ROLLBACK TO SAVEPOINT var_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_ROLLBACK_SAVEPOINT);
  $$->str_value_ = $4->str_value_;
  $$->str_len_ = $4->str_len_;
}
;
release_savepoint_stmt:
RELEASE SAVEPOINT var_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_RELEASE_SAVEPOINT);
  $$->str_value_ = $3->str_value_;
  $$->str_len_ = $3->str_len_;
}
;

/*===========================================================
 *
 *	Name classification
 *
 *===========================================================*/

var_name:
    NAME_OB
    {
      $$ = $1;
    }
  | unreserved_keyword_normal
    {
      get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
    }
;

column_name:
NAME_OB
{ $$ = $1;}
| unreserved_keyword
{
  get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

relation_name:
NAME_OB { $$ = $1; }
| unreserved_keyword
{
  get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

function_name:
NAME_OB
{
  $$ = $1;
}
| DUMP
{
  make_name_node($$, result->malloc_pool_, "dump");
}
| CHARSET
{
  make_name_node($$, result->malloc_pool_, "charset");
}
| COLLATION
{
  make_name_node($$, result->malloc_pool_, "collation");
}
| KEY_VERSION
{
  make_name_node($$, result->malloc_pool_, "version");
}
| USER
{
  make_name_node($$, result->malloc_pool_, "user");
}
| DATABASE
{
  make_name_node($$, result->malloc_pool_, "database");
}
| COALESCE
{
  make_name_node($$, result->malloc_pool_, "coalesce");
}
| REPEAT
{
  make_name_node($$, result->malloc_pool_, "repeat");
}
| ROW_COUNT
{
  make_name_node($$, result->malloc_pool_, "row_count");
}
| REVERSE
{
  make_name_node($$, result->malloc_pool_, "reverse");
}
| RIGHT
{
  make_name_node($$, result->malloc_pool_, "right");
}
| CURRENT_USER
{
  make_name_node($$, result->malloc_pool_, "current_user");
}
| SYSTEM_USER
{
  make_name_node($$, result->malloc_pool_, "user");
}
| SESSION_USER
{
  make_name_node($$, result->malloc_pool_, "user");
}
| REPLACE
{
  make_name_node($$, result->malloc_pool_, "replace");
}
| TRUNCATE
{
  make_name_node($$, result->malloc_pool_, "truncate");
}
| FORMAT
{
  make_name_node($$, result->malloc_pool_, "format");
}
;

column_label:
NAME_OB
{ $$ = $1; }
| unreserved_keyword
{
  get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

date_unit:
DAY
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_DAY;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| DAY_HOUR
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_DAY_HOUR;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| DAY_MICROSECOND
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_DAY_MICROSECOND;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| DAY_MINUTE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_DAY_MINUTE;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| DAY_SECOND
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_DAY_SECOND;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| HOUR
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_HOUR;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| HOUR_MICROSECOND
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_HOUR_MICROSECOND;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| HOUR_MINUTE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_HOUR_MINUTE;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| HOUR_SECOND
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_HOUR_SECOND;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| MICROSECOND
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_MICROSECOND;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| MINUTE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_MINUTE;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| MINUTE_MICROSECOND
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_MINUTE_MICROSECOND;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| MINUTE_SECOND
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_MINUTE_SECOND;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| MONTH
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_MONTH;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| QUARTER
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_QUARTER;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| SECOND
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_SECOND;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| SECOND_MICROSECOND
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_SECOND_MICROSECOND;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| WEEK
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_WEEK;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| YEAR
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_YEAR;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| YEAR_MONTH
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_YEAR_MONTH;
  $$->is_hidden_const_ = 1;
  $$->is_date_unit_ = 1;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
;

unreserved_keyword:
unreserved_keyword_normal { $$=$1;}
| unreserved_keyword_special { $$=$1;}
| unreserved_keyword_extra { $$=$1;}
;

unreserved_keyword_normal:
ACCOUNT
|       ACTION
|       ACTIVE
|       ADDDATE
|       AFTER
|       AGAINST
|       AGGREGATE
|       ALGORITHM
|       ALWAYS
|       ANALYSE
|       ANY
|       APPROX_COUNT_DISTINCT
|       APPROX_COUNT_DISTINCT_SYNOPSIS
|       APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE
|       ARCHIVELOG
|       ASCII
|       AT
|       AUDIT
|       AUTHORS
|       AUTO
|       AUTOEXTEND_SIZE
|       AUTO_INCREMENT
|       AVG
|       AVG_ROW_LENGTH
|       BACKUP
|       BACKUPSET
|       BASE
|       BASELINE
|       BASELINE_ID
|       BASIC
|       BALANCE
|       BEGI
|       BINDING
|       BINLOG
|       BIT
|       BISON_LIST
|       BLOCK
|       BLOCK_SIZE
|       BLOCK_INDEX
|       BLOOM_FILTER
|       BOOL
|       BOOLEAN
|       BOOTSTRAP
|       BTREE
|       BYTE
|       BREADTH
|       BUCKETS
|       CACHE
|       KVCACHE
|       ILOGCACHE
|       CANCEL
|       CASCADED
|       CAST
|       CATALOG_NAME
|       CHAIN
|       CHANGED
|       CHARSET
|       CHECKSUM
|       CHECKPOINT
|       CHUNK
|       CIPHER
|       CLASS_ORIGIN
|       CLEAN
|       CLEAR
|       CLIENT
|       CLOSE
|       CLOG
|       CLUSTER
|       CLUSTER_ID
|       CLUSTER_NAME
|       COALESCE
|       CODE
|       COLLATION
|       COLUMN_FORMAT
|       COLUMN_NAME
|       COLUMN_STAT
|       COLUMNS
|       COMMENT
|       COMMIT
|       COMMITTED
|       COMPACT
|       COMPLETION
|       COMPRESSED
|       COMPRESSION
|       CONCURRENT
|       CONNECTION %prec KILL_EXPR
|       CONSISTENT
|       CONSISTENT_MODE
|       CONSTRAINT_CATALOG
|       CONSTRAINT_NAME
|       CONSTRAINT_SCHEMA
|       CONTAINS
|       CONTEXT
|       CONTRIBUTORS
|       COPY
|       COUNT
|       CPU
|       CREATE_TIMESTAMP
|       CTX_ID
|       CUBE
|       CUME_DIST
|       CURDATE
|       CURRENT
|       CURSOR_NAME
|       CURTIME
|       CYCLE
|       DAG
|       DATA
|       DATABASE_ID
|       DATAFILE
|       DATA_TABLE_ID
|       DATE
|       DATE_ADD
|       DATE_SUB
|       DATETIME
|       DAY
|       DEALLOCATE
|       DECRYPTION
|       DEFAULT_AUTH
|       DEFINER
|       DELAY
|       DELAY_KEY_WRITE
|       DENSE_RANK
|       DEPTH
|       DES_KEY_FILE
|       DESTINATION
|       DIAGNOSTICS
|       DIRECTORY
|       DISABLE
|       DISCARD
|       DISK
|       DISKGROUP
|       DO
|       DUMP
|       DUMPFILE
|       DUPLICATE
|       DUPLICATE_SCOPE
|       DYNAMIC
|       DEFAULT_TABLEGROUP
|       EFFECTIVE
|       ENABLE
|       ENCRYPTION
|       END
|       ENDS
|       ENGINE_
|       ENGINES
|       ENUM
|       ENTITY
|       ERROR_CODE
|       ERROR_P
|       ERRORS
|       ESCAPE
|       EVENT
|       EVENTS
|       EVERY
|       EXCEPT %prec HIGHER_PARENS
|       EXCHANGE
|       EXECUTE
|       EXPANSION
|       EXPIRE
|       EXPIRED
|       EXPIRE_INFO
|       EXPORT
|       EXTENDED
|       EXTENDED_NOADDR
|       EXTENT_SIZE
|       FAILOVER
|       EXTRACT
|       FAST
|       FAULTS
|       FLASHBACK
|       FIELDS
|       FILEX
|       FILE_ID
|       FINAL_COUNT
|       FIRST
|       FIRST_VALUE
|       FIXED
|       FLUSH
|       FOLLOWER
|       FOLLOWING
|       FORMAT
|       FROZEN
|       FOUND
|       FREEZE
|       FREQUENCY
|       FUNCTION
|       FULL %prec HIGHER_PARENS
|       GENERAL
|       GEOMETRY
|       GEOMETRYCOLLECTION
|       GET_FORMAT
|       GLOBAL %prec LOWER_PARENS
|       GLOBAL_NAME
|       GRANTS
|       GROUPING
|       GROUP_CONCAT
|       GTS
|       HANDLER
|       HASH
|       HELP
|       HISTOGRAM
|       HOST
|       HOSTS
|       HOUR
|       ID
|       IDC
|       IDENTIFIED
|       IGNORE_SERVER_IDS
|       ILOG
|       IMPORT
|       INDEXES
|       INDEX_TABLE_ID
|       INCR
|       INFO
|       INITIAL_SIZE
|       INNODB
|       INSERT_METHOD
|       INSTALL
|       INSTANCE
|       INTERSECT
|       INVOKER
|       INCREMENTAL
|       IO
|       IO_THREAD
|       IPC
|       ISNULL
|       ISOLATION
|       ISOLATE
|       ISSUER
|       JOB
|       JSON
|       KEY_BLOCK_SIZE
|       KEY_VERSION
|       LAG
|       LANGUAGE
|       LAST
|       LAST_VALUE
|       LEAD
|       LEADER
|       LEAK
|       LEAK_MOD
|       LEAK_RATE
|       LEAVES
|       LESS
|       LEVEL
|       LINESTRING
|       LIST_
|       LISTAGG
|       LOCAL
|       LOCALITY
|       LOCKED
|       LOCKS
|       LOGFILE
|       LOGONLY_REPLICA_NUM
|       LOGS
|       MAJOR
|       MANUAL
|       MASTER
|       MASTER_AUTO_POSITION
|       MASTER_CONNECT_RETRY
|       MASTER_DELAY
|       MASTER_HEARTBEAT_PERIOD
|       MASTER_HOST
|       MASTER_LOG_FILE
|       MASTER_LOG_POS
|       MASTER_PASSWORD
|       MASTER_PORT
|       MASTER_RETRY_COUNT
|       MASTER_SERVER_ID
|       MASTER_SSL
|       MASTER_SSL_CA
|       MASTER_SSL_CAPATH
|       MASTER_SSL_CERT
|       MASTER_SSL_CIPHER
|       MASTER_SSL_CRL
|       MASTER_SSL_CRLPATH
|       MASTER_SSL_KEY
|       MASTER_USER
|       MAX
|       MAX_CONNECTIONS_PER_HOUR
|       MAX_CPU
|       MAX_DISK_SIZE
|       MAX_IOPS
|       MAX_MEMORY
|       MAX_QUERIES_PER_HOUR
|       MAX_ROWS
|       MAX_SESSION_NUM
|       MAX_SIZE
|       MAX_UPDATES_PER_HOUR
|       MAX_USER_CONNECTIONS
|       MEDIUM
|       MEMORY
|       MEMTABLE
|       MERGE
|       MESSAGE_TEXT
|       MEMSTORE_PERCENT
|       META
|       MICROSECOND
|       MIGRATE
|	    	MIGRATION
|       MIN
|       MIN_CPU
|       MIN_IOPS
|       MIN_MEMORY
|       MINOR
|       MIN_ROWS
|       MINUTE
|       MINUS
|       MODE
|       MODIFY
|       MONTH
|       MOVE
|       MULTILINESTRING
|       MULTIPOINT
|       MULTIPOLYGON
|       MUTEX
|       MYSQL_ERRNO
|       MAX_USED_PART_ID
|       NAME
|       NAMES
|       NATIONAL
|       NCHAR
|       NDB
|       NDBCLUSTER
|       NEW
|       NEXT
|       NO
|       NOARCHIVELOG
|       NOAUDIT
|       NODEGROUP
|       NONE
|       NOPARALLEL
|       NORMAL
|       NOW
|       NOWAIT
|       NO_WAIT
|       NTILE
|       NTH_VALUE
|       NUMBER
|       NULLS
|       NVARCHAR
|       OCCUR
|       OF
|       OFF
|       OFFSET
|       OLD_PASSWORD
|       OLD_KEY
|       OVER
|       ONE
|       ONE_SHOT
|       ONLY
|       OPEN
|       OPTIONS
|       ORIG_DEFAULT
|       REMOTE_OSS
|       OUTLINE
|       OWNER
|       PACK_KEYS
|       PAGE
|       PARALLEL
|       PARAMETERS
|       PARSER
|       PARTIAL
|       PARTITION_ID
|       PARTITIONING
|       PARTITIONS
|       PERCENT_RANK
|       PAUSE
|       PHASE
|       PHYSICAL
|       PL
|       PLANREGRESS
|       PLUGIN
|       PLUGIN_DIR
|       PLUGINS
|       POINT
|       POLYGON
|       POOL
|       PORT
|       POSITION
|       PRECEDING
|       PREPARE
|       PRESERVE
|       PREV
|       PRIMARY_CLUSTER_ID
|       PRIMARY_ZONE
|       PRIMARY_ROOTSERVICE_LIST
|       PRIVILEGES
|       PROCESS
|       PROCESSLIST
|       PROFILE
|       PROFILES
|       PROGRESSIVE_MERGE_NUM
|       PROXY
|       PUBLIC
|       PCTFREE
|       P_ENTITY
|       P_CHUNK
|       QUARTER
|       QUERY %prec KILL_EXPR
|       QUEUE_TIME
|       QUICK
|       RANK
|       READ_ONLY
|       REBUILD
|       RECOVER
|       RECOVERY
|       RECYCLE
|       RECYCLEBIN
|       ROTATE
|       ROW_NUMBER
|       REDO_BUFFER_SIZE
|       REDOFILE
|       REDUNDANT
|       REFRESH
|       REGION
|       RELAY
|       RELAYLOG
|       RELAY_LOG_FILE
|       RELAY_LOG_POS
|       RELAY_THREAD
|       RELOAD
|       REMOVE
|       REORGANIZE
|       REPAIR
|       REPEATABLE
|       REPLICA
|       REPLICA_NUM
|       REPLICA_TYPE
|       REPLICATION
|       REPORT
|       RESET
|       RESOURCE
|       RESOURCE_POOL_LIST
|       RESPECT
|       RESTART
|       RESTORE
|       RESUME
|       RETURNED_SQLSTATE
|       RETURNS
|       REVERSE
|       REWRITE_MERGE_VERSION
|       ROLLBACK
|       ROLLING
|       ROLLUP
|       ROOT
|       ROOTSERVICE
|       ROOTSERVICE_LIST
|       ROOTTABLE
|       ROUTINE
|       ROW
|       ROW_COUNT
|       ROW_FORMAT
|       ROWS
|       RTREE
|       RUN
|       SAMPLE
|       SAVEPOINT
|       SCHEDULE
|       SCHEMA_NAME
|       SCOPE
|       SECOND
|       SECURITY
|       SEED
|       SERIAL
|       SERIALIZABLE
|       SERVER
|       SERVER_IP
|       SERVER_PORT
|       SERVER_TYPE
|       SESSION %prec LOWER_PARENS
|       SESSION_USER
|       SET_MASTER_CLUSTER
|       SET_SLAVE_CLUSTER
|       SET_TP
|       SHARE
|       SHUTDOWN
|       SIGNED
|       SIZE
|       SIMPLE
|       SLAVE
|       SLOW
|       SNAPSHOT
|       SOCKET
|       SOME
|       SONAME
|       SOUNDS
|       SOURCE
|       SPFILE
|       SPLIT
|       SQL_AFTER_GTIDS
|       SQL_AFTER_MTS_GAPS
|       SQL_BEFORE_GTIDS
|       SQL_BUFFER_RESULT
|       SQL_CACHE
|       SQL_ID
|       SQL_NO_CACHE
|       SQL_THREAD
|       SQL_TSI_DAY
|       SQL_TSI_HOUR
|       SQL_TSI_MINUTE
|       SQL_TSI_MONTH
|       SQL_TSI_QUARTER
|       SQL_TSI_SECOND
|       SQL_TSI_WEEK
|       SQL_TSI_YEAR
|       STANDBY
|       START
|       STARTS
|       STAT
|       STATS_AUTO_RECALC
|       STATS_PERSISTENT
|       STATS_SAMPLE_PAGES
|       STATUS
|       STATEMENTS
|       STD
|       STDDEV
|       STDDEV_POP
|       STDDEV_SAMP
|       STOP
|       STORAGE
|       STORAGE_FORMAT_VERSION
|       STORAGE_FORMAT_WORK_VERSION
|       STORING
|       STRONG
|       STRING
|       SUBCLASS_ORIGIN
|       SUBDATE
|       SUBJECT
|       SUBPARTITION
|       SUBPARTITIONS
|       SUBSTR
|       SUBSTRING
|       SUCCESSFUL
|       SUM
|       SUPER
|       SUSPEND
|       SWAPS
|       SWITCH
|       SWITCHES
|       SWITCHOVER
|       SYSTEM
|       SYSTEM_USER
|       SYSDATE
|       SYNONYM
|       TABLE_CHECKSUM
|       TABLE_MODE
|       TABLEGROUPS
|       TABLE_ID
|       TABLE_NAME
|       TABLES
|       TABLESPACE
|       TABLET
|       TABLET_SIZE
|       TABLET_MAX_SIZE
|		    TASK
|       TEMPLATE
|       TEMPORARY
|       TEMPTABLE
|       TENANT
|       TENANT_ID
|       SLOT_IDX
|       TEXT
|       THAN
|       TIME
|       TIMESTAMP
|       TIMESTAMPADD
|       TIMESTAMPDIFF
|       TIME_ZONE_INFO
|       TP_NAME
|       TP_NO
|       TRACE
|       TRANSACTION
|       TRADITIONAL
|       TRIGGERS
|       TRIM
|       TRUNCATE
|       TYPE
|       TYPES
|       TABLEGROUP_ID
|       UNCOMMITTED
|       UNDEFINED
|       UNDO_BUFFER_SIZE
|       UNDOFILE
|       UNICODE
|       UNKNOWN
|       UNINSTALL
|       UNIT
|       UNIT_NUM
|       UNLOCKED
|       UNTIL
|       UNUSUAL
|       UPGRADE
|       USE_BLOOM_FILTER
|       USE_FRM
|       USER
|       USER_RESOURCES
|       UNBOUNDED
|       VALID
|       VALIDATE
|       VALUE
|       VARIANCE
|       VARIABLES
|       VAR_POP
|       VAR_SAMP
|       VERBOSE
|       VIRTUAL_COLUMN_ID
|       MATERIALIZED
|       VIEW
|       VERIFY
|       WAIT
|       WARNINGS
|       WEAK
|       WEEK
|       WEIGHT_STRING
|       WHENEVER
|       WINDOW
|       WORK
|       WRAPPER
|       X509
|       XA
|       XML
|       YEAR
|       ZONE
|       ZONE_LIST
|       ZONE_TYPE
|       LOCATION
|       PLAN
|       VISIBLE
|       INVISIBLE
|       ACTIVATE
|       SYNCHRONIZATION
|       THROTTLE
|       PRIORITY
|       RT
|       NETWORK
|       LOGICAL_READS
|       REDO_TRANSPORT_OPTIONS
|       MAXIMIZE
|       AVAILABILITY
|       PERFORMANCE
|       PROTECTION
|       OBSOLETE
;

unreserved_keyword_special:
PASSWORD
;
unreserved_keyword_extra:
ACCESS
;

mysql_reserved_keyword:
ACCESSIBLE
| ADD
//| ALL
| ALTER
| ANALYZE
| AND
| AS
| ASC
| ASENSITIVE
| BEFORE
| BETWEEN
| BIGINT
| BINARY
| BLOB
//| BOTH
| BY
| CALL
| CASCADE
| CASE
| CHANGE
| CHAR
| CHARACTER
| CHECK
| COLLATE
| COLUMN
| CONDITION
| CONSTRAINT
| CONTINUE
| CONVERT
| CREATE
| CROSS
| CURRENT_DATE
| CURRENT_TIME
| CURRENT_TIMESTAMP
| CURRENT_USER
| CURSOR
| DATABASE
| DATABASES
| DAY_HOUR
| DAY_MICROSECOND
| DAY_MINUTE
| DAY_SECOND
| DECLARE
| DECIMAL
| DEFAULT
| DELAYED
| DELETE
| DESC
| DESCRIBE
| DETERMINISTIC
//| DISTINCT
| DISTINCTROW
| DIV
| DOUBLE
| DROP
| DUAL
| EACH
| ELSE
| ELSEIF
| ENCLOSED
| ESCAPED
| EXISTS
| EXIT
| EXPLAIN
| FETCH
| FLOAT
| FLOAT4
| FLOAT8
| FOR
| FORCE
| FOREIGN
//| FROM
| GENERATED
| GET
| GRANT
| GROUP
| HAVING
| HIGH_PRIORITY
| HOUR_MICROSECOND
| HOUR_MINUTE
| HOUR_SECOND
| IF
| IGNORE
| IN
| INDEX
| INFILE
| INNER
| INOUT
| INSENSITIVE
| INSERT
| INT
| INT1
| INT2
| INT3
| INT4
| INT8
| INTEGER
| INTERVAL
| INTO
| IO_AFTER_GTIDS
| IO_BEFORE_GTIDS
| IS
| ITERATE
| JOIN
| KEY
| KEYS
| KILL
//| LEADING
| LEAVE
| LEFT
| LIKE
| LIMIT
| LINEAR
| LINES
| LOAD
| LOCALTIME
| LOCALTIMESTAMP
| LONG
| LONGBLOB
| LONGTEXT
| LOOP
| LOW_PRIORITY
| MASTER_BIND
| MASTER_SSL_VERIFY_SERVER_CERT
| MATCH
| MAXVALUE
| MEDIUMBLOB
| MEDIUMINT
| MEDIUMTEXT
| MIDDLEINT
| MINUTE_MICROSECOND
| MINUTE_SECOND
| MOD
| MODIFIES
| NATURAL
| NOT
| NO_WRITE_TO_BINLOG
| ON
| OPTIMIZE
| OPTION
| OPTIONALLY
| OR
| ORDER
| OUT
| OUTER
| OUTFILE
| PARTITION
| PRECISION
| PRIMARY
| PROCEDURE
| PURGE
| RANGE
| READ
| READS
| READ_WRITE
| REAL
| REFERENCES
| REGEXP
| RELEASE
| RENAME
| REPEAT
| REPLACE
| REQUIRE
| RESIGNAL
| RESTRICT
| RETURN
| REVOKE
| RIGHT
| RLIKE
| SCHEMA
| SCHEMAS
| SECOND_MICROSECOND
//| SELECT
| SENSITIVE
| SEPARATOR
| SET
| SHOW
| SIGNAL
| SMALLINT
| SPATIAL
| SPECIFIC
| SQL
| SQLEXCEPTION
| SQLSTATE
| SQLWARNING
| SQL_BIG_RESULT
//| SQL_CALC_FOUND_ROWS
| SQL_SMALL_RESULT
| SSL
| STARTING
| STORED
| STRAIGHT_JOIN
| TABLE
| TERMINATED
| THEN
| TINYBLOB
| TINYINT
| TINYTEXT
| TO
//| TRAILING
| TRIGGER
| UNDO
| UNION
//| UNIQUE
| UNLOCK
| UNSIGNED
| UPDATE
| USAGE
| USE
| USING
| UTC_DATE
| UTC_TIME
| UTC_TIMESTAMP
| VALUES
| VARBINARY
| VARCHAR
| VARCHARACTER
| VARYING
| VIRTUAL
//| WHEN
| WHERE
| WHILE
| WITH
| WRITE
| XOR
| YEAR_MONTH
| ZEROFILL

%%
////////////////////////////////////////////////////////////////
void yyerror(void *yylloc, ParseResult *p, char *s, ...)
{
  if (OB_LIKELY(NULL != p)) {
    p->result_tree_ = 0;
    va_list ap;
    va_start(ap, s);
    vsnprintf(p->error_msg_, MAX_ERROR_MSG, s, ap);
    if (OB_LIKELY(NULL != yylloc)) {
      YYLTYPE *yylloc_pointer = (YYLTYPE *)yylloc;
      if (OB_LIKELY(NULL != p->input_sql_) && p->input_sql_[yylloc_pointer->first_column - 1] != '\'') {
        p->start_col_ = yylloc_pointer->first_column;
      }
      p->end_col_ = yylloc_pointer->last_column;
      p->line_ = yylloc_pointer->first_line;
    }
    va_end(ap);
  }
}

void obsql_mysql_parser_fatal_error(yyconst char *msg, yyscan_t yyscanner)
{
  if (OB_LIKELY(NULL != msg)) {
    (void)fprintf(stderr, "FATAL ERROR:%s\n", msg);
  }
  ParseResult *p = obsql_mysql_yyget_extra(yyscanner);
  longjmp(p->jmp_buf_, 1);//the secord param must be non-zero value
}

int obsql_mysql_multi_fast_parse(ParseResult *p)
{
  int ret = 0;
  if (OB_UNLIKELY(NULL == p)) {
    ret = -1;
  } else {
    /* The semantic value of the lookahead symbol.  */
    YYSTYPE yylval;
    /* Location data for the lookahead symbol.  */
    YYLTYPE yylloc;
    int token = YYEMPTY;
    bool has_more = true;
    while (0 == ret && has_more) {
      token = obsql_mysql_yylex(&yylval, &yylloc, p->yyscan_info_);
      switch (token) {
        case ERROR:
          ret = -1;
          break;
        case PARSER_SYNTAX_ERROR:
          ret = OB_PARSER_ERR_PARSE_SQL;
          break;
        case END_P:
        case DELIMITER:
          /* fall through */
          has_more = false;
        default:
          break;
      }
    } /* end while */
    p->end_col_ = yylloc.last_column;
  }
  return ret;
}

int obsql_mysql_fast_parse(ParseResult *p)
{
  int ret = 0;
  if (OB_UNLIKELY(NULL == p)) {
    ret = -1;
  } else {
    /* The semantic value of the lookahead symbol.  */
    YYSTYPE yylval;
    /* Location data for the lookahead symbol.  */
    YYLTYPE yylloc;
    int token = YYEMPTY;
    while ((0 == ret) && END_P != (token = obsql_mysql_yylex(&yylval, &yylloc, p->yyscan_info_))) {
      switch (token) {
        case ERROR:
        case PARSER_SYNTAX_ERROR: {
          ret = -1;
          break;
        }
        default: {
          if (p->is_ignore_token_) {
            if (SELECT_HINT_BEGIN == token ||
                UPDATE_HINT_BEGIN == token ||
                DELETE_HINT_BEGIN == token ||
                INSERT_HINT_BEGIN == token ||
                REPLACE_HINT_BEGIN == token ||
                LOAD_DATA_HINT_BEGIN == token
                /* token == INSERT_HINT_BEGIN */) {
              const char *hint_begin = obsql_mysql_yyget_text(p->yyscan_info_);
              const char *slash = memchr(hint_begin,
                                         '/',
                                         obsql_mysql_yyget_leng(p->yyscan_info_));
              int length = slash - hint_begin;
              memmove(p->no_param_sql_ + p->no_param_sql_len_, hint_begin, slash - hint_begin);
              p->no_param_sql_len_ += length;
              p->token_num_++;
            } else if (token == HINT_END) {
              p->is_ignore_token_ = false;
            } else {/*do nothing*/}
          } else {
            memmove(p->no_param_sql_ + p->no_param_sql_len_,
                    obsql_mysql_yyget_text(p->yyscan_info_),
                    obsql_mysql_yyget_leng(p->yyscan_info_));
            p->no_param_sql_len_ += obsql_mysql_yyget_leng(p->yyscan_info_);
            p->token_num_++;
          }
          break;
        }
      }
    } /*while end*/
    int len = p->no_param_sql_len_;
    p->no_param_sql_[len] = '\0';
  }
  return ret;
}
