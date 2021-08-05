/* Copyright 2010-2017 Alibaba Inc. All Rights Reserved.
// Author:
//     Guibin Du <tianguan.dgb@taobao.com>
// Normalizer:
//     Yao Ying <yaoying.yyy@alipay.com>
*/

%define api.pure
%parse-param {ParseResult *result}
%name-prefix "obsql_oracle_yy"
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
  int32_t ival[3]; //ival[0]表示value, ival[1]表示fast parse在对应的该node及其子node可识别的常量个数, ival[2] for length_semantics
 }

%{
#include "../../../src/sql/parser/sql_parser_oracle_mode_lex.h"
#include "../../../src/sql/parser/sql_parser_base.h"
%}

%destructor {destroy_tree($$);}<node>
%destructor {oceanbase::common::ob_free($$);}<str_value_>

%token <node> NAME_OB
%token <node> STRING_VALUE
%token <node> INTNUM
%token <node> DATE_VALUE
%token <node> INTERVAL_VALUE
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
%token <node> OUTLINE_DEFAULT_TOKEN/*use for outline parser to just filter hint of query_sql*/
%token <node> MULTISET_OP
%token <node> PLSQL_VARIABLE

/*empty_query::
// (1) 对于只有空格或者;的查询语句需要报错：如："" 或者 "   " 或者 ";" 或者 " ;  " 都需要报错：err_msg:Query was empty  errno:1065
// (2) 对于只含有注释或者空格或者;的查询语句则需要返回成功：如："#fadfadf " 或者"/**\/" 或者 "/**\/  ;" 返回成功
*/

/*
 * 以％left开头的行定义算符的结合性
 * ％left表示其后的算符是遵循左结合的；
 * ％right表示右结合性，
 * ％nonassoc表示其后的算符没有结合性。
 * 优先级是隐含的，排在前面行的算符较后面行的算符的优先级低；
 * 排在同一行的算符优先级相同，因此，'+'和'-'优先级相同
 * 而它们的优先级都小于'*’，三个运算符都是左结合
 */

/*
 * 在表达式中有时要用到一元运算符
 * 它可能与某个二元运算符是同一个符号
 * 如一元运算符负号“-”和二元运算符减号’-’相同
 * 一元运算符的优先级应该比相应的二元运算符的优先级高
 * 至少应该与’*'的优先级相同，这可以用yacc的％Prec子句来定义
 * '-'expr %prec '*'
 * 它说明它所在的语法规则中最右边的运算符或终结符的优先级与%prec后面的符号的优先级相同
 */

%nonassoc   BASIC OUTLINE EXTENDED EXTENDED_NOADDR PARTITIONS PLANREGRESS SOME/*for solve explain_stmt conflict*/
%nonassoc   MERGE /*for solve explain_stmt conflict*/
%nonassoc   KILL_EXPR
%nonassoc   CONNECTION QUERY
%left MULTISET_OP AT
%left	UNION EXCEPT MINUS
%left	INTERSECT
%nonassoc LOWER_JOIN
%left JOIN CROSS LEFT FULL RIGHT INNER USING BULK RETURNING RETURN NATURAL LOG
%left	OR
%left	XOR
%left	AND AND_OP
%left   BETWEEN CASE WHEN THEN ELSE
%nonassoc LOWER_THAN_COMP
%nonassoc FROM MEMBER SUBMULTISET CURRENT_SCHEMA ISOLATION_LEVEL/*for solve conflict*/
%left   COMP_EQ COMP_GE COMP_GT COMP_LE COMP_LT COMP_NE IS LIKE IN REGEXP_LIKE COMP_NE_PL
%right  ESCAPE /*for conflict for escape*/
%left   '|'
%left   SHIFT_LEFT SHIFT_RIGHT
%left   '+' '-' CNNOP
%left   '*' '/' '%' MOD DIV POW
%left   POW_PL
%nonassoc LOWER_THAN_NEG /* for unary_expr conflict*/
%left NEG '~' PRIOR CONNECT_BY_ROOT
%nonassoc ','
%nonassoc LOWEST_PARENS SAMPLE DATE SET/*for solve conflict*/
%nonassoc LOWER_PARENS
%left   '(' ')'
%left WRITE LOW_PRIORITY TABLESPACE GLOBAL SESSION SIZE /*for solve conflict*/
%left CHARACTER GENERATED INTERVAL INVISIBLE NCHAR NVARCHAR2 NUMERIC TIMESTAMP VISIBLE OFFSET FETCH
      ORDER DEC REAL BLOB CLOB BINARY_DOUBLE BINARY_FLOAT SQL_CALC_FOUND_ROWS TRANSACTION TENANT WHERE
      NOLOGGING PIVOT UNPIVOT SEED CONSTRAINT PRIMARY ID ORIG_DEFAULT COMMENT /*for solve conflict*/
%nonassoc PARTITION SUBPARTITION REFERENCES WITHIN
%nonassoc UROWID INT
%left   '.'
%right  NOT
%right BINARY COLLATE
%nonassoc LOWER_KEY /* for unique key and unique and key*/
%nonassoc KEY /* for unique key and unique and key*/
%nonassoc LOWER_ON /*on expr*/
%nonassoc ON /*on expr*/
%nonassoc LOWER_OVER
%nonassoc COLUMN_OUTER_JOIN_SYMBOL /*for simple_expr conflict*/
%nonassoc OVER KEEP
%nonassoc LOWER_THAN_TO
%nonassoc TO
%left HIGHER_THAN_TO
%nonassoc LOWER_THAN_BY_ACCESS_SESSION

%token ERROR /*used internal*/
%token PARSER_SYNTAX_ERROR /*used internal*/

%token/*for hint*/

READ_STATIC INDEX_HINT USE_NL FROZEN_VERSION  TOPK QUERY_TIMEOUT READ_CONSISTENCY HOTSPOT LOG_LEVEL
LEADING_HINT ORDERED FULL_HINT USE_MERGE USE_HASH NO_USE_HASH USE_PLAN_CACHE USE_JIT NO_USE_JIT
NO_USE_MERGE NO_USE_NL NO_USE_BNL USE_NL_MATERIALIZATION NO_USE_NL_MATERIALIZATION
NO_REWRITE TRACE_LOG USE_PX QB_NAME USE_HASH_AGGREGATION NO_USE_HASH_AGGREGATION NEG_SIGN
USE_LATE_MATERIALIZATION NO_USE_LATE_MATERIALIZATION USE_BNL MAX_CONCURRENT PX_JOIN_FILTER NO_USE_PX
PQ_DISTRIBUTE RANDOM_LOCAL BROADCAST STAT MERGE_HINT NO_MERGE_HINT NO_EXPAND USE_CONCAT UNNEST NO_UNNEST
PLACE_GROUP_BY NO_PLACE_GROUP_BY NO_PRED_DEDUCE TRANS_PARAM LOAD_BATCH_SIZE FORCE_REFRESH_LOCATION_CACHE
NO_PX_JOIN_FILTER PQ_MAP DISABLE_PARALLEL_DML ENABLE_PARALLEL_DML NO_PARALLEL

%token /*can not be relation name*/
CNNOP
SELECT_HINT_BEGIN UPDATE_HINT_BEGIN DELETE_HINT_BEGIN INSERT_HINT_BEGIN REPLACE_HINT_BEGIN MERGE_HINT_BEGIN HINT_HINT_BEGIN HINT_END
LOAD_DATA_HINT_BEGIN
END_P SET_VAR DELIMITER

/*reserved keyword*/
%token
/*
 * Oracle 11g Reserved Keywords（110个）
 * https://docs.oracle.com/cd/B28359_01/appdev.111/b31231/appb.htm#CJHIIICD
 * 注：NULL注释掉是因为在外面已定义,另外两个注释是因为oracle 12c能够使用，这里ob也并未使用，因此注释掉了
 * 注意！！！非特殊情况，禁止将关键字放到该区域
 * */
        ACCESS ADD ALL ALTER AND ANY /*ARRAYLEN*/ AS ASC AUDIT
        BETWEEN BY
        CHAR CHECK CLUSTER COLUMN COMMENT COMPRESS CONNECT CREATE CURRENT
        DATE DECIMAL DEFAULT DELETE DESC DISTINCT DROP
        ELSE EXCLUSIVE EXISTS
        FILE_KEY FLOAT FOR FROM
        GRANT GROUP
        HAVING
        IDENTIFIED IMMEDIATE IN INCREMENT INDEX INITIAL_ INSERT INTEGER INTERSECT INTO IS
        LEVEL LIKE LOCK LONG
        MAXEXTENTS
        MINUS MODE MODIFY
        NOAUDIT NOCOMPRESS NOT NOTFOUND NOWAIT /*NULL*/ NUMBER
        OF OFFLINE ON ONLINE OPTION OR ORDER
        PCTFREE PRIOR PRIVILEGES PUBLIC
        RAW RENAME RESOURCE REVOKE ROW ROWID ROWLABEL ROWNUM ROWS
        START SELECT SESSION SET SHARE SIZE SMALLINT /*SQLBUF*/ SUCCESSFUL SYNONYM SYSDATE
        TABLE THEN TO TRIGGER
        UID UNION UNIQUE UPDATE USER
        VALIDATE VALUES VARCHAR VARCHAR2 VIEW
        WHENEVER WHERE WITH
  /*ob特有的保留关键字*/
        CASE CONNECT_BY_ROOT DUAL SQL_CALC_FOUND_ROWS

%token <non_reserved_keyword>
/*
 * Oracle 11g Non-Reserved Keywords（171个）(注释的代表目前还没有用到：NORMAL)
 * https://docs.oracle.com/cd/B28359_01/appdev.111/b31231/appb.htm#CJHIIICD
 * 请注意！！！新添加的非保留关键字，肯定是ob特有的，不要再添加在oracle非保留关键字里面了，thanks！！！
 * */
        ADMIN AFTER ALLOCATE ANALYZE ARCHIVE ARCHIVELOG AUTHORIZATION AVG
        BACKUP BEGI BECOME BEFORE BLOCK BODY
        CACHE CANCEL CASCADE CHANGE CHARACTER CHECKPOINT CLOSE COBOL COMMIT COMPILE CONSTRAINT CONSTRAINTS
        CONTENTS CONTINUE CONTROLFILE COUNT CURSOR CYCLE
        DATABASE DATAFILE DBA DEC DECLARE DISABLE DISMOUNT DOUBLE DUMP
        EACH ENABLE END ESCAPE EVENTS EXCEPT EXCEPTIONS EXEC EXPLAIN EXECUTE EXEMPT EXTENT EXTERNALLY
        FETCH FLUSH FREELIST FREELISTS FORCE FOREIGN FORTRAN FOUND FUNCTION
        GO GOTO GROUPS INCLUDING INDICATOR INITRANS INSTANCE INT
        KEY
        LANGUAGE LAYER LINK LISTS LOGFILE
        MANAGE MANUAL MAX MAXDATAFILES MAXINSTANCES MAXLOGFILES MAXLOGHISTORY MAXLOGMEMBERS MAXTRANS
        MAXVALUE MIN MINEXTENTS MINVALUE MODULE MOUNT NEXT NEW
        NOARCHIVELOG NOCACHE NOCYCLE NOMAXVALUE NOMINVALUE NONE NOORDER NORESETLOGS /*NORMAL*/ NOSORT NUMERIC
        OFF OLD ONLY OPEN OPTIMAL OWN
        PACKAGE_KEY PARALLEL PCTINCREASE PCTUSED PLAN PLI PRECISION PRIMARY PRIVATE PROCEDURE PROFILE
        QUOTA READ REAL RECOVER REFERENCES REFERENCING RESETLOGS RESTRICTED REUSE ROLE ROLES ROLLBACK
        SAVEPOINT SCHEMA SCN SECTION SEGMENT SEQUENCE SHARED SNAPSHOT SOME SORT SQL SQLCODE SQLERROR
        SQLSTATE STATEMENT_ID STATISTICS STOP STORAGE SUM SWITCH SYSTEM
        TABLES TABLESPACE TEMPORARY THREAD TIME TRACING DOP TRANSACTION TRIGGERS TRUNCATE
        UNDER UNLIMITED UNTIL USE USING
        WHEN WRITE WORK
/*
 * OB特有的Non-Reserved Keywords
 * 新添加的非保留关键字请按照首字母排序添加到对应行中，thanks！！！
 * */
         A ACCESSIBLE ACCOUNT ACTION ACTIVE ADDDATE ADMINISTER AGAINST AGGREGATE ALGORITHM ALWAYS
         ANALYSE APPROX_COUNT_DISTINCT APPROX_COUNT_DISTINCT_SYNOPSIS AUTHORS AUTO AVG_ROW_LENGTH
         APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE ASENSITIVE AT AUTOEXTEND_SIZE ASCII

         BALANCE BASE BASELINE BASELINE_ID BASIC BINDING BINLOG BIT BLOCK_INDEX BLOCK_SIZE
         BLOOM_FILTER BOOL BOOLEAN BOOTSTRAP BTREE BYTE BREADTH BINARY BOTH BULK BLOB BINARY_DOUBLE
         BINARY_DOUBLE_INFINITY BINARY_DOUBLE_NAN BINARY_FLOAT BINARY_FLOAT_INFINITY
         BINARY_FLOAT_NAN BULK_ROWCOUNT BULK_EXCEPTIONS

         CALL CASCADED CAST CATALOG_NAME CHAIN CHANGED CHARSET CHAR_CS CHECKSUM CIPHER CLASS_ORIGIN CLEAN
         CLEAR CLIENT CLOG COALESCE COLUMN_STAT COLLATE COLLECT CROSS CODE COLLATION
         COLUMN_FORMAT COLUMN_NAME COLUMNS COMMITTED COMPACT COMPLETION COMPRESSED COMPRESSION
         COMPUTE CONCURRENT CONNECTION CONSISTENT CONSISTENT_MODE CONSTRAINT_CATALOG CONSTRAINT_NAME
         CONSTRAINT_SCHEMA CONTAINS CONTEXT CONTRIBUTORS COPY CPU CREATE_TIMESTAMP CUBE CUME_DIST
         CLUSTER_ID CLUSTER_NAME CORR COVAR_POP COVAR_SAMP CLOB CURRENT_SCHEMA CURRENT_USER
         CONNECT_BY_ISCYCLE CONNECT_BY_ISLEAF CURRENT_DATE CURRENT_TIMESTAMP CALC_PARTITION_ID

         DATA DATA_TABLE_ID DATE_ADD DATE_SUB DATETIME DAY DAY_HOUR DAY_MICROSECOND DAY_MINUTE
         DAY_SECOND DBTIMEZONE DEALLOCATE DEFAULT_AUTH DEFINER DELAY DELAYED DELAY_KEY_WRITE DEPTH
         DES_KEY_FILE DENSE_RANK DESCRIBE DESTINATION DETERMINISTIC DIAGNOSTICS DIRECTORY DISCARD
         DISK DISTINCTROW DO DUMPFILE DUPLICATE DUPLICATE_SCOPE DYNAMIC DATABASE_ID DEFAULT_TABLEGROUP
         DATABASES DIV DBA_RECYCLEBIN DICTIONARY DML

         E EFFECTIVE ELSEIF ENDS ENGINE_ ENGINES ENUM ERROR_CODE ERROR_P ERRORS ESTIMATE EVENT EVERY
         EXCHANGE EXIT EXPANSION EXPIRE EXPIRE_INFO ERROR_INDEX EXPORT EXTENDED
         EXTENDED_NOADDR EXTENT_SIZE EXTRACT ENCLOSED ESCAPED ENCRYPTION EMPTY EXCLUDE

         FAILED_LOGIN_ATTEMPTS FAST FAULTS FIELDS FILE_ID FINAL_COUNT FIRST FIRST_VALUE FIXED FLOAT4
         FLOAT8 FOLLOWER FORMAT FREEZE FREQUENCY FOLLOWING FLASHBACK FROZEN FULL

         G GENERAL GENERATED GEOMETRY GEOMETRYCOLLECTION GET_FORMAT GLOBAL GRANTS GROUPING GTS GET
         GLOBAL_ALIAS SESSION_ALIAS COLUMN_OUTER_JOIN_SYMBOL

         HANDLER HASH HELP HOST HOSTS HOUR HIGH_PRIORITY HOUR_MICROSECOND HOUR_MINUTE HOUR_SECOND
         HIGH HIDDEN HYBRID_HIST

         IDENTITY IF IFIGNORE IGNORE IGNORE_SERVER_IDS INFINITE_VALUE ILOG ILOGCACHE IMPORT INCR INDEXES
         INFILE INDEX_TABLE_ID INNER INFO INITIAL_SIZE INOUT INSERT_METHOD INSTALL INSENSITIVE
         INT1 INT2 INT3 INT4 INT8 INTERVAL INVOKER IO IO_AFTER_GTIDS IO_BEFORE_GTIDS IO_THREAD
         IPC ISOLATION ISSUER IS_TENANT_SYS_POOL INVISIBLE ITERATE ISOPEN ID IDC ISOLATION_LEVEL
         INCLUDE INDEXED

         JOB JSON JOIN

         K KEY_BLOCK_SIZE KEYS KEYSTORE KEY_VERSION KVCACHE KILL KEEP

         LAG LAST LAST_VALUE LEAD LEADER LEAVE LEAVES LESS LINEAR LINESTRING LIST LISTAGG LOCAL
         LOCALITY LOCATION LOCKED LOCKS LOGONLY_REPLICA_NUM LOG LOGS LONGBLOB LONGTEXT LOOP LEADING LEFT
         LINES LOAD LOCK_ LOW_PRIORITY LOW LOGICAL_READS LIMIT LNNVL LOCALTIMESTAMP

         M MAJOR MANAGEMENT MASTER MASTER_AUTO_POSITION MASTER_BIND MASTER_CONNECT_RETRY MASTER_DELAY
         MASTER_HEARTBEAT_PERIOD MASTER_HOST MASTER_LOG_FILE MASTER_LOG_POS MASTER_PASSWORD MATERIALIZED
         MASTER_PORT MASTER_RETRY_COUNT MASTER_SERVER_ID MASTER_SSL MASTER_SSL_CA MASTER_SSL_CAPATH
         MASTER_SSL_CERT MASTER_SSL_CIPHER MASTER_SSL_CRL MASTER_SSL_CRLPATH MASTER_SSL_KEY
         MASTER_SSL_VERIFY_SERVER_CERT MASTER_USER MAX_CONNECTIONS_PER_HOUR MAX_CPU MAX_DISK_SIZE
         MAX_IOPS MAX_MEMORY MAX_QUERIES_PER_HOUR MAX_ROWS MAX_SESSION_NUM MAX_SIZE MAX_UPDATES_PER_HOUR
         MAX_USER_CONNECTIONS MEDIUM MEDIUMBLOB MEDIUMINT MEDIUMTEXT MEMORY MEMTABLE MESSAGE_TEXT
         META MICROSECOND MIDDLEINT MIGRATE MIN_CPU MIN_IOPS MIN_MEMORY MINOR MIN_ROWS MINUTE
         MINUTE_MICROSECOND MINUTE_SECOND MODIFIES MONTH MOVE MULTILINESTRING MULTIPOINT MULTIPOLYGON
         MUTEX MYSQL_ERRNO MIGRATION MAX_USED_PART_ID MERGE ISNULL MATCH MOD MOVEMENT MEMBER MATCHED
         MEMSTORE_PERCENT MEDIAN

         NAME NAMES NAN_VALUE NATIONAL NCHAR NCHAR_CS NDB NDBCLUSTER NO NODEGROUP NOW NO_WAIT
         NO_WRITE_TO_BINLOG NULLS NVARCHAR2 NTILE NTH_VALUE NATURAL NOLOGGING NETWORK
         NORELY NOVALIDATE NOPARALLEL

         OCCUR OLD_PASSWORD ONE ONE_SHOT OPTIMIZE OPTIONS OWNER OLD_KEY OLTP OUT OVER OPTIONALLY
         OUTER OUTFILE OFFSET OBJECT ORIG_DEFAULT ORA_ROWSCN OUTLINE

         P PACK_KEYS PAGE PARAMETERS PARSER PARTIAL PARTITION_ID PARTITIONING PARTITIONS PASSWORD
         PASSWORD_LOCK_TIME PASSWORD_VERIFY_FUNCTION PAUSE PERCENTAGE PERCENT_RANK PHASE PLANREGRESS
         PLUGIN PLUGIN_DIR PLUGINS POINT POLYGON PURGE PARTITION POOL PORT POSITION PREPARE PRESERVE
         PREV PRIMARY_ZONE PROCESS PROCESSLIST PROFILES PROXY PRECEDING PARAM_ASSIGN_OPERATOR PERCENT
         PRIMARY_ROOTSERVICE_LIST POLICY PRIVILEGE PIVOT PRIORITY PERCENTILE_DISC PERCENTILE_CONT

         QUARTER QUERY QUICK QUEUE_TIME

         READ_WRITE READS REBUILD RECYCLE REDO_BUFFER_SIZE REDOFILE REDUNDANT REFRESH REGION RELAY
         RELAYLOG RELAY_LOG_FILE RELAY_LOG_POS RELAY_THREAD RELOAD RELEASE REMOVE REORGANIZE REPAIR
         REPEATABLE REPLICA REPLICA_NUM REPLICA_TYPE REPLICATION REPORT RESET RESIGNAL RESOURCE_POOL_LIST
         RESPECT RESTART RESTORE READ_ONLY RANK RESUME RETURNED_SQLSTATE RETURNS REVERSE RELY
         REWRITE_MERGE_VERSION ROLLUP RLIKE ROOT ROOTTABLE ROOTSERVICE ROOTSERVICE_LIST ROUTINE
         RESTRICT RETURN RIGHT RETURNING REDACTION RT ROW_COUNT ROW_FORMAT RTREE RUN RECYCLEBIN
         ROW_NUMBER RANGE RECURSIVE REGEXP_LIKE REPLACE REPEAT REQUIRE RANDOM REGR_SLOPE RATIO_TO_REPORT
         REGR_INTERCEPT REGR_COUNT REGR_R2 REGR_AVGX REGR_AVGY REGR_SXX REJECT REGR_SYY REGR_SXY ROWCOUNT
         REMOTE_OSS

         SAMPLE SCHEDULE SCHEMAS SCHEMA_NAME SCOPE SECOND SECOND_MICROSECOND SECURITY SEED SENSITIVE
         SERIAL SERIALIZABLE SERVER SERVER_IP SERVER_PORT SERVER_TYPE SESSIONTIMEZONE SESSION_USER
         SET_MASTER_CLUSTER SET_SLAVE_CLUSTER SET_TP SHOW SHRINK SHUTDOWN SIGNAL SIGNED SIMPLE
         SLAVE SLOW SOCKET SONAME SOUNDS SOURCE SPACE SPATIAL SPECIFIC SPFILE SPLIT SQLEXCEPTION SQLWARNING
         SQL_AFTER_GTIDS SQL_AFTER_MTS_GAPS SQL_BEFORE_GTIDS SQL_BIG_RESULT SQL_BUFFER_RESULT SQL_CACHE
         SQL_NO_CACHE SQL_ID SQL_CALC_FOUND_ROW SQL_SMALL_RESULT SQL_THREAD SQL_TSI_DAY SQL_TSI_HOUR
         SQL_TSI_MINUTE SQL_TSI_MONTH SQL_TSI_QUARTER SQL_TSI_SECOND SQL_TSI_WEEK SQL_TSI_YEAR STARTS
         STATS_AUTO_RECALC STATS_PERSISTENT STATS_SAMPLE_PAGES STATUS STATEMENTS SUBMULTISET
         PROGRESSIVE_MERGE_NUM STORAGE_FORMAT_VERSION STORAGE_FORMAT_WORK_VERSION STRAIGHT_JOIN STORED
         STORING SUBCLASS_ORIGIN SUBDATE SUBJECT SUBPARTITION SUBPARTITIONS SUBSTR SUPER SUSPEND SWAPS
         SWITCHES SYSTEM_USER SEARCH SEPARATOR SKIP SSL STARTING SYSTIMESTAMP STRONG SYS_CONNECT_BY_PATH
         STDDEV STDDEV_POP STDDEV_SAMP SYSBACKUP SYSDBA SYSKM SYSOPER SIBLINGS SETS SKEWONLY

         T TABLE_CHECKSUM TABLE_MODE TABLE_ID TABLE_NAME TABLEGROUPS TABLET TABLET_MAX_SIZE TEMPLATE
         TEMPTABLE TENANT TEXT THAN TIMESTAMP THROTTLE TIMESTAMPADD TIMESTAMPDIFF TINYBLOB TINYTEXT
         TP_NO TP_NAME TRACE TRADITIONAL TRIM TYPE TYPES TASK TABLET_SIZE TABLEGROUP_ID TERMINATED
         TABLEGROUP TRAILING TIMEZONE_ABBR TIMEZONE_HOUR TIMEZONE_MINUTE TIMEZONE_REGION TIES TRANSLATE TOP_K_FRE_HIST

         UNCOMMITTED UNDEFINED UNDO UNDO_BUFFER_SIZE UNDOFILE UNICODE UNINSTALL UNIT UNIT_NUM UNLOCKED
         UNUSUAL UPGRADE USE_BLOOM_FILTER UNKNOWN USAGE USE_FRM USER_RESOURCES UNBOUNDED
         UNLOCK UTC_DATE UTC_TIMESTAMP UROWID UNPIVOT

         VALID VARIABLES VARYING VAR_POP VAR_SAMP VERBOSE VISIBLE VIRTUAL VARIANCE

         WAIT WARNINGS WEEK WEIGHT_STRING WRAPPER INNER_PARSE WHILE WEAK WITHIN WMSYS WM_CONCAT

         X509 XA XML XOR

         YEAR YEAR_MONTH

         ZONE ZONE_LIST ZEROFILL TIME_ZONE_INFO ZONE_TYPE

%type <node> sql_stmt stmt_list stmt opt_end_p
%type <node> select_stmt update_stmt delete_stmt merge_stmt
%type <node> dml_table_name insert_table_clause dml_table_clause opt_table_alias
%type <node> insert_stmt single_table_insert opt_error_logging_clause multi_table_insert values_clause insert_table_clause_list conditional_insert_clause opt_all_or_first condition_insert_clause_list opt_default_insert_clause condition_insert_clause insert_single_table_clause opt_simple_expression into_err_log_caluse reject_limit
%type <node> create_table_stmt opt_table_option_list table_option_list table_option table_option_list_space_seperated
%type <node> out_of_line_constraint /*out_of_line_ref_constraint*/ constranit_state references_clause
%type <node> create_sequence_stmt alter_sequence_stmt drop_sequence_stmt opt_sequence_option_list sequence_option_list sequence_option simple_num
%type <node> create_synonym_stmt drop_synonym_stmt opt_public opt_force synonym_name synonym_object
%type <node> create_directory_stmt drop_directory_stmt directory_name directory_path
%type <node> create_keystore_stmt alter_keystore_stmt
%type <node> alter_database_stmt
%type <node> opt_database_name database_name database_option database_option_list database_factor
%type <node> create_tenant_stmt opt_tenant_option_list alter_tenant_stmt drop_tenant_stmt
%type <node> create_restore_point_stmt drop_restore_point_stmt
%type <node> create_resource_stmt drop_resource_stmt alter_resource_stmt
%type <node> cur_timestamp_func utc_timestamp_func
%type <node> create_dblink_stmt drop_dblink_stmt dblink tenant opt_cluster opt_dblink
%type <node> opt_create_resource_pool_option_list create_resource_pool_option alter_resource_pool_option_list alter_resource_pool_option
%type <node> opt_shrink_unit_option unit_id_list on_commit_option
%type <node> opt_resource_unit_option_list resource_unit_option
%type <node> tenant_option zone_list resource_pool_list
%type <node> opt_partition_option partition_option partition_option_inner hash_partition_option use_partition range_partition_option subpartition_option opt_range_partition_list opt_range_subpartition_list range_partition_list range_subpartition_list range_partition_element range_subpartition_element range_partition_expr range_expr_list range_expr opt_part_id sample_clause opt_block seed sample_percent opt_sample_scope modify_partition_info  tg_modify_partition_info column_partition_option opt_column_partition_option use_flashback auto_partition_option auto_range_type partition_size auto_partition_type
%type <node> subpartition_template_option subpartition_individual_option opt_hash_partition_list hash_partition_list hash_partition_element opt_hash_subpartition_list hash_subpartition_list hash_subpartition_element opt_subpartition_list subpartition_list
%type <node> date_unit timezone_unit date_unit_for_extract
%type <node> drop_table_stmt table_list drop_view_stmt table_or_tables opt_cascade_constraints opt_purge
%type <node> explain_stmt explainable_stmt format_name kill_stmt help_stmt create_outline_stmt alter_outline_stmt drop_outline_stmt opt_outline_target
%type <node> expr_list expr expr_const conf_const unary_expr simple_expr expr_or_default bit_expr bool_pri predicate explain_or_desc
%type <node> sql_function single_row_function aggregate_function /*analytic_function object_reference_function model_function olap_function data_cartridge_function user_defined_function*/ environment_id_function
%type <node> numeric_function character_function extract_function /*comparison_function*/ conversion_function /*large_object_function collection_function*/ hierarchical_function /*data_mining_function xml_function encoding_decoding_function null_related_function*/
%type <node> column_ref
%type <node> case_expr special_func_expr access_func_expr access_func_expr_count in_expr sub_query_flag
%type <node> simple_when_clause_list bool_when_clause_list simple_when_clause bool_when_clause case_default
%type <node> window_function opt_partition_by generalized_window_clause win_rows_or_range win_preceding_or_following win_interval win_bounding win_window opt_win_window win_fun_lead_lag_params respect_or_ignore opt_respect_or_ignore_nulls win_fun_first_last_params first_or_last opt_from_first_or_last
%type <node> update_asgn_list normal_asgn_list update_asgn_factor
%type <node> table_element_list table_element column_definition column_definition_ref column_definition_list column_name_list aux_column_list vertical_column_name column_definition_opt_datatype column_definition_opt_datatype_list
%type <node> opt_generated_keyname opt_generated_option_list opt_generated_identity_option opt_generated_column_attribute_list generated_column_attribute opt_storage_type
%type <node> opt_identity_attribute
%type <node> data_type opt_visibility_option visibility_option temporary_option opt_charset collation opt_collation cast_data_type
%type <node> insert_with_opt_hint opt_insert_columns column_list opt_replace opt_materialized
%type <node> insert_vals_list insert_vals
%type <node> subquery select_with_parens select_no_parens select_clause
%type <node> select_clause_set select_clause_set_left select_clause_set_right  select_with_hierarchical_query
%type <node> simple_select select_expr_list
%type <node> with_select with_clause with_list common_table_expr opt_column_alias_name_list alias_name_list column_alias_name opt_search_clause opt_cycle_clause search_set_value
%type <node> opt_where opt_order_by order_by opt_groupby_having opt_start_with start_with connect_by opt_fetch_next fetch_next_clause fetch_next fetch_next_count fetch_next_percent fetch_next_expr fetch_next_percent_expr
%type <node> for_update opt_for_wait_or_skip opt_for_update_of opt_where_extension
%type <node> sort_list sort_key opt_asc_desc groupby_clause groupby_element_list groupby_element rollup_clause group_by_expr_list group_by_expr opt_column_id opt_ascending_type opt_null_pos grouping_sets_clause cube_clause grouping_sets_list grouping_sets
%type <node> opt_query_expression_option_list query_expression_option_list query_expression_option opt_distinct_or_all projection
%type <node> from_list table_references table_reference table_factor normal_relation_factor dot_relation_factor relation_factor relation_factor_in_hint relation_factor_in_hint_list relation_factor_in_leading_hint relation_factor_in_pq_hint relation_factor_in_leading_hint_list_entry relation_factor_in_leading_hint_list relation_factor_in_use_join_hint_list joined_table tbl_name table_subquery source_relation_factor relation_factors dual_table
%type <node> qb_name_option
%type <node> outer_join_type join_outer join_condition natural_join_type
%type <node> precision_int_num
%type <ival> string_length_i opt_string_length_i opt_string_length_i_v2 signed_int_num opt_datetime_fsp_i opt_length_semantics_i opt_interval_leading_fsp_i opt_urowid_length_i urowid_length_i nstring_length_i
%type <node> number_precision opt_number_precision opt_float_precision opt_dec_precision opt_numeric_precision
%type <node> udt_type type_name
%type <node> opt_equal_mark opt_default_mark read_only_or_write not
%type <node> int_or_decimal
%type <node> opt_column_attribute_list column_attribute
%type <node> show_stmt from_or_in columns_or_fields opt_from_or_in_database_clause opt_show_condition opt_desc_column_option
%type <node> prepare_stmt stmt_name preparable_stmt
%type <node> variable_set_stmt var_and_val_list var_and_val to_or_eq set_expr_or_default sys_var_and_val_list sys_var_and_val opt_set_sys_var opt_global_sys_vars_set
%type <node> set_var_op
%type <node> execute_stmt argument_list argument opt_using_args
%type <node> deallocate_prepare_stmt deallocate_or_drop
%type <node> call_stmt routine_access_name call_param_list routine_name
%type <ival> opt_scope opt_drop_behavior scope_or_scope_alias
%type <ival> int_type_i varchar_type_i datetime_type_i cast_datetime_type_i double_type_i
%type <node> create_user_stmt alter_user_stmt alter_user_profile_stmt user_specification user password password_str opt_host_name user_with_host_name grant_user grant_user_list default_role_clause
%type <node> create_profile_stmt alter_profile_stmt drop_profile_stmt profile_name verify_function_name password_parameter password_parameters user_profile opt_profile password_parameter_type password_parameter_value opt_default_tables_space
%type <node> drop_user_stmt user_list
%type <node> set_password_stmt opt_for_user
%type <node> rename_table_stmt rename_table_actions rename_table_action
%type <node> truncate_table_stmt
%type <node> lock_user_stmt lock_spec_mysql57
%type <node> grant_stmt /*obj_privileges*/ grant_system_privileges role_sys_obj_all_col_priv_list role_sys_obj_all_col_priv /*priv_type_list*/ priv_type obj_clause opt_privilege opt_grant_options
%type <node> grantee_clause opt_with_admin_option
%type <node> revoke_stmt
%type <node> opt_for_grant_user
%type <node> create_role_stmt role opt_not_identified drop_role_stmt role_list set_role_stmt role_opt_identified_by_list role_opt_identified_by alter_role_stmt
%type <node> parameterized_trim
%type <node> opt_as
%type <ival> opt_with_consistent_snapshot opt_index_keyname opt_full opt_mode_flag
%type <node> opt_work begin_stmt commit_stmt rollback_stmt opt_siblings
%type <node> alter_table_stmt alter_table_actions alter_table_action alter_column_option alter_index_option alter_partition_option opt_to alter_tablegroup_option opt_table opt_tablegroup_option_list
%type <node> alter_index_stmt alter_index_actions alter_index_action alter_index_option_oracle
%type <node> tablegroup_option_list tablegroup_option alter_tablegroup_actions alter_tablegroup_action tablegroup_option_list_space_seperated
%type <node> opt_tg_partition_option tg_hash_partition_option tg_range_partition_option tg_subpartition_option tg_list_partition_option
%type <node> opt_set
%type <node> alter_system_stmt alter_system_settp_actions settp_option server_info_list server_info
%type <node> column_name relation_name function_name column_label var_name relation_name_or_string opt_alter_compress_option compress_option opt_compress_option opt_compress_str opt_compress_level pl_var_name
%type <node> audit_stmt audit_clause  op_audit_tail_clause audit_operation_clause audit_all_shortcut_list audit_all_shortcut auditing_on_clause auditing_by_user_clause
%type <node> opt_hint_list hint_option select_with_opt_hint update_with_opt_hint delete_with_opt_hint merge_with_opt_hint hint_list_with_end
%type <node> create_index_stmt index_name sort_column_list sort_column_key opt_index_option_list index_expr
%type <node> index_option opt_index_using_algorithm index_using_algorithm constraint_name
%type <node> opt_when opt_returning
%type <non_reserved_keyword> unreserved_keyword oracle_unreserved_keyword unreserved_keyword_normal aggregate_function_keyword
%type <ival> consistency_level use_plan_cache_type
%type <ival> set_type_other set_type_union audit_by_session_access_option audit_whenever_option audit_or_noaudit
%type <node> set_type set_expression_option
%type <node> drop_index_stmt hint_options /* opt_expr_list */ substr_params opt_comma
%type <node> /*frozen_type*/ opt_binary
%type <node> ip_port
%type <node> create_view_stmt view_name opt_column_list opt_table_id view_subquery view_with_opt
%type <node> name_list
%type <node> partition_role zone_desc opt_zone_desc server_or_zone opt_server_or_zone opt_partitions opt_subpartitions add_or_alter_zone_options alter_or_change_or_modify
%type <node> partition_id_desc opt_tenant_list_or_partition_id_desc partition_id_or_server_or_zone opt_create_timestamp change_actions change_action add_or_alter_zone_option
%type <node> memstore_percent
%type <node> migrate_action replica_type suspend_or_resume tenant_name opt_tenant_name cache_name opt_cache_name file_id opt_file_id cancel_task_type
%type <node> sql_id_expr opt_sql_id baseline_id_expr opt_baseline_id baseline_asgn_factor
%type <node> server_action server_list opt_ignore_server_list opt_server_list
%type <node> zone_action upgrade_action
%type <node> opt_index_options opt_primary opt_all opt_constraint_and_name constraint_and_name
%type <node> charset_key database_key charset_name charset_name_or_default collation_name trans_param_name trans_param_value
%type <node> set_names_stmt set_charset_stmt
%type <node> complex_string_literal literal number_literal cur_timestamp_func_params now_or_signed_literal signed_literal signed_literal_params
%type <node> create_tablegroup_stmt drop_tablegroup_stmt alter_tablegroup_stmt default_tablegroup
%type <node> set_transaction_stmt transaction_characteristics transaction_access_mode session_isolation_level isolation_level
%type <node> lock_tables_stmt unlock_tables_stmt lock_type lock_table_list lock_table opt_local
%type <node> flashback_stmt purge_stmt opt_flashback_rename_table opt_flashback_rename_database opt_flashback_rename_tenant
%type <node> tenant_name_list opt_tenant_list tenant_list_tuple cache_type flush_scope opt_zone_list
%type <node> opt_into_clause into_clause into_opt field_opt field_term field_term_list line_opt line_term line_term_list into_var_list into_var
%type <node> obj_access_ref opt_obj_access_ref opt_func_access_ref table_element_access_list table_index
%type <node> obj_access_ref_normal opt_obj_access_ref_normal opt_func_access_ref_normal
//%type <node> string_list text_string opt_bit_length_i
%type <node> balance_task_type opt_balance_task_type opt_cluster_type opt_primary_rootservice_list
%type <node> merge_update_clause merge_insert_clause opt_merge_update_delete opt_alias
%type <node> returning_exprs
%type <node> list_expr list_partition_element list_partition_expr list_partition_list list_partition_option opt_list_partition_list opt_list_subpartition_list list_subpartition_list list_subpartition_element
%type <node> primary_zone_name locality_name distribute_method opt_distribute_method
%type <node> split_actions add_range_or_list_partition add_range_or_list_subpartition special_partition_define special_partition_list opt_special_partition_list modify_special_partition split_list_partition split_range_partition drop_partition_name_list
%type <node> common_cursor_attribute cursor_attribute_expr
%type <node> cursor_attribute_bulk_rowcount cursor_attribute_bulk_exceptions
%type <node> implicit_cursor_attribute explicit_cursor_attribute
%type <node> alter_session_stmt current_schema alter_system_set_clause_list alter_system_set_clause set_system_parameter_clause alter_session_set_clause set_system_parameter_clause_list
%type <node> analyze_stmt analyze_statistics_clause opt_analyze_for_clause opt_analyze_for_clause_list opt_analyze_for_clause_element opt_analyze_sample_clause sample_option
%type <node> opt_func_param_list func_param_list func_param func_param_with_assign bool_pri_in_pl_func
%type <node> set_comment_stmt opt_using_index_clause opt_reference_option reference_option require_specification tls_option_list tls_option
%type <node> pl_expr_stmt
%type <ival> reference_action
%type <node> opt_cascade
%type <node> shrink_space_stmt
%type <node> tracing_num_list
%type <node> load_data_stmt opt_load_local opt_duplicate opt_load_charset opt_load_ignore_rows
%type <node> lines_or_rows opt_field_or_var_spec field_or_vars_list field_or_vars opt_load_set_spec
%type <node> load_set_list load_set_element load_data_with_opt_hint
%type <node> keystore_name
%type <node> create_tablespace_stmt alter_tablespace_stmt drop_tablespace_stmt tablespace opt_tablespace_option alter_tablespace_actions alter_tablespace_action
%type <node> permanent_tablespace permanent_tablespace_options permanent_tablespace_option
%type <node> opt_rely_option opt_enable_option enable_option opt_validate_option
%type <node> storage_options_list storage_option size_option int_or_unlimited opt_unit_of_size unit_of_size opt_physical_attributes_options physical_attributes_option_list physical_attributes_option opt_nologging
%type <node> opt_qb_name
%type <node> create_savepoint_stmt rollback_savepoint_stmt
%type <node> sys_and_obj_priv
%type <node> switch_option var_name_of_module var_name_of_forced_module
%type <ival> opt_multiset_modifier
%type <node> transpose_clause pivot_aggr_clause transpose_for_clause transpose_in_clause
%type <node> unpivot_column_clause unpivot_in_clause
%type <node> pivot_single_aggr_clause opt_as_alias transpose_in_args transpose_in_arg
%type <node> unpivot_in_args unpivot_in_arg opt_as_expr
%type <ival> opt_unpivot_include opt_of
%type <node> exists_function_name collection_predicate_expr
%type <node> opt_sql_throttle_for_priority opt_sql_throttle_using_cond sql_throttle_one_or_more_metrics sql_throttle_metric
%type <node> parallel_option
%type <node> translate_charset
%type <node> oracle_pl_non_reserved_words
%type <node> opt_primary_zone
%type <node> is_nan_inf_value
%type <node> method_opt method_list method for_all opt_indexed_hiddden opt_size_clause size_clause for_columns for_columns_list for_columns_item column_clause extension
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
  | merge_stmt             { $$ = $1; question_mark_issue($$, result); }
  | create_table_stmt       { $$ = $1; check_question_mark($$, result); }
  | alter_database_stmt     { $$ = $1; check_question_mark($$, result); }
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
  | call_stmt             { $$ = $1; question_mark_issue($$, result); }
  | alter_table_stmt        { $$ = $1; check_question_mark($$, result); $$->value_ = 0;}
  | alter_index_stmt        { $$ = $1; check_question_mark($$, result); $$->value_ = 1;}
  | alter_system_stmt       { $$ = $1; check_question_mark($$, result); }
  | audit_stmt              { $$ = $1; check_question_mark($$, result); }
  | deallocate_prepare_stmt { $$ = $1; check_question_mark($$, result); }
  | create_user_stmt        { $$ = $1; check_question_mark($$, result); }
  | alter_user_stmt         { $$ = $1; check_question_mark($$, result); }
  | alter_user_profile_stmt { $$ = $1; check_question_mark($$, result); }
  | drop_user_stmt          { $$ = $1; check_question_mark($$, result); }
  | set_password_stmt       { $$ = $1; check_question_mark($$, result); }
  | lock_user_stmt          { $$ = $1; check_question_mark($$, result); }
  | grant_stmt              { $$ = $1; check_question_mark($$, result); }
  | revoke_stmt             { $$ = $1; check_question_mark($$, result); }
  | begin_stmt              { $$ = $1; check_question_mark($$, result); }
  | commit_stmt             { $$ = $1; check_question_mark($$, result); }
  | rollback_stmt           { $$ = $1; check_question_mark($$, result); }
  | create_index_stmt       { $$ = $1; check_question_mark($$, result); }
  | drop_index_stmt         { $$ = $1; check_question_mark($$, result); }
  | kill_stmt               { $$ = $1; check_question_mark($$, result); }
  | help_stmt               { $$ = $1; check_question_mark($$, result); }
  | create_view_stmt        { $$ = $1; check_question_mark($$, result); }
  | create_tenant_stmt      { $$ = $1; check_question_mark($$, result); }
  | alter_tenant_stmt       { $$ = $1; check_question_mark($$, result); }
  | drop_tenant_stmt        { $$ = $1; check_question_mark($$, result); }
  | create_restore_point_stmt { $$ = $1; check_question_mark($$, result); }
  | drop_restore_point_stmt   { $$ = $1; check_question_mark($$, result); }
  | create_resource_stmt      { $$ = $1; check_question_mark($$, result); }
  | alter_resource_stmt       { $$ = $1; check_question_mark($$, result); }
  | drop_resource_stmt        { $$ = $1; check_question_mark($$, result); }
  | set_names_stmt        { $$ = $1; check_question_mark($$, result); }
  | set_charset_stmt        { $$ = $1; check_question_mark($$, result); }
  | create_tablegroup_stmt { $$ = $1; check_question_mark($$, result); }
  | drop_tablegroup_stmt { $$ = $1; check_question_mark($$, result); }
  | alter_tablegroup_stmt   { $$ = $1; check_question_mark($$, result); }
  | rename_table_stmt       { $$ = $1; check_question_mark($$, result); }
  | truncate_table_stmt     { $$ = $1; check_question_mark($$, result); }
  | set_transaction_stmt    { $$ = $1; check_question_mark($$, result); }
  | create_synonym_stmt     { $$ = $1; check_question_mark($$, result); }
  | drop_synonym_stmt       { $$ = $1; check_question_mark($$, result); }
  | create_directory_stmt   { $$ = $1; check_question_mark($$, result); }
  | drop_directory_stmt     { $$ = $1; check_question_mark($$, result); }
  | create_keystore_stmt    { $$ = $1; check_question_mark($$, result); }
  | alter_keystore_stmt     { $$ = $1; check_question_mark($$, result); }
  | create_tablespace_stmt  { $$ = $1; check_question_mark($$, result); }
  | drop_tablespace_stmt    { $$ = $1; check_question_mark($$, result); }
  | alter_tablespace_stmt   { $$ = $1; check_question_mark($$, result); }
  | create_savepoint_stmt   { $$ = $1; check_question_mark($$, result); }
  | rollback_savepoint_stmt { $$ = $1; check_question_mark($$, result); }
  | lock_tables_stmt
  /* banliu.zyd: 该语句为兼容mysqldump的空实现，置为非0使empty
     query resolver不会报错， 这样做失去了原来保存question mark
     的值，不过按代码逻辑这个值本身应该总为0, 且该值对本语句无意义*/
  { $$ = $1; check_question_mark($$, result); $$->value_ = 1; }
  | unlock_tables_stmt
   /* banliu.zyd: 该语句为兼容mysqldump的空实现，置为非0使empty
      query resolver不会报错， 这样做失去了原来保存question mark
      的值，不过按代码逻辑这个值本身应该总为0, 且该值对本语句无意义 */
  { $$ = $1; check_question_mark($$, result); $$->value_ = 1; }
  | flashback_stmt          { $$ = $1; check_question_mark($$, result); }
  | purge_stmt              { $$ = $1; check_question_mark($$, result); }
  | create_sequence_stmt    { $$ = $1; check_question_mark($$, result); }
  | alter_sequence_stmt     { $$ = $1; check_question_mark($$, result); }
  | drop_sequence_stmt      { $$ = $1; check_question_mark($$, result); }
  | alter_session_stmt      { $$ = $1; check_question_mark($$, result); }
  | analyze_stmt            { $$ = $1; check_question_mark($$, result); }
  | set_comment_stmt        { $$ = $1; check_question_mark($$, result); }
  | pl_expr_stmt            { $$ = $1; question_mark_issue($$, result); }
  | shrink_space_stmt       { $$ = $1; check_question_mark($$, result); }
  | load_data_stmt          { $$ = $1; check_question_mark($$, result); }
  | create_dblink_stmt      { $$ = $1; check_question_mark($$, result); }
  | drop_dblink_stmt        { $$ = $1; check_question_mark($$, result); }
  /*| with_select_stmt        { $$ = $1; check_question_mark($$, result); }*/
  | create_role_stmt        { $$ = $1; check_question_mark($$, result); }
  | drop_role_stmt          { $$ = $1; check_question_mark($$, result); }
  | alter_role_stmt         { $$ = $1; check_question_mark($$, result); }
  | set_role_stmt           { $$ = $1; check_question_mark($$, result); }
  | create_profile_stmt     { $$ = $1; check_question_mark($$, result); }
  | alter_profile_stmt      { $$ = $1; check_question_mark($$, result); }
  | drop_profile_stmt       { $$ = $1; check_question_mark($$, result); }
  | method_opt              { $$ = $1; check_question_mark($$, result); }
  ;

// 专门为了解析pl的expr mock的语句类型
pl_expr_stmt:
    DO expr
    {
      $$ = NULL;
      if (!result->pl_parse_info_.is_pl_parse_) {
        yyerror(NULL, result, "pl expr stmt not in pl context\n");
        YYABORT_PARSE_SQL_ERROR;
      } else if (!result->pl_parse_info_.is_pl_parse_expr_) {
        yyerror(NULL, result, "pl expr stmt not in parser expr context");
        YYABORT_PARSE_SQL_ERROR;
      } else if ($2 == NULL) {
        yyerror(NULL, result, "pl expr parser get null\n");
        YYABORT_PARSE_SQL_ERROR;
      } else {
        malloc_non_terminal_node($$, result->malloc_pool_, T_DEFAULT, 1, $2);
      }
    }
;

/*****************************************************************************
*
*	expression grammar
*
*****************************************************************************/

expr_list:
    bit_expr { $$ = $1; }
  | expr_list ',' bit_expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

column_ref:
column_name
{
  $$ = $1;
}
/*添加下面特殊的保留关键字允许在pl模式下作为表名列名使用是因为下面的关键字在oracle的pl中能够使用，由于ob pl parser
  的特殊性，部分sql会在sql parser中解析，导致在pl是非保留关键字，但是在sql中是保留关键字的无法解析，因此这里添加*/
| LEVEL
{
  make_name_node($$, result->malloc_pool_, "LEVEL");
  $$->value_ = 2;//区别于双引号中的"LEVEL"
}
| ROWNUM
{
  make_name_node($$, result->malloc_pool_, "ROWNUM");
  $$->value_ = 2;//区别于双引号中的"ROWNUM"
}
| oracle_pl_non_reserved_words
{
  if (!result->pl_parse_info_.is_pl_parse_) {
    yyerror(&@1, result, "row used to alias_name only support in pl mode\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$ = $1;
  }
}
;

/*由于pl解析部分会进入到sql中进行解析，部分sql中的保留关键字可以在pl中使用*/
oracle_pl_non_reserved_words:
ACCESS
{
  make_name_node($$, result->malloc_pool_, "ACCESS");
}
| ADD
{
  make_name_node($$, result->malloc_pool_, "ADD");
}
| AUDIT
{
  make_name_node($$, result->malloc_pool_, "AUDIT");
}
| CHAR
{
  make_name_node($$, result->malloc_pool_, "CHAR");
}
| COLUMN
{
  make_name_node($$, result->malloc_pool_, "COLUMN");
}
| COMMENT
{
  make_name_node($$, result->malloc_pool_, "COMMENT");
}
| CURRENT
{
  make_name_node($$, result->malloc_pool_, "CURRENT");
}
| DATE
{
  make_name_node($$, result->malloc_pool_, "DATE");
}
| DECIMAL
{
  make_name_node($$, result->malloc_pool_, "DECIMAL");
}
| FILE_KEY
{
  make_name_node($$, result->malloc_pool_, "FILE");
}
| FLOAT
{
  make_name_node($$, result->malloc_pool_, "FLOAT");
}
| IMMEDIATE
{
  make_name_node($$, result->malloc_pool_, "IMMEDIATE");
}
| INCREMENT
{
  make_name_node($$, result->malloc_pool_, "INCREMENT");
}
| INITIAL_
{
  make_name_node($$, result->malloc_pool_, "INITIAL");
}
| INTEGER
{
  make_name_node($$, result->malloc_pool_, "INTEGER");
}
| LONG
{
  make_name_node($$, result->malloc_pool_, "LONG");
}
| MAXEXTENTS
{
  make_name_node($$, result->malloc_pool_, "MAXEXTENTS");
}
| MODIFY
{
  make_name_node($$, result->malloc_pool_, "MODIFY");
}
| NOAUDIT
{
  make_name_node($$, result->malloc_pool_, "NOAUDIT");
}
| NOTFOUND
{
  make_name_node($$, result->malloc_pool_, "NOTFOUND");
}
| NUMBER
{
  make_name_node($$, result->malloc_pool_, "NUMBER");
}
| OFFLINE
{
  make_name_node($$, result->malloc_pool_, "OFFLINE");
}
| ONLINE
{
  make_name_node($$, result->malloc_pool_, "ONLINE");
}
| PCTFREE
{
  make_name_node($$, result->malloc_pool_, "PCTFREE");
}
| PRIVILEGES
{
  make_name_node($$, result->malloc_pool_, "PRIVILEGES");
}
| RAW
{
  make_name_node($$, result->malloc_pool_, "RAW");
}
| RENAME
{
  make_name_node($$, result->malloc_pool_, "RENAME");
}
| ROW
{
  make_name_node($$, result->malloc_pool_, "ROW");
}
| ROWLABEL
{
  make_name_node($$, result->malloc_pool_, "ROWLABEL");
}
| ROWS
{
  make_name_node($$, result->malloc_pool_, "ROWS");
}
| SESSION
{
  make_name_node($$, result->malloc_pool_, "SESSION");
}
| SET
{
  make_name_node($$, result->malloc_pool_, "SET");
}
| SMALLINT
{
  make_name_node($$, result->malloc_pool_, "SMALLINT");
}
| SUCCESSFUL
{
  make_name_node($$, result->malloc_pool_, "SUCCESSFUL");
}
| SYNONYM
{
  make_name_node($$, result->malloc_pool_, "SYNONYM");
}
| TRIGGER
{
  make_name_node($$, result->malloc_pool_, "TRIGGER");
}
| VALIDATE
{
  make_name_node($$, result->malloc_pool_, "VALIDATE");
}
| VARCHAR
{
  make_name_node($$, result->malloc_pool_, "VARCHAR");
}
| VARCHAR2
{
  make_name_node($$, result->malloc_pool_, "VARCHAR2");
}
| WHENEVER
{
  make_name_node($$, result->malloc_pool_, "WHENEVER");
}
| DUAL
{
  make_name_node($$, result->malloc_pool_, "DUAL");
}
/* 打开有规约冲突，暂时解决不了
| PRIOR
{
  make_name_node($$, result->malloc_pool_, "PRIOR");
}
| CONNECT_BY_ROOT
{
  make_name_node($$, result->malloc_pool_, "CONNECT_BY_ROOT");
}
*/
;


/*
| relation_name '.' column_name
  {
    malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, $1, $3);
    dup_node_string($3, $$, result->malloc_pool_);
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
*/
;

/* literal string with  */
complex_string_literal:
STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_,
                           (T_NCHAR ==  $1->type_) ? T_NCHAR : T_CHAR, 2, NULL, $1);
  $$->str_value_ = $1->str_value_;
  $$->str_len_ = $1->str_len_;
  $$->raw_text_ = $1->raw_text_;
  $$->text_len_ = $1->text_len_;
  $$->pos_ = $1->pos_;
  $$->is_copy_raw_text_ = $1->is_copy_raw_text_;
  if (T_NCHAR !=  $1->type_) {
    $$->is_change_to_char_ = 1;
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
| NULLX { $$ = $1; }
| INTERVAL_VALUE { $$ = $1; }
;

number_literal:
INTNUM { $$ = $1; $$->param_num_ = 1;}
| DECIMAL_VAL { $$ = $1; $$->param_num_ = 1;}
;

expr_const:
literal { $$ = $1; }
| SYSTEM_VARIABLE { $$ = $1; }
| QUESTIONMARK { $$ = $1; }
| GLOBAL_ALIAS '.' column_name
{
  $3->type_ = T_SYSTEM_VARIABLE;
  $3->value_ = 1;
  $$ = $3;
}
| SESSION_ALIAS '.' column_name
{
  $3->type_ = T_SYSTEM_VARIABLE;
  $3->value_ = 2;
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
| GLOBAL_ALIAS '.' column_name
{
  $3->type_ = T_SYSTEM_VARIABLE;
  $3->value_ = 1;
  $$ = $3;
}
| SESSION_ALIAS '.' column_name
{
  $3->type_ = T_SYSTEM_VARIABLE;
  $3->value_ = 2;
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

bool_pri:
bit_expr IS NULLX %prec IS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr IS is_nan_inf_value %prec IS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr IS not NULLX %prec IS
{
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr IS not is_nan_inf_value %prec IS
{
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr COMP_LE bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LE, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr COMP_LE sub_query_flag bit_expr
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, $3->type_, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LE, 2, $1, sub_expr);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr COMP_LT COMP_EQ bit_expr /*基本比较符<=的拆分版本， 将其拆分为'<', '='以支持基本比较符中间可以出现空格*/
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LE, 2, $1, $4);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr COMP_LT COMP_EQ sub_query_flag bit_expr %prec COMP_LE /*基本比较符<=的拆分版本，
                                               将其拆分为'<', '='以支持基本比较符中间可以出现空格*/
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, $4->type_, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LE, 2, $1, sub_expr);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @5.last_column),
            &@1, result);
}
| bit_expr COMP_LE SOME bit_expr
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, T_ANY, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LE, 2, $1, sub_expr);
}
| bit_expr COMP_LT bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LT, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr COMP_LT sub_query_flag bit_expr
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, $3->type_, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LT, 2, $1, sub_expr);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr COMP_LT SOME bit_expr
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, T_ANY, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LT, 2, $1, sub_expr);
}
| bit_expr COMP_EQ bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr COMP_EQ sub_query_flag bit_expr
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, $3->type_, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, sub_expr);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr COMP_EQ SOME bit_expr
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, T_ANY, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, sub_expr);
}
| bit_expr COMP_GE bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GE, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr COMP_GE sub_query_flag bit_expr
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, $3->type_, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GE, 2, $1, sub_expr);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr COMP_GT COMP_EQ bit_expr %prec COMP_GE/*基本比较符>=的拆分版本， 将其拆分为'>', '='以支持基本比较符中间可以出现空格*/
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GE, 2, $1, $4);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr COMP_GT COMP_EQ sub_query_flag bit_expr %prec COMP_GE/*基本比较符>=的拆分版本，
                                            将其拆分为'>', '='以支持基本比较符中间可以出现空格*/
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, $4->type_, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GE, 2, $1, sub_expr);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @5.last_column),
            &@1, result);
}
| bit_expr COMP_GE SOME bit_expr
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, T_ANY, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GE, 2, $1, sub_expr);
}
| bit_expr COMP_GT bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GT, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr COMP_GT sub_query_flag bit_expr
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, $3->type_, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GT, 2, $1, sub_expr);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr COMP_GT SOME bit_expr
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, T_ANY, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GT, 2, $1, sub_expr);
}
| bit_expr COMP_NE bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr COMP_NE sub_query_flag bit_expr
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, $3->type_, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, sub_expr);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr COMP_LT COMP_GT bit_expr %prec COMP_NE/*基本比较符<>的拆分版本， 将其拆分为'<', '>'以支持基本比较符中间可以出现空格*/
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, $4);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr COMP_LT COMP_GT sub_query_flag bit_expr %prec COMP_NE/*基本比较符<>的拆分版本，
                                          将其拆分为'<', '>'以支持基本比较符中间可以出现空格*/
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, $4->type_, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, sub_expr);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @5.last_column),
            &@1, result);
}
| bit_expr '!' COMP_EQ bit_expr %prec COMP_NE/*基本比较符!=的拆分版本， 将其拆分为'!', '='以支持基本比较符中间可以出现空格*/
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, $4);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr '!' COMP_EQ sub_query_flag bit_expr %prec COMP_NE/*基本比较符!=的拆分版本，
                                     将其拆分为'!', '='以支持基本比较符中间可以出现空格*/
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, $4->type_, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, sub_expr);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @5.last_column),
            &@1, result);
}
| bit_expr '^' COMP_EQ bit_expr %prec COMP_NE/*基本比较符^=的拆分版本， 将其拆分为'^', '='以支持基本比较符中间可以出现空格*/
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, $4);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr '^' COMP_EQ sub_query_flag bit_expr %prec COMP_NE/*基本比较符^=的拆分版本，
                                    将其拆分为'^', '='以支持基本比较符中间可以出现空格*/
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, $4->type_, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, sub_expr);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @5.last_column),
            &@1, result);
}
| bit_expr COMP_NE SOME bit_expr
{
  ParseNode *sub_expr = NULL;
  malloc_non_terminal_node(sub_expr, result->malloc_pool_, T_ANY, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, sub_expr);
}
| bit_expr COMP_NE_PL bit_expr %prec COMP_NE_PL
{
  if (!result->pl_parse_info_.is_pl_parse_expr_) {
    yyerror(NULL, result, "~= only support in pl mode\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, $3);
  }
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
|
predicate {
  $$ = $1;
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @1.last_column),
            &@1, result);
}
;

predicate:
LNNVL '(' bool_pri ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_LNNVL, 1, $3);
}
| bit_expr IN in_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IN, 2, $1, $3);
}
| bit_expr not IN in_expr
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_IN, 2, $1, $4);
}
| bit_expr not BETWEEN bit_expr AND bit_expr  %prec BETWEEN
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_BTW, 3, $1, $4, $6);
}
| bit_expr BETWEEN bit_expr AND bit_expr %prec BETWEEN
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_BTW, 3, $1, $3, $5);
}
| bit_expr LIKE bit_expr
{
  //在resolver时，如果发现只有两个children，会将escape 参数设置为‘\’
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LIKE, 2, $1, $3);
}
| bit_expr LIKE bit_expr ESCAPE bit_expr %prec LIKE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LIKE, 3, $1, $3, $5);
}
| bit_expr not LIKE bit_expr
{
  (void)($2);
  //在resolver时，如果发现只有两个children，会将escape 参数设置为‘\’
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_LIKE, 2, $1, $4);
}
| bit_expr not LIKE bit_expr ESCAPE bit_expr %prec LIKE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_LIKE, 3, $1, $4, $6);
}
| REGEXP_LIKE '(' substr_params ')'
{
  (void)($1);               /* unused */
  make_name_node($$, result->malloc_pool_, "regexp_like");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_REGEXP_LIKE, 2, $$, $3);
}
| exists_function_name select_with_parens
{
  (void)($1);
  if ($2->children_[PARSE_SELECT_ORDER] != NULL && $2->children_[PARSE_SELECT_FETCH] == NULL) {
    yyerror(NULL, result, "only order by clause can't occur subquery\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EXISTS, 1, $2);
  }
}
| collection_predicate_expr
{
  $$ = $1;
}
;

opt_of:
  { $$[0] = 0;}
| OF
{
  $$[0] = 0;
}
;

collection_predicate_expr:
bit_expr MEMBER opt_of bit_expr
{
  (void)($2);
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_COLL_PRED, 2, $1, $4);
  $$->int32_values_[1] = 4;
  $$->int32_values_[0] = -1;
}
| bit_expr NOT MEMBER opt_of bit_expr
{
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_COLL_PRED, 2, $1, $5);
  $$->int32_values_[1] = 4;
  $$->int32_values_[0] = -1;
}
| bit_expr SUBMULTISET opt_of bit_expr
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_COLL_PRED, 2, $1, $4);
  $$->int32_values_[1] = 3;
  $$->int32_values_[0] = -1;
}
| bit_expr NOT SUBMULTISET opt_of bit_expr
{
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_COLL_PRED, 2, $1, $5);
  $$->int32_values_[1] = 3;
  $$->int32_values_[0] = 2;
}
| bit_expr IS A SET
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_COLL_PRED, 2, $1, $1);
  $$->int32_values_[1] = 5;
  $$->int32_values_[0] = -1;
}
| bit_expr IS NOT A SET
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_COLL_PRED, 2, $1, $1);
  $$->int32_values_[1] = 5;
  $$->int32_values_[0] = 2;
}
| bit_expr IS EMPTY
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_COLL_PRED, 2, $1, $1);
  $$->int32_values_[1] = 6;
  $$->int32_values_[0] = -1;
}
| bit_expr IS NOT EMPTY
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_COLL_PRED, 2, $1, $1);
  $$->int32_values_[1] = 6;
  $$->int32_values_[0] = 2;
}
;

bit_expr:
  bit_expr '+' bit_expr %prec '+'
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
| bit_expr CNNOP bit_expr %prec CNNOP
  {
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_CNN, 2, $1, $3);
    check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
  }
| bit_expr AT TIME ZONE bit_expr %prec AT
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $1, $5);
  make_name_node($$, result->malloc_pool_, "AT_TIME_ZONE");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| bit_expr AT LOCAL %prec AT
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $1);
  make_name_node($$, result->malloc_pool_, "AT_LOCAL");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| bit_expr MULTISET_OP opt_multiset_modifier bit_expr %prec MULTISET_OP
  {
    const char *type_name = $2->str_value_;
    int64_t len = $2->str_len_;
    int64_t tlen1 = (int64_t)sizeof("UNION");
    int64_t tlen2 = (int64_t)sizeof("INTERSECT");
    int64_t tlen3 = (int64_t)sizeof("EXCEPT");
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MULTISET, 2, $1, $4);
    // ObMultiSetModifier
    $$->int32_values_[0] = $3[0];
    // ObMultiSetTypec
    if(0 == strncmp("UNION", type_name, len <= tlen1 ? len : tlen1)) {
      $$->int32_values_[1] = 0;
    } else if (0 == strncmp("INTERSECT", type_name, len <= tlen2 ? len : tlen2)) {
      $$->int32_values_[1] = 1;
    } else if (0 == strncmp("EXCEPT", type_name, len <= tlen3 ? len : tlen3)) {
      $$->int32_values_[1] = 2;
    } else {
      $$->int32_values_[1] = -1;
    }
    check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
  }
  | bit_expr POW_PL bit_expr %prec POW_PL
{
  if (!result->pl_parse_info_.is_pl_parse_expr_) {
    yyerror(NULL, result, "** operator only support in pl mode\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    ParseNode *pow_ident = NULL;
    ParseNode *params = NULL;
    malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $1, $3);
    make_name_node(pow_ident, result->malloc_pool_, "power");
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, pow_ident, params);
  }
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bit_expr MOD bit_expr %prec MOD
{
  if (!result->pl_parse_info_.is_pl_parse_expr_) {
    yyerror(NULL, result, " x mod y expression only support in pl mode\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MOD, 2, $1, $3);
  }
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| unary_expr %prec LOWER_THAN_NEG
  {
    $$ = $1;
    check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @1.last_column),
              &@1, result);
  }
| BOOL_VALUE
  {
    if (!(result->pl_parse_info_.is_pl_parse_
          && result->pl_parse_info_.is_pl_parse_expr_)
         && !result->may_bool_value_) {
      result->line_ -= 1;
      yyerror(NULL, result, "expr not accept boolean value");
      YYABORT_PARSE_SQL_ERROR;
    }
    $$ = $1;
    check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @1.last_column),
              &@1, result);
  }
;

is_nan_inf_value:
NAN_VALUE
{
    malloc_terminal_node($$, result->malloc_pool_, T_IEEE754_NAN);
    $$->value_ = 1;
}
| INFINITE_VALUE
{
    malloc_terminal_node($$, result->malloc_pool_, T_IEEE754_INFINITE);
    $$->value_ = 1;
}



opt_multiset_modifier:
{ $$[0] = 0; }
| ALL
{ $$[0] = 0; }
| DISTINCT
{ $$[0] = 1; }
;

unary_expr:
'+' simple_expr %prec NEG
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_POS, 1, $2);
}
| '-' simple_expr %prec NEG
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NEG, 1, $2);
}
| simple_expr %prec NEG
{
$$ = $1;
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
/*
| ROWNUM
{
  make_name_node($$, result->malloc_pool_, "rownum");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $$);
}
*/
| obj_access_ref COLUMN_OUTER_JOIN_SYMBOL
{
  if (NULL == $1 || (NULL != $1->children_[1] && NULL != $1->children_[1]->children_[1] && NULL != $1->children_[1]->children_[1]->children_[1])) {
    yyerror(NULL, result, "Error column;\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    ParseNode *column_ref_node = NULL;
    ParseNode *column_name = NULL == $1->children_[1] ? $1->children_[0] : (NULL == $1->children_[1]->children_[1] ? $1->children_[1]->children_[0] : $1->children_[1]->children_[1]->children_[0]);
    ParseNode *relation_name = NULL == $1->children_[1] ? NULL : (NULL == $1->children_[1]->children_[1] ? $1->children_[0] : $1->children_[1]->children_[0]);
    ParseNode *db_name = NULL == $1->children_[1] ? NULL : (NULL == $1->children_[1]->children_[1] ? NULL : $1->children_[0]);
    malloc_non_terminal_node(column_ref_node, result->malloc_pool_, T_COLUMN_REF, 3, db_name, relation_name, column_name);
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_ORACLE_OUTER_JOIN_SYMBOL, 1, column_ref_node);
  }
}
| expr_const { $$ = $1; }
| select_with_parens %prec NEG
{
  if ($1->children_[PARSE_SELECT_ORDER] != NULL && $1->children_[PARSE_SELECT_FETCH] == NULL) {
    yyerror(NULL, result, "only order by clause can't occur subquery\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$ = $1;
  }
}
| CURSOR '(' select_stmt ')'
{
  $3->value_ = 1; //means cursor expression
  $$ = $3;
}
| '(' bit_expr ')'
{
  merge_nodes($$, result, T_EXPR_LIST, $2);
}
| '(' expr_list ',' bit_expr ')'
{
  ParseNode *node = NULL;
  malloc_non_terminal_node(node, result->malloc_pool_, T_LINK_NODE, 2, $2, $4);
  merge_nodes($$, result, T_EXPR_LIST, node);
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
| obj_access_ref %prec LOWER_OVER
{
  if ($1 != NULL && $1->type_ == T_OBJ_ACCESS_REF && $1->num_child_ == 2 &&
      $1->children_[0] != NULL && $1->children_[1] == NULL && $1->children_[0]->num_child_ == 0) {
    ParseNode *obj_node = $1->children_[0];
    if (obj_node->value_ != 1 && nodename_equal(obj_node, "SYSTIMESTAMP", 12)) {
      malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_SYSTIMESTAMP, 1, NULL);
    } else if (obj_node->value_ != 1 && nodename_equal(obj_node, "CURRENT_DATE", 12)) {
      malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_DATE, 1, NULL);
    } else if (obj_node->value_ != 1 && nodename_equal(obj_node, "LOCALTIMESTAMP", 14)) {
      malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_LOCALTIMESTAMP, 1, NULL);
    } else if (obj_node->value_ != 1 && nodename_equal(obj_node, "CURRENT_TIMESTAMP", 17)) {
      malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIMESTAMP, 1, NULL);
    } else if (obj_node->value_ != 1 && nodename_equal(obj_node, "SESSIONTIMEZONE", 15)) {
      make_name_node($$, result->malloc_pool_, "SESSIONTIMEZONE");
      malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $$);
    } else if (obj_node->value_ != 1 && nodename_equal(obj_node, "DBTIMEZONE", 10)) {
      make_name_node($$, result->malloc_pool_, "DBTIMEZONE");
      malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $$);
    } else if (obj_node->value_ != 1 && nodename_equal(obj_node, "CONNECT_BY_ISLEAF", 17)) {
      malloc_terminal_node($$, result->malloc_pool_, T_CONNECT_BY_ISLEAF);
      malloc_non_terminal_node($$, result->malloc_pool_, T_PSEUDO_COLUMN, 1, $$);
    } else if (obj_node->value_ != 1 && nodename_equal(obj_node, "CONNECT_BY_ISCYCLE", 18)) {
      malloc_terminal_node($$, result->malloc_pool_, T_CONNECT_BY_ISCYCLE);
      malloc_non_terminal_node($$, result->malloc_pool_, T_PSEUDO_COLUMN, 1, $$);
    } else if (nodename_equal(obj_node, "BINARY_DOUBLE_INFINITY", 22)
                && obj_node->value_ != 1) {
      malloc_terminal_node($$, result->malloc_pool_, T_DOUBLE);
      $$->str_len_ = strlen("BINARY_DOUBLE_INFINITY");
      $$->str_value_ = parse_strdup("BINARY_DOUBLE_INFINITY", result->malloc_pool_, &($$->str_len_));
      $$->text_len_ = $$->str_len_;
      $$->raw_text_ = $$->str_value_;
    } else if (nodename_equal(obj_node, "BINARY_DOUBLE_NAN", 17)
                && obj_node->value_ != 1) {
      malloc_terminal_node($$, result->malloc_pool_, T_DOUBLE);
      $$->str_len_ = strlen("BINARY_DOUBLE_NAN");
      $$->str_value_ = parse_strdup("BINARY_DOUBLE_NAN", result->malloc_pool_, &($$->str_len_));
      $$->text_len_ = $$->str_len_;
      $$->raw_text_ = $$->str_value_;
    } else if (nodename_equal(obj_node, "BINARY_FLOAT_INFINITY", 21)
                && obj_node->value_ != 1) {
      malloc_terminal_node($$, result->malloc_pool_, T_FLOAT);
      $$->str_len_ = strlen("BINARY_FLOAT_INFINITY");
      $$->str_value_ = parse_strdup("BINARY_FLOAT_INFINITY", result->malloc_pool_, &($$->str_len_));
      $$->text_len_ = $$->str_len_;
      $$->raw_text_ = $$->str_value_;
    } else if (nodename_equal(obj_node, "BINARY_FLOAT_NAN", 16)
                && obj_node->value_ != 1) {
      malloc_terminal_node($$, result->malloc_pool_, T_FLOAT);
      $$->str_len_ = strlen("BINARY_FLOAT_NAN");
      $$->str_value_ = parse_strdup("BINARY_FLOAT_NAN", result->malloc_pool_, &($$->str_len_));
      $$->text_len_ = $$->str_len_;
      $$->raw_text_ = $$->str_value_;
    } else if (!result->pl_parse_info_.is_pl_parse_expr_) {
      ParseNode *level_node = NULL;
      ParseNode *rownum_node = NULL;
      make_name_node(level_node, result->malloc_pool_, "LEVEL");
      make_name_node(rownum_node, result->malloc_pool_, "ROWNUM");
      level_node->value_ = 2;
      rownum_node->value_ = 2;
      if (parsenode_equal(obj_node, level_node, &result->extra_errno_)) {
        malloc_terminal_node($$, result->malloc_pool_, T_LEVEL);
        malloc_non_terminal_node($$, result->malloc_pool_, T_PSEUDO_COLUMN, 1, $$);
      } else if (parsenode_equal(obj_node, rownum_node, &result->extra_errno_)) {
        make_name_node($$, result->malloc_pool_, "rownum");
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $$);
      } else {
        $$ = $1;
      }
      check_parser_size_overflow(result->extra_errno_);
    } else {
      $$ = $1;
    }
  } else {
    $$ = $1;
  }
}
| sql_function %prec LOWER_OVER
{
  $$ = $1;
}
| cursor_attribute_expr
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
| PLSQL_VARIABLE
{
  $1->int32_values_[0] += result->pl_parse_info_.plsql_line_ + 1;
  $1->int16_values_[2] = @1.first_column;
  $1->int16_values_[3] = @1.last_column;
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_PLSQL_VARIABLE, 1, $1);
}
| PRIOR unary_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_PRIOR, 1, $2);
}
| CONNECT_BY_ROOT unary_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_CONNECT_BY_ROOT, 1, $2);
}
/*
| LEVEL
{
  malloc_terminal_node($$, result->malloc_pool_, T_LEVEL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PSEUDO_COLUMN, 1, $$);
}
*/
/* 下面两个语法已经支持，由于和obj_access_ref语法规则有规约冲突，因此注释了，对应的语法规则在解析obj_access_ref语
   法规则时进行了特殊处理支持，处理细节见ob_raw_expr_resolver_impl.cpp文件中的process_obj_access_node函数
| CONNECT_BY_ISLEAF
{
  malloc_terminal_node($$, result->malloc_pool_, T_CONNECT_BY_ISLEAF);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PSEUDO_COLUMN, 1, $$);
}
| CONNECT_BY_ISCYCLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_CONNECT_BY_ISCYCLE);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PSEUDO_COLUMN, 1, $$);
}
*/
| SET '(' bit_expr ')'
{
  ParseNode *name = NULL;
  make_name_node(name, result->malloc_pool_, "set");
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, name, expr_list);
}
/* | ORA_ROWSCN */
/* { */
  /* malloc_terminal_node($$, result->malloc_pool_, T_ORA_ROWSCN); */
  /* malloc_non_terminal_node($$, result->malloc_pool_, T_PSEUDO_COLUMN, 1, $$); */
/* } */
;

common_cursor_attribute:
    ISOPEN { malloc_terminal_node($$, result->malloc_pool_, T_SP_CURSOR_ISOPEN); }
  | FOUND { malloc_terminal_node($$, result->malloc_pool_, T_SP_CURSOR_FOUND); }
  | NOTFOUND { malloc_terminal_node($$, result->malloc_pool_, T_SP_CURSOR_NOTFOUND); }
  | ROWCOUNT { malloc_terminal_node($$, result->malloc_pool_, T_SP_CURSOR_ROWCOUNT); }
;

cursor_attribute_bulk_rowcount:
    BULK_ROWCOUNT '(' bit_expr ')'
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SP_CURSOR_BULK_ROWCOUNT, 1, $3);
    }
;

cursor_attribute_bulk_exceptions:
    BULK_EXCEPTIONS '.' COUNT
    {
      malloc_terminal_node($$, result->malloc_pool_, T_SP_CURSOR_BULK_EXCEPTIONS_COUNT);
    }
  | BULK_EXCEPTIONS '(' bit_expr ')' '.' ERROR_INDEX
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SP_CURSOR_BULK_EXCEPTIONS, 1, $3);
      $$->value_ = 0;
    }
  | BULK_EXCEPTIONS '(' bit_expr ')' '.' ERROR_CODE
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SP_CURSOR_BULK_EXCEPTIONS, 1, $3);
      $$->value_ = 1;
    }
;

implicit_cursor_attribute:
    SQL '%' common_cursor_attribute
    {
      malloc_terminal_node($$, result->malloc_pool_, T_SP_IMPLICIT_CURSOR_ATTR);
      $$->value_ = $3->type_;
    }
  | SQL '%' cursor_attribute_bulk_rowcount
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SP_IMPLICIT_CURSOR_ATTR, 1, $3);
    }
  | SQL '%' cursor_attribute_bulk_exceptions
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SP_IMPLICIT_CURSOR_ATTR, 1, $3);
    }
;

explicit_cursor_attribute:
    obj_access_ref '%' common_cursor_attribute
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SP_EXPLICIT_CURSOR_ATTR, 1, $1);
      $$->value_ = $3->type_;
    }
;

cursor_attribute_expr:
    explicit_cursor_attribute
    {
      if (result->pl_parse_info_.is_pl_parse_) {
        $$ = $1;
      } else {
        yyerror(NULL, result, "cursor not in pl context;");
        YYABORT_PARSE_SQL_ERROR;
      }
    }
  | implicit_cursor_attribute
    {
      if (result->pl_parse_info_.is_pl_parse_) {
        $$ = $1;
      } else {
        yyerror(NULL, result, "cursor not in pl context;");
        YYABORT_PARSE_SQL_ERROR;
      }
    }
;
obj_access_ref:
    column_ref opt_obj_access_ref
    {
      if (NULL != $2) {
        ParseNode *level_node = NULL;
        make_name_node(level_node, result->malloc_pool_, "LEVEL");
        level_node->value_ = 2;
        if (parsenode_equal($1, level_node, &result->extra_errno_) &&
          !result->pl_parse_info_.is_pl_parse_expr_) {
          yyerror(NULL, result, "LEVEL not in pl context;");
          YYABORT_PARSE_SQL_ERROR;
        } else if (T_STAR == $2->type_) {
          malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, $1, $2);
          $$->value_ = 0;
        } else if (T_COLUMN_REF == $2->type_) {
          if (NULL == $2->children_[0]) {
            $$ = $2;
            $$->children_[0] = $1;
          } else {
            yyerror(NULL, result, "Error STAR;\n");
            YYABORT_PARSE_SQL_ERROR;
          }
        } else if ($2->type_ == T_OBJ_ACCESS_REF && $2->num_child_ == 2 &&
                   $2->children_[0] != NULL && $2->children_[0]->value_ == 2 &&
                   parsenode_equal($2->children_[0], level_node, &result->extra_errno_) &&
                   !result->pl_parse_info_.is_pl_parse_expr_) {
          yyerror(NULL, result, "LEVEL not in pl context;");
          YYABORT_PARSE_SQL_ERROR;
        } else {
          malloc_non_terminal_node($$, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, $1, $2);
        }
        check_parser_size_overflow(result->extra_errno_);
      } else {
        malloc_non_terminal_node($$, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, $1, $2);
      }
    }
  | access_func_expr opt_func_access_ref
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, $1, $2);
    }
  | column_ref '.' FIRST '(' ')' /*解决first last带括号的问题*/
  {
    ParseNode *level_node = NULL;
    ParseNode *id_node = NULL;
    ParseNode *obj_id_node = NULL;
    make_name_node(level_node, result->malloc_pool_, "LEVEL");
    level_node->value_ = 2;
    make_name_node(id_node, result->malloc_pool_, "first");
    if (parsenode_equal($1, level_node, &result->extra_errno_) &&
        !result->pl_parse_info_.is_pl_parse_expr_) {
      yyerror(NULL, result, "LEVEL not in pl context;");
      YYABORT_PARSE_SQL_ERROR;
    } else {
      malloc_non_terminal_node(obj_id_node, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, id_node, NULL);
      malloc_non_terminal_node($$, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, $1, obj_id_node);
    }
    check_parser_size_overflow(result->extra_errno_);
  }
  | column_ref '.' LAST '(' ')'
  {
    ParseNode *level_node = NULL;
    ParseNode *id_node = NULL;
    ParseNode *obj_id_node = NULL;
    make_name_node(level_node, result->malloc_pool_, "LEVEL");
    level_node->value_ = 2;
    make_name_node(id_node, result->malloc_pool_, "last");
    if (parsenode_equal($1, level_node, &result->extra_errno_) &&
        !result->pl_parse_info_.is_pl_parse_expr_) {
      yyerror(NULL, result, "LEVEL not in pl context;");
      YYABORT_PARSE_SQL_ERROR;
    } else {
      malloc_non_terminal_node(obj_id_node, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, id_node, NULL);
      malloc_non_terminal_node($$, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, $1, obj_id_node);
    }
    check_parser_size_overflow(result->extra_errno_);
  }
;

obj_access_ref_normal:
    pl_var_name opt_obj_access_ref_normal
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, $1, $2);
    }
    | access_func_expr_count opt_func_access_ref_normal
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, $1, $2);
    }
    | var_name '(' opt_func_param_list ')' opt_func_access_ref_normal
      {
        //oracle 中这部分关键字有特殊含义，作为pl function_name无法区分，兼容oracle行为，保留sql中语义
        if ($1->value_ != 1 &&
            (nodename_equal($1, "SYSTIMESTAMP", 12) ||
             nodename_equal($1, "CURRENT_DATE", 12) ||
             nodename_equal($1, "LOCALTIMESTAMP", 14) ||
             nodename_equal($1, "CURRENT_TIMESTAMP", 17) ||
             nodename_equal($1, "SESSIONTIMEZONE", 15) ||
             nodename_equal($1, "DBTIMEZONE", 10) ||
             nodename_equal($1, "CONNECT_BY_ISLEAF", 17) ||
             nodename_equal($1, "CONNECT_BY_ISCYCLE", 18) ||
             nodename_equal($1, "LEVEL", 5) ||
             nodename_equal($1, "REGEXP_LIKE", 11) ||
             nodename_equal($1, "LNNVL", 5))) {
          yyerror(&@1, result, "current keywords can't use pl funciton name;");
          YYABORT_PARSE_SQL_ERROR;
        }
        if (NULL != $3)
        {
          ParseNode *params = NULL;
          merge_nodes(params, result, T_EXPR_LIST, $3);
          malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $1, params);
          ParseNode *func_name = $$;
          (void)func_name;
        }
        else
        {
          malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $1);
        }
        malloc_non_terminal_node($$, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, $$, $5);
      }
    | PRIOR '(' opt_func_param_list ')' opt_func_access_ref_normal
     {
        ParseNode *name_node = NULL;
        make_name_node(name_node, result->malloc_pool_, "PRIOR");
        if (NULL != $3)
        {
          ParseNode *params = NULL;
          merge_nodes(params, result, T_EXPR_LIST, $3);
          malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, name_node, params);
          ParseNode *func_name = $$;
          (void)func_name;
        }
        else
        {
          malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, name_node);
        }
        malloc_non_terminal_node($$, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, $$, $5);
     }
    | exists_function_name '(' opt_func_param_list ')' opt_func_access_ref_normal
      {
        if (NULL != $3)
        {
          ParseNode *params = NULL;
          merge_nodes(params, result, T_EXPR_LIST, $3);
          malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $1, params);
        }
        else
        {
          malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $1);
        }
        malloc_non_terminal_node($$, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, $$, $5);
      }
;

opt_obj_access_ref:
    /*EMPTY*/ { $$ = NULL; }
  | '.' obj_access_ref
    {
      $$ = $2;
    }
  | '.' '*'
    {
      malloc_terminal_node($$, result->malloc_pool_, T_STAR);
    }
;

opt_obj_access_ref_normal:
    /*EMPTY*/ { $$ = NULL; }
  | '.' obj_access_ref_normal
    {
      $$ = $2;
    }
;

opt_func_access_ref:
    /*EMPTY*/ { $$ = NULL; }
  | '.' obj_access_ref
    {
      ParseNode *level_node = NULL;
      make_name_node(level_node, result->malloc_pool_, "LEVEL");
      level_node->value_ = 2;
      if ($2->type_ == T_OBJ_ACCESS_REF && $2->num_child_ == 2 &&
          $2->children_[0] != NULL && $2->children_[0]->value_ == 2 &&
          parsenode_equal($2->children_[0], level_node, &result->extra_errno_) &&
          !result->pl_parse_info_.is_pl_parse_expr_) {
        yyerror(NULL, result, "LEVEL not in pl context;");
        YYABORT_PARSE_SQL_ERROR;
      } else {
        $$ = $2;
      }
      check_parser_size_overflow(result->extra_errno_);
    }
  | table_element_access_list
    {
       merge_nodes($$, result, T_EXPR_LIST, $1);
    }
;

opt_func_access_ref_normal:
    /*EMPTY*/ { $$ = NULL; }
  | '.' obj_access_ref_normal
    {
      $$ = $2;
    }
  | table_element_access_list
    {
       merge_nodes($$, result, T_EXPR_LIST, $1);
    }
;

table_element_access_list:
    '(' table_index ')' { $$ = $2; }
  | table_element_access_list '(' table_index ')'
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

table_index:
    INTNUM { $$ = $1; }
  | var_name { $$ = $1; }
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
| expr OR expr %prec OR
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_OR, 2, $1, $3);
}
| NOT expr %prec NOT
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT, 1, $2);
}
| expr IS NULLX
{
  if (!(result->pl_parse_info_.is_pl_parse_
        && result->pl_parse_info_.is_pl_parse_expr_)
       && !result->may_bool_value_) {
    result->line_ -= 1;
    yyerror(NULL, result, "expr not accept boolean value");
    YYABORT_PARSE_SQL_ERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| expr IS not NULLX
{
  if (!(result->pl_parse_info_.is_pl_parse_
        && result->pl_parse_info_.is_pl_parse_expr_)
       && !result->may_bool_value_) {
    result->line_ -= 1;
    yyerror(NULL, result, "expr not accept boolean value");
    YYABORT_PARSE_SQL_ERROR;
  }
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bit_expr %prec LOWER_THAN_COMP
  {
    if (result->pl_parse_info_.is_pl_parse_) {
      $$ = $1;
    } else {
      yyerror(NULL, result, "not in pl context;");
      YYABORT_PARSE_SQL_ERROR;
    }
  }
| bool_pri %prec LOWER_THAN_COMP
{ $$ = $1;}
| USER_VARIABLE set_var_op bit_expr
{
  (void)($2);
  $1->type_ = T_LEFT_VALUE;
  result->may_bool_value_ = false;
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_ASSIGN, 2, $1, $3);
}
| '(' expr ')'
{
  $$ = $2;
}
;

not:
NOT { $$ = NULL;}
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
;

in_expr:
bit_expr
{  $$ = $1; }
;

//区分simple case 和 bool case
//前者 case c1 when 1 then 'true' when 0 then 'false' else '-' end
//后者 case when c1 = 1 then 'true' when c1 = 0 then 'false' else '-' end
case_expr:
CASE bit_expr simple_when_clause_list case_default END %prec LOWER_JOIN
{
  merge_nodes($$, result, T_WHEN_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CASE, 3, $2, $$, $4);
}
| CASE bit_expr simple_when_clause_list case_default END CASE
{
  merge_nodes($$, result, T_WHEN_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CASE, 3, $2, $$, $4);
}
| CASE bool_when_clause_list case_default END %prec LOWER_JOIN
{
  merge_nodes($$, result, T_WHEN_LIST, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CASE, 3, NULL, $$, $3);
}
| CASE bool_when_clause_list case_default END CASE
{
  merge_nodes($$, result, T_WHEN_LIST, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CASE, 3, NULL, $$, $3);
}
;

window_function:
COUNT '(' opt_all '*' ')' OVER '(' generalized_window_clause ')'
{
  (void)($3);
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 1, node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| COUNT '(' opt_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_terminal_node($3, result->malloc_pool_, T_ALL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| COUNT '(' DISTINCT bit_expr ')' OVER '(' generalized_window_clause ')'
{
  ParseNode *distinct = NULL;
  malloc_terminal_node(distinct, result->malloc_pool_, T_DISTINCT);
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 2, distinct, expr_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| APPROX_COUNT_DISTINCT '(' expr_list ')' OVER '(' generalized_window_clause ')'
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_APPROX_COUNT_DISTINCT, 1, expr_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| APPROX_COUNT_DISTINCT_SYNOPSIS '(' expr_list ')' OVER '(' generalized_window_clause ')'
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS, 1, expr_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE '(' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE, 1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| SUM '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SUM, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| MAX '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MAX, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| MIN '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MIN, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| AVG '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_AVG, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| MEDIAN '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MEDIAN, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| STDDEV '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| VARIANCE '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_VARIANCE, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| STDDEV_POP '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV_POP, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| STDDEV_SAMP '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV_SAMP, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| LISTAGG '(' opt_distinct_or_all expr_list ')' WITHIN GROUP '(' order_by ')' OVER '(' generalized_window_clause ')'
{
  ParseNode *group_concat_exprs = NULL;
  merge_nodes(group_concat_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_CONCAT, 4, $3, group_concat_exprs, $9, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $13);
}
| LISTAGG '(' opt_distinct_or_all expr_list ')' OVER '(' generalized_window_clause ')'
{
  ParseNode *group_concat_exprs = NULL;
  merge_nodes(group_concat_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_CONCAT, 4, $3, group_concat_exprs, NULL, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| RANK '(' ')' OVER '(' generalized_window_clause ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_WIN_FUN_RANK);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $6);
}
| DENSE_RANK '(' ')' OVER '(' generalized_window_clause ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_WIN_FUN_DENSE_RANK);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $6);
}
| PERCENT_RANK '(' ')' OVER '(' generalized_window_clause ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_WIN_FUN_PERCENT_RANK);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $6);
}
| ROW_NUMBER '(' ')' OVER '(' generalized_window_clause ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_WIN_FUN_ROW_NUMBER);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $6);
}
| NTILE '(' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_FUN_NTILE, 1, $3 );
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| CUME_DIST '(' ')' OVER '(' generalized_window_clause ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_WIN_FUN_CUME_DIST);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $6);
}
| FIRST_VALUE win_fun_first_last_params OVER '(' generalized_window_clause ')'
{
  $$ = $2;
  $$->type_ = T_WIN_FUN_FIRST_VALUE;
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $5);
}
| LAST_VALUE win_fun_first_last_params OVER '(' generalized_window_clause ')'
{
  $$ = $2;
  $$->type_ = T_WIN_FUN_LAST_VALUE;
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $5);
}
| LEAD win_fun_lead_lag_params OVER '(' generalized_window_clause ')'
{
  $$ = $2;
  $$->type_ = T_WIN_FUN_LEAD;
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $5);
}
| LAG win_fun_lead_lag_params OVER '(' generalized_window_clause ')'
{
  $$ = $2;
  $$->type_ = T_WIN_FUN_LAG;
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $5);
}
| NTH_VALUE '(' bit_expr ',' bit_expr ')' opt_from_first_or_last opt_respect_or_ignore_nulls OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_FUN_NTH_VALUE, 4, $3, $5, $7, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $11);
}
| RATIO_TO_REPORT '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_FUN_RATIO_TO_REPORT, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| CORR '(' opt_distinct_or_all bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_CORR, 3, $3, $4, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| COVAR_POP '(' opt_distinct_or_all bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COVAR_POP, 3, $3, $4, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| COVAR_SAMP '(' opt_distinct_or_all bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COVAR_SAMP, 3, $3, $4, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| VAR_POP '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_VAR_POP, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| VAR_SAMP '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_VAR_SAMP, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| REGR_SLOPE '(' opt_distinct_or_all bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_SLOPE, 3, $3, $4, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| REGR_INTERCEPT '(' opt_distinct_or_all bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_INTERCEPT, 3, $3, $4, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| REGR_COUNT '(' opt_distinct_or_all bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_COUNT, 3, $3, $4, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| REGR_R2 '(' opt_distinct_or_all bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_R2, 3, $3, $4, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| REGR_AVGX '(' opt_distinct_or_all bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_AVGX, 3, $3, $4, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| REGR_AVGY '(' opt_distinct_or_all bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_AVGY, 3, $3, $4, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| REGR_SXX '(' opt_distinct_or_all bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_SXX, 3, $3, $4, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| REGR_SYY '(' opt_distinct_or_all bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_SYY, 3, $3, $4, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| REGR_SXY '(' opt_distinct_or_all bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_SXY, 3, $3, $4, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| MAX '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_MAX, 4, $3, $4, $9, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $14);
}
| MIN '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_MIN, 4, $3, $4, $9, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $14);
}
| SUM '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_SUM, 4, $3, $4, $9, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $14);
}
| COUNT '(' opt_all '*' ')' KEEP '(' DENSE_RANK first_or_last order_by ')' OVER '(' generalized_window_clause ')'
{
  (void)($3);
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_COUNT, 4, NULL, node, $9, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $14);
}
| COUNT '(' opt_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')' OVER '(' generalized_window_clause ')'
{
  malloc_terminal_node($3, result->malloc_pool_, T_ALL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_COUNT, 4, $3, $4, $9, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $14);
}
| AVG '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_AVG, 4, $3, $4, $9, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $14);
}
| VARIANCE '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_VARIANCE, 4, $3, $4, $9, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $14);
}
| STDDEV '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_STDDEV, 4, $3, $4, $9, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $14);
}
| PERCENTILE_CONT '(' opt_distinct_or_all expr_list ')' WITHIN GROUP '(' order_by ')' OVER '(' generalized_window_clause ')'
{
  ParseNode *percentile_cont_exprs = NULL;
  merge_nodes(percentile_cont_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_PERCENTILE_CONT, 4, $3, percentile_cont_exprs, $9, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $13);
}
| PERCENTILE_DISC '(' opt_distinct_or_all expr_list ')' WITHIN GROUP '(' order_by ')' OVER '(' generalized_window_clause ')'
{
  ParseNode *percentile_disc_exprs = NULL;
  merge_nodes(percentile_disc_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_PERCENTILE_DISC, 4, $3, percentile_disc_exprs, $9, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $13);
}
| WM_CONCAT '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_WM_CONCAT, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| WMSYS '.' WM_CONCAT '(' opt_distinct_or_all bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_WM_CONCAT, 2, $5, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| WM_CONCAT '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_WM_CONCAT, 4, $3, $4, $9, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $14);
}
| WMSYS '.' WM_CONCAT '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_WM_CONCAT, 4, $5, $6, $11, $12);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $16);
}
| TOP_K_FRE_HIST '(' bit_expr ',' bit_expr  ','  bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_TOP_FRE_HIST, 3, $3, $5, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $11);
}
| HYBRID_HIST '(' bit_expr ',' bit_expr ')' OVER '(' generalized_window_clause ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_HYBRID_HIST, 2, $3, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $9);
}
/*for pl udf agg*/
| function_name '(' func_param_list ')' OVER '(' generalized_window_clause ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $3);
  ParseNode *all_node = NULL;
  malloc_terminal_node(all_node, result->malloc_pool_, T_ALL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_PL_AGG_UDF, 3, $1, params, all_node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| function_name '(' ALL func_param_list ')' OVER '(' generalized_window_clause ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $4);
  ParseNode *all_node = NULL;
  malloc_terminal_node(all_node, result->malloc_pool_, T_ALL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_PL_AGG_UDF, 3, $1, params, all_node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| function_name '(' DISTINCT func_param_list ')' OVER '(' generalized_window_clause ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $4);
  ParseNode *distinct_node = NULL;
  malloc_terminal_node(distinct_node, result->malloc_pool_, T_DISTINCT);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_PL_AGG_UDF, 3, $1, params, distinct_node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| function_name '(' UNIQUE func_param_list ')' OVER '(' generalized_window_clause ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $4);
  ParseNode *distinct_node = NULL;
  malloc_terminal_node(distinct_node, result->malloc_pool_, T_DISTINCT);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_PL_AGG_UDF, 3, $1, params, distinct_node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
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
'(' bit_expr respect_or_ignore NULLS ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INVALID, 2, $2, $3);
}
| '(' bit_expr ')' opt_respect_or_ignore_nulls
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INVALID, 2, $2, $4);
}
;

win_fun_lead_lag_params:
'(' bit_expr respect_or_ignore NULLS ')'
{
  ParseNode *params_node = NULL;
  malloc_non_terminal_node(params_node, result->malloc_pool_, T_LINK_NODE, 2, $2, NULL);
  merge_nodes(params_node, result, T_EXPR_LIST, params_node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INVALID, 2, params_node, $3);
}
|
'(' bit_expr respect_or_ignore NULLS ',' expr_list ')'
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

generalized_window_clause:
opt_partition_by opt_order_by opt_win_window
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_GENERALIZED_WINDOW, 3, $1, $2, $3);
};

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
bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_INTERVAL, 1, $1);
  $$->value_ = 1;
  dup_string($$, result, @1.first_column, @1.last_column);
}
/*
| INTERVAL STRING_VALUE date_unit
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_INTERVAL, 2, $2, $3);
  $$->value_ = 2;
  dup_string($$, result, @1.first_column, @3.last_column);
}
*/
;

win_bounding:
CURRENT ROW
{
  malloc_terminal_node($$, result->malloc_pool_, T_WIN_BOUND);
  $$->value_ = 1;
}
| win_interval win_preceding_or_following
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_BOUND, 2, $1, $2);
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

simple_when_clause_list:
simple_when_clause { $$ = $1; }
| simple_when_clause_list simple_when_clause
{ malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2); }
;

simple_when_clause:
WHEN bit_expr THEN bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WHEN, 2, $2, $4);
}
;

bool_when_clause_list:
bool_when_clause { $$ = $1; }
| bool_when_clause_list bool_when_clause
{ malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2); }
;

bool_when_clause:
WHEN expr THEN bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WHEN, 2, $2, $4);
}
;

case_default:
ELSE bit_expr   { $$ = $2; }
| /*EMPTY*/ { malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_NULL); }
;

opt_all:
ALL {
  $$ = NULL;
}
| /*empty*/{ $$ = NULL; }
;

sql_function:
single_row_function { $$ = $1; }
| aggregate_function { $$ = $1; }
//| analytic_function { $$ = $1; }
//| object_reference_function { $$ = $1; }
//| model_function { $$ = $1; }
//| olap_function { $$ = $1; }
//| data_cartridge_function  { $$ = $1; }
//| user_defined_function { $$ = $1; }
| special_func_expr { $$ = $1; } //mysql的函数或OB特有函数，后续应该全部去掉
;

single_row_function:
numeric_function { $$ = $1; }
| character_function { $$ = $1; }
| extract_function { $$ = $1; }
//| comparison_function { $$ = $1; }
| conversion_function { $$ = $1; }
//| large_object_function { $$ = $1; }
//| collection_function { $$ = $1; }
| hierarchical_function { $$ = $1; }
//| data_mining_function { $$ = $1; }
//| xml_function { $$ = $1; }
//| encoding_decoding_function { $$ = $1; }
//| null_related_function { $$ = $1; }
| environment_id_function { $$ = $1; }
;

numeric_function:
MOD '(' bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MOD, 2, $3, $5);
}

character_function:
TRIM '(' parameterized_trim ')'
{
  make_name_node($$, result->malloc_pool_, "trim");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, $3);
}
| ASCII '(' bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "ascii");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| TRANSLATE '(' bit_expr USING translate_charset ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "translate");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| TRANSLATE '(' bit_expr ',' bit_expr ',' bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 3, $3, $5, $7);
  make_name_node($$, result->malloc_pool_, "translate");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
;

translate_charset:
CHAR_CS
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = TRANSLATE_CHAR_CS;
}
| NCHAR_CS
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = TRANSLATE_NCHAR_CS;
}
;

extract_function:
EXTRACT '(' date_unit_for_extract FROM bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "extract");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
/*
| SESSIONTIMEZONE
{
  make_name_node($$, result->malloc_pool_, "sessiontimezone");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $$);
}
| DBTIMEZONE
{
  make_name_node($$, result->malloc_pool_, "dbtimezone");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $$);
}
*/
;

/*
comparison_function:
{
  $$ = NULL;
}
;
*/

conversion_function:
CAST '(' bit_expr AS cast_data_type ')'
{
  //cast_data_type is a T_CAST_ARGUMENT rather than a T_INT to avoid being parameterized automatically
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "cast");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
;

/*
large_object_function:
{
  $$ = NULL;
}
;

collection_function:
{
  $$ = NULL;
}
;
*/

hierarchical_function:
SYS_CONNECT_BY_PATH '(' bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CONNECT_BY_PATH, 2, $3, $5);
}
;

/*
data_mining_function:
{
  $$ = NULL;
}
;

xml_function:
{
  $$ = NULL;
}
;

encoding_decoding_function:
{
  $$ = NULL;
}
;

null_related_function:
{
  $$ = NULL;
}
;
*/

environment_id_function:
USER
{
   make_name_node($$, result->malloc_pool_, "user");
   malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $$);
}
| UID
{
  make_name_node($$, result->malloc_pool_, "uid");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $$);
}
;

aggregate_function:
APPROX_COUNT_DISTINCT '(' expr_list ')'
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
| APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE '(' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE, 1, $3);
}
| SUM '(' opt_distinct_or_all bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SUM, 2, $3, $4);
}
| MAX '(' opt_distinct_or_all bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MAX, 2, $3, $4);
}
| MIN '(' opt_distinct_or_all bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MIN, 2, $3, $4);
}
| AVG '(' opt_distinct_or_all bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_AVG, 2, $3, $4);
}
| MEDIAN '(' opt_distinct_or_all bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MEDIAN, 2, $3, $4);
}
| STDDEV '(' opt_distinct_or_all bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV, 2, $3, $4);
}
| VARIANCE '(' opt_distinct_or_all bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_VARIANCE, 2, $3, $4);
}
| STDDEV_POP '(' opt_distinct_or_all bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV_POP, 2, $3, $4);
}
| STDDEV_SAMP '(' opt_distinct_or_all bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_STDDEV_SAMP, 2, $3, $4);
}
| GROUPING '(' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUPING, 1, $3);
}
| LISTAGG '(' opt_distinct_or_all expr_list ')' WITHIN GROUP '(' order_by ')'
{
  ParseNode *group_concat_exprs = NULL;
  merge_nodes(group_concat_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_CONCAT, 4, $3, group_concat_exprs, $9, NULL);
}
| LISTAGG '(' opt_distinct_or_all expr_list ')'  %prec LOWER_PARENS
{
  ParseNode *group_concat_exprs = NULL;
  merge_nodes(group_concat_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_CONCAT, 4, $3, group_concat_exprs, NULL, NULL);
}
| CORR '(' opt_distinct_or_all bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_CORR, 3, $3, $4, $6);
}
| COVAR_POP '(' opt_distinct_or_all bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COVAR_POP, 3, $3, $4, $6);
}
| COVAR_SAMP '('opt_distinct_or_all bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COVAR_SAMP, 3, $3, $4, $6);
}
| VAR_POP '(' opt_distinct_or_all bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_VAR_POP, 2, $3, $4);
}
| VAR_SAMP '(' opt_distinct_or_all bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_VAR_SAMP, 2, $3, $4);
}
| REGR_SLOPE '(' opt_distinct_or_all bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_SLOPE, 3, $3, $4, $6);
}
| REGR_INTERCEPT '(' opt_distinct_or_all bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_INTERCEPT, 3, $3, $4, $6);
}
| REGR_COUNT '(' opt_distinct_or_all bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_COUNT, 3, $3, $4, $6);
}
| REGR_R2 '(' opt_distinct_or_all bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_R2, 3, $3, $4, $6);
}
| REGR_AVGX '(' opt_distinct_or_all bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_AVGX, 3, $3, $4, $6);
}
| REGR_AVGY '(' opt_distinct_or_all bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_AVGY, 3, $3, $4, $6);
}
| REGR_SXX '(' opt_distinct_or_all bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_SXX, 3, $3, $4, $6);
}
| REGR_SYY '(' opt_distinct_or_all bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_SYY, 3, $3, $4, $6);
}
| REGR_SXY '(' opt_distinct_or_all bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_REGR_SXY, 3, $3, $4, $6);
}
| RANK '(' opt_distinct_or_all expr_list ')' WITHIN GROUP '(' order_by ')'
{
  ParseNode *rank_exprs = NULL;
  merge_nodes(rank_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_RANK, 4, $3, rank_exprs, $9, NULL);
}
| PERCENT_RANK '(' opt_distinct_or_all expr_list ')' WITHIN GROUP '(' order_by ')'
{
  ParseNode *percent_rank_exprs = NULL;
  merge_nodes(percent_rank_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_PERCENT_RANK, 4, $3, percent_rank_exprs, $9, NULL);
}
| DENSE_RANK '(' opt_distinct_or_all expr_list ')' WITHIN GROUP '(' order_by ')'
{
  ParseNode *dense_rank_exprs = NULL;
  merge_nodes(dense_rank_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_DENSE_RANK, 4, $3, dense_rank_exprs, $9, NULL);
}
| CUME_DIST '(' opt_distinct_or_all expr_list ')' WITHIN GROUP '(' order_by ')'
{
  ParseNode *cume_dist_exprs = NULL;
  merge_nodes(cume_dist_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_CUME_DIST, 4, $3, cume_dist_exprs, $9, NULL);
}
| PERCENTILE_CONT '(' opt_distinct_or_all expr_list ')' WITHIN GROUP '(' order_by ')'
{
  ParseNode *percentile_cont_exprs = NULL;
  merge_nodes(percentile_cont_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_PERCENTILE_CONT, 4, $3, percentile_cont_exprs, $9, NULL);
}
| PERCENTILE_DISC '(' opt_distinct_or_all expr_list ')' WITHIN GROUP '(' order_by ')'
{
  ParseNode *percentile_disc_exprs = NULL;
  merge_nodes(percentile_disc_exprs, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_GROUP_PERCENTILE_DISC, 4, $3, percentile_disc_exprs, $9, NULL);
}
| MAX '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_MAX, 4, $3, $4, $9, $10);
}
| MIN '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_MIN, 4, $3, $4, $9, $10);
}
| SUM '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_SUM, 4, $3, $4, $9, $10);
}
| AVG '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_AVG, 4, $3, $4, $9, $10);
}
| VARIANCE '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_VARIANCE, 4, $3, $4, $9, $10);
}
| STDDEV '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_STDDEV, 4, $3, $4, $9, $10);
}
| WM_CONCAT '(' opt_distinct_or_all bit_expr ')'  %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_WM_CONCAT, 2, $3, $4);
}
| WMSYS '.' WM_CONCAT '(' opt_distinct_or_all bit_expr ')'  %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_WM_CONCAT, 2, $5, $6);
}
| WM_CONCAT '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_WM_CONCAT, 4, $3, $4, $9, $10);
}
| WMSYS '.' WM_CONCAT '(' opt_distinct_or_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_WM_CONCAT, 4, $5, $6, $11, $12);
}
| TOP_K_FRE_HIST '(' bit_expr ',' bit_expr  ','  bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_TOP_FRE_HIST, 3, $3, $5, $7);
}
| HYBRID_HIST '(' bit_expr ',' bit_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_HYBRID_HIST, 2, $3, $5);
}
;




/*
analytic_function:
{
  $$ = NULL;
}
;

object_reference_function:
{
  $$ = NULL;
}
;

model_function:
{
  $$ = NULL;
}
;

olap_function:
{
  $$ = NULL;
}
;

data_cartridge_function:
{
  $$ = NULL;
}
;

user_defined_function:
{
  $$ = NULL;
}
;
*/

/*special_func_expr是mysql的函数或OB特有函数，后续应该全部去掉*/
special_func_expr:
ISNULL '(' bit_expr ')'
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
| utc_timestamp_func
{
  $$ = $1;
}
| INSERT '(' bit_expr ',' bit_expr ',' bit_expr ',' bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 4, $3, $5, $7, $9);
  make_name_node($$, result->malloc_pool_, "insert");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| LEFT '(' bit_expr ',' bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "left");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| POSITION '(' bit_expr IN bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "position");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| DATE '(' bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "date");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| YEAR '(' bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "year");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| TIME '(' bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "time");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| MONTH '(' bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "month");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
/*
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
*/
| DEFAULT '(' column_definition_ref ')'
{
  ParseNode *node = NULL;
  ParseNode *null_node1 = NULL;
  ParseNode *null_node2 = NULL;
  ParseNode *null_node3 = NULL;
  ParseNode *null_node4 = NULL;
  //以下null_node只负责占坑，resolve阶段会填充具体的内容
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
/*oracle中是无用的语法，先注释掉
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
*/
| CALC_PARTITION_ID '(' bit_expr ',' bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "calc_partition_id");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
;

access_func_expr_count:
COUNT '(' opt_all '*' ')'
{
  (void)($3);
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 1, node);
}
| COUNT '(' opt_all bit_expr ')'
{
  malloc_terminal_node($3, result->malloc_pool_, T_ALL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 2, $3, $4);
}
| COUNT '(' DISTINCT bit_expr ')'
{
  ParseNode *distinct = NULL;
  malloc_terminal_node(distinct, result->malloc_pool_, T_DISTINCT);
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 2, distinct, expr_list);
}
| COUNT '(' UNIQUE bit_expr ')'
{
  ParseNode *distinct = NULL;
  malloc_terminal_node(distinct, result->malloc_pool_, T_DISTINCT);
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 2, distinct, expr_list);
}
| COUNT '(' opt_all '*' ')' KEEP '(' DENSE_RANK first_or_last order_by ')'
{
  (void)($3);
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_COUNT, 4, NULL, node, $9, $10);
}
| COUNT '(' opt_all bit_expr ')' KEEP '(' DENSE_RANK first_or_last order_by ')'
{
  malloc_terminal_node($3, result->malloc_pool_, T_ALL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_KEEP_COUNT, 4, $3, $4, $9, $10);
}
;

access_func_expr:
access_func_expr_count
{
  $$ = $1;
}
| function_name '(' ')'
{
  //oracle 中这部分关键字有特殊含义，作为pl function_name无法区分，兼容oracle行为，保留sql中语义
  if ($1->value_ != 1 &&
      (nodename_equal($1, "SYSTIMESTAMP", 12) ||
       nodename_equal($1, "CURRENT_DATE", 12) ||
       nodename_equal($1, "LOCALTIMESTAMP", 14) ||
       nodename_equal($1, "CURRENT_TIMESTAMP", 17) ||
       nodename_equal($1, "SESSIONTIMEZONE", 15) ||
       nodename_equal($1, "DBTIMEZONE", 10) ||
       nodename_equal($1, "CONNECT_BY_ISLEAF", 17) ||
       nodename_equal($1, "CONNECT_BY_ISCYCLE", 18) ||
       nodename_equal($1, "LEVEL", 5) ||
       nodename_equal($1, "REGEXP_LIKE", 11) ||
       nodename_equal($1, "LNNVL", 5))) {
    yyerror(&@1, result, "current keywords can't use pl funciton name");
    YYABORT_PARSE_SQL_ERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $1);
}
| function_name '(' func_param_list ')'
{
  //oracle 中这部分关键字有特殊含义，作为pl function_name无法区分，兼容oracle行为，保留sql中语义
  if ($1->value_ != 1 &&
      (nodename_equal($1, "SYSTIMESTAMP", 12) ||
       nodename_equal($1, "CURRENT_DATE", 12) ||
       nodename_equal($1, "LOCALTIMESTAMP", 14) ||
       nodename_equal($1, "CURRENT_TIMESTAMP", 17) ||
       nodename_equal($1, "SESSIONTIMEZONE", 15) ||
       nodename_equal($1, "DBTIMEZONE", 10) ||
       nodename_equal($1, "CONNECT_BY_ISLEAF", 17) ||
       nodename_equal($1, "CONNECT_BY_ISCYCLE", 18) ||
       nodename_equal($1, "LEVEL", 5) ||
       nodename_equal($1, "REGEXP_LIKE", 11) ||
       nodename_equal($1, "LNNVL", 5))) {
    yyerror(&@1, result, "current keywords can't use pl funciton name");
    YYABORT_PARSE_SQL_ERROR;
  }
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $1, params);
}
| aggregate_function_keyword '(' ')'
{
  ParseNode *name_node = NULL;
  get_oracle_non_reserved_node(name_node, result->malloc_pool_, @1.first_column, @1.last_column);
  //oracle 中这部分关键字有特殊含义，作为pl function_name无法区分，兼容oracle行为，保留sql中语义
  if (name_node->value_ != 1 &&
      (nodename_equal(name_node, "SYSTIMESTAMP", 12) ||
        nodename_equal(name_node, "CURRENT_DATE", 12) ||
        nodename_equal(name_node, "LOCALTIMESTAMP", 14) ||
        nodename_equal(name_node, "CURRENT_TIMESTAMP", 17) ||
        nodename_equal(name_node, "SESSIONTIMEZONE", 15) ||
        nodename_equal(name_node, "DBTIMEZONE", 10) ||
        nodename_equal(name_node, "CONNECT_BY_ISLEAF", 17) ||
        nodename_equal(name_node, "CONNECT_BY_ISCYCLE", 18) ||
        nodename_equal(name_node, "LEVEL", 5) ||
        nodename_equal(name_node, "REGEXP_LIKE", 11) ||
        nodename_equal(name_node, "LNNVL", 5))) {
    yyerror(&@1, result, "current keywords can't use pl funciton name");
    YYABORT_PARSE_SQL_ERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, name_node);
}
| aggregate_function_keyword '(' func_param_list ')'
{
  ParseNode *name_node = NULL;
  get_oracle_non_reserved_node(name_node, result->malloc_pool_, @1.first_column, @1.last_column);
  //oracle 中这部分关键字有特殊含义，作为pl function_name无法区分，兼容oracle行为，保留sql中语义
  if (name_node->value_ != 1 &&
      (nodename_equal(name_node, "SYSTIMESTAMP", 12) ||
        nodename_equal(name_node, "CURRENT_DATE", 12) ||
        nodename_equal(name_node, "LOCALTIMESTAMP", 14) ||
        nodename_equal(name_node, "CURRENT_TIMESTAMP", 17) ||
        nodename_equal(name_node, "SESSIONTIMEZONE", 15) ||
        nodename_equal(name_node, "DBTIMEZONE", 10) ||
        nodename_equal(name_node, "CONNECT_BY_ISLEAF", 17) ||
        nodename_equal(name_node, "CONNECT_BY_ISCYCLE", 18) ||
        nodename_equal(name_node, "LEVEL", 5) ||
        nodename_equal(name_node, "REGEXP_LIKE", 11) ||
        nodename_equal(name_node, "LNNVL", 5))) {
    yyerror(&@1, result, "current keywords can't use pl funciton name");
    YYABORT_PARSE_SQL_ERROR;
  }
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, name_node, params);
}
| function_name '(' ALL func_param_list ')'
{
  //oracle 中这部分关键字有特殊含义，作为pl function_name无法区分，兼容oracle行为，保留sql中语义
  if ($1->value_ != 1 &&
      (nodename_equal($1, "SYSTIMESTAMP", 12) ||
       nodename_equal($1, "CURRENT_DATE", 12) ||
       nodename_equal($1, "LOCALTIMESTAMP", 14) ||
       nodename_equal($1, "CURRENT_TIMESTAMP", 17) ||
       nodename_equal($1, "SESSIONTIMEZONE", 15) ||
       nodename_equal($1, "DBTIMEZONE", 10) ||
       nodename_equal($1, "CONNECT_BY_ISLEAF", 17) ||
       nodename_equal($1, "CONNECT_BY_ISCYCLE", 18) ||
       nodename_equal($1, "LEVEL", 5) ||
       nodename_equal($1, "REGEXP_LIKE", 11) ||
       nodename_equal($1, "LNNVL", 5))) {
    yyerror(&@1, result, "current keywords can't use pl funciton name");
    YYABORT_PARSE_SQL_ERROR;
  }
  ParseNode *all_node = NULL;
  malloc_terminal_node(all_node, result->malloc_pool_, T_ALL);
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 3, $1, params, all_node);
}
| function_name '(' DISTINCT func_param_list ')'
{
  //oracle 中这部分关键字有特殊含义，作为pl function_name无法区分，兼容oracle行为，保留sql中语义
  if ($1->value_ != 1 &&
      (nodename_equal($1, "SYSTIMESTAMP", 12) ||
       nodename_equal($1, "CURRENT_DATE", 12) ||
       nodename_equal($1, "LOCALTIMESTAMP", 14) ||
       nodename_equal($1, "CURRENT_TIMESTAMP", 17) ||
       nodename_equal($1, "SESSIONTIMEZONE", 15) ||
       nodename_equal($1, "DBTIMEZONE", 10) ||
       nodename_equal($1, "CONNECT_BY_ISLEAF", 17) ||
       nodename_equal($1, "CONNECT_BY_ISCYCLE", 18) ||
       nodename_equal($1, "LEVEL", 5) ||
       nodename_equal($1, "REGEXP_LIKE", 11) ||
       nodename_equal($1, "LNNVL", 5))) {
    yyerror(&@1, result, "current keywords can't use pl funciton name");
    YYABORT_PARSE_SQL_ERROR;
  }
  ParseNode *distinct_node = NULL;
  malloc_terminal_node(distinct_node, result->malloc_pool_, T_DISTINCT);
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 3, $1, params, distinct_node);
}
| function_name '(' UNIQUE func_param_list ')'
{
  //oracle 中这部分关键字有特殊含义，作为pl function_name无法区分，兼容oracle行为，保留sql中语义
  if ($1->value_ != 1 &&
      (nodename_equal($1, "SYSTIMESTAMP", 12) ||
       nodename_equal($1, "CURRENT_DATE", 12) ||
       nodename_equal($1, "LOCALTIMESTAMP", 14) ||
       nodename_equal($1, "CURRENT_TIMESTAMP", 17) ||
       nodename_equal($1, "SESSIONTIMEZONE", 15) ||
       nodename_equal($1, "DBTIMEZONE", 10) ||
       nodename_equal($1, "CONNECT_BY_ISLEAF", 17) ||
       nodename_equal($1, "CONNECT_BY_ISCYCLE", 18) ||
       nodename_equal($1, "LEVEL", 5) ||
       nodename_equal($1, "REGEXP_LIKE", 11) ||
       nodename_equal($1, "LNNVL", 5))) {
    yyerror(&@1, result, "current keywords can't use pl funciton name");
    YYABORT_PARSE_SQL_ERROR;
  }
  ParseNode *distinct_node = NULL;
  malloc_terminal_node(distinct_node, result->malloc_pool_, T_DISTINCT);
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 3, $1, params, distinct_node);
}
| exists_function_name '(' opt_func_param_list ')'
{
  if (NULL != $3)
  {
    ParseNode *params = NULL;
    merge_nodes(params, result, T_EXPR_LIST, $3);
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $1, params);
  }
  else
  {
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $1);
  }
}
;

opt_func_param_list:
    /*EMPTY*/ { $$ = NULL; }
  | func_param_list { $$ = $1; }
;
func_param_list:
    func_param { $$ = $1; }
  | func_param_list ',' func_param
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
;
func_param:
    func_param_with_assign { $$ = $1; }
  | bit_expr { $$ = $1; }
  | bool_pri_in_pl_func { $$ = $1; }
;
func_param_with_assign:
    var_name PARAM_ASSIGN_OPERATOR bit_expr
    {
      if (NULL == $3) {
        YYERROR;
      }
      // 这种参数赋值方式目前只有PL FUNCTION会用到, 因此复用T_SP_CPARAM
      malloc_non_terminal_node($$, result->malloc_pool_, T_SP_CPARAM, 2, $1, $3);
    }
  | var_name PARAM_ASSIGN_OPERATOR bool_pri_in_pl_func
    {
      if (NULL == $3) {
        YYERROR;
      }
      // 这种参数赋值方式目前只有PL FUNCTION会用到, 因此复用T_SP_CPARAM
      malloc_non_terminal_node($$, result->malloc_pool_, T_SP_CPARAM, 2, $1, $3);
    }
;

pl_var_name:
var_name { $$ = $1; }
| oracle_pl_non_reserved_words { $$ = $1; }

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

bool_pri_in_pl_func:
    bool_pri
    {
      if (!result->pl_parse_info_.is_pl_parse_expr_) {
        yyerror(&@1, result, "");
        YYABORT_PARSE_SQL_ERROR;
      }
      $$ = $1;
    }
  | bool_pri_in_pl_func AND bool_pri_in_pl_func
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_AND, 2, $1, $3);
    }
  | bool_pri_in_pl_func OR bool_pri_in_pl_func
    { 
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_OR, 2, $1, $3);
    }
  | NOT bool_pri_in_pl_func
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT, 1, $2);
    }
  | '(' bool_pri_in_pl_func ')'
    {
      $$ = $2;
    }

;

/*
注意！！！cur_timestamp_func下面的语法规则里注释的语法都是支持的，注释的原因是因为在obj_access_ref语法规则已经存在，
出现了规约冲突，同时对应的语法规则在解析obj_access_ref语法规则时进行了特殊处理支持，处理细节见ob_raw_expr_resolver_impl.cpp
文件中的process_obj_access_node函数
*/
cur_timestamp_func:
SYSDATE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_SYSDATE, 1, NULL);
}
/*
| SYSTIMESTAMP
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_SYSTIMESTAMP, 1, NULL);
}
*/
| SYSTIMESTAMP '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_SYSTIMESTAMP, 1, $3);
}
/*
| CURRENT_DATE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_DATE, 1, NULL);
}
| LOCALTIMESTAMP
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_LOCALTIMESTAMP, 1, NULL);
}
*/
| LOCALTIMESTAMP '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_LOCALTIMESTAMP, 1, $3);
}
/*
| CURRENT_TIMESTAMP
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIMESTAMP, 1, NULL);
}
*/
| CURRENT_TIMESTAMP '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIMESTAMP, 1, $3);
}
;


substr_params:
bit_expr ',' bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 2, $1, $3);
}
| bit_expr ',' bit_expr ',' bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, $1, $3, $5);
}
;

/*
date_params:
bit_expr ',' INTERVAL bit_expr date_unit
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, $1, $4, $5);
}
;

timestamp_params:
date_unit ',' bit_expr ',' bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, $1, $3, $5);
}
;
*/

// opt_expr_list:
//  /* EMPTY */ { $$ = NULL; }
//  | expr_list { $$ = $1; }
// ;

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

/*****************************************************************************
 *
 *	delete grammar
 *
 *****************************************************************************/

delete_stmt:
delete_with_opt_hint FROM table_factor opt_where_extension opt_returning opt_error_logging_clause
{
  ParseNode *from_list = NULL;
  ParseNode *delete_table_node = NULL;
  merge_nodes(from_list, result, T_TABLE_REFERENCES, $3);
  malloc_non_terminal_node(delete_table_node, result->malloc_pool_, T_DELETE_TABLE_NODE, 2,
                           NULL, /*0. delete list*/
                           from_list);    /*1. from list*/
  malloc_non_terminal_node($$, result->malloc_pool_, T_DELETE, 8,
                           delete_table_node,   /* 0. table_node */
                           $4,      /* 1. where      */
                           NULL,      /* 2. order by, unused in oracle mode   */
                           NULL,      /* 3. limit, unused in oracle mode    */
                           NULL,      /* 4. when, deprecated clause   */
                           $1,      /* 5. hint       */
                           $5,       /* 6. returning  */
                           $6);      /* 7. error logging caluse  */

}
|
delete_with_opt_hint table_factor opt_where opt_returning opt_error_logging_clause
{
  ParseNode *from_list = NULL;
  ParseNode *delete_table_node = NULL;
  merge_nodes(from_list, result, T_TABLE_REFERENCES, $2);
  malloc_non_terminal_node(delete_table_node, result->malloc_pool_, T_DELETE_TABLE_NODE, 2,
                           NULL, /*0. delete list*/
                           from_list);    /*1. from list*/
  malloc_non_terminal_node($$, result->malloc_pool_, T_DELETE, 8,
                           delete_table_node,   /* 0. table_node */
                           $3,      /* 1. where      */
                           NULL,      /* 2. order by, unused in oracle mode   */
                           NULL,      /* 3. limit, unused in oracle mode    */
                           NULL,      /* 4. when, deprecated clause   */
                           $1,      /* 5. hint       */
                           $4,       /* 6. returning  */
                           $5);     /* 7. error logging caluse */
}
;

/*****************************************************************************
 *
 *	update grammar
 *
 *****************************************************************************/

update_stmt:
update_with_opt_hint dml_table_clause SET update_asgn_list opt_where_extension opt_returning opt_error_logging_clause
{
  ParseNode *from_list = NULL;
  ParseNode *assign_list = NULL;
  merge_nodes(from_list, result, T_TABLE_REFERENCES, $2);
  merge_nodes(assign_list, result, T_ASSIGN_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_UPDATE, 10,
                           from_list,     /* 0. table node */
                           assign_list,   /* 1. update list */
                           $5,            /* 2. where node */
                           NULL,            /* 3. order by node */
                           NULL,            /* 4. limit node */
                           NULL,            /* 5. when node */
                           $1,            /* 6. hint node */
                           NULL,            /* 7. ignore */
                           $6,            /* 8. returning */
                           $7);           /* error logging caluse*/
}
;

update_asgn_list:
normal_asgn_list
{
  $$ = $1;
}
| ROW COMP_EQ obj_access_ref_normal
{
  $$ = $3;
}
;

normal_asgn_list:
update_asgn_factor
{
  $$ = $1;
}
| normal_asgn_list ',' update_asgn_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

update_asgn_factor:
column_definition_ref COMP_EQ expr_or_default
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ASSIGN_ITEM, 2, $1, $3);
}
|
'(' column_list ')' COMP_EQ '(' subquery ')'
{
  ParseNode *col_list = NULL;
  merge_nodes(col_list, result, T_COLUMN_LIST, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ASSIGN_ITEM, 2, col_list, $6);
}
;
/*****************************************************************************
 *
 *	resource unit & resource pool grammar
 *
 *****************************************************************************/

create_resource_stmt:
CREATE RESOURCE UNIT relation_name opt_resource_unit_option_list
{
  ParseNode *resource_options = NULL;
  merge_nodes(resource_options, result, T_RESOURCE_UNIT_OPTION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_RESOURCE_UNIT, 3,
                           NULL,
                           $4,                     /* resource unit name */
                           resource_options);      /* resource opt */
}
| CREATE RESOURCE POOL relation_name opt_create_resource_pool_option_list
{
  ParseNode *resource_options = NULL;
  merge_nodes(resource_options, result, T_RESOURCE_POOL_OPTION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_RESOURCE_POOL, 3,
                           NULL,
                           $4,                     // resource pool name
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
DROP RESOURCE UNIT relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_RESOURCE_UNIT, 2, NULL, $4);
}
| DROP RESOURCE POOL relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_RESOURCE_POOL, 2, NULL, $4);
}
;

/*****************************************************************************
 *
 *	tenant grammar
 *
 *****************************************************************************/
create_tenant_stmt:
CREATE TENANT relation_name
opt_tenant_option_list opt_set_sys_var
{
  ParseNode *tenant_options = NULL;
  merge_nodes(tenant_options, result, T_TENANT_OPTION_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TENANT, 4,
                           NULL,                   /* if not exists */
                           $3,                   /* tenant name */
                           tenant_options,      /* tenant opt */
                           $5);      /* system variable set opt */
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



/* TODO: (xiaochu.yh) refactor after requirement determined */
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
                           NULL);
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
DROP TENANT relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_TENANT, 2, NULL, $3);
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
database_name
{
  $$ = $1;
}
|
{
  $$ = NULL;
}
;

database_name:
NAME_OB
{
  $$ = $1;
}
;

/*****************************************************************************
 *
 *	load data grammar
 *
 *****************************************************************************/
load_data_stmt:
load_data_with_opt_hint opt_load_local INFILE STRING_VALUE opt_duplicate INTO TABLE
relation_factor opt_load_charset field_opt line_opt opt_load_ignore_rows
opt_field_or_var_spec opt_load_set_spec
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOAD_DATA, 11,
                           $2,            /* 0. local */
                           $4,            /* 1. filename */
                           $5,            /* 2. duplicate  */
                           $8,            /* 3. table */
                           $9,           /* 4. charset */
                           $10,           /* 5. field */
                           $11,           /* 6. line */
                           $12,           /* 7. ignore rows */
                           $13,           /* 8. field or vars */
                           $14,           /* 9. set field  */
                           $1             /* 10. hint */
                           );
}
| load_data_with_opt_hint opt_load_local INFILE STRING_VALUE opt_duplicate INTO TABLE
relation_factor use_partition opt_load_charset field_opt line_opt opt_load_ignore_rows
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
 *	create synonym grammar
 *
 *****************************************************************************/

create_synonym_stmt:
CREATE opt_replace opt_public SYNONYM  synonym_name FOR synonym_object opt_dblink
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

| CREATE opt_replace opt_public SYNONYM database_factor '.' synonym_name FOR synonym_object opt_dblink
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

| CREATE opt_replace opt_public SYNONYM  synonym_name FOR database_factor '.' synonym_object opt_dblink
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
| CREATE opt_replace opt_public SYNONYM  database_factor '.' synonym_name FOR database_factor '.' synonym_object opt_dblink
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
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

synonym_object:
NAME_OB
{ $$ = $1; }
| unreserved_keyword
{
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
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

temporary_option:
GLOBAL TEMPORARY
{
  malloc_terminal_node($$, result->malloc_pool_, T_TEMPORARY); }
| /* EMPTY */
{ $$ = NULL; }
;

on_commit_option:
ON COMMIT DELETE ROWS
{
  malloc_terminal_node($$, result->malloc_pool_, T_TRANSACTION); //会话级别的临时表
}
| ON COMMIT PRESERVE ROWS
{
  malloc_terminal_node($$, result->malloc_pool_, T_MAX_SESSION_NUM); //借用其中的SESS*来标识是session级别的
}
| /* EMPTY */
{ $$ = NULL; }
;

/*****************************************************************************
 *
 *	create directory grammar
 *
 *****************************************************************************/
create_directory_stmt:
CREATE opt_replace DIRECTORY directory_name AS directory_path
{
  malloc_non_terminal_node($$,
                           result->malloc_pool_,
                           T_CREATE_DIRECTORY,
                           3,
                           $2,                   /* or replace */
                           $4,                   /* directory name */
                           $6                    /* directory path */
                           );
}
;

directory_name:
NAME_OB
{ $$ = $1; }
| unreserved_keyword
{
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

directory_path:
STRING_VALUE
{ $$ = $1; }
;

/*****************************************************************************
 *
 *	drop directory grammar
 *
 *****************************************************************************/
drop_directory_stmt:
DROP DIRECTORY directory_name
{
  malloc_non_terminal_node($$,
                           result->malloc_pool_,
                           T_DROP_DIRECTORY,
                           1,
                           $3                    /* directory name */
                           );
}
;

/*****************************************************************************
 *
 *	create keystore grammar
 *
 *****************************************************************************/
create_keystore_stmt:
ADMINISTER KEY MANAGEMENT CREATE KEYSTORE keystore_name IDENTIFIED BY password
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_KEYSTORE, 2, $6, $9);
}
;

/*****************************************************************************
 *
 *	alter keystore grammar
 *
 *****************************************************************************/
alter_keystore_stmt:
ADMINISTER KEY MANAGEMENT ALTER KEYSTORE PASSWORD IDENTIFIED BY password SET password
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_KEYSTORE_PASSWORD, 2, $9, $11);
}
| ADMINISTER KEY MANAGEMENT SET KEY IDENTIFIED BY password
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_KEYSTORE_SET_KEY, 1, $8);
}
| ADMINISTER KEY MANAGEMENT SET KEYSTORE CLOSE IDENTIFIED BY password
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_KEYSTORE_CLOSE, 1, $9);
}
| ADMINISTER KEY MANAGEMENT SET KEYSTORE OPEN IDENTIFIED BY password
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_KEYSTORE_OPEN, 1, $9);
}
;

/*****************************************************************************
 *
 *	create table grammar
 *
 *****************************************************************************/

create_table_stmt:
CREATE temporary_option TABLE relation_factor '(' table_element_list ')'
opt_table_option_list opt_partition_option on_commit_option
{
  ParseNode *table_elements = NULL;
  ParseNode *table_options = NULL;
  merge_nodes(table_elements, result, T_TABLE_ELEMENT_LIST, $6);
  merge_nodes(table_options, result, T_TABLE_OPTION_LIST, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 7,
                           $2,                   /* temporary option */
                           NULL,                   /* if not exists */
                           $4,                   /* table name */
                           table_elements,       /* columns or primary key */
                           table_options,        /* table option(s) */
                           $9,                  /* partition optition */
                           $10)                 /* on commit delete/preserve rows option */
}
| CREATE temporary_option TABLE relation_factor '(' table_element_list ')'
 opt_table_option_list opt_partition_option on_commit_option AS subquery opt_order_by opt_fetch_next
{
  ParseNode *table_elements = NULL;
  ParseNode *table_options = NULL;
  merge_nodes(table_elements, result, T_TABLE_ELEMENT_LIST, $6);
  merge_nodes(table_options, result, T_TABLE_OPTION_LIST, $8);
  $12->children_[PARSE_SELECT_ORDER] = $13;
  if ($12->children_[PARSE_SELECT_FETCH] != NULL && ($13 != NULL || $14 != NULL)) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    if ($14 != NULL) {
      $12->children_[PARSE_SELECT_FETCH] = $14;
    }
    malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 8,
                            $2,                   /* temporary option */
                            NULL,                   /* if not exists */
                            $4,                   /* table name */
                            table_elements,       /* columns or primary key */
                            table_options,        /* table option(s) */
                            $9,                   /* partition optition */
                            $10,                  /* on commit delete/preserve rows option */
                            $12);                 /* subquery */
  }
}
| CREATE temporary_option TABLE relation_factor table_option_list opt_partition_option
 on_commit_option AS subquery opt_order_by opt_fetch_next
{
  ParseNode *table_options = NULL;
  merge_nodes(table_options, result, T_TABLE_OPTION_LIST, $5);
  $9->children_[PARSE_SELECT_ORDER] = $10;
  if ($9->children_[PARSE_SELECT_FETCH] != NULL && ($10 != NULL || $11 != NULL)) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    if ($11 != NULL) {
      $9->children_[PARSE_SELECT_FETCH] = $11;
    }
    malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 8,
                             $2,                   /* temporary option */
                             NULL,                   /* if not exists */
                             $4,                   /* table name */
                             NULL,                 /* columns or primary key */
                             table_options,        /* table option(s) */
                             $6,                   /* partition optition */
                             $7,                   /* on commit delete/preserve rows option */
                             $9);                  /* subquery */
  }
}
| CREATE temporary_option TABLE relation_factor partition_option on_commit_option
 AS subquery opt_order_by opt_fetch_next
{
  $8->children_[PARSE_SELECT_ORDER] = $9;
  if ($8->children_[PARSE_SELECT_FETCH] != NULL && ($9 != NULL || $10 != NULL)) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    if ($10 != NULL) {
      $8->children_[PARSE_SELECT_FETCH] = $10;
    }
    malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 8,
                            $2,                   /* temporary option */
                            NULL,                   /* if not exists */
                            $4,                   /* table name */
                            NULL,                 /* columns or primary key */
                            NULL,                 /* table option(s) */
                            $5,                   /* partition optition */
                            $6,                   /* on commit delete/preserve rows option */
                            $8);                  /* subquery */
  }
}
| CREATE temporary_option TABLE relation_factor on_commit_option
 AS subquery opt_order_by opt_fetch_next
{
  $7->children_[PARSE_SELECT_ORDER] = $8;
  if ($7->children_[PARSE_SELECT_FETCH] != NULL && ($8 != NULL || $9 != NULL)) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    if ($9 != NULL) {
      $7->children_[PARSE_SELECT_FETCH] = $9;
    }
    malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 8,
                            $2,                   /* temporary option */
                            NULL,                   /* if not exists */
                            $4,                   /* table name */
                            NULL,                 /* columns or primary key */
                            NULL,                 /* table option(s) */
                            NULL,                 /* partition optition */
                            $5,                   /* on commit delete/preserve rows option */
                            $7);                  /* subquery */
  }
}
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
| out_of_line_constraint
{
  $$ = $1;
}
| INDEX opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list
{
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $4);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX, 4, NULL, col_list, index_option, $2);
  $$->value_ = 0;
}
| INDEX index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list
{
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $5);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX, 4, $2, col_list, index_option, $3);
  $$->value_ = 0;
}
/*
| out_of_line_ref_constraint
{
  $$ = $1;
}
*/
;

column_definition:
//column_name data_type opt_column_attribute_list
column_definition_ref data_type opt_visibility_option opt_column_attribute_list
{
  ParseNode *attributes = NULL;
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 4, $1, $2, attributes, $3);
}
| column_definition_ref data_type opt_visibility_option opt_generated_keyname AS '(' bit_expr ')' opt_storage_type opt_generated_column_attribute_list
{
  ParseNode *attributes = NULL;
  dup_expr_string($7, result, @7.first_column, @7.last_column);
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 7, $1, $2, attributes, $7, $9, $3, $4);
}
| column_definition_ref opt_visibility_option opt_generated_keyname AS '(' bit_expr ')' opt_storage_type opt_generated_column_attribute_list
{
  ParseNode *attributes = NULL;
  dup_expr_string($6, result, @6.first_column, @6.last_column);
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $9);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 7, $1, NULL, attributes, $6, $8, $2, $3);
}
| column_definition_ref data_type opt_visibility_option opt_generated_keyname AS opt_identity_attribute opt_sequence_option_list opt_column_attribute_list
{
  ParseNode *attributes = NULL;
  ParseNode *sequence_option = NULL;
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $8);
  merge_nodes(sequence_option, result, T_SEQUENCE_OPTION_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 7, $1, $2, attributes, sequence_option, $6, $3, $4);
}
| column_definition_ref opt_visibility_option opt_column_attribute_list
{
  ParseNode *attributes = NULL;
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 4, $1, NULL, attributes, $2);
}
| column_definition_ref opt_visibility_option opt_generated_keyname AS opt_identity_attribute opt_sequence_option_list opt_column_attribute_list
{
  ParseNode *attributes = NULL;
  ParseNode *sequence_option = NULL;
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $7);
  merge_nodes(sequence_option, result, T_SEQUENCE_OPTION_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 7, $1, NULL, attributes, sequence_option, $5, $2, $3);
}
;

column_definition_opt_datatype:
//column_name data_type opt_column_attribute_list
column_definition_ref
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 4, $1, NULL, NULL, NULL);
}
| column_definition_ref opt_column_attribute_list column_attribute
{
  ParseNode *attributes = NULL;
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $2, $3);
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $$);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 4, $1, NULL, attributes, NULL);
}
| column_definition_ref visibility_option opt_column_attribute_list
{
  ParseNode *attributes = NULL;
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 4, $1, NULL, attributes, $2);
}
| column_definition_ref data_type opt_visibility_option opt_column_attribute_list
{
  ParseNode *attributes = NULL;
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 4, $1, $2, attributes, $3);
}
| column_definition_ref opt_visibility_option opt_generated_keyname AS '(' bit_expr ')' opt_storage_type opt_generated_column_attribute_list
{
  ParseNode *attributes = NULL;
  dup_expr_string($6, result, @6.first_column, @6.last_column);
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $9);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 7, $1, NULL, attributes, $6, $8, $2, $3);
}
| column_definition_ref data_type opt_visibility_option opt_generated_keyname AS '(' bit_expr ')' opt_storage_type opt_generated_column_attribute_list
{
  ParseNode *attributes = NULL;
  dup_expr_string($7, result, @7.first_column, @7.last_column);
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 7, $1, $2, attributes, $7, $9, $3, $4);
}
| column_definition_ref data_type opt_visibility_option opt_generated_keyname AS opt_identity_attribute opt_sequence_option_list opt_column_attribute_list
{
  ParseNode *attributes = NULL;
  ParseNode *sequence_option = NULL;
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $8);
  merge_nodes(sequence_option, result, T_SEQUENCE_OPTION_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 7, $1, $2, attributes, sequence_option, $6, $3, $4);
}
;

out_of_line_constraint:
opt_constraint_and_name UNIQUE '(' sort_column_list ')' opt_using_index_clause
{
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $4);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX, 4, $1, col_list, index_option, NULL);
  $$->value_ = 1;
}
//PRIMARY是非保留关键字，所以不用opt_constraint_and_name
| PRIMARY KEY '(' column_name_list ')' opt_using_index_clause
{
  (void)($6);
  ParseNode *col_list= NULL;
  merge_nodes(col_list, result, T_COLUMN_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_KEY, 2, col_list, NULL);
}
| constraint_and_name PRIMARY KEY '(' column_name_list ')' opt_using_index_clause
{
  (void)($1);
  (void)($7);
  ParseNode *col_list= NULL;
  merge_nodes(col_list, result, T_COLUMN_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_KEY, 2, col_list, $1);
}
//FOREIGN是非保留关键字，所以不用opt_constraint_and_name
| constraint_and_name FOREIGN KEY '(' column_name_list ')' references_clause constranit_state
{
  (void)($8);
  ParseNode *child_col_list= NULL;
  merge_nodes(child_col_list, result, T_COLUMN_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOREIGN_KEY, 4, $1, child_col_list, $7, $8);
}
| FOREIGN KEY '(' column_name_list ')' references_clause constranit_state
{
  (void)($7);
  ParseNode *child_col_list= NULL;
  merge_nodes(child_col_list, result, T_COLUMN_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOREIGN_KEY, 4, NULL, child_col_list, $6, $7);
}
| opt_constraint_and_name CHECK '(' expr ')' constranit_state
{
  dup_expr_string($4, result, @4.first_column, @4.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 3, $1, $4, $6);
  $$->value_ = 1;
}
;

/* out_of_line_ref_constraint: { $$ = NULL; } */

constranit_state:
opt_rely_option opt_using_index_clause opt_enable_option opt_validate_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTRAINT_STATE, 4, $1, $2, $3, $4);
}
;

opt_rely_option:
RELY
{
  malloc_terminal_node($$, result->malloc_pool_, T_RELY_CONSTRAINT);
}
| NORELY
{
  malloc_terminal_node($$, result->malloc_pool_, T_NORELY_CONSTRAINT);
}
| /*empty*/
{
  $$ = NULL;
}
;

opt_using_index_clause:
USING INDEX opt_index_options %prec LOWER_PARENS
{
  $$ = $3;
}
| USING INDEX %prec LOWER_PARENS
{
  $$ = NULL;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

opt_enable_option:
enable_option
{
  $$ = $1;
}
| /*empty*/
{
  $$ = NULL;
}
;

enable_option:
ENABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ENABLE_CONSTRAINT);
}
| DISABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISABLE_CONSTRAINT);
}
;

opt_validate_option:
VALIDATE
{
  malloc_terminal_node($$, result->malloc_pool_, T_VALIDATE_CONSTRAINT);
}
| NOVALIDATE
{
  malloc_terminal_node($$, result->malloc_pool_, T_NOVALIDATE_CONSTRAINT);
}
| /*empty*/
{
  $$ = NULL;
}
;

references_clause:
REFERENCES normal_relation_factor '(' column_name_list ')' opt_reference_option
{
  ParseNode *parent_col_list= NULL;
  merge_nodes(parent_col_list, result, T_COLUMN_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_REFERENCES_CLAUSE, 3, $2, parent_col_list, $6);
}
| REFERENCES normal_relation_factor opt_reference_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_REFERENCES_CLAUSE, 3, $2, NULL, $3);
}
;

opt_reference_option:
reference_option
{
  $$ = $1;
}
| /*empty*/
{
  $$ = NULL;
}
;

reference_option:
ON DELETE reference_action
{
  malloc_terminal_node($$, result->malloc_pool_, T_REFERENCE_OPTION);
  $$->int32_values_[0] = T_DELETE;
  $$->int32_values_[1] = $3[0];
}
;

reference_action:
CASCADE
{
  $$[0] = T_CASCADE;
}
| SET NULLX
{
  (void)($2);
  $$[0] = T_SET_NULL;
}
;

opt_generated_keyname:
GENERATED opt_generated_option_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_GENERATED_COLUMN, 1, $2);
}
| /*empty*/
{
  $$ = NULL;
}
;

opt_generated_option_list:
ALWAYS
{
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_ALWAYS);
}
| BY DEFAULT opt_generated_identity_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_DEFAULT, 1, $3);
}
| /*empty*/
{
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_ALWAYS);
}
;

opt_generated_identity_option:
ON NULLX
{
  (void)($2) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NULL);
}
| /*empty*/
{
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NOT_NULL);
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
opt_constraint_and_name NOT NULLX constranit_state
{
  (void)($3) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_NOT_NULL, 2, $1, $4);
}
| opt_constraint_and_name NULLX
{
  (void)($1) ;
  (void)($2) ; /* make bison mute */
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
| /*empty*/
{
  $$ = NULL;
}
;

opt_identity_attribute:
IDENTITY
{
  malloc_terminal_node($$, result->malloc_pool_, T_IDENTITY_COLUMN);
}
;

/* 用于insert列列表，column_list，update的assignment等 */
/* 相比于column_ref，不包含collate等表达式中出现时候可以使用的语法 */
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

column_definition_opt_datatype_list:
column_definition_opt_datatype
{
  $$ = $1;
}
| column_definition_opt_datatype_list ',' column_definition_opt_datatype
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}

column_name_list:
column_name
{
  $$ = $1;
}
|  column_name_list ',' column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

cast_data_type:
RAW '(' INTNUM ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_RAW;
  $$->int16_values_[OB_NODE_CAST_COLL_IDX] = BINARY_COLLATION;
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = $3->value_;
  $$->param_num_ = 1;
}
| CHARACTER opt_string_length_i_v2 opt_binary
{
  (void)($3);
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_CHAR;//to keep consitent with mysql
  $$->int16_values_[OB_NODE_CAST_COLL_IDX] = INVALID_COLLATION;        /* is char */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = $2[0];        /* length */
  $$->length_semantics_ = ($2[2] & 0x03); /* length semantics type */
  $$->param_num_ = $2[1]; /* opt_binary的常数个数一定为0 */
}
| CHAR opt_string_length_i_v2 opt_binary
{
  (void)($3);
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_CHAR;//to keep consitent with mysql
  $$->int16_values_[OB_NODE_CAST_COLL_IDX] = INVALID_COLLATION;        /* is char */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = $2[0];        /* length */
  $$->length_semantics_ = ($2[2] & 0x03); /* length semantics type */
  $$->param_num_ = $2[1]; /* opt_binary的常数个数一定为0 */
}
| varchar_type_i string_length_i opt_binary
{
  if (result->pl_parse_info_.is_pl_parse_ && result->pl_parse_info_.is_pl_parse_expr_) {
    yyerror(NULL, result, "cast(x as varchar2(length)) not allow in pl expr context");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    (void)($3);
    malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
    $$->value_ = 0;
    $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_VARCHAR;//to keep consitent with mysql
    $$->int16_values_[OB_NODE_CAST_COLL_IDX] = INVALID_COLLATION;        /* is char */
    $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = $2[0];        /* length */
    $$->length_semantics_ = ($2[2] & 0x03); /* length semantics type */
    $$->param_num_ = $2[1]; /* opt_binary的常数个数一定为0 */
  }
}
| varchar_type_i
{
  if (!result->pl_parse_info_.is_pl_parse_) {
    yyerror(NULL, result, "cast(x as varchar2) not in pl context");
    YYABORT_PARSE_SQL_ERROR;
  } else if (!result->pl_parse_info_.is_pl_parse_expr_) {
    yyerror(NULL, result, "cast(x as varchar2) not in pl expr context");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
    $$->value_ = 0;
    $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_VARCHAR;
    $$->int16_values_[OB_NODE_CAST_COLL_IDX] = INVALID_COLLATION;
    $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = DEFAULT_STR_LENGTH;
    $$->length_semantics_ = (2 &0x3);
  }
}
| NVARCHAR2 nstring_length_i
{
  if (result->pl_parse_info_.is_pl_parse_ && result->pl_parse_info_.is_pl_parse_expr_) {
    yyerror(NULL, result, "cast(x as varchar2(length)) not allow in pl expr context");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
    $$->value_ = 0;
    $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NVARCHAR2;
    $$->int16_values_[OB_NODE_CAST_COLL_IDX] = INVALID_COLLATION;
    $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = $2[0];
    $$->length_semantics_ = 1; /* character */
    $$->param_num_ = $2[1];
  }
}
| NCHAR nstring_length_i
{
  if (result->pl_parse_info_.is_pl_parse_ && result->pl_parse_info_.is_pl_parse_expr_) {
    yyerror(NULL, result, "cast(x as varchar2(length)) not allow in pl expr context");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
    $$->value_ = 0;
    $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NCHAR;
    $$->int16_values_[OB_NODE_CAST_COLL_IDX] = INVALID_COLLATION;
    $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = $2[0];
    $$->length_semantics_ = 1; /* character */
    $$->param_num_ = $2[1];
  }
}
| UROWID opt_urowid_length_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_UROWID;
  $$->int16_values_[OB_NODE_CAST_C_LEN_IDX] = $2[0];
  $$->param_num_ = $2[1];
}
| ROWID opt_urowid_length_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_UROWID;
  $$->int16_values_[OB_NODE_CAST_C_LEN_IDX] = $2[0];
  $$->param_num_ = $2[1];
}
| cast_datetime_type_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = $1[0];
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = 0;
  $$->param_num_ = $1[1];
}
| TIMESTAMP opt_datetime_fsp_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_TIMESTAMP_NANO;
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2[0];
  $$->param_num_ = $2[1];
}
| TIMESTAMP opt_datetime_fsp_i WITH TIME ZONE
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_TIMESTAMP_TZ;
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2[0];
  $$->param_num_ = $2[1];
}
| TIMESTAMP opt_datetime_fsp_i WITH LOCAL TIME ZONE
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_TIMESTAMP_LTZ;
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2[0];
  $$->param_num_ = $2[1];
}
| int_type_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NUMBER;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = 38;    /* precision, ORACLE treat int as number(38, 0) */
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = 0;    /* scale */
  $$->param_num_ = 0; /* 对于cast (bit_expr as int), if param_num_ > 0, normal parser的常量个数会比fast parser多param_num_，所以这里设置0*/
}
| NUMBER opt_number_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NUMBER;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = $2->int16_values_[0];    /* precision */
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2->int16_values_[1];    /* scale */
  $$->int16_values_[OB_NODE_CAST_NUMBER_TYPE_IDX] = $2->int16_values_[2];    /* number type */
  $$->param_num_ = $2->param_num_;
}
| NUMERIC opt_numeric_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NUMBER;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = $2->int16_values_[0];    /* precision */
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2->int16_values_[1];    /* scale */
  $$->int16_values_[OB_NODE_CAST_NUMBER_TYPE_IDX] = $2->int16_values_[2];    /* number type */
  $$->param_num_ = $2->param_num_;
}
| DECIMAL opt_numeric_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NUMBER;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = $2->int16_values_[0];    /* precision */
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2->int16_values_[1];    /* scale */
  $$->int16_values_[OB_NODE_CAST_NUMBER_TYPE_IDX] = $2->int16_values_[2];    /* number type */
  $$->param_num_ = $2->param_num_;
}
| DEC opt_dec_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NUMBER;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = $2->int16_values_[0];    /* precision */
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2->int16_values_[1];    /* scale */
  $$->int16_values_[OB_NODE_CAST_NUMBER_TYPE_IDX] = $2->int16_values_[2];    /* number type */
  $$->param_num_ = $2->param_num_;
}
| FLOAT opt_float_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NUMBER_FLOAT;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = $2->int16_values_[0];    /* precision */
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2->int16_values_[1];    /* scale */
  $$->param_num_ = $2->param_num_;
}
| REAL
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NUMBER_FLOAT;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = 63;    /* precision */
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = -1;   /* scale */
  $$->param_num_ = 0;
}
| double_type_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = $1[0];
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = -1;
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = -85;  /* SCALE_UNKNOWN_YET */
  $$->param_num_ = 0;
}
| INTERVAL YEAR opt_interval_leading_fsp_i TO MONTH
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_INTERVAL_YM;
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $3[0];
  $$->param_num_ = $3[1];
}
| INTERVAL DAY opt_interval_leading_fsp_i TO SECOND opt_datetime_fsp_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_INTERVAL_DS;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = $3[0]; //day scale
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $6[0]; //second scale
  $$->param_num_ = $3[1] + $6[1];
}
| udt_type
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT, 1, $1);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_EXTEND;
  $$->param_num_ = 0;
}
| CLOB
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_LONGTEXT;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = 0;
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = 0;
  $$->param_num_ = 0;
}
| BLOB
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_LONGTEXT;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = 0;
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = 0;
  $$->param_num_ = 0;
}
;

udt_type:
type_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SP_TYPE, 2, NULL, $1);
}
| database_name '.' type_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SP_TYPE, 2, $1, $3);
}
;

type_name:
NAME_OB
{ $$ = $1; }
;

cast_datetime_type_i:
DATE        { $$[0] = T_DATETIME; $$[1] = 0; }
;

data_type:
int_type_i
{
  /* same as number(38, 0) */
  malloc_terminal_node($$, result->malloc_pool_, T_NUMBER);
  $$->int16_values_[0] = 38;
  $$->int16_values_[1] = 0;
  /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
  $$->int16_values_[2] = 0;
}
| FLOAT opt_float_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_NUMBER_FLOAT);
  $$->int16_values_[0] = $2->int16_values_[0];
  $$->int16_values_[1] = $2->int16_values_[1];
}
| REAL
{
  malloc_terminal_node($$, result->malloc_pool_, T_NUMBER_FLOAT);
  $$->int16_values_[0] = 63;
  $$->int16_values_[1] = -1;
}
| double_type_i
{
  malloc_terminal_node($$, result->malloc_pool_, $1[0]);
  $$->int16_values_[0] = -1;
  $$->int16_values_[1] = -85;  /* SCALE_UNKNOWN_YET */   $$->int16_values_[2] = 0;
}
| NUMBER opt_number_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_NUMBER);
  $$->int16_values_[0] = $2->int16_values_[0];
  $$->int16_values_[1] = $2->int16_values_[1];
  $$->int16_values_[2] = $2->int16_values_[2];
  $$->param_num_ = $2->param_num_;
}
| NUMERIC opt_numeric_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_NUMBER);
  $$->int16_values_[0] = $2->int16_values_[0];
  $$->int16_values_[1] = $2->int16_values_[1];
  $$->int16_values_[2] = $2->int16_values_[2];
  $$->param_num_ = $2->param_num_;
}
| DECIMAL opt_numeric_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_NUMBER);
  $$->int16_values_[0] = $2->int16_values_[0];
  $$->int16_values_[1] = $2->int16_values_[1];
  $$->int16_values_[2] = $2->int16_values_[2];
  $$->param_num_ = $2->param_num_;
}
| DEC opt_dec_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_NUMBER);
  $$->int16_values_[0] = $2->int16_values_[0];
  $$->int16_values_[1] = $2->int16_values_[1];
  $$->int16_values_[2] = $2->int16_values_[2];
  $$->param_num_ = $2->param_num_;
}
| TIMESTAMP opt_datetime_fsp_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_TIMESTAMP_NANO);
  $$->int16_values_[1] = $2[0];
  $$->int16_values_[2] = $2[1];
}
| TIMESTAMP opt_datetime_fsp_i WITH TIME ZONE
{
  malloc_terminal_node($$, result->malloc_pool_, T_TIMESTAMP_TZ);
  $$->int16_values_[1] = $2[0];
  $$->int16_values_[2] = $2[1];
}
| TIMESTAMP opt_datetime_fsp_i WITH LOCAL TIME ZONE
{
  malloc_terminal_node($$, result->malloc_pool_, T_TIMESTAMP_LTZ);
  $$->int16_values_[1] = $2[0];
  $$->int16_values_[2] = $2[1];
}
| datetime_type_i
{
  malloc_terminal_node($$, result->malloc_pool_, $1[0]);
  $$->int16_values_[1] = 0;
  $$->int16_values_[2] = 0;
}
| CHARACTER opt_string_length_i opt_binary opt_charset opt_collation
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHAR, 3, $4, $5, $3);
  if ($2[0] < 0) {
    $2[0] = 1;
  }
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 0; /* is char */
  $$->length_semantics_ = ($2[2] & 0x03); /* length semantics type */
}
| CHAR opt_string_length_i opt_binary opt_charset opt_collation
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHAR, 3, $4, $5, $3);
  if ($2[0] < 0) {
    $2[0] = 1;
  }
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 0; /* is char */
  $$->length_semantics_ = ($2[2] & 0x03); /* length semantics type */
}
| varchar_type_i string_length_i opt_binary opt_charset opt_collation
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 3, $4, $5, $3);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 0; /* is char */
  $$->length_semantics_ = ($2[2] & 0x03); /* length semantics type */
}
| RAW '(' INTNUM ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_RAW);
  $$->int32_values_[0] = $3->value_;
  $$->int32_values_[1] = 1;
}
| STRING_VALUE /* wrong or unsupported data type */
{
  malloc_terminal_node($$, result->malloc_pool_, T_INVALID);
  $$->str_value_ = $1->str_value_;
  $$->str_len_ = $1->str_len_;
}
| BLOB /* oracle 12c only support collate @hanhui TODO */
{
  malloc_terminal_node($$, result->malloc_pool_, T_LONGTEXT);
  $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
  $$->int32_values_[1] = 1; /* is binary */
}
| CLOB opt_charset opt_collation/* oracle 12c only support collate @hanhui TODO */
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LONGTEXT, 2, $2, $3);
  $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
  $$->int32_values_[1] = 0; /* is text */
}
/*
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
/*
}
| SET '(' string_list ')' opt_binary opt_charset opt_collation
{
  ParseNode *string_list_node = NULL;
  merge_nodes(string_list_node, result, T_STRING_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET, 4, $6, $7, $5, string_list_node);
  $$->int32_values_[0] = 0;//not used so far
  $$->int32_values_[1] = 0; /* is char */
/*} */
| INTERVAL YEAR opt_interval_leading_fsp_i TO MONTH
{
  malloc_terminal_node($$, result->malloc_pool_, T_INTERVAL_YM);
  $$->int16_values_[0] = $3[0];
  $$->int16_values_[1] = $3[1];
}
| INTERVAL DAY opt_interval_leading_fsp_i TO SECOND opt_datetime_fsp_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_INTERVAL_DS);
  $$->int16_values_[0] = $3[0];
  $$->int16_values_[1] = $3[1];
  $$->int16_values_[2] = $6[0];
  $$->int16_values_[3] = $6[1];
}
| NVARCHAR2 nstring_length_i
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NVARCHAR2, 3, NULL, NULL, NULL);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 0; /* is char */
  $$->length_semantics_ = 1; /* length semantics type */
}
| NCHAR nstring_length_i
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NCHAR, 3, NULL, NULL, NULL);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 0; /* is char */
  $$->length_semantics_ = 1; /* length semantics type */
}
| UROWID opt_urowid_length_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_UROWID);
  $$->int32_values_[0] = $2[0];
}
| ROWID opt_urowid_length_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_UROWID);
  $$->int32_values_[0] = $2[0];
}
;
/*
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
};
*/

int_type_i:
SMALLINT    { $$[0] = T_SMALLINT; }
| INT     { $$[0] = T_INT32; }
| INTEGER     { $$[0] = T_INT32; }
;

varchar_type_i:
VARCHAR     { $$[0] = T_VARCHAR; }
| VARCHAR2    { $$[0] = T_VARCHAR; }
;

double_type_i:
BINARY_DOUBLE      { $$[0] = T_DOUBLE; }
| BINARY_FLOAT       { $$[0] = T_FLOAT; }
;

//oracle mode treate date as datetime
datetime_type_i:
DATE        { $$[0] = T_DATETIME; }
;

//opt_bit_length_i:
//'(' INTNUM ')'  { $$[0] = $2->value_; }
//| /*EMPTY*/       { $$[0] = 1; }
//;



opt_float_precision:
'(' INTNUM ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  $$->int16_values_[0] = $2->value_;
  $$->int16_values_[1] = -1;
  $$->param_num_ = 1;
}
| '('  ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  $$->int16_values_[0] = 0;
  $$->int16_values_[1] = -1;
  $$->param_num_ = 0;
}
| /*empty*/
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  $$->int16_values_[0] = 126;
  $$->int16_values_[1] = -1;
  $$->param_num_ = 0;
}
;


number_precision:
'(' signed_int_num ',' signed_int_num ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  if($2[0] > OB_MAX_PARSER_INT16_VALUE) {
    $$->int16_values_[0] = OB_MAX_PARSER_INT16_VALUE;
  } else {
    $$->int16_values_[0] = $2[0];
  }
  if($4[0] > OB_MAX_PARSER_INT16_VALUE) {
    $$->int16_values_[1] = OB_MAX_PARSER_INT16_VALUE;
  } else {
    $$->int16_values_[1] = $4[0];
  }
  $$->int16_values_[2] = NPT_PERC_SCALE;
  $$->param_num_ = 2;
}
| '(' '*' ',' signed_int_num ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  $$->int16_values_[0] = -1;
  if($4[0] > OB_MAX_PARSER_INT16_VALUE) {
    $$->int16_values_[1] = OB_MAX_PARSER_INT16_VALUE;
  } else {
    $$->int16_values_[1] = $4[0];
  }
  $$->int16_values_[2] = NPT_STAR_SCALE;
  $$->param_num_ = 1;
}
| '(' '*' ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  $$->int16_values_[0] = -1;
  $$->int16_values_[1] = -85;  /* SCALE_UNKNOWN_YET */
  $$->int16_values_[2] = NPT_STAR;
  $$->param_num_ = 0;
}
| '(' signed_int_num ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  if($2[0] > OB_MAX_PARSER_INT16_VALUE) {
    $$->int16_values_[0] = OB_MAX_PARSER_INT16_VALUE;
  } else {
    $$->int16_values_[0] = $2[0];
  }
  $$->int16_values_[1] = 0;
  $$->int16_values_[2] = NPT_PERC;
  $$->param_num_ = 1;
};

opt_number_precision:
number_precision { $$ = $1; }
| /*EMPTY*/
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  $$->int16_values_[0] = -1;
  $$->int16_values_[1] = -85;  /* SCALE_UNKNOWN_YET */
  $$->int16_values_[2] = NPT_EMPTY;
  $$->param_num_ = 0;
}
;

opt_numeric_precision:
number_precision { $$ = $1; }
| /*EMPTY*/
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  $$->int16_values_[0] = -1;
  $$->int16_values_[1] = 0;  /* SCALE_UNKNOWN_YET */
  $$->int16_values_[2] = NPT_EMPTY;
  $$->param_num_ = 0;
}
;

opt_dec_precision:
number_precision
{
  if (NPT_STAR == $1->int16_values_[2]) {
    $1->int16_values_[1] = 0;
  }
  $$ = $1;
}
| /*EMPTY*/
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  $$->int16_values_[0] = -1;
  $$->int16_values_[1] = 0;
  $$->int16_values_[2] = NPT_EMPTY;
  $$->param_num_ = 0;
}
;

signed_int_num:
INTNUM { $$[0] = $1->value_; }
| '-' INTNUM
{
  $$[0] = -$2->value_;
}
;

precision_int_num:
INTNUM {
  $$ = $1;
  if ($1->value_ >= INT16_MAX) {
    $$->value_ = INT16NUM_OVERFLOW; //let resolver failed
  }
}
;

opt_datetime_fsp_i:
'(' precision_int_num ')'  { $$[0] = $2->value_; $$[1] = 1; }
| /*EMPTY*/     { $$[0] = 6; $$[1] = 0;}
;

//interval year/day precision
opt_interval_leading_fsp_i:
'(' precision_int_num ')'  { $$[0] = $2->value_; $$[1] = 1; }
| /*EMPTY*/     { $$[0] = 2; $$[1] = 0;}
;

nstring_length_i:
'(' INTNUM ')'
{
  if ($2->value_ > UINT32_MAX) {
    $$[0] = OUT_OF_STR_LEN;
  } else if ($2->value_ > MAX_VARCHAR_LENGTH) {
    $$[0] = MAX_VARCHAR_LENGTH;
  } else {
    $$[0] = $2->value_;
  }
  $$[1] = 1;
}
;


string_length_i:
'(' INTNUM opt_length_semantics_i ')'
{
  if ($2->value_ > UINT32_MAX) {
    $$[0] = OUT_OF_STR_LEN;
  } else if ($2->value_ > MAX_VARCHAR_LENGTH) {
    $$[0] = MAX_VARCHAR_LENGTH;
  } else {
    $$[0] = $2->value_;
  }
  $$[1] = 1;
  $$[2] = $3[0];
}
;

opt_length_semantics_i:
/*EMPTY*/         { $$[0] = 0; }
| CHARACTER       { $$[0] = 1; }
| CHAR       { $$[0] = 1; }
| BYTE            { $$[0] = 2; }
;

opt_string_length_i:
string_length_i
{
  $$[0] = $1[0];
  $$[1] = $1[1];
  $$[2] = $1[2];
}
| /*EMPTY*/
{
  $$[0] = 1;
  $$[1] = 0;
  $$[2] = 0;
}
;

opt_string_length_i_v2:
string_length_i { $$[0] = $1[0]; $$[1] = $1[1]; $$[2] = $1[2]; }
| /*EMPTY*/       { $$[0] = 1; $$[1] = 0; $$[2] = 0; } // undefined str_len , -1.


opt_binary:
BINARY
{
  malloc_terminal_node($$, result->malloc_pool_, T_BINARY);
  $$->value_ = 1;
}
| /*EMPTY*/ {$$ = 0; }
;

urowid_length_i:
'(' INTNUM ')'
{
  $$[0] = $2->value_;
}
;

opt_urowid_length_i:
urowid_length_i
{
  $$[0] = $1[0];
  $$[1] = 1;
}
| /*EMPTY*/
{
  $$[0] = 4000; // 默认是4000
  $$[1] = 0;
}
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
opt_constraint_and_name NOT NULLX constranit_state
{
  (void)($3) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_NOT_NULL, 2, $1, $4);
}
| opt_constraint_and_name NULLX
{
  (void)($1) ;
  (void)($2) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NULL);
}
| DEFAULT bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_DEFAULT, 1, $2);
  dup_expr_string($$, result, @2.first_column, @2.last_column);
}
| ORIG_DEFAULT now_or_signed_literal
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_ORIG_DEFAULT, 1, $2);
}
| opt_constraint_and_name PRIMARY KEY
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_PRIMARY_KEY, 1, $1);
}
| opt_constraint_and_name UNIQUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_UNIQUE_KEY, 1, $1);
}
| ID INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ID, 1, $2);
}
| opt_constraint_and_name CHECK '(' expr ')' constranit_state
{
  dup_expr_string($4, result, @4.first_column, @4.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 3, $1, $4, $6);
  $$->value_ = 1;
}
| opt_constraint_and_name references_clause constranit_state
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOREIGN_KEY, 3, $1, $2, $3);
}
;

now_or_signed_literal:
cur_timestamp_func_params
{
  $$ = $1;
}
| signed_literal_params
{
  $$ = $1;
}
;

cur_timestamp_func_params:
'('  cur_timestamp_func_params ')'
{
  $$ = $2;
}
| cur_timestamp_func
{
  $$ = $1;
}
;

signed_literal_params:
'('  signed_literal_params ')'
{
  $$ = $2;
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
  $2->value_ = -$2->value_;
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
| relation_name_or_string
{
  $$ = $1;
}
;

opt_tablespace_option:
TABLESPACE tablespace
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLESPACE, 1, $2);
}
|
{
  $$ = NULL;
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
parallel_option
{
  $$ = $1
}
| TABLE_MODE opt_equal_mark STRING_VALUE
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
| EXPIRE_INFO opt_equal_mark '(' bit_expr ')'
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
| compress_option
{
  $$ = $1;
}
| USE_BLOOM_FILTER opt_equal_mark BOOL_VALUE
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_BLOOM_FILTER, 1, $3);
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
| MAX_USED_PART_ID opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_USED_PART_ID, 1, $3);
}
| ENABLE ROW MOVEMENT
{
  ParseNode *boo_node = NULL;
  malloc_terminal_node(boo_node, result->malloc_pool_, T_BOOL);
  boo_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ENABLE_ROW_MOVEMENT, 1, boo_node);
}
| DISABLE ROW MOVEMENT
{
  ParseNode *boo_node = NULL;
  malloc_terminal_node(boo_node, result->malloc_pool_, T_BOOL);
  boo_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ENABLE_ROW_MOVEMENT, 1, boo_node);
}
| physical_attributes_option
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
};

storage_options_list:
storage_option
{
  $$ = $1;
}
| storage_options_list storage_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
};

storage_option:
INITIAL_ size_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_STORAGE_INITIAL, 1, $2);
}
| NEXT size_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_STORAGE_NEXT, 1, $2);
}
| MINEXTENTS INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_STORAGE_MINEXTENTS, 1, $2);
}
| MAXEXTENTS int_or_unlimited
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_STORAGE_MAXEXTENTS, 1, $2);
};

size_option:
INTNUM opt_unit_of_size
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SIZE_OPTION, 2, $1, $2);
};

int_or_unlimited:
INTNUM
{
  malloc_terminal_node($$, result->malloc_pool_, T_SIZE_INT);
  $$->value_ = $1->value_;
}
| UNLIMITED
{
  malloc_terminal_node($$, result->malloc_pool_, T_SIZE_UNLIMITED);
};

opt_unit_of_size:
unit_of_size
{
  $$ =$1;
}
| // EMPTY
{
  $$ = NULL;
};

unit_of_size:
K
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = SIZE_UNIT_TYPE_K;
}
| M
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = SIZE_UNIT_TYPE_M;
}
| G
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = SIZE_UNIT_TYPE_G;
}
| T
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = SIZE_UNIT_TYPE_T;
}
| P
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = SIZE_UNIT_TYPE_P;
}
| E
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = SIZE_UNIT_TYPE_E;
}
;

relation_name_or_string:
relation_name
{ $$ = $1; $$->type_ = T_VARCHAR;}
| STRING_VALUE {$$ = $1;}
;


opt_equal_mark:
COMP_EQ     { $$ = NULL; }
| /*EMPTY*/   { $$ = NULL; }
;

opt_default_mark:
DEFAULT     { $$ = NULL; }
| /*EMPTY*/   { $$ = NULL; }
;

partition_option_inner:
hash_partition_option
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
partition_option
{
  $$ = $1;
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_COLUMNS_PARTITION, 1, params);
}
| PARTITION BY RANGE '('column_name_list')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_COLUMNS_PARTITION, 1, params);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
;


partition_option:
partition_option_inner opt_column_partition_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_OPTION, 2, $1, $2);
}
;

hash_partition_option:
PARTITION BY HASH '(' column_name_list ')' subpartition_option opt_partitions opt_hash_partition_list opt_tablespace_option opt_compress_option
{
  (void)($11);
  ParseNode *params = NULL;
  ParseNode *hash_func = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_hash");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, params);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 6, $$, $8, $9, $7, NULL, $10);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
;

list_partition_option:
PARTITION BY LIST '(' column_name_list ')' subpartition_option opt_list_partition_list
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_COLUMNS_PARTITION, 5, params, $8, $7, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
;

range_partition_option:
PARTITION BY RANGE '(' column_name_list ')' subpartition_option opt_range_partition_list
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_COLUMNS_PARTITION, 5, params, $8, $7, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
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
;

subpartition_template_option:
SUBPARTITION BY HASH '(' column_name_list ')' SUBPARTITION TEMPLATE opt_hash_subpartition_list
{
  ParseNode *params = NULL;
  ParseNode *hash_func = NULL;
  ParseNode *template_mark = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_hash");
  make_name_node(template_mark, result->malloc_pool_, "template_mark");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, params);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 6, $$, NULL, $9, NULL, template_mark, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| SUBPARTITION BY RANGE '(' column_name_list ')' SUBPARTITION TEMPLATE opt_range_subpartition_list
{
  ParseNode *params = NULL;
  ParseNode *template_mark = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $5);
  make_name_node(template_mark, result->malloc_pool_, "template_mark");
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_COLUMNS_PARTITION, 5, params, $9, NULL, NULL, template_mark);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| SUBPARTITION BY LIST '(' column_name_list ')' SUBPARTITION TEMPLATE opt_list_subpartition_list
{
  ParseNode *params = NULL;
  ParseNode *template_mark = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $5);
  make_name_node(template_mark, result->malloc_pool_, "template_mark");
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_COLUMNS_PARTITION, 5, params, $9, NULL, NULL, template_mark);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
|
{
  $$ = NULL;
}
;

subpartition_individual_option:
SUBPARTITION BY HASH '(' column_name_list ')' opt_subpartitions
{
  ParseNode *params = NULL;
  ParseNode *hash_func = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_hash");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, params);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 6, $$, $7, NULL, NULL, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| SUBPARTITION BY RANGE '(' column_name_list ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_COLUMNS_PARTITION, 5, params, NULL, NULL, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| SUBPARTITION BY LIST '(' column_name_list ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_COLUMNS_PARTITION, 5, params, NULL, NULL, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
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

opt_hash_partition_list:
'(' hash_partition_list')'
{
  merge_nodes($$, result, T_PARTITION_LIST, $2);
}
|
{
  $$ = NULL;
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
// PARTITION relation_factor opt_part_id opt_physical_attributes_options opt_compress_option opt_subpartition_list
// {
//   (void) ($5);
//   malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, $3, $4, $6);
// }
// | PARTITION opt_part_id opt_physical_attributes_options opt_compress_option opt_subpartition_list
// {
//   (void) ($4);
//   malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, $2, $3, $5);
// }
// ;
// 上面这种写法会造成规约冲突，只能拆成下面这种很复杂的写法
PARTITION %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, NULL, NULL, NULL);
}
| PARTITION relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, NULL, NULL, NULL);
}
| PARTITION ID INTNUM
{
  ParseNode *part_id = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, part_id, NULL, NULL);
}
| PARTITION relation_factor ID INTNUM
{
  ParseNode *part_id = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, part_id, NULL, NULL);
}
| PARTITION TABLESPACE tablespace
{
  (void)($2);
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, NULL, tablespace, NULL);
}
| PARTITION relation_factor TABLESPACE tablespace
{
  (void)($3);
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, NULL, tablespace, NULL);
}
| PARTITION ID INTNUM TABLESPACE tablespace
{
  (void)($4);
  ParseNode *part_id = NULL;
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $3);
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, part_id, tablespace, NULL);
}
| PARTITION relation_factor ID INTNUM TABLESPACE tablespace
{
  (void)($5);
  ParseNode *part_id = NULL;
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $4);
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, part_id, tablespace, NULL);
}
| PARTITION compress_option
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, NULL, NULL, NULL);
}
| PARTITION relation_factor compress_option
{
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, NULL, NULL, NULL);
}
| PARTITION ID INTNUM compress_option
{
  (void)($4);
  ParseNode *part_id = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, part_id, NULL, NULL);
}
| PARTITION relation_factor ID INTNUM compress_option
{
  (void)($5);
  ParseNode *part_id = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, part_id, NULL, NULL);
}
| PARTITION TABLESPACE tablespace compress_option
{
  (void)($2);
  (void)($4);
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, NULL, tablespace, NULL);
}
| PARTITION relation_factor TABLESPACE tablespace compress_option
{
  (void)($3);
  (void)($5);
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, NULL, tablespace, NULL);
}
| PARTITION ID INTNUM TABLESPACE tablespace compress_option
{
  (void)($4);
  (void)($6);
  ParseNode *part_id = NULL;
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $3);
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, part_id, tablespace, NULL);
}
| PARTITION relation_factor ID INTNUM TABLESPACE tablespace compress_option
{
  (void)($5);
  (void)($7);
  ParseNode *part_id = NULL;
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $4);
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, part_id, tablespace, NULL);
}
| PARTITION subpartition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, NULL, NULL, $2);
}
| PARTITION relation_factor subpartition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, NULL, NULL, $3);
}
| PARTITION ID INTNUM subpartition_list
{
  ParseNode *part_id = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, part_id, NULL, $4);
}
| PARTITION relation_factor ID INTNUM subpartition_list
{
  ParseNode *part_id = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, part_id, NULL, $5);
}
| PARTITION TABLESPACE tablespace subpartition_list
{
  (void)($2);
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, NULL, tablespace, $4);
}
| PARTITION relation_factor TABLESPACE tablespace subpartition_list
{
  (void)($3);
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, NULL, tablespace, $5);
}
| PARTITION ID INTNUM TABLESPACE tablespace subpartition_list
{
  (void)($4);
  ParseNode *part_id = NULL;
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $3);
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, part_id, tablespace, $6);
}
| PARTITION relation_factor ID INTNUM TABLESPACE tablespace subpartition_list
{
  (void)($5);
  ParseNode *part_id = NULL;
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $4);
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, part_id, tablespace, $7);
}
| PARTITION compress_option subpartition_list
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, NULL, NULL, $3);
}
| PARTITION relation_factor compress_option subpartition_list
{
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, NULL, NULL, $4);
}
| PARTITION ID INTNUM compress_option subpartition_list
{
  (void)($4);
  ParseNode *part_id = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, part_id, NULL, $5);
}
| PARTITION relation_factor ID INTNUM compress_option subpartition_list
{
  (void)($5);
  ParseNode *part_id = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, part_id, NULL, $6);
}
| PARTITION TABLESPACE tablespace compress_option subpartition_list
{
  (void)($2);
  (void)($4);
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, NULL, tablespace, $5);
}
| PARTITION relation_factor TABLESPACE tablespace compress_option subpartition_list
{
  (void)($3);
  (void)($5);
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, NULL, tablespace, $6);
}
| PARTITION ID INTNUM TABLESPACE tablespace compress_option subpartition_list
{
  (void)($4);
  (void)($6);
  ParseNode *part_id = NULL;
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $3);
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, NULL, part_id, tablespace, $7);
}
| PARTITION relation_factor ID INTNUM TABLESPACE tablespace compress_option subpartition_list
{
  (void)($5);
  (void)($7);
  ParseNode *part_id = NULL;
  ParseNode *tablespace = NULL;
  malloc_non_terminal_node(part_id, result->malloc_pool_, T_PART_ID, 1, $4);
  malloc_non_terminal_node(tablespace, result->malloc_pool_, T_TABLESPACE, 1, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, part_id, tablespace, $8);
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
PARTITION relation_factor VALUES LESS THAN range_partition_expr opt_part_id opt_physical_attributes_options opt_compress_option opt_subpartition_list
{
  (void)($9);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, $6, $7, $8, $10);
}
| PARTITION VALUES LESS THAN range_partition_expr opt_part_id opt_physical_attributes_options opt_compress_option opt_subpartition_list
{
  (void)($8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, $5, $6, $7, $9);
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
PARTITION relation_factor VALUES list_partition_expr opt_part_id opt_physical_attributes_options opt_compress_option opt_subpartition_list
{
  (void)($7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, $4, $5, $6, $8);
}
| PARTITION VALUES list_partition_expr opt_part_id opt_physical_attributes_options opt_compress_option opt_subpartition_list
{
  (void)($6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, NULL, $3, $4, $5, $7);
}
;

opt_subpartition_list:
subpartition_list
{
  $$ = $1;
}
|
{
  $$ = NULL;
}
;

subpartition_list:
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
;

opt_hash_subpartition_list:
'(' hash_subpartition_list ')'
{
  merge_nodes($$, result, T_HASH_SUBPARTITION_LIST, $2);
}
;

hash_subpartition_list:
hash_subpartition_element
{
  $$ = $1;
}
| hash_subpartition_list ',' hash_subpartition_element
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

hash_subpartition_element:
SUBPARTITION relation_factor opt_physical_attributes_options
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, NULL, NULL, $3, NULL);
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
SUBPARTITION relation_factor VALUES LESS THAN range_partition_expr opt_physical_attributes_options
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, $6, NULL, $7, NULL);
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
SUBPARTITION relation_factor VALUES list_partition_expr opt_physical_attributes_options
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 5, $2, $4, NULL, $5, NULL);
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
bit_expr {
  $$ = $1;
}
|
list_expr ',' bit_expr {
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

opt_physical_attributes_options:
physical_attributes_option_list
{
  $$ =$1;
}
| // EMPTY
{
  $$ = NULL;
};

physical_attributes_option_list:
physical_attributes_option
{
  $$ = $1;
}
| physical_attributes_option_list physical_attributes_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
};

physical_attributes_option:
PCTFREE opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_PCTFREE, 1, $3);
}
| PCTUSED INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PCTUSED, 1, $2);
}
| INITRANS INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INITRANS, 1, $2);
}
| MAXTRANS INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAXTRANS, 1, $2);
}
| STORAGE '(' storage_options_list ')'
{
  merge_nodes($$, result, T_STORAGE_OPTIONS, $3);
}
| TABLESPACE tablespace
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLESPACE, 1, $2);
}
;

opt_special_partition_list:
'('special_partition_list')'
{
  merge_nodes($$, result, T_PARTITION_LIST, $2);
}
special_partition_list:
special_partition_define
{
  $$ = $1;
}
|
special_partition_list ',' special_partition_define
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}

special_partition_define:
PARTITION opt_part_id
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 3, NULL, NULL, $2);
}
|
PARTITION relation_factor opt_part_id
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_ELEMENT, 3, $2, NULL, $3);
}
;

range_partition_expr:
'(' range_expr_list ')'
{
  merge_nodes($$, result, T_EXPR_LIST, $2);
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
literal
{
  $$ = $1;
}
| '+' literal
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_POS, 1, $2);
}
| '-' literal
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NEG, 1, $2);
}
| access_func_expr
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
;

/*tablegroup partition option*/
opt_tg_partition_option:
tg_hash_partition_option
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
| PARTITION BY HASH INTNUM tg_subpartition_option opt_partitions
{
  ParseNode *hash_func = NULL;
  make_name_node(hash_func, result->malloc_pool_, "partition_hash");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 3, $$, $6, $5);
}
;

tg_range_partition_option:
// PARTITION BY RANGE tg_subpartition_option opt_partitions opt_range_partition_list
// {
//   malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_PARTITION, 4, NULL, $6, $4, $5);
// }
// |
PARTITION BY RANGE COLUMNS INTNUM tg_subpartition_option opt_range_partition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_COLUMNS_PARTITION, 4, $5, $7, $6, NULL);
}
;

tg_list_partition_option:
// PARTITION BY BISON_LIST tg_subpartition_option opt_partitions opt_list_partition_list
// {
//   malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_PARTITION, 4, NULL, $6, $4, $5);
// }
// |
PARTITION BY LIST COLUMNS INTNUM tg_subpartition_option opt_list_partition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_COLUMNS_PARTITION, 4, $5, $7, $6, NULL);
}
;

tg_subpartition_option:
// SUBPARTITION BY RANGE SUBPARTITION TEMPLATE opt_range_subpartition_list
// {
//   malloc_non_terminal_node($$, result->malloc_pool_, T_RANGE_PARTITION, 4, NULL, $6, NULL, NULL);
// }
// |
SUBPARTITION BY RANGE COLUMNS INTNUM SUBPARTITION TEMPLATE opt_range_subpartition_list
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
// | SUBPARTITION BY KEY INTNUM opt_subpartitions
// {
//   ParseNode *hash_func = NULL;
//   make_name_node(hash_func, result->malloc_pool_, "partition_key_v2");
//   malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, hash_func, $4);
//   malloc_non_terminal_node($$, result->malloc_pool_, T_KEY_PARTITION, 3, $$, $5, NULL);
// }
// | SUBPARTITION BY BISON_LIST SUBPARTITION TEMPLATE opt_list_subpartition_list
// {
//   malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_PARTITION, 4, NULL, $6, NULL, NULL);
// }
| SUBPARTITION BY LIST COLUMNS INTNUM SUBPARTITION TEMPLATE opt_list_subpartition_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_COLUMNS_PARTITION, 4, $5, $8, NULL, NULL);
}
|
{
  $$ = NULL;
}
;

opt_alter_compress_option:
MOVE compress_option
{
  (void)($1);
  $$ = $2;
}
;

opt_compress_option:
compress_option
{
  $$ = $1;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

compress_option:
NOCOMPRESS
{
  ParseNode *param = NULL;
  malloc_terminal_node(param, result->malloc_pool_, T_INT);
  param->value_ = 11;
  malloc_non_terminal_node($$, result->malloc_pool_, T_STORE_FORMAT, 1, param);
}
| COMPRESS opt_compress_str
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_STORE_FORMAT, 1, $2);
}
;

opt_compress_str:
BASIC
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 12;
}
| FOR OLTP
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 13;
}
| FOR QUERY opt_compress_level
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  if (NULL == $3 || nodename_equal($3, "HIGH", 4)) {
    $$->value_ = 14;
  } else if (nodename_equal($3, "LOW", 3)) {
    $$->value_ = 16;
  }
}
| FOR ARCHIVE opt_compress_level
{
  (void) ($3);
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 15;
}
| /*EMPTY*/
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 12;
}
;

opt_compress_level:
LOW
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "LOW";
  $$->str_len_ = 3;
}
| HIGH
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "HIGH";
  $$->str_len_ = 4;
}
| /*EMPTY*/ { $$ = NULL;}
;
/*****************************************************************************
 *
 * create tablegroup
 *
 *****************************************************************************/
create_tablegroup_stmt:
CREATE TABLEGROUP relation_name opt_tablegroup_option_list opt_tg_partition_option
{
  ParseNode *tablegroup_options = NULL;
  merge_nodes(tablegroup_options, result, T_TABLEGROUP_OPTION_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLEGROUP, 4, NULL, $3, tablegroup_options, $5);
}
;

/*****************************************************************************
 *
 * drop tablegroup
 *
 *****************************************************************************/
drop_tablegroup_stmt:
DROP TABLEGROUP relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_TABLEGROUP, 2, NULL, $3);
}
;

/*****************************************************************************
 *
 * alter tablegroup
 *
 *****************************************************************************/
alter_tablegroup_stmt:
ALTER TABLEGROUP relation_name ADD table_list
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_TABLE_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLEGROUP, 2, $3, table_list);
}
| ALTER TABLEGROUP relation_name ADD TABLE table_list
{
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
| ALTER TABLEGROUP relation_name alter_partition_option
{
  ParseNode *partition_options = NULL;
  malloc_non_terminal_node(partition_options, result->malloc_pool_, T_ALTER_PARTITION_OPTION, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLEGROUP, 2, $3, partition_options);
}
| ALTER TABLEGROUP relation_name tg_modify_partition_info
{
  ParseNode *partition_options = NULL;
  ParseNode *modify_partitions = NULL;
  malloc_non_terminal_node(modify_partitions, result->malloc_pool_, T_ALTER_PARTITION_PARTITIONED, 1, $4);
  malloc_non_terminal_node(partition_options, result->malloc_pool_, T_ALTER_PARTITION_OPTION, 1, modify_partitions);
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
DEFAULT_TABLEGROUP opt_equal_mark relation_name
{
  (void)($2) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_TABLEGROUP);
  $$->str_value_ = $3->str_value_;
  $$->str_len_ = $3->str_len_;
}
| DEFAULT_TABLEGROUP opt_equal_mark NULLX
{
  (void)($2) ; /* make bison mute */
  (void)($3) ; /* make bison mute */
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
CREATE opt_replace VIEW view_name opt_column_alias_name_list opt_table_id AS view_subquery view_with_opt
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_VIEW, 7,
                           NULL,  /* obsoleted: opt_materialized */
                           $4,    /* view name */
                           $5,    /* column list */
                           $6,    /* table_id */
                           $8,    /* subquery */
                           $2,
                           $9     /* view opt */
						   );
  dup_expr_string($8, result, @8.first_column, @8.last_column);
}
;

view_subquery:
subquery
{
  $$ = $1;
}
| subquery fetch_next_clause
{
  $$ = $1;
  if ($$->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$->children_[PARSE_SELECT_FETCH] = $2;
  }
}
| subquery order_by
{
  $$ = $1;
  if ($1->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$->children_[PARSE_SELECT_ORDER] = $2;
  }
}
| subquery order_by fetch_next_clause
{
  $$ = $1;
  if ($$->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$->children_[PARSE_SELECT_ORDER] = $2;
    $$->children_[PARSE_SELECT_FETCH] = $3;
  }
}
;

view_with_opt:
WITH READ ONLY
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
| /* empty */
{ $$ = NULL; }
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
'(' column_list ')'
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
CREATE opt_index_keyname INDEX normal_relation_factor opt_index_using_algorithm ON relation_factor '(' sort_column_list ')'
opt_index_option_list opt_partition_option
{
  ParseNode *idx_columns = NULL;
  ParseNode *index_options = NULL;
  merge_nodes(idx_columns, result, T_INDEX_COLUMN_LIST, $9);
  merge_nodes(index_options, result, T_TABLE_OPTION_LIST, $11);
  $4->value_ = $2[0]; /* index prefix keyname */
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_INDEX, 7,
                           $4,                   /* index name */
                           $7,                   /* table name */
                           idx_columns,          /* index columns */
                           index_options,        /* index option(s) */
                           $5,                   /* index method */
                           $12,                  /* partition method*/
                           NULL);                  /* if not exists*/
};

opt_index_keyname:
UNIQUE { $$[0] = 1; }
| /*EMPTY*/ { $$[0] = 0; }
;

index_name:
relation_name {$$ = $1;}
;

opt_constraint_and_name:
/*empty*/
{
  $$ = NULL;
}
| constraint_and_name
{
  $$ = $1;
}
;

constraint_and_name:
CONSTRAINT constraint_name
{
  $$ = $2;
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
index_expr opt_asc_desc opt_column_id
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SORT_COLUMN_KEY, 4,
                           $1, /*index_expr*/
                           NULL, /*sort_column_key_length, unused in oracle mode*/
                           $2, /*asc or desc flag*/
                           $3); /*column_id*/
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

index_expr:
bit_expr
{
  if (NULL == $1) {
    yyerror(NULL, result, "index_expr is null\n");
    YYERROR;
  } else if (T_COLUMN_REF == $1->type_
      && NULL == $1->children_[0]
      && NULL == $1->children_[1]
      && NULL != $1->children_[2]
      && T_IDENT == $1->children_[2]->type_) {
    $$ = $1->children_[2];
  } else if (T_OBJ_ACCESS_REF == $1->type_
      && NULL != $1->children_[0]
      && T_IDENT == $1->children_[0]->type_
      && NULL == $1->children_[1]) {
    $$ = $1->children_[0];
  } else {
    $$ = $1;
    dup_string($$, result, @1.first_column, @1.last_column);
  }
}
;

opt_column_id:
/* EMPTY */
{ $$ = 0; }
| ID INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ID, 1, $2);
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
| WITH ROWID
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
| MAX_USED_PART_ID opt_equal_mark INTNUM
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_USED_PART_ID, 1, $3);
}
| physical_attributes_option
{
  $$ = $1;
}
| REVERSE
{
  malloc_terminal_node($$, result->malloc_pool_, T_REVERSE);
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

/*****************************************************************************
 *
 *	drop table grammar
 *
 *****************************************************************************/

drop_table_stmt:
DROP TABLE relation_factor opt_cascade_constraints opt_purge
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_TABLE, 3,
      $4,     /* cascade_constraints */
      $5,     /* purge */
      $3);    /* table */
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
DROP opt_materialized VIEW relation_factor opt_cascade_constraints
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_VIEW, 3,
      $2,     /* opt_materialized */
      $5,     /* cascade_constraints */
      $4);    /* view name */
}
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

opt_cascade_constraints:
/*EMPTY*/ { $$ = NULL; }
| CASCADE CONSTRAINTS
{ malloc_terminal_node($$, result->malloc_pool_, T_CASCADE_CONSTRAINTS); }
;

opt_purge:
/*EMPTY*/ { $$ = NULL; }
| PURGE
{ malloc_terminal_node($$, result->malloc_pool_, T_PURGE); }
;

/*****************************************************************************
 *
 *	drop index grammar
 *
 *****************************************************************************/

drop_index_stmt:
DROP INDEX relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_INDEX, 1, $3);
}
|
DROP INDEX relation_name '.' relation_name
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
insert_with_opt_hint single_table_insert
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INSERT, 4,
                           $2, /*single or multi table insert node*/
                           $1->children_[0], /* is replacement */
                           $1->children_[1], /* hint */
                           NULL /*ignore node*/);
}
| insert_with_opt_hint multi_table_insert
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MULTI_INSERT, 4,
                           $2, /*multi table insert node*/
                           $1->children_[0], /* is replacement */
                           $1->children_[1], /* hint */
                           NULL /*ignore node*/);
}
;

opt_simple_expression:
/*empty*/
{
  $$ = NULL;
}
| '(' simple_expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ERR_LOG_SIMPLE_EXPR, 1, $2 );
}
into_err_log_caluse:
/*empty*/
{
  $$ = NULL;
}
| INTO relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INTO_ERR_LOG_TABLE, 1, $2 /*relation_factor*/);
}
;
reject_limit:
{
 $$ = NULL;
}
| REJECT LIMIT INTNUM
{
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = $3->value_;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ERR_LOG_LIMIT, 1, $$ /*reject limit*/);
}
| REJECT LIMIT UNLIMITED
{
  malloc_terminal_node($$, result->malloc_pool_, T_SIZE_UNLIMITED);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ERR_LOG_LIMIT, 1, $$ /*reject limit unlimited*/);
}
;

opt_error_logging_clause:
/*empty*/
{
  $$ = NULL;
}
| LOG ERRORS into_err_log_caluse opt_simple_expression reject_limit
{
    malloc_non_terminal_node($$, result->malloc_pool_, T_ERR_LOG_CALUSE, 3, $3, /*into_err_log_caluse*/
                              $4, /*opt_simple_express*/
                              $5 /*reject_limit*/);
}
;

single_table_insert:
INTO insert_table_clause opt_nologging '(' column_list ')' values_clause opt_returning opt_error_logging_clause
{
  (void)($3);
  ParseNode *into_clause = NULL;
  ParseNode *column_list = NULL;
  merge_nodes(column_list, result, T_COLUMN_LIST, $5);
  malloc_non_terminal_node(into_clause, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 2, $2, column_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_TABLE_INSERT, 4,
                           into_clause, /*insert_into_clause*/
                           $7, /*values_clause*/
                           $8, /*returing_node*/
                           $9 /*opt_error_logging_clause*/);
}
| INTO insert_table_clause opt_nologging '(' ')' values_clause opt_returning opt_error_logging_clause
{
  (void)($3);
  ParseNode *into_clause = NULL;
  malloc_non_terminal_node(into_clause, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 2, $2, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_TABLE_INSERT, 4,
                           into_clause, /*insert_into_clause*/
                           $6, /*values_clause*/
                           $7, /*returing_node*/
                           $8 /*opt_error_logging_clause*/);
}
| INTO insert_table_clause opt_nologging values_clause opt_returning opt_error_logging_clause
{
  (void)($3);
  ParseNode *into_clause = NULL;
  malloc_non_terminal_node(into_clause, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 2, $2, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_TABLE_INSERT, 4,
                           into_clause, /*insert_into_clause*/
                           $4, /*values_clause*/
                           $5, /*returing_node*/
                           $6 /*opt_error_logging_clause*/);
}
;

multi_table_insert:
ALL insert_table_clause_list subquery opt_order_by opt_fetch_next
{
  ParseNode *insert_table_list = NULL;
  merge_nodes(insert_table_list, result, T_INSERT_TABLE_LIST, $2);
  if ($3->children_[PARSE_SELECT_FETCH] != NULL && ($4 != NULL || $5 != NULL)) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $3->children_[PARSE_SELECT_ORDER] = $4;
    $3->children_[PARSE_SELECT_FETCH] = $5;
    malloc_non_terminal_node($$, result->malloc_pool_, T_MULTI_TABLE_INSERT, 2,
                             insert_table_list, /*insert table list*/
                             $3 /* subquery*/
                             );
  }
}
| conditional_insert_clause subquery opt_order_by opt_fetch_next
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL && ($3 != NULL || $4 != NULL)) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    malloc_non_terminal_node($$, result->malloc_pool_, T_MULTI_TABLE_INSERT, 2,
                             $1, /*insert table list*/
                             $2 /* subquery*/
                             );
  }
}
;

insert_table_clause_list:
insert_table_clause_list insert_single_table_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| insert_single_table_clause
{
  $$ = $1;
}
;

insert_single_table_clause:
INTO dml_table_name /*opt_error_logging_clause*/ %prec LOWER_PARENS
{
  ParseNode *into_clause = NULL;
  ParseNode *table_clause = NULL;
  if (T_ORG != $2->type_) {
    result->extra_errno_ = OB_PARSER_ERR_UNEXPECTED;
    yyerror(NULL, result, "dml table name is invalid\n");
    YYERROR;
  } else {
    malloc_non_terminal_node(table_clause, result->malloc_pool_, T_ALIAS, 5,
                             $2->children_[0],
                             NULL,
                             $2->children_[1],
                             $2->children_[2],
                             $2->children_[3]);
    malloc_non_terminal_node(into_clause, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 2, table_clause, NULL);
    malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_INSERT, 2,
                            into_clause, /*insert_into_clause*/
                            NULL /*values_clause*/);
  }
}
| INTO dml_table_name '(' column_list ')' /*opt_error_logging_clause*/
{
  ParseNode *into_clause = NULL;
  ParseNode *column_list = NULL;
  ParseNode *table_clause = NULL;
  if (T_ORG != $2->type_) {
    result->extra_errno_ = OB_PARSER_ERR_UNEXPECTED;
    yyerror(NULL, result, "dml table name is invalid\n");
    YYERROR;
  } else {
    malloc_non_terminal_node(table_clause, result->malloc_pool_, T_ALIAS, 5,
                             $2->children_[0],
                             NULL,
                             $2->children_[1],
                             $2->children_[2],
                             $2->children_[3]);
    merge_nodes(column_list, result, T_COLUMN_LIST, $4);
    malloc_non_terminal_node(into_clause, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 2, table_clause, column_list);
    malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_INSERT, 2,
                            into_clause, /*insert_into_clause*/
                            NULL /*values_clause*/);
  }
}
| INTO dml_table_name VALUES '(' insert_vals ')' /*opt_error_logging_clause*/
{
  ParseNode *val_vector = NULL;
  ParseNode *table_clause = NULL;
  ParseNode *into_clause = NULL;
  if (T_ORG != $2->type_) {
    result->extra_errno_ = OB_PARSER_ERR_UNEXPECTED;
    yyerror(NULL, result, "dml table name is invalid\n");
    YYERROR;
  } else {
    malloc_non_terminal_node(table_clause, result->malloc_pool_, T_ALIAS, 5,
                             $2->children_[0],
                             NULL,
                             $2->children_[1],
                             $2->children_[2],
                             $2->children_[3]);
    merge_nodes(val_vector, result, T_VALUE_VECTOR, $5);
    malloc_non_terminal_node(into_clause, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 2, table_clause, NULL);
    malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_INSERT, 2,
                           into_clause, /*insert_into_clause*/
                           val_vector /*values_clause*/);
  }
}
| INTO dml_table_name '(' column_list ')' VALUES '(' insert_vals ')' /*opt_error_logging_clause*/
{
  ParseNode *into_clause = NULL;
  ParseNode *column_list = NULL;
  ParseNode *val_vector = NULL;
  ParseNode *table_clause = NULL;
  if (T_ORG != $2->type_) {
    result->extra_errno_ = OB_PARSER_ERR_UNEXPECTED;
    yyerror(NULL, result, "dml table name is invalid\n");
    YYERROR;
  } else {
    malloc_non_terminal_node(table_clause, result->malloc_pool_, T_ALIAS, 5,
                             $2->children_[0],
                             NULL,
                             $2->children_[1],
                             $2->children_[2],
                             $2->children_[3]);
    merge_nodes(val_vector, result, T_VALUE_VECTOR, $8);
    merge_nodes(column_list, result, T_COLUMN_LIST, $4);
    malloc_non_terminal_node(into_clause, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 2, table_clause, column_list);
    malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_INSERT, 2,
                           into_clause, /*insert_into_clause*/
                           val_vector /*values_clause*/);
  }
}
;

conditional_insert_clause:
opt_all_or_first condition_insert_clause_list opt_default_insert_clause
{
  ParseNode *condition_insert_list = NULL;
  merge_nodes(condition_insert_list, result, T_CONDITIION_INSERT_LIST, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_MULTI_CONDITION_INSERT, 3,
                           $1, /*all or first*/
                           condition_insert_list, /*condition insert list*/
                           $3 /*default insert clause*/
                           );
}
;

opt_all_or_first:
/*empty*/
{
  malloc_terminal_node($$, result->malloc_pool_, T_ALL);
}
| ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_ALL);
}
| FIRST
{
  malloc_terminal_node($$, result->malloc_pool_, T_FIRST);
}
;

condition_insert_clause_list:
condition_insert_clause_list condition_insert_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| condition_insert_clause
{
  $$ = $1;
}
;

condition_insert_clause:
WHEN expr THEN insert_table_clause_list
{
  ParseNode *insert_table_list = NULL;
  merge_nodes(insert_table_list, result, T_INSERT_TABLE_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONDITION_INSERT, 2,
                           $2, /*condition*/
                           insert_table_list /*insert clause*/
                           );
}
;

opt_default_insert_clause:
/*EMPTY*/
{
  $$ = NULL;
}
| ELSE insert_table_clause_list
{
  merge_nodes($$, result, T_INSERT_TABLE_LIST, $2);
}
;

opt_nologging:
NOLOGGING
{
  $$ = NULL;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

values_clause:
VALUES insert_vals_list
{
  merge_nodes($$, result, T_VALUE_LIST, $2);
}
| VALUES obj_access_ref_normal
{
  merge_nodes($$, result, T_VALUE_LIST, $2);
}
| subquery opt_order_by opt_fetch_next
{
  $$ = $1;
  $$->children_[PARSE_SELECT_ORDER] = $2;
  if ($$->children_[PARSE_SELECT_FETCH] != NULL && ($2 != NULL || $3 != NULL)) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else if ($3 != NULL) {
    $$->children_[PARSE_SELECT_FETCH] = $3;
  }
}
;

opt_into_clause:
    /*EMPTY*/ { $$ = NULL; }
  | into_clause { $$ = $1; }
;

opt_returning:
RETURNING returning_exprs opt_into_clause
{
  ParseNode *returning_exprs_list = NULL;
  merge_nodes(returning_exprs_list, result, T_PROJECT_LIST, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RETURNING, 2, returning_exprs_list, $3);
}
| RETURN returning_exprs opt_into_clause
{
  ParseNode *returning_exprs_list = NULL;
  merge_nodes(returning_exprs_list, result, T_PROJECT_LIST, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RETURNING, 2, returning_exprs_list, $3);
}
|/* EMPTY */
{
  $$ = NULL;
}
;
returning_exprs:
returning_exprs ',' projection
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| projection
{
  $$ = $1;
}
;

opt_when:
/* EMPTY */
{ $$ = NULL; }

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

opt_insert_columns:
'(' column_list ')'
{
  merge_nodes($$, result, T_COLUMN_LIST, $2);
}
| /* EMPTY */
{
  malloc_terminal_node($$, result->malloc_pool_, T_EMPTY);
}
;

column_list:
column_definition_ref { $$ = $1; }
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
;
expr_or_default:
bit_expr { $$ = $1;}
| DEFAULT
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT);
}
;

/*****************************************************************************
 *
 *	merge grammar
 *
 *****************************************************************************/
merge_with_opt_hint:
MERGE {$$ = NULL;}
| MERGE_HINT_BEGIN hint_list_with_end
{$$ = $2;}
;

merge_stmt:
merge_with_opt_hint INTO source_relation_factor USING source_relation_factor opt_alias
ON '(' expr ')'
merge_update_clause merge_insert_clause
{
  ParseNode *target_node = NULL;
  ParseNode *source_node = NULL;
  malloc_non_terminal_node(target_node, result->malloc_pool_, T_ALIAS, 4, $3, NULL, NULL, NULL);
  malloc_non_terminal_node(source_node, result->malloc_pool_, T_ALIAS, 4, $5, $6, NULL, NULL);

  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE, 6,
                           target_node,   /* target relation */
                           source_node,   /* source relation */
                           $9, /*match condition*/
                           $11, /*merge update clause*/
                           $12, /*merge insert clause*/
                           $1 /*hint*/
                           );
}
| merge_with_opt_hint INTO source_relation_factor relation_name USING source_relation_factor opt_alias
ON '(' expr ')'
merge_update_clause merge_insert_clause
{
  ParseNode *target_node = NULL;
  ParseNode *source_node = NULL;
  malloc_non_terminal_node(target_node, result->malloc_pool_, T_ALIAS, 4, $3, $4, NULL, NULL);
  malloc_non_terminal_node(source_node, result->malloc_pool_, T_ALIAS, 4, $6, $7, NULL, NULL);

  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE, 6,
                           target_node,   /* target relation */
                           source_node,   /* source relation */
                           $10, /*match condition*/
                           $12, /*merge update clause*/
                           $13, /*merge insert clause*/
                           $1 /*hint*/
                           );
}
| merge_with_opt_hint INTO source_relation_factor USING source_relation_factor opt_alias
ON '(' expr ')'
merge_insert_clause merge_update_clause
{
  ParseNode *target_node = NULL;
  ParseNode *source_node = NULL;
  malloc_non_terminal_node(target_node, result->malloc_pool_, T_ALIAS, 4, $3, NULL, NULL, NULL);
  malloc_non_terminal_node(source_node, result->malloc_pool_, T_ALIAS, 4, $5, $6, NULL, NULL);

  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE, 6,
                           target_node,   /* target relation */
                           source_node,   /* source relation */
                           $9, /*match condition*/
                           $12, /*merge update clause*/
                           $11, /*merge insert clause*/
                           $1 /*hint*/
                           );
}
| merge_with_opt_hint INTO source_relation_factor relation_name USING source_relation_factor opt_alias
ON '(' expr ')'
merge_insert_clause merge_update_clause
{
  ParseNode *target_node = NULL;
  ParseNode *source_node = NULL;
  malloc_non_terminal_node(target_node, result->malloc_pool_, T_ALIAS, 4, $3, $4, NULL, NULL);
  malloc_non_terminal_node(source_node, result->malloc_pool_, T_ALIAS, 4, $6, $7, NULL, NULL);

  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE, 6,
                           target_node,   /* target relation */
                           source_node,   /* source relation */
                           $10, /*match condition*/
                           $13, /*merge update clause*/
                           $12, /*merge insert clause*/
                           $1 /*hint*/
                           );
}
| merge_with_opt_hint INTO source_relation_factor USING source_relation_factor opt_alias
ON '(' expr ')'
merge_insert_clause
{
  ParseNode *target_node = NULL;
  ParseNode *source_node = NULL;
  malloc_non_terminal_node(target_node, result->malloc_pool_, T_ALIAS, 4, $3, NULL, NULL, NULL);
  malloc_non_terminal_node(source_node, result->malloc_pool_, T_ALIAS, 4, $5, $6, NULL, NULL);

  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE, 6,
                           target_node,   /* target relation */
                           source_node,   /* source relation */
                           $9, /*match condition*/
                           NULL, /*merge update clause*/
                           $11, /*merge insert clause*/
                           $1 /*hint*/
                           );
}
| merge_with_opt_hint INTO source_relation_factor relation_name USING source_relation_factor opt_alias
ON '(' expr ')'
merge_insert_clause
{
  ParseNode *target_node = NULL;
  ParseNode *source_node = NULL;
  malloc_non_terminal_node(target_node, result->malloc_pool_, T_ALIAS, 4, $3, $4, NULL, NULL);
  malloc_non_terminal_node(source_node, result->malloc_pool_, T_ALIAS, 4, $6, $7, NULL, NULL);

  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE, 6,
                           target_node,   /* target relation */
                           source_node,   /* source relation */
                           $10, /*match condition*/
                           NULL, /*merge update clause*/
                           $12, /*merge insert clause*/
                           $1 /*hint*/
                           );
}
| merge_with_opt_hint INTO source_relation_factor USING source_relation_factor opt_alias
ON '(' expr ')'
merge_update_clause
{
  ParseNode *target_node = NULL;
  ParseNode *source_node = NULL;
  malloc_non_terminal_node(target_node, result->malloc_pool_, T_ALIAS, 4, $3, NULL, NULL, NULL);
  malloc_non_terminal_node(source_node, result->malloc_pool_, T_ALIAS, 4, $5, $6, NULL, NULL);

  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE, 6,
                           target_node,   /* target relation */
                           source_node,   /* source relation */
                           $9, /*match condition*/
                           $11, /*merge update clause*/
                           NULL, /*merge insert clause*/
                           $1 /*hint*/
                           );
}
| merge_with_opt_hint INTO source_relation_factor relation_name USING source_relation_factor opt_alias
ON '(' expr ')'
merge_update_clause
{
  ParseNode *target_node = NULL;
  ParseNode *source_node = NULL;
  malloc_non_terminal_node(target_node, result->malloc_pool_, T_ALIAS, 4, $3, $4, NULL, NULL);
  malloc_non_terminal_node(source_node, result->malloc_pool_, T_ALIAS, 4, $6, $7, NULL, NULL);

  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE, 6,
                           target_node,   /* target relation */
                           source_node,   /* source relation */
                           $10, /*match condition*/
                           $12, /*merge update clause*/
                           NULL, /*merge insert clause*/
                           $1 /*hint*/
                           );
}
;

merge_update_clause:
WHEN MATCHED THEN UPDATE SET update_asgn_list opt_where opt_merge_update_delete
{
  ParseNode *assign_list = NULL;
  merge_nodes(assign_list, result, T_ASSIGN_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_UPDATE, 3,
                           assign_list, /* assign list */
                           $7, /* where condition */
                           $8 /* update delete condition */
                           );
}
;

opt_merge_update_delete:
/* EMPTY */
{$$ = NULL;}
| DELETE WHERE expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WHERE_CLAUSE, 2, $3, NULL);
}
;

merge_insert_clause:
WHEN NOT MATCHED THEN INSERT opt_insert_columns VALUES '(' insert_vals ')' opt_where
{
  ParseNode *val_vector = NULL;
  ParseNode *val_list = NULL;
  merge_nodes(val_vector, result, T_VALUE_VECTOR, $9);
  merge_nodes(val_list, result, T_VALUE_LIST, val_vector);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INSERT, 3,
                           $6,           /* column list */
                           val_list,     /* value list */
                           $11            /* opt where clause */
                           );

}
;

opt_alias:
/* EMPTY */
{$$ = NULL;}
| relation_name { $$=$1; }
;

source_relation_factor:
relation_factor
{
  $$ = $1
}
| select_with_parens
{
  $$ = $1
};

/*****************************************************************************
 *
 *	select grammar
 *
 *****************************************************************************/
select_stmt:
subquery opt_fetch_next
{
  $$ =$1;
  if ($$->children_[PARSE_SELECT_FETCH] != NULL && $2 != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else if ($$->children_[PARSE_SELECT_FETCH] == NULL) {
    $$->children_[PARSE_SELECT_FETCH] = $2;
  }
}
| subquery for_update
{
  $$ =$1;
  $$->children_[PARSE_SELECT_FOR_UPD] = $2;
}
| subquery fetch_next for_update
{
  $$ =$1;
  if ($$->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$->children_[PARSE_SELECT_FETCH] = $2;
    $$->children_[PARSE_SELECT_FOR_UPD] = $3;
  }
}
| subquery order_by opt_fetch_next
{
  $$ = $1;
  if ($$->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$->children_[PARSE_SELECT_ORDER] = $2;
    $$->children_[PARSE_SELECT_FETCH] = $3;
  }
}
| subquery order_by opt_fetch_next for_update
{
  $$ = $1;
  if ($$->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur with FOR_UPDATE\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$->children_[PARSE_SELECT_ORDER] = $2;
    $$->children_[PARSE_SELECT_FETCH] = $3;
    $$->children_[PARSE_SELECT_FOR_UPD] = $4;
  }
}
| subquery for_update order_by
{
  $$ = $1;
  if ($$->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur with FOR_UPDATE\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$->children_[PARSE_SELECT_ORDER] = $3;
    $$->children_[PARSE_SELECT_FOR_UPD] = $2;
  }
}
;

subquery:
select_no_parens %prec LOWER_PARENS
{
  $$ = $1;
  $$->children_[PARSE_SELECT_WHEN] = NULL;
}
| select_with_parens %prec LOWER_PARENS
{
  if ($1->children_[PARSE_SELECT_ORDER] != NULL && $1->children_[PARSE_SELECT_FETCH] == NULL) {
    yyerror(NULL, result, "only order by clause can't occur subquery\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$ = $1;
  }
}
| with_select %prec LOWER_PARENS
{
  $$ = $1;
}
;

select_with_parens:
'(' select_no_parens ')'
{
  $$ = $2;
}
| '(' select_with_parens ')'
{
  $$ = $2;
}
| '(' with_select ')'
{
  $$ = $2;
}
| '(' select_no_parens fetch_next_clause ')'
{
  $$ = $2;
  $$->children_[PARSE_SELECT_FETCH] = $3;
}
| '(' select_no_parens order_by ')'
{
  $$ = $2;
  $$->children_[PARSE_SELECT_ORDER] = $3;
}
| '(' select_no_parens order_by fetch_next_clause ')'
{
  $$ = $2;
  $$->children_[PARSE_SELECT_ORDER] = $3;
  $$->children_[PARSE_SELECT_FETCH] = $4;
}
| '(' with_select fetch_next_clause ')'
{
  $$ = $2;
  $$->children_[PARSE_SELECT_FETCH] = $3;
}
| '(' with_select order_by fetch_next_clause ')'
{
  $$ = $2;
  $$->children_[PARSE_SELECT_ORDER] = $3;
  $$->children_[PARSE_SELECT_FETCH] = $4;
}
| '(' with_select order_by ')'
{
  $$ = $2;
  $$->children_[PARSE_SELECT_ORDER] = $3;
}
;

select_no_parens:
select_clause
{
  $$ = $1;
}
| select_clause_set
{
  $$ = $1;
}
;

select_clause:
simple_select
{
  $$ = $1;
  }
| select_with_hierarchical_query
{
  $$ = $1;
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
  /*special deal with for fetch clause, please don't change it*/
  if ($3->children_[PARSE_SELECT_FETCH_TEMP] != NULL) {
    select_node->children_[PARSE_SELECT_FETCH] = $3->children_[PARSE_SELECT_FETCH_TEMP];
    $3->children_[PARSE_SELECT_FETCH_TEMP] = NULL;
  }
  $$ = select_node;
}
| select_clause_set_left set_type select_clause_set_right {
  ParseNode *select_node = NULL;
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_SET] = $2;
  select_node->children_[PARSE_SELECT_FORMER] = $1;
  select_node->children_[PARSE_SELECT_LATER] = $3;
  /*special deal with for fetch clause, please don't change it*/
  if ($1->children_[PARSE_SELECT_FETCH_TEMP] != NULL) {
    yyerror(NULL, result, "fetch next clause occur incorrect place in set stmt\n");
    YYABORT_PARSE_SQL_ERROR;
  } else if ($3->children_[PARSE_SELECT_FETCH_TEMP] != NULL) {
    select_node->children_[PARSE_SELECT_FETCH] = $3->children_[PARSE_SELECT_FETCH_TEMP];
    $3->children_[PARSE_SELECT_FETCH_TEMP] = NULL;
  }
  $$ = select_node;
}
;

select_clause_set_right:
simple_select
{
  $$ = $1;
  /*special deal with for fetch clause, please don't change it*/
  if ($$->children_[PARSE_SELECT_FETCH] != NULL) {
    $$->children_[PARSE_SELECT_FETCH_TEMP] = $$->children_[PARSE_SELECT_FETCH];
    $$->children_[PARSE_SELECT_FETCH] = NULL;
  }
}
| select_with_hierarchical_query
{
  $$ = $1;
  /*special deal with for fetch clause, please don't change it*/
  if ($$->children_[PARSE_SELECT_FETCH] != NULL) {
    $$->children_[PARSE_SELECT_FETCH_TEMP] = $$->children_[PARSE_SELECT_FETCH];
    $$->children_[PARSE_SELECT_FETCH] = NULL;
  }
}
| select_with_parens
{
  if ($1->children_[PARSE_SELECT_ORDER] != NULL && $1->children_[PARSE_SELECT_FETCH] == NULL) {
    yyerror(NULL, result, "only order by clause can't occur subquery\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$ = $1;
  }
}
;

select_clause_set_left:
select_clause_set_right
{
  $$ = $1;
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
      // @1.last_column: pos of '+' (12)
      // @2.first_column: start pos of 'no_rewrite' (10)
      // @2.last_column: end pos of '*/' (24)
      $$->token_off_ = @2.last_column - 1;
      $$->token_len_ = @2.last_column - @1.last_column + 1;
      result->stop_add_comment_ = true;
    }
  }
#endif
}
;

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
opt_where opt_groupby_having
{
  ParseNode *project_list = NULL;
  ParseNode *from_list = NULL;
  ParseNode *groupby_clause = NULL;
  ParseNode *having_list = NULL;
  merge_nodes(project_list, result, T_PROJECT_LIST, $3);
  if (T_ALIAS == $6->type_ && -1 == $6->value_) {
    // 单表无别名 from dual
    from_list = NULL;
  } else {
    merge_nodes(from_list, result, T_FROM_LIST, $6);
  }

  if (NULL != $8) {
    groupby_clause = $8->children_[0];
    having_list = $8->children_[1];
  }

  ParseNode *select_node = NULL;
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_DISTINCT] = $2;
  select_node->children_[PARSE_SELECT_SELECT] = project_list;
  select_node->children_[PARSE_SELECT_FROM] = from_list;
  select_node->children_[PARSE_SELECT_WHERE] = $7;
  select_node->children_[PARSE_SELECT_GROUP] = groupby_clause;
  select_node->children_[PARSE_SELECT_HAVING] = having_list;
  select_node->children_[PARSE_SELECT_HINTS] = $1;
  select_node->children_[PARSE_SELECT_INTO] = $4;

  if (NULL != from_list && from_list->children_[from_list->num_child_ - 1] != NULL) {
    ParseNode *child_node = from_list->children_[from_list->num_child_ - 1];
    if (child_node->num_child_ == 8 && child_node->children_[7] != NULL) {
      if ($7 != NULL || groupby_clause != NULL || having_list != NULL) {
        yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
        YYABORT_PARSE_SQL_ERROR;
      } else {
        select_node->children_[PARSE_SELECT_FETCH] = child_node->children_[7];
        child_node->children_[7] = NULL;
      }
    }
  }

  $$ = select_node;

  setup_token_pos_info(from_list, @5.first_column - 1, 4);
  setup_token_pos_info(select_node, @1.first_column - 1, 6);
}
;

select_with_hierarchical_query:
select_with_opt_hint opt_query_expression_option_list select_expr_list into_opt
FROM from_list
opt_where start_with connect_by
opt_groupby_having
{
  ParseNode *project_list = NULL;
  ParseNode *from_list = NULL;
  ParseNode *groupby_clause = NULL;
  ParseNode *having_list = NULL;
  merge_nodes(project_list, result, T_PROJECT_LIST, $3);
  merge_nodes(from_list, result, T_FROM_LIST, $6);

  if (NULL != $10) {
    groupby_clause = $10->children_[0];
    having_list = $10->children_[1];
  }

  ParseNode *select_node = NULL;
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_DISTINCT] = $2;
  select_node->children_[PARSE_SELECT_SELECT] = project_list;
  select_node->children_[PARSE_SELECT_FROM] = from_list;
  select_node->children_[PARSE_SELECT_WHERE] = $7;
  select_node->children_[PARSE_SELECT_GROUP] = groupby_clause;
  select_node->children_[PARSE_SELECT_HAVING] = having_list;
  select_node->children_[PARSE_SELECT_HINTS] = $1;
  select_node->children_[PARSE_SELECT_INTO] = $4;
  select_node->children_[PARSE_SELECT_DYNAMIC_SW_CBY] = $8;
  select_node->children_[PARSE_SELECT_DYNAMIC_CBY_SW] = $9;
  $$ = select_node;

}
|
select_with_opt_hint opt_query_expression_option_list select_expr_list into_opt
FROM from_list
opt_where connect_by opt_start_with
opt_groupby_having
{
  ParseNode *project_list = NULL;
  ParseNode *from_list = NULL;
  ParseNode *groupby_clause = NULL;
  ParseNode *having_list = NULL;
  merge_nodes(project_list, result, T_PROJECT_LIST, $3);
  merge_nodes(from_list, result, T_FROM_LIST, $6);

  if (NULL != $10) {
    groupby_clause = $10->children_[0];
    having_list = $10->children_[1];
  }

  ParseNode *select_node = NULL;
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_DISTINCT] = $2;
  select_node->children_[PARSE_SELECT_SELECT] = project_list;
  select_node->children_[PARSE_SELECT_FROM] = from_list;
  select_node->children_[PARSE_SELECT_WHERE] = $7;
  select_node->children_[PARSE_SELECT_GROUP] = groupby_clause;
  select_node->children_[PARSE_SELECT_HAVING] = having_list;
  select_node->children_[PARSE_SELECT_HINTS] = $1;
  select_node->children_[PARSE_SELECT_INTO] = $4;
  select_node->children_[PARSE_SELECT_DYNAMIC_SW_CBY] = $8;
  select_node->children_[PARSE_SELECT_DYNAMIC_CBY_SW] = $9;
  $$ = select_node;

}

;

opt_start_with:
/* EMPTY */
{ $$ = NULL; }
| start_with
{ $$ = $1; }
;

start_with:
START WITH expr
{
  $$ = $3;
}
;

opt_fetch_next:
 /* EMPTY */
 { $$ = NULL; }
 | fetch_next_clause
 {
   $$ = $1;
 }
 ;

fetch_next_clause:
OFFSET bit_expr ROW fetch_next
{
  if ($2 != NULL) {
    $4->children_[0] = $2;
  }
  $$ = $4;
}
| OFFSET bit_expr ROWS fetch_next
{
  if ($2 != NULL) {
    $4->children_[0] = $2;
  }
  $$ = $4;
}
| fetch_next {
  $$ = $1;
}
| OFFSET bit_expr ROW %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FETCH_CLAUSE, 3, $2, NULL, NULL);
}
| OFFSET bit_expr ROWS %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FETCH_CLAUSE, 3, $2, NULL, NULL);
}
;

fetch_next:
fetch_next_count
{
  $$ = $1;
}
| fetch_next_percent
{
  $$ = $1;
}
;

fetch_next_count:
fetch_next_expr ONLY
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FETCH_CLAUSE, 3, NULL, $1, NULL);
}
| fetch_next_expr WITH TIES
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FETCH_TIES_CLAUSE, 3, NULL, $1, NULL);
}
;

fetch_next_percent:
fetch_next_percent_expr ONLY
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FETCH_CLAUSE, 3, NULL, NULL, $1);
}
| fetch_next_percent_expr WITH TIES
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FETCH_TIES_CLAUSE, 3, NULL, NULL, $1);
}
;

fetch_next_expr:
FETCH NEXT ROW
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_INT);
  $$->value_ = 1;
}
| FETCH NEXT bit_expr ROW
{
  $$ = $3;
}
| FETCH NEXT ROWS
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_INT);
  $$->value_ = 1;
}
| FETCH NEXT bit_expr ROWS
{
  $$ = $3;
}
| FETCH FIRST ROW
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_INT);
  $$->value_ = 1;
}
| FETCH FIRST bit_expr ROW
{
  $$ = $3;
}
| FETCH FIRST ROWS
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_INT);
  $$->value_ = 1;
}
| FETCH FIRST bit_expr ROWS
{
  $$ = $3;
}
;

fetch_next_percent_expr:
FETCH NEXT bit_expr PERCENT ROW
{
  $$ = $3;
}
| FETCH NEXT bit_expr PERCENT ROWS
{
  $$ = $3;
}
| FETCH FIRST bit_expr PERCENT ROW
{
  $$ = $3;
}
| FETCH FIRST bit_expr PERCENT ROWS
{
  $$ = $3;
}
;

connect_by:
CONNECT BY expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONNECT_BY_CLAUSE, 2, NULL, $3);
}
|
CONNECT BY NOCYCLE expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_NOCYCLE);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONNECT_BY_CLAUSE, 2, default_type, $4);
}
;

set_type_union:
UNION     { $$[0] = T_SET_UNION; }
;

set_type_other:
INTERSECT { $$[0] = T_SET_INTERSECT; }
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
;

opt_where:
/* EMPTY */
{$$ = NULL;}
| WHERE expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WHERE_CLAUSE, 2, $2, NULL);
}
| WHERE HINT_VALUE expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WHERE_CLAUSE, 2, $3, $2);
  setup_token_pos_info($$, @1.first_column - 1, 5);
}
;

opt_where_extension:
opt_where
{
  $$ = $1;
}
| WHERE CURRENT OF obj_access_ref
{
  ParseNode *current_of = NULL;
  malloc_non_terminal_node(current_of, result->malloc_pool_, T_SP_EXPLICIT_CURSOR_ATTR, 1, $4);
  current_of->value_ = T_SP_CURSOR_ROWID;
  malloc_non_terminal_node($$, result->malloc_pool_, T_WHERE_CLAUSE, 2, current_of, NULL);
}
;

into_clause:
  INTO into_var_list
  {
    ParseNode *vars_list = NULL;
    merge_nodes(vars_list, result, T_INTO_VARS_LIST, $2);
    malloc_non_terminal_node($$, result->malloc_pool_, T_INTO_VARIABLES, 1, vars_list);
  }
| BULK COLLECT INTO into_var_list
  {
    ParseNode *vars_list = NULL;
    merge_nodes(vars_list, result, T_INTO_VARS_LIST, $4);
    malloc_non_terminal_node($$, result->malloc_pool_, T_INTO_VARIABLES, 1, vars_list);
    $$->value_ = 1; //bulk
  }
;

into_opt:
INTO OUTFILE STRING_VALUE opt_charset field_opt line_opt
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INTO_OUTFILE, 4, $3, $4, $5, $6);
}
| INTO DUMPFILE STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INTO_DUMPFILE, 1, $3);
}
| into_clause { $$ = $1; }
|
{
  $$ = NULL;
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
obj_access_ref_normal
{
  $$ = $1;
}
|
QUESTIONMARK
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
relation_name
{
  $$ = $1;
}
| name_list relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| name_list ',' relation_name
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
| USE_JIT
{
  malloc_terminal_node($$, result->malloc_pool_, T_USE_JIT);
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
| DOP '(' INTNUM ',' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DOP, 2, $3, $5);
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
| PQ_MAP '(' qb_name_option relation_factor_in_hint ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PQ_MAP, 2, $3, $4);
}
| LOAD_BATCH_SIZE '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOAD_BATCH_SIZE, 1, $3);
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

for_update:
FOR UPDATE opt_for_update_of opt_for_wait_or_skip
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOR_UPDATE, 2, $3, $4);
}
;

opt_for_update_of:
/* EMPTY */
{
  $$ = NULL;
}
| OF column_list
{
  merge_nodes($$, result, T_COLUMN_LIST, $2);
}

opt_for_wait_or_skip:
/* EMPTY */
{
  $$ = NULL;
}
| WAIT INTNUM
{
  /* USE T_SFU_XXX to avoid being parsed by plan cache as template var */
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = $2->value_;
}
| NOWAIT
{
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = 0;
}
| SKIP LOCKED
{
  malloc_terminal_node($$, result->malloc_pool_, T_SKIP_LOCKED);
}
;

parameterized_trim:
bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 1, $1);
}
| bit_expr FROM bit_expr
{
  ParseNode *default_type = NULL;
  //avoid parameterized, so use T_DEFAULT_INT
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, $1, $3);
}
| BOTH bit_expr FROM bit_expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, $2, $4);
}
| LEADING bit_expr FROM bit_expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, $2, $4);
}
| TRAILING bit_expr FROM bit_expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, $2, $4);
}
| BOTH FROM bit_expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 2, default_type, $3);
}
| LEADING FROM bit_expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 2, default_type, $3);
}
| TRAILING FROM bit_expr
{
  ParseNode *default_type = NULL;
  malloc_terminal_node(default_type, result->malloc_pool_, T_DEFAULT_INT);
  default_type->value_ = 2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 2, default_type, $3);
}
;

opt_groupby_having:
/* EMPTY */
{ $$ = NULL; }
| GROUP BY groupby_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INVALID, 2, $3, NULL);
}
| HAVING expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INVALID, 2, NULL, $2);
}
| GROUP BY groupby_clause HAVING expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INVALID, 2, $3, $5);
}
| HAVING expr GROUP BY groupby_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INVALID, 2, $5, $2);
}
;

groupby_clause:
groupby_element_list
{
  merge_nodes($$, result, T_GROUPBY_CLAUSE, $1);
}
;

groupby_element_list:
groupby_element
{
  $$ = $1;
}
| groupby_element_list ',' groupby_element
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

groupby_element:
group_by_expr
{ $$ = $1; }
| rollup_clause
{ $$ = $1; }
| cube_clause
{ $$ = $1; }
| grouping_sets_clause
{ $$ = $1; }
| '(' ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_NULL);
}
;

group_by_expr:
bit_expr
{ malloc_non_terminal_node($$, result->malloc_pool_, T_GROUPBY_KEY, 1, $1); }
;

rollup_clause:
ROLLUP '(' group_by_expr_list ')'
{
  merge_nodes($$, result, T_ROLLUP_LIST, $3);
}
;

cube_clause:
CUBE '(' group_by_expr_list ')'
{
  merge_nodes($$, result, T_CUBE_LIST, $3);
}
;

group_by_expr_list:
group_by_expr
{ $$ = $1; }
| group_by_expr_list ',' group_by_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

grouping_sets_clause:
GROUPING SETS '(' grouping_sets_list ')'
{
  merge_nodes($$, result, T_GROUPING_SETS_LIST, $4);
}
;


grouping_sets_list:
grouping_sets
{
  $$ = $1;
}
| grouping_sets_list ',' grouping_sets
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}

grouping_sets:
group_by_expr
{ $$ = $1; }
| rollup_clause
{ $$ = $1; }
| cube_clause
{ $$ = $1; }
| '(' ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_NULL);
}
;

opt_order_by:
order_by                { $$ = $1; }
| /*EMPTY*/             { $$ = NULL; }
;

opt_siblings:
{ $$ = NULL; }
| SIBLINGS
{ malloc_terminal_node($$, result->malloc_pool_, T_SIBLINGS); }
;

order_by:
ORDER opt_siblings BY sort_list
{
  ParseNode *sort_list = NULL;
  merge_nodes(sort_list, result, T_SORT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORDER_BY, 2, sort_list, $2);
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
bit_expr opt_asc_desc
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SORT_KEY, 2, $1, $2);
  if (NULL == $1->str_value_) {
    dup_string($1, result, @1.first_column, @1.last_column);
  }
}
;


opt_null_pos:
/* EMPTY */
{ $$ = NULL; }
| NULLS LAST
{ malloc_terminal_node($$, result->malloc_pool_, T_INT); $$->value_ = 1; }
| NULLS FIRST
{ malloc_terminal_node($$, result->malloc_pool_, T_INT); $$->value_ = 2; }
;

opt_ascending_type:
/* EMPTY */
{ malloc_terminal_node($$, result->malloc_pool_, T_SORT_ASC); }
| ASC
{ malloc_terminal_node($$, result->malloc_pool_, T_SORT_ASC); }
| DESC
{ malloc_terminal_node($$, result->malloc_pool_, T_SORT_DESC); }
;

opt_asc_desc:
opt_ascending_type opt_null_pos
{
  $$ = $1;
  if (NULL == $2) {
    if (T_SORT_ASC == $$->type_) {
      $$->value_ = 1; // NULLS LAST
    } else {
      $$->value_ = 2; // NULLS FIRST
    }
  } else {
    $$->value_ = $2->value_;
  }
}

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
query_expression_option %prec LOWER_PARENS
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
bit_expr %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, $1);
  if (T_VARCHAR == $1->type_ || T_CHAR == $1->type_) {
    if (2 == $1->num_child_ && 1 == $1->children_[1]->num_child_) {
      char * str_ptr = NULL;
      str_ptr = strndup_with_prefix_and_postfix("'", "'",
                        $1->children_[1]->children_[0]->str_value_,
                        $1->children_[1]->children_[0]->str_len_,
                        result->malloc_pool_);
      if (NULL == str_ptr) {
        yyerror(NULL, result, "no memory to strdup sql string\n");
        YYERROR;
      }
      $$->str_len_ = $1->children_[1]->children_[0]->str_len_ + 2;

      // projection的原始串保留在raw_text_，用于select item常量参数化
      $$->raw_text_ = cp_str_value(str_ptr, $$->str_len_, result->malloc_pool_);
      $$->text_len_ = $$->str_len_;

      $$->str_len_ = str_remove_space(str_ptr, $$->str_len_);
      $$->str_value_ = str_toupper(str_ptr, $$->str_len_);
    } else {
      char * str_ptr = NULL;
      str_ptr = strndup_with_prefix_and_postfix("'", "'",
                        $1->str_value_, $1->str_len_,
                        result->malloc_pool_);
      if (NULL == str_ptr) {
        yyerror(NULL, result, "no memory to strdup sql string\n");
        YYERROR;
      }
      $$->str_len_ = $1->str_len_ + 2;

      // projection的原始串保留在raw_text_，用于select item常量参数化
      $$->raw_text_ = cp_str_value(str_ptr, $$->str_len_, result->malloc_pool_);
      $$->text_len_ = $$->str_len_;

      $$->str_len_ = str_remove_space(str_ptr, $$->str_len_);
      $$->str_value_ = str_toupper(str_ptr, $$->str_len_);
    }
  } else if ($1->type_ == T_FUN_SYS_REGEXP_LIKE) {
    yyerror(NULL, result, "regexp_like not in select;");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    dup_expr_string($$, result, @1.first_column, @1.last_column);
    char *dup_value = parse_strdup($$->str_value_, result->malloc_pool_, &($$->str_len_));
    if (OB_UNLIKELY(NULL == dup_value)) {
      yyerror(NULL, result, "No more space for mallocing string");
      YYABORT_NO_MEMORY;
    } else {
      // projection的原始串保留在raw_text_，用于select item常量参数化
      $$->raw_text_ = cp_str_value(dup_value, $$->str_len_, result->malloc_pool_);
      $$->text_len_ = $$->str_len_;

      $$->str_len_ = str_remove_space(dup_value, $$->str_len_);
      dup_value = str_toupper(dup_value, $$->str_len_);
      $$->str_value_ = dup_value;
    }
  }
  $$->raw_sql_offset_ = @1.first_column - 1;
}
| bit_expr column_label
{
  ParseNode *alias_node = NULL;
  if (OB_UNLIKELY((NULL == $1))) {
    yyerror(NULL, result, "alias expr can not be NULL\n");
    YYABORT_UNEXPECTED;
  } else if (OB_UNLIKELY(T_COLUMN_REF == $1->type_ && NULL != $1->children_ && NULL != $1->children_[1]
             && NULL != $1->children_[2] && T_STAR == $1->children_[2]->type_)) {
    yyerror(&@2, result, "select table.* as label is invalid\n");
    YYERROR;
  } else if ($1->type_ == T_FUN_SYS_REGEXP_LIKE ) {
    yyerror(NULL, result, "regexp_like not in select;");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, $2);
    malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
    dup_expr_string($$, result, @1.first_column, @1.last_column);
    dup_node_string($2, alias_node, result->malloc_pool_);
    alias_node->param_num_ = 0;
  }
}
| bit_expr AS column_label
{
  ParseNode *alias_node = NULL;
  if (OB_UNLIKELY((NULL == $1))) {
    yyerror(NULL, result, "alias expr can not be NULL\n");
    YYABORT_UNEXPECTED;
  } else if (OB_UNLIKELY(T_COLUMN_REF == $1->type_ && NULL != $1->children_ && NULL != $1->children_[1]
             && NULL != $1->children_[2] && T_STAR == $1->children_[2]->type_)) {
    yyerror(&@2, result, "select table.* as label is invalid\n");
    YYERROR;
  } else if ($1->type_ == T_FUN_SYS_REGEXP_LIKE ) {
    yyerror(NULL, result, "regexp_like not in select;");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, $3);
    malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
    dup_expr_string($$, result, @1.first_column, @1.last_column);
    dup_node_string($3, alias_node, result->malloc_pool_);
    alias_node->param_num_ = 0;
  }
}
| '*'
{
  ParseNode *star_node = NULL;
  malloc_terminal_node(star_node, result->malloc_pool_, T_STAR);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, star_node);
  dup_expr_string($$, result, @1.first_column, @1.last_column);
  setup_token_pos_info($$, @1.first_column - 1, 1);
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

// @xiyu TODO not support  like 'select * from (t1,t2) join (t3,t4)'
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
| '(' table_reference ')'
{
  $$ = $2;
}
/*
| TABLE '(' function_name '(' opt_func_param_list ')' ')' opt_table_alias
{
  ParseNode *func_node = NULL;
  if (NULL != $5)
  {
    ParseNode *params = NULL;
    merge_nodes(params, result, T_EXPR_LIST, $5);
    malloc_non_terminal_node(func_node, result->malloc_pool_, T_FUN_SYS, 2, $3, params);
  }
  else
  {
    malloc_non_terminal_node(func_node, result->malloc_pool_, T_FUN_SYS, 1, $3);
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_COLLECTION_EXPRESSION, 2, func_node, $8);
}*/
| TABLE '(' simple_expr ')' %prec LOWER_PARENS
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_COLLECTION_EXPRESSION, 2, $3, unname_node);
}
| TABLE '(' simple_expr ')' relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_COLLECTION_EXPRESSION, 2, $3, $5);
}
;

tbl_name:
relation_factor %prec LOWEST_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 5, $1, NULL, NULL, NULL, NULL);
}
| relation_factor use_partition %prec LOWEST_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 5, $1, NULL, $2, NULL, NULL);
}
| relation_factor use_flashback %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 5, $1, NULL, NULL, NULL, $2);
}
| relation_factor use_partition use_flashback %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 5, $1, NULL, $2, NULL, $3);
}
| relation_factor sample_clause %prec LOWEST_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, NULL, NULL, $2);
}
| relation_factor use_partition sample_clause %prec LOWEST_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, NULL, $2, $3);
}
| relation_factor sample_clause use_flashback %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 5, $1, NULL, NULL, $2, $3);
}
| relation_factor use_partition sample_clause use_flashback %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 5, $1, NULL, $2, $3, $4);
}
| relation_factor sample_clause seed %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, NULL, NULL, $2);
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
}
| relation_factor use_partition sample_clause seed %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, NULL, $2, $3);
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
}
| relation_factor sample_clause seed use_flashback %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 5, $1, NULL, NULL, $2, $4);
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
}
| relation_factor use_partition sample_clause seed use_flashback %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 5, $1, NULL, $2, $3, $5);
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
}
| relation_factor relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $2, NULL, NULL, NULL);
}
| relation_factor use_partition relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, NULL, $2, NULL);
}
| relation_factor use_flashback relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $3, NULL, NULL, NULL, $2);
}
| relation_factor use_partition use_flashback relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $4, NULL, $2, NULL, $3);
}
| relation_factor sample_clause relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, NULL, NULL, $2);
}
| relation_factor use_partition sample_clause relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $4, NULL, $2, $3, NULL);
}
| relation_factor sample_clause use_flashback relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $4, NULL, NULL, $2, $3);
}
| relation_factor use_partition sample_clause use_flashback relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $5, NULL, $2, $3, $4);
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $5, NULL, $2, $3, NULL);
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
}
| relation_factor sample_clause seed use_flashback relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $5, NULL, NULL, $2, $4);
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
}
| relation_factor use_partition sample_clause seed use_flashback relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $6, NULL, $2, $3, $5);
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
}
| relation_factor fetch_next_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, NULL, NULL, NULL, NULL, NULL, NULL, $2);
}
| relation_factor use_partition fetch_next_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, NULL, NULL, $2, NULL, NULL, NULL, $3);
}
| relation_factor use_flashback fetch_next_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, NULL, NULL, NULL, NULL, $2, NULL, $3);
}
| relation_factor use_partition use_flashback fetch_next_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, NULL, NULL, $2, NULL, $3, NULL, $4);
}
| relation_factor sample_clause fetch_next_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, NULL, NULL, NULL, $2, NULL, NULL, $3);
}
| relation_factor use_partition sample_clause fetch_next_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, NULL, NULL, $2, $3, NULL, NULL, $4);
}
| relation_factor sample_clause use_flashback fetch_next_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, NULL, NULL, NULL, $2, $3, NULL, $4);
}
| relation_factor use_partition sample_clause use_flashback fetch_next_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, NULL, NULL, $2, $3, $4, NULL, $5);
}
| relation_factor sample_clause seed fetch_next_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, NULL, NULL, NULL, $2, NULL, NULL, $4);
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
}
| relation_factor use_partition sample_clause seed fetch_next_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, NULL, NULL, $2, $3, NULL, NULL, $5);
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
}
| relation_factor sample_clause seed use_flashback fetch_next_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, NULL, NULL, NULL, $2, $4, NULL, $5);
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
}
| relation_factor use_partition sample_clause seed use_flashback fetch_next_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, NULL, NULL, $2, $3, $5, NULL, $6);
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
}
| relation_factor transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, NULL, NULL, NULL, NULL, NULL, $2);
}
| relation_factor relation_name transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, $2, NULL, NULL, NULL, NULL, $3);
}
| relation_factor use_partition transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, NULL, NULL, $2, NULL, NULL, $3);
}
| relation_factor use_partition relation_name transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, $3, NULL, $2, NULL, NULL, $4);
}
| relation_factor sample_clause transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, NULL, NULL, NULL, $2, NULL, $3);
}
| relation_factor sample_clause relation_name transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, $3, NULL, NULL, $2, NULL, $4);
}
| relation_factor use_partition sample_clause transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, NULL, NULL, $2, $3, NULL, $4);
}
| relation_factor use_partition sample_clause relation_name transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, $4, NULL, $2, $3, NULL, $5);
}
| relation_factor sample_clause seed transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, NULL, NULL, NULL, $2, NULL, $4);
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
}
| relation_factor sample_clause seed relation_name transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, $4, NULL, NULL, $2, NULL, $5);
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
}
| relation_factor use_partition sample_clause seed transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, NULL, NULL, $2, $3, NULL, $5);
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
}
| relation_factor use_partition sample_clause seed relation_name transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, $5, NULL, $2, $3, NULL, $6);
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
}
| dual_table relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $2, NULL, NULL, NULL);
}
| dual_table %prec LOWER_PARENS
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, unname_node, NULL, NULL, NULL);
  $$->value_ = -1; // 用于区分单表无别名 from dual 表
}

dual_table:
DUAL
{
  /*
  * We make dual as a subquery.
  * select 1 from dual; -> select 1 from (select * from dual);
  */
  ParseNode *star_node = NULL;
  malloc_terminal_node(star_node, result->malloc_pool_, T_STAR);

  ParseNode *project_node = NULL;
  malloc_non_terminal_node(project_node, result->malloc_pool_, T_PROJECT_STRING, 1, star_node);
  dup_expr_string(project_node, result, @1.first_column, @1.last_column);

  ParseNode *project_list = NULL;
  merge_nodes(project_list, result, T_PROJECT_LIST, project_node);

  malloc_select_node($$, result->malloc_pool_);
  $$->children_[PARSE_SELECT_SELECT] = project_list;
  // end mock subquery
}
;

opt_table_alias:
relation_name
{
  $$ = $1;
}
| /*Empty*/
{
  $$ = NULL;
}
;

transpose_clause:
PIVOT '(' pivot_aggr_clause transpose_for_clause transpose_in_clause ')'
{
  ParseNode *pivot_aggr_node = NULL;
  merge_nodes(pivot_aggr_node, result, T_PIVOT_AGGR_LIST, $3);
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_PIVOT, 4, pivot_aggr_node, $4, $5, unname_node);
}
| PIVOT '(' pivot_aggr_clause transpose_for_clause transpose_in_clause ')' relation_name
{
  ParseNode *pivot_aggr_node = NULL;
  merge_nodes(pivot_aggr_node, result, T_PIVOT_AGGR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PIVOT, 4, pivot_aggr_node, $4, $5, $7);
}
| UNPIVOT opt_unpivot_include '(' unpivot_column_clause transpose_for_clause unpivot_in_clause ')'
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_UNPIVOT, 4, $4, $5, $6, unname_node);
  $$->value_ = $2[0];
}
| UNPIVOT opt_unpivot_include '(' unpivot_column_clause transpose_for_clause unpivot_in_clause ')' relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_UNPIVOT, 4, $4, $5, $6, $8);
  $$->value_ = $2[0];
}
;

pivot_aggr_clause:
pivot_single_aggr_clause
{
  $$ = $1;
}
| pivot_aggr_clause ',' pivot_single_aggr_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

opt_as_alias:
opt_as relation_name
{
 (void)($1);
  $$ = $2;
}
| /*empty*/
{
  $$ = NULL;
}
;

pivot_single_aggr_clause:
aggregate_function opt_as_alias
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PIVOT_AGGR, 2, $1, $2);
}
| access_func_expr_count opt_as_alias
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PIVOT_AGGR, 2, $1, $2);
}
;

transpose_for_clause:
FOR column_name
{
  $$ = $2;
}
| FOR '(' column_name_list ')'
{
  merge_nodes($$, result, T_COLUMN_LIST, $3);
}
;

transpose_in_clause:
IN '(' transpose_in_args ')'
{
  merge_nodes($$, result, T_PIVOT_IN_LIST, $3);
}
;

transpose_in_args:
transpose_in_arg
{
  $$ = $1;
}
| transpose_in_args ',' transpose_in_arg
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

transpose_in_arg:
bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PIVOT_IN, 2, $1, NULL);
}
| bit_expr relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PIVOT_IN, 2, $1, $2);
}
| bit_expr AS relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PIVOT_IN, 2, $1, $3);
}
;

opt_unpivot_include:
/*empty*/
{
  $$[0] = 0;
}
| EXCLUDE NULLS
{
  $$[0] = 1;
}
| INCLUDE NULLS
{
  $$[0] = 2;
}
;

unpivot_column_clause:
column_name
{
  $$ = $1;
}
| '(' column_name_list ')'
{
  merge_nodes($$, result, T_COLUMN_LIST, $2);
}
;

unpivot_in_clause:
IN '(' unpivot_in_args ')'
{
  merge_nodes($$, result, T_UNPIVOT_IN_LIST, $3);
}
;

unpivot_in_args:
unpivot_in_arg
{
  $$ = $1;
}
| unpivot_in_args ',' unpivot_in_arg
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

opt_as_expr:
AS bit_expr
{
  $$ = $2;
}
| /*empty*/
{
  $$ = NULL;
}
;

unpivot_in_arg:
unpivot_column_clause opt_as_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_UNPIVOT_IN, 2, $1, $2);
}
;

dml_table_name:
relation_factor %prec LOWEST_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, NULL, NULL, NULL);
}
| relation_factor use_partition
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 4, $1, NULL, $2, NULL);
}
;

insert_table_clause:
dml_table_name %prec LOWER_PARENS
{
  if (T_ORG != $1->type_) {
    result->extra_errno_ = OB_PARSER_ERR_UNEXPECTED;
    yyerror(NULL, result, "dml table name is invalid\n");
    YYERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5,
                           $1->children_[0],
                           NULL,
                           $1->children_[1],
                           $1->children_[2],
                           $1->children_[3]);
}
| dml_table_name relation_name
{
  if (T_ORG != $1->type_) {
    result->extra_errno_ = OB_PARSER_ERR_UNEXPECTED;
    yyerror(NULL, result, "dml table name is invalid\n");
    YYERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5,
                           $1->children_[0],
                           $2,
                           $1->children_[1],
                           $1->children_[2],
                           $1->children_[3]);
}
| select_with_parens %prec LOWER_PARENS
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, unname_node);
}
| select_with_parens relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, $2);
}
| '(' subquery fetch_next_clause ')' %prec LOWER_PARENS
{
  (void)($2);
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before FETCH.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $2, unname_node);
  }
}
| '(' subquery fetch_next_clause ')' relation_name
{
  (void)($2);
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before FETCH.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $2, $5);
  }
}
| '(' subquery  order_by opt_fetch_next ')' %prec LOWER_PARENS
{
  (void)($2);
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $2, unname_node);
  }
}
| '(' subquery  order_by opt_fetch_next ')' relation_name
{
  (void)($2);
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $2, $6);
  }
}
;

dml_table_clause:
dml_table_name opt_table_alias
{
  if (T_ORG != $1->type_) {
    result->extra_errno_ = OB_PARSER_ERR_UNEXPECTED;
    yyerror(NULL, result, "dml table name is invalid\n");
    YYERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5,
                           $1->children_[0],
                           $2,
                           $1->children_[1],
                           $1->children_[2],
                           $1->children_[3]);
}
| ONLY '(' dml_table_name ')' opt_table_alias
{
  if (T_ORG != $3->type_) {
    result->extra_errno_ = OB_PARSER_ERR_UNEXPECTED;
    yyerror(NULL, result, "dml table name is invalid\n");
    YYERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5,
                           $3->children_[0],
                           $5,
                           $3->children_[1],
                           $3->children_[2],
                           $3->children_[3]);
}
| select_with_parens opt_table_alias
{
  if (NULL == $2) {
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, unname_node);
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, $2);
  }
}
| '(' subquery fetch_next_clause ')' opt_table_alias
{
  (void)($2);
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    if (NULL == $5) {
      ParseNode *unname_node = NULL;
      make_name_node(unname_node, result->malloc_pool_, "");
      malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $2, unname_node);
    } else {
      malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $2, $5);
    }
  }
}
| '(' subquery  order_by opt_fetch_next ')' opt_table_alias
{
  (void)($2);
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    if (NULL == $6) {
      ParseNode *unname_node = NULL;
      make_name_node(unname_node, result->malloc_pool_, "");
      malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $2, unname_node);
    } else {
      malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $2, $6);
    }
  }
}
;

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
}
;

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
select_with_parens %prec LOWER_PARENS
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, unname_node);
}
| select_with_parens use_flashback %prec LOWER_PARENS
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, unname_node, NULL, NULL, NULL, $2);
}
| '(' subquery order_by ')' %prec LOWER_PARENS
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $2, unname_node, NULL, NULL, NULL, NULL);
  }
}
| '(' subquery order_by ')' use_flashback %prec LOWER_PARENS
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $2, unname_node, NULL, NULL, NULL, $5);
  }
}
| '(' subquery order_by fetch_next_clause ')' %prec LOWER_PARENS
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $2, unname_node, NULL, NULL, NULL);
  }
}
| '(' subquery order_by fetch_next_clause ')' use_flashback %prec LOWER_PARENS
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $2, unname_node, NULL, NULL, NULL, $6);
  }
}
| '(' subquery fetch_next_clause ')' %prec LOWER_PARENS
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $2, unname_node, NULL, NULL, NULL);
  }
}
| '(' subquery fetch_next_clause ')'  use_flashback %prec LOWER_PARENS
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $2, unname_node, NULL, NULL, NULL, $5);
  }
}
| select_with_parens relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $2, NULL, NULL, NULL);
}
| select_with_parens use_flashback relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $3, NULL, NULL, NULL, $2);
}
| '(' subquery order_by ')' relation_name
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $2, $5, NULL, NULL, NULL);
  }
}
| '(' subquery order_by ')' use_flashback relation_name
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $2, $6, NULL, NULL, NULL, $5);
  }
}
| '(' subquery fetch_next_clause ')' relation_name
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $2, $5, NULL, NULL, NULL);
  }
}
| '(' subquery fetch_next_clause ')' use_flashback relation_name
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $2, $6, NULL, NULL, NULL, $5);
  }
}
| '(' subquery order_by fetch_next_clause ')' relation_name
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $2, $6, NULL, NULL, NULL);
  }
}
| '(' subquery order_by fetch_next_clause ')' use_flashback relation_name
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $2, $7, NULL, NULL, NULL, $6);
  }
}
| select_with_parens fetch_next_clause
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, unname_node, NULL, NULL, NULL, NULL, NULL, $2);
}
| select_with_parens use_flashback fetch_next_clause
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $1, unname_node, NULL, NULL, NULL, $2, NULL, $3);
}
| '(' subquery order_by ')' fetch_next_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $2, unname_node, NULL, NULL, NULL, NULL, NULL, $5);
  }
}
| '(' subquery order_by ')' use_flashback fetch_next_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $2, unname_node, NULL, NULL, NULL, $5, NULL, $6);
  }
}
| '(' subquery fetch_next_clause ')' fetch_next_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $2, unname_node, NULL, NULL, NULL, NULL, NULL, $5);
  }
}
| '(' subquery fetch_next_clause ')' use_flashback fetch_next_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $2, unname_node, NULL, NULL, NULL, $5, NULL, $6);
  }
}
| '(' subquery order_by fetch_next_clause ')' fetch_next_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $2, unname_node, NULL, NULL, NULL, NULL, NULL, $6);
  }
}
| '(' subquery order_by fetch_next_clause ')' use_flashback fetch_next_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 8, $2, unname_node, NULL, NULL, NULL, $6, NULL, $7);
  }
}
| select_with_parens transpose_clause
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, unname_node, NULL, NULL, NULL, NULL, $2);
}
| select_with_parens use_flashback transpose_clause
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, unname_node, NULL, NULL, NULL, $2, $3);
}
| '(' subquery order_by ')' transpose_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $2, unname_node, NULL, NULL, NULL, NULL, $5);
  }
}
| '(' subquery order_by ')' use_flashback transpose_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $2, unname_node, NULL, NULL, NULL, $5, $6);
  }
}
| '(' subquery order_by fetch_next_clause ')' transpose_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $2, unname_node, NULL, NULL, NULL, NULL, $6);
  }
}
| '(' subquery order_by fetch_next_clause ')' use_flashback transpose_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    ParseNode *unname_node = NULL;
    make_name_node(unname_node, result->malloc_pool_, "");
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $2, unname_node, NULL, NULL, NULL, $6, $7);
  }
}
| '(' subquery fetch_next_clause ')' transpose_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $2, $5, NULL, NULL, NULL, NULL, $5);
  }
}
| '(' subquery fetch_next_clause ')'  use_flashback transpose_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $2, $5, NULL, NULL, NULL, $5, $6);
  }
}
| select_with_parens relation_name transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, $2, NULL, NULL, NULL, NULL, $3);
}
| select_with_parens use_flashback relation_name transpose_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $1, $3, NULL, NULL, NULL, $2, $4);
}
| '(' subquery order_by ')' relation_name transpose_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $2, $5, NULL, NULL, NULL, NULL, $6);
  }
}
| '(' subquery order_by ')' use_flashback relation_name transpose_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $2, $6, NULL, NULL, NULL, $5, $7);
  }
}
| '(' subquery fetch_next_clause ')' relation_name transpose_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $2, $5, NULL, NULL, NULL, NULL, $6);
  }
}
| '(' subquery fetch_next_clause ')' use_flashback relation_name transpose_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_FETCH] = $3;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $2, $6, NULL, NULL, NULL, $5, $7);
  }
}
| '(' subquery order_by fetch_next_clause ')' relation_name transpose_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $2, $6, NULL, NULL, NULL, NULL, $7);
  }
}
| '(' subquery order_by fetch_next_clause ')' use_flashback relation_name transpose_clause
{
  if ($2->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $2->children_[PARSE_SELECT_ORDER] = $3;
    $2->children_[PARSE_SELECT_FETCH] = $4;
    malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 7, $2, $7, NULL, NULL, NULL, $6, $8);
  }
}

/*
  table PARTITION(list of partitions)
*/

use_partition:
PARTITION '(' name_list ')'
{
  ParseNode *name_list = NULL;
  merge_nodes(name_list, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_PARTITION, 1, name_list);
}
| SUBPARTITION '(' name_list ')'
{
  ParseNode *name_list = NULL;
  merge_nodes(name_list, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_SUBPARTITION, 1, name_list);
}
;

use_flashback:
AS OF TIMESTAMP bit_expr %prec LOWER_PARENS
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_FLASHBACK_QUERY_TIMESTAMP, 2, $4, unname_node);
}
| AS OF SCN bit_expr %prec LOWER_PARENS
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_FLASHBACK_QUERY_SCN, 2, $4, unname_node);
}
;

/* diff normal_relation_facotr and dot_relation_factor for relation_factor_in_hint*/
relation_factor:
normal_relation_factor
{
  $$ = $1;
}
| dot_relation_factor
{
  $$ = $1;
}
;

normal_relation_factor:
relation_name opt_dblink
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 3, NULL, $1, $2);
  dup_node_string($1, $$, result->malloc_pool_);
}
| database_factor '.' relation_name opt_dblink
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 3, $1, $3, $4);
  dup_node_string($3, $$, result->malloc_pool_);
}
;

dot_relation_factor:
'.' relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 2, NULL, $2);
  dup_node_string($2, $$, result->malloc_pool_);
}
;

opt_dblink:
USER_VARIABLE    /* USER_VARIABLE is '@xxxx', see sql_parser.l */
{
  $$ = $1;
  $$->type_ = T_IDENT;
}
|
{ $$ = NULL; }
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
/* we do not support cross join and natural join
 * using clause is not supported either
 */
table_reference outer_join_type JOIN table_factor join_condition
{
  JOIN_MERGE_NODES($1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $2, $1, $4, $5, NULL);
}
| table_reference INNER JOIN table_factor ON expr %prec LOWER_ON
{
  JOIN_MERGE_NODES($1, $4);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_INNER);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $$, $1, $4, $6, NULL);
}
| table_reference INNER JOIN table_factor USING '(' column_list ')'
{
  JOIN_MERGE_NODES($1, $4);
  ParseNode *condition_node = NULL;
  merge_nodes(condition_node, result, T_COLUMN_LIST, $7);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_INNER);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $$, $1, $4, condition_node, NULL);
}
| table_reference JOIN table_factor ON expr %prec LOWER_ON
{
  JOIN_MERGE_NODES($1, $3);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_INNER);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $$, $1, $3, $5, NULL);
}
| table_reference JOIN table_factor USING '(' column_list ')'
{
  JOIN_MERGE_NODES($1, $3);
  ParseNode *condition_node = NULL;
  merge_nodes(condition_node, result, T_COLUMN_LIST, $6);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_INNER);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $$, $1, $3, condition_node, NULL);
}
| table_reference natural_join_type table_factor
{
  JOIN_MERGE_NODES($1, $3);

  ParseNode *join_attr = NULL;
  malloc_terminal_node(join_attr, result->malloc_pool_, T_NATURAL_JOIN);

  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $2, $1, $3, NULL, join_attr);
}
| table_reference CROSS JOIN table_factor
{
  JOIN_MERGE_NODES($1, $4);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_INNER);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $$, $1, $4, NULL, NULL);
}
;

natural_join_type:
NATURAL outer_join_type JOIN
{
  $$ = $2
}
| NATURAL JOIN
{
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_INNER);
}
| NATURAL INNER JOIN
{
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_INNER);
}
;

outer_join_type:
FULL join_outer
{
  /* make bison mute */
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_FULL);
}
| LEFT join_outer
{
  /* make bison mute */
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_LEFT);
}
| RIGHT join_outer
{
  /* make bison mute */
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_RIGHT);
}
;

join_outer:
OUTER                    { $$ = NULL; }
| /* EMPTY */               { $$ = NULL; }
;

/*****************************************************************************
 *
 *	with clause (common table expression) (Mysql CTE grammer implement)
 *  TODO muhang.zb
 *
*****************************************************************************/

with_select:
with_clause select_no_parens opt_when
{
  $$ = $2;
  $$->children_[PARSE_SELECT_WHEN] = $3;
  if (NULL == $$->children_[PARSE_SELECT_FOR_UPD] && NULL != $3)
  {
    malloc_terminal_node($$->children_[PARSE_SELECT_FOR_UPD], result->malloc_pool_, T_INT);
    $$->children_[PARSE_SELECT_FOR_UPD]->value_ = -1;
  }
  $$->children_[PARSE_SELECT_WITH] = $1;
}
| with_clause select_with_parens
{
  $$ = $2;
  $$->children_[PARSE_SELECT_WITH] = $1;
}
;

with_clause:
WITH with_list
{
  ParseNode *with_list = NULL;
  merge_nodes(with_list, result, T_WITH_CLAUSE_LIST, $2);
  $$ = with_list;
}
|
WITH RECURSIVE common_table_expr
{
  $$ = $3;
}
;

with_list:
with_list ',' common_table_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
|common_table_expr
{
  $$ = $1;
}
;


common_table_expr:
relation_name opt_column_alias_name_list AS '(' select_no_parens ')' opt_search_clause opt_cycle_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WITH_CLAUSE_AS, 5, $1, $2, $5, $7, $8);
}
| relation_name opt_column_alias_name_list AS '(' with_select ')' opt_search_clause opt_cycle_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WITH_CLAUSE_AS, 5, $1, $2, $5, $7, $8);
}
| relation_name opt_column_alias_name_list AS '(' select_with_parens ')' opt_search_clause opt_cycle_clause
{
  if ($5->children_[PARSE_SELECT_ORDER] != NULL && $5->children_[PARSE_SELECT_FETCH] == NULL) {
    yyerror(NULL, result, "only order by clause can't occur subquery\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_WITH_CLAUSE_AS, 5, $1, $2, $5, $7, $8);
  }
}
| relation_name opt_column_alias_name_list AS '(' subquery order_by opt_fetch_next ')' opt_search_clause opt_cycle_clause
{
  if ($5->children_[PARSE_SELECT_FETCH] != NULL) {
    yyerror(NULL, result, "fetch next clause can't occur before GROUP.etc\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $5->children_[PARSE_SELECT_ORDER] = $6;
    $5->children_[PARSE_SELECT_FETCH] = $7;
    malloc_non_terminal_node($$, result->malloc_pool_, T_WITH_CLAUSE_AS, 5, $1, $2, $5, $9, $10);
  }
}
;

opt_column_alias_name_list:
'(' alias_name_list ')'
{
  ParseNode *col_alias_list = NULL;
  merge_nodes(col_alias_list, result, T_COLUMN_LIST, $2);
  $$ = col_alias_list;
}
|/*EMPTY*/
{ $$ = NULL; }
;

alias_name_list:
column_alias_name
{
  $$ = $1;
}
|alias_name_list ',' column_alias_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

column_alias_name:
column_name
{
  $$ = $1;
}
;

opt_search_clause:
SEARCH DEPTH FIRST BY sort_list search_set_value
{
  ParseNode *sort_list = NULL;
  merge_nodes(sort_list, result, T_SORT_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SEARCH_DEPTH_NODE, 2, sort_list, $6);
}
|SEARCH BREADTH FIRST BY sort_list search_set_value
{
  ParseNode *sort_list = NULL;
  merge_nodes(sort_list, result, T_SORT_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SEARCH_BREADTH_NODE, 2, sort_list, $6);
}
|/*EMPTY*/
{$$ = NULL;};

search_set_value:
SET var_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_CTE_SEARCH_COLUMN);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PSEUDO_COLUMN, 2, $$, $2);
}

/*
search_list:
search_key
{ $$ = $1; }
| search_list ',' search_key
{ malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3); }
;

search_key:
column_name opt_asc_desc
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SORT_KEY, 2, $1, $2);
}
;*/

opt_cycle_clause:
CYCLE alias_name_list SET var_name TO STRING_VALUE DEFAULT STRING_VALUE
{
ParseNode *cte_cyc_col_list = NULL;
$6->param_num_ = 0;
$6->type_ = T_VARCHAR;
$6->is_hidden_const_ = 0;
$8->param_num_ = 0;
$8->type_ = T_VARCHAR;
$8->is_hidden_const_ = 0;
merge_nodes(cte_cyc_col_list, result, T_COLUMN_LIST, $2);
malloc_terminal_node($$, result->malloc_pool_, T_CTE_CYCLE_COLUMN);
malloc_non_terminal_node($$, result->malloc_pool_, T_PSEUDO_COLUMN, 2, $$, $4);
malloc_non_terminal_node($$, result->malloc_pool_, T_CYCLE_NODE, 4, cte_cyc_col_list, $$, $6, $8);
}
|
{$$ = NULL;};

/*****************************************************************************
 *
 *	analyze clause (oracle compatible)
 *  added by guoping.wgp
 *
*****************************************************************************/
analyze_stmt:
ANALYZE TABLE relation_factor analyze_statistics_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ANALYZE, 3, $3, NULL, $4);
}
| ANALYZE TABLE relation_factor use_partition analyze_statistics_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ANALYZE, 3, $3, $4, $5);
}
;

analyze_statistics_clause:
COMPUTE STATISTICS opt_analyze_for_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ANALYZE_STATISTICS, 2, $3, NULL);
}
| ESTIMATE STATISTICS opt_analyze_for_clause opt_analyze_sample_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ANALYZE_STATISTICS, 2, $3, $4);
}
;

opt_analyze_for_clause:
opt_analyze_for_clause_list
{
  merge_nodes($$, result, T_ANALYZE_FOR_CLAUSE_LIST, $1);
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

opt_analyze_for_clause_list:
opt_analyze_for_clause_element
{
  $$ = $1;
}
// | opt_analyze_for_clause_list opt_analyze_for_clause_element
// {
//   malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
// }
;

opt_analyze_for_clause_element:
FOR TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ANALYZE_TABLE);
}
| for_all
{
  $$ = $1;
}
| for_columns
{
  $$ = $1;
}
;

opt_analyze_sample_clause:
SAMPLE INTNUM sample_option
{
  if (OB_UNLIKELY($2->value_ < 1)) {
    yyerror(&@1, result, "sample number must not be less than 1!\n");
    YYERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_ANALYZE_SAMPLE_INFO, 2, $2, $3);
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

sample_option:
ROWS
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_INT);
  $$->value_ = 0;
}
| PERCENTAGE
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_INT);
  $$->value_ = 1;
}
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
  malloc_non_terminal_node(name_node, result->malloc_pool_, T_RELATION_FACTOR, 2, NULL, $4); //前面一个null表示database name
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
| merge_stmt         { $$ = $1; }
| update_stmt         { $$ = $1; }
/*| with_select         { $$ = $1; }*/
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
SHOW opt_full columns_or_fields from_or_in relation_factor opt_from_or_in_database_clause opt_show_condition
{
  (void)$3;
  (void)$4;
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = $2[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_COLUMNS, 4, $$, $5, $6, $7);
}
| SHOW TABLE STATUS opt_from_or_in_database_clause opt_show_condition
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_TABLE_STATUS, 2, $4, $5); }
| SHOW opt_scope VARIABLES opt_show_condition
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = $2[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_VARIABLES, 2, $$, $4);
}
| SHOW CREATE TABLE relation_factor
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_TABLE, 1, $4); }
| SHOW CREATE VIEW relation_factor
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_VIEW, 1, $4); }
| SHOW CREATE PROCEDURE relation_factor
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_PROCEDURE, 1, $4); }
| SHOW CREATE FUNCTION relation_factor
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_FUNCTION, 1, $4); }
| SHOW CREATE TRIGGER relation_factor
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_TRIGGER, 1, $4); }
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
| SHOW PARAMETERS opt_show_condition opt_tenant_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_PARAMETERS, 2, $3, $4);
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

opt_for_grant_user:
opt_for_user
{ $$ = $1; }
| FOR CURRENT_USER '(' ')'
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
 *	help grammar
 *
 *****************************************************************************/
help_stmt:
HELP STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_HELP, 1, $2);
}
| HELP NAME_OB
{
  $2->type_ = T_VARCHAR;
  malloc_non_terminal_node($$, result->malloc_pool_, T_HELP, 1, $2);
}
;

/*****************************************************************************
 *
 *	create user grammar
 *
 *****************************************************************************/
create_user_stmt:
CREATE USER user_specification opt_profile opt_default_tables_space opt_primary_zone
{
  (void)($5);               /* unused */
  ParseNode *users_node = NULL;
  merge_nodes(users_node, result, T_USERS, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER, 5, NULL, users_node, NULL, $4, $6);
}
| CREATE USER user_specification require_specification opt_profile opt_default_tables_space opt_primary_zone
{
  (void)($6);               /* unused */
  ParseNode *users_node = NULL;
  merge_nodes(users_node, result, T_USERS, $3);
  ParseNode *require_node = NULL;
  merge_nodes(require_node, result, T_TLS_OPTIONS, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER, 5, NULL, users_node, require_node, $5, $7);
}
;

opt_primary_zone:
PRIMARY_ZONE opt_equal_mark primary_zone_name
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_ZONE, 1, $3);
}
|
{
  $$ = NULL;
}
;

default_role_clause:
/*role_list*/
role_opt_identified_by_list
{
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_INT);
  type_node->value_ = 0;

  ParseNode *role_list = NULL;
  merge_nodes(role_list, result, T_LINK_NODE, $1);

  malloc_non_terminal_node($$, result->malloc_pool_, T_DEFAULT_ROLE, 2, type_node, role_list);
}
| ALL
{
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_INT);
  type_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_DEFAULT_ROLE, 1, type_node);
}
| ALL EXCEPT role_list
{
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_INT);
  type_node->value_ = 2;

  ParseNode *role_list = NULL;
  merge_nodes(role_list, result, T_LINK_NODE, $3);

  malloc_non_terminal_node($$, result->malloc_pool_, T_DEFAULT_ROLE, 2, type_node, role_list);
}
| NONE
{
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_INT);
  type_node->value_ = 3;
  malloc_non_terminal_node($$, result->malloc_pool_, T_DEFAULT_ROLE, 1, type_node);
}

alter_user_stmt:
ALTER USER user_with_host_name DEFAULT ROLE default_role_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_USER_DEFAULT_ROLE, 2, $3, $6);
}
| ALTER USER user_with_host_name PRIMARY_ZONE opt_equal_mark primary_zone_name
{
  (void)$5;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_USER_PRIMARY_ZONE, 2, $3, $6);
};

alter_user_profile_stmt:
ALTER USER user_with_host_name user_profile
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_USER_PROFILE, 2, $3, $4);
}
;

alter_role_stmt:
ALTER ROLE role opt_not_identified
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_ROLE, 2,
                           $3, // role name
                           $4);  // NOT IDENTIFIED
  $$->value_ = 1;
}
| ALTER ROLE role IDENTIFIED BY password
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_ROLE, 3,
                           $3, // role name
                           need_enc_node,
                           $6); // password
  $$->value_ = 2;
}
| ALTER ROLE role IDENTIFIED BY VALUES password
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_ROLE, 3,
                           $3, // role name
                           need_enc_node,
                           $7); // password
  $$->value_ = 3;
}
;

user_specification:
user opt_host_name IDENTIFIED BY password
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER_SPEC, 4, $1, $5, need_enc_node, $2);
}
| user opt_host_name IDENTIFIED BY VALUES password
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

grant_user:
user opt_host_name
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER_SPEC, 4, $1, NULL, need_enc_node, $2);
}
| CONNECT
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_len_ = strlen("CONNECT");
  $$->str_value_ = parse_strdup("CONNECT", result->malloc_pool_, &($$->str_len_));
}
| RESOURCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_len_ = strlen("RESOURCE");
  $$->str_value_ = parse_strdup("RESOURCE", result->malloc_pool_, &($$->str_len_));
}
| PUBLIC
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_len_ = strlen("PUBLIC");
  $$->str_value_ = parse_strdup("PUBLIC", result->malloc_pool_, &($$->str_len_));
}
;

grant_user_list:
grant_user
{
  $$ = $1;
}
| grant_user_list ',' grant_user
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
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
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
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
| CONNECT
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_len_ = strlen("CONNECT");
  $$->str_value_ = parse_strdup("CONNECT", result->malloc_pool_, &($$->str_len_));
}
| RESOURCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_len_ = strlen("RESOURCE");
  $$->str_value_ = parse_strdup("RESOURCE", result->malloc_pool_, &($$->str_len_));
}
| PUBLIC
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_len_ = strlen("PUBLIC");
  $$->str_value_ = parse_strdup("PUBLIC", result->malloc_pool_, &($$->str_len_));
}
;

password:
INTNUM
{
  $$ = $1;
  $$->stmt_loc_.first_column_ = @1.first_column - 1;
  $$->stmt_loc_.last_column_ = @1.last_column - 1;
}
| NAME_OB
{
  $$ = $1;
  //不转成大写,因为这是密码!
  $$->str_value_ = $$->raw_text_;
  $$->stmt_loc_.first_column_ = @1.first_column - 1;
  $$->stmt_loc_.last_column_ = @1.last_column - 1;
}
| unreserved_keyword
{
  get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
  $$->stmt_loc_.first_column_ = @1.first_column - 1;
  $$->stmt_loc_.last_column_ = @1.last_column - 1;
}
;

password_str:
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
DROP USER user_list opt_cascade
{
  ParseNode *drop_user_list = NULL;
  merge_nodes(drop_user_list, result, T_DROP_USER_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_USER, 2, drop_user_list, $4);
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

opt_cascade:
CASCADE
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_INT);
  $$->value_ = 1;
}
| /*empty*/
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_INT);
  $$->value_ = 2;
}
;

/*****************************************************************************
 *
 *	set password grammar
 *
 *****************************************************************************/
set_password_stmt:
SET PASSWORD COMP_EQ password_str
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 4, NULL, $4, need_enc_node, NULL);
}
| SET PASSWORD FOR user opt_host_name COMP_EQ password_str
{
  ParseNode *need_enc_node = NULL;
  ParseNode *user_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  malloc_non_terminal_node(user_node, result->malloc_pool_, T_USER_WITH_HOST_NAME, 2, $4, $5);
  need_enc_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 4, user_node, $7, need_enc_node, NULL);
}
| SET PASSWORD COMP_EQ PASSWORD '(' password ')'
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 4, NULL, $6, need_enc_node, NULL);
}
| SET PASSWORD FOR user opt_host_name COMP_EQ PASSWORD '(' password ')'
{
  ParseNode *need_enc_node = NULL;
  ParseNode *user_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  malloc_non_terminal_node(user_node, result->malloc_pool_, T_USER_WITH_HOST_NAME, 2, $4, $5);
  need_enc_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 4, user_node, $9, need_enc_node, NULL);
}
| ALTER USER user_with_host_name IDENTIFIED BY password
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 4, $3, $6, need_enc_node, NULL);
}
| ALTER USER user_with_host_name IDENTIFIED BY VALUES password_str
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 4, $3, $7, need_enc_node, NULL);
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
LOCK
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
 *  banliu.zyd: 只进行空实现
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
relation_factor lock_type %prec LOWER_PARENS
{
  (void)$$;
  (void)$1;
  (void)$2;
}
|
relation_factor relation_name lock_type
{
  (void)$$;
  (void)$1;
  (void)$2;
  (void)$3;
}
|
relation_factor AS relation_name lock_type
{
  (void)$$;
  (void)$1;
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
 *	create sequence grammar
 *
 *****************************************************************************/

create_sequence_stmt:
CREATE SEQUENCE relation_factor opt_sequence_option_list
{
  ParseNode *sequence_option = NULL;
  merge_nodes(sequence_option, result, T_SEQUENCE_OPTION_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_SEQUENCE, 2, $3, sequence_option);
}
;

opt_sequence_option_list:
sequence_option_list
{
  $$ = $1;
}
|
{
  $$ = NULL;
}
;

sequence_option_list:
sequence_option
{
  $$ = $1;
}
| sequence_option_list  sequence_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

sequence_option:
INCREMENT BY simple_num
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INCREMENT_BY, 1, $3);
}
|
START WITH simple_num
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_START_WITH, 1, $3);
}
|
MAXVALUE simple_num
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAXVALUE, 1, $2);
}
|
NOMAXVALUE
{
  malloc_terminal_node($$, result->malloc_pool_, T_NOMAXVALUE);
}
|
MINVALUE simple_num
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MINVALUE, 1, $2);
}
|
NOMINVALUE
{
  malloc_terminal_node($$, result->malloc_pool_, T_NOMINVALUE);
}
|
CYCLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_CYCLE);
}
|
NOCYCLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_NOCYCLE);
}
|
CACHE simple_num
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CACHE, 1, $2);
}
|
NOCACHE
{
  malloc_terminal_node($$, result->malloc_pool_, T_NOCACHE);
}
|
ORDER
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORDER);
}
|
NOORDER
{
  malloc_terminal_node($$, result->malloc_pool_, T_NOORDER);
}
;

simple_num:
'+' INTNUM %prec '+'
{
  $$ = $2;
}
|
'-' INTNUM %prec '-'
{
  $2->value_ = 0 - $2->value_;
  $$ = $2;
}
|
INTNUM
{
  $$ = $1;
}
| '+' DECIMAL_VAL %prec '+'
{
    $$ = $2;
}
| '-' DECIMAL_VAL %prec '-'
{
  int32_t len = $2->str_len_ + 2; /* 2 bytes for sign and terminator '\0' */
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
| DECIMAL_VAL
{
    $$ = $1;
}
;


drop_sequence_stmt:
DROP SEQUENCE relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_SEQUENCE, 1, $3);
}
;

alter_sequence_stmt:
ALTER SEQUENCE relation_factor opt_sequence_option_list
{
  ParseNode *sequence_option = NULL;
  merge_nodes(sequence_option, result, T_SEQUENCE_OPTION_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SEQUENCE, 2, $3, sequence_option);
}
;

/*****************************************************************************
 *
 *   create / drop database link grammer
 *
 ******************************************************************************/
create_dblink_stmt:
CREATE DATABASE LINK dblink CONNECT TO user tenant IDENTIFIED BY password ip_port opt_cluster
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_DBLINK, 6,
                           $4,    /* dblink name */
                           $7,    /* user name */
                           $8,    /* tenant name */
                           $11,   /* password */
                           $12,   /* host ip port*/
                           $13);  /* optional cluster */
}
;

drop_dblink_stmt:
DROP DATABASE LINK dblink
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_DBLINK, 1, $4);
}
;

dblink:
relation_name
{ $$ = $1; }
;

tenant:
USER_VARIABLE    /* USER_VARIABLE is '@xxxx', see sql_parser.l */
{ $$ = $1; }
;

opt_cluster:
CLUSTER relation_name
{ $$ = $2; }
|
{ $$ = NULL; }
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
 *  commit grammer
 *
 ******************************************************************************/
commit_stmt:
COMMIT opt_work
{
  (void)$2;
  malloc_terminal_node($$, result->malloc_pool_, T_COMMIT);
}
| COMMIT COMMENT STRING_VALUE
{
  (void)$3;
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
KILL bit_expr
{
  ParseNode *opt_node = NULL;
  malloc_terminal_node(opt_node, result->malloc_pool_, T_BOOL);
  opt_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_KILL, 2, opt_node, $2);
}
|
KILL CONNECTION bit_expr
{
  ParseNode *opt_node = NULL;
  malloc_terminal_node(opt_node, result->malloc_pool_, T_BOOL);
  opt_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_KILL, 2, opt_node, $3);
}
|
KILL QUERY bit_expr
{
  ParseNode *opt_node = NULL;
  malloc_terminal_node(opt_node, result->malloc_pool_, T_BOOL);
  opt_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_KILL, 2, opt_node, $3);
}
;

/*****************************************************************************
 *
 *	role grammar
 *
 *****************************************************************************/
create_role_stmt:
CREATE ROLE role opt_not_identified
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_ROLE, 2, $3, $4);
  $$->value_ = 1;
}
| CREATE ROLE role IDENTIFIED BY password
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_ROLE, 3,
                           $3,
                           need_enc_node,
                           $6);
  $$->value_ = 2;
}
| CREATE ROLE role IDENTIFIED BY VALUES password
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_ROLE, 3,
                           $3,
                           need_enc_node,
                           $7);
  $$->value_ = 3;
}
;

role_list:
role
{
  $$ = $1;
}
| role_list ',' role
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

role:
STRING_VALUE
{
  $$ = $1;
}
| NAME_OB
{
  $$ = $1;
}
| DBA
{
  make_name_node($$, result->malloc_pool_, "DBA");
}
| RESOURCE
{
  make_name_node($$, result->malloc_pool_, "RESOURCE");
}
| CONNECT
{
  make_name_node($$, result->malloc_pool_, "CONNECT");
}
| PUBLIC
{
  make_name_node($$, result->malloc_pool_, "PUBLIC");
}
/*
| unreserved_keyword
{
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
*/
;

drop_role_stmt:
DROP ROLE role
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_ROLE, 1, $3);
}
;

set_role_stmt:
SET ROLE default_role_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_ROLE, 1,
                           $3);
}
;

role_opt_identified_by_list:
role_opt_identified_by
{
  $$ = $1;
}
| role_opt_identified_by_list ',' role_opt_identified_by
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

role_opt_identified_by:
role
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_ROLE_PASSWORD, 1, $1);
  $$->value_ = 1;
}
| role IDENTIFIED BY password
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_ROLE_PASSWORD, 2, $1, $4);
  $$->value_ = 2;
}
;

opt_not_identified:
NOT IDENTIFIED
{ $$ = NULL; }
| { $$ = NULL; }


sys_and_obj_priv:
priv_type
{
  $$ = $1;
}
|
CREATE SESSION
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_SESSION;
}
| EXEMPT REDACTION POLICY
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_EXEMPT_RED_PLY;
}
| SYSDBA
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_SYSDBA;
}
| SYSOPER
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_SYSOPER;
}
| SYSBACKUP
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_SYSBACKUP;
}
| CREATE TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_TABLE;
}
| CREATE ANY TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_ANY_TABLE;
}
| ALTER ANY TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_ANY_TABLE;
}
| BACKUP ANY TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_BACKUP_ANY_TABLE;
}
| DROP ANY TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_ANY_TABLE;
}
| LOCK ANY TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_LOCK_ANY_TABLE;
}
| COMMENT ANY TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_COMMENT_ANY_TABLE;
}
| SELECT ANY TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_SELECT_ANY_TABLE;
}
| INSERT ANY TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_INSERT_ANY_TABLE;
}
| UPDATE ANY TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_UPDATE_ANY_TABLE;
}
| DELETE ANY TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DELETE_ANY_TABLE;
}
| FLASHBACK ANY TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_FLASHBACK_ANY_TABLE;
}
| CREATE ROLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_ROLE;
}
| DROP ANY ROLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_ANY_ROLE;
}
| GRANT ANY ROLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_GRANT_ANY_ROLE;
}
| ALTER ANY ROLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_ANY_ROLE;
}
| AUDIT ANY
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_AUDIT_ANY;
}
| GRANT ANY PRIVILEGE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_GRANT_ANY_PRIV;
}
| GRANT ANY OBJECT PRIVILEGE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_GRANT_ANY_OBJECT_PRIV;
}
| CREATE ANY INDEX
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_ANY_INDEX;
}
| ALTER ANY INDEX
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_ANY_INDEX;
}
| DROP ANY INDEX
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_ANY_INDEX;
}
| CREATE ANY VIEW
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_ANY_VIEW;
}
| DROP ANY VIEW
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_ANY_VIEW;
}
| CREATE VIEW
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_VIEW;
}
| SELECT ANY DICTIONARY
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_SELECT_ANY_DICTIONARY;
}
| CREATE PROCEDURE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_PROC;
}
| CREATE ANY PROCEDURE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_ANY_PROC;
}
| ALTER ANY PROCEDURE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_ANY_PROC;
}
| DROP ANY PROCEDURE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_ANY_PROC;
}
| EXECUTE ANY PROCEDURE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_EXEC_ANY_PROC;
}
| CREATE SYNONYM
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_SYN;
}
| CREATE ANY SYNONYM
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_ANY_SYN;
}
| DROP ANY SYNONYM
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_ANY_SYN;
}
| CREATE PUBLIC SYNONYM
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_PUB_SYN;
}
| DROP PUBLIC SYNONYM
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_PUB_SYN;
}
| CREATE SEQUENCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_SEQ;
}
| CREATE ANY SEQUENCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_ANY_SEQ;
}
| ALTER ANY SEQUENCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_ANY_SEQ;
}
| DROP ANY SEQUENCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_ANY_SEQ;
}
| SELECT ANY SEQUENCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_SELECT_ANY_SEQ;
}
| CREATE TRIGGER
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_TRIG;
}
| CREATE ANY TRIGGER
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_ANY_TRIG;
}
| ALTER ANY TRIGGER
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_ANY_TRIG;
}
| DROP ANY TRIGGER
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_ANY_TRIG;
}
| CREATE PROFILE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_PROFILE;
}
| ALTER PROFILE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_PROFILE;
}
| DROP PROFILE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_PROFILE;
}
| CREATE USER
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_USER;
}
| ALTER USER
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_USER;
}
| DROP USER
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_USER;
}
| CREATE TYPE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_TYPE;
}
| CREATE ANY TYPE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_ANY_TYPE;
}
| ALTER ANY TYPE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_ANY_TYPE;
}
| DROP ANY TYPE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_ANY_TYPE;
}
| EXECUTE ANY TYPE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_EXEC_ANY_TYPE;
}
| UNDER ANY TYPE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_UNDER_ANY_TYPE;
}
| PURGE DBA_RECYCLEBIN
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_PURGE_DBA_RECYCLEBIN;
}
| CREATE ANY OUTLINE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_ANY_OUTLINE;
}
| ALTER ANY OUTLINE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_ANY_OUTLINE;
}
| DROP ANY OUTLINE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_ANY_OUTLINE;
}
| SYSKM
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_SYSKM;
}
| CREATE TABLESPACE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_TABLESPACE;
}
| ALTER TABLESPACE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_TABLESPACE;
}
| DROP TABLESPACE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_TABLESPACE;
}
| SHOW PROCESS
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_SHOW_PROCESS;
}
| ALTER SYSTEM
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_SYSTEM;
}
| CREATE DATABASE LINK
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_DBLINK;
}
| CREATE PUBLIC DATABASE LINK
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_PUBLIC_DBLINK;
}
| DROP DATABASE LINK
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_DBLINK;
}
| ALTER SESSION
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_SESSION;
}
| ALTER DATABASE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_ALTER_DATABASE;
}
| CREATE ANY DIRECTORY
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_CREATE_ANY_DIRECTORY;
}
| DROP ANY DIRECTORY
{
  malloc_terminal_node($$, result->malloc_pool_, T_ORACLE_SYS_PRIV_TYPE);
  $$->value_ = PRIV_ID_DROP_ANY_DIRECTORY;
}

/*****************************************************************************
 *
 *	grant grammar
 *
 *****************************************************************************/
grant_stmt:
// Grant obj privileges
GRANT role_sys_obj_all_col_priv_list /*opt_column_list*/ ON obj_clause TO grant_user_list opt_grant_options
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
// Grant role or system privileges
| GRANT grant_system_privileges
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SYSTEM_GRANT, 1, $2);
}
;

grant_system_privileges:
role_sys_obj_all_col_priv_list TO grantee_clause opt_with_admin_option
{
  ParseNode *sys_priv_list = NULL;
  merge_nodes(sys_priv_list, result, T_LINK_NODE, $1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SYSTEM_GRANT, 3, sys_priv_list, $3, $4);
}
;

grantee_clause:
grant_user_list
{
  ParseNode* grant_user_list = NULL;
  merge_nodes(grant_user_list, result, T_LINK_NODE, $1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_USERS, 2, grant_user_list, NULL);
  $$->value_ = 1;
}
| grant_user IDENTIFIED BY password
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_GRANT, 2, $1, $4);
  $$->value_ = 2;
}
;

opt_with_admin_option:
WITH ADMIN OPTION
{
  malloc_terminal_node($$, result->malloc_pool_, T_WITH_ADMIN_OPTION);
}
| /*empty*/
{
  $$ = NULL;
}
;

// Combination of role, sys priv, obj priv, all priv
role_sys_obj_all_col_priv_list:
role_sys_obj_all_col_priv
{
  $$ = $1;
}
| role_sys_obj_all_col_priv_list ',' role_sys_obj_all_col_priv
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

role_sys_obj_all_col_priv:
role
{
  $$ = $1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORA_ROLE_TYPE, 1, $1);
}
| sys_and_obj_priv opt_column_list
{
  $$ = $1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORA_PRIV_TYPE, 2, $1, $2);
}
|
ALL opt_privilege opt_column_list
{
  ParseNode *all_priv = NULL;
  (void)$2;                 /* useless */
  malloc_terminal_node(all_priv, result->malloc_pool_, T_PRIV_TYPE);
  all_priv->value_ = OB_PRIV_ALL;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORA_PRIV_TYPE, 2, all_priv, $3);
}
;

/*obj_privileges:
priv_type_list
{
  $$ = $1;
}
| ALL opt_privilege
{
  (void)$2;
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_ALL;
}
;*/

/*priv_type_list:
priv_type
{
  $$ = $1;
}
| priv_type_list ',' priv_type
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;*/

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
/*| CREATE USER
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE_USER;
}*/
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
/* create view在oracle里面是系统权限，不是对象权限
| CREATE VIEW
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE_VIEW;
}*/
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
| REFERENCES
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_REFERENCES;
}
| EXECUTE
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_EXECUTE;
}
| FLASHBACK
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_FLASHBACK;
}
| READ
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_READ;
}
| WRITE
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_WRITE;
}
/*| CREATE SYNONYM
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE_SYNONYM;
}*/
| FILE_KEY
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_FILE;
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

obj_clause:
'*'
{
  /* means global obj_clause */
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
| DIRECTORY relation_name
{
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_PRIV_TYPE);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIV_LEVEL, 2, node, $2);
}
;

opt_grant_options:
WITH GRANT OPTION
{
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_PRIV_TYPE);
  node->value_ = OB_PRIV_GRANT;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORA_PRIV_TYPE, 2, node, NULL);
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
// revoke obj privileges
REVOKE role_sys_obj_all_col_priv_list ON obj_clause FROM user_list
{
  ParseNode *privileges_node = NULL;
  ParseNode *users_node = NULL;
  merge_nodes(privileges_node, result, T_PRIVILEGES, $2);
  merge_nodes(users_node, result, T_USERS, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_REVOKE,
                           3, privileges_node, $4, users_node);
}
// revoke role and system privileges
| REVOKE role_sys_obj_all_col_priv_list FROM grantee_clause
{
  ParseNode *sys_priv_list = NULL;
  merge_nodes(sys_priv_list, result, T_LINK_NODE, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SYSTEM_REVOKE, 2, sys_priv_list, $4);
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
  dup_expr_string($4, result, @4.first_column, @4.last_column);
}
;

stmt_name:
column_label
{ $$ = $1; }
;

preparable_stmt:
select_stmt { $$ = $1; }
| insert_stmt { $$ = $1; }
| merge_stmt { $$ = $1; }
| update_stmt { $$ = $1; }
| delete_stmt { $$ = $1; }
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
  bit_expr
  {
    if ($1 != NULL && $1->type_ == T_OBJ_ACCESS_REF && $1->num_child_ == 2 &&
      $1->children_[0] != NULL && $1->children_[1] == NULL && $1->children_[0]->num_child_ == 0) {
      ParseNode *obj_node = $1->children_[0];
      if (obj_node->value_ != 1 && nodename_equal(obj_node, "OFF", 3)) {
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
/* bit_expr中已经包含了OFF情形，不用在单独列出，否则会有规约冲突
| OFF
  {
    malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
    $$->str_value_ = "OFF";
    $$->str_len_ = 3;
  }
*/
| DEFAULT
  {
    malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT);
  }
;

var_and_val:
USER_VARIABLE to_or_eq bit_expr
{
  (void)($2);
  result->may_bool_value_ = false;
  malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
  $$->value_ = 2;
}
| USER_VARIABLE set_var_op bit_expr
{
  (void)($2);
  result->may_bool_value_ = false;
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
  result->may_bool_value_ = false;
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
  result->may_bool_value_ = false;
  malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
  $$->value_ = 2;
}
;

sys_var_and_val:
obj_access_ref_normal to_or_eq set_expr_or_default
{
  (void)($2);
  result->may_bool_value_ = false;
  malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
  $$->value_ = 2;
};

scope_or_scope_alias:
GLOBAL              { $$[0] = 1; }
| SESSION           { $$[0] = 2; }
| GLOBAL_ALIAS '.'  { $$[0] = 1; }
| SESSION_ALIAS '.' { $$[0] = 2; }
;

to_or_eq:
  TO      { result->may_bool_value_ = true; $$ = NULL; }
| COMP_EQ { result->may_bool_value_ = true; $$ = NULL; }
;

set_var_op:
  SET_VAR { result->may_bool_value_ = true; $$ = NULL; }
;

argument:
USER_VARIABLE
{ $$ = $1; }
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
 *      call stmt grammar
 *
 *****************************************************************************/
call_stmt:
    CALL routine_access_name  call_param_list
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SP_CALL_STMT, 2, $2, $3);
    }
;

call_param_list:
    '(' opt_func_param_list ')'
    {
      merge_nodes($$, result, T_SP_CPARAM_LIST, $2);
    }
;

routine_access_name:
  var_name '.' var_name '.' routine_name
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SP_ACCESS_NAME, 3, $1, $3, $5);
    }
  | var_name '.' routine_name %prec ';'
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SP_ACCESS_NAME, 3, NULL, $1, $3);
    }
  | routine_name %prec ';'
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SP_ACCESS_NAME, 3, NULL, NULL, $1);
    }
;

routine_name:
    NAME_OB
    {
      $$ = $1;
    }
  | oracle_unreserved_keyword
      {
        get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
      }
  | unreserved_keyword_normal
    {
      get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
    }
  | aggregate_function_keyword
    {
      get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
    }
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
 *	RENAME grammar
 *
 *****************************************************************************/

rename_table_stmt:
RENAME rename_table_actions
{
  merge_nodes($$, result, T_RENAME_TABLE, $2);
}
;

// oracle 模式下的 rename 一次只允许改一个对象的名字
// 这样写是为了能够复用 mysql 模式下的 resolver 代码
rename_table_actions:
rename_table_action
{
  $$ = $1;
}

rename_table_action:
relation_factor TO relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RENAME_TABLE_ACTION, 2, $1, $3);
}
;

/*****************************************************************************
 *
 *  ALTER INDEX grammar
 *
 *****************************************************************************/
alter_index_stmt:
ALTER INDEX relation_factor alter_index_actions
{
  ParseNode *index_actions = NULL;
  merge_nodes(index_actions, result, T_ALTER_TABLE_ACTION_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLE, 2, $3, index_actions);
  $$->value_ = 1;
}
;

alter_index_actions:
alter_index_action
{
  $$ = $1;
}
;

alter_index_action:
alter_index_option_oracle
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_INDEX_OPTION_ORACLE, 1, $1);
}
;

alter_index_option_oracle:
RENAME TO index_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_RENAME, 2, NULL, $3);
}
| parallel_option
{
  $$ = $1;
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
;

alter_table_action:
table_option_list_space_seperated
{
  merge_nodes($$, result, T_TABLE_OPTION_LIST, $1);
}
| SET table_option_list_space_seperated
{
  merge_nodes($$, result, T_TABLE_OPTION_LIST, $2);
}
| opt_alter_compress_option
{
  merge_nodes($$, result, T_TABLE_OPTION_LIST, $1);
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
| modify_partition_info
{
  ParseNode *modify_partitions = NULL;
  malloc_non_terminal_node(modify_partitions, result->malloc_pool_, T_ALTER_PARTITION_PARTITIONED, 1, $1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_OPTION, 1, modify_partitions);
}
| DROP CONSTRAINT constraint_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_CONSTRAINT, 1, $3);
  $$->value_ = 0;
}
;

alter_partition_option:
DROP PARTITION drop_partition_name_list
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_DROP, 1, $$);
}
| DROP PARTITION drop_partition_name_list UPDATE GLOBAL INDEXES
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_DROP, 2, $$, NULL);
}
|
add_range_or_list_partition
{
  merge_nodes($$, result, T_PARTITION_LIST, $1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_ADD, 1, $$);
  dup_string($$, result, @1.first_column, @1.last_column);
}
| SPLIT PARTITION relation_factor split_actions
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_SPLIT, 2, $3, $4);
}
| TRUNCATE PARTITION name_list %prec '|'
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_TRUNCATE, 1, $$);
}
| TRUNCATE PARTITION name_list %prec '|' UPDATE GLOBAL INDEXES
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_TRUNCATE, 2, $$, NULL);

}
| MODIFY PARTITION relation_factor add_range_or_list_subpartition
{
  merge_nodes($$, result, T_PARTITION_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SUBPARTITION_ADD, 2, $3, $$);
  dup_string($$, result, @1.first_column, @1.last_column);
}
| DROP SUBPARTITION drop_partition_name_list
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SUBPARTITION_DROP, 1, $$);
}
| DROP SUBPARTITION drop_partition_name_list UPDATE GLOBAL INDEXES
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SUBPARTITION_DROP, 2, $$, NULL);
}
| TRUNCATE SUBPARTITION name_list %prec '|'
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SUBPARTITION_TRUNCATE, 1, $$);
}
| TRUNCATE SUBPARTITION name_list %prec '|' UPDATE GLOBAL INDEXES
{
  merge_nodes($$, result, T_NAME_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SUBPARTITION_TRUNCATE, 2, $$, NULL);

}
;

drop_partition_name_list:
name_list %prec '|'
{
  $$ = $1;
}
|
'(' name_list ')'
{
  $$ = $2;
}
;
split_actions:
VALUES '(' list_expr ')' modify_special_partition
{
  merge_nodes($$, result, T_EXPR_LIST, $3);
  ParseNode *partition_type = NULL;
  malloc_terminal_node(partition_type, result->malloc_pool_, T_SPLIT_LIST);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SPLIT_ACTION, 3, $5, $$, partition_type);
}
| AT '(' range_expr_list ')' modify_special_partition
{
  merge_nodes($$, result, T_EXPR_LIST, $3);
  ParseNode *partition_type = NULL;
  malloc_terminal_node(partition_type, result->malloc_pool_, T_SPLIT_RANGE);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SPLIT_ACTION, 3, $5, $$, partition_type);
}
| split_range_partition
{
  merge_nodes($$, result, T_PARTITION_LIST, $1);
  ParseNode *partition_type = NULL;
  malloc_terminal_node(partition_type, result->malloc_pool_, T_SPLIT_RANGE);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SPLIT_ACTION, 3, $$, NULL, partition_type);
}
| split_list_partition
{
  merge_nodes($$, result, T_PARTITION_LIST, $1);
  ParseNode *partition_type = NULL;
  malloc_terminal_node(partition_type, result->malloc_pool_, T_SPLIT_LIST);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SPLIT_ACTION, 3, $$, NULL, partition_type);
}
;
add_range_or_list_partition:
ADD range_partition_list %prec '|'
{
  merge_nodes($$, result, T_RANGE_PARTITION_LIST, $2);
}
|
ADD list_partition_list %prec '|'
{
  merge_nodes($$, result, T_LIST_PARTITION_LIST, $2);
}
;
add_range_or_list_subpartition:
ADD range_subpartition_list %prec '|'
{
  merge_nodes($$, result, T_RANGE_SUBPARTITION_LIST, $2);
}
|
ADD list_subpartition_list %prec '|'
{
  merge_nodes($$, result, T_LIST_SUBPARTITION_LIST, $2);
}
;

modify_special_partition:
INTO opt_special_partition_list
{
  merge_nodes($$, result, T_PARTITION_LIST, $2);
}
| /*EMPTY*/
{
 $$ = NULL;
}
;
split_range_partition:
INTO opt_range_partition_list
{
  $$ = $2;
}
|
INTO '('range_partition_list ',' special_partition_list')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $3, $5);
  merge_nodes($$, result, T_PARTITION_LIST, $$);
}
;
split_list_partition:
INTO opt_list_partition_list
{
  $$ = $2;
}
|
INTO '('list_partition_list ',' special_partition_list')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $3, $5);
  merge_nodes($$, result, T_PARTITION_LIST, $$);
}
;

modify_partition_info:
MODIFY hash_partition_option
{
  $$ = $2;
}
| MODIFY list_partition_option
{
  $$ = $2;
}
| MODIFY range_partition_option
{
  $$ = $2;
}
;
tg_modify_partition_info:
MODIFY tg_hash_partition_option
{
  $$ = $2;
}
| MODIFY tg_range_partition_option
{
  $$ = $2;
}
| MODIFY tg_list_partition_option
{
  $$ = $2;
}
;

alter_index_option:
ADD out_of_line_constraint
{
  if (T_INDEX == $2->type_) {
    malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_ADD, 2, $2->children_[0], $2->children_[1]);
    $$->value_ = 1;
  } else {
    $$ = $2;
  }
}
/*| ADD out_of_line_ref_constraint
{
  (void)($2);
  $$ = NULL;
}*/
| ALTER INDEX index_name visibility_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_ALTER, 2, $3, $4);
}
| MODIFY CONSTRAINT constraint_name opt_rely_option opt_enable_option opt_validate_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MODIFY_CONSTRAINT_OPTION, 4, $3, $4, $5, $6);
}
| enable_option opt_validate_option CONSTRAINT constraint_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MODIFY_CONSTRAINT_OPTION, 4, $4, NULL, $1, $2);
}
;

opt_visibility_option:
visibility_option
{
  $$ = $1;
}
| /*empty*/
{
  $$ = NULL;
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
ADD column_definition
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ADD, 1, $2);
}
| ADD '(' column_definition_list ')'
{
  merge_nodes($$, result, T_COLUMN_ADD, $3);
}
| DROP COLUMN column_definition_ref opt_drop_behavior
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DROP, 1, $3);
  $$->value_ = $4[0];
}
| DROP '(' column_list ')'
{
  merge_nodes($$, result, T_COLUMN_DROP, $3);
}
| RENAME COLUMN column_definition_ref TO column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_RENAME, 2, $3, $5);
}
| MODIFY column_definition_opt_datatype
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_MODIFY, 1, $2);
}
| MODIFY '(' column_definition_opt_datatype_list ')'
{
  merge_nodes($$, result, T_COLUMN_MODIFY, $3);
}

/* we don't have table constraint, so ignore it */
;

alter_tablegroup_option:
DROP TABLEGROUP
{
  malloc_terminal_node($$, result->malloc_pool_, T_TABLEGROUP_DROP);
}
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

/*****************************************************************************
 *
 *	flashback grammar
 *
 *****************************************************************************/
flashback_stmt:
FLASHBACK TABLE relation_factors TO BEFORE DROP opt_flashback_rename_table
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FLASHBACK_TABLE_FROM_RECYCLEBIN, 2, $3, $7);
}
|
FLASHBACK database_key database_factor TO BEFORE DROP opt_flashback_rename_database
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FLASHBACK_DATABASE, 2, $3, $7);
}
|
FLASHBACK TENANT relation_name TO BEFORE DROP opt_flashback_rename_tenant
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FLASHBACK_TENANT, 2, $3, $7);
}
|
FLASHBACK TABLE relation_factors TO TIMESTAMP bit_expr
{
  ParseNode *from_list = NULL;
  merge_nodes(from_list, result, T_RELATION_FACTORS, $3);
  malloc_non_terminal_node($$, result->malloc_pool_,
                           T_FLASHBACK_TABLE_TO_TIMESTAMP, 2, from_list, $6);
}
|
FLASHBACK TABLE relation_factors TO SCN bit_expr
{
  ParseNode *from_list = NULL;
  merge_nodes(from_list, result, T_RELATION_FACTORS, $3);
  malloc_non_terminal_node($$, result->malloc_pool_,
                           T_FLASHBACK_TABLE_TO_SCN, 2, from_list, $6);
}
;

relation_factors:
relation_factor %prec HIGHER_THAN_TO
{
  $$ = $1;
}
| relation_factors ',' relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

opt_flashback_rename_table:
RENAME TO relation_factor
{
  $$ = $3;
}
| /*EMPTY*/  { $$ = NULL; }

opt_flashback_rename_database:
RENAME TO database_factor
{
  $$ = $3;
}
| /*EMPTY*/  { $$ = NULL; }

opt_flashback_rename_tenant:
RENAME TO relation_name
{
  $$ = $3;
}
| /*EMPTY*/  { $$ = NULL; }

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
;

shrink_space_stmt:
ALTER TABLE relation_factor SHRINK SPACE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OPTIMIZE_TABLE, 1, $3);
}
|
ALTER TENANT relation_name SHRINK SPACE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OPTIMIZE_TENANT, 1, $3);
}
|
ALTER TENANT ALL SHRINK SPACE
{
  malloc_terminal_node($$, result->malloc_pool_, T_OPTIMIZE_ALL);
}
;

/*****************************************************************************
 *
 *  AUDIT grammar
 *
 *  ref: https://docs.oracle.com/cd/E11882_01/server.112/e41084/statements_4007.htm
 *****************************************************************************/
audit_stmt:
audit_or_noaudit audit_clause
{
  ParseNode *audit_node = NULL;
  malloc_terminal_node(audit_node, result->malloc_pool_, T_INT);
  audit_node->value_ = $1[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_AUDIT, 2, audit_node, $2);
}
;


audit_or_noaudit:
AUDIT
{
  $$[0] = 1;
}
| NOAUDIT
{
  $$[0] = 0;
}
;


audit_clause:
audit_operation_clause auditing_on_clause op_audit_tail_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_AUDIT_OBJECT, 3, $1, $2, $3);
}
| audit_operation_clause op_audit_tail_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_AUDIT_STMT, 3, $1, NULL, $2);
}
| audit_operation_clause auditing_by_user_clause op_audit_tail_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_AUDIT_STMT, 3, $1, $2, $3);
}
;


audit_operation_clause:
audit_all_shortcut_list
{
  merge_nodes($$, result, T_LINK_NODE, $1);
}
| ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_ALL);
}
| ALL STATEMENTS
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_ALL_STMT);
}
;

audit_all_shortcut_list:
audit_all_shortcut
{
  $$ = $1;
}
| audit_all_shortcut_list ',' audit_all_shortcut
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;


auditing_on_clause:
ON normal_relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_AUDIT, 1, $2);
}
| ON DEFAULT
{
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT);
}
;

auditing_by_user_clause:
BY user_list
{
  merge_nodes($$, result, T_LINK_NODE, $2);
}
;

op_audit_tail_clause:
/*EMPTY*/
{
  $$ = NULL;
}
| audit_by_session_access_option
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = $1[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_AUDIT, 2, $$, NULL);
}
| audit_whenever_option
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = $1[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_AUDIT, 2, NULL, $$);
}
| audit_by_session_access_option audit_whenever_option
{
  ParseNode *by_node = NULL;
  ParseNode *when_node = NULL;
  malloc_terminal_node(by_node, result->malloc_pool_, T_INT);
  by_node->value_ = $1[0];
  malloc_terminal_node(when_node, result->malloc_pool_, T_INT);
  when_node->value_ = $2[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_AUDIT, 2, by_node, when_node);
}
;


audit_by_session_access_option:
/*BY SESSION  {  $$[0] = 1;} | */
BY ACCESS {  $$[0] = 2;}
;

audit_whenever_option:
WHENEVER NOT SUCCESSFUL { $$[0] = 1;}
| WHENEVER SUCCESSFUL   { $$[0] = 2;}
;


audit_all_shortcut:
ALTER SYSTEM
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_ALTER_SYSTEM);
}
| CLUSTER
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_CLUSTER);
}
| CONTEXT
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_CONTEXT);
}
| DATABASE LINK
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_DBLINK);
}
| DIRECTORY
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_DIRECTORY);
}
| MATERIALIZED VIEW
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_MATERIALIZED_VIEW);
}
| NOT EXISTS
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_NOT_EXIST);
}
| OUTLINE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_OUTLINE);
}
| PROCEDURE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_PROCEDURE);
}
| PROFILE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_PROFILE);
}
| PUBLIC DATABASE LINK
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_PUBLIC_DBLINK);
}
| PUBLIC SYNONYM
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_PUBLIC_SYNONYM);
}
| ROLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_ROLE);
}
| SEQUENCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_SEQUENCE);
}
| SESSION
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_SESSION);
}
| SYNONYM
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_SYNONYM);
}
| SYSTEM AUDIT
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_SYSTEM_AUDIT);
}
| SYSTEM GRANT
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_SYSTEM_GRANT);
}
| TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_TABLE);
}
| TABLESPACE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_TABLESPACE);
}
| TRIGGER
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_TRIGGER);
}
| TYPE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_TYPE);
}
| USER
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_USER);
}
| VIEW
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_VIEW);
}
| ALTER SEQUENCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_ALTER_SEQUENCE);
}
| ALTER TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_ALTER_TABLE);
}
| COMMENT TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_COMMENT_TABLE);
}
| DELETE TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_DELETE_TABLE);
}
| EXECUTE PROCEDURE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_EXECUTE_PROCEDURE);
}
| GRANT PROCEDURE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_GRANT_PROCEDURE);
}
| GRANT SEQUENCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_GRANT_SEQUENCE);
}
| GRANT TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_GRANT_TABLE);
}
| GRANT TYPE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_GRANT_TYPE);
}
| INSERT TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_INSERT_TABLE);
}
| SELECT SEQUENCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_SELECT_SEQUENCE);
}
| SELECT TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_SELECT_TABLE);
}
| UPDATE TABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_UPDATE_TABLE);
}
| ALTER
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_ALTER);
}
| AUDIT
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_AUDIT);
}
| COMMENT
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_COMMENT);
}
| DELETE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_DELETE);
}
| EXECUTE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_EXECUTE);
}
| FLASHBACK
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_FLASHBACK);
}
| GRANT
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_GRANT);
}
| INDEX
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_INDEX);
}
| INSERT
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_INSERT);
}
/*| LOCK
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_LOCK);
}*/
| RENAME
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_RENAME);
}
| SELECT
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_SELECT);
}
| UPDATE
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_UPDATE);
}
;



/*****************************************************************************
 *
 *	ALTER SYSTEM grammar
 *
 *****************************************************************************/
alter_system_stmt:
ALTER SYSTEM BOOTSTRAP opt_cluster_type server_info_list opt_primary_rootservice_list
{
  ParseNode *server_list = NULL;
  merge_nodes(server_list, result, T_SERVER_INFO_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_BOOTSTRAP, 3, server_list, $4, $6);
}
|
ALTER SYSTEM FLUSH cache_type CACHE flush_scope
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FLUSH_CACHE, 5, $4, NULL, NULL, NULL, $6);
  $$->reserved_ = 1;
}
|
ALTER SYSTEM FLUSH KVCACHE opt_tenant_name opt_cache_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FLUSH_KVCACHE, 2, $5, $6);
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
ALTER SYSTEM SET alter_system_set_clause_list
{
  merge_nodes($$, result, T_ALTER_SYSTEM_SET, $4);
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

alter_system_set_clause_list:
alter_system_set_clause
{
  $$ = $1;
}
| alter_system_set_clause_list alter_system_set_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

alter_system_set_clause:
set_system_parameter_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SYSTEM_SET_PARAMETER, 1, $1);
}
;

set_system_parameter_clause:
var_name COMP_EQ bit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
  $$->value_ = 1;
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

opt_cluster_type:
CLUSTER partition_role
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CLUSTER_TYPE, 1, $2);
}
| /* EMPTY */
{
  $$ = NULL;
}
;

opt_primary_rootservice_list:
PRIMARY_ROOTSERVICE_LIST STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_ROOTSERVICE_LIST, 1, $2);
}
| /* EMPTY */
{
  $$ = NULL;
}
;

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
  $$->value_ = 1; // 1 表示global, 即flush 租户涉及的所有server上对应的cache
}
| /* empty */
{
  malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
  $$->value_ = 0; // 0 表示flush租户当前server上对应的cache
}

opt_zone_list:
ZONE COMP_EQ zone_list
{
  merge_nodes($$, result, T_ZONE_LIST, $3);
}
| /*empty*/ {$$=NULL;}

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
}
;

ip_port:
SERVER opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_IP_PORT, 1, $3);
}
|
HOST STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_IP_PORT, 1, $2);
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

/*****************************************************************************
 *
 *	ALTER SESSION grammar
 *
 *****************************************************************************/
alter_session_stmt:
ALTER SESSION SET CURRENT_SCHEMA COMP_EQ current_schema
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_DATABASE, 1, $6);
}
|
ALTER SESSION SET ISOLATION_LEVEL COMP_EQ session_isolation_level
{
  // construct the parser node tree as same as set_transaction_stmt exactly,
  // then we can reuse the functions from ObSetTransactionResolver::resolve().
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;  // 2 is SESSION, see the defination of opt_scope.
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANSACTION, 2, $$, $6);
}
|
ALTER SESSION SET alter_session_set_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SESSION_SET, 1, $4);
}
|
ALTER SESSION FORCE var_name_of_forced_module PARALLEL INTNUM
{
  if (OB_UNLIKELY($6->value_ < 1)) {
    yyerror(&@1, result, "value for PARALLEL must be greater than 0!\n");
    YYERROR;
  }

  // dop val node
  ParseNode *var_value = NULL;
  malloc_terminal_node(var_value, result->malloc_pool_, T_INT);
  var_value->value_ = $6->value_;

  // variable = dop value
  ParseNode *set_var_val = NULL;
  malloc_non_terminal_node(set_var_val, result->malloc_pool_, T_VAR_VAL, 2, $4, $6);
  set_var_val->value_ = 2;

  ParseNode *set_param_list = NULL;
  malloc_non_terminal_node(set_param_list, result->malloc_pool_, T_ALTER_SESSION_SET_PARAMETER_LIST, 1, set_var_val);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SESSION_SET, 1, set_param_list);
}
|
ALTER SESSION switch_option var_name_of_module
{
  ParseNode *var_value = NULL;
  malloc_terminal_node(var_value, result->malloc_pool_, T_INT);
  var_value->value_ = (T_ENABLE == $3->type_) ? 1 : 0;

  ParseNode *set_var_val = NULL;
  malloc_non_terminal_node(set_var_val, result->malloc_pool_, T_VAR_VAL, 2, $4, var_value);
  set_var_val->value_ = 1;

  ParseNode *set_param_list = NULL;
  malloc_non_terminal_node(set_param_list, result->malloc_pool_, T_ALTER_SESSION_SET_PARAMETER_LIST, 1, set_var_val);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SESSION_SET, 1, set_param_list);
};


var_name_of_forced_module:
PARALLEL DML
{
  const char* var_name = "_FORCE_PARALLEL_DML_DOP";
  malloc_terminal_node($$, result->malloc_pool_, T_IDENT);
  $$->str_value_ = var_name;
  $$->str_len_ = strlen(var_name);
}
| PARALLEL QUERY
{
  const char* var_name = "_FORCE_PARALLEL_QUERY_DOP";
  malloc_terminal_node($$, result->malloc_pool_, T_IDENT);
  $$->str_value_ = var_name;
  $$->str_len_ = strlen(var_name);
}
;

var_name_of_module:
PARALLEL DML
{
  const char* var_name = "_ENABLE_PARALLEL_DML";
  malloc_terminal_node($$, result->malloc_pool_, T_IDENT);
  $$->str_value_ = var_name;
  $$->str_len_ = strlen(var_name);
}
| PARALLEL QUERY
{
  const char* var_name = "_ENABLE_PARALLEL_QUERY";
  malloc_terminal_node($$, result->malloc_pool_, T_IDENT);
  $$->str_value_ = var_name;
  $$->str_len_ = strlen(var_name);
}
;

switch_option:
ENABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_ENABLE);
}
| DISABLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISABLE);
};

session_isolation_level:
isolation_level
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANSACTION_CHARACTERISTICS, 2, NULL, $1);
};

alter_session_set_clause:
set_system_parameter_clause_list
{
  merge_nodes($$, result, T_ALTER_SESSION_SET_PARAMETER_LIST, $1);
}
;

set_system_parameter_clause_list:
set_system_parameter_clause
{
  $$ = $1;
}
| set_system_parameter_clause_list set_system_parameter_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

current_schema:
relation_name
{
  $$ = $1;
}
;

/*****************************************************************************
 *
 *	COMMENT ON grammar
 *
 *****************************************************************************/
set_comment_stmt:
COMMENT ON TABLE normal_relation_factor IS STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_TABLE_COMMENT, 2,
      $4,   /*[db].table_name*/
      $6    /*string_value*/);
}
|
COMMENT ON COLUMN column_definition_ref IS STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_COLUMN_COMMENT, 2,
      $4,   /*[db].table.column*/
      $6    /*comment_string*/);
}
;

/*****************************************************************************
 *
 *	tablespace grammar
 *
 *****************************************************************************/
create_tablespace_stmt:
CREATE TABLESPACE tablespace permanent_tablespace
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLESPACE, 2, $3, $4);
}
;

drop_tablespace_stmt:
DROP TABLESPACE tablespace
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_TABLESPACE, 1, $3);
}
;

//TODO: 测试关键字和非保留关键字
tablespace:
NAME_OB
{
  $$ = $1;
}
;

alter_tablespace_stmt:
ALTER TABLESPACE tablespace alter_tablespace_actions
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLESPACE, 2, $3, $4);
}
;

alter_tablespace_actions:
alter_tablespace_action
{
  $$ = $1;
}
| alter_tablespace_action ',' alter_tablespace_action
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

alter_tablespace_action:
permanent_tablespace_option
{
  merge_nodes($$, result, T_TABLESPACE_OPTION_LIST, $1);
}
;

permanent_tablespace:
permanent_tablespace_options
{
  ParseNode *tablespace_options= NULL;
  merge_nodes(tablespace_options, result, T_LINK_NODE, $1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PERMANENT_TABLESPACE, 1, $1);
}
| // EMPTY
{
  $$ = NULL;
}
;

permanent_tablespace_options:
permanent_tablespace_option
{
  $$ = $1;
}
| permanent_tablespace_options ',' permanent_tablespace_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

permanent_tablespace_option:
ENCRYPTION USING STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ENCRYPTION, 1, $3);
}
;

/*****************************************************************************
 *
 *	profile grammar
 *
 *****************************************************************************/
create_profile_stmt:
CREATE PROFILE profile_name LIMIT password_parameters
{
  ParseNode *pwd_param_list = NULL;
  merge_nodes(pwd_param_list, result, T_PROFILE_PARAM_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_PROFILE, 2, $3, pwd_param_list);
}
/*| CREATE PROFILE profile_name LIMIT resource_parameters
{
}
*/
;

alter_profile_stmt:
ALTER PROFILE profile_name LIMIT password_parameters
{
  ParseNode *pwd_param_list = NULL;
  merge_nodes(pwd_param_list, result, T_PROFILE_PARAM_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PROFILE, 2, $3, pwd_param_list);
}
;

drop_profile_stmt:
DROP PROFILE profile_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_PROFILE, 2, $3, NULL);
}
;

profile_name:
NAME_OB
{ $$ = $1;}
| unreserved_keyword
{
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
| DEFAULT
{
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

password_parameters:
password_parameter
{
  $$ = $1;
}
| password_parameters password_parameter
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

password_parameter:
password_parameter_type password_parameter_value
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PROFILE_PAIR, 2, $1, $2);
}
;


verify_function_name:
relation_name
{
  $$ = $1;
}
| NULLX
{
  $$ = $1;
}
;

password_parameter_value:
number_literal
{
  $$ = $1;
}
/*
| UNLIMITED
{
  malloc_terminal_node($$, result->malloc_pool_, T_PROFILE_UNLIMITED);
}
*/
| verify_function_name
{
  if ($1->value_ != 1 && nodename_equal($1, "UNLIMITED", 9)) {
    malloc_terminal_node($$, result->malloc_pool_, T_PROFILE_UNLIMITED);
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_PROFILE_VERIFY_FUNCTION_NAME, 1, $1);
  }
}
| DEFAULT
{
  malloc_terminal_node($$, result->malloc_pool_, T_PROFILE_DEFAULT);
}
;

password_parameter_type:
FAILED_LOGIN_ATTEMPTS
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
}
| PASSWORD_LOCK_TIME
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
| PASSWORD_VERIFY_FUNCTION
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
;

user_profile:
PROFILE profile_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USER_PROFILE, 1, $2);
}
;

opt_profile:
user_profile
{
  $$ = $1;
}
|
{
  $$ = NULL;
}
;

opt_default_tables_space:
DEFAULT TABLESPACE tablespace
{
  $$ = $3;
}
|
{
  $$ = NULL;
}
;

/*****************************************************************************
METHOD_OPT grammar ==> used for GatherTableStats
METHOD_OPT - The value controls column statistics collection and histogram creation. It accepts
either of the following options, or both in combination:
  FOR ALL [INDEXED | HIDDEN] COLUMNS [size_clause]
  FOR COLUMNS [size clause] column [size_clause] [,column [size_clause]...]
size_clause is defined as size_clause := SIZE {integer | REPEAT | AUTO | SKEWONLY}
column is defined as column := column_name | extension name | extension
- integer : Number of histogram buckets. Must be in the range [1,2048].
- REPEAT : Collects histograms only on the columns that already have histograms
- AUTO : Oracle determines the columns to collect histograms based on data distribution and the workload of the columns
- SKEWONLY : Oracle determines the columns to collect histograms based on the data distribution of the columns
- column_name : name of a column
- extension : can be either a column group in the format of (column_name, colume_name [, ...]) or an expression
The default is FOR ALL COLUMNS SIZE AUTO.
https://blogs.oracle.com/optimizer/how-does-the-methodopt-parameter-work
******************************************************************************/

method_opt:
method_list
{
  merge_nodes($$, result, T_METHOD_OPT_LIST, $1);
}
;

method_list:
method
{
  $$ = $1;
}
| method_list method
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

method:
for_all
{
  $$ = $1;
}
| for_columns
{
  $$ = $1;
}
;

for_all:
FOR ALL opt_indexed_hiddden COLUMNS opt_size_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOR_ALL, 2,
                           $3, /*opt_indexed_hiddden*/
                           $5  /*opt_size_clause*/);
}
;

opt_indexed_hiddden:
/*empty*/
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
}
| INDEXED
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
}
| HIDDEN
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
}
;

opt_size_clause:
/*empty*/
{
  $$ = NULL;
}
| size_clause
{
  $$ = $1;
}

size_clause:
SIZE AUTO
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
  $$->reserved_ = 0;
}
| SIZE REPEAT
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
  $$->reserved_ = 0;
}
| SIZE SKEWONLY
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
  $$->reserved_ = 0;
}
| SIZE number_literal
{
  $$ = $2;
  $$->reserved_ = 1; /*mark size integer*/
}
;

for_columns:
FOR COLUMNS for_columns_list %prec LOWER_PARENS
{
  merge_nodes($$, result, T_FOR_COLUMNS, $3);
}
| FOR COLUMNS %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOR_COLUMN_ITEM, 2,
                           NULL, /*column_clause*/
                           NULL /*opt_size_clause*/);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOR_COLUMNS, 1, $$);
}
;

for_columns_list:
for_columns_item
{
  $$ = $1;
}
| for_columns_list for_columns_item
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| for_columns_list ',' for_columns_item
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

for_columns_item:
column_clause %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOR_COLUMN_ITEM, 2,
                           $1, /*column_clause*/
                           NULL/*opt_size_clause*/);
}
| size_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOR_COLUMN_ITEM, 2,
                           NULL, /*column_clause*/
                           $1  /*opt_size_clause*/);
}
| column_clause size_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOR_COLUMN_ITEM, 2,
                           $1, /*column_clause*/
                           $2  /*opt_size_clause*/);
}
;

column_clause:
column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, $1, NULL);
}
| extension
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXTENSION, 2, $1, NULL);
}
/*TODO @jiangxiu.wt
| extension relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXTENSION, 2, $1, $2);
}
*/

extension:
'(' column_name_list ')'
{
  ParseNode *col_list= NULL;
  merge_nodes(col_list, result, T_COLUMN_LIST, $2);
  $$ = col_list;
}
//TODO @jiangxiu.wt
//| bit_expr
//{
//  $$ = $1;
//}
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
}
| SET GLOBAL TRANSACTION transaction_characteristics
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANSACTION, 2, $$, $4);
}
| SET SESSION TRANSACTION transaction_characteristics
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANSACTION, 2, $$, $4);
}
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

/*===========================================================
 *
 *	Name classification
 *
 *===========================================================*/

var_name:
    NAME_OB
    {
      $$ = $1;
      $$->value_ = 1;//区别添加双引号之后去除关键字属性的字符串
    }
  | oracle_unreserved_keyword
    {
      get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
    }
  | unreserved_keyword_normal
    {
      get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
    }
  | aggregate_function_keyword
    {
      get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
    }
;

column_name:
NAME_OB
{
  $$ = $1;
  $$->value_ = 1;//区别添加双引号之后去除关键字属性的字符串
}
| unreserved_keyword
{
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
  setup_token_pos_info($$, @1.first_column - 1, $$->str_len_);
}
| ROWID
{
  make_name_node($$, result->malloc_pool_, "ROWID");
  setup_token_pos_info($$, @1.first_column - 1, $$->str_len_);
}
;

relation_name:
NAME_OB
{
  $$ = $1;
  $$->value_ = 1;//区别加双引号去除关键字属性的字符
}
| unreserved_keyword
{
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
  setup_token_pos_info($$, @1.first_column - 1, $$->str_len_);
}
;

exists_function_name:
EXISTS
{
  make_name_node($$, result->malloc_pool_, "exists");
}
;

function_name:
NAME_OB
{
  $$ = $1;
  $$->value_ = 1;//区别加双引号去除关键字属性的字符
}
| oracle_unreserved_keyword
{
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
| unreserved_keyword_normal
{
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
| oracle_pl_non_reserved_words
{
  if (!result->pl_parse_info_.is_pl_parse_) {
    yyerror(&@1, result, "row used to alias_name only support in pl mode\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$ = $1;
  }
}
| PRIOR
{
  make_name_node($$, result->malloc_pool_, "PRIOR");
}
;

column_label:
NAME_OB
{ $$ = $1; }
| unreserved_keyword
{
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

keystore_name:
NAME_OB
{ $$ = $1; }
| unreserved_keyword
{
  get_oracle_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

date_unit:
YEAR
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_YEAR;
  $$->is_hidden_const_ = 1;
}
| MONTH
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_MONTH;
  $$->is_hidden_const_ = 1;
}
| DAY
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_DAY;
  $$->is_hidden_const_ = 1;
}
| HOUR
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_HOUR;
  $$->is_hidden_const_ = 1;
}
| MINUTE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_MINUTE;
  $$->is_hidden_const_ = 1;
}
| SECOND
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_SECOND;
  $$->is_hidden_const_ = 1;
}
;

timezone_unit:
TIMEZONE_HOUR
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_TIMEZONE_HOUR;
  $$->is_hidden_const_ = 1;
}
| TIMEZONE_MINUTE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_TIMEZONE_MINUTE;
  $$->is_hidden_const_ = 1;
}
| TIMEZONE_REGION
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_TIMEZONE_REGION;
  $$->is_hidden_const_ = 1;
}
| TIMEZONE_ABBR
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_TIMEZONE_ABBR;
  $$->is_hidden_const_ = 1;
}
;

date_unit_for_extract:
date_unit
{
  $$ = $1;
}
|
timezone_unit
{
  $$ = $1;
}
;

unreserved_keyword:
oracle_unreserved_keyword { $$=$1; }
| unreserved_keyword_normal { $$=$1; }
| aggregate_function_keyword { $$=$1; }
;

/*why need this aggregate_function_keyword, because compatible oracle pl udf aggr, oracle pl udf aggr
  can use unreserved keyword name in window function, if we don't this, we can't use unreserved keyword name
  in oracle pl udf aggr with window function, eg:
  select MAXVALUE(c1) over (partition by c1) from t1; ==> MAXVALUE is pl udf aggr.
*/
aggregate_function_keyword:
COUNT %prec LOWER_PARENS
|       MAX %prec LOWER_PARENS
|       MIN %prec LOWER_PARENS
|       SUM %prec LOWER_PARENS
|       AVG %prec LOWER_PARENS
|       APPROX_COUNT_DISTINCT %prec LOWER_PARENS
|       APPROX_COUNT_DISTINCT_SYNOPSIS %prec LOWER_PARENS
|       APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE %prec LOWER_PARENS
|       MEDIAN %prec LOWER_PARENS
|       STDDEV %prec LOWER_PARENS
|       VARIANCE %prec LOWER_PARENS
|       STDDEV_POP %prec LOWER_PARENS
|       STDDEV_SAMP %prec LOWER_PARENS
|       LISTAGG %prec LOWER_PARENS
|       RANK %prec LOWER_PARENS
|       DENSE_RANK %prec LOWER_PARENS
|       PERCENT_RANK %prec LOWER_PARENS
|       ROW_NUMBER %prec LOWER_PARENS
|       NTILE %prec LOWER_PARENS
|       CUME_DIST %prec LOWER_PARENS
|       FIRST_VALUE %prec LOWER_PARENS
|       LAST_VALUE %prec LOWER_PARENS
|       LEAD %prec LOWER_PARENS
|       LAG %prec LOWER_PARENS
|       NTH_VALUE %prec LOWER_PARENS
|       RATIO_TO_REPORT %prec LOWER_PARENS
|       CORR %prec LOWER_PARENS
|       COVAR_POP %prec LOWER_PARENS
|       COVAR_SAMP %prec LOWER_PARENS
|       VAR_POP %prec LOWER_PARENS
|       VAR_SAMP %prec LOWER_PARENS
|       REGR_SLOPE %prec LOWER_PARENS
|       REGR_INTERCEPT %prec LOWER_PARENS
|       REGR_COUNT %prec LOWER_PARENS
|       REGR_R2 %prec LOWER_PARENS
|       REGR_AVGX %prec LOWER_PARENS
|       REGR_AVGY %prec LOWER_PARENS
|       REGR_SXX %prec LOWER_PARENS
|       REGR_SYY %prec LOWER_PARENS
|       REGR_SXY %prec LOWER_PARENS
|       PERCENTILE_CONT %prec LOWER_PARENS
|       PERCENTILE_DISC %prec LOWER_PARENS
|       WM_CONCAT %prec LOWER_PARENS
|       TOP_K_FRE_HIST %prec LOWER_PARENS
|       HYBRID_HIST %prec LOWER_PARENS
;

oracle_unreserved_keyword:
        ADMIN
|       AFTER
|       ALLOCATE
|       ANALYZE
|       ARCHIVE
|       ARCHIVELOG
|       AUTHORIZATION
|       BACKUP
|       BECOME
|       BEFORE
|       BEGI { if (result->pl_parse_info_.is_pl_parse_expr_) {
                 yyerror(NULL, result, "begin in pl is reserved word\n");
                 YYABORT_PARSE_SQL_ERROR;
                }
              }
|       BLOCK
|       BODY
|       CACHE
|       CANCEL
|       CASCADE
|       CHANGE
|       CHARACTER
|       CHECKPOINT
|       CLOSE
|       COBOL
|       COMMIT
|       COMPILE
|       CONSTRAINT %prec LOWER_PARENS
|       CONSTRAINTS
|       CONTENTS
|       CONTINUE
|       CONTROLFILE
|       CURSOR %prec LOWER_PARENS
|       CYCLE
|       DATABASE
|       DATAFILE
|       DBA
|       DEC
|       DECLARE
|       DISABLE
|       DISMOUNT
|       DOP
|       DOUBLE
|       DUMP
|       EACH
|       ENABLE
|       END
|       ESCAPE
|       EVENTS
|       EXCEPT
|       EXCEPTIONS
|       EXEC
|       EXECUTE
|       EXPLAIN
|       EXTENT
|       EXTERNALLY
|       FETCH
|       FLUSH
|       FORCE
|       FOREIGN
|       FORTRAN
|       FOUND
|       FREELIST
|       FREELISTS
|       FUNCTION
|       GO
|       GOTO
|       GROUPS
|       INCLUDING
|       INDICATOR
|       INITRANS
|       INSTANCE
|       INT
|       KEY
|       LANGUAGE
|       LAYER
|       LINK
|       LISTS
|       LOGFILE
|       MANAGE
|       MANUAL
|       MAXDATAFILES
|       MAXINSTANCES
|       MAXLOGFILES
|       MAXLOGHISTORY
|       MAXLOGMEMBERS
|       MAXTRANS
|       MAXVALUE
|       MINEXTENTS
|       MINVALUE
|       MODULE
|       MOUNT
|       NEW
|       NEXT
|       NOARCHIVELOG
|       NOCACHE
|       NOCYCLE %prec LOWER_THAN_COMP
|       NOMAXVALUE
|       NOMINVALUE
|       NONE
|       NOORDER
|       NORESETLOGS
|       NOSORT
|       NUMERIC
|       OFF
|       OLD
|       ONLY
|       OPEN
|       OPTIMAL
|       OWN
|       PACKAGE_KEY
|       PARALLEL
|       NOPARALLEL
|       PCTINCREASE
|       PCTUSED
|       PLAN
|       PLI
|       PRECISION
|       PRIMARY
|       PRIVATE
|       PROCEDURE
|       PROFILE
|       QUOTA
|       READ
|       REAL
|       RECOVER
|       REFERENCES
|       REFERENCING
|       RESETLOGS
|       RESTRICTED
|       REUSE
|       ROLE
|       ROLES
|       ROLLBACK
|       SAVEPOINT
|       SCHEMA
|       SCN
|       SECTION
|       SEGMENT
|       SEQUENCE
|       SHARED
|       SNAPSHOT
|       SOME
|       SORT
|       SQL %prec LOWER_THAN_COMP
|       SQLCODE
|       SQLERROR
|       SQLSTATE
|       STATEMENT_ID
|       STATISTICS
|       STOP
|       STORAGE
|       SWITCH
|       SYSTEM
|       TABLES
|       TABLESPACE
|       TEMPORARY
|       THREAD
|       TIME %prec LOWER_PARENS
|       TRACING
|       TRANSACTION
|       TRIGGERS
|       TRUNCATE
|       UNDER
|       UNLIMITED
|       UNTIL
|       USE
|       USING
|       WHEN
|       WORK
|       WRITE
;


unreserved_keyword_normal:
ACCOUNT
|       ACCESSIBLE
|       ACTION
|       ACTIVE
|       ADDDATE
|       ADMINISTER
|       AGGREGATE
|       AGAINST
|       ALGORITHM
|       ALWAYS
|       ANALYSE
|       ASCII %prec LOWER_PARENS
|       ASENSITIVE
|       AT
|       AUTHORS
|       AUTO
|       AUTOEXTEND_SIZE
|       AVG_ROW_LENGTH
|       BASE
|       BASELINE
|       BASELINE_ID
|       BASIC
|       BALANCE
|       BINARY
|       BINARY_DOUBLE
|       BINARY_DOUBLE_INFINITY
|       BINARY_DOUBLE_NAN
|       BINARY_FLOAT
|       BINARY_FLOAT_INFINITY
|       BINARY_FLOAT_NAN
|       BINDING
|       BINLOG
|       BIT
|       BLOB
|       BLOCK_SIZE
|       BLOCK_INDEX
|       BLOOM_FILTER
|       BOOL
|       BOOLEAN
|       BOOTSTRAP
|       BOTH %prec LOWER_THAN_COMP
|       BTREE
|       BULK
|       BULK_EXCEPTIONS
|       BULK_ROWCOUNT
|       BYTE
|       BREADTH
|       CALC_PARTITION_ID %prec LOWER_PARENS
|       CALL
|       CASCADED
|       CAST %prec LOWER_PARENS
|       CATALOG_NAME
|       CHAIN
|       CHANGED
|       CHARSET
|       CHAR_CS
|       CHECKSUM
|       CIPHER
|       CLASS_ORIGIN
|       CLEAN
|       CLEAR
|       CLIENT
|       CLOB
|       CLOG
|       CLUSTER_ID
|       CLUSTER_NAME
|       COALESCE
|       CODE
|       COLLATE
|       COLLATION
|       COLLECT
|       COLUMN_FORMAT
|       COLUMN_NAME
|       COLUMN_OUTER_JOIN_SYMBOL
|       COLUMN_STAT
|       COLUMNS
|       COMMITTED
|       COMPACT
|       COMPLETION
|       COMPRESSED
|       COMPRESSION
|       COMPUTE
|       CONCURRENT
|       CONNECTION %prec KILL_EXPR
|       CONNECT_BY_ISCYCLE
|       CONNECT_BY_ISLEAF
|       CONSISTENT
|       CONSISTENT_MODE
|       CONSTRAINT_CATALOG
|       CONSTRAINT_NAME
|       CONSTRAINT_SCHEMA
|       CONTAINS
|       CONTEXT
|       CONTRIBUTORS
|       COPY
|       CPU
|       CREATE_TIMESTAMP
|       CROSS
|       CUBE %prec LOWER_PARENS
|       CURRENT_USER
|       CURRENT_SCHEMA
|       CURRENT_DATE
|       CURRENT_TIMESTAMP %prec LOWER_PARENS
|       DATA
|       DATABASES
|       DATABASE_ID
|       DATA_TABLE_ID
|       DATE_ADD
|       DATE_SUB
|       DATETIME
|       DAY
|       DAY_HOUR
|       DAY_MICROSECOND
|       DAY_MINUTE
|       DAY_SECOND
|       DBA_RECYCLEBIN
|       DBTIMEZONE
|       DEALLOCATE
|       DEFAULT_AUTH
|       DEFINER
|       DELAY
|       DELAYED
|       DELAY_KEY_WRITE
|       DEPTH
|       DES_KEY_FILE
|       DESCRIBE
|       DESTINATION
|       DETERMINISTIC
|       DIAGNOSTICS
|       DICTIONARY
|       DIRECTORY
|       DISCARD
|       DISK
|       DML
|       DISTINCTROW
|       DIV
|       DO
|       DUMPFILE
|       DUPLICATE
|       DUPLICATE_SCOPE
|       DYNAMIC
|       DEFAULT_TABLEGROUP
|       E
|       EFFECTIVE
|       ELSEIF
|       ENCLOSED
|       ENCRYPTION
|       ENDS
|       ENGINE_
|       ENGINES
|       ENUM
|       ERROR_CODE
|       ERROR_P
|       ERROR_INDEX
|       ERRORS
|       ESCAPED
|       ESTIMATE
|       EVENT
|       EVERY
|       EXCHANGE
|       EXCLUDE
|       EXEMPT
|       EXIT
|       EXPANSION
|       EXPIRE
|       EXPIRE_INFO
|       EXPORT
|       EXTENDED
|       EXTENDED_NOADDR
|       EXTENT_SIZE
|       EXTRACT %prec LOWER_PARENS
|       FAILED_LOGIN_ATTEMPTS
|       FAST
|       FAULTS
|       FIELDS
|       FILE_ID
|       FINAL_COUNT
|       FIRST %prec LOWER_PARENS
|       FIXED
|       FLASHBACK
|       FLOAT4
|       FLOAT8
|       FOLLOWER
|       FOLLOWING
|       FORMAT
|       FREEZE
|       FREQUENCY
|       FROZEN
|       FULL
|       G
|       GENERAL
|       GENERATED
|       GEOMETRY
|       GEOMETRYCOLLECTION
|       GET
|       GET_FORMAT
|       GLOBAL
|       GLOBAL_ALIAS %prec LOWER_PARENS
|       GRANTS
|       GROUPING %prec LOWER_PARENS
|       GTS
|       HANDLER
|       HASH
|       HELP
|       HIGH
|       HIGH_PRIORITY
|       HOUR_MICROSECOND
|       HOUR_MINUTE
|       HOUR_SECOND
|       HOST
|       HOSTS
|       HOUR
|       ID
|       IDC
|       IDENTITY
|       IF
|       IFIGNORE
|       IGNORE
|       IGNORE_SERVER_IDS
|       ILOG
|       ILOGCACHE
|       IMPORT
|       INDEXES
|       INDEX_TABLE_ID
|       INCR
|       INCLUDE
|       INFO
|       INFILE
|       INFINITE_VALUE
|       INITIAL_SIZE
|       INNER
|       INNER_PARSE
|       INOUT
|       INSENSITIVE
|       INSERT_METHOD
|       INSTALL
|       INT1
|       INT2
|       INT3
|       INT4
|       INT8
|       INTERVAL
|       INVOKER
|       IO
|       IO_AFTER_GTIDS
|       IO_BEFORE_GTIDS
|       IO_THREAD
|       IPC
|       ISNULL %prec LOWER_PARENS
|       ISOLATION
|       ISSUER
|       ITERATE
|       JOB
|       JOIN %prec LOWER_JOIN
|       JSON
|       K
|       KEY_BLOCK_SIZE
|       KEYS
|       KEYSTORE
|       KEY_VERSION
|       KILL
|       KEEP
|       KVCACHE
|       LAST %prec LOWER_PARENS
|       LEADER
|       LEADING %prec LOWER_THAN_COMP
|       LEAVE
|       LEAVES
|       LEFT
|       LESS
|       LIMIT
|       LINEAR
|       LINES
|       LINESTRING
|       LIST
|       LNNVL %prec LOWER_PARENS
|       LOAD
|       LOCAL
|       LOCALITY
|       LOCALTIMESTAMP %prec LOWER_PARENS
|       LOCK_
|       LOCKED
|       LOCKS
|       LOGONLY_REPLICA_NUM
|       LOG
|       LOGS
|       LONGBLOB
|       LONGTEXT
|       LOOP
|       LOW
|       LOW_PRIORITY
|       ISOPEN
|       ISOLATION_LEVEL
|       M
|       MAJOR
|       MANAGEMENT
|       MASTER
|       MASTER_AUTO_POSITION
|       MASTER_BIND
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
|       MASTER_SSL_VERIFY_SERVER_CERT
|       MASTER_USER
|       MATCH %prec LOWER_PARENS
|       MATCHED
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
|       MAX_USED_PART_ID
|       MAX_USER_CONNECTIONS
|       MEDIUM
|       MEDIUMBLOB
|       MEDIUMINT
|       MEDIUMTEXT
|       MEMORY
|       MEMSTORE_PERCENT
|       MEMTABLE
|       MERGE
|       MESSAGE_TEXT
|       META
|       MICROSECOND
|       MIDDLEINT
|       MIGRATE
|		    MIGRATION
|       MIN_CPU
|       MIN_IOPS
|       MIN_MEMORY
|       MINOR
|       MIN_ROWS
|       MINUTE
|       MINUTE_MICROSECOND
|       MINUTE_SECOND
|       MOD
|       MODIFIES
|       MONTH %prec LOWER_PARENS
|       MOVE
|       MOVEMENT
|       MULTILINESTRING
|       MULTIPOINT
|       MULTIPOLYGON
|       MUTEX
|       MYSQL_ERRNO
|       NAME
|       NAMES
|       NAN_VALUE
|       NATIONAL
|       NATURAL
|       NCHAR
|       NCHAR_CS
|       NDB
|       NDBCLUSTER
|       NO
|       NODEGROUP
|       NOLOGGING
|       NOW
|       NO_WAIT
|       NO_WRITE_TO_BINLOG
|       NULLS
|       NVARCHAR2
|       OBJECT
|       OCCUR
|       OFFSET
|       OLD_PASSWORD
|       OLD_KEY
|       OLTP
|       OVER
|       ONE
|       ONE_SHOT
|       OPTIONS
|       OPTIMIZE
|       OPTIONALLY
|       ORA_ROWSCN
|       ORIG_DEFAULT
|       OUT
|       OUTER
|       OUTFILE
|       OUTLINE
|       OWNER
|       P
|       PACK_KEYS
|       PAGE
|       PARAMETERS
|       PARAM_ASSIGN_OPERATOR
|       PARSER
|       PARTIAL
|       PARTITION
|       PARTITION_ID
|       PARTITIONING
|       PARTITIONS
|       PASSWORD %prec LOWER_THAN_COMP
|       PASSWORD_LOCK_TIME
|       PASSWORD_VERIFY_FUNCTION
|       PAUSE
|       PERCENTAGE
|       PHASE
|       PLANREGRESS
|       PLUGIN
|       PLUGIN_DIR
|       PLUGINS
|       PIVOT
|       POINT
|       POLICY
|       POLYGON
|       POOL
|       PORT
|       POSITION %prec LOWER_PARENS
|       PRECEDING
|       PREPARE
|       PRESERVE
|       PREV
|       PRIMARY_ZONE
|       PRIVILEGE
|       PROCESS
|       PROCESSLIST
|       PROFILES
|       PROGRESSIVE_MERGE_NUM
|       PROXY
|       PURGE
|       QUARTER
|       QUERY %prec KILL_EXPR
|       QUICK
|       RANGE
|       READ_WRITE
|       READS
|       READ_ONLY
|       REBUILD
|       RECURSIVE
|       RECYCLE
|       RECYCLEBIN
|       REDACTION
|       REDO_BUFFER_SIZE
|       REDOFILE
|       REDUNDANT
|       REFRESH
|       REGEXP_LIKE
|       REGION
|       REJECT
|       RELAY
|       RELAYLOG
|       RELAY_LOG_FILE
|       RELAY_LOG_POS
|       RELAY_THREAD
|       RELEASE
|       RELOAD
|       REMOVE
|       REORGANIZE
|       REPAIR
|       REPEAT
|       REPEATABLE
|       REPLACE
|       REPLICA
|       REPLICA_NUM
|       REPLICA_TYPE
|       REPLICATION
|       REPORT
|       REQUIRE
|       RESET
|       RESIGNAL
|       RESOURCE_POOL_LIST
|       RESPECT
|       RESTART
|       RESTORE
|       RESTRICT
|       RESUME
|       RETURN
|       RETURNED_SQLSTATE
|       RETURNING
|       RETURNS
|       REVERSE
| 	    REWRITE_MERGE_VERSION
|       REMOTE_OSS
|       RLIKE
|       RIGHT
|       ROLLUP %prec LOWER_PARENS
|       ROOT
|       ROOTTABLE
|       ROOTSERVICE
|       ROOTSERVICE_LIST
|       ROUTINE
|       ROWCOUNT
|       ROW_COUNT
|       ROW_FORMAT
|       RTREE
|       RUN
|       SAMPLE
|       SCHEDULE
|       SCHEMAS
|       SCHEMA_NAME
|       SCOPE
|       SEARCH
|       SECOND
|       SECOND_MICROSECOND
|       SECURITY
|       SEED
|       SENSITIVE
|       SEPARATOR
|       SERIAL
|       SERIALIZABLE
|       SERVER
|       SERVER_IP
|       SERVER_PORT
|       SERVER_TYPE
|       SESSION_ALIAS %prec LOWER_PARENS
|       SESSION_USER
|       SESSIONTIMEZONE
|       SET_MASTER_CLUSTER
|       SET_SLAVE_CLUSTER
|       SET_TP
|       SETS
|       SHRINK
|       SHOW
|       SHUTDOWN
|       SIBLINGS
|       SIGNAL
|       SIGNED
|       SIMPLE
|       SKIP
|       SLAVE
|       SLOW
|       SOCKET
|       SONAME
|       SOUNDS
|       SOURCE
|       SPACE
|       SPATIAL
|       SPECIFIC
|       SPFILE
|       SPLIT
|       SQLEXCEPTION
|       SQLWARNING
|       SQL_BIG_RESULT
|       SQL_CALC_FOUND_ROW
|       SQL_SMALL_RESULT
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
|       SSL
|       STRAIGHT_JOIN
|       STARTING
|       STARTS
|       STATS_AUTO_RECALC
|       STATS_PERSISTENT
|       STATS_SAMPLE_PAGES
|       STATUS
|       STATEMENTS
|       STORAGE_FORMAT_VERSION
|       STORAGE_FORMAT_WORK_VERSION
|       STORED
|       STORING
|       STRONG
|       SUBCLASS_ORIGIN
|       SUBDATE
|       SUBJECT
|       SUBPARTITION
|       SUBPARTITIONS
|       SUBSTR
|       SUPER
|       SUSPEND
|       SWAPS
|       SWITCHES
|       SYSTEM_USER
|       SYSTIMESTAMP %prec LOWER_PARENS
|       SYSBACKUP
|       SYSDBA
|       SYSKM
|       SYSOPER
|       SYS_CONNECT_BY_PATH %prec LOWER_PARENS
|       T
|       TABLEGROUP
|       TABLE_CHECKSUM
|       TABLE_MODE
|       TABLEGROUPS
|       TABLEGROUP_ID
|       TABLE_ID
|       TABLE_NAME
|       TABLET
|       TABLET_SIZE
|       TABLET_MAX_SIZE
|		    TASK
|       TEMPLATE
|       TEMPTABLE
|       TENANT
|       TERMINATED
|       TEXT
|       THAN
|       TIMESTAMP
|       TIMESTAMPADD
|       TIMESTAMPDIFF
|       TIMEZONE_ABBR
|       TIMEZONE_HOUR
|       TIMEZONE_MINUTE
|       TIMEZONE_REGION
|       TIME_ZONE_INFO
|       TINYBLOB
|       TINYTEXT
|       TP_NAME
|       TP_NO
|       TRACE
|       TRADITIONAL
|       TRAILING %prec LOWER_THAN_COMP
|       TRIM %prec LOWER_PARENS
|       TRANSLATE %prec LOWER_PARENS
|       TYPE
|       TYPES
|       UNCOMMITTED
|       UNDEFINED
|       UNDO
|       UNDO_BUFFER_SIZE
|       UNDOFILE
|       UNICODE
|       UNKNOWN
|       UNINSTALL
|       UNIT
|       UNIT_NUM
|       UNLOCK
|       UNLOCKED
|       UNUSUAL
|       UNPIVOT
|       UPGRADE
|       UROWID
|       USAGE
|       USE_BLOOM_FILTER
|       USE_FRM
|       USER_RESOURCES
|       UTC_DATE
|       UTC_TIMESTAMP %prec LOWER_THAN_COMP
|       UNBOUNDED
|       VALID
|       VARIABLES
|       VERBOSE
|       MATERIALIZED
|       WAIT
|       WARNINGS
|       WEEK
|       WEIGHT_STRING
|       WMSYS %prec LOWER_PARENS
|       WRAPPER
|       X509
|       XA
|       XML
|       YEAR %prec LOWER_THAN_COMP
|       ZONE
|       ZONE_LIST
|       ZONE_TYPE
|       LOCATION
|       VARYING
|       VIRTUAL
|       VISIBLE
|       INVISIBLE
|       RELY
|       NORELY
|       NOVALIDATE
|       WITHIN
|       WEAK
|       WHILE
|       XOR
|       YEAR_MONTH
|       ZEROFILL
|       PERCENT
|       TIES
|       MEMBER
|       SUBMULTISET
|       EMPTY
|       A
|       THROTTLE
|       PRIORITY
|       RT
|       NETWORK
|       LOGICAL_READS
|       QUEUE_TIME
|       HIDDEN
|       INDEXED
|       SKEWONLY
;

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

void obsql_oracle_parser_fatal_error(yyconst char *msg, yyscan_t yyscanner)
{
  if (OB_LIKELY(NULL != msg)) {
    (void)fprintf(stderr, "FATAL ERROR:%s\n", msg);
  }
  ParseResult *p = obsql_oracle_yyget_extra(yyscanner);
  longjmp(*p->jmp_buf_, 1);//the secord param must be non-zero value
}

/* 用于将一条多语句SQL按照分号切分成多个独立SQL */
int obsql_oracle_multi_fast_parse(ParseResult *p)
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
      token = obsql_oracle_yylex(&yylval, &yylloc, p->yyscan_info_);
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

int obsql_oracle_fast_parse(ParseResult *p)
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
    while ((0 == ret) && END_P != (token = obsql_oracle_yylex(&yylval, &yylloc, p->yyscan_info_))) {
      switch (token) {
        case ERROR:
        case PARSER_SYNTAX_ERROR: {
          ret = -1;
          break;
        }
        default: {
          //将匹配的内容直接拷贝
          if (p->is_ignore_token_) {
            if (SELECT_HINT_BEGIN == token ||
                UPDATE_HINT_BEGIN == token ||
                DELETE_HINT_BEGIN == token ||
                INSERT_HINT_BEGIN == token ||
                REPLACE_HINT_BEGIN == token ||
                MERGE_HINT_BEGIN == token ||
                LOAD_DATA_HINT_BEGIN == token
                /* token == INSERT_HINT_BEGIN */) {
              const char *hint_begin = obsql_oracle_yyget_text(p->yyscan_info_);
              const char *slash = memchr(hint_begin,
                                         '/',
                                         obsql_oracle_yyget_leng(p->yyscan_info_));
              int length = slash - hint_begin;
              memmove(p->no_param_sql_ + p->no_param_sql_len_, hint_begin, slash - hint_begin);
              p->no_param_sql_len_ += length;
              p->token_num_++;
            } else if (token == HINT_END) {
              p->is_ignore_token_ = false;
            } else {/*do nothing*/}
          } else {
            memmove(p->no_param_sql_ + p->no_param_sql_len_,
                    obsql_oracle_yyget_text(p->yyscan_info_),
                    obsql_oracle_yyget_leng(p->yyscan_info_));
            p->no_param_sql_len_ += obsql_oracle_yyget_leng(p->yyscan_info_);
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
