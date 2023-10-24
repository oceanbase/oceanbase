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
  int32_t ival[2]; //ival[0]表示value, ival[1]表示fast parse在对应的该node及其子node可识别的常量个数
 }

%{
#include "../../../src/sql/parser/sql_parser_mysql_mode_lex.h"
#include "../../../src/sql/parser/sql_parser_base.h"

extern void obsql_oracle_parse_fatal_error(int32_t errcode, yyscan_t yyscanner, yyconst char *msg, ...);

#define GEN_EXPLAN_STMT(no_use, explain_stmt, explain_type, display_type, stmt, into_table, set_statement_id) \
  (void)(no_use); \
  ParseNode *type_node = NULL; \
  ParseNode *display_node = NULL; \
  if (0 != explain_type) { \
    malloc_terminal_node(type_node, result->malloc_pool_, explain_type); \
  } \
  if (0 != display_type) { \
    malloc_terminal_node(display_node, result->malloc_pool_, display_type); \
  } \
  malloc_non_terminal_node(explain_stmt, result->malloc_pool_, T_EXPLAIN, 5, \
                           type_node, display_node, stmt, into_table, set_statement_id);

%}

%destructor {destroy_tree($$);}<node>
%destructor {oceanbase::common::ob_free($$);}<str_value_>

%token <node> NAME_OB
%token <node> STRING_VALUE
%token <node> NATIONAL_LITERAL
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
%token <node> REMAP_TABLE_VAL
%token <node> OUTLINE_DEFAULT_TOKEN/*use for outline parser to just filter hint of query_sql*/

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

%nonassoc BASIC OUTLINE EXTENDED EXTENDED_NOADDR PARTITIONS PLANREGRESS
%nonassoc PRETTY PRETTY_COLOR
%nonassoc   KILL_EXPR
%nonassoc   CONNECTION QUERY
%nonassoc   LOWER_COMMA
%nonassoc   REMAP
%nonassoc   ',' WITH
%left	UNION EXCEPT MINUS
%left	INTERSECT
%left   JOIN CROSS LEFT FULL RIGHT INNER WINDOW
%left   SET_VAR
%left	OR OR_OP
%left	XOR
%left	AND AND_OP
%left   BETWEEN CASE WHEN THEN ELSE
%nonassoc LOWER_THAN_COMP
%left   COMP_EQ COM P_NSEQ COMP_GE COMP_GT COMP_LE COMP_LT COMP_NE IS LIKE IN REGEXP SOUNDS
%nonassoc STRING_VALUE
%right  ESCAPE /*for conflict for escape*/
%left   '|'
%left   '&'
%left   SHIFT_LEFT SHIFT_RIGHT
%left   JSON_EXTRACT JSON_EXTRACT_UNQUOTED MEMBER
%left   '+' '-'
%left   '*' '/' '%' MOD DIV POW
%left   '^'
%nonassoc LOWER_THAN_NEG SAMPLE/* for simple_expr conflict*/
%left CNNOP
%left   NEG '~'
%nonassoc LOWER_PARENS
//%nonassoc STRING_VALUE
%left   '(' ')'
%nonassoc SQL_CACHE SQL_NO_CACHE CHARSET DATABASE_ID REPLICA_NUM/*for shift/reduce conflict between opt_query_expresion_option_list and SQL_CACHE*/
%nonassoc HIGHER_PARENS TRANSACTION SIZE AUTO SKEWONLY /*for simple_expr conflict*/
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
%nonassoc NESTED
%nonassoc PATH
%nonassoc LOWER_THAN_BY_ACCESS_SESSION

%token ERROR /*used internal*/
%token PARSER_SYNTAX_ERROR /*used internal*/

%token/*for hint*/
// hint structure
BEGIN_OUTLINE_DATA END_OUTLINE_DATA OPTIMIZER_FEATURES_ENABLE QB_NAME
// global hint
FROZEN_VERSION TOPK QUERY_TIMEOUT READ_CONSISTENCY LOG_LEVEL USE_PLAN_CACHE
TRACE_LOG LOAD_BATCH_SIZE TRANS_PARAM OPT_PARAM OB_DDL_SCHEMA_VERSION FORCE_REFRESH_LOCATION_CACHE
DISABLE_PARALLEL_DML ENABLE_PARALLEL_DML MONITOR NO_PARALLEL CURSOR_SHARING_EXACT
MAX_CONCURRENT DOP TRACING NO_QUERY_TRANSFORMATION NO_COST_BASED_QUERY_TRANSFORMATION
// transform hint
NO_REWRITE MERGE_HINT NO_MERGE_HINT NO_EXPAND USE_CONCAT UNNEST NO_UNNEST
PLACE_GROUP_BY NO_PLACE_GROUP_BY INLINE MATERIALIZE SEMI_TO_INNER NO_SEMI_TO_INNER
PRED_DEDUCE NO_PRED_DEDUCE PUSH_PRED_CTE NO_PUSH_PRED_CTE
REPLACE_CONST NO_REPLACE_CONST SIMPLIFY_ORDER_BY NO_SIMPLIFY_ORDER_BY
SIMPLIFY_GROUP_BY NO_SIMPLIFY_GROUP_BY SIMPLIFY_DISTINCT NO_SIMPLIFY_DISTINCT
SIMPLIFY_WINFUNC NO_SIMPLIFY_WINFUNC SIMPLIFY_EXPR NO_SIMPLIFY_EXPR SIMPLIFY_LIMIT
NO_SIMPLIFY_LIMIT SIMPLIFY_SUBQUERY NO_SIMPLIFY_SUBQUERY FAST_MINMAX NO_FAST_MINMAX
PROJECT_PRUNE NO_PROJECT_PRUNE SIMPLIFY_SET NO_SIMPLIFY_SET OUTER_TO_INNER NO_OUTER_TO_INNER
COALESCE_SQ NO_COALESCE_SQ COUNT_TO_EXISTS NO_COUNT_TO_EXISTS LEFT_TO_ANTI NO_LEFT_TO_ANTI
ELIMINATE_JOIN NO_ELIMINATE_JOIN PUSH_LIMIT NO_PUSH_LIMIT PULLUP_EXPR NO_PULLUP_EXPR
WIN_MAGIC NO_WIN_MAGIC AGGR_FIRST_UNNEST NO_AGGR_FIRST_UNNEST JOIN_FIRST_UNNEST NO_JOIN_FIRST_UNNEST
// optimize hint
INDEX_HINT FULL_HINT NO_INDEX_HINT USE_DAS_HINT NO_USE_DAS_HINT
INDEX_SS_HINT INDEX_SS_ASC_HINT INDEX_SS_DESC_HINT
LEADING_HINT ORDERED
USE_NL USE_MERGE USE_HASH NO_USE_HASH NO_USE_MERGE NO_USE_NL
USE_NL_MATERIALIZATION NO_USE_NL_MATERIALIZATION
USE_HASH_AGGREGATION NO_USE_HASH_AGGREGATION
PARTITION_SORT NO_PARTITION_SORT
USE_LATE_MATERIALIZATION NO_USE_LATE_MATERIALIZATION
PX_JOIN_FILTER NO_PX_JOIN_FILTER PX_PART_JOIN_FILTER NO_PX_PART_JOIN_FILTER
PQ_MAP PQ_DISTRIBUTE PQ_DISTRIBUTE_WINDOW PQ_SET RANDOM_LOCAL BROADCAST BC2HOST LIST
GBY_PUSHDOWN NO_GBY_PUSHDOWN
USE_HASH_DISTINCT NO_USE_HASH_DISTINCT
DISTINCT_PUSHDOWN NO_DISTINCT_PUSHDOWN
USE_HASH_SET NO_USE_HASH_SET
USE_DISTRIBUTED_DML NO_USE_DISTRIBUTED_DML
PUSHDOWN
// direct load data hint
DIRECT
// hint related to optimizer statistics
APPEND NO_GATHER_OPTIMIZER_STATISTICS GATHER_OPTIMIZER_STATISTICS DBMS_STATS FLASHBACK_READ_TX_UNCOMMITTED
// optimizer dynamic sampling hint
DYNAMIC_SAMPLING
// other
NEG_SIGN

%token /*can not be relation name*/
_BINARY _UTF8 _UTF8MB4 _GBK _UTF16 _GB18030 _GB18030_2022 _LATIN1 CNNOP
SELECT_HINT_BEGIN UPDATE_HINT_BEGIN DELETE_HINT_BEGIN INSERT_HINT_BEGIN REPLACE_HINT_BEGIN HINT_HINT_BEGIN HINT_END
LOAD_DATA_HINT_BEGIN CREATE_HINT_BEGIN
END_P SET_VAR DELIMITER

/*reserved keyword*/
%token <reserved_keyword>
/*
 * MySQL 5.7 Reserved Keywords（mysql5.7一共有235个保留关键字，这里兼容mysql5.7的229个，NULL关键字在
   外面已经定义，所以这里注释了，而DEC、NUMERIC关键字在lex文件会转为NUMBER，同时另外4个关键字老版本一直没有定义为保
   留关键字，出于兼容性考虑, 这里也不添加到其中，注释了，因此这6个关键字（DEC，NUMERIC，FALSE，LOCK，NUMERIC，
   OPTIMIZER_COSTS， TRUE）在ob里面是可以使用的，和mysql有区别）
 * https://dev.mysql.com/doc/refman/5.7/en/keywords.html
 * 注意！！！非特殊情况，禁止将关键字放到该区域
 * */
        ACCESSIBLE ADD ALL ALTER ANALYZE AND AS ASC ASENSITIVE
        BEFORE BETWEEN BIGINT BINARY BLOB BOTH BY
        CALL CASCADE CASE CHANGE CHAR CHARACTER CHECK COLLATE COLUMN CONDITION CONSTRAINT CONTINUE
        CONVERT CREATE CROSS CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER CURSOR
        DATABASE DATABASES DAY_HOUR DAY_MICROSECOND DAY_MINUTE DAY_SECOND /*DEC*/ DECLARE DECIMAL DEFAULT
        DELAYED DELETE DESC DESCRIBE DETERMINISTIC DISTINCT DISTINCTROW DIV DOUBLE DROP DUAL
        EACH ELSE ELSEIF ENCLOSED ESCAPED EXISTS EXIT EXPLAIN
        /*FALSE*/ FETCH FLOAT FLOAT4 FLOAT8 FOR FORCE FOREIGN FROM FULLTEXT
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
        NATURAL NOT NO_WRITE_TO_BINLOG /*NULL*/ NUMERIC
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

/*OB 特有的保留关键字*/
        TABLEGROUP

%token <non_reserved_keyword>
        ACCESS ACCOUNT ACTION ACTIVE ADDDATE AFTER AGAINST AGGREGATE ALGORITHM ALL_META ALL_USER ALWAYS ANALYSE ANY
        APPROX_COUNT_DISTINCT APPROX_COUNT_DISTINCT_SYNOPSIS APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE
        ARBITRATION ASCII AT AUTHORS AUTO AUTOEXTEND_SIZE AUTO_INCREMENT AUTO_INCREMENT_MODE AVG AVG_ROW_LENGTH
        ACTIVATE AVAILABILITY ARCHIVELOG AUDIT

        BACKUP BACKUP_COPIES BALANCE BANDWIDTH BASE BASELINE BASELINE_ID BASIC BEGI BINDING SHARDING BINLOG BIT BIT_AND
        BIT_OR BIT_XOR BLOCK BLOCK_INDEX BLOCK_SIZE BLOOM_FILTER BOOL BOOLEAN BOOTSTRAP BTREE BYTE
        BREADTH BUCKETS BISON_LIST BACKUPSET BACKED BACKUPPIECE BACKUP_BACKUP_DEST BACKUPROUND
        BADFILE

        CACHE CALIBRATION CALIBRATION_INFO CANCEL CASCADED CAST CATALOG_NAME CHAIN CHANGED CHARSET CHECKSUM CHECKPOINT CHUNK CIPHER
        CLASS_ORIGIN CLEAN CLEAR CLIENT CLOG CLOSE CLUSTER CLUSTER_ID CLUSTER_NAME COALESCE COLUMN_STAT
        CODE COLLATION COLUMN_FORMAT COLUMN_NAME COLUMNS COMMENT COMMIT COMMITTED COMPACT COMPLETION
        COMPRESSED COMPRESSION COMPUTE CONCURRENT CONDENSED CONNECTION CONSISTENT CONSISTENT_MODE CONSTRAINT_CATALOG
        CONSTRAINT_NAME CONSTRAINT_SCHEMA CONTAINS CONTEXT CONTRIBUTORS COPY COUNT CPU CREATE_TIMESTAMP
        CTXCAT CTX_ID CUBE CURDATE CURRENT STACKED CURTIME CURSOR_NAME CUME_DIST CYCLE CALC_PARTITION_ID CONNECT

        DAG DATA DATAFILE DATA_TABLE_ID DATE DATE_ADD DATE_SUB DATETIME DAY DEALLOCATE DECRYPTION
        DEFAULT_AUTH DEFINER DELAY DELAY_KEY_WRITE DEPTH DES_KEY_FILE DENSE_RANK DESCRIPTION DESTINATION DIAGNOSTICS
        DIRECTORY DISABLE DISCARD DISK DISKGROUP DO DUMP DUMPFILE DUPLICATE DUPLICATE_SCOPE DYNAMIC
        DATABASE_ID DEFAULT_TABLEGROUP DISCONNECT

        EFFECTIVE EMPTY ENABLE ENABLE_ARBITRATION_SERVICE ENABLE_EXTENDED_ROWID ENCRYPTED ENCRYPTION END ENDS ENFORCED ENGINE_ ENGINES ENUM ENTITY ERROR_CODE ERROR_P ERRORS ESTIMATE
        ESCAPE EVENT EVENTS EVERY EXCHANGE EXECUTE EXPANSION EXPIRE EXPIRE_INFO EXPORT OUTLINE EXTENDED
        EXTENDED_NOADDR EXTENT_SIZE EXTRACT EXCEPT EXPIRED ENCODING EMPTY_FIELD_AS_NULL EXTERNAL

        FAILOVER FAST FAULTS FIELDS FILEX FINAL_COUNT FIRST FIRST_VALUE FIXED FLUSH FOLLOWER FORMAT
        FOUND FREEZE FREQUENCY FUNCTION FOLLOWING FLASHBACK FULL FRAGMENTATION FROZEN FILE_ID
        FIELD_OPTIONALLY_ENCLOSED_BY FIELD_DELIMITER

        GENERAL GEOMETRY GEOMCOLLECTION GEOMETRYCOLLECTION GET_FORMAT GLOBAL GRANTS GROUP_CONCAT GROUPING GTS
        GLOBAL_NAME GLOBAL_ALIAS

        HANDLER HASH HELP HISTOGRAM HOST HOSTS HOUR HIDDEN HYBRID_HIST

        ID IDC IDENTIFIED IGNORE_SERVER_IDS ILOG IMPORT INCR INDEXES INDEX_TABLE_ID INFO INITIAL_SIZE
        INNODB INSERT_METHOD INSTALL INSTANCE INVOKER IO IOPS_WEIGHT IO_THREAD IPC ISOLATE ISOLATION ISSUER
        INCREMENT IS_TENANT_SYS_POOL INVISIBLE MERGE ISNULL INTERSECT INCREMENTAL INNER_PARSE ILOGCACHE INPUT INDEXED

        JOB JSON JSON_ARRAYAGG JSON_OBJECTAGG JSON_VALUE JSON_TABLE

        KEY_BLOCK_SIZE KEY_VERSION KVCACHE KV_ATTRIBUTES

        LAG LANGUAGE LAST LAST_VALUE LEAD LEADER LEAVES LESS LEAK LEAK_MOD LEAK_RATE LIB LINESTRING LIST_
        LISTAGG LOCAL LOCALITY LOCATION LOCKED LOCKS LOGFILE LOGONLY_REPLICA_NUM LOGS LOCK_ LOGICAL_READS

        LEVEL LN LOG LS LINK LOG_RESTORE_SOURCE LINE_DELIMITER

        MAJOR MANUAL MASTER MASTER_AUTO_POSITION MASTER_CONNECT_RETRY MASTER_DELAY MASTER_HEARTBEAT_PERIOD
        MASTER_HOST MASTER_LOG_FILE MASTER_LOG_POS MASTER_PASSWORD MASTER_PORT MASTER_RETRY_COUNT
        MASTER_SERVER_ID MASTER_SSL MASTER_SSL_CA MASTER_SSL_CAPATH MASTER_SSL_CERT MASTER_SSL_CIPHER
        MASTER_SSL_CRL MASTER_SSL_CRLPATH MASTER_SSL_KEY MASTER_USER MAX MAX_CONNECTIONS_PER_HOUR MAX_CPU
        LOG_DISK_SIZE MAX_IOPS MEMORY_SIZE MAX_QUERIES_PER_HOUR MAX_ROWS MAX_SIZE
        MAX_UPDATES_PER_HOUR MAX_USER_CONNECTIONS MEDIUM MEMORY MEMTABLE MESSAGE_TEXT META MICROSECOND
        MIGRATE MIN MIN_CPU MIN_IOPS MINOR MIN_ROWS MINUS MINUTE MODE MODIFY MONTH MOVE
        MULTILINESTRING MULTIPOINT MULTIPOLYGON MUTEX MYSQL_ERRNO MIGRATION MAX_USED_PART_ID MAXIMIZE
        MATERIALIZED MEMBER MEMSTORE_PERCENT MINVALUE MY_NAME

        NAME NAMES NAMESPACE NATIONAL NCHAR NDB NDBCLUSTER NESTED NEW NEXT NO NOAUDIT NODEGROUP NONE NORMAL NOW NOWAIT
        NOMINVALUE NOMAXVALUE NOORDER NOCYCLE NOCACHE NO_WAIT NULLS NUMBER NVARCHAR NTILE NTH_VALUE NOARCHIVELOG NETWORK NOPARALLEL
        NULL_IF_EXETERNAL

        OBSOLETE OCCUR OF OFF OFFSET OLD OLD_PASSWORD ONE ONE_SHOT ONLY OPEN OPTIONS ORDINALITY ORIG_DEFAULT OWNER OLD_KEY OVER
        OBCONFIG_URL OJ

        PACK_KEYS PAGE PARALLEL PARAMETERS PARSER PARTIAL PARTITION_ID PARTITIONING PARTITIONS PASSWORD PATH PAUSE PERCENTAGE
        PERCENT_RANK PHASE PLAN PHYSICAL PLANREGRESS PLUGIN PLUGIN_DIR PLUGINS POINT POLYGON PERFORMANCE
        PROTECTION PRIORITY PL POLICY POOL PORT POSITION PREPARE PRESERVE PRETTY PRETTY_COLOR PREV PRIMARY_ZONE PRIVILEGES PROCESS
        PROCESSLIST PROFILE PROFILES PROXY PRECEDING PCTFREE P_ENTITY P_CHUNK
        PUBLIC PROGRESSIVE_MERGE_NUM PREVIEW PS PLUS PATTERN

        QUARTER QUERY QUERY_RESPONSE_TIME QUEUE_TIME QUICK

        REBUILD RECOVER RECOVERY_WINDOW RECYCLE REDO_BUFFER_SIZE REDOFILE REDUNDANCY REDUNDANT REFRESH REGION RELAY RELAYLOG
        RELAY_LOG_FILE RELAY_LOG_POS RELAY_THREAD RELOAD REMAP REMOVE REORGANIZE REPAIR REPEATABLE REPLICA
        REPLICA_NUM REPLICA_TYPE REPLICATION REPORT RESET RESOURCE RESOURCE_POOL_LIST RESPECT RESTART
        RESTORE RESUME RETURNED_SQLSTATE RETURNS RETURNING REVERSE ROLLBACK ROLLUP ROOT
        ROOTTABLE ROOTSERVICE ROOTSERVICE_LIST ROUTINE ROW ROLLING ROW_COUNT ROW_FORMAT ROWS RTREE RUN
        RECYCLEBIN ROTATE ROW_NUMBER RUDUNDANT RECURSIVE RANDOM REDO_TRANSPORT_OPTIONS REMOTE_OSS RT
        RANK READ_ONLY RECOVERY REJECT

        SAMPLE SAVEPOINT SCHEDULE SCHEMA_NAME SCN SCOPE SECOND SECURITY SEED SEQUENCES SERIAL SERIALIZABLE SERVER
        SERVER_IP SERVER_PORT SERVER_TYPE SERVICE SESSION SESSION_USER SET_MASTER_CLUSTER SET_SLAVE_CLUSTER
        SET_TP SHARE SHUTDOWN SIGNED SIMPLE SLAVE SLOW SLOT_IDX SNAPSHOT SOCKET SOME SONAME SOUNDS
        SOURCE SPFILE SPLIT SQL_AFTER_GTIDS SQL_AFTER_MTS_GAPS SQL_BEFORE_GTIDS SQL_BUFFER_RESULT
        SQL_CACHE SQL_NO_CACHE SQL_ID SQL_THREAD SQL_TSI_DAY SQL_TSI_HOUR SQL_TSI_MINUTE SQL_TSI_MONTH
        SQL_TSI_QUARTER SQL_TSI_SECOND SQL_TSI_WEEK SQL_TSI_YEAR SRID STANDBY STAT START STARTS STATS_AUTO_RECALC
        STATS_PERSISTENT STATS_SAMPLE_PAGES STATUS STATEMENTS STATISTICS STD STDDEV STDDEV_POP STDDEV_SAMP STRONG
        SYNCHRONIZATION STOP STORAGE STORAGE_FORMAT_VERSION STORING STRING
        SUBCLASS_ORIGIN SUBDATE SUBJECT SUBPARTITION SUBPARTITIONS SUBSTR SUBSTRING SUCCESSFUL SUM
        SUPER SUSPEND SWAPS SWITCH SWITCHES SWITCHOVER SYSTEM SYSTEM_USER SYSDATE SESSION_ALIAS
        SIZE SKEWONLY SEQUENCE SLOG STATEMENT_ID SKIP_HEADER SKIP_BLANK_LINES

        TABLE_CHECKSUM TABLE_MODE TABLE_ID TABLE_NAME TABLEGROUPS TABLES TABLESPACE TABLET TABLET_ID TABLET_MAX_SIZE
        TEMPLATE TEMPORARY TEMPTABLE TENANT TEXT THAN TIME TIMESTAMP TIMESTAMPADD TIMESTAMPDIFF TP_NO
        TP_NAME TRACE TRADITIONAL TRANSACTION TRIGGERS TRIM TRUNCATE TYPE TYPES TASK TABLET_SIZE
        TABLEGROUP_ID TENANT_ID THROTTLE TIME_ZONE_INFO TOP_K_FRE_HIST TIMES TRIM_SPACE TTL

        UNCOMMITTED UNDEFINED UNDO_BUFFER_SIZE UNDOFILE UNICODE UNINSTALL UNIT UNIT_GROUP UNIT_NUM UNLOCKED UNTIL
        UNUSUAL UPGRADE USE_BLOOM_FILTER UNKNOWN USE_FRM USER USER_RESOURCES UNBOUNDED UP UNLIMITED

        VALID VALUE VARIANCE VARIABLES VERBOSE VERIFY VIEW VISIBLE VIRTUAL_COLUMN_ID VALIDATE VAR_POP
        VAR_SAMP

        WAIT WARNINGS WASH WEEK WEIGHT_STRING WHENEVER WITH_ROWID WORK WRAPPER WINDOW WEAK

        X509 XA XML

        YEAR

        ZONE ZONE_LIST ZONE_TYPE

%type <node> sql_stmt stmt_list stmt opt_end_p
%type <node> select_stmt update_stmt delete_stmt
%type <node> insert_stmt single_table_insert values_clause dml_table_name
%type <node> create_table_stmt create_table_like_stmt opt_table_option_list table_option_list table_option table_option_list_space_seperated create_function_stmt drop_function_stmt parallel_option
%type <node> opt_force
%type <node> create_sequence_stmt alter_sequence_stmt drop_sequence_stmt opt_sequence_option_list sequence_option_list sequence_option simple_num
%type <node> create_database_stmt drop_database_stmt alter_database_stmt use_database_stmt
%type <node> opt_database_name database_option database_option_list opt_database_option_list database_factor databases_expr opt_databases
%type <node> create_tenant_stmt opt_tenant_option_list alter_tenant_stmt drop_tenant_stmt create_standby_tenant_stmt log_restore_source_option
%type <node> create_restore_point_stmt drop_restore_point_stmt
%type <node> create_resource_stmt drop_resource_stmt alter_resource_stmt
%type <node> cur_timestamp_func cur_time_func cur_date_func now_synonyms_func utc_timestamp_func utc_time_func utc_date_func sys_interval_func sysdate_func cur_user_func
%type <node> create_dblink_stmt drop_dblink_stmt dblink tenant opt_cluster opt_dblink
%type <node> opt_create_resource_pool_option_list create_resource_pool_option alter_resource_pool_option_list alter_resource_pool_option
%type <node> opt_shrink_unit_option id_list opt_shrink_tenant_unit_option
%type <node> opt_resource_unit_option_list resource_unit_option
%type <node> tenant_option zone_list resource_pool_list
%type <node> opt_partition_option partition_option hash_partition_option key_partition_option opt_use_partition use_partition range_partition_option subpartition_option opt_range_partition_list opt_range_subpartition_list range_partition_list range_subpartition_list range_partition_element range_subpartition_element range_partition_expr range_expr_list range_expr opt_part_id sample_clause opt_block seed sample_percent opt_sample_scope modify_partition_info modify_tg_partition_info opt_partition_range_or_list auto_partition_option auto_range_type partition_size auto_partition_type use_flashback
%type <node> subpartition_template_option subpartition_individual_option opt_hash_partition_list hash_partition_list hash_partition_element opt_hash_subpartition_list hash_subpartition_list hash_subpartition_element opt_subpartition_list opt_engine_option
%type <node> date_unit date_params timestamp_params
%type <node> drop_table_stmt table_list drop_view_stmt table_or_tables
%type <node> explain_stmt explainable_stmt format_name kill_stmt help_stmt create_outline_stmt alter_outline_stmt drop_outline_stmt opt_outline_target
%type <node> expr_list expr expr_const conf_const simple_expr expr_or_default bit_expr bool_pri predicate explain_or_desc pl_expr_stmt
%type <node> column_ref multi_delete_table
%type <node> case_expr func_expr in_expr sub_query_flag
%type <node> case_arg when_clause_list when_clause case_default
%type <node> window_function opt_partition_by generalized_window_clause win_rows_or_range win_preceding_or_following win_interval win_bounding win_window opt_win_window win_fun_lead_lag_params respect_or_ignore opt_respect_or_ignore_nulls win_fun_first_last_params first_or_last opt_from_first_or_last new_generalized_window_clause new_generalized_window_clause_with_blanket opt_named_windows named_windows named_window
%type <node> win_dist_list win_dist_desc
%type <ival> opt_hash_sort_and_pushdown
%type <node> update_asgn_list update_asgn_factor
%type <node> update_basic_stmt delete_basic_stmt
%type <node> table_element_list table_element column_definition column_definition_ref column_definition_list column_name_list
%type <node> opt_generated_keyname opt_generated_option_list opt_generated_column_attribute_list generated_column_attribute opt_storage_type
%type <node> data_type special_table_type opt_if_not_exists opt_if_exists opt_charset collation opt_collation cast_data_type
%type <node> replace_with_opt_hint insert_with_opt_hint column_list opt_on_duplicate_key_clause opt_into opt_replace opt_temporary opt_algorithm opt_sql_security opt_definer view_algorithm no_param_column_ref
%type <node> insert_vals_list insert_vals value_or_values
%type <node> select_with_parens select_no_parens select_clause select_into no_table_select_with_order_and_limit simple_select_with_order_and_limit select_with_parens_with_order_and_limit select_clause_set select_clause_set_left select_clause_set_right  select_clause_set_with_order_and_limit
%type <node> simple_select no_table_select limit_clause select_expr_list
%type <node> with_select with_clause with_list common_table_expr opt_column_alias_name_list alias_name_list column_alias_name
%type <node> opt_where opt_hint_value opt_groupby opt_rollup opt_order_by order_by opt_having groupby_clause
%type <node> opt_limit_clause limit_expr opt_lock_type opt_for_update opt_for_update_wait opt_lock_in_share_mode
%type <node> sort_list sort_key opt_asc_desc sort_list_for_group_by sort_key_for_group_by opt_asc_desc_for_group_by opt_column_id
%type <node> opt_query_expression_option_list query_expression_option_list query_expression_option opt_distinct opt_distinct_or_all opt_separator projection
%type <node> from_list table_references table_reference table_factor normal_relation_factor dot_relation_factor relation_factor
%type <node> relation_factor_in_hint relation_factor_in_hint_list relation_factor_in_pq_hint opt_relation_factor_in_hint_list relation_factor_in_use_join_hint_list
%type <node> relation_factor_in_leading_hint_list joined_table tbl_name table_subquery table_subquery_alias
%type <node> relation_factor_with_star relation_with_star_list opt_with_star
%type <node> index_hint_type key_or_index index_hint_scope index_element index_list opt_index_list
%type <node> add_key_or_index_opt add_key_or_index add_unique_key_opt add_unique_key add_constraint_uniq_key_opt add_constraint_uniq_key add_constraint_pri_key_opt add_constraint_pri_key add_primary_key_opt add_primary_key add_spatial_index_opt add_spatial_index
%type <node> index_hint_definition index_hint_list
%type <node> intnum_list
%type <node> qb_name_option qb_name_string qb_name_list multi_qb_name_list
%type <node> join_condition inner_join_type opt_inner outer_join_type opt_outer natural_join_type except_full_outer_join_type opt_full_table_factor
%type <ival> string_length_i opt_string_length_i opt_string_length_i_v2 opt_int_length_i opt_bit_length_i opt_datetime_fsp_i opt_unsigned_i opt_zerofill_i opt_year_i opt_time_func_fsp_i opt_cast_float_precision
%type <node> opt_float_precision opt_number_precision
%type <node> opt_equal_mark opt_default_mark read_only_or_write not not2 opt_disk_alias
%type <node> int_or_decimal
%type <node> opt_column_attribute_list column_attribute column_attribute_list
%type <node> show_stmt from_or_in columns_or_fields database_or_schema index_or_indexes_or_keys opt_from_or_in_database_clause opt_show_condition opt_desc_column_option opt_status opt_storage
%type <node> prepare_stmt stmt_name preparable_stmt
%type <node> variable_set_stmt var_and_val_list var_and_val to_or_eq set_expr_or_default sys_var_and_val_list sys_var_and_val opt_set_sys_var opt_global_sys_vars_set
%type <node> execute_stmt argument_list argument opt_using_args
%type <node> deallocate_prepare_stmt deallocate_or_drop
%type <ival> opt_scope opt_drop_behavior opt_integer scope_or_scope_alias global_or_session_alias
%type <ival> int_type_i float_type_i datetime_type_i date_year_type_i cast_datetime_type_i text_type_i blob_type_i
%type <node> create_user_stmt user_specification user_specification_list user password opt_host_name user_with_host_name opt_auth_plugin
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
%type <ival> opt_with_consistent_snapshot opt_config_scope opt_index_keyname opt_full
%type <node> opt_work begin_stmt commit_stmt rollback_stmt opt_ignore xa_begin_stmt xa_end_stmt xa_prepare_stmt xa_commit_stmt xa_rollback_stmt
%type <node> alter_table_stmt alter_table_actions alter_table_action_list alter_table_action alter_column_option alter_index_option alter_constraint_option standalone_alter_action alter_partition_option opt_to alter_tablegroup_option opt_table opt_tablegroup_option_list alter_tg_partition_option
%type <node> tablegroup_option_list tablegroup_option alter_tablegroup_actions alter_tablegroup_action tablegroup_option_list_space_seperated
%type <node> opt_tg_partition_option tg_hash_partition_option tg_key_partition_option tg_range_partition_option tg_subpartition_option tg_list_partition_option
%type <node> alter_column_behavior opt_set opt_position_column
%type <node> alter_system_stmt alter_system_set_parameter_actions alter_system_settp_actions settp_option alter_system_set_parameter_action server_info_list server_info
%type <node> opt_comment opt_as
%type <node> column_name relation_name function_name column_label var_name relation_name_or_string row_format_option
%type <node> audit_stmt audit_clause op_audit_tail_clause audit_operation_clause audit_all_shortcut_list audit_all_shortcut auditing_on_clause auditing_by_user_clause audit_user_list audit_user audit_user_with_host_name
%type <node> opt_hint_list hint_option select_with_opt_hint update_with_opt_hint delete_with_opt_hint hint_list_with_end global_hint transform_hint optimize_hint
%type <node> create_index_stmt index_name sort_column_list sort_column_key opt_index_option_list index_option opt_sort_column_key_length opt_index_using_algorithm index_using_algorithm visibility_option opt_constraint_name constraint_name create_with_opt_hint index_expr
%type <node> opt_when check_state constraint_definition
%type <non_reserved_keyword> unreserved_keyword unreserved_keyword_normal unreserved_keyword_special unreserved_keyword_extra
%type <reserved_keyword> mysql_reserved_keyword
%type <ival> set_type_other set_type_union audit_by_session_access_option audit_whenever_option audit_or_noaudit
%type <ival> consistency_level use_plan_cache_type
%type <node> set_type set_expression_option
%type <node> drop_index_stmt hint_options opt_expr_as_list expr_as_list expr_with_opt_alias substr_params opt_comma substr_or_substring
%type <node> /*frozen_type*/ opt_binary
%type <node> ip_port
%type <node> create_view_stmt view_name opt_column_list opt_table_id opt_tablet_id view_select_stmt opt_check_option
%type <node> name_list
%type <node> partition_role ls_role zone_desc opt_zone_desc server_or_zone opt_server_or_zone opt_partitions opt_subpartitions add_or_alter_zone_options alter_or_change_or_modify
%type <node> ls opt_tenant_list_or_ls_or_tablet_id ls_server_or_server_or_zone_or_tenant add_or_alter_zone_option
%type <node> opt_tenant_list_v2
%type <node> suspend_or_resume tenant_name opt_tenant_name cache_name opt_cache_name file_id opt_file_id cancel_task_type
%type <node> sql_id_expr opt_sql_id
%type <node> namespace_expr opt_namespace
%type <node> server_action server_list opt_server_list
%type <node> zone_action upgrade_action
%type <node> opt_index_name opt_key_or_index opt_index_options opt_primary  opt_all
%type <node> charset_key database_key charset_name charset_name_or_default collation_name databases_or_schemas trans_param_name trans_param_value
%type <node> set_names_stmt set_charset_stmt
%type <node> charset_introducer complex_string_literal literal number_literal now_or_signed_literal signed_literal
%type <node> create_tablegroup_stmt drop_tablegroup_stmt alter_tablegroup_stmt default_tablegroup
%type <node> set_transaction_stmt transaction_characteristics transaction_access_mode isolation_level
%type <node> lock_tables_stmt unlock_tables_stmt lock_type lock_table_list lock_table opt_local
%type <node> flashback_stmt purge_stmt opt_flashback_rename_table opt_flashback_rename_database opt_flashback_rename_tenant
%type <node> tenant_name_list opt_tenant_list tenant_list_tuple cache_type flush_scope opt_zone_list
%type <node> into_opt into_clause field_opt field_term field_term_list line_opt line_term line_term_list into_var_list into_var
%type <node> string_list text_string string_val_list
%type <node> balance_task_type opt_balance_task_type
%type <node> list_expr list_partition_element list_partition_expr list_partition_list list_partition_option opt_list_partition_list opt_list_subpartition_list list_subpartition_list list_subpartition_element drop_partition_name_list
%type <node> primary_zone_name change_tenant_name_or_tenant_id distribute_method distribute_method_list
%type <node> load_data_stmt opt_load_local opt_duplicate opt_load_charset opt_load_ignore_rows
%type <node> lines_or_rows opt_field_or_var_spec field_or_vars_list field_or_vars opt_load_set_spec opt_load_data_extended_option_list load_data_extended_option_list load_data_extended_option
%type <node> load_set_list load_set_element load_data_with_opt_hint
%type <node> ret_type opt_agg
%type <node> opt_match_option
%type <ival> match_action
%type <node> opt_reference_option_list reference_option require_specification tls_option_list tls_option
%type <node> opt_resource_option resource_option_list resource_option
%type <ival> reference_action
%type <node> alter_foreign_key_action
%type <node> analyze_stmt analyze_statistics_clause opt_analyze_for_clause opt_analyze_for_clause_list opt_analyze_for_clause_element opt_analyze_sample_clause sample_option for_all opt_indexed_hiddden opt_size_clause size_clause for_columns for_columns_list for_columns_item column_clause
%type <node> optimize_stmt
%type <node> dump_memory_stmt
%type <node> create_savepoint_stmt rollback_savepoint_stmt release_savepoint_stmt
%type <node> opt_qb_name parallel_hint pq_set_hint_desc
%type <node> create_tablespace_stmt drop_tablespace_stmt tablespace rotate_master_key_stmt
%type <node> alter_tablespace_stmt
%type <node> permanent_tablespace permanent_tablespace_options permanent_tablespace_option alter_tablespace_actions alter_tablespace_action opt_force_purge
%type <node> opt_sql_throttle_for_priority opt_sql_throttle_using_cond sql_throttle_one_or_more_metrics sql_throttle_metric
%type <node> opt_copy_id opt_backup_dest opt_backup_backup_dest opt_tenant_info opt_with_active_piece get_format_unit opt_backup_tenant_list opt_backup_to opt_description policy_name opt_recovery_window opt_redundancy opt_backup_copies opt_restore_until opt_backup_key_info opt_encrypt_key
%type <node> opt_recover_tenant recover_table_list recover_table_relation_name restore_remap_list remap_relation_name table_relation_name opt_recover_remap_item_list restore_remap_item_list restore_remap_item remap_item
%type <node> new_or_old new_or_old_column_ref diagnostics_info_ref
%type <node> on_empty on_error json_on_response opt_returning_type opt_on_empty_or_error json_value_expr opt_ascii opt_truncate_clause
%type <node> ws_nweights opt_ws_as_char opt_ws_levels ws_level_flag_desc ws_level_flag_reverse ws_level_flags ws_level_list ws_level_list_item ws_level_number ws_level_range ws_level_list_or_range
%type <node> get_diagnostics_stmt get_statement_diagnostics_stmt get_condition_diagnostics_stmt statement_information_item_list condition_information_item_list statement_information_item condition_information_item statement_information_item_name condition_information_item_name condition_arg
%type <node> method_opt method_list method extension
%type <node> opt_storage_name opt_calibration_list calibration_info_list
%type <node> switchover_tenant_stmt switchover_clause opt_verify
%type <node> recover_tenant_stmt recover_point_clause
%type <node> external_file_format_list external_file_format external_table_partition_option
%type <node> dynamic_sampling_hint
%type <node> json_table_expr mock_jt_on_error_on_empty jt_column_list json_table_column_def
%type <node> json_table_ordinality_column_def json_table_exists_column_def json_table_value_column_def json_table_nested_column_def
%type <node> opt_value_on_empty_or_error_or_mismatch opt_on_mismatch
%type <node> table_values_caluse table_values_caluse_with_order_by_and_limit values_row_list row_value

%type <node> ttl_definition ttl_expr ttl_unit
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
  | create_table_stmt       {
    $$ = $1;
    ParseNode *parse_tree = $1;
    if (NULL != parse_tree && 7 < parse_tree->num_child_
                           && NULL != parse_tree->children_[7]
                           && T_SELECT == parse_tree->children_[7]->type_) {
      question_mark_issue($$, result);
    } else {
      check_question_mark($$, result);
    }
  }
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
  | audit_stmt              { $$ = $1; check_question_mark($$, result); }
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
  | create_tablespace_stmt  { $$ = $1; check_question_mark($$, result); }
  | drop_tablespace_stmt    { $$ = $1; check_question_mark($$, result); }
  | alter_tablespace_stmt   { $$ = $1; check_question_mark($$, result); }
  | rotate_master_key_stmt  { $$ = $1; check_question_mark($$, result); }
  | create_index_stmt       { $$ = $1; check_question_mark($$, result); }
  | drop_index_stmt         { $$ = $1; check_question_mark($$, result); }
  | kill_stmt               { $$ = $1; question_mark_issue($$, result); }
  | help_stmt               { $$ = $1; check_question_mark($$, result); }
  | create_view_stmt
  {
    $$ = $1;

    if (OB_UNLIKELY(NULL == $$ || NULL == result)) {
      yyerror(NULL, result, "node or result is NULL\n");
      YYABORT_UNEXPECTED;
    } else if (OB_UNLIKELY(!result->pl_parse_info_.is_pl_parse_ && 0 != result->question_mark_ctx_.count_)) {
      if (result->pl_parse_info_.is_inner_parse_) {
        result->extra_errno_ = OB_PARSER_ERR_VIEW_SELECT_CONTAIN_QUESTIONMARK;
        YYABORT;
      } else {
        yyerror(NULL, result, "Unknown column '?'\n");
        YYABORT_PARSE_SQL_ERROR;
      }
    } else {
      $$->value_ = result->question_mark_ctx_.count_;
    }
  }
  | create_tenant_stmt      { $$ = $1; check_question_mark($$, result); }
  | create_standby_tenant_stmt { $$ = $1; check_question_mark($$, result); }
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
  | create_savepoint_stmt   { $$ = $1; check_question_mark($$, result); }
  | rollback_savepoint_stmt { $$ = $1; check_question_mark($$, result); }
  | release_savepoint_stmt  { $$ = $1; check_question_mark($$, result); }
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
  | analyze_stmt            { $$ = $1; check_question_mark($$, result); }
  | load_data_stmt          { $$ = $1; check_question_mark($$, result); }
  | create_dblink_stmt      { $$ = $1; check_question_mark($$, result); }
  | drop_dblink_stmt        { $$ = $1; check_question_mark($$, result); }
  | create_sequence_stmt    { $$ = $1; check_question_mark($$, result); }
  | alter_sequence_stmt     { $$ = $1; check_question_mark($$, result); }
  | drop_sequence_stmt      { $$ = $1; check_question_mark($$, result); }
  | xa_begin_stmt           { $$ = $1; check_question_mark($$, result); }
  | xa_end_stmt             { $$ = $1; check_question_mark($$, result); }
  | xa_prepare_stmt         { $$ = $1; check_question_mark($$, result); }
  | xa_commit_stmt          { $$ = $1; check_question_mark($$, result); }
  | xa_rollback_stmt        { $$ = $1; check_question_mark($$, result); }
  | optimize_stmt     { $$ = $1; check_question_mark($$, result); }
  | dump_memory_stmt  { $$ = $1; check_question_mark($$, result); }
  | get_diagnostics_stmt    { $$ = $1; question_mark_issue($$, result); }
  | pl_expr_stmt            { $$ = $1; question_mark_issue($$, result); }
  | method_opt              { $$ = $1; check_question_mark($$, result); }
  | switchover_tenant_stmt   { $$ = $1; check_question_mark($$, result); }
  | recover_tenant_stmt   { $$ = $1; check_question_mark($$, result); }
  ;

/*****************************************************************************
*
*	expression grammar
*
*****************************************************************************/

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
  if (OB_UNLIKELY((NULL == $$->str_value_)) && $$->type_ != T_NCHAR && $$->type_ != T_VARCHAR) {
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
    alias_node->sql_str_off_ = @2.first_column;
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
    if (0 == $3->str_len_) {
      alias_name_node->str_value_ = NULL;
      alias_name_node->str_len_ = 0;
    } else if (result->is_not_utf8_connection_) {
      alias_name_node->str_value_ = parse_str_convert_utf8(result->charset_info_, $3->str_value_,
                                                           result->malloc_pool_, &(alias_name_node->str_len_),
                                                           &(result->extra_errno_));
      if (OB_PARSER_ERR_ILLEGAL_NAME == result->extra_errno_) {
        yyerror(NULL, result, "alias '%s' is illegal\n", $3->str_value_);
        YYABORT_UNEXPECTED;
      }
    } else {
      dup_node_string($3, alias_name_node, result->malloc_pool_);
    }
    alias_name_node->sql_str_off_ = $3->sql_str_off_;

    malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, alias_name_node);
    malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_WITH_ALIAS, 1, alias_node);
    dup_expr_string($$, result, @3.first_column, @3.last_column);
    if (0 == $3->str_len_) {
      alias_node->str_value_ = NULL;
      alias_node->str_len_ = 0;
      alias_node->sql_str_off_ = @2.first_column;
    } else {
      dup_node_string($3, alias_node, result->malloc_pool_);
      alias_node->sql_str_off_ = @2.first_column;
    }
    alias_node->param_num_ = 1;
  }
}


column_ref:
column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, NULL, $1);
  dup_node_string($1, $$, result->malloc_pool_);
#ifndef SQL_PARSER_COMPILATION
  lookup_pl_exec_symbol($$, result, @1.first_column, @1.last_column, false, false, false);
#endif
}
| relation_name '.' column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, $1, $3);
  dup_node_string($3, $$, result->malloc_pool_);
#ifndef SQL_PARSER_COMPILATION
  if (3 == $1->str_len_) {
    if (0 == strcasecmp("NEW", $1->str_value_) || 0 == strcasecmp("OLD", $1->str_value_)) {
      lookup_pl_exec_symbol($$, result, @1.first_column, @3.last_column, true, false, false);
    }
  }
#endif
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
| FORCE
{
  if (result->pl_parse_info_.is_pl_parse_) {
    ParseNode *col_name = NULL;
    get_non_reserved_node(col_name, result->malloc_pool_, @1.first_column, @1.last_column);
    malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, NULL, col_name);
    dup_node_string(col_name, $$, result->malloc_pool_);
  #ifndef SQL_PARSER_COMPILATION
    lookup_pl_exec_symbol($$, result, @1.first_column, @1.last_column, false, false, false);
  #endif
  } else {
    yyerror(&@1, result, "force key work can be used to be name in PL\n");
  }
}
| CASCADE
{
  if (result->pl_parse_info_.is_pl_parse_) {
    ParseNode *col_name = NULL;
    get_non_reserved_node(col_name, result->malloc_pool_, @1.first_column, @1.last_column);
    malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, NULL, col_name);
    dup_node_string(col_name, $$, result->malloc_pool_);
  #ifndef SQL_PARSER_COMPILATION
    lookup_pl_exec_symbol($$, result, @1.first_column, @1.last_column, false, false, false);
  #endif
  } else {
    yyerror(&@1, result, "cascade key work can be used to be name in PL\n");
  }
}
;

/* literal string with  */
complex_string_literal:
STRING_VALUE %prec LOWER_THAN_COMP
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 2, NULL, $1);
  $$->str_value_ = $1->str_value_;
  $$->str_len_ = $1->str_len_;
  $$->raw_text_ = $1->raw_text_;
  $$->text_len_ = $1->text_len_;
  $$->sql_str_off_ = @1.first_column;
  @$.first_column = @1.first_column;
  @$.last_column = @1.last_column;
}
| charset_introducer STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 2, $1, $2);
  $$->str_value_ = $2->str_value_;
  $$->str_len_ = $2->str_len_;
  $$->raw_text_ = $2->raw_text_;
  $$->text_len_ = $2->text_len_;
  $$->sql_str_off_ = $2->sql_str_off_;
}
| charset_introducer HEX_STRING_VALUE
{
  /* _utf8mb4 0x42 作为字符串处理 */
  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 1, $1);
  $$->str_value_ = $2->str_value_;
  $$->str_len_ = $2->str_len_;
  $$->raw_text_ = $2->raw_text_;
  $$->text_len_ = $2->text_len_;
  $$->sql_str_off_ = $2->sql_str_off_;
}
| STRING_VALUE string_val_list %prec LOWER_THAN_COMP
{
  ParseNode *str_node = NULL;
  malloc_non_terminal_node(str_node, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
  ParseNode *string_list_node = NULL;
  merge_nodes(string_list_node, result, T_EXPR_LIST, str_node);
  ParseNode *concat_node = NULL;
  make_name_node(concat_node, result->malloc_pool_, "concat");
  concat_node->reserved_ = 1; /* mark special concat */
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, concat_node, string_list_node);
}
| NATIONAL_LITERAL
{
   $$ = $1;
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
| _LATIN1
{
  malloc_terminal_node($$, result->malloc_pool_, T_CHARSET);
  $$->str_value_ = parse_strdup("latin1", result->malloc_pool_, &($$->str_len_));
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
| _GB18030_2022
{
  malloc_terminal_node($$, result->malloc_pool_, T_CHARSET);
  $$->str_value_ = parse_strdup("gb18030_2022", result->malloc_pool_, &($$->str_len_));
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
INTNUM { $$ = $1; $$->param_num_ = 1; $$->sql_str_off_=$1->sql_str_off_;}
| DECIMAL_VAL { $$ = $1; $$->param_num_ = 1;$$->sql_str_off_=$1->sql_str_off_;}
;

expr_const:
literal
{
  $$ = $1;
  $$->sql_str_off_ = $1->sql_str_off_;
  CHECK_MYSQL_COMMENT(result, $$);
}
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
| bool_pri COMP_LE sub_query_flag select_with_parens
{
	ParseNode *sub_query = NULL;
	malloc_non_terminal_node(sub_query, result->malloc_pool_, $3->type_, 1, $4);
	malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LE, 2, $1, sub_query);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bool_pri COMP_LT predicate
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LT, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bool_pri COMP_LT sub_query_flag select_with_parens
{
  ParseNode *sub_query = NULL;
  malloc_non_terminal_node(sub_query, result->malloc_pool_, $3->type_, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LT, 2, $1, sub_query);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bool_pri COMP_EQ predicate %prec COMP_EQ
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bool_pri COMP_EQ sub_query_flag select_with_parens %prec COMP_EQ
{
  ParseNode *sub_query = NULL;
  malloc_non_terminal_node(sub_query, result->malloc_pool_, $3->type_, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, sub_query);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
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
| bool_pri COMP_GE sub_query_flag select_with_parens %prec COMP_GE
{
  ParseNode *sub_query = NULL;
  malloc_non_terminal_node(sub_query, result->malloc_pool_, $3->type_, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GE, 2, $1, sub_query);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bool_pri COMP_GT predicate %prec COMP_GT
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GT, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bool_pri COMP_GT sub_query_flag select_with_parens %prec COMP_GT
{
  ParseNode *sub_query = NULL;
  malloc_non_terminal_node(sub_query, result->malloc_pool_, $3->type_, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GT, 2, $1, sub_query);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| bool_pri COMP_NE predicate %prec COMP_NE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, $3);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @3.last_column),
            &@1, result);
}
| bool_pri COMP_NE sub_query_flag select_with_parens %prec COMP_NE
{
  ParseNode *sub_query = NULL;
  malloc_non_terminal_node(sub_query, result->malloc_pool_, $3->type_, 1, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, sub_query);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| predicate {
  $$ = $1;
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @1.last_column),
            &@1, result);
}
;

opt_of:
OF
|
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
| bit_expr SOUNDS LIKE simple_expr
{
  ParseNode *soundex_node1 = NULL;
  ParseNode *soundex_node2 = NULL;
  ParseNode *name_node1 = NULL;
  ParseNode *name_node2 = NULL;
  ParseNode *param1 = NULL;
  ParseNode *param2 = NULL;
  make_name_node(name_node1, result->malloc_pool_, "soundex");
  make_name_node(name_node2, result->malloc_pool_, "soundex");
  malloc_non_terminal_node(param1, result->malloc_pool_, T_EXPR_LIST, 1, $1);
  malloc_non_terminal_node(param2, result->malloc_pool_, T_EXPR_LIST, 1, $4);
  malloc_non_terminal_node(soundex_node1, result->malloc_pool_, T_FUN_SYS, 2, name_node1, param1);
  malloc_non_terminal_node(soundex_node2, result->malloc_pool_, T_FUN_SYS, 2, name_node2, param2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, soundex_node1, soundex_node2);
}
| bit_expr LIKE simple_expr
{
  //在resolver时，如果发现只有两个children，会将escape 参数设置为‘\’
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LIKE, 2, $1, $3);
}
| bit_expr LIKE simple_expr ESCAPE simple_expr %prec LIKE
{
  // 如果escape 为空串 '', 则使用默认值'\'
  if (OB_UNLIKELY(T_VARCHAR == $5->type_ && 0 == $5->str_len_)) {
    ParseNode *node = NULL;
    malloc_terminal_node(node, result->malloc_pool_, T_VARCHAR);
    node->str_value_ = "\\";
    node->str_len_ = 1;
    node->sql_str_off_ = @5.first_column;
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LIKE, 3, $1, $3, node);
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LIKE, 3, $1, $3, $5);
  }
}
| bit_expr not LIKE simple_expr
{
  (void)($2);
  //在resolver时，如果发现只有两个children，会将escape 参数设置为‘\’
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_LIKE, 2, $1, $4);
}
| bit_expr not LIKE simple_expr ESCAPE simple_expr %prec LIKE
{
  (void)($2);
  // 如果escape 为空串 '', 则使用默认值'\'
  if (OB_UNLIKELY(T_VARCHAR == $6->type_ && 0 == $6->str_len_)) {
    ParseNode *node = NULL;
    malloc_terminal_node(node, result->malloc_pool_, T_VARCHAR);
    node->str_value_ = "\\";
    node->str_len_ = 1;
    node->sql_str_off_ = @6.first_column;
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
| bit_expr MEMBER opt_of '(' simple_expr ')' %prec LOWER_THAN_COMP
{
  (void)($2);
  ParseNode *json_member_of_node = NULL;
  make_name_node(json_member_of_node, result->malloc_pool_, "JSON_MEMBER_OF");
  ParseNode *link_params = NULL;
  malloc_non_terminal_node(link_params, result->malloc_pool_, T_LINK_NODE, 2, $1, $5);
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, link_params);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, json_member_of_node, params);
}
| bit_expr %prec LOWER_THAN_COMP
{ $$ = $1; }
;

string_val_list:
STRING_VALUE
{
 $$ = $1;
}
| string_val_list STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
};

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
  ParseNode *tmp_node = NULL;
  malloc_terminal_node(tmp_node, result->malloc_pool_, T_DEFAULT_INT);
  tmp_node->value_ = 1;
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 4, $1, $4, $5, tmp_node);
  make_name_node($$, result->malloc_pool_, "date_add");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| INTERVAL expr date_unit '+' bit_expr
{
  ParseNode *tmp_node = NULL;
  malloc_terminal_node(tmp_node, result->malloc_pool_, T_DEFAULT_INT);
  tmp_node->value_ = 2;
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 4, $2, $3, $5, tmp_node);
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_SET_COLLATION, 2, $$, params);
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
| expr_const {
  $$ = $1;
  $$->sql_str_off_ = $1->sql_str_off_;
}
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
    //在非FAST_PARSER场景中，处于[INT64_MAX + 1, UINT64_MAX]之间的整数都被解析为UINT64
    // 在simple expr是一个T_UINT64且值是'INT64_MAX + 1'时，需要将
    // '-'和'INT64_MAX + 1'打包在一起作为一个T_INT的常量节点，因为'-' 'INT64_MAX + 1' = 'INT64_MIN'
    // 但是对于-(INT64_MAX + 1)，由于词法不能知道这整体是一个整数，只能得到-(?)
    // 所以生成NEG T_NUMBER。 -(INT64_MAX + 1)时，表达式的is_assigned_from_child_为1
    //对于[INT64_MAX + 2, UINT64_MAX]之间的值，需要把simple_expr类型转换为T_NUMBER
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
  /* mysql 允许 select * from dual 出现在 exists 中, 此处更改 from dual 的 select list 为常量 1 */
  if (NULL == $2->children_[PARSE_SELECT_FROM]) {
    $2->value_ = 2;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EXISTS, 1, $2);
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
//ODBC escape sequences syntax for scalar function/time literals and so on. compatible Mysql8.0
| '{' relation_name expr '}'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ODBC_ESCAPE_SEQUENCES, 2, $2, $3);
}
| USER_VARIABLE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GET_USER_VAR, 1, $1);
}
;
| column_definition_ref JSON_EXTRACT complex_string_literal
{
  ParseNode *json_extract_node = NULL;
  make_name_node(json_extract_node, result->malloc_pool_, "JSON_EXTRACT");
  ParseNode *link_params = NULL;
  malloc_non_terminal_node(link_params, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, link_params);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, json_extract_node, params);
  store_pl_ref_object_symbol($$, result, REF_FUNC);
}
| column_definition_ref JSON_EXTRACT_UNQUOTED complex_string_literal
{
  ParseNode *json_extract_node = NULL;
  make_name_node(json_extract_node, result->malloc_pool_, "JSON_EXTRACT");
  ParseNode *link_params = NULL;
  malloc_non_terminal_node(link_params, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, link_params);
  malloc_non_terminal_node(json_extract_node, result->malloc_pool_, T_FUN_SYS, 2, json_extract_node, params);
  ParseNode *json_unquoted_node = NULL;
  make_name_node(json_unquoted_node, result->malloc_pool_, "JSON_UNQUOTE");
  merge_nodes(params, result, T_EXPR_LIST, json_extract_node);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, json_unquoted_node, params);
  store_pl_ref_object_symbol($$, result, REF_FUNC);
}
| relation_name '.' relation_name USER_VARIABLE
{
  ParseNode *dblink_node = $4;
  if (NULL != dblink_node) {
    dblink_node->type_ = T_DBLINK_NAME;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_REMOTE_SEQUENCE, 4, NULL, $1, $3, $4);
}
| relation_name '.' relation_name '.' relation_name USER_VARIABLE
{
  ParseNode *dblink_node = $6;
  if (NULL != dblink_node) {
    dblink_node->type_ = T_DBLINK_NAME;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_REMOTE_SEQUENCE, 4, $1, $3, $5, $6);
}
;
expr:
expr AND expr %prec AND
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_AND, 2, $1, $3);
  if (result->pl_parse_info_.is_pl_parse_) {
    dup_expr_string($$, result, @1.first_column, @3.last_column);
  }
}
| expr AND_OP expr %prec AND
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_AND, 2, $1, $3);
  if (result->pl_parse_info_.is_pl_parse_) {
    dup_expr_string($$, result, @1.first_column, @3.last_column);
  }
}
| expr OR expr %prec OR
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_OR, 2, $1, $3);
  if (result->pl_parse_info_.is_pl_parse_) {
    dup_expr_string($$, result, @1.first_column, @3.last_column);
  }
}
| expr OR_OP expr %prec OR
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_OR, 2, $1, $3);
  if (result->pl_parse_info_.is_pl_parse_) {
    dup_expr_string($$, result, @1.first_column, @3.last_column);
  }
}
| expr XOR expr %prec XOR
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_XOR, 2, $1, $3);
  if (result->pl_parse_info_.is_pl_parse_) {
    dup_expr_string($$, result, @1.first_column, @3.last_column);
  }
}
| NOT expr %prec NOT
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT, 1, $2);
  if (result->pl_parse_info_.is_pl_parse_) {
    dup_expr_string($$, result, @1.first_column, @2.last_column);
  }
}
| bool_pri IS BOOL_VALUE %prec IS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS, 2, $1, $3);
  if (result->pl_parse_info_.is_pl_parse_) {
    dup_expr_string($$, result, @1.first_column, @3.last_column);
  }
}
| bool_pri IS not BOOL_VALUE %prec IS
{
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4);
  if (result->pl_parse_info_.is_pl_parse_) {
    dup_expr_string($$, result, @1.first_column, @4.last_column);
  }
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
  if (result->pl_parse_info_.is_pl_parse_) {
    dup_expr_string($$, result, @1.first_column, @3.last_column);
  }
}
| bool_pri IS not UNKNOWN %prec IS
{
  (void)($3);
  ParseNode *node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_DEFAULT_NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS_NOT, 2, $1, node);
  if (result->pl_parse_info_.is_pl_parse_) {
    dup_expr_string($$, result, @1.first_column, @4.last_column);
  }
}
| bool_pri %prec LOWER_THAN_COMP
{
  $$ = $1;
  const char *str = $$->str_value_;
  if (T_VARCHAR == $$->type_ && 0 == $$->str_len_) {
    //空串不做拷贝
  } else if (NULL == str && result->pl_parse_info_.is_pl_parse_) {
    dup_expr_string($$, result, @1.first_column, @1.last_column);
  }
}
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
| JSON_ARRAYAGG '(' opt_distinct_or_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_JSON_ARRAYAGG, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| JSON_OBJECTAGG '(' expr ',' expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_JSON_OBJECTAGG, 2, $3, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
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
| NTH_VALUE '(' expr ',' simple_expr ')' opt_from_first_or_last opt_respect_or_ignore_nulls OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_FUN_NTH_VALUE, 4, $3, $5, $7, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| TOP_K_FRE_HIST '(' DECIMAL_VAL ',' bit_expr  ','  INTNUM ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_TOP_FRE_HIST, 3, $3, $5, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $10);
}
| HYBRID_HIST '(' bit_expr ',' INTNUM ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_HYBRID_HIST, 2, $3, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $8);
}
| BIT_AND '(' opt_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_BIT_AND, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| BIT_OR '(' opt_all expr ')' OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_BIT_OR, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
}
| BIT_XOR '(' opt_all expr ')'OVER new_generalized_window_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_BIT_XOR, 2, $3, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WINDOW_FUNCTION, 2, $$, $7);
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
INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_INTERVAL, 1, $1);
  $$->value_ = 1;
}
| QUESTIONMARK
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_INTERVAL, 1, $1);
  $$->value_ = 1;
}
| DECIMAL_VAL
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_INTERVAL, 1, $1);
  $$->value_ = 1;
}
| UNBOUNDED
{
  get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_INTERVAL, 1, $$);
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
| JSON_ARRAYAGG '(' opt_distinct_or_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_JSON_ARRAYAGG, 2, $3, $4);
}
| JSON_OBJECTAGG  '(' expr ',' expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_JSON_OBJECTAGG, 2, $3, $5);
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
| BIT_AND '(' opt_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_BIT_AND, 2, $3, $4);
}
| BIT_OR '(' opt_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_BIT_OR, 2, $3, $4);
}
| BIT_XOR '(' opt_all expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_BIT_XOR, 2, $3, $4);
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
| TOP_K_FRE_HIST '(' DECIMAL_VAL ',' bit_expr  ','  INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_TOP_FRE_HIST, 3, $3, $5, $7);
}
| HYBRID_HIST '(' bit_expr ',' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_HYBRID_HIST, 2, $3, $5);
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
| cur_user_func
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
| utc_time_func
{
  $$ = $1;
}
| utc_date_func
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
| DAY '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "day");
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
| TIMESTAMP '(' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $3);
  make_name_node($$, result->malloc_pool_, "timestamp");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| TIMESTAMP '(' expr ',' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "timestamp");
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
| GET_FORMAT '(' get_format_unit ',' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "get_format");
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
  malloc_non_terminal_node(params_node, result->malloc_pool_, T_LINK_NODE, 2, $3, charset_node);
  merge_nodes(params_node, result, T_EXPR_LIST, params_node);

  make_name_node($$, result->malloc_pool_, "char");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params_node);

}
| CHARACTER '(' expr_list USING charset_name')'
{
  ParseNode *params_node = NULL;
  malloc_non_terminal_node(params_node, result->malloc_pool_, T_LINK_NODE, 2, $3, $5);
  merge_nodes(params_node, result, T_EXPR_LIST, params_node);

  make_name_node($$, result->malloc_pool_, "char");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params_node);
}
| LOG '(' expr ',' expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "log");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| LOG '(' expr ')'
{
  ParseNode *param_node = NULL;
  malloc_terminal_node(param_node, result->malloc_pool_, T_SFU_DOUBLE);
  int64_t len = strlen("2.718281828459045");
  param_node->str_value_ = parse_strndup("2.718281828459045", len, result->malloc_pool_);
  param_node->is_hidden_const_ = 1;
  if (OB_UNLIKELY(NULL == param_node->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
  param_node->str_len_ = len;
  ParseNode *param_list_node = NULL;
  malloc_non_terminal_node(param_list_node, result->malloc_pool_, T_LINK_NODE, 2, param_node, $3);
  merge_nodes(param_list_node, result, T_EXPR_LIST, param_list_node);
  make_name_node($$, result->malloc_pool_, "log");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, param_list_node);
}
| LN '(' expr ')'
{
  ParseNode *param_node = NULL;
  malloc_terminal_node(param_node, result->malloc_pool_, T_SFU_DOUBLE);
  int64_t len = strlen("2.718281828459045");
  param_node->str_value_ = parse_strndup("2.718281828459045", len, result->malloc_pool_);
  param_node->is_hidden_const_ = 1;
  if (OB_UNLIKELY(NULL == param_node->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
  param_node->str_len_ = len;
  ParseNode *param_list_node = NULL;
  malloc_non_terminal_node(param_list_node, result->malloc_pool_, T_LINK_NODE, 2, param_node, $3);
  merge_nodes(param_list_node, result, T_EXPR_LIST, param_list_node);
  make_name_node($$, result->malloc_pool_, "log");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, param_list_node);
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
  ParseNode *params = NULL;
  ParseNode *function = NULL;
  ParseNode *sub_obj_access_ref = NULL;
  ParseNode *udf_node = NULL;
  if (NULL != $5)
  {
    merge_nodes(params, result, T_EXPR_LIST, $5);
  }
  malloc_non_terminal_node(function, result->malloc_pool_, T_FUN_SYS, 2, $3, params);
  malloc_non_terminal_node(sub_obj_access_ref, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, function, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, $1, sub_obj_access_ref);
  malloc_non_terminal_node(udf_node, result->malloc_pool_, T_FUN_UDF, 4, $3, params, $1, NULL);
  store_pl_ref_object_symbol(udf_node, result, REF_FUNC);
}
| sys_interval_func
{
  $$ = $1;
}
| CALC_PARTITION_ID '(' bit_expr ',' bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
  make_name_node($$, result->malloc_pool_, "calc_partition_id");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| CALC_PARTITION_ID '(' bit_expr ',' bit_expr ',' bit_expr ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 3, $3, $5, $7);
  make_name_node($$, result->malloc_pool_, "calc_partition_id");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| WEIGHT_STRING '(' expr opt_ws_as_char opt_ws_levels ')'
{
  ParseNode *zeroNode1 = NULL;
  malloc_terminal_node(zeroNode1, result->malloc_pool_, T_INT);
  zeroNode1->value_ = 0;
  zeroNode1->is_hidden_const_ = 1;

  if($4->value_ > 0){
    $5->value_ |= OB_STRXFRM_PAD_WITH_SPACE;
  }

  ParseNode *falseNode = NULL;
  malloc_terminal_node(falseNode, result->malloc_pool_, T_INT);
  falseNode->value_ = 0;
  falseNode->is_hidden_const_ = 1;

  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST , 5, $3 , zeroNode1 , $4, $5 ,falseNode);
  make_name_node($$, result->malloc_pool_, "weight_string");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| WEIGHT_STRING '(' expr AS BINARY ws_nweights ')'
{
  ParseNode *zeroNode1 = NULL;
  malloc_terminal_node(zeroNode1, result->malloc_pool_, T_INT);
  zeroNode1->value_ = 0;
  zeroNode1->is_hidden_const_ = 1;

  ParseNode *padNode = NULL;
  malloc_terminal_node(padNode, result->malloc_pool_, T_INT);
  padNode->value_ = OB_STRXFRM_PAD_WITH_SPACE;
  padNode->is_hidden_const_ = 1;

  ParseNode *trueNode = NULL;
  malloc_terminal_node(trueNode, result->malloc_pool_, T_INT);
  trueNode->value_ = 1;
  trueNode->is_hidden_const_ = 1;

  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST , 5, $3 , zeroNode1 , $6, padNode ,trueNode);
  make_name_node($$, result->malloc_pool_, "weight_string");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| WEIGHT_STRING '(' expr ',' INTNUM ',' INTNUM ',' INTNUM ',' INTNUM ')'
{
  ParseNode *params = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST , 5, $3 , $5 , $7, $9 ,$11);
  make_name_node($$, result->malloc_pool_, "weight_string");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $$, params);
}
| json_value_expr
{
  $$ = $1;
}
| POINT '(' expr ',' expr ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_POINT, 2, $3, $5);
}
| LINESTRING '(' expr_list ')'
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_LINESTRING, 1, expr_list);
}
| MULTIPOINT '(' expr_list ')'
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_MULTIPOINT, 1, expr_list);
}
| MULTILINESTRING '(' expr_list ')'
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_MULTILINESTRING, 1, expr_list);
}
| POLYGON '(' expr_list ')'
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_POLYGON, 1, expr_list);
}
| MULTIPOLYGON '(' expr_list ')'
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_MULTIPOLYGON, 1, expr_list);
}
| GEOMETRYCOLLECTION '(' expr_list ')'
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_GEOMCOLLECTION, 1, expr_list);
}
| GEOMETRYCOLLECTION '(' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_GEOMCOLLECTION, 1, NULL);
}
| GEOMCOLLECTION '(' expr_list ')'
{
  ParseNode *expr_list = NULL;
  merge_nodes(expr_list, result, T_EXPR_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_GEOMCOLLECTION, 1, expr_list);
}
| GEOMCOLLECTION '(' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_GEOMCOLLECTION, 1, NULL);
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
| CHECK '(' expr ')'
{
  // just compatible with mysql, do nothing
  (void)($3);
  malloc_terminal_node($$, result->malloc_pool_, T_EMPTY);
}
;

utc_timestamp_func:
UTC_TIMESTAMP
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_UTC_TIMESTAMP, 1, NULL);
}
| UTC_TIMESTAMP '(' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_UTC_TIMESTAMP, 1, NULL);
}
| UTC_TIMESTAMP '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_UTC_TIMESTAMP, 1, $3);
}
;

utc_time_func:
UTC_TIME
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_UTC_TIME, 1, NULL);
}
| UTC_TIME '(' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_UTC_TIME, 1, NULL);
}
| UTC_TIME '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_UTC_TIME, 1, $3);
}
;

utc_date_func:
UTC_DATE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_UTC_DATE, 1, NULL);
}
| UTC_DATE '(' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_UTC_DATE, 1, NULL);
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
    params->sql_str_off_ = @$.first_column;
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
    params->sql_str_off_ = @1.first_column;
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIME, 1, params);
  }
  else
  {
    malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CUR_TIME, 1, NULL);
  }
}
;

cur_user_func:
CURRENT_USER
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CURRENT_USER, 1, NULL);
}
| CURRENT_USER '(' ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_CURRENT_USER, 1, NULL);
}
| relation_name '.' CURRENT_USER '(' ')'
{
  ParseNode *name_node = NULL;
  ParseNode *function = NULL;
  ParseNode *sub_obj_access_ref = NULL;
  ParseNode *udf_node = NULL;
  make_name_node(name_node, result->malloc_pool_, "current_user");
  malloc_non_terminal_node(function, result->malloc_pool_, T_FUN_SYS, 2, name_node, NULL);
  malloc_non_terminal_node(sub_obj_access_ref, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, function, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OBJ_ACCESS_REF, 2, $1, sub_obj_access_ref);
  malloc_non_terminal_node(udf_node, result->malloc_pool_, T_FUN_UDF, 4, $3, NULL, $1, NULL);
  store_pl_ref_object_symbol(udf_node, result, REF_FUNC);
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
opt_ws_as_char:
/* EMPTY */
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->is_hidden_const_ = 1;
  $$->value_ = 0;
  $$->param_num_ = 1;
}
| AS CHARACTER ws_nweights
{
  $$ = $3;
}
;

opt_ws_levels:
/* EMPTY */
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->is_hidden_const_ = 1;
  $$->value_ = 0;
  $$->param_num_ = 1;
}
| LEVEL ws_level_list_or_range
{
  (void)($1);
  $$ = $2;
}
;

ws_level_list_or_range:
ws_level_list
{
  $$ = $1;
}
| ws_level_range
{
  $$ = $1;
}
;

ws_level_list:
ws_level_list_item
{
  $$ = $1;
}
| ws_level_list ',' ws_level_list_item
{
  malloc_terminal_node($$, result->malloc_pool_, T_WEIGHT_STRING_LEVEL_PARAM);
  $$->value_ = $3->value_ | $1->value_;
  $$->param_num_ = $1->param_num_ + $3->param_num_;
  $$->sql_str_off_ = $1->sql_str_off_;
}
;

ws_level_list_item:
ws_level_number ws_level_flags
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = (1 | $2->value_) << $1->value_ ;
  $$->param_num_ = 1;
  $$->sql_str_off_ = $1->sql_str_off_;
}
;

ws_level_range:
ws_level_number '-' ws_level_number
{
  malloc_terminal_node($$, result->malloc_pool_, T_WEIGHT_STRING_LEVEL_PARAM);
  $$->sql_str_off_ = $1->sql_str_off_;
  uint32_t res = 0;
  uint32_t start = $1->value_ ;
  uint32_t end = $3->value_ ;
  if (end < start) {
    end = start;
  }
  for ( ; start <= end; start++) {
    res |= (1 << start);
  }
  $$->value_ = res;
  $$->param_num_ = 2;
}
;

ws_level_number:
INTNUM
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->sql_str_off_ = $1->sql_str_off_;
  if ($1->value_ < 1) {
    $$->value_ = 1;
  } else if ($1->value_ > OB_STRXFRM_NLEVELS) {
    $$->value_ = OB_STRXFRM_NLEVELS;
  } else{
    $$->value_ = $1->value_;
  }
  $$->value_ =  $$->value_ - 1;
  $$->param_num_ = 1;
}
;

ws_level_flags:
/* empty */
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_  = 0;
  $$->param_num_ = 1;
}
| ws_level_flag_desc
{
  $$= $1;
}
| ws_level_flag_desc ws_level_flag_reverse
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = $1->value_ | $2->value_;
  $$->param_num_ = 1;
  $$->sql_str_off_ = $1->sql_str_off_;
}
| ws_level_flag_reverse
{
  $$ = $1 ;
}
;

ws_nweights:
'(' INTNUM ')'
{
  if ($2->value_ < 1) {
    yyerror(&@1, result, "Incorrect arguments to WEIGHT_STRING()\n");
    YYABORT_PARSE_SQL_ERROR;
  }
  $$ = $2;
}
;

ws_level_flag_desc:
ASC
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
  $$->param_num_ = 1;
}
| DESC
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1 << OB_STRXFRM_DESC_SHIFT;
  $$->param_num_ = 1;
}
;

ws_level_flag_reverse:
REVERSE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1 << OB_STRXFRM_REVERSE_SHIFT;
  $$->param_num_ = 1;
}
;


/*****************************************************************************
 *
 *	delete grammar
 *
 *****************************************************************************/
delete_stmt:
delete_basic_stmt
{
  $$ = $1;
}
| with_clause delete_basic_stmt
{
  $$ = $2;
  $$->children_[0] = $1; /* with clause */
}
;

delete_basic_stmt:
delete_with_opt_hint FROM tbl_name opt_where opt_order_by opt_limit_clause
{
  ParseNode *from_list = NULL;
  ParseNode *delete_table_node = NULL;
  merge_nodes(from_list, result, T_TABLE_REFERENCES, $3);
  malloc_non_terminal_node(delete_table_node, result->malloc_pool_, T_DELETE_TABLE_NODE, 2,
                           NULL, /*0. delete list*/
                           from_list);    /*1. from list*/
  malloc_non_terminal_node($$, result->malloc_pool_, T_DELETE, 9,
                           NULL,                /* 0. with clause*/
                           delete_table_node,   /* 1. table_node */
                           $4,      /* 2. where      */
                           $5,      /* 3. order by   */
                           $6,      /* 4. limit      */
                           NULL,    /* 5. when       */
                           $1,      /* 6. hint       */
                           NULL,    /* 7. returning, unused in mysql  */
                           NULL);   /* 8. error logging, unused in mysql  */

}
| delete_with_opt_hint multi_delete_table opt_where
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DELETE, 9,
                           NULL, /* 0. with clause */
                           $2,   /* 1. table_node */
                           $3,        /* 2. where      */
                           NULL,      /* 3. order by   */
                           NULL,      /* 4. limit      */
                           NULL,      /* 5. when       */
                           $1,        /* 6. hint       */
                           NULL,      /* 7. returning, unused in mysql  */
                           NULL);     /* 8. error logging, unused in mysql  */
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
update_basic_stmt
{
  $$ = $1;
}
| with_clause update_basic_stmt
{
  $$ = $2;
  $$->children_[0] = $1; /* with clause */
}
;

update_basic_stmt:
update_with_opt_hint opt_ignore table_references SET update_asgn_list opt_where opt_order_by opt_limit_clause
{
  ParseNode *from_list = NULL;
  ParseNode *assign_list = NULL;
  merge_nodes(from_list, result, T_TABLE_REFERENCES, $3);
  merge_nodes(assign_list, result, T_ASSIGN_LIST, $5);

  malloc_non_terminal_node($$, result->malloc_pool_, T_UPDATE, 11,
                           NULL,          /* 0. with clause*/
                           from_list,     /* 1. table node */
                           assign_list,   /* 2. update list */
                           $6,            /* 3. where node */
                           $7,            /* 4. order by node */
                           $8,            /* 5. limit node */
                           NULL,          /* 7. when node */
                           $1,            /* 8. hint node */
                           $2,            /* 9. ignore */
                           NULL,          /* 10. returning, unused in mysql */
                           NULL);         /* 11. error  logging caluse */
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
no_param_column_ref COMP_EQ expr_or_default
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
create_with_opt_hint RESOURCE UNIT opt_if_not_exists relation_name opt_resource_unit_option_list
{
  ParseNode *resource_options = NULL;
  (void)($1);
  merge_nodes(resource_options, result, T_RESOURCE_UNIT_OPTION_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_RESOURCE_UNIT, 3,
                           $4,
                           $5,                     /* resource unit name */
                           resource_options);      /* resource opt */
}
| create_with_opt_hint RESOURCE POOL opt_if_not_exists relation_name opt_create_resource_pool_option_list
{
  ParseNode *resource_options = NULL;
  (void)($1);
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
| MAX_CPU opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_CPU, 1, $3);
}
| MEMORY_SIZE opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MEMORY_SIZE, 1, $3);
}
| MAX_IOPS opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_IOPS, 1, $3);
}
| IOPS_WEIGHT opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_IOPS_WEIGHT, 1, $3);
}
| LOG_DISK_SIZE opt_equal_mark conf_const
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOG_DISK_SIZE, 1, $3);
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

id_list:
INTNUM
{
  $$ = $1;
}
|
id_list ',' INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

opt_shrink_unit_option:
DELETE UNIT opt_equal_mark '(' id_list ')'
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
| ALTER RESOURCE TENANT relation_name UNIT_NUM opt_equal_mark INTNUM opt_shrink_tenant_unit_option
{
  (void)($6); /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_RESOURCE_TENANT, 3,
                           $4,                     /* tenant name */
                           $7,                     /* new unit num */
                           $8);                    /* shrink tenant unit option */
}
;

opt_shrink_tenant_unit_option:
DELETE UNIT_GROUP opt_equal_mark '(' id_list ')'
{
  (void)($3); /* make bison mute */
  merge_nodes($$, result, T_UNIT_GROUP_ID_LIST, $5);
}
| /* EMPTY */
{
  $$ = NULL;
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
create_with_opt_hint TENANT opt_if_not_exists relation_name
opt_tenant_option_list opt_set_sys_var
{
  ParseNode *tenant_options = NULL;
  (void)($1);
  merge_nodes(tenant_options, result, T_TENANT_OPTION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TENANT, 4,
                           $3,                   /* if not exists */
                           $4,                   /* tenant name */
                           tenant_options,      /* tenant opt */
                           $6);      /* system variable set opt */
};

create_standby_tenant_stmt:
CREATE STANDBY TENANT opt_if_not_exists relation_name
log_restore_source_option opt_tenant_option_list
{
  ParseNode *tenant_options = NULL;
  merge_nodes(tenant_options, result, T_TENANT_OPTION_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_STANDBY_TENANT, 4,
                           $4,                   /* if not exists */
                           $5,                   /* tenant name */
                           $6,                   /* log_restore_source */
                           tenant_options);      /* tenant opt */
};

log_restore_source_option:
LOG_RESTORE_SOURCE opt_equal_mark conf_const
{
  UNUSED($2);
  merge_nodes($$, result, T_LOG_RESTORE_SOURCE, $3);
}
| /*empty*/
{
  $$ = NULL;
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
| ENABLE_ARBITRATION_SERVICE opt_equal_mark BOOL_VALUE
{
  (void)($2) ;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ENABLE_ARBITRATION_SERVICE, 1, $3);
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
  $$->sql_str_off_ = $3->sql_str_off_;
}
| COLLATE opt_equal_mark collation_name
{
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_COLLATION);
  $$->str_value_ = $3->str_value_;
  $$->str_len_ = $3->str_len_;
  $$->param_num_ = $3->param_num_;
  $$->sql_str_off_ = $3->sql_str_off_;
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
| ENABLE_EXTENDED_ROWID opt_equal_mark BOOL_VALUE
{
  (void)($2); /*  make bison mute*/
  malloc_non_terminal_node($$, result->malloc_pool_, T_ENABLE_EXTENDED_ROWID, 1, $3);
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
create_with_opt_hint RESTORE POINT relation_name
{
  (void)($1);
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
create_with_opt_hint DATABASE database_factor opt_database_option_list
{
  (void)($1);
  ParseNode *database_option = NULL;
  merge_nodes(database_option, result, T_DATABASE_OPTION_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_DATABASE, 3, NULL, $3, database_option);
}
| create_with_opt_hint SCHEMA database_factor opt_database_option_list
{
  (void)($1);
  ParseNode *database_option = NULL;
  merge_nodes(database_option, result, T_DATABASE_OPTION_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_DATABASE, 3, NULL, $3, database_option);
}
| create_with_opt_hint DATABASE IF not EXISTS database_factor opt_database_option_list
{
  (void)($1);
  (void)($4);
  ParseNode *database_option = NULL;
  merge_nodes(database_option, result, T_DATABASE_OPTION_LIST, $7);
  malloc_terminal_node($$, result->malloc_pool_, T_IF_NOT_EXISTS);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_DATABASE, 3, $$, $6, database_option);
}
| create_with_opt_hint SCHEMA IF not EXISTS database_factor opt_database_option_list
{
  (void)($1);
  (void)($4);
  ParseNode *database_option = NULL;
  merge_nodes(database_option, result, T_DATABASE_OPTION_LIST, $7);
  malloc_terminal_node($$, result->malloc_pool_, T_IF_NOT_EXISTS);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_DATABASE, 3, $$, $6, database_option);
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

databases_expr:
DATABASES opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DATABASE_LIST, 1, $3);
};

opt_databases:
databases_expr
{
  $$ = $1;
}
| /*EMPTY*/
{
  $$ = NULL;
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
  $$->sql_str_off_ = $4->sql_str_off_;
}
| opt_default_mark COLLATE opt_equal_mark collation_name
{
  (void)($1);
  (void)($3);
  malloc_terminal_node($$, result->malloc_pool_, T_COLLATION);
  $$->str_value_ = $4->str_value_;
  $$->str_len_ = $4->str_len_;
  $$->param_num_ = $4->param_num_;
  $$->sql_str_off_ = $4->sql_str_off_;
}
| REPLICA_NUM opt_equal_mark INTNUM
{
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_REPLICA_NUM);
  $$->value_ = $3->value_;
  $$->sql_str_off_ = $3->sql_str_off_;
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
DROP DATABASE database_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_DATABASE, 2, NULL, $3);
}
| DROP SCHEMA database_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_DATABASE, 2, NULL, $3);
}
| DROP DATABASE IF EXISTS database_factor
{
  malloc_terminal_node($$, result->malloc_pool_, T_IF_EXISTS);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_DATABASE, 2, $$, $5);
}
| DROP SCHEMA IF EXISTS database_factor
{
  malloc_terminal_node($$, result->malloc_pool_, T_IF_EXISTS);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_DATABASE, 2, $$, $5);
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
opt_field_or_var_spec opt_load_set_spec opt_load_data_extended_option_list
{
  (void) $9;
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOAD_DATA, 12,
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
                           $1,            /* 10. hint */
                           $16            /* 11. extended option list */
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

opt_load_data_extended_option_list:
load_data_extended_option_list
{
  $$ = $1;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

load_data_extended_option_list:
load_data_extended_option
{
  $$ = $1;
}
| load_data_extended_option load_data_extended_option_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

load_data_extended_option:
LOGFILE opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOAD_DATA_ERR_FILE, 1, $3);
}
| REJECT LIMIT opt_equal_mark INTNUM
{
  (void)($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOAD_DATA_REJECT_LIMIT, 1, $4);
}
| BADFILE opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOAD_DATA_BAD_FILE, 1, $3);
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

special_table_type:
TEMPORARY
{
  malloc_terminal_node($$, result->malloc_pool_, T_TEMPORARY);
}
| EXTERNAL
{
  result->contain_sensitive_data_ = true;
  malloc_terminal_node($$, result->malloc_pool_, T_EXTERNAL);
}
| /* EMPTY */
{ $$ = NULL; }
;

/*****************************************************************************
 *
 *	CREATE TABLE LIKE grammar
 *
 *****************************************************************************/

create_table_like_stmt:
create_with_opt_hint special_table_type TABLE opt_if_not_exists relation_factor LIKE relation_factor
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE_LIKE, 4, $2, $4, $5, $7);
}
| create_with_opt_hint special_table_type TABLE opt_if_not_exists relation_factor '(' LIKE relation_factor ')'
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE_LIKE, 4, $2, $4, $5, $8);
}
;

/*****************************************************************************
 *
 *	create table grammar
 *  查询建表的语法有些啰嗦, 但为了消除移进规约冲突, 目前没想到别的方法...
 *****************************************************************************/

create_table_stmt:
create_with_opt_hint special_table_type TABLE opt_if_not_exists relation_factor '(' table_element_list ')'
opt_table_option_list opt_partition_option
{
  ParseNode *table_elements = NULL;
  ParseNode *table_options = NULL;
  (void)($1);
  merge_nodes(table_elements, result, T_TABLE_ELEMENT_LIST, $7);
  merge_nodes(table_options, result, T_TABLE_OPTION_LIST, $9);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 7,
                           $2,                   /* temporary option */
                           $4,                   /* if not exists */
                           $5,                   /* table name */
                           table_elements,       /* columns or primary key */
                           table_options,        /* table option(s) */
                           $10,                 /* partition optition */
                           NULL);               /* oracle兼容模式下存放临时表的 on commit 选项 */
  $$->reserved_ = 0;
}
| create_with_opt_hint special_table_type TABLE opt_if_not_exists relation_factor '(' table_element_list ')'
 opt_table_option_list opt_partition_option opt_as select_stmt
{
  (void)($1);
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
                           NULL,                 /* oracle兼容模式下存放临时表的 on commit 选项 */
                           $12);                 /* select_stmt */
  $$->reserved_ = 0;
}
| create_with_opt_hint special_table_type TABLE opt_if_not_exists relation_factor table_option_list opt_partition_option opt_as select_stmt
{
  (void)($1);
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
                           NULL,                 /* oracle兼容模式下存放临时表的 on commit 选项 */
                           $9);                  /* select_stmt */
  $$->reserved_ = 0;
}
| create_with_opt_hint special_table_type TABLE opt_if_not_exists relation_factor partition_option opt_as select_stmt
{
  (void)($1);
  (void)$7;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 8,
                           $2,                   /* temporary option */
                           $4,                   /* if not exists */
                           $5,                   /* table name */
                           NULL,                 /* columns or primary key */
                           NULL,                 /* table option(s) */
                           $6,                   /* partition optition */
                           NULL,                 /* oracle兼容模式下存放临时表的 on commit 选项 */
                           $8);                  /* select_stmt */
  $$->reserved_ = 1; /* mean partition optition is partition_option, not opt_partition_option*/
}
| create_with_opt_hint special_table_type TABLE opt_if_not_exists relation_factor select_stmt
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 8,
                           $2,                   /* temporary option */
                           $4,                   /* if not exists */
                           $5,                   /* table name */
                           NULL,                 /* columns or primary key */
                           NULL,                 /* table option(s) */
                           NULL,                 /* partition optition */
                           NULL,                 /* oracle兼容模式下存放临时表的 on commit 选项 */
                           $6);                  /* select_stmt */
  $$->reserved_ = 0;
}
| create_with_opt_hint special_table_type TABLE opt_if_not_exists relation_factor AS select_stmt
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 8,
                           $2,                   /* temporary option */
                           $4,                   /* if not exists */
                           $5,                   /* table name */
                           NULL,                 /* columns or primary key */
                           NULL,                 /* table option(s) */
                           NULL,                 /* partition optition */
                           NULL,                 /* oracle兼容模式下存放临时表的 on commit 选项 */
                           $7);                  /* select_stmt */
  $$->reserved_ = 0;
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
|
FIXED {
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 4;
}
|
NUMERIC
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 4;
}
;

create_function_stmt:
create_with_opt_hint opt_agg FUNCTION NAME_OB RETURNS ret_type SONAME STRING_VALUE
{
  (void)($1);
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
| constraint_definition
{
  $$ = $1;
}
| CONSTRAINT opt_constraint_name PRIMARY KEY opt_index_using_algorithm '(' column_name_list ')' opt_index_using_algorithm opt_comment
{
  (void)($2);
  ParseNode *col_list= NULL;
  merge_nodes(col_list, result, T_COLUMN_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_KEY, 3, col_list, NULL != $9 ? $9 : $5, $10);
}
| PRIMARY KEY opt_index_name opt_index_using_algorithm '(' column_name_list ')' opt_index_using_algorithm opt_comment
{
  (void)($3);
  ParseNode *col_list= NULL;
  merge_nodes(col_list, result, T_COLUMN_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_KEY, 3, col_list, NULL != $8 ? $8 : $4, $9);
}
| key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list opt_partition_option
{
  (void)($1);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $5);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX, 5, $2, col_list, index_option, $3, $8);
  $$->value_ = 0;
}
| UNIQUE opt_key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list opt_partition_option
{
  (void)($2);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $6);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX, 5, $3, col_list, index_option, $4, $9);
  $$->value_ = 1;
}
| CONSTRAINT opt_constraint_name UNIQUE opt_key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list
{
  (void)($4);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $8);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX, 5, $5 ? $5 : $2, col_list, index_option, $6, NULL);
  $$->value_ = 1;
}
| CONSTRAINT opt_constraint_name FOREIGN KEY opt_index_name '(' column_name_list ')' REFERENCES relation_factor '(' column_name_list ')' opt_match_option opt_reference_option_list
{
  ParseNode *child_col_list= NULL;
  ParseNode *parent_col_list= NULL;
  ParseNode *reference_option_list = NULL;
  ParseNode *constraint_node = NULL;
  merge_nodes(child_col_list, result, T_COLUMN_LIST, $7);
  merge_nodes(parent_col_list, result, T_COLUMN_LIST, $12);
  merge_nodes(reference_option_list, result, T_REFERENCE_OPTION_LIST, $15);
  malloc_non_terminal_node(constraint_node, result->malloc_pool_, T_CHECK_CONSTRAINT, 1, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOREIGN_KEY, 7, child_col_list, $10, parent_col_list, reference_option_list, constraint_node, $5, $14);
}
| SPATIAL opt_key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list
{
  (void)($2);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $6);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX, 5, $3, col_list, index_option, $4, NULL);
  $$->value_ = 2;
}
| FOREIGN KEY opt_index_name '(' column_name_list ')' REFERENCES relation_factor '(' column_name_list ')' opt_match_option opt_reference_option_list
{
  ParseNode *child_col_list= NULL;
  ParseNode *parent_col_list= NULL;
  ParseNode *reference_option_list = NULL;
  merge_nodes(child_col_list, result, T_COLUMN_LIST, $5);
  merge_nodes(parent_col_list, result, T_COLUMN_LIST, $10);
  merge_nodes(reference_option_list, result, T_REFERENCE_OPTION_LIST, $13);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOREIGN_KEY, 7, child_col_list, $8, parent_col_list, reference_option_list, NULL, $3, $12);
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
| CHECK '(' expr ')'
{
  // just compatible with mysql, do nothing
  (void)($3);
  malloc_terminal_node($$, result->malloc_pool_, T_EMPTY);
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
column_definition_ref data_type opt_column_attribute_list opt_position_column
{
  ParseNode *attributes = NULL;
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $3);
  set_data_type_collation($2, attributes, false, true);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 4, $1, $2, attributes, $4);
}
| column_definition_ref data_type opt_collation opt_generated_keyname AS '(' expr ')' opt_storage_type opt_generated_column_attribute_list opt_position_column
{
  ParseNode *attributes = NULL;
  set_data_type_collation($2, $3, true, false);
  dup_expr_string($7, result, @7.first_column, @7.last_column);
  merge_nodes(attributes, result, T_COLUMN_ATTRIBUTES, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 7, $1, $2, attributes, $7, $9, $11, $4);
}
;

constraint_definition:
CONSTRAINT opt_constraint_name CHECK '(' expr ')' check_state
{
  ParseNode *constraint_node = NULL;
  dup_expr_string($5, result, @5.first_column, @5.last_column);
  malloc_non_terminal_node(constraint_node, result->malloc_pool_, T_CHECK_CONSTRAINT, 1, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 3, constraint_node, $5, $7);
  $$->value_ = 1;
}
| CHECK '(' expr ')' check_state
{
  dup_expr_string($3, result, @3.first_column, @3.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 3, NULL, $3, $5);
  $$->value_ = 1;
}
| CONSTRAINT opt_constraint_name CHECK '(' expr ')' %prec LOWER_PARENS
{
  ParseNode *constraint_node = NULL;
  dup_expr_string($5, result, @5.first_column, @5.last_column);
  malloc_terminal_node($$, result->malloc_pool_, T_ENFORCED_CONSTRAINT);
  malloc_non_terminal_node(constraint_node, result->malloc_pool_, T_CHECK_CONSTRAINT, 1, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 3, constraint_node, $5, $$);
  $$->value_ = 1;
}
| CHECK '(' expr ')' %prec LOWER_PARENS
{
  dup_expr_string($3, result, @3.first_column, @3.last_column);
  malloc_terminal_node($$, result->malloc_pool_, T_ENFORCED_CONSTRAINT);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 3, NULL, $3, $$);
  $$->value_ = 1;
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
  $$->sql_str_off_ = $2->sql_str_off_;
}
| NULLX
{
  (void)($1) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NULL);
  $$->sql_str_off_ = $1->sql_str_off_;
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
| constraint_definition
{
  $$ = $1;
}
| SRID INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_SRID, 1, $2);
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

cast_data_type:
BINARY opt_string_length_i_v2
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_CHAR; /* data type */
  $$->int16_values_[OB_NODE_CAST_COLL_IDX] = BINARY_COLLATION; /* is binary */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = $2[0];        /* length */
  $$->param_num_ = $2[1];
  $$->sql_str_off_ = @1.first_column;
}
| CHARACTER opt_string_length_i_v2 opt_binary
{
  (void)($3);
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_CHAR;//to keep consitent with mysql
  $$->int16_values_[OB_NODE_CAST_COLL_IDX] = INVALID_COLLATION;        /* is char */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = $2[0];        /* length */
  $$->param_num_ = $2[1]; /* opt_binary的常数个数一定为0 */
  $$->sql_str_off_ = @1.first_column;
}
| CHARACTER opt_string_length_i_v2 charset_key charset_name
{
  (void)($3);
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_CHAR; /* data type */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = $2[0];        /* length */
  $$->param_num_ = $2[1] + $4->param_num_;
  $$->str_value_ = $4->str_value_;
  $$->str_len_ = $4->str_len_;
  $$->sql_str_off_ = @1.first_column;
}
| cast_datetime_type_i opt_datetime_fsp_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = $1[0];
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2[0];
  $$->param_num_ = $1[1] + $2[1];
  $$->sql_str_off_ = @1.first_column;
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
    $$->sql_str_off_ = $2->sql_str_off_;
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
    $$->sql_str_off_ = $2->sql_str_off_;
  }
}
| FIXED opt_number_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NUMBER;
  if (NULL != $2) {
    $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = $2->int16_values_[0];    /* precision */
    $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2->int16_values_[1];    /* scale */
    $$->param_num_ = $2->param_num_;
    $$->sql_str_off_ = $2->sql_str_off_;
  }
}
| NUMERIC opt_number_precision
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NUMBER;
  if (NULL != $2) {
    $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = $2->int16_values_[0];    /* precision */
    $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = $2->int16_values_[1];    /* scale */
    $$->param_num_ = $2->param_num_;
    $$->sql_str_off_ = $2->sql_str_off_;
  }
}
| SIGNED opt_integer
{
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_INT;
  $$->param_num_ = $2[1];
  $$->sql_str_off_ = @1.first_column;
}
| UNSIGNED opt_integer
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_UINT64;
  $$->param_num_ = $2[1];
  $$->sql_str_off_ = @1.first_column;
}
| DOUBLE
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_DOUBLE;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = -1;    /* precision */
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = -1;    /* scale */
  $$->param_num_ = 0;
  $$->sql_str_off_ = @1.first_column;
}
| FLOAT opt_cast_float_precision
{ /* If p is provided and 0 <= < p <= 24, the result is of type FLOAT. */
  /* If 25 <= p <= 53, the result is of type DOUBLE. If p < 0 or p > 53, an error is returned. */
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_FLOAT;
  $$->int16_values_[OB_NODE_CAST_N_PREC_IDX] = $2[0];  /* precision */
  $$->int16_values_[OB_NODE_CAST_N_SCALE_IDX] = -1;    /* scale */
  $$->param_num_ = $2[1]; /* param only use to choose float or double convert */
  $$->sql_str_off_ = @1.first_column;
}
| JSON
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_JSON; /* data type */
  $$->int16_values_[OB_NODE_CAST_COLL_IDX] = INVALID_COLLATION;
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = 0;        /* length */
  $$->param_num_ = 0;
  $$->sql_str_off_ = @1.first_column;
}
| POINT
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_GEOMETRY; /* data type */
  $$->int16_values_[OB_NODE_CAST_GEO_TYPE_IDX] = 1;      /* point */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = 0;         /* length */
  $$->param_num_ = 0;
  $$->sql_str_off_ = @1.first_column;
}
| LINESTRING
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_GEOMETRY; /* data type */
  $$->int16_values_[OB_NODE_CAST_GEO_TYPE_IDX] = 2;      /* linestring */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = 0;         /* length */
  $$->param_num_ = 0;
  $$->sql_str_off_ = @1.first_column;
}
| POLYGON
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_GEOMETRY; /* data type */
  $$->int16_values_[OB_NODE_CAST_GEO_TYPE_IDX] = 3;      /* polygon */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = 0;         /* length */
  $$->param_num_ = 0;
  $$->sql_str_off_ = @1.first_column;
}
| MULTIPOINT
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_GEOMETRY; /* data type */
  $$->int16_values_[OB_NODE_CAST_GEO_TYPE_IDX] = 4;      /* multipoint */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = 0;         /* length */
  $$->param_num_ = 0;
  $$->sql_str_off_ = @1.first_column;
}
| MULTILINESTRING
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_GEOMETRY; /* data type */
  $$->int16_values_[OB_NODE_CAST_GEO_TYPE_IDX] = 5;      /* multilinestring */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = 0;         /* length */
  $$->param_num_ = 0;
  $$->sql_str_off_ = @1.first_column;
}
| MULTIPOLYGON
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_GEOMETRY;  /* data type */
  $$->int16_values_[OB_NODE_CAST_GEO_TYPE_IDX] = 6;       /* multipolygon */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = 0;          /* length */
  $$->param_num_ = 0;
  $$->sql_str_off_ = @1.first_column;
}
| GEOMETRYCOLLECTION
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_GEOMETRY; /* data type */
  $$->int16_values_[OB_NODE_CAST_GEO_TYPE_IDX] = 7;      /* geometrycollection */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = 0;         /* length */
  $$->param_num_ = 0;
  $$->sql_str_off_ = @1.first_column;
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
| YEAR        { $$[0] = T_YEAR; $$[1] = 0; }
;

get_format_unit:
DATETIME
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = GET_FORMAT_DATETIME;
  $$->is_hidden_const_ = 1;
}
| TIMESTAMP
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = GET_FORMAT_DATETIME;
  $$->is_hidden_const_ = 1;
}
| DATE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = GET_FORMAT_DATE;
  $$->is_hidden_const_ = 1;
}
| TIME
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = GET_FORMAT_TIME;
  $$->is_hidden_const_ = 1;
}
;

data_type:
int_type_i opt_int_length_i opt_unsigned_i opt_zerofill_i
{
  malloc_terminal_node($$, result->malloc_pool_, ($3[0] || $4[0]) ? $1[0] + (T_UTINYINT - T_TINYINT) : $1[0]);
  $$->int16_values_[0] = $2[0];
  $$->int16_values_[1] = 0;       /* distinct int and bool */
  $$->int16_values_[2] = $4[0];   /* 2 is the same index as float or number. */
  $$->int16_values_[3] = $3[0];
  $$->sql_str_off_ = @1.first_column;
}
| float_type_i opt_float_precision opt_unsigned_i opt_zerofill_i
{
  if (T_FLOAT != $1[0] && NULL != $2 && -1 == $2->int16_values_[1]) {
    yyerror(&@2, result, "double type not support double(M) syntax\n");
    YYERROR;
  }
  malloc_terminal_node($$, result->malloc_pool_, ($3[0] || $4[0]) ? $1[0] + (T_UFLOAT - T_FLOAT) : $1[0]);
  if (NULL != $2) {
    $$->int16_values_[0] = $2->int16_values_[0];
    $$->int16_values_[1] = $2->int16_values_[1];
  }
  /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
  $$->int16_values_[3] = $3[0];
  $$->int16_values_[2] = $4[0];
  $$->sql_str_off_ = @$.first_column;
}
| NUMBER opt_number_precision opt_unsigned_i opt_zerofill_i
{
  malloc_terminal_node($$, result->malloc_pool_, ($3[0] || $4[0]) ? T_UNUMBER : T_NUMBER);
  if (NULL != $2) {
    $$->int16_values_[0] = $2->int16_values_[0];
    $$->int16_values_[1] = $2->int16_values_[1];
  }
  /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
  $$->int16_values_[3] = $3[0];
  $$->int16_values_[2] = $4[0];
  $$->sql_str_off_ = $2->sql_str_off_;
}
| DECIMAL opt_number_precision opt_unsigned_i opt_zerofill_i
{
  malloc_terminal_node($$, result->malloc_pool_, ($3[0] || $4[0]) ? T_UNUMBER : T_NUMBER);
  if (NULL != $2) {
    $$->int16_values_[0] = $2->int16_values_[0];
    $$->int16_values_[1] = $2->int16_values_[1];
  }
  /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
  $$->int16_values_[3] = $3[0];
  $$->int16_values_[2] = $4[0];
  $$->sql_str_off_ = $2->sql_str_off_;
}
| FIXED opt_number_precision opt_unsigned_i opt_zerofill_i
{
  malloc_terminal_node($$, result->malloc_pool_, ($3[0] || $4[0]) ? T_UNUMBER : T_NUMBER);
  if (NULL != $2) {
    $$->int16_values_[0] = $2->int16_values_[0];
    $$->int16_values_[1] = $2->int16_values_[1];
  }
  /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
  $$->int16_values_[3] = $3[0];
  $$->int16_values_[2] = $4[0];
  $$->sql_str_off_ = $2->sql_str_off_;
}
| NUMERIC opt_number_precision opt_unsigned_i opt_zerofill_i
{
  malloc_terminal_node($$, result->malloc_pool_, ($3[0] || $4[0]) ? T_UNUMBER : T_NUMBER);
  if (NULL != $2) {
    $$->int16_values_[0] = $2->int16_values_[0];
    $$->int16_values_[1] = $2->int16_values_[1];
  }
  /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
  $$->int16_values_[3] = $3[0];
  $$->int16_values_[2] = $4[0];
  $$->sql_str_off_ = $2->sql_str_off_;
}
| BOOL
{
  malloc_terminal_node($$, result->malloc_pool_, T_TINYINT);
  $$->int16_values_[0] = 1;
  $$->int16_values_[1] = 1;
  $$->int16_values_[2] = 0;  // zerofill always false
}
| BOOLEAN
{
  malloc_terminal_node($$, result->malloc_pool_, T_TINYINT);
  $$->int16_values_[0] = 1;
  $$->int16_values_[1] = 2;
  $$->int16_values_[2] = 0; // zerofill always false
}
| datetime_type_i opt_datetime_fsp_i
{
  malloc_terminal_node($$, result->malloc_pool_, $1[0]);
  $$->int16_values_[1] = $2[0];
  $$->sql_str_off_ = @1.first_column;
}
| date_year_type_i
{
  malloc_terminal_node($$, result->malloc_pool_, $1[0]);
  $$->sql_str_off_ = @1.first_column;
}
| CHARACTER opt_string_length_i opt_binary opt_charset
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHAR, 3, $4, NULL, $3);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 0; /* is char */
  $$->sql_str_off_ = @1.first_column;
}
| NCHAR opt_string_length_i opt_binary
{
  ParseNode *charset_node = NULL;
  ParseNode *charset_name = NULL;
  malloc_terminal_node(charset_name, result->malloc_pool_, T_VARCHAR);
  malloc_terminal_node(charset_node, result->malloc_pool_, T_CHARSET);
  charset_name->str_value_ = parse_strdup("utf8mb4", result->malloc_pool_, &(charset_name->str_len_));
  if (OB_UNLIKELY(NULL == charset_name->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
  charset_name->type_ = T_CHAR_CHARSET;
  charset_name->param_num_ = 0;
  charset_name->is_hidden_const_ = 1;
  charset_node->str_value_ = charset_name->str_value_;
  charset_node->str_len_ = charset_name->str_len_;
  charset_node->sql_str_off_ = charset_name->sql_str_off_;

  malloc_non_terminal_node($$, result->malloc_pool_, T_CHAR, 3, charset_node, NULL, $3);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 0; /* is char */
  $$->sql_str_off_ = @1.first_column;
}
| NATIONAL CHARACTER opt_string_length_i opt_binary
{
  ParseNode *charset_node = NULL;
  ParseNode *charset_name = NULL;
  malloc_terminal_node(charset_name, result->malloc_pool_, T_VARCHAR);
  malloc_terminal_node(charset_node, result->malloc_pool_, T_CHARSET);
  charset_name->str_value_ = parse_strdup("utf8mb4", result->malloc_pool_, &(charset_name->str_len_));
  if (OB_UNLIKELY(NULL == charset_name->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
  charset_name->type_ = T_CHAR_CHARSET;
  charset_name->param_num_ = 0;
  charset_name->is_hidden_const_ = 1;
  charset_node->str_value_ = charset_name->str_value_;
  charset_node->str_len_ = charset_name->str_len_;
  charset_node->sql_str_off_ = charset_name->sql_str_off_;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHAR, 3, charset_node, NULL, $4);
  $$->int32_values_[0] = $3[0];
  $$->int32_values_[1] = 0; /* is char */
  $$->sql_str_off_ = @1.first_column;
}

/*  | TEXT opt_binary opt_charset
//  {
//    (void)($2);
//    malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 3, $3, $4, $2);
//    $$->int32_values_[0] = 256;
//    $$->int32_values_[1] = 0; /* is char */
/*  }*/
| VARCHAR string_length_i opt_binary opt_charset
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 3, $4, NULL, $3);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 0; /* is char */
}
| NCHAR VARCHAR string_length_i opt_binary
{
  ParseNode *charset_node = NULL;
  ParseNode *charset_name = NULL;
  malloc_terminal_node(charset_name, result->malloc_pool_, T_VARCHAR);
  malloc_terminal_node(charset_node, result->malloc_pool_, T_CHARSET);
  charset_name->str_value_ = parse_strdup("utf8mb4", result->malloc_pool_, &(charset_name->str_len_));
  if (OB_UNLIKELY(NULL == charset_name->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
  charset_name->type_ = T_CHAR_CHARSET;
  charset_name->param_num_ = 0;
  charset_name->is_hidden_const_ = 1;
  charset_node->str_value_ = charset_name->str_value_;
  charset_node->str_len_ = charset_name->str_len_;
  charset_node->sql_str_off_ = charset_name->sql_str_off_;

  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 3, charset_node, NULL, $4);
  $$->int32_values_[0] = $3[0];
  $$->int32_values_[1] = 0; /* is char */
}
| NVARCHAR string_length_i opt_binary
{
  ParseNode *charset_node = NULL;
  ParseNode *charset_name = NULL;
  malloc_terminal_node(charset_name, result->malloc_pool_, T_VARCHAR);
  malloc_terminal_node(charset_node, result->malloc_pool_, T_CHARSET);
  charset_name->str_value_ = parse_strdup("utf8mb4", result->malloc_pool_, &(charset_name->str_len_));
  if (OB_UNLIKELY(NULL == charset_name->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
  charset_name->type_ = T_CHAR_CHARSET;
  charset_name->param_num_ = 0;
  charset_name->is_hidden_const_ = 1;
  charset_node->str_value_ = charset_name->str_value_;
  charset_node->str_len_ = charset_name->str_len_;
  charset_node->sql_str_off_ = charset_name->sql_str_off_;

  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 3, charset_node, NULL, $3);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 0; /* is char */
}
| NATIONAL VARCHAR string_length_i opt_binary
{
  ParseNode *charset_node = NULL;
  ParseNode *charset_name = NULL;
  malloc_terminal_node(charset_name, result->malloc_pool_, T_VARCHAR);
  malloc_terminal_node(charset_node, result->malloc_pool_, T_CHARSET);
  charset_name->str_value_ = parse_strdup("utf8mb4", result->malloc_pool_, &(charset_name->str_len_));
  if (OB_UNLIKELY(NULL == charset_name->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
  charset_name->type_ = T_CHAR_CHARSET;
  charset_name->param_num_ = 0;
  charset_name->is_hidden_const_ = 1;
  charset_node->str_value_ = charset_name->str_value_;
  charset_node->str_len_ = charset_name->str_len_;
  charset_node->sql_str_off_ = charset_name->sql_str_off_;
  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 3, charset_node, NULL, $4);
  $$->int32_values_[0] = $3[0];
  $$->int32_values_[1] = 0; /* is char */
}
| CHARACTER VARYING string_length_i opt_binary opt_charset
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 3, $5, NULL, $4);
  $$->int32_values_[0] = $3[0];
  $$->int32_values_[1] = 0; /* is char */
}
| NATIONAL CHARACTER VARYING string_length_i opt_binary
{
  ParseNode *charset_node = NULL;
  ParseNode *charset_name = NULL;
  malloc_terminal_node(charset_name, result->malloc_pool_, T_VARCHAR);
  malloc_terminal_node(charset_node, result->malloc_pool_, T_CHARSET);
  charset_name->str_value_ = parse_strdup("utf8mb4", result->malloc_pool_, &(charset_name->str_len_));
  if (OB_UNLIKELY(NULL == charset_name->str_value_)) {
    yyerror(NULL, result, "No more space for mallocing string\n");
    YYABORT_NO_MEMORY;
  }
  charset_name->type_ = T_CHAR_CHARSET;
  charset_name->param_num_ = 0;
  charset_name->is_hidden_const_ = 1;
  charset_node->str_value_ = charset_name->str_value_;
  charset_node->str_len_ = charset_name->str_len_;
  charset_node->sql_str_off_ = charset_name->sql_str_off_;
  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 3, charset_node, NULL, $5);
  $$->int32_values_[0] = $4[0];
  $$->int32_values_[1] = 0; /* is char */
}
| blob_type_i opt_string_length_i_v2
{
  malloc_terminal_node($$, result->malloc_pool_, $1[0]);
  if ($1[0] != T_TEXT && $2[0] != -1) {
    yyerror(&@2, result, "not support to specify the length in parentheses\n");
    YYERROR;
  } else if (0 == $2[1]) {
    $2[0] = 0; /* change default string len from -1 to 0 for compat mysql */
  }
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 1; /* is binary */
  $$->sql_str_off_ = @1.first_column;
}
| text_type_i opt_string_length_i_v2 opt_binary opt_charset
{
  malloc_non_terminal_node($$, result->malloc_pool_, $1[0], 3, $4, NULL, $3);
  if ($1[0] != T_TEXT && $2[0] != -1) {
    yyerror(&@2, result, "not support to specify the length in parentheses\n");
    YYERROR;
  } else if (0 == $2[1]) {
    $2[0] = 0; /* change default string len from -1 to 0 for compat mysql */
  }
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
  $$->sql_str_off_ = @1.first_column;
}
| VARBINARY string_length_i
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->int32_values_[0] = $2[0];
  $$->int32_values_[1] = 1; /* is binary */
  $$->sql_str_off_ = @1.first_column;
}
| STRING_VALUE /* wrong or unsupported data type */
{
  malloc_terminal_node($$, result->malloc_pool_, T_INVALID);
  $$->str_value_ = $1->str_value_;
  $$->str_len_ = $1->str_len_;
  $$->sql_str_off_ = $1->sql_str_off_;
}
| BIT opt_bit_length_i
{
  // MAX BIT length
  if (NULL != result->pl_parse_info_.pl_ns_ && 64 < $2[0]) {
    yyerror(&@2, result, "Too big precision . Maximum is 65.\n");
    YYABORT_TOO_BIG_DISPLAYWIDTH;
  } else {
    malloc_terminal_node($$, result->malloc_pool_, T_BIT);
    $$->int16_values_[0] = $2[0];
    $$->sql_str_off_ = @1.first_column;
  }
}
| ENUM '(' string_list ')' opt_binary opt_charset
{
  ParseNode *string_list_node = NULL;
  merge_nodes(string_list_node, result, T_STRING_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ENUM, 4, $6, NULL, $5, string_list_node);
  $$->int32_values_[0] = 0;//not used so far
  $$->int32_values_[1] = 0; /* is char */
}
| SET '(' string_list ')' opt_binary opt_charset
{
  ParseNode *string_list_node = NULL;
  merge_nodes(string_list_node, result, T_STRING_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET, 4, $6, NULL, $5, string_list_node);
  $$->int32_values_[0] = 0;//not used so far
  $$->int32_values_[1] = 0; /* is char */
}
| JSON
{
  malloc_terminal_node($$, result->malloc_pool_, T_JSON);
  $$->int32_values_[0] = 0; /* length */
}
| GEOMETRY
{
  malloc_terminal_node($$, result->malloc_pool_, T_GEOMETRY);
  $$->int32_values_[0] = 0; /* length */
  $$->int32_values_[1] = 0; /* geometry, geometry uses collation type value convey sub geometry type. */
}
| POINT
{
  malloc_terminal_node($$, result->malloc_pool_, T_GEOMETRY);
  $$->int32_values_[0] = 0; /* length */
  $$->int32_values_[1] = 1; /* point, geometry uses collation type value convey sub geometry type. */
}
| LINESTRING
{
  malloc_terminal_node($$, result->malloc_pool_, T_GEOMETRY);
  $$->int32_values_[0] = 0; /* length */
  $$->int32_values_[1] = 2; /* linestring, geometry uses collation type value convey sub geometry type. */
}
| POLYGON
{
  malloc_terminal_node($$, result->malloc_pool_, T_GEOMETRY);
  $$->int32_values_[0] = 0; /* length */
  $$->int32_values_[1] = 3; /* polygon, geometry uses collation type value convey sub geometry type. */
}
| MULTIPOINT
{
  malloc_terminal_node($$, result->malloc_pool_, T_GEOMETRY);
  $$->int32_values_[0] = 0; /* length */
  $$->int32_values_[1] = 4; /* mutipoint, geometry uses collation type value convey sub geometry type. */
}
| MULTILINESTRING
{
  malloc_terminal_node($$, result->malloc_pool_, T_GEOMETRY);
  $$->int32_values_[0] = 0; /* length */
  $$->int32_values_[1] = 5; /* multilinestring, geometry uses collation type value convey sub geometry type. */
}
| MULTIPOLYGON
{
  malloc_terminal_node($$, result->malloc_pool_, T_GEOMETRY);
  $$->int32_values_[0] = 0; /* length */
  $$->int32_values_[1] = 6; /* multipolygon, geometry uses collation type value convey sub geometry type. */
}
| GEOMETRYCOLLECTION
{
  malloc_terminal_node($$, result->malloc_pool_, T_GEOMETRY);
  $$->int32_values_[0] = 0; /* length */
  $$->int32_values_[1] = 7; /* geometrycollection, geometry uses collation type value convey sub geometry type. */
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
| REAL
{
  if (SMO_REAL_AS_FLOAT & result->sql_mode_) {
    $$[0] = T_FLOAT;
  } else {
    $$[0] = T_DOUBLE;
  }
}
| DOUBLE PRECISION   { $$[0] = T_DOUBLE; }
| REAL PRECISION
{
  if (SMO_REAL_AS_FLOAT & result->sql_mode_) {
    $$[0] = T_FLOAT;
  } else {
    $$[0] = T_DOUBLE;
  }
}
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
| MEDIUMTEXT VARCHAR { $$[0] = T_MEDIUMTEXT; } /*LONG VARCHAR*/
;

blob_type_i:
TINYBLOB     { $$[0] = T_TINYTEXT; }
| BLOB   { $$[0] = T_TEXT; }
| MEDIUMBLOB   { $$[0] = T_MEDIUMTEXT; }
| LONGBLOB   { $$[0] = T_LONGTEXT;  }
| MEDIUMTEXT VARBINARY { $$[0] = T_MEDIUMTEXT; } /*LONG VARBINARY*/
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
  $$->sql_str_off_ = $2->sql_str_off_;
}
| '(' INTNUM ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  $$->int16_values_[0] = $2->value_;
  $$->int16_values_[1] = -1;
  $$->sql_str_off_ = $2->sql_str_off_;
}
| '(' DECIMAL_VAL ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  int err_no = 0;
  $2->value_ = ob_strntoll($2->str_value_, $2->str_len_, 10, NULL, &err_no);
  $$->int16_values_[0] = $2->value_;
  $$->int16_values_[1] = -1;
  $$->sql_str_off_ = $2->sql_str_off_;
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
  $$->sql_str_off_ = $2->sql_str_off_;
  $$->param_num_ = 2;
}
| '(' INTNUM ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_LINK_NODE);
  if($2->value_ > OB_MAX_PARSER_INT16_VALUE) {
    $$->int16_values_[0] = OB_MAX_PARSER_INT16_VALUE;
  } else {
    $$->int16_values_[0] = $2->value_;
    $$->sql_str_off_ = $2->sql_str_off_;
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
| /*EMPTY*/ { $$[0] = 0; $$[1] = 0; }
;

opt_cast_float_precision:
'(' INTNUM  ')' { $$[0] = $2->value_; $$[1] = 1; }
| /*EMPTY*/ { $$[0] = 0; $$[1] = 0;}
;

string_length_i:
'(' number_literal ')'
{
  // 在 `*` 处报语法错误
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
  } else if (T_UINT64 == $2->type_) {
    uint64_t value = $2->value_;
    if (value > UINT32_MAX) {
      $$[0] = OUT_OF_STR_LEN;;
    } else if (value > INT32_MAX) {
      $$[0] = DEFAULT_STR_LENGTH;
    } else {
      $$[0] = $2->value_;
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
STRING_VALUE
{
  $$ = $1;
  $$->param_num_ = 1;
  $$->is_hidden_const_ = 0;
}
;

trans_param_value:
STRING_VALUE
{
  $$ = $1;
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
  $$->type_ = T_CHAR_CHARSET;
  $$->param_num_ = 0;
  $$->is_hidden_const_ = 1;
}
| STRING_VALUE
{
  $$ = $1;
  $$->type_ = T_VARCHAR;
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
  $$->type_ = T_CHAR_CHARSET;
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
  $$->sql_str_off_ = $2->sql_str_off_;
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
  $$->sql_str_off_ = $2->sql_str_off_;
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
column_attribute_list
{
  $$ = $1;
}
| /*EMPTY*/
{ $$ = NULL; }
;

column_attribute_list:
column_attribute_list column_attribute
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| column_attribute
{
  $$ = $1;
}

column_attribute:
not NULLX
{
  (void)($1) ;
  (void)($2) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NOT_NULL);
  $$->sql_str_off_ = $2->sql_str_off_;
}
| NULLX
{
  (void)($1) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NULL);
  $$->sql_str_off_ = $1->sql_str_off_;
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
| constraint_definition
{
  $$ = $1;
}
| SRID INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_SRID, 1, $2);
}
| COLLATE collation_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_COLLATION);
  $$->str_value_ = $2->str_value_;
  $$->str_len_ = $2->str_len_;
  $$->param_num_ = $2->param_num_;
  $$->sql_str_off_ = $2->sql_str_off_;
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

tablespace:
NAME_OB
{
  $$ = $1;
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
  $$->sql_str_off_ = $4->sql_str_off_;
}
| COMMENT opt_equal_mark STRING_VALUE
{
  (void)($2); /*  make bison mute*/
  malloc_non_terminal_node($$, result->malloc_pool_, T_COMMENT, 1, $3);
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
| TABLESPACE tablespace
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLESPACE, 1, $2);
}
| parallel_option
{
  $$ = $1;
}
| DELAY_KEY_WRITE opt_equal_mark INTNUM
{
  (void)($2);
  (void) ($$);
  if ($3->value_ < 0) {
    yyerror(&@1, result, "value for DELAY_KEY_WRITE shouldn't be negative");
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_DELAY_KEY_WRITE, 1, $3);
  }
}
| AVG_ROW_LENGTH opt_equal_mark INTNUM
{
  (void)($2);
  if ($3->value_ < 0) {
    yyerror(&@1, result, "value for AVG_ROW_LENGTH shouldn't be negative");
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_AVG_ROW_LENGTH, 1, $3);
  }
}
| CHECKSUM opt_equal_mark INTNUM
{
  (void)($2);
  if ($3->value_ < 0) {
    yyerror(&@1, result, "value for CHECKSUM shouldn't be negative");
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_CHECKSUM, 1, $3);
  }
}
| AUTO_INCREMENT_MODE opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_AUTO_INCREMENT_MODE, 1, $3);
}
| ENABLE_EXTENDED_ROWID opt_equal_mark BOOL_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ENABLE_EXTENDED_ROWID, 1, $3);
}
| LOCATION opt_equal_mark STRING_VALUE
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXTERNAL_FILE_LOCATION, 1, $3);
  $3->stmt_loc_.first_column_ = @3.first_column - 1;
  $3->stmt_loc_.last_column_ = @3.last_column - 1;
}
| FORMAT opt_equal_mark '(' external_file_format_list ')'
{
  (void)($2) ; /* make bison mute */
  merge_nodes($$, result, T_EXTERNAL_FILE_FORMAT, $4);
}
| PATTERN opt_equal_mark STRING_VALUE
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXTERNAL_FILE_PATTERN, 1, $3);
}
| TTL '(' ttl_definition ')'
{
  merge_nodes($$, result, T_TTL_DEFINITION, $3);
  dup_expr_string($$, result, @3.first_column, @3.last_column);
}
| KV_ATTRIBUTES opt_equal_mark STRING_VALUE
{
  (void)($2); /*  make bison mute*/
  malloc_non_terminal_node($$, result->malloc_pool_, T_KV_ATTRIBUTES, 1, $3);
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

ttl_definition:
ttl_expr
{
  $$ = $1;
}
| ttl_definition ',' ttl_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

ttl_expr:
simple_expr '+' INTERVAL INTNUM ttl_unit
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TTL_EXPR, 3, $1, $4, $5);
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
;

ttl_unit:
SECOND
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_SECOND;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| MINUTE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_MINUTE;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| HOUR
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_HOUR;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| DAY
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_DAY;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| MONTH
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_MONTH;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
}
| YEAR
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = DATE_UNIT_YEAR;
  dup_expr_string($$, result, @1.first_column, @1.last_column);
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
| external_table_partition_option
{
  $$ = $1;
}
;

external_table_partition_option: /* list partition without partition defines*/
PARTITION BY '(' column_name_list ')'
{
  ParseNode *column_names = NULL;
  ParseNode *partition_defs = NULL;
  merge_nodes(column_names, result, T_EXPR_LIST, $4);
  malloc_terminal_node(partition_defs, result->malloc_pool_, T_PARTITION_LIST);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIST_COLUMNS_PARTITION, 5, column_names, partition_defs, NULL, NULL, NULL);
  dup_expr_string($$, result, @4.first_column, @4.last_column);
}
;

opt_partition_option:
partition_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_OPTION, 2, $1, NULL);
}
| /*EMPTY*/
{
  $$ = NULL;
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_HASH, 2, hash_func, params);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 6, $$, $8, NULL, $7, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| PARTITION BY HASH '(' expr ')' subpartition_option opt_partitions opt_hash_partition_list
{
  ParseNode *params = NULL;
  ParseNode *hash_func = NULL;
  malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 1, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_hash");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_HASH, 2, hash_func, params);
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
  make_name_node(hash_func, result->malloc_pool_, "partition_key");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_KEY, 2, hash_func, column_name_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_KEY_PARTITION, 6, $$, $8, NULL, $7, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| PARTITION BY KEY '(' column_name_list ')' subpartition_option opt_partitions opt_hash_partition_list
{
  ParseNode *column_name_list = NULL;
  ParseNode *hash_func = NULL;
  merge_nodes(column_name_list, result, T_EXPR_LIST, $5);
  make_name_node(hash_func, result->malloc_pool_, "partition_key");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_KEY, 2, hash_func, column_name_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_KEY_PARTITION, 6, $$, $8, $9, $7, NULL, NULL);
  dup_expr_string($$, result, @5.first_column, @5.last_column);
}
| PARTITION BY KEY '(' ')' subpartition_option opt_partitions %prec LOWER_PARENS
{
  ParseNode *hash_func = NULL;
  ParseNode *column_name_list = NULL;
  make_name_node(hash_func, result->malloc_pool_, "partition_key");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_KEY, 2, hash_func, column_name_list);
  malloc_non_terminal_node($$, result->malloc_pool_, T_KEY_PARTITION, 6, $$, $7, NULL, $6, NULL, NULL);
}
| PARTITION BY KEY '(' ')' subpartition_option opt_partitions opt_hash_partition_list
{
  ParseNode *hash_func = NULL;
  ParseNode *column_name_list = NULL;
  make_name_node(hash_func, result->malloc_pool_, "partition_key");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_KEY, 2, hash_func, column_name_list);
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_HASH, 2, hash_func, params);
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
  make_name_node(hash_func, result->malloc_pool_, "partition_key");
  make_name_node(template_mark, result->malloc_pool_, "template_mark");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_KEY, 2, hash_func, column_name_list);
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_HASH, 2, hash_func, params);
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
  make_name_node(hash_func, result->malloc_pool_, "partition_key");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_KEY, 2, hash_func, column_name_list);
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
PARTITION relation_factor opt_part_id opt_engine_option opt_subpartition_list
{
  UNUSED($4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_HASH_ELEMENT, 5, $2, NULL, $3, NULL, $5);
}
;

opt_range_partition_list:
'(' range_partition_list ')'
{
  merge_nodes($$, result, T_RANGE_PARTITION_LIST, $2);
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
PARTITION relation_factor VALUES LESS THAN range_partition_expr opt_part_id opt_engine_option opt_subpartition_list
{
  UNUSED($8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_RANGE_ELEMENT, 5, $2, $6, $7, NULL, $9);
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
PARTITION relation_factor VALUES IN list_partition_expr opt_part_id opt_engine_option opt_subpartition_list
{
  UNUSED($7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_LIST_ELEMENT, 5, $2, $5, $6, NULL, $8);
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
SUBPARTITION relation_factor opt_engine_option
{
  UNUSED($3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_HASH_ELEMENT, 5, $2, NULL, NULL, NULL, NULL);
}
;

opt_engine_option:
ENGINE_ COMP_EQ INNODB
{
  // fix the error report by xabank.xyhf_mysql
  $$ = NULL;
}
| /* empty */{$$=NULL;};

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
SUBPARTITION relation_factor VALUES LESS THAN range_partition_expr opt_engine_option
{
  UNUSED($7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_RANGE_ELEMENT, 5, $2, $6, NULL, NULL, NULL);
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
SUBPARTITION relation_factor VALUES IN list_partition_expr opt_engine_option
{
  UNUSED($6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARTITION_LIST_ELEMENT, 5, $2, $5, NULL, NULL, NULL);
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_HASH, 2, hash_func, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 3, $$, $5, $4);
}
;

tg_key_partition_option:
PARTITION BY KEY INTNUM tg_subpartition_option opt_partitions
{
  ParseNode *hash_func = NULL;
  make_name_node(hash_func, result->malloc_pool_, "partition_key");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_KEY, 2, hash_func, $4);
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_HASH, 2, hash_func, NULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_HASH_PARTITION, 3, $$, $4, NULL);
}
| SUBPARTITION BY KEY INTNUM opt_subpartitions
{
  ParseNode *hash_func = NULL;
  make_name_node(hash_func, result->malloc_pool_, "partition_key");
  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_PART_KEY, 2, hash_func, $4);
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
  $$->is_hidden_const_ = 1;
}
| COMPACT
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
  $$->is_hidden_const_ = 1;
}
| DYNAMIC
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 3;
  $$->is_hidden_const_ = 1;
}
| COMPRESSED
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 4;
  $$->is_hidden_const_ = 1;
}
| CONDENSED
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 5;
  $$->is_hidden_const_ = 1;
}
| DEFAULT
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 3;
  $$->is_hidden_const_ = 1;
}
;

external_file_format_list:
external_file_format
{
  $$ = $1;
}
| external_file_format_list opt_comma external_file_format
{
  (void) ($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

external_file_format:
TYPE COMP_EQ STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXTERNAL_FILE_FORMAT_TYPE, 1, $3);
}
| FIELD_DELIMITER COMP_EQ expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FIELD_TERMINATED_STR, 1, $3);
  dup_expr_string($$, result, @3.first_column, @3.last_column);
}
| LINE_DELIMITER COMP_EQ expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINE_TERMINATED_STR, 1, $3);
  dup_expr_string($$, result, @3.first_column, @3.last_column);
}
| ESCAPE COMP_EQ expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ESCAPED_STR, 1, $3);
  dup_expr_string($$, result, @3.first_column, @3.last_column);
}
| FIELD_OPTIONALLY_ENCLOSED_BY COMP_EQ expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CLOSED_STR, 1, $3);
  dup_expr_string($$, result, @3.first_column, @3.last_column);
}
| ENCODING COMP_EQ STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHARSET, 1, $3);
}
| SKIP_HEADER COMP_EQ INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SKIP_HEADER, 1, $3);
}
| SKIP_BLANK_LINES COMP_EQ BOOL_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SKIP_BLANK_LINE, 1, $3);
}
| TRIM_SPACE COMP_EQ BOOL_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRIM_SPACE, 1, $3);
}
| NULL_IF_EXETERNAL COMP_EQ '(' expr_list ')'
{
  ParseNode *expr_list_node = NULL;
  merge_nodes(expr_list_node, result, T_EXPR_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_NULL_IF_EXETERNAL, 1, expr_list_node);
  dup_expr_string($$, result, @4.first_column, @4.last_column);
}
| EMPTY_FIELD_AS_NULL COMP_EQ BOOL_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_EMPTY_FIELD_AS_NULL, 1, $3);
}
;

/*****************************************************************************
 *
 * create tablegroup
 *
 *****************************************************************************/
create_tablegroup_stmt:
create_with_opt_hint TABLEGROUP opt_if_not_exists relation_name opt_tablegroup_option_list opt_tg_partition_option
{
  ParseNode *tablegroup_options = NULL;
  (void)($1);
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
TABLEGROUP_ID opt_equal_mark INTNUM
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLEGROUP_ID, 1, $3);
}
| BINDING opt_equal_mark BOOL_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLEGROUP_BINDING, 1, $3);
}
| SHARDING opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLEGROUP_SHARDING, 1, $3);
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
  $$->sql_str_off_ = $4->sql_str_off_;
}
| opt_default_mark TABLEGROUP opt_equal_mark NULLX
{
  (void)($1) ; /* make bison mute */
  (void)($3) ; /* make bison mute */
  (void)($4) ; /* make bison mute */
  malloc_terminal_node($$, result->malloc_pool_, T_DEFAULT_TABLEGROUP);
  $$->sql_str_off_ = $4->sql_str_off_;
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
create_with_opt_hint opt_replace opt_algorithm opt_definer opt_sql_security VIEW view_name opt_column_list opt_table_id AS view_select_stmt opt_check_option
{
  (void)($1);
  UNUSED($3);
  UNUSED($4);
  UNUSED($5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_VIEW, 8,
                           NULL,    /* opt_materialized, not support*/
                           $7,    /* view name */
                           $8,    /* column list */
                           $9,    /* table_id */
                           $11,    /* select_stmt */
                           $2,
						               $12,   /* with option */
                           NULL   /* force view opt */
						   );
  dup_expr_string($11, result, @11.first_column, @11.last_column);
  $$->reserved_ = 0; /* is create view */
}
// alter view 功能类似于 create or replace view，代码基本可以直接复用，区别仅有在原有视图不存在时需要报错
| ALTER opt_algorithm opt_definer opt_sql_security VIEW view_name opt_column_list opt_table_id AS view_select_stmt opt_check_option
{
  UNUSED($2);
  UNUSED($3);
  UNUSED($4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_VIEW, 8,
                           NULL,    /* opt_materialized */
                           $6,    /* view name */
                           $7,    /* column list */
                           $8,    /* table_id */
                           $10,    /* select_stmt */
                           NULL,
                           $11,    /* with option */
                           NULL   /* force view opt */
               );
  dup_expr_string($10, result, @10.first_column, @10.last_column);
  $$->reserved_ = 1; /* is alter view */
}
;

opt_algorithm:
ALGORITHM COMP_EQ view_algorithm
{
  (void)($3);
  $$ = NULL;
}
| { $$ = NULL; };

view_algorithm:
UNDEFINED { $$ = NULL; }
| MERGE { $$ = NULL; }
| TEMPTABLE { $$ = NULL; }

opt_definer:
DEFINER COMP_EQ user
{
  (void)($3);
  $$ = NULL;
}
| { $$ = NULL; };

opt_sql_security:
SQL SECURITY DEFINER { $$ = NULL; }
| SQL SECURITY INVOKER { $$ = NULL; }
| { $$ = NULL; };

view_select_stmt:
select_stmt
{
  $$ = $1;
}
;

opt_check_option:
WITH CHECK OPTION
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
  $$->is_hidden_const_ = 1;
}
| WITH CASCADED CHECK OPTION
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
  $$->is_hidden_const_ = 1;
}
| WITH LOCAL CHECK OPTION
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
  $$->is_hidden_const_ = 1;
}
| /* EMPTY */
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
  $$->is_hidden_const_ = 1;
}
;

opt_replace:
OR REPLACE
{ malloc_terminal_node($$, result->malloc_pool_, T_IF_NOT_EXISTS); }
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

opt_tablet_id:
TABLET_ID COMP_EQ INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLET_ID, 1, $3);
}
| /*EMPTY*/ { $$ = NULL; }
;
/*****************************************************************************
 *
 *	create index
 *
 *****************************************************************************/

create_index_stmt:
create_with_opt_hint opt_index_keyname INDEX opt_if_not_exists normal_relation_factor opt_index_using_algorithm ON relation_factor '(' sort_column_list ')'
opt_index_option_list opt_partition_option
{
  ParseNode *idx_columns = NULL;
  ParseNode *index_options = NULL;
  merge_nodes(idx_columns, result, T_INDEX_COLUMN_LIST, $10);
  merge_nodes(index_options, result, T_TABLE_OPTION_LIST, $12);
  $5->value_ = $2[0]; /* index prefix keyname */
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_INDEX, 8,
                           $5,                   /* index name */
                           $8,                   /* table name */
                           idx_columns,          /* index columns */
                           index_options,        /* index option(s) */
                           $6,                   /* index method */
                           $13,                  /* partition method*/
                           $4,                   /* if not exists*/
                           $1);                  /* index hint*/
};

create_with_opt_hint:
CREATE {$$ = NULL;}
| CREATE_HINT_BEGIN hint_list_with_end
{$$ = $2;}
;

opt_index_keyname:
SPATIAL { $$[0] = 2; }
| UNIQUE { $$[0] = 1; }
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

/*opt_constraint:
CONSTRAINT opt_constraint_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 1, $2);
}
|
{
  $$ = NULL;
}
;*/

check_state:
NOT ENFORCED
{
  malloc_terminal_node($$, result->malloc_pool_, T_NOENFORCED_CONSTRAINT);
}
| ENFORCED
{
  malloc_terminal_node($$, result->malloc_pool_, T_ENFORCED_CONSTRAINT);
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
| '(' index_expr ')' opt_asc_desc opt_column_id
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SORT_COLUMN_KEY, 4, $2, NULL, $4, $5);
}
;

index_expr:
expr
{
  if (NULL == $1) {
    yyerror(NULL, result, "index_expr is null\n");
    YYERROR;
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

opt_temporary:
TEMPORARY
{ malloc_terminal_node($$, result->malloc_pool_, T_TEMPORARY); }
| /* EMPTY */
{ $$ = NULL; }
;
/*****************************************************************************
 *
 *	drop table grammar
 *
 *****************************************************************************/

drop_table_stmt:
DROP opt_temporary table_or_tables opt_if_exists table_list opt_drop_behavior
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
DROP VIEW opt_if_exists table_list opt_drop_behavior
{
  ParseNode *views = NULL;
  merge_nodes(views, result, T_VIEW_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_VIEW, 3, NULL, $3, views);
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
  refine_insert_values_table($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_TABLE_INSERT, 4,
                           into_node, /*insert_into_clause*/
                           $2, /*values_clause*/
                           NULL, /*duplicate key node*/
                           NULL /*error logging caluse*/);
}
| dml_table_name '(' ')' values_clause
{
  ParseNode *into_node = NULL;
  malloc_non_terminal_node(into_node, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 2, $1, NULL);
  refine_insert_values_table($4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_TABLE_INSERT, 4,
                           into_node, /*insert_into_clause*/
                           $4, /*values_clause*/
                           NULL, /*duplicate key node*/
                           NULL /*error logging caluse*/);
}
| dml_table_name '(' column_list ')' values_clause
{
  ParseNode *into_node = NULL;
  ParseNode *column_list = NULL;
  merge_nodes(column_list, result, T_COLUMN_LIST, $3);
  malloc_non_terminal_node(into_node, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 2, $1, column_list);
  refine_insert_values_table($5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_TABLE_INSERT, 4,
                           into_node, /*insert_into_clause*/
                           $5, /*values_clause*/
                           NULL, /*duplicate key node*/
                           NULL /*error logging caluse*/);
}
| dml_table_name SET update_asgn_list
{
  ParseNode *val_list = NULL;
  ParseNode *into_node = NULL;
  merge_nodes(val_list, result, T_ASSIGN_LIST, $3);
  malloc_non_terminal_node(into_node, result->malloc_pool_, T_INSERT_INTO_CLAUSE, 1, $1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SINGLE_TABLE_INSERT, 4,
                           into_node, /*insert_into_clause*/
                           val_list, /*values_list*/
                           NULL, /*duplicate key node*/
                           NULL /*error logging caluse*/);
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
no_param_column_ref { $$ = $1; }
| column_list ',' no_param_column_ref
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

/*
  there are three type of column_ref:
  1. column_ref -- that is used in dml stmt, and will be parameterized in pl.
                   e.g., in "create procedure p1() begin declare c1 int; set c1 = 1; select c1 from t1; end//"
                   ---> select c1 from t1; will be transform to select :0 from t1;
  2. no_param_column_ref: that is used in dml stmt, but won't be parameterized in pl.
  3. column_definition_ref: that is used in ddl stmt (e.g., CREATE and ALTER).

  column_ref and no_param_column_ref support mysql_reserved_keyword.
  column_definition_ref doesn't support mysql_reserved_keyword.
*/
no_param_column_ref:
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
| with_select
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
| '(' with_select ')'         { $$ = $2; };

select_no_parens:
select_clause opt_lock_type
{
  $$ = $1;
  $$->children_[PARSE_SELECT_FOR_UPD] = $2;
}
| select_clause_set opt_lock_type
{
  $$ = $1;
  $$->children_[PARSE_SELECT_FOR_UPD] = $2;
}
| select_clause_set_with_order_and_limit opt_lock_type
{
  $$ = $1;
  $$->children_[PARSE_SELECT_FOR_UPD] = $2;
}
;

opt_lock_type:
/* EMPTY */
{ $$ = NULL; }
| opt_for_update
{
  $$ = $1;
}
| opt_lock_in_share_mode
{
  $$ = $1;
}
;

no_table_select:
select_with_opt_hint opt_query_expression_option_list select_expr_list into_opt
FROM DUAL opt_where opt_groupby opt_having opt_named_windows
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
  select_node->children_[PARSE_SELECT_DYNAMIC_GROUP] = $8;
  select_node->children_[PARSE_SELECT_DYNAMIC_HAVING] = $9;
  select_node->children_[PARSE_SELECT_NAMED_WINDOWS] = $10;
  $$ = select_node;

  setup_token_pos_info(select_node, @1.first_column - 1, 6);
}
| select_with_opt_hint opt_query_expression_option_list select_expr_list into_opt opt_where opt_groupby opt_having opt_named_windows
{
  ParseNode *project_list = NULL;
  ParseNode *select_node = NULL;
  merge_nodes(project_list, result, T_PROJECT_LIST, $3);
  setup_token_pos_info($$, @1.first_column - 1, 5);
  malloc_select_node(select_node, result->malloc_pool_);
  select_node->children_[PARSE_SELECT_DISTINCT] = $2;
  select_node->children_[PARSE_SELECT_SELECT] = project_list;
  select_node->children_[PARSE_SELECT_WHERE] = $5;
  select_node->children_[PARSE_SELECT_HINTS] = $1;
  select_node->children_[PARSE_SELECT_INTO] = $4;
  select_node->children_[PARSE_SELECT_DYNAMIC_GROUP] = $6;
  select_node->children_[PARSE_SELECT_DYNAMIC_HAVING] = $7;
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
| table_values_caluse
{
  $$ = $1;
}
| table_values_caluse_with_order_by_and_limit
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
| table_values_caluse
{
  $$ = $1;
}
;

select_clause_set_left:
select_clause_set_right
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
  alias_node->sql_str_off_ = @1.first_column;
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
  alias_node->sql_str_off_ = @1.first_column;
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
  select_node->children_[PARSE_SELECT_DYNAMIC_GROUP] = $8;
  select_node->children_[PARSE_SELECT_DYNAMIC_HAVING] = $9;
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIMIT_CLAUSE, 2, $2, $4);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @4.last_column),
            &@1, result);
}
| LIMIT limit_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LIMIT_CLAUSE, 2, $2, NULL);
  check_ret(setup_token_pos_info_and_dup_string($$, result, @1.first_column, @2.last_column),
            &@1, result);
}
| LIMIT limit_expr ',' limit_expr
{
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
NAME_OB
{
  $$ = $1;
}
|
unreserved_keyword_normal
{
  get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
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
| unreserved_keyword
{
  get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

hint_option:
global_hint
{
  $$ = $1;
}
| transform_hint
{
  $$ = $1;
}
| optimize_hint
{
  $$ = $1;
}
| BEGIN_OUTLINE_DATA
{
  malloc_terminal_node($$, result->malloc_pool_, T_BEGIN_OUTLINE_DATA);
}
| END_OUTLINE_DATA
{
  malloc_terminal_node($$, result->malloc_pool_, T_END_OUTLINE_DATA);
}
| OPTIMIZER_FEATURES_ENABLE '(' STRING_VALUE ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OPTIMIZER_FEATURES_ENABLE, 1, $3);
}
| QB_NAME '(' qb_name_string ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_QB_NAME, 1, $3);
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
;

qb_name_string:
NAME_OB
{
  str_toupper((char*)($1->str_value_), $1->str_len_);
  $$ = $1;
}

global_hint:
READ_CONSISTENCY '(' consistency_level ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_READ_CONSISTENCY);
  $$->value_ = $3[0];
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
| LOG_LEVEL '(' NAME_OB ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOG_LEVEL, 1, $3);
}
| LOG_LEVEL '(' STRING_VALUE ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOG_LEVEL, 1, $3);
}
| USE_PLAN_CACHE '(' use_plan_cache_type ')'
{
  malloc_terminal_node($$, result->malloc_pool_, T_USE_PLAN_CACHE);
  $$->value_ = $3[0];
}
| CURSOR_SHARING_EXACT
{
  malloc_terminal_node($$, result->malloc_pool_, T_CURSOR_SHARING_EXACT);
}
| TRACE_LOG
{
  malloc_terminal_node($$, result->malloc_pool_, T_TRACE_LOG);
}
| STAT '(' intnum_list ')'
{
  ParseNode *tracing_nums = NULL;
  merge_nodes(tracing_nums, result, T_STAT, $3);
  $$=tracing_nums;
}
| TRACING '(' intnum_list ')'
{
  ParseNode *tracing_nums = NULL;
  merge_nodes(tracing_nums, result, T_TRACING, $3);
  $$=tracing_nums;
}
| DOP '(' INTNUM ',' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DOP, 2, $3, $5);
}
| TRANS_PARAM '(' trans_param_name opt_comma trans_param_value ')'
{
  (void) $4;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TRANS_PARAM, 2, $3, $5);
}
| OPT_PARAM '(' trans_param_name opt_comma trans_param_value ')'
{
  (void) $4;
  malloc_non_terminal_node($$, result->malloc_pool_, T_OPT_PARAM_HINT, 2, $3, $5);
}
| OB_DDL_SCHEMA_VERSION '(' relation_factor_in_hint opt_comma INTNUM ')'
{
  (void) $4;
  malloc_non_terminal_node($$, result->malloc_pool_, T_OB_DDL_SCHEMA_VERSION, 2, $3, $5);
}
| FORCE_REFRESH_LOCATION_CACHE
{
  malloc_terminal_node($$, result->malloc_pool_, T_FORCE_REFRESH_LOCATION_CACHE);
}
| MAX_CONCURRENT '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MAX_CONCURRENT, 1, $3);
}
| PARALLEL '(' parallel_hint ')'
{
  $$ = $3;
}
| NO_PARALLEL
{
  malloc_terminal_node($$, result->malloc_pool_, T_NO_PARALLEL);
}
| MONITOR
{
  malloc_terminal_node($$, result->malloc_pool_, T_MONITOR);
}
| LOAD_BATCH_SIZE '(' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LOAD_BATCH_SIZE, 1, $3);
}
| DIRECT '(' BOOL_VALUE ',' INTNUM ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DIRECT, 2, $3, $5);
}
| APPEND
{
  malloc_terminal_node($$, result->malloc_pool_, T_APPEND);
}
| ENABLE_PARALLEL_DML
{
  malloc_terminal_node($$, result->malloc_pool_, T_ENABLE_PARALLEL_DML);
}
| DISABLE_PARALLEL_DML
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISABLE_PARALLEL_DML);
}
| NO_QUERY_TRANSFORMATION
{
  malloc_terminal_node($$, result->malloc_pool_, T_NO_QUERY_TRANSFORMATION);
}
| NO_COST_BASED_QUERY_TRANSFORMATION
{
  malloc_terminal_node($$, result->malloc_pool_, T_NO_COST_BASED_QUERY_TRANSFORMATION);
}
| NO_GATHER_OPTIMIZER_STATISTICS
{
  malloc_terminal_node($$, result->malloc_pool_, T_NO_GATHER_OPTIMIZER_STATISTICS);
}
| GATHER_OPTIMIZER_STATISTICS
{
  malloc_terminal_node($$, result->malloc_pool_, T_GATHER_OPTIMIZER_STATISTICS);
}
| DBMS_STATS
{
  malloc_terminal_node($$, result->malloc_pool_, T_DBMS_STATS);
}
| FLASHBACK_READ_TX_UNCOMMITTED
{
  malloc_terminal_node($$, result->malloc_pool_, T_FLASHBACK_READ_TX_UNCOMMITTED);
}
;

transform_hint:
NO_REWRITE opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_REWRITE, 1, $2);
}
| MERGE_HINT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE_HINT, 2, $2, NULL);
  $$->value_ = 0;
}
| MERGE_HINT '(' qb_name_option COMP_GT qb_name_string ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE_HINT, 2, $3, $5);
  $$->value_ = 0;
}
| MERGE_HINT '(' qb_name_option COMP_LT qb_name_string ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE_HINT, 2, $3, $5);
  $$->value_ = 1;
}
| NO_MERGE_HINT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_MERGE_HINT, 2, $2, NULL);
}
| NO_EXPAND opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_EXPAND, 2, $2, NULL);
}
| USE_CONCAT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_CONCAT, 2, $2, NULL);
}
| USE_CONCAT '(' qb_name_option STRING_VALUE ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_CONCAT, 2, $3, $4);
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_PLACE_GROUP_BY, 2, $2, NULL);
}
| PLACE_GROUP_BY '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
    ParseNode *table_list = NULL;
    merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
    malloc_non_terminal_node($$, result->malloc_pool_, T_PLACE_GROUP_BY, 2, $3, table_list);
}
| NO_PLACE_GROUP_BY opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PLACE_GROUP_BY, 2, $2, NULL);
}
| PRED_DEDUCE opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRED_DEDUCE, 1, $2);
}
| NO_PRED_DEDUCE opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PRED_DEDUCE, 1, $2);
}
| PUSH_PRED_CTE opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PUSH_PRED_CTE, 1, $2);
}
| NO_PUSH_PRED_CTE opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PUSH_PRED_CTE, 1, $2);
}
| INLINE opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INLINE, 1, $2);
}
| MATERIALIZE opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MATERIALIZE, 1, $2);
}
| MATERIALIZE '(' qb_name_option multi_qb_name_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MATERIALIZE, 2, $3, $4);
}
| SEMI_TO_INNER '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SEMI_TO_INNER, 2, $3, $4);
}
| NO_SEMI_TO_INNER opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_SEMI_TO_INNER, 1, $2);
}
| COALESCE_SQ '(' qb_name_option multi_qb_name_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COALESCE_SQ, 2, $3, $4);
}
| NO_COALESCE_SQ opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_COALESCE_SQ, 1, $2);
}
| REPLACE_CONST opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_REPLACE_CONST, 1, $2);
}
| NO_REPLACE_CONST opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_REPLACE_CONST, 1, $2);
}
| SIMPLIFY_ORDER_BY opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SIMPLIFY_ORDER_BY, 1, $2);
}
| NO_SIMPLIFY_ORDER_BY opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_SIMPLIFY_ORDER_BY, 1, $2);
}
| SIMPLIFY_GROUP_BY opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SIMPLIFY_GROUP_BY, 1, $2);
}
| NO_SIMPLIFY_GROUP_BY opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_SIMPLIFY_GROUP_BY, 1, $2);
}
| SIMPLIFY_DISTINCT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SIMPLIFY_DISTINCT, 1, $2);
}
| NO_SIMPLIFY_DISTINCT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_SIMPLIFY_DISTINCT, 1, $2);
}
| SIMPLIFY_WINFUNC opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SIMPLIFY_WINFUNC, 1, $2);
}
| NO_SIMPLIFY_WINFUNC opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_SIMPLIFY_WINFUNC, 1, $2);
}
| SIMPLIFY_EXPR opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SIMPLIFY_EXPR, 1, $2);
}
| NO_SIMPLIFY_EXPR opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_SIMPLIFY_EXPR, 1, $2);
}
| SIMPLIFY_LIMIT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SIMPLIFY_LIMIT, 1, $2);
}
| NO_SIMPLIFY_LIMIT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_SIMPLIFY_LIMIT, 1, $2);
}
| SIMPLIFY_SUBQUERY opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SIMPLIFY_SUBQUERY, 1, $2);
}
| NO_SIMPLIFY_SUBQUERY opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_SIMPLIFY_SUBQUERY, 1, $2);
}
| FAST_MINMAX opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FAST_MINMAX, 1, $2);
}
| NO_FAST_MINMAX opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_FAST_MINMAX, 1, $2);
}
| PROJECT_PRUNE opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_PRUNE, 1, $2);
}
| NO_PROJECT_PRUNE opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PROJECT_PRUNE, 1, $2);
}
| SIMPLIFY_SET opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SIMPLIFY_SET, 1, $2);
}
| NO_SIMPLIFY_SET opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_SIMPLIFY_SET, 1, $2);
}
| OUTER_TO_INNER opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OUTER_TO_INNER, 1, $2);
}
| NO_OUTER_TO_INNER opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_OUTER_TO_INNER, 1, $2);
}
| COUNT_TO_EXISTS opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COUNT_TO_EXISTS, 2, $2, NULL);
}
| COUNT_TO_EXISTS '(' qb_name_option qb_name_list ')'
{
  ParseNode *name_list = NULL;
  merge_nodes(name_list, result, T_QB_NAME_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_COUNT_TO_EXISTS, 2, $3, name_list);
}
| NO_COUNT_TO_EXISTS opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_COUNT_TO_EXISTS, 2, $2, NULL);
}
| LEFT_TO_ANTI opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LEFT_TO_ANTI, 2, $2, NULL);
}
| LEFT_TO_ANTI '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LEFT_TO_ANTI, 2, $3, table_list);
}
| NO_LEFT_TO_ANTI opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_LEFT_TO_ANTI, 2, $2, NULL);
}
| PUSH_LIMIT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PUSH_LIMIT, 1, $2);
}
| NO_PUSH_LIMIT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PUSH_LIMIT, 1, $2);
}
| ELIMINATE_JOIN opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ELIMINATE_JOIN, 2, $2, NULL);
}
| ELIMINATE_JOIN '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ELIMINATE_JOIN, 2, $3, table_list);
}
| NO_ELIMINATE_JOIN opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_ELIMINATE_JOIN, 2, $2, NULL);
}
| WIN_MAGIC opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_MAGIC, 2, $2, NULL);
}
| WIN_MAGIC '(' qb_name_option relation_factor_in_use_join_hint_list ')'
{
    ParseNode *table_list = NULL;
    merge_nodes(table_list, result, T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST, $4);
    malloc_non_terminal_node($$, result->malloc_pool_, T_WIN_MAGIC, 2, $3, table_list);
}
| NO_WIN_MAGIC opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_WIN_MAGIC, 2, $2, NULL);
}
| PULLUP_EXPR opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PULLUP_EXPR, 1, $2);
}
| NO_PULLUP_EXPR opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PULLUP_EXPR, 1, $2);
}
| AGGR_FIRST_UNNEST opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_AGGR_FIRST_UNNEST, 1, $2);
}
| NO_AGGR_FIRST_UNNEST opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_AGGR_FIRST_UNNEST, 1, $2);
}
| JOIN_FIRST_UNNEST opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOIN_FIRST_UNNEST, 1, $2);
}
| NO_JOIN_FIRST_UNNEST opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_JOIN_FIRST_UNNEST, 1, $2);
}
;

multi_qb_name_list:
  '(' qb_name_list ')'
  {
    merge_nodes($$, result, T_QB_NAME_LIST, $2);
  }
  | multi_qb_name_list '(' qb_name_list ')'
  {
    ParseNode *name_list = NULL;
    merge_nodes(name_list, result, T_QB_NAME_LIST, $3);
    malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, name_list);
  }
  ;

qb_name_list:
  qb_name_string
  {
    $$ = $1;
  }
  | qb_name_list ',' qb_name_string
  {
    malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
  }
  | qb_name_list qb_name_string
  {
    malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
  }
  ;

optimize_hint:
INDEX_HINT '(' qb_name_option relation_factor_in_hint NAME_OB ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_HINT, 3, $3, $4, $5);
}
| NO_INDEX_HINT '(' qb_name_option relation_factor_in_hint NAME_OB ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_INDEX_HINT, 3, $3, $4, $5);
}
| FULL_HINT '(' qb_name_option relation_factor_in_hint ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FULL_HINT, 2, $3, $4);
}
| USE_DAS_HINT '(' qb_name_option relation_factor_in_hint ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_DAS_HINT, 2, $3, $4);
}
| NO_USE_DAS_HINT '(' qb_name_option relation_factor_in_hint ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_DAS_HINT, 2, $3, $4);
}
| INDEX_SS_HINT '(' qb_name_option relation_factor_in_hint NAME_OB ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_SS_HINT, 3, $3, $4, $5);
}
| INDEX_SS_ASC_HINT '(' qb_name_option relation_factor_in_hint NAME_OB ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_SS_ASC_HINT, 3, $3, $4, $5);
}
| INDEX_SS_DESC_HINT '(' qb_name_option relation_factor_in_hint NAME_OB ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_SS_DESC_HINT, 3, $3, $4, $5);
}
| LEADING_HINT '(' qb_name_option relation_factor_in_leading_hint_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LEADING, 2, $3, $4);
}
| ORDERED opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORDERED, 1, $2);
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
| USE_HASH_AGGREGATION opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_HASH_AGGREGATE, 1, $2);
}
| NO_USE_HASH_AGGREGATION opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_HASH_AGGREGATE, 1, $2);
  $$->value_ = -1;
}
| NO_USE_HASH_AGGREGATION '(' qb_name_option NO_PARTITION_SORT ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_HASH_AGGREGATE, 1, $3);
  $$->value_ = 0;
}
| NO_USE_HASH_AGGREGATION '(' qb_name_option PARTITION_SORT ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_HASH_AGGREGATE, 1, $3);
  $$->value_ = 1;
}
| USE_LATE_MATERIALIZATION opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_LATE_MATERIALIZATION, 1, $2);
}
| NO_USE_LATE_MATERIALIZATION opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_LATE_MATERIALIZATION, 1, $2);
}
| PX_JOIN_FILTER '(' qb_name_option relation_factor_in_hint opt_relation_factor_in_hint_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PX_JOIN_FILTER, 4, $3, $4, $5, NULL);
}
| NO_PX_JOIN_FILTER '(' qb_name_option relation_factor_in_hint opt_relation_factor_in_hint_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PX_JOIN_FILTER, 4, $3, $4, $5, NULL);
}
| PX_PART_JOIN_FILTER '(' qb_name_option relation_factor_in_hint opt_relation_factor_in_hint_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PX_PART_JOIN_FILTER, 4, $3, $4, $5, NULL);
}
| NO_PX_PART_JOIN_FILTER '(' qb_name_option relation_factor_in_hint opt_relation_factor_in_hint_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PX_PART_JOIN_FILTER, 4, $3, $4, $5, NULL);
}
| PX_JOIN_FILTER '(' qb_name_option relation_factor_in_hint relation_factor_in_pq_hint relation_factor_in_hint ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PX_JOIN_FILTER, 4, $3, $4, $5, $6);
}
| NO_PX_JOIN_FILTER '(' qb_name_option relation_factor_in_hint relation_factor_in_pq_hint relation_factor_in_hint ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PX_JOIN_FILTER, 4, $3, $4, $5, $6);
}
| PX_PART_JOIN_FILTER '(' qb_name_option relation_factor_in_hint relation_factor_in_pq_hint relation_factor_in_hint ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PX_PART_JOIN_FILTER, 4, $3, $4, $5, $6);
}
| NO_PX_PART_JOIN_FILTER '(' qb_name_option relation_factor_in_hint relation_factor_in_pq_hint relation_factor_in_hint ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_PX_PART_JOIN_FILTER, 4, $3, $4, $5, $6);
}
| PQ_DISTRIBUTE '(' qb_name_option relation_factor_in_pq_hint opt_comma distribute_method opt_comma distribute_method ')'
{
  (void)($5);               /* unused */
  (void)($7);               /* unused */
  malloc_non_terminal_node($$, result->malloc_pool_, T_PQ_DISTRIBUTE, 4, $3, $4, $6, $8);
}
| PQ_DISTRIBUTE '(' qb_name_option relation_factor_in_pq_hint ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PQ_DISTRIBUTE, 4, $3, $4, NULL, NULL);
}
| PQ_MAP '(' qb_name_option relation_factor_in_hint ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PQ_MAP, 2, $3, $4);
}
| PQ_DISTRIBUTE_WINDOW '('qb_name_option opt_comma win_dist_list')'
{
  (void) $4;
  ParseNode *method_list = NULL;
  merge_nodes(method_list, result, T_METHOD_OPT_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PQ_DISTRIBUTE_WINDOW, 2, $3, method_list);
}
| PQ_SET '(' pq_set_hint_desc ')'
{
  $$ = $3;
}
| GBY_PUSHDOWN opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_GBY_PUSHDOWN, 1, $2);
}
| NO_GBY_PUSHDOWN opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_GBY_PUSHDOWN, 1, $2);
}
| USE_HASH_DISTINCT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_HASH_DISTINCT, 1, $2);
}
| NO_USE_HASH_DISTINCT opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_HASH_DISTINCT, 1, $2);
}
| DISTINCT_PUSHDOWN opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DISTINCT_PUSHDOWN, 1, $2);
}
| NO_DISTINCT_PUSHDOWN opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_DISTINCT_PUSHDOWN, 1, $2);
}
| USE_HASH_SET opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_HASH_SET, 1, $2);
}
| NO_USE_HASH_SET opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_HASH_SET, 1, $2);
}
| USE_DISTRIBUTED_DML opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USE_DISTRIBUTED_DML, 1, $2);
}
| NO_USE_DISTRIBUTED_DML opt_qb_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_NO_USE_DISTRIBUTED_DML, 1, $2);
}
| DYNAMIC_SAMPLING '(' dynamic_sampling_hint ')'
{
  $$ = $3;
}
;

win_dist_list:
win_dist_desc
{
  $$ = $1;
}
| win_dist_list opt_comma win_dist_desc
{
  (void)($2);               /* unused */
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

win_dist_desc:
'(' intnum_list ')' distribute_method opt_hash_sort_and_pushdown
{
  ParseNode *win_func_idxs = NULL;
  merge_nodes(win_func_idxs, result, T_WIN_FUNC_IDX_LIST, $2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_METHOD_OPT, 2, win_func_idxs, $4);
  $4->value_ = $5[0];
}
| distribute_method opt_hash_sort_and_pushdown
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_METHOD_OPT, 2, NULL, $1);
  $1->value_ = $2[0];
}
;

opt_hash_sort_and_pushdown:
/*empty*/
{
  $$[0] = 0;
}
| PARTITION_SORT
{
  $$[0] = 1;
}
| PUSHDOWN
{
  $$[0] = 2;
}
| PARTITION_SORT PUSHDOWN
{
  $$[0] = 3;
}
| PUSHDOWN PARTITION_SORT
{
  $$[0] = 3;
}
;

pq_set_hint_desc:
'@' qb_name_string qb_name_string distribute_method_list
{
  ParseNode *method_list = NULL;
  merge_nodes(method_list, result, T_DISTRIBUTE_METHOD_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PQ_SET, 3, $2, $3, method_list);
}
| '@' qb_name_string distribute_method_list
{
  ParseNode *method_list = NULL;
  merge_nodes(method_list, result, T_DISTRIBUTE_METHOD_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PQ_SET, 3, $2, NULL, method_list);
}
| '@' qb_name_string qb_name_string
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PQ_SET, 3, $2, $3, NULL);
}
| '@' qb_name_string
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PQ_SET, 3, $2, NULL, NULL);
}
;

opt_qb_name:
'(' qb_name_option ')'
{
  $$ = $2;
}
| /*empty*/
{
  $$ = NULL;
}
;

parallel_hint:
INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PARALLEL, 1, $1);
  $$->value_ = 0;
}
| MANUAL
{
  (void)($1);               /* unused */
  malloc_terminal_node($$, result->malloc_pool_, T_PARALLEL);
  $$->value_ = 1;
}
| AUTO
{
  (void)($1);               /* unused */
  malloc_terminal_node($$, result->malloc_pool_, T_PARALLEL);
  $$->value_ = 2;
}
| '@' qb_name_string relation_factor_in_hint opt_comma INTNUM
{
  (void)$4;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_PARALLEL, 3, $2, $3, $5);
}
| relation_factor_in_hint opt_comma INTNUM
{
  (void)$2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_PARALLEL, 3, NULL, $1, $3);
}
;

dynamic_sampling_hint:
INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DYNAMIC_SAMPLING, 1, $1);
}
| qb_name_option relation_factor_in_hint opt_comma INTNUM
{
  (void)$3;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_DYNAMIC_SAMPLING, 4, $1, $2, $4, NULL);
}
| qb_name_option relation_factor_in_hint opt_comma INTNUM opt_comma INTNUM
{
  (void)$3;
  (void)$5;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_DYNAMIC_SAMPLING, 4, $1, $2, $4, $6);
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

distribute_method_list:
distribute_method
{
  $$ = $1;
}
| distribute_method_list opt_comma distribute_method
{
  (void)($2);               /* unused */
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

distribute_method:
ALL
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_ALL);
}
| NONE
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_NONE);
}
| PARTITION
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_PARTITION);
}
| RANDOM
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_RANDOM);
}
| HASH
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_HASH);
}
| BROADCAST
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_BROADCAST);
}
| LOCAL
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_LOCAL);
}
| BC2HOST
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_BC2HOST);
}
| RANGE
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_RANGE);
}
| LIST
{
  malloc_terminal_node($$, result->malloc_pool_, T_DISTRIBUTE_LIST);
}
;

limit_expr:
INTNUM
{ $$ = $1; }
| QUESTIONMARK
{ $$ = $1; }
| column_ref
{
  if (!result->pl_parse_info_.is_pl_parse_) {
    yyerror(&@1, result, "pl expr stmt not in pl context\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    $$ = $1;
  }
}
;

opt_limit_clause:
/* EMPTY */
{ $$ = NULL; }
| limit_clause
{ $$ = $1; }
;

opt_for_update:
FOR UPDATE opt_for_update_wait
{
  $$ = $3;
}
;

opt_lock_in_share_mode:
LOCK_ IN SHARE MODE
{
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = -1;
}
;

opt_for_update_wait:
/* EMPTY */
{
  /* USE T_SFU_XXX to avoid being parsed by plan cache as template var */
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->is_hidden_const_ = 1;
  $$->value_ = -1;
}
| WAIT DECIMAL_VAL
{
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_DECIMAL);
  $$->str_value_ = $2->str_value_;
  $$->str_len_ = $2->str_len_;
  $$->sql_str_off_ = $2->sql_str_off_;
}
| WAIT INTNUM
{
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = $2->value_;
  $$->sql_str_off_ = $2->sql_str_off_;
}
| NOWAIT
{
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = 0;
  $$->sql_str_off_ = @1.first_column;
  $$->is_hidden_const_ = 1;
}
| NO_WAIT
{
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = 0;
  $$->sql_str_off_ = @1.first_column;
  $$->is_hidden_const_ = 1;
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
/* EMPTY */ %prec LOWER_COMMA
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
{ malloc_terminal_node($$, result->malloc_pool_, T_SORT_ASC); $$->value_ = 2; $$->is_empty_ = 1;}
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
query_expression_option_list %prec LOWER_PARENS
{
  if ($1 == NULL) {
    $$ = NULL;
  } else {
    merge_nodes($$, result, T_QEURY_EXPRESSION_LIST, $1);
  }
}
| %prec LOWER_PARENS
{
  $$ = NULL;
}
;

query_expression_option_list:
query_expression_option
{
  $$ = $1;
}
| query_expression_option_list  query_expression_option
{
  if ($1 == NULL) {
    $$ = $2;
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
  }
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
| SQL_NO_CACHE
{
  // SQL_NO_CACHE/SQL_CACHE is deprecated and will be removed in a future release
  // we only support it in parser, but actually do nothing.
  $$=NULL;
}
| SQL_CACHE
{
  $$=NULL;
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
      if (0 == $1->str_len_) {
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
#ifndef SQL_PARSER_COMPILATION
    if (T_COLUMN_REF == $1->type_ && 3 == $1->num_child_ &&
        NULL != $1->children_[2] && T_STAR == $1->children_[2]->type_) {
          /* do nothing */
    } else {
      lookup_pl_exec_symbol($$, result, @1.first_column, @1.last_column, false, true, false);
    }
#endif
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
    alias_node->sql_str_off_ = @1.first_column;
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
    alias_node->sql_str_off_ = @1.first_column;
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
    } else if (result->is_not_utf8_connection_) {
      alias_name_node->str_value_ = parse_str_convert_utf8(result->charset_info_, $3->str_value_,
                                                           result->malloc_pool_, &(alias_name_node->str_len_),
                                                           &(result->extra_errno_));
      if (OB_PARSER_ERR_ILLEGAL_NAME == result->extra_errno_) {
        yyerror(NULL, result, "alias '%s' is illegal\n", $3->str_value_);
        YYABORT_UNEXPECTED;
      }
    } else {
      dup_node_string($3, alias_name_node, result->malloc_pool_);
    }
    malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, alias_name_node);
    malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
    dup_expr_string($$, result, @1.first_column, @1.last_column);
    if (NULL == $3->str_value_) {
      alias_node->str_value_ = NULL;
      alias_node->str_len_ = 0;
      alias_node->sql_str_off_ = $3->sql_str_off_;
    } else {
      dup_node_string($3, alias_node, result->malloc_pool_);
      alias_node->sql_str_off_ = $3->sql_str_off_;
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
/*The ODBC escape syntax for Outer Join, compatible Mysql8.0*/
| '{' OJ table_factor '}'
{
  $$ = $3;
}
| '{' OJ joined_table '}'
{
  $$ = $3;
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
  unname_node->sql_str_off_ = @1.first_column;
}
| select_with_parens use_flashback %prec LOWER_PARENS
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, unname_node, unname_node, unname_node, unname_node, $2);
  unname_node->sql_str_off_ = @1.first_column;
}
| '(' table_references ')'
{
  $$ = $2;
}
| json_table_expr
{
  $$ = $1;
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
| relation_factor use_flashback %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 5, $1, NULL, NULL, NULL, $2);
}
| relation_factor use_partition use_flashback %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ORG, 5, $1, NULL, $2, NULL, $3);
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
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_partition AS relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, NULL, $2, NULL);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_flashback AS relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $4, NULL, NULL, NULL, $2);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_partition use_flashback AS relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $5, NULL, $2, NULL, $3);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor sample_clause AS relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, NULL, NULL, $2);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor sample_clause seed AS relation_name
{
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $5, NULL, NULL, $2);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_partition sample_clause AS relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $5, NULL, $2, $3);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_partition sample_clause seed AS relation_name
{
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $6, NULL, $2, $3);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor AS relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, $$, NULL, NULL);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_partition AS relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, $$, $2, NULL);
  $$->sql_str_off_ = @1.first_column;
}

| relation_factor sample_clause AS relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, $$, NULL, $2);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor sample_clause seed AS relation_name index_hint_list
{
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
  merge_nodes($$, result, T_INDEX_HINT_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $5, $$, NULL, $2);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_partition sample_clause AS relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $5, $$, $2, $3);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_partition sample_clause seed AS relation_name index_hint_list
{
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
  merge_nodes($$, result, T_INDEX_HINT_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $6, $$, $2, $3);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $2, NULL, NULL, NULL);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_partition relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, NULL, $2, NULL);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_flashback relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $3, NULL, NULL, NULL, $2);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_partition use_flashback relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $4, NULL, $2, NULL, $3);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $2, $$, NULL, NULL);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_partition relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, $$, $2, NULL);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor sample_clause seed relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, NULL, NULL, $2);
  $$->sql_str_off_ = @1.first_column;
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
}
| relation_factor use_partition sample_clause seed relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $5, NULL, $2, $3);
  $$->sql_str_off_ = @1.first_column;
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
}
| relation_factor sample_clause seed relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, $$, NULL, $2);
  $$->sql_str_off_ = @1.first_column;
  if ($2 != NULL) {
    $2->children_[2] = $3;
  }
}
| relation_factor use_partition sample_clause seed relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $5, $$, $2, $3);
  $$->sql_str_off_ = @1.first_column;
  if ($3 != NULL) {
    $3->children_[2] = $4;
  }
}
| relation_factor sample_clause relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, NULL, NULL, $2);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_partition sample_clause relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, NULL, $2, $3);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor sample_clause relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $3, $$, NULL, $2);
  $$->sql_str_off_ = @1.first_column;
}
| relation_factor use_partition sample_clause relation_name index_hint_list
{
  merge_nodes($$, result, T_INDEX_HINT_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 5, $1, $4, $$, $2, $3);
  $$->sql_str_off_ = @1.first_column;
}
| TABLE '(' simple_expr ')' %prec LOWER_PARENS
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_COLLECTION_EXPRESSION, 2, $3, NULL);
}
| TABLE '(' simple_expr ')' relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_COLLECTION_EXPRESSION, 2, $3, $5);
}
| TABLE '(' simple_expr ')' AS relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_COLLECTION_EXPRESSION, 2, $3, $6);
}
;



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
  $$->sql_str_off_ = $3->sql_str_off_;
};

sample_percent:
INTNUM
{
  /* USE T_SFU_XXX to avoid being parsed by plan cache as template var */
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_INT);
  $$->value_ = $1->value_;
  $$->sql_str_off_ = $1->sql_str_off_;
}
|
DECIMAL_VAL
{
  /* USE T_SFU_XXX to avoid being parsed by plan cache as template var */
  malloc_terminal_node($$, result->malloc_pool_, T_SFU_DECIMAL);
  $$->str_len_ = $1->str_len_;
  $$->str_value_ = $1->str_value_;
  $$->sql_str_off_ = $1->sql_str_off_;
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
select_with_parens table_subquery_alias
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, $2);
  $$->sql_str_off_ = @1.first_column;
}
| select_with_parens AS table_subquery_alias
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, $3);
  $$->sql_str_off_ = @1.first_column;
}
| select_with_parens use_flashback table_subquery_alias
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $3, NULL, NULL, NULL, $2);
  $$->sql_str_off_ = @1.first_column;
}
| select_with_parens use_flashback AS table_subquery_alias
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 6, $1, $4, NULL, NULL, NULL, $2);
  $$->sql_str_off_ = @1.first_column;
}
;

table_subquery_alias:
relation_name
{
  $$ = $1;
}
| relation_name '(' alias_name_list ')'
{
  ParseNode *col_alias_list = NULL;
  merge_nodes(col_alias_list, result, T_COLUMN_LIST, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, col_alias_list);
}

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

use_flashback:
AS OF SNAPSHOT bit_expr %prec LOWER_PARENS
{
  ParseNode *unname_node = NULL;
  make_name_node(unname_node, result->malloc_pool_, "");
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_FLASHBACK_QUERY_SCN, 2, $4, unname_node);
}
;

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
relation_name opt_dblink
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 3, NULL, $1, $2);
  dup_node_string($1, $$, result->malloc_pool_);
}
| relation_name '.' relation_name opt_dblink
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 3, $1, $3, $4);
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

opt_dblink:
USER_VARIABLE  /* USER_VARIABLE is '@xxxx', see sql_parser.l */
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DBLINK_NAME, 2, $1, NULL);
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
'@' qb_name_string
{
  $$ = $2;
}
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

opt_relation_factor_in_hint_list:
/* EMPTY */
{
  $$ = NULL;
}
| relation_factor_in_hint
{
  $$ = $1;
}
| '(' relation_factor_in_hint_list ')'
{
  merge_nodes($$, result, T_RELATION_FACTOR_IN_HINT_LIST, $2);
}
;

relation_factor_in_pq_hint:
relation_factor_in_hint
{
  $$ = $1;
}
| '(' relation_factor_in_hint_list ')'
{
  merge_nodes($$, result, T_RELATION_FACTOR_IN_HINT_LIST, $2);
}
;

relation_factor_in_leading_hint_list:
relation_factor_in_hint
{
  $$ = $1;
}
| '(' relation_factor_in_leading_hint_list ')'
{
  $$ = $2;
}
| relation_factor_in_leading_hint_list relation_sep_option relation_factor_in_hint
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| relation_factor_in_leading_hint_list relation_sep_option '(' relation_factor_in_leading_hint_list ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $4);
}
;

relation_factor_in_use_join_hint_list:
relation_factor_in_hint
{
  $$ = $1;
}
| '(' relation_factor_in_hint_list ')'
{
  merge_nodes($$, result, T_RELATION_FACTOR_IN_HINT_LIST, $2);
}
| relation_factor_in_use_join_hint_list relation_sep_option relation_factor_in_hint
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| relation_factor_in_use_join_hint_list relation_sep_option '(' relation_factor_in_hint_list ')'
{
  ParseNode *table_list = NULL;
  merge_nodes(table_list, result, T_RELATION_FACTOR_IN_HINT_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, table_list);
}
;

intnum_list:
INTNUM relation_sep_option intnum_list
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
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
table_reference inner_join_type opt_full_table_factor %prec LOWER_ON
{
  JOIN_MERGE_NODES($1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $2, $1, $3, NULL, NULL);
}
| table_reference inner_join_type opt_full_table_factor ON expr
{
  JOIN_MERGE_NODES($1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $2, $1, $3, $5, NULL);
}
| table_reference inner_join_type opt_full_table_factor USING '(' column_list ')'
{
  JOIN_MERGE_NODES($1, $3);
  ParseNode *condition_node = NULL;
  merge_nodes(condition_node, result, T_COLUMN_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $2, $1, $3, condition_node, NULL);
}
| table_reference except_full_outer_join_type opt_full_table_factor join_condition
{
  JOIN_MERGE_NODES($1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $2, $1, $3, $4, NULL);
}
| table_reference FULL JOIN opt_full_table_factor join_condition
{
  JOIN_MERGE_NODES($1, $4);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_FULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $$, $1, $4, $5, NULL);
}
| table_reference FULL OUTER JOIN opt_full_table_factor join_condition
{
  JOIN_MERGE_NODES($1, $5);
  malloc_terminal_node($$, result->malloc_pool_, T_JOIN_FULL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $$, $1, $5, $6, NULL);
}
| table_reference FULL %prec LOWER_COMMA
{
  if ($1->type_ == T_ORG) {
    ParseNode *name_node = NULL;
    make_name_node(name_node, result->malloc_pool_, "full");
    $$ = new_node(result->malloc_pool_, T_ALIAS, $1->num_child_ + 1);
    if (OB_UNLIKELY($$ == NULL)) {
      yyerror(NULL, result, "No more space for malloc\n");
      YYABORT_NO_MEMORY;
    } else {
      for (int i = 0; i <= $1->num_child_; ++i) {
        if (i == 0) {
          $$->children_[i] = $1->children_[i];
        } else if (i == 1) {
          $$->children_[i] = name_node;
        } else {
          $$->children_[i] = $1->children_[i - 1];
        }
      }
      $$->sql_str_off_ = @1.first_column;
    }
  } else if ($1->type_ == T_ALIAS && $1->children_[1] != NULL &&
             strlen($1->children_[1]->str_value_) == 0) {
    ParseNode *name_node = NULL;
    make_name_node(name_node, result->malloc_pool_, "full");
    $1->children_[1] = name_node;
    $$ = $1;
  } else {
    yyerror(&@2, result, "occur multi alias name\n");
    YYERROR;
  }
}
| table_reference natural_join_type opt_full_table_factor
{
  JOIN_MERGE_NODES($1, $3);

  ParseNode *join_attr = NULL;
  malloc_terminal_node(join_attr, result->malloc_pool_, T_NATURAL_JOIN);

  malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 5, $2, $1, $3, NULL, join_attr);
}
;

opt_full_table_factor:
table_factor %prec LOWER_COMMA
{
  $$ = $1;
}
| table_factor FULL
{
  if ($1->type_ == T_ORG) {
    ParseNode *name_node = NULL;
    make_name_node(name_node, result->malloc_pool_, "full");
    $$ = new_node(result->malloc_pool_, T_ALIAS, $1->num_child_ + 1);
    if (OB_UNLIKELY($$ == NULL)) {
      yyerror(NULL, result, "No more space for malloc\n");
      YYABORT_NO_MEMORY;
    } else {
      for (int i = 0; i <= $1->num_child_; ++i) {
        if (i == 0) {
          $$->children_[i] = $1->children_[i];
        } else if (i == 1) {
          $$->children_[i] = name_node;
        } else {
          $$->children_[i] = $1->children_[i - 1];
        }
      }
      $$->sql_str_off_ = @1.first_column;
    }
  } else if ($1->type_ == T_ALIAS && $1->children_[1] != NULL &&
             strlen($1->children_[1]->str_value_) == 0) {
    ParseNode *name_node = NULL;
    make_name_node(name_node, result->malloc_pool_, "full");
    $1->children_[1] = name_node;
    $$ = $1;
  } else {
    yyerror(&@2, result, "occur multi alias name\n");
    YYERROR;
  }
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
| STRAIGHT_JOIN
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

except_full_outer_join_type:
LEFT opt_outer JOIN
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
 *	with clause (common table expression) (Mysql CTE grammer implement)
 *
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
  $$->value_ = 0;
}
|
WITH RECURSIVE with_list
{
  ParseNode *with_list = NULL;
  merge_nodes(with_list, result, T_WITH_CLAUSE_LIST, $3);
  $$ = with_list;
  $$->value_ = 1;
}
;

with_list:
with_list ',' common_table_expr
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| common_table_expr
{
  $$ = $1;
}
;

common_table_expr:
relation_name opt_column_alias_name_list AS '(' select_no_parens ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WITH_CLAUSE_AS, 5, $1, $2, $5, NULL, NULL);
}
| relation_name opt_column_alias_name_list AS '(' with_select ')'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WITH_CLAUSE_AS, 5, $1, $2, $5, NULL, NULL);
}
| relation_name opt_column_alias_name_list AS '(' select_with_parens ')'
{
  if ($5->children_[PARSE_SELECT_ORDER] != NULL && $5->children_[PARSE_SELECT_FETCH] == NULL) {
    yyerror(NULL, result, "only order by clause can't occur subquery\n");
    YYABORT_PARSE_SQL_ERROR;
  } else {
    malloc_non_terminal_node($$, result->malloc_pool_, T_WITH_CLAUSE_AS, 5, $1, $2, $5, NULL, NULL);
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

/*****************************************************************************
 *
 *	Values statement clause (Mysql8.0 values statement grammer implement)
 *  https://dev.mysql.com/doc/refman/8.0/en/values.html
 *
*****************************************************************************/
table_values_caluse:
VALUES values_row_list
{
  ParseNode *values_node = NULL;
  ParseNode *value_list = NULL;
  merge_nodes(value_list, result, T_VALUES_ROW_LIST, $2);
  malloc_non_terminal_node(values_node, result->malloc_pool_, T_VALUES_TABLE_EXPRESSION, 1, value_list);
  malloc_select_values_stmt($$, result, values_node, NULL, NULL);
}
;

table_values_caluse_with_order_by_and_limit:
VALUES values_row_list order_by
{
  ParseNode *values_node = NULL;
  ParseNode *value_list = NULL;
  merge_nodes(value_list, result, T_VALUES_ROW_LIST, $2);
  malloc_non_terminal_node(values_node, result->malloc_pool_, T_VALUES_TABLE_EXPRESSION, 1, value_list);
  malloc_select_values_stmt($$, result, values_node, $3, NULL);
}
| VALUES values_row_list opt_order_by limit_clause
{
  ParseNode *values_node = NULL;
  ParseNode *value_list = NULL;
  merge_nodes(value_list, result, T_VALUES_ROW_LIST, $2);
  malloc_non_terminal_node(values_node, result->malloc_pool_, T_VALUES_TABLE_EXPRESSION, 1, value_list);
  malloc_select_values_stmt($$, result, values_node, $3, $4);
}
;

values_row_list:
values_row_list ',' row_value
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| row_value
{
  $$ = $1;
}
;

row_value:
ROW '(' insert_vals ')'
{
  merge_nodes($$, result, T_VALUE_VECTOR, $3);
}
;

/*****************************************************************************
 *
 *	analyze clause (mysql compatible)
 *  added by guoping.wgp
 *
*****************************************************************************/
analyze_stmt:
ANALYZE TABLE relation_factor UPDATE HISTOGRAM ON column_name_list WITH INTNUM BUCKETS
{
  ParseNode *column_name_list = NULL;
  merge_nodes(column_name_list, result, T_ANALYZE_MYSQL_COLUMN_LIST, $7);
  if (OB_UNLIKELY($9->value_ < 1) || OB_UNLIKELY($9->value_ > 1024)) {
    yyerror(&@1, result, "bucket number should between 1 and 1024\n");
    YYERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_MYSQL_UPDATE_HISTOGRAM, 3, $3, column_name_list, $9);
}
| ANALYZE TABLE relation_factor DROP HISTOGRAM ON column_name_list
{
  ParseNode *column_name_list = NULL;
  merge_nodes(column_name_list, result, T_ANALYZE_MYSQL_COLUMN_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_MYSQL_DROP_HISTOGRAM, 2, $3, column_name_list);
}
| ANALYZE TABLE relation_factor analyze_statistics_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ANALYZE, 3, $3, NULL, $4);
}
| ANALYZE TABLE relation_factor use_partition analyze_statistics_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ANALYZE, 3, $3, $4, $5);
}
;

/*****************************************************************************
 *
 *	analyze clause (oracle syntax)
 *  added by link.zt
 *
*****************************************************************************/

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
//| opt_analyze_for_clause_list opt_analyze_for_clause_element
//{
//  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
//}
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
create_with_opt_hint opt_replace OUTLINE relation_name ON explainable_stmt opt_outline_target
{
  ParseNode *name_node = NULL;
  ParseNode *flag_node = new_terminal_node(result->malloc_pool_, T_DEFAULT);
  flag_node->value_ = 1;

  (void)($1);
  malloc_non_terminal_node(name_node, result->malloc_pool_, T_RELATION_FACTOR, 2, NULL, $4);
  dup_node_string($4, name_node, result->malloc_pool_);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_OUTLINE, 5, $2, name_node, flag_node, $6, $7);
  dup_expr_string($6, result, @6.first_column, @6.last_column);
}
|
create_with_opt_hint opt_replace OUTLINE relation_name ON STRING_VALUE USING HINT_HINT_BEGIN hint_list_with_end
{
  ParseNode *name_node = NULL;
  (void)($1);
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

| explain_or_desc explainable_stmt { GEN_EXPLAN_STMT($1, $$, 0, 0, $2, NULL, NULL);}
| explain_or_desc PRETTY explainable_stmt { GEN_EXPLAN_STMT($1, $$, 0, T_PRETTY, $3, NULL, NULL);}
| explain_or_desc PRETTY_COLOR explainable_stmt { GEN_EXPLAN_STMT($1, $$, 0, T_PRETTY_COLOR, $3, NULL, NULL);}

| explain_or_desc BASIC explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_BASIC, 0, $3, NULL, NULL);}
| explain_or_desc BASIC PRETTY explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_BASIC, T_PRETTY, $4, NULL, NULL);}
| explain_or_desc BASIC PRETTY_COLOR explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_BASIC, T_PRETTY_COLOR, $4, NULL, NULL);}

| explain_or_desc OUTLINE explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_OUTLINE, 0, $3, NULL, NULL);}
| explain_or_desc OUTLINE PRETTY explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_OUTLINE, T_PRETTY, $4, NULL, NULL);}
| explain_or_desc OUTLINE PRETTY_COLOR explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_OUTLINE, T_PRETTY_COLOR, $4, NULL, NULL);}

| explain_or_desc EXTENDED explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_EXTENDED, 0, $3, NULL, NULL);}
| explain_or_desc EXTENDED PRETTY explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_EXTENDED, T_PRETTY, $4, NULL, NULL);}
| explain_or_desc EXTENDED PRETTY_COLOR explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_EXTENDED, T_PRETTY_COLOR, $4, NULL, NULL);}

| explain_or_desc EXTENDED_NOADDR explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_EXTENDED_NOADDR, 0, $3, NULL, NULL);}
| explain_or_desc EXTENDED_NOADDR PRETTY explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_EXTENDED_NOADDR, T_PRETTY, $4, NULL, NULL);}
| explain_or_desc EXTENDED_NOADDR PRETTY_COLOR explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_EXTENDED_NOADDR, T_PRETTY_COLOR, $4, NULL, NULL);}

| explain_or_desc PLANREGRESS explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_PLANREGRESS, 0, $3, NULL, NULL);}
| explain_or_desc PLANREGRESS PRETTY explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_PLANREGRESS, T_PRETTY, $4, NULL, NULL);}
| explain_or_desc PLANREGRESS PRETTY_COLOR explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_PLANREGRESS, T_PRETTY_COLOR, $4, NULL, NULL);}

| explain_or_desc PARTITIONS explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_PARTITIONS, 0, $3, NULL, NULL);}
| explain_or_desc PARTITIONS PRETTY explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_PARTITIONS, T_PRETTY, $4, NULL, NULL);}
| explain_or_desc PARTITIONS PRETTY_COLOR explainable_stmt { GEN_EXPLAN_STMT($1, $$, T_PARTITIONS, T_PRETTY_COLOR, $4, NULL, NULL);}

| explain_or_desc SET STATEMENT_ID COMP_EQ literal explainable_stmt { GEN_EXPLAN_STMT($1, $$, 0, 0, $6, NULL, $5); }
| explain_or_desc INTO relation_name explainable_stmt { GEN_EXPLAN_STMT($1, $$, 0, 0, $4, $3, 0); }
| explain_or_desc INTO relation_name SET STATEMENT_ID COMP_EQ literal explainable_stmt { GEN_EXPLAN_STMT($1, $$, 0, 0, $8, $3, $7); }

| explain_or_desc FORMAT COMP_EQ format_name explainable_stmt
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_EXPLAIN, 5, $4, NULL, $5, NULL, NULL);
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
{ malloc_terminal_node($$, result->malloc_pool_, T_FORMAT_JSON); }
;

/*****************************************************************************
 *
 *	get diagnostics grammar
 *
 *****************************************************************************/
get_diagnostics_stmt:
get_condition_diagnostics_stmt
{
  $$ = $1;
}
| get_statement_diagnostics_stmt
{
  $$ = $1;
}
;

get_condition_diagnostics_stmt:
GET DIAGNOSTICS CONDITION condition_arg condition_information_item_list
{
  ParseNode *is_condition = NULL;
  ParseNode *is_current = NULL;
  malloc_terminal_node(is_condition, result->malloc_pool_, T_BOOL);
  malloc_terminal_node(is_current, result->malloc_pool_, T_BOOL);
  is_condition->value_ = 1;
  is_current->value_ = 1;
  ParseNode *item_list = NULL;
  merge_nodes(item_list, result, T_LINK_NODE, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DIAGNOSTICS, 4, is_condition, is_current, $4, item_list);
}
| GET CURRENT DIAGNOSTICS CONDITION condition_arg condition_information_item_list
{
  ParseNode *is_condition = NULL;
  ParseNode *is_current = NULL;
  malloc_terminal_node(is_condition, result->malloc_pool_, T_BOOL);
  malloc_terminal_node(is_current, result->malloc_pool_, T_BOOL);
  is_condition->value_ = 1;
  is_current->value_ = 1;
  ParseNode *item_list = NULL;
  merge_nodes(item_list, result, T_LINK_NODE, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DIAGNOSTICS, 4, is_condition, is_current, $5, item_list);
}
| GET STACKED DIAGNOSTICS CONDITION condition_arg condition_information_item_list
{
  ParseNode *is_condition = NULL;
  ParseNode *is_current = NULL;
  malloc_terminal_node(is_condition, result->malloc_pool_, T_BOOL);
  malloc_terminal_node(is_current, result->malloc_pool_, T_BOOL);
  is_condition->value_ = 1;
  is_current->value_ = 0;
  ParseNode *item_list = NULL;
  merge_nodes(item_list, result, T_LINK_NODE, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DIAGNOSTICS, 4, is_condition, is_current, $5, item_list);
}

condition_arg:
INTNUM
{
  $$ = $1;
}
| USER_VARIABLE
{
  $$ = $1;
}
| STRING_VALUE
{
  $$ = $1;
}
| BOOL_VALUE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->sql_str_off_ = $1->sql_str_off_;
  $$->value_ = $1->value_;
}
| QUESTIONMARK
{
  $$ = $1;
  if (result->pl_parse_info_.is_pl_parse_ || result->pl_parse_info_.is_inner_parse_) {
    dup_string($$, result, @1.first_column + 1, @1.last_column);
  } else {
    yyerror(&@1, result, "question mark as condition arg not allowed\n");
    YYABORT_PARSE_SQL_ERROR;
  }
}
| column_name
{
  $$ = $1;
  if (result->pl_parse_info_.is_pl_parse_) {
#ifndef SQL_PARSER_COMPILATION
    lookup_pl_exec_symbol($$, result, @1.first_column, @1.last_column, false, false, false);
#endif
  }
}
;

get_statement_diagnostics_stmt:
GET DIAGNOSTICS statement_information_item_list
{
  ParseNode *is_condition = NULL;
  ParseNode *is_current = NULL;
  malloc_terminal_node(is_condition, result->malloc_pool_, T_BOOL);
  malloc_terminal_node(is_current, result->malloc_pool_, T_BOOL);
  is_condition->value_ = 0;
  is_current->value_ = 1;
  ParseNode *item_list = NULL;
  merge_nodes(item_list, result, T_LINK_NODE, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DIAGNOSTICS, 4, is_condition, is_current, NULL, item_list);
}
| GET CURRENT DIAGNOSTICS statement_information_item_list
{
  ParseNode *is_condition = NULL;
  ParseNode *is_current = NULL;
  malloc_terminal_node(is_condition, result->malloc_pool_, T_BOOL);
  malloc_terminal_node(is_current, result->malloc_pool_, T_BOOL);
  is_condition->value_ = 0;
  is_current->value_ = 1;
  ParseNode *item_list = NULL;
  merge_nodes(item_list, result, T_LINK_NODE, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DIAGNOSTICS, 4, is_condition, is_current, NULL, item_list);
}
| GET STACKED DIAGNOSTICS statement_information_item_list
{
  ParseNode *is_condition = NULL;
  ParseNode *is_current = NULL;
  malloc_terminal_node(is_condition, result->malloc_pool_, T_BOOL);
  malloc_terminal_node(is_current, result->malloc_pool_, T_BOOL);
  is_condition->value_ = 0;
  is_current->value_ = 0;
  ParseNode *item_list = NULL;
  merge_nodes(item_list, result, T_LINK_NODE, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_DIAGNOSTICS, 4, is_condition, is_current, NULL, item_list);
}
;

statement_information_item_list:
statement_information_item_list ',' statement_information_item
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| statement_information_item
{
  $$ = $1;
}
;

condition_information_item_list:
condition_information_item_list ',' condition_information_item
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
| condition_information_item
{
  $$ = $1;
}
;

statement_information_item:
USER_VARIABLE COMP_EQ statement_information_item_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, $3);
}
| QUESTIONMARK COMP_EQ statement_information_item_name
{
  dup_string($1, result, @1.first_column + 1, @1.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, $3);
}
| diagnostics_info_ref COMP_EQ statement_information_item_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, $3);
}
;

condition_information_item:
USER_VARIABLE COMP_EQ condition_information_item_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, $3);
}
| QUESTIONMARK COMP_EQ condition_information_item_name
{
  dup_string($1, result, @1.first_column + 1, @1.last_column);
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, $3);
}
| diagnostics_info_ref COMP_EQ condition_information_item_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, $3);
}
;

diagnostics_info_ref:
column_name
{
  $$ = $1;
  if (result->pl_parse_info_.is_pl_parse_) {
#ifndef SQL_PARSER_COMPILATION
    lookup_pl_exec_symbol($$, result, @1.first_column, @1.last_column, false, false, true);
#endif
  }
}
;

statement_information_item_name:
NUMBER
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "NUMBER";
  $$->str_len_ = strlen("NUMBER");
}
| ROW_COUNT
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "ROW_COUNT";
  $$->str_len_ = strlen("ROW_COUNT");
}
;

condition_information_item_name:
CLASS_ORIGIN
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "CLASS_ORIGIN";
  $$->str_len_ = strlen("CLASS_ORIGIN");
}
| SUBCLASS_ORIGIN
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "SUBCLASS_ORIGIN";
  $$->str_len_ = strlen("SUBCLASS_ORIGIN");
}
| RETURNED_SQLSTATE
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "RETURNED_SQLSTATE";
  $$->str_len_ = strlen("RETURNED_SQLSTATE");
}
| MESSAGE_TEXT
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "MESSAGE_TEXT";
  $$->str_len_ = strlen("MESSAGE_TEXT");
}
| MYSQL_ERRNO
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "MYSQL_ERRNO";
  $$->str_len_ = strlen("MYSQL_ERRNO");
}
| CONSTRAINT_CATALOG
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "CONSTRAINT_CATALOG";
  $$->str_len_ = strlen("CONSTRAINT_CATALOG");
}
| CONSTRAINT_SCHEMA
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "CONSTRAINT_SCHEMA";
  $$->str_len_ = strlen("CONSTRAINT_SCHEMA");
}
| CONSTRAINT_NAME
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "CONSTRAINT_NAME";
  $$->str_len_ = strlen("CONSTRAINT_NAME");
}
| CATALOG_NAME
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "CATALOG_NAME";
  $$->str_len_ = strlen("CATALOG_NAME");
}
| SCHEMA_NAME
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "SCHEMA_NAME";
  $$->str_len_ = strlen("SCHEMA_NAME");
}
| TABLE_NAME
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "TABLE_NAME";
  $$->str_len_ = strlen("TABLE_NAME");
}
| COLUMN_NAME
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "COLUMN_NAME";
  $$->str_len_ = strlen("COLUMN_NAME");
}
| CURSOR_NAME
{
  malloc_terminal_node($$, result->malloc_pool_, T_VARCHAR);
  $$->str_value_ = "CURSOR_NAME";
  $$->str_len_ = strlen("CURSOR_NAME");
}
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
| SHOW create_with_opt_hint database_or_schema opt_if_not_exists database_factor
{
  (void)($2);
  (void)$3;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_DATABASE, 2, $4, $5);
}
| SHOW create_with_opt_hint TABLE relation_factor
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_TABLE, 1, $4);
}
| SHOW create_with_opt_hint VIEW relation_factor
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_VIEW, 1, $4);
}
| SHOW create_with_opt_hint PROCEDURE relation_factor
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_PROCEDURE, 1, $4);
}
| SHOW create_with_opt_hint FUNCTION relation_factor
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_FUNCTION, 1, $4);
}
| SHOW create_with_opt_hint TRIGGER relation_factor
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_TRIGGER, 1, $4);
}
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
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_TRACE, 2, $3, NULL); }
| SHOW TRACE FORMAT COMP_EQ STRING_VALUE opt_show_condition
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_TRACE, 2, $6, $5);
}
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
    ((ParseResult *)yyextra)->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;
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
| SHOW create_with_opt_hint TENANT relation_name
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_TENANT, 1, $4);
}
| SHOW opt_storage ENGINES
{
  (void)$2;
  malloc_terminal_node($$, result->malloc_pool_, T_SHOW_ENGINES);
}
| SHOW PRIVILEGES
{
  malloc_terminal_node($$, result->malloc_pool_, T_SHOW_PRIVILEGES);
}
| SHOW QUERY_RESPONSE_TIME
{
  malloc_terminal_node($$, result->malloc_pool_, T_SHOW_QUERY_RESPONSE_TIME);
}
| SHOW RECYCLEBIN
{
  malloc_terminal_node($$, result->malloc_pool_, T_SHOW_RECYCLEBIN);
}
| SHOW create_with_opt_hint TABLEGROUP relation_name
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_TABLEGROUP, 1, $4);
}
| SHOW RESTORE PREVIEW
{
  malloc_terminal_node($$, result->malloc_pool_, T_SHOW_RESTORE_PREVIEW);
}
| SHOW SEQUENCES opt_show_condition opt_from_or_in_database_clause
{ malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_SEQUENCES, 2, $3, $4); }
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

opt_storage_name:
/* EMPTY */
{
  $$ = NULL;
}
| STORAGE opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 1, $3);
}
;

calibration_info_list:
/* EMPTY */
{
  $$ = NULL;
}
| STRING_VALUE
{
  $$ = $1
}
| calibration_info_list ',' STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

opt_calibration_list:
/* EMPTY */
{
  $$ = NULL;
}
| CALIBRATION_INFO opt_equal_mark '(' calibration_info_list ')'
{
  (void)($2);
  merge_nodes($$, result, T_CALIBRATION_INFO_LIST, $4);
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
 *	tablespace grammar
 *
 *****************************************************************************/
create_tablespace_stmt:
create_with_opt_hint TABLESPACE tablespace permanent_tablespace
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLESPACE, 2, $3, $4);
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
permanent_tablespace_option:
ENCRYPTION opt_equal_mark STRING_VALUE
{
  (void)$2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ENCRYPTION, 1, $3);
}
;

drop_tablespace_stmt:
DROP TABLESPACE tablespace
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_TABLESPACE, 1, $3);
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
opt_set permanent_tablespace_option
{
  (void)$1;
  merge_nodes($$, result, T_TABLESPACE_OPTION_LIST, $2);
}
;

alter_tablespace_stmt:
ALTER TABLESPACE tablespace alter_tablespace_actions
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLESPACE, 2, $3, $4);
}
;

rotate_master_key_stmt:
ALTER INSTANCE ROTATE INNODB MASTER KEY
{
  malloc_terminal_node($$, result->malloc_pool_, T_ALTER_KEYSTORE_SET_KEY);
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


/*****************************************************************************
 *
 *	create user grammar
 *
 *****************************************************************************/
create_user_stmt:
create_with_opt_hint USER opt_if_not_exists user_specification_list opt_resource_option
{
  ParseNode *users_node = NULL;
  (void)($1);
  merge_nodes(users_node, result, T_USERS, $4);
  ParseNode *res_opt_node = NULL;
  merge_nodes(res_opt_node, result, T_USER_RESOURCE_OPTIONS, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER, 4, $3, users_node, NULL, res_opt_node);
}
| create_with_opt_hint USER opt_if_not_exists user_specification_list require_specification opt_resource_option
{
  ParseNode *users_node = NULL;
  (void)($1);
  merge_nodes(users_node, result, T_USERS, $4);
  ParseNode *require_node = NULL;
  merge_nodes(require_node, result, T_TLS_OPTIONS, $5);
  ParseNode *res_opt_node = NULL;
  merge_nodes(res_opt_node, result, T_USER_RESOURCE_OPTIONS, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER, 4, $3, users_node, require_node, res_opt_node);
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

opt_auth_plugin:
WITH STRING_VALUE
{
  $$ = $2;
}
| WITH NAME_OB
{
  $$ = $2;
}
|
{
  $$ = NULL;
}
;

user_specification:
user opt_host_name
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER_SPEC, 5, $1, NULL, need_enc_node, $2, NULL);
}
| user opt_host_name IDENTIFIED opt_auth_plugin BY password
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER_SPEC, 5, $1, $6, need_enc_node, $2, $4);
}
| user opt_host_name IDENTIFIED opt_auth_plugin BY PASSWORD password
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER_SPEC, 5, $1, $7, need_enc_node, $2, $4);
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

opt_resource_option:
WITH resource_option_list
{
  $$ = $2;
}
|
{
  $$ = NULL;
}
;

resource_option_list:
resource_option_list resource_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
| resource_option
{
  $$ = $1;
}

resource_option:
MAX_CONNECTIONS_PER_HOUR INTNUM
{
  malloc_terminal_node($$, result->malloc_pool_, T_MAX_CONNECTIONS_PER_HOUR);
  $$->value_ = $2->value_;
  $$->sql_str_off_ = $2->sql_str_off_;
}
| MAX_USER_CONNECTIONS INTNUM
{
  malloc_terminal_node($$, result->malloc_pool_, T_MAX_USER_CONNECTIONS);
  $$->value_ = $2->value_;
  $$->sql_str_off_ = $2->sql_str_off_;
}
/*
| MAX_QUERIES_PER_HOUR INTNUM
{
  malloc_terminal_node($$, result->malloc_pool_, T_MAX_QUERIES_PER_HOUR);
  $$->value_ = $2->value_;
}
| MAX_UPDATES_PER_HOUR INTNUM
{
  malloc_terminal_node($$, result->malloc_pool_, T_MAX_UPDATES_PER_HOUR);
  $$->value_ = $2->value_;
}
*/
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
  result->may_contain_sensitive_data_ = true;
  $$ = $1;
}
| NAME_OB
{
  result->may_contain_sensitive_data_ = true;
  $$ = $1;
}
| unreserved_keyword
{
  result->may_contain_sensitive_data_ = true;
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
  result->contain_sensitive_data_ = true;
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 5, $3, $5, need_enc_node, NULL, NULL);
}
| SET PASSWORD opt_for_user COMP_EQ PASSWORD '(' password ')'
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 5, $3, $7, need_enc_node, NULL, NULL);
}
| ALTER USER user_with_host_name IDENTIFIED opt_auth_plugin BY password
{
  ParseNode *need_enc_node = NULL;
  malloc_terminal_node(need_enc_node, result->malloc_pool_, T_BOOL);
  need_enc_node->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 5, $3, $7, need_enc_node, NULL, $5);
}
| ALTER USER user_with_host_name require_specification
{
  ParseNode *require_node = NULL;
  merge_nodes(require_node, result, T_TLS_OPTIONS, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 5, $3, NULL, NULL, require_node, NULL);
}
| ALTER USER user_with_host_name WITH resource_option_list
{
  ParseNode *res_opt_node = NULL;
  merge_nodes(res_opt_node, result, T_USER_RESOURCE_OPTIONS, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 5, $3, NULL, NULL, res_opt_node, NULL);
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
 *  banliu.zyd: 只进行空实现
 *
 *****************************************************************************/


lock_tables_stmt:
LOCK_ table_or_tables lock_table_list
{
  (void)$2;
  (void)$3;
  malloc_terminal_node($$, result->malloc_pool_, T_LOCK_TABLE);
}
;

unlock_tables_stmt:
UNLOCK TABLES
{
  malloc_terminal_node($$, result->malloc_pool_, T_LOCK_TABLE);
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
 *	create sequence grammar
 *
 *****************************************************************************/

create_sequence_stmt:
create_with_opt_hint SEQUENCE opt_if_not_exists relation_factor opt_sequence_option_list
{
  ParseNode *sequence_option = NULL;
  (void)($1);
  merge_nodes(sequence_option, result, T_SEQUENCE_OPTION_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_SEQUENCE, 3, $4, sequence_option, $3);
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
|
RESTART
{
  malloc_terminal_node($$, result->malloc_pool_, T_RESTART);
}
;

simple_num:
'+' INTNUM %prec '+'
{
  $2->type_ = T_NUMBER;
  $$ = $2;
}
|
'-' INTNUM %prec '-'
{
  int32_t len = $2->str_len_ + 2; /* 2 bytes for sign and terminator '\0' */
  char *str_value = (char *)parse_malloc(len, result->malloc_pool_);
  if (OB_LIKELY(NULL != str_value)) {
    snprintf(str_value, len, "-%.*s", (int32_t)($2->str_len_), $2->str_value_);
    $2->type_ = T_NUMBER;
    $$ = $2;
    $$->str_value_ = str_value;
    $$->str_len_ = $2->str_len_ + 1;
  } else {
    yyerror(NULL, result, "No more space for copying expression string\n");
    YYABORT_NO_MEMORY;
  }
}
|
INTNUM
{
  $1->type_ = T_NUMBER;
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
DROP SEQUENCE opt_if_exists relation_factor
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_SEQUENCE, 2, $4, $3);
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
create_with_opt_hint DATABASE LINK dblink CONNECT TO user tenant DATABASE database_factor IDENTIFIED BY password ip_port opt_cluster
{
  (void)($1);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_DBLINK, 10,
                           NULL,    /* opt_if_not_exists */
                           $4,    /* dblink name */
                           $7,    /* user name */
                           $8,    /* tenant name */
                           $10,   /* database name */
                           $13,   /* password */
                           NULL,   /* driver */
                           $14,   /* host ip port*/
                           $15,   /* optional cluster */
                           NULL    /* optional self credential */);
}
| create_with_opt_hint DATABASE LINK IF not EXISTS dblink CONNECT TO user tenant DATABASE database_factor IDENTIFIED BY password ip_port opt_cluster
{
  (void)($1);
  (void)($5);
  malloc_terminal_node($$, result->malloc_pool_, T_IF_NOT_EXISTS);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_DBLINK, 10,
                           $$,    /* opt_if_not_exists */
                           $7,    /* dblink name */
                           $10,    /* user name */
                           $11,    /* tenant name */
                           $13,   /* database name */
                           $16,   /* password */
                           NULL,   /* driver */
                           $17,   /* host ip port*/
                           $18,   /* optional cluster */
                           NULL    /* optional self credential */);
}
;

drop_dblink_stmt:
DROP DATABASE LINK opt_if_exists dblink
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_DBLINK, 2, $4, $5);
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
CLUSTER STRING_VALUE
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
BEGI opt_hint_value opt_work
{
  (void)$3;
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BEGIN, 2, $$, $2);
}
| START opt_hint_value TRANSACTION opt_with_consistent_snapshot
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = $4[0];
  malloc_non_terminal_node($$, result->malloc_pool_, T_BEGIN, 2, $$, $2);
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
COMMIT opt_hint_value opt_work
{
  (void)$3;
  malloc_non_terminal_node($$, result->malloc_pool_, T_COMMIT, 1, $2);
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_ROLLBACK, 1, NULL);
}
| ROLLBACK HINT_VALUE opt_work
{
  (void)$3;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ROLLBACK, 1, $2);
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
|create_with_opt_hint
{
  (void)($1);
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE;
}
| create_with_opt_hint USER
{
  (void)($1);
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
| create_with_opt_hint VIEW
{
  (void)($1);
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
| create_with_opt_hint RESOURCE POOL
{
  (void)($1);
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE_RESOURCE_POOL;
}
| create_with_opt_hint RESOURCE UNIT
{
  (void)($1);
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE_RESOURCE_UNIT;
}
| REPLICATION SLAVE
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_REPL_SLAVE;
}
| REPLICATION CLIENT
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_REPL_CLIENT;
}
| DROP DATABASE LINK
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_DROP_DATABASE_LINK;
}
| CREATE DATABASE LINK
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
  $$->value_ = OB_PRIV_CREATE_DATABASE_LINK;
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

audit_user_list:
audit_user_with_host_name
{
  $$ = $1;
}
| audit_user_list ',' audit_user_with_host_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

audit_user_with_host_name:
audit_user opt_host_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_USER_WITH_HOST_NAME, 2, $1, $2);
}
;

audit_user:
STRING_VALUE
{
  $$ = $1;
}
| NAME_OB
{
  $$ = $1;
}
| unreserved_keyword_normal
{
  get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
}
;

auditing_by_user_clause:
BY audit_user_list
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
| SESSION
{
  malloc_terminal_node($$, result->malloc_pool_, T_AUDIT_SESSION);
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLE, 3, $3, table_actions, NULL);
  $$->value_ = 0;
}
|
ALTER EXTERNAL TABLE relation_factor alter_table_actions
{
  ParseNode *table_actions = NULL;
  merge_nodes(table_actions, result, T_ALTER_TABLE_ACTION_LIST, $5);
  ParseNode *external_node = NULL;
  malloc_terminal_node(external_node, result->malloc_pool_, T_EXTERNAL);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLE, 3, $4, table_actions, external_node);
  $$->value_ = 0;
}
;

alter_table_actions:
alter_table_action_list
{
  $$ = $1;
}
| standalone_alter_action
{
  $$ = $1;
}
|
{ $$ = NULL; }
;

alter_table_action_list:
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
opt_set table_option_list_space_seperated
{
  (void)$1;
  merge_nodes($$, result, T_TABLE_OPTION_LIST, $2);
}
| CONVERT TO CHARACTER SET charset_name opt_collation
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CONVERT_TO_CHARACTER, 2, $5, $6);
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
| alter_constraint_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_CHECK_CONSTRAINT_OPTION, 1, $1);
}
| alter_foreign_key_action
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_FOREIGN_KEY_OPTION, 1, $1);
}
|
DROP CONSTRAINT constraint_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_CONSTRAINT, 1, $3); // drop foreign key or check constraint, to be compatible with mysql
}
| REFRESH
{
  malloc_terminal_node($$, result->malloc_pool_, T_ALTER_REFRESH_EXTERNAL_TABLE);
}
/*  | ORDER BY column_list
//    {
//      ParseNode *col_list = NULL;
//      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, $3);
//      malloc_non_terminal_node($$, result->malloc_pool_, T_ORDER_BY, 1, col_list);
//    }*/
| REMOVE TTL
{
  malloc_terminal_node($$, result->malloc_pool_, T_REMOVE_TTL);
}
;

// mysql 模式下的 constraint 特指 check constraint
alter_constraint_option:
DROP CONSTRAINT '(' name_list ')'
{
  merge_nodes($$, result, T_NAME_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 1, $$);
  $$->value_ = 0; //only support drop check constraint
}
|
DROP CHECK '(' name_list ')'
{
  merge_nodes($$, result, T_NAME_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 1, $$);
  $$->value_ = 0; //drop check constraint
}
|
DROP CHECK constraint_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECK_CONSTRAINT, 1, $3);
  $$->value_ = 0; //drop check constraint
}
|
ADD constraint_definition
{
  $$ = $2;
  $$->value_ = 1;
}
;

standalone_alter_action:
alter_partition_option
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_PARTITION_OPTION, 1, $1);
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
ADD add_key_or_index_opt
{
  $$ = $2;
}
| ADD add_unique_key_opt
{
  $$ = $2;
}
| ADD add_constraint_uniq_key_opt
{
  $$ = $2;
}
| ADD add_spatial_index_opt
{
  $$ = $2;
}
| ADD add_constraint_pri_key_opt
{
  $$ = $2;
}
| ADD add_primary_key_opt
{
  $$ = $2;
}
| DROP key_or_index index_name
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_DROP, 1, $3);
}
| DROP PRIMARY KEY
{
  malloc_terminal_node($$, result->malloc_pool_, T_PRIMARY_KEY_DROP);
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
| ALTER CONSTRAINT constraint_name check_state
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MODIFY_CONSTRAINT_OPTION, 2, $3, $4);
  $$->value_ = 0; // alter state of a check constraint or foreign key
}
| ALTER CHECK constraint_name check_state
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MODIFY_CONSTRAINT_OPTION, 2, $3, $4);
  $$->value_ = 1; // alter state of a check constraint
}
;

add_key_or_index_opt:
add_key_or_index
{
  $$ = $1;
}
| '(' add_key_or_index ')'
{
  $$ = $2;
}
;

add_key_or_index:
key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list opt_partition_option
{
  (void)($1);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $5);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_ADD, 5, $2, col_list, index_option, $3, $8);
  $$->value_ = 0;
}
;

add_unique_key_opt:
add_unique_key
{
  $$ = $1;
}
| '(' add_unique_key ')'
{
  $$ = $2;
}
;

add_unique_key:
UNIQUE opt_key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list opt_partition_option
{
  (void)($2);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $6);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_ADD, 5, $3, col_list, index_option, $4, $9);
  $$->value_ = 1;
}
;

add_constraint_uniq_key_opt:
add_constraint_uniq_key
{
  $$ = $1;
}
| '(' add_constraint_uniq_key ')'
{
  $$ = $2;
}
;

add_constraint_uniq_key:
CONSTRAINT opt_constraint_name UNIQUE opt_key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list opt_partition_option
{
  (void)($4);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $8);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $10);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_ADD, 5, $5 ? $5 : $2, col_list, index_option, $6, $11);
  $$->value_ = 1;
}
;

add_constraint_pri_key_opt:
add_constraint_pri_key
{
  $$ = $1;
}
| '(' add_constraint_pri_key ')'
{
  $$ = $2;
}
;

add_constraint_pri_key:
CONSTRAINT opt_constraint_name PRIMARY KEY '(' column_name_list ')' opt_index_option_list
{
  (void)($2);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_COLUMN_LIST, $6);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_KEY, 2, col_list, index_option);
}
;

add_spatial_index_opt:
add_spatial_index
{
  $$ = $1;
}
| '(' add_spatial_index ')'
{
  $$ = $2;
}
;

add_spatial_index:
SPATIAL opt_key_or_index opt_index_name opt_index_using_algorithm '(' sort_column_list ')' opt_index_option_list opt_partition_option
{
  (void)($2);
  (void)($9);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_INDEX_COLUMN_LIST, $6);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $8);
  malloc_non_terminal_node($$, result->malloc_pool_, T_INDEX_ADD, 5, $3, col_list, index_option, $4, NULL);
  $$->value_ = 2;
}
;

add_primary_key_opt:
add_primary_key
{
  $$ = $1;
}
| '(' add_primary_key ')'
{
  $$ = $2;
}
;

add_primary_key:
PRIMARY KEY opt_index_name '(' column_name_list ')' opt_index_option_list
{
  (void)($3);
  ParseNode *col_list = NULL;
  ParseNode *index_option = NULL;
  merge_nodes(col_list, result, T_COLUMN_LIST, $5);
  merge_nodes(index_option, result, T_TABLE_OPTION_LIST, $7);
  malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_KEY, 2, col_list, index_option);
}
;

alter_foreign_key_action:
DROP FOREIGN KEY index_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOREIGN_KEY_DROP, 1, $4);
}
| ADD CONSTRAINT opt_constraint_name FOREIGN KEY opt_index_name '(' column_name_list ')' REFERENCES relation_factor '(' column_name_list ')' opt_match_option opt_reference_option_list
{
  ParseNode *child_col_list= NULL;
  ParseNode *parent_col_list= NULL;
  ParseNode *reference_option_list = NULL;
  ParseNode *constraint_node = NULL;
  merge_nodes(child_col_list, result, T_COLUMN_LIST, $8);
  merge_nodes(parent_col_list, result, T_COLUMN_LIST, $13);
  merge_nodes(reference_option_list, result, T_REFERENCE_OPTION_LIST, $16);
  malloc_non_terminal_node(constraint_node, result->malloc_pool_, T_CHECK_CONSTRAINT, 1, $3);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOREIGN_KEY, 7, child_col_list, $11, parent_col_list, reference_option_list, constraint_node, $6, $15);
}
| ADD FOREIGN KEY opt_index_name '(' column_name_list ')' REFERENCES relation_factor '(' column_name_list ')' opt_match_option opt_reference_option_list
{
  ParseNode *child_col_list= NULL;
  ParseNode *parent_col_list= NULL;
  ParseNode *reference_option_list = NULL;
  merge_nodes(child_col_list, result, T_COLUMN_LIST, $6);
  merge_nodes(parent_col_list, result, T_COLUMN_LIST, $11);
  merge_nodes(reference_option_list, result, T_REFERENCE_OPTION_LIST, $14);
  malloc_non_terminal_node($$, result->malloc_pool_, T_FOREIGN_KEY, 7, child_col_list, $9, parent_col_list, reference_option_list, NULL, $4, $13);
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
ADD COLUMN column_definition
{
  (void)($2); /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ADD, 1, $3);
}
| ADD column_definition
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ADD, 1, $2);
}
| ADD COLUMN '(' column_definition_list ')'
{
  (void)($2); /* make bison mute */
  merge_nodes($$, result, T_COLUMN_ADD, $4);
}
| ADD '(' column_definition_list ')'
{
  merge_nodes($$, result, T_COLUMN_ADD, $3);
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
| ALTER COLUMN column_definition_ref alter_column_behavior
{
  (void)($2); /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ALTER, 2, $3, $4);
}
| ALTER column_definition_ref alter_column_behavior
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ALTER, 2, $2, $3);
}
| CHANGE COLUMN column_definition_ref column_definition
{
  (void)($2); /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_CHANGE, 2, $3, $4 );
}
| CHANGE column_definition_ref column_definition
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_CHANGE, 2, $2, $3 );
}
| MODIFY COLUMN column_definition
{
  (void)($2); /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_MODIFY, 1, $3);
}
| MODIFY column_definition
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_MODIFY, 1, $2);
}
| RENAME COLUMN column_definition_ref TO column_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_RENAME, 2, $3, $5);
  if ($3->children_[0] != NULL || $3->children_[1] != NULL) {
    yyerror(&@3, result, "");
    YYERROR;
  }
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
flashback_stmt:
FLASHBACK TABLE relation_factor TO BEFORE DROP opt_flashback_rename_table
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
DUMP CHUNK TENANT_ID COMP_EQ INTNUM ',' CTX_ID COMP_EQ relation_name_or_string
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
  malloc_non_terminal_node($$, result->malloc_pool_, T_BOOTSTRAP, 1, server_list);
}
|
ALTER SYSTEM FLUSH cache_type CACHE opt_namespace opt_sql_id opt_databases opt_tenant_list flush_scope
{
  // system tenant use only.
  malloc_non_terminal_node($$, result->malloc_pool_, T_FLUSH_CACHE, 6, $4, $6, $7, $8, $9, $10);
}
|
// this just is a Syntactic sugar, only used to be compatible to plan cache's Grammar
ALTER SYSTEM FLUSH SQL cache_type opt_tenant_list flush_scope
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FLUSH_CACHE, 6, $5, NULL, NULL, NULL, $6, $7);
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
ALTER SYSTEM SWITCH REPLICA ls_role ls_server_or_server_or_zone_or_tenant
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SWITCH_REPLICA_ROLE, 2, $5, $6);
}
|
ALTER SYSTEM SWITCH ROOTSERVICE partition_role server_or_zone
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SWITCH_RS_ROLE, 2, $5, $6);
}
|
ALTER SYSTEM REPORT REPLICA opt_server_or_zone
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
ALTER SYSTEM suspend_or_resume MERGE opt_tenant_list_v2
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_MERGE_CONTROL, 2, $3, $5);
}
|
ALTER SYSTEM suspend_or_resume RECOVERY opt_zone_desc
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RECOVERY_CONTROL, 2, $3, $5);
}
|
ALTER SYSTEM CLEAR MERGE ERROR_P opt_tenant_list_v2
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CLEAR_MERGE_ERROR, 1, $6);
}
|
ALTER SYSTEM ADD ARBITRATION SERVICE STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ADD_ARBITRATION_SERVICE, 1, $6);
}
|
ALTER SYSTEM REMOVE ARBITRATION SERVICE STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_REMOVE_ARBITRATION_SERVICE, 1, $6);
}
|
ALTER SYSTEM REPLACE ARBITRATION SERVICE STRING_VALUE WITH STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_REPLACE_ARBITRATION_SERVICE, 2, $6, $8);
}
|
ALTER SYSTEM CANCEL cancel_task_type TASK STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CANCEL_TASK, 2, $4, $6);
}
|
ALTER SYSTEM MAJOR FREEZE opt_tenant_list_v2
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
ALTER SYSTEM MINOR FREEZE opt_tenant_list_or_ls_or_tablet_id opt_server_list opt_zone_desc
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_FREEZE, 4, type, $5, $6, $7);
}
|
ALTER SYSTEM CHECKPOINT SLOG opt_tenant_info ip_port
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CHECKPOINT_SLOG, 2, $5, $6);
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
ALTER SYSTEM WASH MEMORY FRAGMENTATION opt_server_or_zone
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_WASH_MEMORY_FRAGMENTATION, 1, $6);
}
|
ALTER SYSTEM REFRESH IO CALIBRATION opt_storage_name opt_calibration_list opt_server_or_zone
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_REFRESH_IO_CALIBRATION, 3, $6, $7, $8);
}
|
ALTER SYSTEM opt_set alter_system_set_parameter_actions
{
  (void)$3;
  result->contain_sensitive_data_ = true;
  merge_nodes($$, result, T_SYTEM_ACTION_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SYSTEM_SET_PARAMETER, 1, $$);
}
|
ALTER SYSTEM SET_TP alter_system_settp_actions opt_server_or_zone
{
  merge_nodes($$, result, T_SYTEM_SETTP_LIST, $4);
  malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SYSTEM_SETTP, 2, $$, $5);
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
ALTER SYSTEM RUN UPGRADE JOB STRING_VALUE opt_tenant_list_v2
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ADMIN_RUN_UPGRADE_JOB, 2, $6, $7);
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
ALTER SYSTEM SET NETWORK BANDWIDTH REGION relation_name_or_string TO relation_name_or_string conf_const
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SET_REGION_NETWORK_BANDWIDTH, 3, $7, $9, $10);
}
|
ALTER SYSTEM ADD RESTORE SOURCE STRING_VALUE
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_ADD_RESTORE_SOURCE, 1, $6);
}
|
ALTER SYSTEM CLEAR RESTORE SOURCE
{
  malloc_terminal_node($$, result->malloc_pool_, T_CLEAR_RESTORE_SOURCE);
}
|
ALTER SYSTEM RECOVER TABLE recover_table_list opt_recover_tenant opt_backup_dest opt_restore_until WITH STRING_VALUE opt_encrypt_key opt_backup_key_info opt_recover_remap_item_list opt_description
{
  ParseNode *tables = NULL;
  merge_nodes(tables, result, T_TABLE_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_RECOVER_TABLE, 9, $6, $7, $8, $10, tables, $11, $12, $13, $14);
}
|
ALTER SYSTEM RESTORE FROM STRING_VALUE opt_restore_until PREVIEW
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PHYSICAL_RESTORE_TENANT, 2, $5, $6);
}
|
ALTER SYSTEM RESTORE relation_name opt_backup_dest opt_restore_until WITH STRING_VALUE opt_encrypt_key opt_backup_key_info opt_description
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_PHYSICAL_RESTORE_TENANT, 7, $4, $5, $6, $8, $9, $10, $11);
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
ALTER SYSTEM ARCHIVELOG opt_backup_tenant_list opt_description
{
  ParseNode *enable = NULL;
  malloc_terminal_node(enable, result->malloc_pool_, T_INT);
  enable->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ARCHIVE_LOG, 3, enable, $4, $5);
}
|
ALTER SYSTEM NOARCHIVELOG opt_backup_tenant_list opt_description
{
  ParseNode *enable = NULL;
  malloc_terminal_node(enable, result->malloc_pool_, T_INT);
  enable->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_ARCHIVE_LOG, 3, enable, $4, $5);
}
|
ALTER SYSTEM BACKUP DATABASE opt_backup_to opt_description
{
  ParseNode *incremental = NULL;
  malloc_terminal_node(incremental, result->malloc_pool_, T_INT);
  incremental->value_ = 0;
  ParseNode *compl_log = NULL;
  malloc_terminal_node(compl_log, result->malloc_pool_, T_INT);
  compl_log->value_ = 0;
  ParseNode *tenant = NULL;
  malloc_terminal_node(tenant, result->malloc_pool_, T_INT);
  tenant->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_DATABASE, 5, tenant, compl_log, incremental, $5, $6);
}
|
ALTER SYSTEM BACKUP INCREMENTAL DATABASE opt_backup_to opt_description
{
  ParseNode *incremental = NULL;
  malloc_terminal_node(incremental, result->malloc_pool_, T_INT);
  incremental->value_ = 1;

  ParseNode *compl_log = NULL;
  malloc_terminal_node(compl_log, result->malloc_pool_, T_INT);
  compl_log->value_ = 0;
  ParseNode *tenant = NULL;
  malloc_terminal_node(tenant, result->malloc_pool_, T_INT);
  tenant->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_DATABASE, 5, tenant, compl_log, incremental, $6, $7);
}
|
ALTER SYSTEM BACKUP opt_backup_tenant_list opt_backup_to opt_description
{
  ParseNode *incremental = NULL;
  malloc_terminal_node(incremental, result->malloc_pool_, T_INT);
  incremental->value_ = 0;
  ParseNode *compl_log = NULL;
  malloc_terminal_node(compl_log, result->malloc_pool_, T_INT);
  compl_log->value_ = 0;
  ParseNode *tenant = NULL;
  malloc_terminal_node(tenant, result->malloc_pool_, T_INT);
  tenant->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_DATABASE, 6, tenant, compl_log, incremental, $4, $5, $6);
}
|
ALTER SYSTEM BACKUP INCREMENTAL opt_backup_tenant_list opt_backup_to opt_description
{
  ParseNode *incremental = NULL;
  malloc_terminal_node(incremental, result->malloc_pool_, T_INT);
  incremental->value_ = 1;
  ParseNode *compl_log = NULL;
  malloc_terminal_node(compl_log, result->malloc_pool_, T_INT);
  compl_log->value_ = 0;
  ParseNode *tenant = NULL;
  malloc_terminal_node(tenant, result->malloc_pool_, T_INT);
  tenant->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_DATABASE, 6, tenant, compl_log, incremental, $5, $6, $7);
}
|
ALTER SYSTEM BACKUP DATABASE opt_backup_to PLUS ARCHIVELOG opt_description
{
  ParseNode *incremental = NULL;
  malloc_terminal_node(incremental, result->malloc_pool_, T_INT);
  incremental->value_ = 0;
  ParseNode *compl_log = NULL;
  malloc_terminal_node(compl_log, result->malloc_pool_, T_INT);
  compl_log->value_ = 1;
  ParseNode *tenant = NULL;
  malloc_terminal_node(tenant, result->malloc_pool_, T_INT);
  tenant->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_DATABASE, 5, tenant, compl_log, incremental, $5, $8);
}
|
ALTER SYSTEM BACKUP INCREMENTAL DATABASE opt_backup_to PLUS ARCHIVELOG opt_description
{
  ParseNode *incremental = NULL;
  malloc_terminal_node(incremental, result->malloc_pool_, T_INT);
  incremental->value_ = 1;
  ParseNode *compl_log = NULL;
  malloc_terminal_node(compl_log, result->malloc_pool_, T_INT);
  compl_log->value_ = 1;
  ParseNode *tenant = NULL;
  malloc_terminal_node(tenant, result->malloc_pool_, T_INT);
  tenant->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_DATABASE, 5, tenant, compl_log, incremental, $6, $9);
}
|
ALTER SYSTEM BACKUP opt_backup_tenant_list opt_backup_to PLUS ARCHIVELOG opt_description
{
  ParseNode *incremental = NULL;
  malloc_terminal_node(incremental, result->malloc_pool_, T_INT);
  incremental->value_ = 0;
  ParseNode *compl_log = NULL;
  malloc_terminal_node(compl_log, result->malloc_pool_, T_INT);
  compl_log->value_ = 1;
  ParseNode *tenant = NULL;
  malloc_terminal_node(tenant, result->malloc_pool_, T_INT);
  tenant->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_DATABASE, 6, tenant, compl_log, incremental, $4, $5, $8);
}
|
ALTER SYSTEM BACKUP INCREMENTAL opt_backup_tenant_list opt_backup_to PLUS ARCHIVELOG opt_description
{
  ParseNode *incremental = NULL;
  malloc_terminal_node(incremental, result->malloc_pool_, T_INT);
  incremental->value_ = 1;
  ParseNode *compl_log = NULL;
  malloc_terminal_node(compl_log, result->malloc_pool_, T_INT);
  compl_log->value_ = 1;
  ParseNode *tenant = NULL;
  malloc_terminal_node(tenant, result->malloc_pool_, T_INT);
  tenant->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_DATABASE, 6, tenant, compl_log, incremental, $5, $6, $9);
}
|
ALTER SYSTEM BACKUP KEY opt_backup_to opt_encrypt_key
{
  ParseNode *tenant = NULL;
  malloc_terminal_node(tenant, result->malloc_pool_, T_INT);
  tenant->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_KEY, 3, tenant, $5, $6);
}
|
ALTER SYSTEM BACKUP KEY tenant_list_tuple opt_backup_to opt_encrypt_key
{
  ParseNode *tenant = NULL;
  malloc_terminal_node(tenant, result->malloc_pool_, T_INT);
  tenant->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_KEY, 4, tenant, $5, $6, $7);
}
|
ALTER SYSTEM CANCEL BACKUP opt_backup_tenant_list
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 0;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;

  ParseNode *tenant = NULL;
  malloc_terminal_node(tenant, result->malloc_pool_, T_INT);
  tenant->value_ = 1;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 4, type, value, tenant, $5);
}
|
ALTER SYSTEM CANCEL RESTORE relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CANCEL_RESTORE, 1, $5);
}
|
ALTER SYSTEM CANCEL RECOVER TABLE relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_CANCEL_RECOVER_TABLE, 1, $6);
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
ALTER SYSTEM TRIGGER TTL opt_tenant_list_v2
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_TTL, 2, type, $5);
}
|
ALTER SYSTEM SUSPEND TTL opt_tenant_list_v2
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_TTL, 2, type, $5);
}
|
ALTER SYSTEM RESUME TTL opt_tenant_list_v2
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 2;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_TTL, 2, type, $5);
}
|
ALTER SYSTEM CANCEL TTL opt_tenant_list_v2
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 3;
  malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_TTL, 2, type, $5);
}
|
ALTER SYSTEM VALIDATE DATABASE opt_copy_id
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 5;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 3, type, value, $5);
}
|
ALTER SYSTEM VALIDATE BACKUPSET INTNUM opt_copy_id
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 6;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = $5->value_;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 3, type, value, $6);
}
|
ALTER SYSTEM CANCEL VALIDATE INTNUM opt_copy_id
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 7;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = $5->value_;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 3, type, value, $6);
}
|
ALTER SYSTEM CANCEL BACKUP BACKUPSET
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 9;
  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
ALTER SYSTEM CANCEL BACKUP BACKUPPIECE
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 13;
  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
ALTER SYSTEM CANCEL ALL BACKUP FORCE
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 15;
  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_MANAGE, 2, type, value);
}
|
ALTER SYSTEM DELETE BACKUPSET INTNUM opt_copy_id opt_backup_tenant_list opt_description
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 1;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = $5->value_;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_CLEAN, 5, type, value, $8, $6, $7);
}
|
ALTER SYSTEM DELETE BACKUPPIECE INTNUM opt_copy_id opt_backup_tenant_list opt_description
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 2;

  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = $5->value_;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_CLEAN, 5, type, value, $8, $6, $7);
}
|
ALTER SYSTEM DELETE OBSOLETE BACKUP opt_backup_tenant_list opt_description
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 4;
  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_CLEAN, 4, type, value, $7, $6);
}
|
ALTER SYSTEM CANCEL DELETE BACKUP opt_backup_tenant_list opt_description
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 6;
  ParseNode *value = NULL;
  malloc_terminal_node(value, result->malloc_pool_, T_INT);
  value->value_ = 0;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_CLEAN, 4, type, value, $7, $6);
}
|
ALTER SYSTEM ADD DELETE BACKUP policy_name opt_recovery_window opt_redundancy opt_backup_copies opt_backup_tenant_list
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_DELETE_POLICY, 6, type, $10, $6, $7, $8, $9);
}
|
ALTER SYSTEM DROP DELETE BACKUP policy_name opt_backup_tenant_list
{
  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_DELETE_POLICY, 3, type, $7, $6);
}
|
ALTER SYSTEM BACKUP BACKUPSET ALL opt_tenant_info opt_backup_backup_dest
{
  ParseNode *backup_set_id = NULL;
  malloc_terminal_node(backup_set_id, result->malloc_pool_, T_INT);
  backup_set_id->value_ = 0;

  ParseNode *max_backup_times = NULL;
  malloc_terminal_node(max_backup_times, result->malloc_pool_, T_INT);
  max_backup_times->value_ = -1;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_BACKUPSET, 4, backup_set_id, $6, $7, max_backup_times);
}
|
ALTER SYSTEM BACKUP BACKUPSET opt_equal_mark INTNUM opt_tenant_info opt_backup_backup_dest
{
  (void)($5);
  ParseNode *backup_set_id= NULL;
  malloc_terminal_node(backup_set_id, result->malloc_pool_, T_INT);
  backup_set_id->value_ = $6->value_;

  ParseNode *max_backup_times = NULL;
  malloc_terminal_node(max_backup_times, result->malloc_pool_, T_INT);
  max_backup_times->value_ = -1;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_BACKUPSET, 4, backup_set_id, $7, $8, max_backup_times);
}
|
ALTER SYSTEM BACKUP BACKUPSET ALL NOT BACKED UP INTNUM TIMES opt_tenant_info opt_backup_backup_dest
{
  ParseNode *backup_set_id = NULL;
  malloc_terminal_node(backup_set_id, result->malloc_pool_, T_INT);
  backup_set_id->value_ = 0;

  ParseNode *max_backup_times = NULL;
  malloc_terminal_node(max_backup_times, result->malloc_pool_, T_INT);
  max_backup_times->value_ = $9->value_;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_BACKUPSET, 4, backup_set_id, $11, $12, max_backup_times);
}
|
ALTER SYSTEM START BACKUP ARCHIVELOG
{
  ParseNode *enable = NULL;
  malloc_terminal_node(enable, result->malloc_pool_, T_INT);
  enable->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_ARCHIVELOG, 1, enable);
}
|
ALTER SYSTEM STOP BACKUP ARCHIVELOG
{
  ParseNode *enable = NULL;
  malloc_terminal_node(enable, result->malloc_pool_, T_INT);
  enable->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_ARCHIVELOG, 1, enable);
}
|
ALTER SYSTEM BACKUP BACKUPPIECE ALL opt_with_active_piece opt_tenant_info opt_backup_backup_dest
{
  ParseNode *piece_id = NULL;
  malloc_terminal_node(piece_id, result->malloc_pool_, T_INT);
  piece_id->value_ = 0;

  ParseNode *backup_all = NULL;
  malloc_terminal_node(backup_all, result->malloc_pool_, T_INT);
  backup_all->value_ = 1;

  ParseNode *backup_times = NULL;
  malloc_terminal_node(backup_times, result->malloc_pool_, T_INT);
  backup_times->value_ = -1;

  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 0;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_BACKUPPIECE, 7, piece_id, backup_all, backup_times, $6, $7, $8, type);
}
|
ALTER SYSTEM BACKUP BACKUPPIECE opt_equal_mark INTNUM opt_with_active_piece opt_tenant_info opt_backup_backup_dest
{
  ParseNode *piece_id = NULL;
  malloc_terminal_node(piece_id, result->malloc_pool_, T_INT);
  piece_id->value_ = $6->value_;

  ParseNode *backup_all = NULL;
  malloc_terminal_node(backup_all, result->malloc_pool_, T_INT);
  backup_all->value_ = 0;

  ParseNode *backup_times = NULL;
  malloc_terminal_node(backup_times, result->malloc_pool_, T_INT);
  backup_times->value_ = -1;

  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 1;

  (void)($5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_BACKUPPIECE, 7, piece_id, backup_all, backup_times, $7, $8, $9, type);
}
|
ALTER SYSTEM BACKUP BACKUPPIECE ALL NOT BACKED UP INTNUM TIMES opt_with_active_piece opt_tenant_info opt_backup_backup_dest
{
  ParseNode *piece_id = NULL;
  malloc_terminal_node(piece_id, result->malloc_pool_, T_INT);
  piece_id->value_ = 0;

  ParseNode *backup_all = NULL;
  malloc_terminal_node(backup_all, result->malloc_pool_, T_INT);
  backup_all->value_ = 1;

  ParseNode *backup_times = NULL;
  malloc_terminal_node(backup_times, result->malloc_pool_, T_INT);
  backup_times->value_ = $9->value_;

  ParseNode *type = NULL;
  malloc_terminal_node(type, result->malloc_pool_, T_INT);
  type->value_ = 2;

  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_BACKUPPIECE, 7, piece_id, backup_all, backup_times, $11, $12, $13, type);
}
|
SET ENCRYPTION ON IDENTIFIED BY STRING_VALUE ONLY
{
  ParseNode *mode = NULL;
  result->contain_sensitive_data_ = true;
  malloc_terminal_node(mode, result->malloc_pool_, T_INT);
  mode->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_SET_ENCRYPTION, 2, mode, $6);
}
|
SET DECRYPTION IDENTIFIED BY string_list
{
  ParseNode *string_list_node = NULL;
  result->contain_sensitive_data_ = true;
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
  $$->sql_str_off_ = $1->sql_str_off_;
}
| TENANT_ID opt_equal_mark INTNUM
{
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_TENANT_ID);
  $$->value_ = $3->value_;
  $$->sql_str_off_ = $3->sql_str_off_;
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
| LIB
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = CACHE_TYPE_LIB_CACHE;
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
;

opt_tenant_list_v2:
tenant_list_tuple
{
  $$ = $1;
}
| /* empty */
{
  $$ = NULL;
}
;

tenant_list_tuple:
TENANT opt_equal_mark tenant_name_list
{
  (void)($2) ; /* make bison mute */
  merge_nodes($$, result, T_TENANT_LIST, $3);
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
;

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

opt_backup_dest:
/*empty*/
{
  $$ = NULL;
}
| FROM STRING_VALUE
{
  result->contain_sensitive_data_ = true;
  $$ = $2;
}
;

recover_table_list:
recover_table_relation_name
{
  $$ = $1;
}
| recover_table_list ',' recover_table_relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

recover_table_relation_name:
'*' '.' '*'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 3, NULL, NULL, NULL);
}
| relation_name '.' table_relation_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 3, $1, $3, NULL);
}
| relation_name '.' '*'
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RELATION_FACTOR, 3, $1, NULL, NULL);
}
| { result->is_for_remap_ = 1; } REMAP_TABLE_VAL
{
  $$ = $2;
}
;

opt_recover_tenant:
TO tenant_name
{
  $$ = $2;
}
;

opt_recover_remap_item_list:
/*empty*/
{
  $$ = NULL;
}
| restore_remap_item_list
{
  ParseNode *remap_items = NULL;
  merge_nodes(remap_items, result, T_TABLE_LIST, $1);
  $$ = remap_items;
}
;

restore_remap_item_list:
restore_remap_item
{
  $$ = $1;
}
| restore_remap_item_list restore_remap_item
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
}
;

restore_remap_item:
REMAP { result->is_for_remap_ = 1 } remap_item
{
  $$ = $3;
}
;

remap_item:
TABLE restore_remap_list
{
  ParseNode *tables = NULL;
  merge_nodes(tables, result, T_REMAP_TABLE, $2);
  $$ = tables;
}
| TABLEGROUP restore_remap_list
{
  ParseNode *tablegroups = NULL;
  merge_nodes(tablegroups, result, T_REMAP_TABLEGROUP, $2);
  $$ = tablegroups;
}
| TABLESPACE restore_remap_list
{
  ParseNode *tablespaces = NULL;
  merge_nodes(tablespaces, result, T_REMAP_TABLESPACE, $2);
  $$ = tablespaces;
}
;

restore_remap_list:
remap_relation_name
{
  $$ = $1;
}
| restore_remap_list ',' REMAP_TABLE_VAL
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

remap_relation_name:
REMAP_TABLE_VAL
{
  $$ = $1;
}
;

table_relation_name:
relation_name
{
  $$ = $1;
}
| mysql_reserved_keyword
{
  ParseNode *table_name = NULL;
  get_non_reserved_node(table_name, result->malloc_pool_, @1.first_column, @1.last_column);
  $$ = table_name;
}
;

opt_backup_backup_dest:
/*empty*/
{
  $$ = NULL;
}
| BACKUP_BACKUP_DEST opt_equal_mark STRING_VALUE
{
  (void)($2);
  result->contain_sensitive_data_ = true;
  $$ = $3;
}
;

opt_with_active_piece:
/*empty*/
{
  $$ = NULL;
}
| WITH ACTIVE
{
  malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
  $$->value_ = 1;
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

opt_copy_id:
COPY INTNUM
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_COPY_ID, 1, $2);
}
| /* EMPTY */
{
  $$ = NULL;
}
;

policy_name:
POLICY opt_equal_mark STRING_VALUE
{
  (void)($2);
  $$ = $3;
}
;

opt_recovery_window:
RECOVERY_WINDOW opt_equal_mark STRING_VALUE
{
  (void)($2);
  $$ = $3;
}
| /* EMPTY */
{
  $$ = NULL;
}
;

opt_redundancy:
REDUNDANCY opt_equal_mark INTNUM
{
  (void)($2);
  $$ = $3;
}
| /* EMPTY */
{
  $$ = NULL;
}
;

opt_backup_copies:
BACKUP_COPIES opt_equal_mark INTNUM
{
  (void)($2);
  $$ = $3;
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

ls:
LS opt_equal_mark INTNUM
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_LS, 1, $3);
}
;

opt_tenant_list_or_ls_or_tablet_id:
tenant_list_tuple opt_tablet_id
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TENANT_TABLET, 2, $1, $2);
}
| tenant_list_tuple ls opt_tablet_id
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_TENANT_LS_TABLET, 3, $1, $2, $3);
}
| /*EMPTY*/
{
  $$ = NULL;
}
;


ls_server_or_server_or_zone_or_tenant:
ls ip_port tenant_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LS_SERVER_TENANT, 3, $1, $2, $3);
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

namespace_expr:
NAMESPACE opt_equal_mark STRING_VALUE
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_NAMESPACE, 1, $3);
}
;

opt_namespace:
namespace_expr
{
  $$ = $1;
}
| /*EMPTY*/
{
  $$ = NULL;
}
;

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

opt_tenant_info:
TENANT_ID opt_equal_mark INTNUM
{
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_TENANT_ID);
  $$->value_ = $3->value_;
  $$->sql_str_off_ = $3->sql_str_off_;
}
|
TENANT opt_equal_mark relation_name_or_string
{
  (void)($2);
  malloc_terminal_node($$, result->malloc_pool_, T_TENANT_NAME);
  $$->str_value_ = $3->str_value_;
  $$->str_len_ = $3->str_len_;
  $$->sql_str_off_ = $3->sql_str_off_;
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
|
BACKUP_BACKUP_DEST COMP_EQ STRING_VALUE opt_comment opt_config_scope
opt_server_or_zone opt_tenant_name
{
  ParseNode *backup_backup_dest = NULL;
  make_name_node(backup_backup_dest, result->malloc_pool_, "backup_backup_dest");
  malloc_non_terminal_node($$, result->malloc_pool_, T_SYSTEM_ACTION, 5,
                           backup_backup_dest,    /* param_name */
                           $3,    /* param_value */
                           $4,    /* comment */
                           $6,    /* zone or server */
                           $7     /* tenant */
                           );
  $$->value_ = $5[0];                /* scope */
}
|
OBCONFIG_URL COMP_EQ STRING_VALUE opt_comment opt_config_scope
opt_server_or_zone opt_tenant_name
{
  ParseNode *obconfig_url = NULL;
  make_name_node(obconfig_url, result->malloc_pool_, "obconfig_url");
  malloc_non_terminal_node($$, result->malloc_pool_, T_SYSTEM_ACTION, 5,
                           obconfig_url,    /* param_name */
                           $3,    /* param_value */
                           $4,    /* comment */
                           $6,    /* zone or server */
                           $7     /* tenant */
                           );
  $$->value_ = $5[0];                /* scope */
}
|
LOG_DISK_SIZE COMP_EQ STRING_VALUE opt_comment opt_config_scope
opt_server_or_zone opt_tenant_name
{
  ParseNode *log_disk_size= NULL;
  make_name_node(log_disk_size, result->malloc_pool_, "log_disk_size");
  malloc_non_terminal_node($$, result->malloc_pool_, T_SYSTEM_ACTION, 5,
                           log_disk_size,    /* param_name */
                           $3,    /* param_value */
                           $4,    /* comment */
                           $6,    /* zone or server */
                           $7     /* tenant */
                           );
  $$->value_ = $5[0];                /* scope */
}
|
LOG_RESTORE_SOURCE COMP_EQ STRING_VALUE opt_comment opt_config_scope
opt_server_or_zone opt_tenant_name
{
  ParseNode *log_restore_source= NULL;
  make_name_node(log_restore_source, result->malloc_pool_, "log_restore_source");
  malloc_non_terminal_node($$, result->malloc_pool_, T_SYSTEM_ACTION, 5,
                           log_restore_source,    /* param_name */
                           $3,    /* param_value */
                           $4,    /* comment */
                           $6,    /* zone or server */
                           $7     /* tenant */
                           );
  $$->value_ = $5[0];                /* scope */
};

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
| MATCH opt_equal_mark INTNUM
{
  (void)($2) ; /* make bison mute */
  malloc_non_terminal_node($$, result->malloc_pool_, T_TP_COND, 1, $3);
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

ls_role:
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
| DEFAULT
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 2;
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
- extension : can be either a column group in the format of (column_name, column_name [, ...]) or an expression
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

opt_backup_to:
/*EMPTY*/  { $$ = NULL; }
| TO opt_equal_mark STRING_VALUE
{
  (void)($2);
  result->contain_sensitive_data_ = true;
  $$ = $3;
}
;

opt_backup_tenant_list:
/*EMPTY*/  { $$ = NULL; }
| TENANT opt_equal_mark tenant_name_list
{
	(void)($2) ; /* make bison mute */
  merge_nodes($$, result, T_TENANT_LIST, $3);
}
;

opt_description:
/*EMPTY*/  { $$ = NULL; }
| DESCRIPTION opt_equal_mark STRING_VALUE
{
  (void)($2);
  $$ = $3;
}
;

opt_restore_until:
/*EMPTY*/
{ $$ = NULL; }
| UNTIL TIME COMP_EQ STRING_VALUE
{
  ParseNode *is_scn = NULL;
  malloc_terminal_node(is_scn, result->malloc_pool_, T_INT);
  is_scn->value_ = 0;
  malloc_non_terminal_node($$, result->malloc_pool_, T_PHYSICAL_RESTORE_UNTIL, 2, is_scn, $4);
}
| UNTIL SCN COMP_EQ INTNUM
{
  ParseNode *is_scn = NULL;
  malloc_terminal_node(is_scn, result->malloc_pool_, T_INT);
  is_scn->value_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_PHYSICAL_RESTORE_UNTIL, 2, is_scn, $4);
}
;

opt_backup_key_info:
/*EMPTY*/  { $$ = NULL; }
| WITH KEY FROM STRING_VALUE opt_encrypt_key
{
  result->contain_sensitive_data_ = true;
  malloc_non_terminal_node($$, result->malloc_pool_, T_BACKUP_KEY, 2, $4, $5);
}
;

opt_encrypt_key:
/*EMPTY*/  { $$ = NULL; }
| ENCRYPTED BY STRING_VALUE
{
  result->contain_sensitive_data_ = true;
  $$ = $3;
}
;

/*===========================================================
 * savepoint
 *===========================================================*/
create_savepoint_stmt:
SAVEPOINT var_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_CREATE_SAVEPOINT);
  $$->str_value_ = $2->str_value_;
  $$->str_len_ = $2->str_len_;
  $$->sql_str_off_ = $2->sql_str_off_;
}
;
rollback_savepoint_stmt:
ROLLBACK TO var_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_ROLLBACK_SAVEPOINT);
  $$->str_value_ = $3->str_value_;
  $$->str_len_ = $3->str_len_;
  $$->sql_str_off_ = $3->sql_str_off_;
}
| ROLLBACK WORK TO var_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_ROLLBACK_SAVEPOINT);
  $$->str_value_ = $4->str_value_;
  $$->str_len_ = $4->str_len_;
  $$->sql_str_off_ = $4->sql_str_off_;
}
| ROLLBACK TO SAVEPOINT var_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_ROLLBACK_SAVEPOINT);
  $$->str_value_ = $4->str_value_;
  $$->str_len_ = $4->str_len_;
  $$->sql_str_off_ = $4->sql_str_off_;
}
;
release_savepoint_stmt:
RELEASE SAVEPOINT var_name
{
  malloc_terminal_node($$, result->malloc_pool_, T_RELEASE_SAVEPOINT);
  $$->str_value_ = $3->str_value_;
  $$->str_len_ = $3->str_len_;
  $$->sql_str_off_ = $3->sql_str_off_;
}
;

/*===========================================================
 *
 * 租户级主备库运维命令
 *
 *===========================================================*/

switchover_tenant_stmt:
ALTER SYSTEM switchover_clause opt_verify
{
  (void)($2);
  malloc_non_terminal_node($$, result->malloc_pool_, T_SWITCHOVER, 2, $3, $4);
}
;

switchover_clause:
ACTIVATE STANDBY opt_tenant_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_FAILOVER_TO_PRIMARY, 1, $3);
}
| SWITCHOVER TO PRIMARY opt_tenant_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SWITCHOVER_TO_PRIMARY, 1, $4);
}
| SWITCHOVER TO STANDBY opt_tenant_name
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_SWITCHOVER_TO_STANDBY, 1, $4);
}
;

opt_verify:
VERIFY
{
  malloc_terminal_node($$, result->malloc_pool_, T_VERIFY); }
| /* EMPTY */
{ $$ = NULL; }
;

recover_tenant_stmt:
ALTER SYSTEM RECOVER STANDBY opt_tenant_name recover_point_clause
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_RECOVER, 2, $5, $6);
}
;

recover_point_clause:
opt_restore_until
{
  $$ = $1;
}
| UNTIL UNLIMITED
{
  malloc_terminal_node($$, result->malloc_pool_, T_RECOVER_UNLIMITED);
}
| CANCEL
{
  malloc_terminal_node($$, result->malloc_pool_, T_RECOVER_CANCEL);
};

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
  | new_or_old_column_ref
    {
      $$ = $1;
    }
;

new_or_old:
  NEW
  {
    get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
  }
| OLD
  {
    get_non_reserved_node($$, result->malloc_pool_, @1.first_column, @1.last_column);
  }

new_or_old_column_ref:
  new_or_old '.' column_name
{
  if (!result->is_for_trigger_) {
    yyerror(&@2, result, "");
    YYERROR;
  }
  malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_REF, 3, NULL, $1, $3);
  dup_node_string($3, $$, result->malloc_pool_);
#ifndef SQL_PARSER_COMPILATION
  lookup_pl_exec_symbol($$, result, @1.first_column, @3.last_column, true, false, false);
#endif
}

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
| SCHEMA
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
| NORMAL
{
  make_name_node($$, result->malloc_pool_, "normal");
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

/*===========================================================
*
*  JSON TABLE
*
*============================================================*/

json_table_expr:
JSON_TABLE '(' simple_expr ',' literal mock_jt_on_error_on_empty COLUMNS '(' jt_column_list ')' ')' relation_name
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $9);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JSON_TABLE_EXPRESSION, 5, $3, $5, $6, params, $12);
  if (result->pl_parse_info_.is_pl_parse_ && !result->pl_parse_info_.is_pl_parse_expr_) {
    result->pl_parse_info_.is_forbid_pl_fp_ = true;
  }
}
| JSON_TABLE '(' simple_expr ',' literal mock_jt_on_error_on_empty COLUMNS '(' jt_column_list ')' ')' AS relation_name
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $9);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JSON_TABLE_EXPRESSION, 5, $3, $5, $6, params, $13);
  if (result->pl_parse_info_.is_pl_parse_ && !result->pl_parse_info_.is_pl_parse_expr_) {
    result->pl_parse_info_.is_forbid_pl_fp_ = true;
  }
}
| JSON_TABLE '(' simple_expr ',' literal mock_jt_on_error_on_empty COLUMNS '(' jt_column_list ')' ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $9);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JSON_TABLE_EXPRESSION, 5, $3, $5, $6, params, NULL);
  if (result->pl_parse_info_.is_pl_parse_ && !result->pl_parse_info_.is_pl_parse_expr_) {
    result->pl_parse_info_.is_forbid_pl_fp_ = true;
  }
}
;

mock_jt_on_error_on_empty:
{
  ParseNode *emp_node = NULL;
  malloc_terminal_node(emp_node, result->malloc_pool_, T_INT);
  emp_node->value_ = 3;
  emp_node->is_hidden_const_ = 1;
  ParseNode *err_node = NULL;
  malloc_terminal_node(err_node, result->malloc_pool_, T_INT);
  err_node->value_ = 3;
  err_node->is_hidden_const_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, err_node, emp_node);
}
;

jt_column_list:
json_table_column_def
{
  $$ = $1;
}
| jt_column_list ',' json_table_column_def
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
}
;

json_table_column_def:
  json_table_ordinality_column_def
  | json_table_exists_column_def
  | json_table_value_column_def
  | json_table_nested_column_def
  ;

json_table_ordinality_column_def:
column_name FOR ORDINALITY
{
  ParseNode* node = NULL;
  malloc_terminal_node(node, result->malloc_pool_, T_INT);
  node->is_hidden_const_ = 1;

  malloc_non_terminal_node($$, result->malloc_pool_, T_JSON_TABLE_COLUMN, 2, $1, node);
  $$->value_ = 1;
}
;

json_table_exists_column_def:
column_name data_type opt_collation EXISTS PATH literal mock_jt_on_error_on_empty
{
  ParseNode *truncate_node = NULL;
  malloc_terminal_node(truncate_node, result->malloc_pool_, T_INT);
  truncate_node->value_ = 0;
  truncate_node->is_hidden_const_ = 1;
  set_data_type_collation($2, $3, true, false);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JSON_TABLE_COLUMN, 5, $1, $2, truncate_node, $6, $7);
  $$->value_ = 2;
}
;

json_table_value_column_def:
column_name data_type opt_collation PATH literal opt_value_on_empty_or_error_or_mismatch
{
  ParseNode *truncate_node = NULL;
  malloc_terminal_node(truncate_node, result->malloc_pool_, T_INT);
  truncate_node->value_ = 0;
  truncate_node->is_hidden_const_ = 1;
  set_data_type_collation($2, $3, true, false);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JSON_TABLE_COLUMN, 5, $1, $2, truncate_node, $5, $6);
  $$->value_ = 4;
}
;

json_table_nested_column_def:
NESTED literal COLUMNS '(' jt_column_list ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $5);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JSON_TABLE_COLUMN, 2, $2, params);
  $$->value_ = 5;
}
| NESTED PATH literal COLUMNS '(' jt_column_list ')'
{
  ParseNode *params = NULL;
  merge_nodes(params, result, T_EXPR_LIST, $6);
  malloc_non_terminal_node($$, result->malloc_pool_, T_JSON_TABLE_COLUMN, 2, $3, params);
  $$->value_ = 5;
}
;

opt_value_on_empty_or_error_or_mismatch:
opt_on_empty_or_error opt_on_mismatch
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 5, $1->children_[0], $1->children_[1], $1->children_[2], $1->children_[3], $2);
}
;

opt_on_mismatch:
{
  ParseNode *on_mismatch = NULL;
  malloc_terminal_node(on_mismatch, result->malloc_pool_, T_INT);
  on_mismatch->value_ = 3;
  on_mismatch->is_hidden_const_ = 1;

  ParseNode *mismatch_type = NULL;
  malloc_terminal_node(mismatch_type, result->malloc_pool_, T_INT);
  mismatch_type->value_ = 3;
  mismatch_type->is_hidden_const_ = 1;
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, on_mismatch, mismatch_type);
}
;

/*===========================================================
 *
 *	json value
 *
 *===========================================================*/

json_value_expr:
JSON_VALUE '(' simple_expr ',' complex_string_literal opt_returning_type opt_truncate_clause opt_ascii opt_on_empty_or_error ')'
{
  ParseNode *empty_value = $9->children_[1];
  ParseNode *error_value = $9->children_[3];

  ParseNode *on_mismatch = NULL;
  malloc_terminal_node(on_mismatch, result->malloc_pool_, T_INT);
  on_mismatch->value_ = 3;
  on_mismatch->is_hidden_const_ = 1;

  ParseNode *mismatch_type = NULL;
  malloc_terminal_node(mismatch_type, result->malloc_pool_, T_INT);
  mismatch_type->value_ = 3;
  mismatch_type->is_hidden_const_ = 1;
  ParseNode *mismatch_options = NULL;
  malloc_non_terminal_node(mismatch_options, result->malloc_pool_, T_LINK_NODE, 2, on_mismatch, mismatch_type);

  malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS_JSON_VALUE, 10, $3, $5, $6, $7, $8, $9->children_[0], empty_value, $9->children_[2], error_value, mismatch_options);
}
;

opt_truncate_clause:
TRUNCATE
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
  $$->is_hidden_const_ = 1;
}
|
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
  $$->is_hidden_const_ = 1;
}
;

opt_ascii:
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 0;
  $$->is_hidden_const_ = 1;
}
| ASCII
{
  malloc_terminal_node($$, result->malloc_pool_, T_INT);
  $$->value_ = 1;
  $$->is_hidden_const_ = 1;
}
;

opt_returning_type:
RETURNING cast_data_type
{
  $$ = $2;
}
| // The default returning type is CHAR(512).
{
  malloc_terminal_node($$, result->malloc_pool_, T_CAST_ARGUMENT);
  $$->value_ = 0;
  $$->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_CHAR; // to keep consitent with mysql
  $$->int16_values_[OB_NODE_CAST_COLL_IDX] = INVALID_COLLATION;        /* is char */
  $$->int32_values_[OB_NODE_CAST_C_LEN_IDX] = 512;        /* length */
  $$->param_num_ = 1; /* 1 */
  $$->is_hidden_const_ = 1;
}
;

opt_on_empty_or_error:
/* empty */
{
  ParseNode *empty_type = NULL;
  malloc_terminal_node(empty_type, result->malloc_pool_, T_INT);
  empty_type->value_ = 3;
  empty_type->is_hidden_const_ = 1;

  ParseNode *empty_node = NULL;
  malloc_terminal_node(empty_node, result->malloc_pool_, T_NULL);
  empty_node->is_hidden_const_ = 1;

  ParseNode *error_type = NULL;
  malloc_terminal_node(error_type, result->malloc_pool_, T_INT);
  error_type->value_ = 3;
  error_type->is_hidden_const_ = 1;

  ParseNode *error_node = NULL;
  malloc_terminal_node(error_node, result->malloc_pool_, T_NULL);
  error_node->is_hidden_const_ = 1;

  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 4, empty_type, empty_node, error_type, error_node);
}
| on_empty
{
  ParseNode *error_type = NULL;
  malloc_terminal_node(error_type, result->malloc_pool_, T_INT);
  error_type->value_ = 3;
  error_type->is_hidden_const_ = 1;

  ParseNode *error_node = NULL;
  malloc_terminal_node(error_node, result->malloc_pool_, T_NULL);
  error_node->is_hidden_const_ = 1;

  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 4, $1->children_[0], $1->children_[1], error_type, error_node);
}
| on_error
{
  ParseNode *empty_type = NULL;
  malloc_terminal_node(empty_type, result->malloc_pool_, T_INT);
  empty_type->value_ = 3;
  empty_type->is_hidden_const_ = 1;

  ParseNode *empty_node = NULL;
  malloc_terminal_node(empty_node, result->malloc_pool_, T_NULL);
  empty_node->is_hidden_const_ = 1;

  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 4, empty_type, empty_node, $1->children_[0], $1->children_[1]);
}
| on_empty on_error
{
  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 4, $1->children_[0], $1->children_[1], $2->children_[0], $2->children_[1]);
}

on_empty:
json_on_response ON EMPTY
{
  $$ = $1;
}
;

on_error:
json_on_response ON ERROR_P
{
  $$ = $1;
}
;

// type : { error : 0, null : 1, default : 2, implict : 3 }
json_on_response:
ERROR_P
{
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_INT);
  type_node->value_ = 0;
  type_node->is_hidden_const_ = 1;

  ParseNode *v_node = NULL;
  malloc_terminal_node(v_node, result->malloc_pool_, T_NULL);
  v_node->is_hidden_const_ = 1;

  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, type_node, v_node);
}
| NULLX
{
  (void)($1) ; /* make bison mute */
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_NULLX_CLAUSE);
  type_node->value_ = 1;
  type_node->param_num_ = 1;
  type_node->sql_str_off_ = $1->sql_str_off_;

  ParseNode *v_node = NULL;
  malloc_terminal_node(v_node, result->malloc_pool_, T_NULL);
  v_node->is_hidden_const_ = 1;
  v_node->sql_str_off_ = $1->sql_str_off_;

  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, type_node, v_node);
}
| DEFAULT signed_literal
{
  ParseNode *type_node = NULL;
  malloc_terminal_node(type_node, result->malloc_pool_, T_INT);
  type_node->value_ = 2;
  type_node->is_hidden_const_ = 1;

  malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, type_node, $2);
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
|       ALL_META
|       ALL_USER
|       ALWAYS
|       ANALYSE
|       ANY
|       APPROX_COUNT_DISTINCT
|       APPROX_COUNT_DISTINCT_SYNOPSIS
|       APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE
|       ARCHIVELOG
|       ARBITRATION
|       ASCII
|       AT
|       AUDIT
|       AUTHORS
|       AUTO
|       AUTOEXTEND_SIZE
|       AUTO_INCREMENT
|       AUTO_INCREMENT_MODE
|       AVG
|       AVG_ROW_LENGTH
|       BACKUP
|       BACKUPSET
|       BACKUP_COPIES
|       BADFILE
|       BASE
|       BASELINE
|       BASELINE_ID
|       BASIC
|       BALANCE
|       BANDWIDTH
|       BEGI
|       BINDING
|       BINLOG
|       BIT
|       BIT_AND
|       BIT_OR
|       BIT_XOR
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
|       CALIBRATION
|       CALIBRATION_INFO
|       KVCACHE
|       ILOGCACHE
|       CALC_PARTITION_ID
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
|       COMPUTE
|       CONCURRENT
|       CONDENSED
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
|       CTXCAT
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
|       DESCRIPTION
|       DESTINATION
|       DIAGNOSTICS
|       DIRECTORY
|       DISABLE
|       DISCARD
|       DISK
|       DISKGROUP
|       DISCONNECT
|       DO
|       DUMP
|       DUMPFILE
|       DUPLICATE
|       DUPLICATE_SCOPE
|       DYNAMIC
|       DEFAULT_TABLEGROUP
|       EFFECTIVE
|       EMPTY
|       EMPTY_FIELD_AS_NULL
|       ENABLE
|       ENABLE_ARBITRATION_SERVICE
|       ENABLE_EXTENDED_ROWID
|       ENCODING
|       ENCRYPTED
|       ENCRYPTION
|       END
|       ENDS
|       ENFORCED
|       ENGINE_
|       ENGINES
|       ENUM
|       ENTITY
|       ERROR_CODE
|       ERROR_P
|       ERRORS
|       ESCAPE
|       ESTIMATE
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
|       EXTERNAL
|       FAILOVER
|       EXTRACT
|       FAST
|       FAULTS
|       FLASHBACK
|       FIELDS
|       FIELD_DELIMITER
|       FIELD_OPTIONALLY_ENCLOSED_BY
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
|       FRAGMENTATION
|       FREEZE
|       FREQUENCY
|       FUNCTION
|       FULL %prec HIGHER_PARENS
|       GENERAL
|       GEOMETRY
|       GEOMCOLLECTION
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
|       HYBRID_HIST
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
|       INCREMENT
|       INCREMENTAL
|       IO
|       IOPS_WEIGHT
|       IO_THREAD
|       IPC
|       ISNULL
|       ISOLATION
|       ISOLATE
|       ISSUER
|       JOB
|       JSON
|       JSON_VALUE
|       JSON_ARRAYAGG
|       JSON_OBJECTAGG
|       JSON_TABLE
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
|       LINE_DELIMITER
|       LINESTRING
|       LIST_
|       LISTAGG
|       LN
|       LOCAL
|       LOCALITY
|       LOCKED
|       LOCKS
|       LOG
|       LOGFILE
|       LOGONLY_REPLICA_NUM
|       LOGS
|       LOG_RESTORE_SOURCE
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
|       LOG_DISK_SIZE
|       MAX_IOPS
|       MEMORY_SIZE
|       MAX_QUERIES_PER_HOUR
|       MAX_ROWS
|       MAX_SIZE
|       MAX_UPDATES_PER_HOUR
|       MAX_USER_CONNECTIONS
|       MEDIUM
|       MEMBER
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
|       MINVALUE
|       MIN_CPU
|       MIN_IOPS
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
|       NESTED
|       NEW
|       NEXT
|       NO
|       NOARCHIVELOG
|       NOAUDIT
|       NOCACHE
|       NOCYCLE
|       NODEGROUP
|       NOMINVALUE
|       NOMAXVALUE
|       NONE
|       NOORDER
|       NOPARALLEL
|       NORMAL
|       NOW
|       NOWAIT
|       NO_WAIT
|       NTILE
|       NTH_VALUE
|       NUMBER
|       NULL_IF_EXETERNAL
|       NULLS
|       NVARCHAR
|       OCCUR
|       OF
|       OFF
|       OFFSET
|       OLD
|       OLD_PASSWORD
|       OLD_KEY
|       OJ
|       OVER
|       OBCONFIG_URL
|       ONE
|       ONE_SHOT
|       ONLY
|       OPEN
|       OPTIONS
|       ORDINALITY
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
|       PATH
|       LS
|       PARTITIONING
|       PARTITIONS
|       PATTERN
|       PERCENT_RANK
|       PAUSE
|       PERCENTAGE
|       PHASE
|       PHYSICAL
|       PL
|       PLANREGRESS
|       PLUGIN
|       PLUGIN_DIR
|       PLUGINS
|       PLUS
|       POINT
|       POLICY
|       POLYGON
|       POOL
|       PORT
|       POSITION
|       PRECEDING
|       PREPARE
|       PRESERVE
|       PRETTY
|       PRETTY_COLOR
|       PREV
|       PRIMARY_ZONE
|       PRIVILEGES
|       PROCESS
|       PROCESSLIST
|       PROFILE
|       PROFILES
|       PROGRESSIVE_MERGE_NUM
|       PROXY
|       PS
|       PUBLIC
|       PCTFREE
|       P_ENTITY
|       P_CHUNK
|       QUARTER
|       QUERY %prec KILL_EXPR
|       QUERY_RESPONSE_TIME
|       QUEUE_TIME
|       QUICK
|       RANK
|       READ_ONLY
|       REBUILD
|       RECOVER
|       RECOVERY
|       RECOVERY_WINDOW
|       RECURSIVE
|       RECYCLE
|       RECYCLEBIN
|       ROTATE
|       ROW_NUMBER
|       REDO_BUFFER_SIZE
|       REDOFILE
|       REDUNDANCY
|       REDUNDANT
|       REFRESH
|       REGION
|       REJECT
|       RELAY
|       RELAYLOG
|       RELAY_LOG_FILE
|       RELAY_LOG_POS
|       RELAY_THREAD
|       RELOAD
|       REMAP
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
|       RETURNING
|       RETURNS
|       REVERSE
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
|       SCN
|       SCOPE
|       SECOND
|       SECURITY
|       SEED
|       SEQUENCE
|       SEQUENCES
|       SERIAL
|       SERIALIZABLE
|       SERVER
|       SERVER_IP
|       SERVER_PORT
|       SERVER_TYPE
|       SERVICE
|       SESSION %prec LOWER_PARENS
|       SESSION_USER
|       SET_MASTER_CLUSTER
|       SET_SLAVE_CLUSTER
|       SET_TP
|       SHARDING
|       SHARE
|       SHUTDOWN
|       SIGNED
|       SIZE %prec LOWER_PARENS
|       SIMPLE
|       SKIP_BLANK_LINES
|       SKIP_HEADER
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
|       SRID
|       STACKED
|       STANDBY
|       START
|       STARTS
|       STAT
|       STATISTICS
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
|       TABLE_CHECKSUM
|       TABLE_MODE
|       TABLEGROUPS
|       TABLE_ID
|       TABLE_NAME
|       TABLES
|       TABLESPACE
|       TABLET
|       TABLET_ID
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
|       TRIM_SPACE
|       TRUNCATE
|       TTL
|       TYPE
|       TYPES
|       TABLEGROUP_ID
|       TOP_K_FRE_HIST
|       UNCOMMITTED
|       UNDEFINED
|       UNDO_BUFFER_SIZE
|       UNDOFILE
|       UNICODE
|       UNKNOWN
|       UNINSTALL
|       UNIT
|       UNIT_GROUP
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
|       UNLIMITED
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
|       WASH
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
|       HIDDEN
|       INDEXED
|       SKEWONLY
|       BACKUPPIECE
|       PREVIEW
|       BACKUP_BACKUP_DEST
|       BACKUPROUND
|       UP
|       TIMES
|       BACKED
|       NAMESPACE
|       LIB
|       LINK %prec LOWER_PARENS
|       MY_NAME
|       CONNECT
|       STATEMENT_ID
|       KV_ATTRIBUTES
;

unreserved_keyword_special:
PASSWORD
;
unreserved_keyword_extra:
ACCESS
;

/*注释掉的关键字有规约冲突暂时注释了,都是一些sql中常用的关键字,后面按需打开,增加这块代码逻辑是为了支持在mysql中允许以
  表名+列名的方式使用关键字，比如"select key.key from test.key"(
*/
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
| FULLTEXT
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
| NUMERIC
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
    char *escaped_s = NULL;
    ESCAPE_PERCENT(p, s, escaped_s);
    if (OB_NOT_NULL(escaped_s)) {
      vsnprintf(p->error_msg_, MAX_ERROR_MSG, escaped_s, ap);
    } else {
      vsnprintf(p->error_msg_, MAX_ERROR_MSG, s, ap);
    }
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

/* 用于将一条多语句SQL按照分号切分成多个独立SQL */
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
          break;
        default:
          break;
      }
    } /* end while */
    p->end_col_ = yylloc.last_column;
  }
  return ret;
}

enum state_multi_values {
  STMT_START_STATE = 0,
  STMT_INS_OR_REPLACE_STATE = 1,
  STMT_HINT_START_STATE = 2,
  STMT_HINT_END_STATE = 3,
  STMT_INTO_STATE = 4,
  STMT_VALUES_STATE = 5,
  STMT_LEFT_PAR_STATE = 6,
  STMT_RIGHT_PAR_STATE = 7,
  STMT_ON_STATE = 8,
  STMT_DUPLICATE_STATE = 9,
  STMT_KEY_STATE = 10,
  STMT_UPDATE_STATE = 11,
  STMT_END_SUCCESS = 12,
  ERROR_STATE = 13
};

 // Used to split an insert SQL with multiple values
 // example: insert /*+parllen(5)*/ into test.t1(c1,c2,c3) values(1,1,1),((1),(2),3);
 // insert /*+parllen(5)*/ into test.t1(c1,c2,c3) values(1,1,1);
 // insert /*+parllen(5)*/ into test.t1(c1,c2,c3) values((1),(2),3);
int obsql_mysql_multi_values_parse(ParseResult *p)
{
  int SUCCESS = 0;
  int UNEXPECTED_RRROR = -1;
  int ret = SUCCESS;
  int64_t pair_count = 0;
  bool is_end = false;
  int state = STMT_START_STATE;
  if (OB_UNLIKELY(NULL == p)) {
    ret = UNEXPECTED_RRROR;
  } else {
    YYSTYPE yylval;
    YYLTYPE yylloc;
    int token = YYEMPTY;
    int left_parentheses = 0;
    int right_parentheses = 0;
    int on_start_pos = 0;
    p->ins_multi_value_res_->on_duplicate_pos_  = -1;
    p->ins_multi_value_res_->values_col_ = -1;
    while (SUCCESS == ret && !is_end) {
      token = obsql_mysql_yylex(&yylval, &yylloc, p->yyscan_info_);
      if (token == PARSER_SYNTAX_ERROR || token == ERROR) {
        state = ERROR_STATE;
        is_end = true;
      } else if (token == END_P || token == DELIMITER) {
        is_end = true;
      }
      switch (state) {
        case STMT_START_STATE:
          if (token == INSERT || token == REPLACE) {
            state = STMT_INS_OR_REPLACE_STATE;
          } else if (token == INSERT_HINT_BEGIN || token == REPLACE_HINT_BEGIN) {
            state = STMT_HINT_START_STATE;
          }
          break;
        case STMT_INS_OR_REPLACE_STATE:
          if (token == INTO) {
            state = STMT_INTO_STATE;
          } else if (token == VALUES || token == VALUE) {
            p->ins_multi_value_res_->values_col_ = yylloc.last_column;
            state = STMT_VALUES_STATE;
          }
          break;
        case STMT_HINT_START_STATE:
          if (token == HINT_END) {
            state = STMT_HINT_END_STATE;
          }
          break;
        case STMT_HINT_END_STATE:
          if (token == INTO) {
            state = STMT_INTO_STATE;
          } else if (token == VALUES || token == VALUE) {
            state = STMT_VALUES_STATE;
          }
          break;
        case STMT_INTO_STATE:
          if (token == VALUES || token == VALUE) {
            state = STMT_VALUES_STATE;
            p->ins_multi_value_res_->values_col_ = yylloc.last_column;
          }
          break;
        case STMT_VALUES_STATE:
          if (token == '(') {
            state = STMT_LEFT_PAR_STATE;
            // Record the starting position of the left bracket
            pair_count++;
            left_parentheses = yylloc.last_column;
          } else {
            // If the first token following 'values' is not '(', report an error directly
            ret = UNEXPECTED_RRROR;
          }
          break;
        case STMT_LEFT_PAR_STATE:
          if (token == ')') {
            pair_count--;
            if (pair_count == 0) {
              // After the ')' are matched, record the position of the left and right brackets at the moment
              if (left_parentheses > 0) {
                right_parentheses = yylloc.last_column;
                on_start_pos = yylloc.last_column;
                ret = store_prentthese_info(left_parentheses, right_parentheses, p);
                left_parentheses = 0;
                right_parentheses = 0;
                state = STMT_RIGHT_PAR_STATE;
              } else {
                // only ')', without the position of '('
                ret = UNEXPECTED_RRROR;
              }
            }
          } else if (token == '(') {
            pair_count++;
          } else {
            // Other tokens will not be processed
          }
          break;
        case STMT_RIGHT_PAR_STATE:
          // Ended the right bracket matching, if you encounter ',', it means that the next '(' is about to start
          // If you encounter ON, it means that this is an inset ... on duplicate key statement
          // If the terminator is encountered, it means the end of the current SQL
          if (token == ',') {
            state = STMT_VALUES_STATE;
          } else if (token == ON) {
            // Is insert on duplicate key update...
            // Record the start position of on duplicate key
            p->ins_multi_value_res_->on_duplicate_pos_ = on_start_pos;
            state = STMT_ON_STATE;
          } else if (token == END_P || token == DELIMITER) {
            // When the termination symbol appears, it jumps out of the loop
            state = STMT_END_SUCCESS;
          } else {
            // Other symbol appear, it jumps out of the loop
            ret = UNEXPECTED_RRROR;
          }
          break;
        case STMT_ON_STATE:
          if (token == DUPLICATE) {
            state = STMT_DUPLICATE_STATE;
          } else {
            ret = UNEXPECTED_RRROR;
          }
          break;
        case STMT_DUPLICATE_STATE:
          if (token == KEY) {
            state = STMT_KEY_STATE;
          } else {
            ret = UNEXPECTED_RRROR;
          }
          break;
        case STMT_KEY_STATE:
          if (token == UPDATE) {
            state = STMT_UPDATE_STATE;
          } else {
            ret = UNEXPECTED_RRROR;
          }
          break;
        case STMT_UPDATE_STATE:
          if (token == END_P || token == DELIMITER) {
            // When the termination symbol appears, it jumps out of the loop
            state = STMT_END_SUCCESS;
          }
          break;
        case ERROR_STATE:
        default:
          state = ERROR_STATE;
          break;
      }
    } /* end while */
    if (STMT_END_SUCCESS != state) {
      ret = UNEXPECTED_RRROR;
    }
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
          //将匹配的内容直接拷贝
          if (p->is_ignore_token_) {
            if (SELECT_HINT_BEGIN == token ||
                UPDATE_HINT_BEGIN == token ||
                DELETE_HINT_BEGIN == token ||
                INSERT_HINT_BEGIN == token ||
                REPLACE_HINT_BEGIN == token ||
                LOAD_DATA_HINT_BEGIN == token ||
                CREATE_HINT_BEGIN == token
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
