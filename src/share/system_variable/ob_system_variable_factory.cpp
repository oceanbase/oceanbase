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

#define USING_LOG_PREFIX SQL_SESSION
#include "share/ob_define.h"
#include "share/system_variable/ob_system_variable_factory.h"
#include "share/ob_errno.h"
#include <algorithm>
using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{
const char *ObSysVarBinlogRowImage::BINLOG_ROW_IMAGE_NAMES[] = {
  "MINIMAL",
  "NOBLOB",
  "FULL",
  0
};
const char *ObSysVarQueryCacheType::QUERY_CACHE_TYPE_NAMES[] = {
  "OFF",
  "ON",
  "DEMAND",
  0
};
const char *ObSysVarBinlogFormat::BINLOG_FORMAT_NAMES[] = {
  "MIXED",
  "STATEMENT",
  "ROW",
  0
};
const char *ObSysVarObReadConsistency::OB_READ_CONSISTENCY_NAMES[] = {
  "",
  "FROZEN",
  "WEAK",
  "STRONG",
  0
};
const char *ObSysVarObCompatibilityMode::OB_COMPATIBILITY_MODE_NAMES[] = {
  "MYSQL",
  "ORACLE",
  0
};
const char *ObSysVarObRoutePolicy::OB_ROUTE_POLICY_NAMES[] = {
  "",
  "READONLY_ZONE_FIRST",
  "ONLY_READONLY_ZONE",
  "UNMERGE_ZONE_FIRST",
  "UNMERGE_FOLLOWER_FIRST",
  0
};
const char *ObSysVarObEnableJit::OB_ENABLE_JIT_NAMES[] = {
  "OFF",
  "AUTO",
  "FORCE",
  0
};
const char *ObSysVarBlockEncryptionMode::BLOCK_ENCRYPTION_MODE_NAMES[] = {
  "aes-128-ecb",
  "aes-192-ecb",
  "aes-256-ecb",
  "aes-128-cbc",
  "aes-192-cbc",
  "aes-256-cbc",
  "aes-128-cfb1",
  "aes-192-cfb1",
  "aes-256-cfb1",
  "aes-128-cfb8",
  "aes-192-cfb8",
  "aes-256-cfb8",
  "aes-128-cfb128",
  "aes-192-cfb128",
  "aes-256-cfb128",
  "aes-128-ofb",
  "aes-192-ofb",
  "aes-256-ofb",
  0
};
const char *ObSysVarValidatePasswordCheckUserName::VALIDATE_PASSWORD_CHECK_USER_NAME_NAMES[] = {
  "on",
  "off",
  0
};
const char *ObSysVarValidatePasswordPolicy::VALIDATE_PASSWORD_POLICY_NAMES[] = {
  "low",
  "medium",
  0
};
const char *ObSysVarObPxBcastOptimization::_OB_PX_BCAST_OPTIMIZATION_NAMES[] = {
  "WORKER",
  "SERVER",
  0
};
const char *ObSysVarCursorSharing::CURSOR_SHARING_NAMES[] = {
  "FORCE",
  "EXACT",
  0
};
const char *ObSysVarPxPartialRollupPushdown::_PX_PARTIAL_ROLLUP_PUSHDOWN_NAMES[] = {
  "OFF",
  "ADAPTIVE",
  0
};
const char *ObSysVarPxDistAggPartialRollupPushdown::_PX_DIST_AGG_PARTIAL_ROLLUP_PUSHDOWN_NAMES[] = {
  "OFF",
  "ADAPTIVE",
  0
};
const char *ObSysVarParallelDegreePolicy::PARALLEL_DEGREE_POLICY_NAMES[] = {
  "MANUAL",
  "AUTO",
  0
};

const char *ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_NAME[] = {
  "_aggregation_optimization_settings",
  "_clear_last_archive_timestamp",
  "_create_audit_purge_job",
  "_drop_audit_purge_job",
  "_enable_mysql_pl_priv_check",
  "_enable_parallel_ddl",
  "_enable_parallel_dml",
  "_enable_parallel_query",
  "_force_order_preserve_set",
  "_force_parallel_ddl_dop",
  "_force_parallel_dml_dop",
  "_force_parallel_query_dop",
  "_groupby_nopushdown_cut_ratio",
  "_nlj_batching_enabled",
  "_ob_ols_policy_session_labels",
  "_ob_proxy_session_temporary_table_used",
  "_ob_proxy_weakread_feedback",
  "_ob_px_bcast_optimization",
  "_ob_px_slave_mapping_threshold",
  "_optimizer_gather_stats_on_load",
  "_optimizer_null_aware_antijoin",
  "_priv_control",
  "_px_broadcast_fudge_factor",
  "_px_dist_agg_partial_rollup_pushdown",
  "_px_min_granules_per_slave",
  "_px_partial_rollup_pushdown",
  "_px_partition_scan_threshold",
  "_px_shared_hash_join",
  "_set_last_archive_timestamp",
  "_set_purge_job_interval",
  "_set_purge_job_status",
  "_set_reverse_dblink_infos",
  "_show_ddl_in_compat_mode",
  "_windowfunc_optimization_settings",
  "auto_increment_cache_size",
  "auto_increment_increment",
  "auto_increment_offset",
  "autocommit",
  "automatic_sp_privileges",
  "binlog_checksum",
  "binlog_format",
  "binlog_row_image",
  "binlog_rows_query_log_events",
  "block_encryption_mode",
  "character_set_client",
  "character_set_connection",
  "character_set_database",
  "character_set_filesystem",
  "character_set_results",
  "character_set_server",
  "character_set_system",
  "collation_connection",
  "collation_database",
  "collation_server",
  "concurrent_insert",
  "connect_timeout",
  "cte_max_recursion_depth",
  "cursor_sharing",
  "datadir",
  "debug_sync",
  "default_authentication_plugin",
  "default_password_lifetime",
  "default_storage_engine",
  "disabled_storage_engines",
  "div_precision_increment",
  "error_count",
  "error_on_overlap_time",
  "explicit_defaults_for_timestamp",
  "foreign_key_checks",
  "general_log",
  "group_concat_max_len",
  "have_openssl",
  "have_profiling",
  "have_query_cache",
  "have_ssl",
  "hostname",
  "identity",
  "init_connect",
  "innodb_strict_mode",
  "interactive_timeout",
  "is_result_accurate",
  "last_insert_id",
  "lc_messages",
  "license",
  "local_infile",
  "lock_wait_timeout",
  "log_bin",
  "log_row_value_options",
  "long_query_time",
  "lower_case_table_names",
  "max_allowed_packet",
  "max_connections",
  "max_execution_time",
  "max_sp_recursion_depth",
  "max_user_connections",
  "ncharacter_set_connection",
  "net_buffer_length",
  "net_read_timeout",
  "net_write_timeout",
  "nls_calendar",
  "nls_characterset",
  "nls_comp",
  "nls_currency",
  "nls_date_format",
  "nls_date_language",
  "nls_dual_currency",
  "nls_iso_currency",
  "nls_language",
  "nls_length_semantics",
  "nls_nchar_characterset",
  "nls_nchar_conv_excp",
  "nls_numeric_characters",
  "nls_sort",
  "nls_territory",
  "nls_timestamp_format",
  "nls_timestamp_tz_format",
  "ob_bnl_join_cache_size",
  "ob_capability_flag",
  "ob_check_sys_variable",
  "ob_compatibility_mode",
  "ob_early_lock_release",
  "ob_enable_aggregation_pushdown",
  "ob_enable_index_direct_select",
  "ob_enable_jit",
  "ob_enable_plan_cache",
  "ob_enable_rich_error_msg",
  "ob_enable_show_trace",
  "ob_enable_sql_audit",
  "ob_enable_transformation",
  "ob_enable_transmission_checksum",
  "ob_enable_truncate_flashback",
  "ob_global_debug_sync",
  "ob_interm_result_mem_limit",
  "ob_last_schema_version",
  "ob_log_level",
  "ob_max_read_stale_time",
  "ob_org_cluster_id",
  "ob_pl_block_timeout",
  "ob_plan_cache_evict_high_percentage",
  "ob_plan_cache_evict_low_percentage",
  "ob_plan_cache_percentage",
  "ob_proxy_global_variables_version",
  "ob_proxy_partition_hit",
  "ob_proxy_set_trx_executed",
  "ob_proxy_user_privilege",
  "ob_query_timeout",
  "ob_read_consistency",
  "ob_reserved_meta_memory_percentage",
  "ob_route_policy",
  "ob_safe_weak_read_snapshot",
  "ob_sql_audit_percentage",
  "ob_sql_work_area_percentage",
  "ob_statement_trace_id",
  "ob_tcp_invited_nodes",
  "ob_temp_tablespace_size_percentage",
  "ob_trace_info",
  "ob_trx_idle_timeout",
  "ob_trx_lock_timeout",
  "ob_trx_timeout",
  "optimizer_capture_sql_plan_baselines",
  "optimizer_dynamic_sampling",
  "optimizer_features_enable",
  "optimizer_use_sql_plan_baselines",
  "parallel_degree_limit",
  "parallel_degree_policy",
  "parallel_min_scan_time_threshold",
  "parallel_servers_target",
  "performance_schema",
  "plsql_ccflags",
  "plsql_warnings",
  "plugin_dir",
  "privilege_features_enable",
  "protocol_version",
  "query_cache_limit",
  "query_cache_min_res_unit",
  "query_cache_size",
  "query_cache_type",
  "query_cache_wlock_invalidate",
  "read_only",
  "recyclebin",
  "regexp_stack_limit",
  "regexp_time_limit",
  "resource_manager_plan",
  "runtime_bloom_filter_max_size",
  "runtime_filter_max_in_num",
  "runtime_filter_type",
  "runtime_filter_wait_time_ms",
  "secure_file_priv",
  "server_id",
  "server_uuid",
  "session_track_schema",
  "session_track_state_change",
  "session_track_system_variables",
  "sql_auto_is_null",
  "sql_mode",
  "sql_notes",
  "sql_quote_show_create",
  "sql_safe_updates",
  "sql_select_limit",
  "sql_throttle_cpu",
  "sql_throttle_current_priority",
  "sql_throttle_io",
  "sql_throttle_logical_reads",
  "sql_throttle_network",
  "sql_throttle_priority",
  "sql_throttle_rt",
  "sql_warnings",
  "ssl_ca",
  "ssl_capath",
  "ssl_cert",
  "ssl_cipher",
  "ssl_crl",
  "ssl_crlpath",
  "ssl_key",
  "system_time_zone",
  "time_format",
  "time_zone",
  "timestamp",
  "tls_version",
  "tmp_table_size",
  "tmpdir",
  "tracefile_identifier",
  "transaction_isolation",
  "transaction_read_only",
  "tx_isolation",
  "tx_read_only",
  "unique_checks",
  "validate_password_check_user_name",
  "validate_password_length",
  "validate_password_mixed_case_count",
  "validate_password_number_count",
  "validate_password_policy",
  "validate_password_special_char_count",
  "version",
  "version_comment",
  "version_compile_machine",
  "version_compile_os",
  "wait_timeout",
  "warning_count"
};

const ObSysVarClassType ObSysVarFactory::SYS_VAR_IDS_SORTED_BY_NAME[] = {
  SYS_VAR__AGGREGATION_OPTIMIZATION_SETTINGS,
  SYS_VAR__CLEAR_LAST_ARCHIVE_TIMESTAMP,
  SYS_VAR__CREATE_AUDIT_PURGE_JOB,
  SYS_VAR__DROP_AUDIT_PURGE_JOB,
  SYS_VAR__ENABLE_MYSQL_PL_PRIV_CHECK,
  SYS_VAR__ENABLE_PARALLEL_DDL,
  SYS_VAR__ENABLE_PARALLEL_DML,
  SYS_VAR__ENABLE_PARALLEL_QUERY,
  SYS_VAR__FORCE_ORDER_PRESERVE_SET,
  SYS_VAR__FORCE_PARALLEL_DDL_DOP,
  SYS_VAR__FORCE_PARALLEL_DML_DOP,
  SYS_VAR__FORCE_PARALLEL_QUERY_DOP,
  SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO,
  SYS_VAR__NLJ_BATCHING_ENABLED,
  SYS_VAR__OB_OLS_POLICY_SESSION_LABELS,
  SYS_VAR__OB_PROXY_SESSION_TEMPORARY_TABLE_USED,
  SYS_VAR__OB_PROXY_WEAKREAD_FEEDBACK,
  SYS_VAR__OB_PX_BCAST_OPTIMIZATION,
  SYS_VAR__OB_PX_SLAVE_MAPPING_THRESHOLD,
  SYS_VAR__OPTIMIZER_GATHER_STATS_ON_LOAD,
  SYS_VAR__OPTIMIZER_NULL_AWARE_ANTIJOIN,
  SYS_VAR__PRIV_CONTROL,
  SYS_VAR__PX_BROADCAST_FUDGE_FACTOR,
  SYS_VAR__PX_DIST_AGG_PARTIAL_ROLLUP_PUSHDOWN,
  SYS_VAR__PX_MIN_GRANULES_PER_SLAVE,
  SYS_VAR__PX_PARTIAL_ROLLUP_PUSHDOWN,
  SYS_VAR__PX_PARTITION_SCAN_THRESHOLD,
  SYS_VAR__PX_SHARED_HASH_JOIN,
  SYS_VAR__SET_LAST_ARCHIVE_TIMESTAMP,
  SYS_VAR__SET_PURGE_JOB_INTERVAL,
  SYS_VAR__SET_PURGE_JOB_STATUS,
  SYS_VAR__SET_REVERSE_DBLINK_INFOS,
  SYS_VAR__SHOW_DDL_IN_COMPAT_MODE,
  SYS_VAR__WINDOWFUNC_OPTIMIZATION_SETTINGS,
  SYS_VAR_AUTO_INCREMENT_CACHE_SIZE,
  SYS_VAR_AUTO_INCREMENT_INCREMENT,
  SYS_VAR_AUTO_INCREMENT_OFFSET,
  SYS_VAR_AUTOCOMMIT,
  SYS_VAR_AUTOMATIC_SP_PRIVILEGES,
  SYS_VAR_BINLOG_CHECKSUM,
  SYS_VAR_BINLOG_FORMAT,
  SYS_VAR_BINLOG_ROW_IMAGE,
  SYS_VAR_BINLOG_ROWS_QUERY_LOG_EVENTS,
  SYS_VAR_BLOCK_ENCRYPTION_MODE,
  SYS_VAR_CHARACTER_SET_CLIENT,
  SYS_VAR_CHARACTER_SET_CONNECTION,
  SYS_VAR_CHARACTER_SET_DATABASE,
  SYS_VAR_CHARACTER_SET_FILESYSTEM,
  SYS_VAR_CHARACTER_SET_RESULTS,
  SYS_VAR_CHARACTER_SET_SERVER,
  SYS_VAR_CHARACTER_SET_SYSTEM,
  SYS_VAR_COLLATION_CONNECTION,
  SYS_VAR_COLLATION_DATABASE,
  SYS_VAR_COLLATION_SERVER,
  SYS_VAR_CONCURRENT_INSERT,
  SYS_VAR_CONNECT_TIMEOUT,
  SYS_VAR_CTE_MAX_RECURSION_DEPTH,
  SYS_VAR_CURSOR_SHARING,
  SYS_VAR_DATADIR,
  SYS_VAR_DEBUG_SYNC,
  SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN,
  SYS_VAR_DEFAULT_PASSWORD_LIFETIME,
  SYS_VAR_DEFAULT_STORAGE_ENGINE,
  SYS_VAR_DISABLED_STORAGE_ENGINES,
  SYS_VAR_DIV_PRECISION_INCREMENT,
  SYS_VAR_ERROR_COUNT,
  SYS_VAR_ERROR_ON_OVERLAP_TIME,
  SYS_VAR_EXPLICIT_DEFAULTS_FOR_TIMESTAMP,
  SYS_VAR_FOREIGN_KEY_CHECKS,
  SYS_VAR_GENERAL_LOG,
  SYS_VAR_GROUP_CONCAT_MAX_LEN,
  SYS_VAR_HAVE_OPENSSL,
  SYS_VAR_HAVE_PROFILING,
  SYS_VAR_HAVE_QUERY_CACHE,
  SYS_VAR_HAVE_SSL,
  SYS_VAR_HOSTNAME,
  SYS_VAR_IDENTITY,
  SYS_VAR_INIT_CONNECT,
  SYS_VAR_INNODB_STRICT_MODE,
  SYS_VAR_INTERACTIVE_TIMEOUT,
  SYS_VAR_IS_RESULT_ACCURATE,
  SYS_VAR_LAST_INSERT_ID,
  SYS_VAR_LC_MESSAGES,
  SYS_VAR_LICENSE,
  SYS_VAR_LOCAL_INFILE,
  SYS_VAR_LOCK_WAIT_TIMEOUT,
  SYS_VAR_LOG_BIN,
  SYS_VAR_LOG_ROW_VALUE_OPTIONS,
  SYS_VAR_LONG_QUERY_TIME,
  SYS_VAR_LOWER_CASE_TABLE_NAMES,
  SYS_VAR_MAX_ALLOWED_PACKET,
  SYS_VAR_MAX_CONNECTIONS,
  SYS_VAR_MAX_EXECUTION_TIME,
  SYS_VAR_MAX_SP_RECURSION_DEPTH,
  SYS_VAR_MAX_USER_CONNECTIONS,
  SYS_VAR_NCHARACTER_SET_CONNECTION,
  SYS_VAR_NET_BUFFER_LENGTH,
  SYS_VAR_NET_READ_TIMEOUT,
  SYS_VAR_NET_WRITE_TIMEOUT,
  SYS_VAR_NLS_CALENDAR,
  SYS_VAR_NLS_CHARACTERSET,
  SYS_VAR_NLS_COMP,
  SYS_VAR_NLS_CURRENCY,
  SYS_VAR_NLS_DATE_FORMAT,
  SYS_VAR_NLS_DATE_LANGUAGE,
  SYS_VAR_NLS_DUAL_CURRENCY,
  SYS_VAR_NLS_ISO_CURRENCY,
  SYS_VAR_NLS_LANGUAGE,
  SYS_VAR_NLS_LENGTH_SEMANTICS,
  SYS_VAR_NLS_NCHAR_CHARACTERSET,
  SYS_VAR_NLS_NCHAR_CONV_EXCP,
  SYS_VAR_NLS_NUMERIC_CHARACTERS,
  SYS_VAR_NLS_SORT,
  SYS_VAR_NLS_TERRITORY,
  SYS_VAR_NLS_TIMESTAMP_FORMAT,
  SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT,
  SYS_VAR_OB_BNL_JOIN_CACHE_SIZE,
  SYS_VAR_OB_CAPABILITY_FLAG,
  SYS_VAR_OB_CHECK_SYS_VARIABLE,
  SYS_VAR_OB_COMPATIBILITY_MODE,
  SYS_VAR_OB_EARLY_LOCK_RELEASE,
  SYS_VAR_OB_ENABLE_AGGREGATION_PUSHDOWN,
  SYS_VAR_OB_ENABLE_INDEX_DIRECT_SELECT,
  SYS_VAR_OB_ENABLE_JIT,
  SYS_VAR_OB_ENABLE_PLAN_CACHE,
  SYS_VAR_OB_ENABLE_RICH_ERROR_MSG,
  SYS_VAR_OB_ENABLE_SHOW_TRACE,
  SYS_VAR_OB_ENABLE_SQL_AUDIT,
  SYS_VAR_OB_ENABLE_TRANSFORMATION,
  SYS_VAR_OB_ENABLE_TRANSMISSION_CHECKSUM,
  SYS_VAR_OB_ENABLE_TRUNCATE_FLASHBACK,
  SYS_VAR_OB_GLOBAL_DEBUG_SYNC,
  SYS_VAR_OB_INTERM_RESULT_MEM_LIMIT,
  SYS_VAR_OB_LAST_SCHEMA_VERSION,
  SYS_VAR_OB_LOG_LEVEL,
  SYS_VAR_OB_MAX_READ_STALE_TIME,
  SYS_VAR_OB_ORG_CLUSTER_ID,
  SYS_VAR_OB_PL_BLOCK_TIMEOUT,
  SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE,
  SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE,
  SYS_VAR_OB_PLAN_CACHE_PERCENTAGE,
  SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION,
  SYS_VAR_OB_PROXY_PARTITION_HIT,
  SYS_VAR_OB_PROXY_SET_TRX_EXECUTED,
  SYS_VAR_OB_PROXY_USER_PRIVILEGE,
  SYS_VAR_OB_QUERY_TIMEOUT,
  SYS_VAR_OB_READ_CONSISTENCY,
  SYS_VAR_OB_RESERVED_META_MEMORY_PERCENTAGE,
  SYS_VAR_OB_ROUTE_POLICY,
  SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT,
  SYS_VAR_OB_SQL_AUDIT_PERCENTAGE,
  SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE,
  SYS_VAR_OB_STATEMENT_TRACE_ID,
  SYS_VAR_OB_TCP_INVITED_NODES,
  SYS_VAR_OB_TEMP_TABLESPACE_SIZE_PERCENTAGE,
  SYS_VAR_OB_TRACE_INFO,
  SYS_VAR_OB_TRX_IDLE_TIMEOUT,
  SYS_VAR_OB_TRX_LOCK_TIMEOUT,
  SYS_VAR_OB_TRX_TIMEOUT,
  SYS_VAR_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES,
  SYS_VAR_OPTIMIZER_DYNAMIC_SAMPLING,
  SYS_VAR_OPTIMIZER_FEATURES_ENABLE,
  SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES,
  SYS_VAR_PARALLEL_DEGREE_LIMIT,
  SYS_VAR_PARALLEL_DEGREE_POLICY,
  SYS_VAR_PARALLEL_MIN_SCAN_TIME_THRESHOLD,
  SYS_VAR_PARALLEL_SERVERS_TARGET,
  SYS_VAR_PERFORMANCE_SCHEMA,
  SYS_VAR_PLSQL_CCFLAGS,
  SYS_VAR_PLSQL_WARNINGS,
  SYS_VAR_PLUGIN_DIR,
  SYS_VAR_PRIVILEGE_FEATURES_ENABLE,
  SYS_VAR_PROTOCOL_VERSION,
  SYS_VAR_QUERY_CACHE_LIMIT,
  SYS_VAR_QUERY_CACHE_MIN_RES_UNIT,
  SYS_VAR_QUERY_CACHE_SIZE,
  SYS_VAR_QUERY_CACHE_TYPE,
  SYS_VAR_QUERY_CACHE_WLOCK_INVALIDATE,
  SYS_VAR_READ_ONLY,
  SYS_VAR_RECYCLEBIN,
  SYS_VAR_REGEXP_STACK_LIMIT,
  SYS_VAR_REGEXP_TIME_LIMIT,
  SYS_VAR_RESOURCE_MANAGER_PLAN,
  SYS_VAR_RUNTIME_BLOOM_FILTER_MAX_SIZE,
  SYS_VAR_RUNTIME_FILTER_MAX_IN_NUM,
  SYS_VAR_RUNTIME_FILTER_TYPE,
  SYS_VAR_RUNTIME_FILTER_WAIT_TIME_MS,
  SYS_VAR_SECURE_FILE_PRIV,
  SYS_VAR_SERVER_ID,
  SYS_VAR_SERVER_UUID,
  SYS_VAR_SESSION_TRACK_SCHEMA,
  SYS_VAR_SESSION_TRACK_STATE_CHANGE,
  SYS_VAR_SESSION_TRACK_SYSTEM_VARIABLES,
  SYS_VAR_SQL_AUTO_IS_NULL,
  SYS_VAR_SQL_MODE,
  SYS_VAR_SQL_NOTES,
  SYS_VAR_SQL_QUOTE_SHOW_CREATE,
  SYS_VAR_SQL_SAFE_UPDATES,
  SYS_VAR_SQL_SELECT_LIMIT,
  SYS_VAR_SQL_THROTTLE_CPU,
  SYS_VAR_SQL_THROTTLE_CURRENT_PRIORITY,
  SYS_VAR_SQL_THROTTLE_IO,
  SYS_VAR_SQL_THROTTLE_LOGICAL_READS,
  SYS_VAR_SQL_THROTTLE_NETWORK,
  SYS_VAR_SQL_THROTTLE_PRIORITY,
  SYS_VAR_SQL_THROTTLE_RT,
  SYS_VAR_SQL_WARNINGS,
  SYS_VAR_SSL_CA,
  SYS_VAR_SSL_CAPATH,
  SYS_VAR_SSL_CERT,
  SYS_VAR_SSL_CIPHER,
  SYS_VAR_SSL_CRL,
  SYS_VAR_SSL_CRLPATH,
  SYS_VAR_SSL_KEY,
  SYS_VAR_SYSTEM_TIME_ZONE,
  SYS_VAR_TIME_FORMAT,
  SYS_VAR_TIME_ZONE,
  SYS_VAR_TIMESTAMP,
  SYS_VAR_TLS_VERSION,
  SYS_VAR_TMP_TABLE_SIZE,
  SYS_VAR_TMPDIR,
  SYS_VAR_TRACEFILE_IDENTIFIER,
  SYS_VAR_TRANSACTION_ISOLATION,
  SYS_VAR_TRANSACTION_READ_ONLY,
  SYS_VAR_TX_ISOLATION,
  SYS_VAR_TX_READ_ONLY,
  SYS_VAR_UNIQUE_CHECKS,
  SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME,
  SYS_VAR_VALIDATE_PASSWORD_LENGTH,
  SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT,
  SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT,
  SYS_VAR_VALIDATE_PASSWORD_POLICY,
  SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT,
  SYS_VAR_VERSION,
  SYS_VAR_VERSION_COMMENT,
  SYS_VAR_VERSION_COMPILE_MACHINE,
  SYS_VAR_VERSION_COMPILE_OS,
  SYS_VAR_WAIT_TIMEOUT,
  SYS_VAR_WARNING_COUNT
};

const char *ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_ID[] = {
  "auto_increment_increment",
  "auto_increment_offset",
  "autocommit",
  "character_set_client",
  "character_set_connection",
  "character_set_database",
  "character_set_results",
  "character_set_server",
  "character_set_system",
  "collation_connection",
  "collation_database",
  "collation_server",
  "interactive_timeout",
  "last_insert_id",
  "max_allowed_packet",
  "sql_mode",
  "time_zone",
  "tx_isolation",
  "version_comment",
  "wait_timeout",
  "binlog_row_image",
  "character_set_filesystem",
  "connect_timeout",
  "datadir",
  "debug_sync",
  "div_precision_increment",
  "explicit_defaults_for_timestamp",
  "group_concat_max_len",
  "identity",
  "lower_case_table_names",
  "net_read_timeout",
  "net_write_timeout",
  "read_only",
  "sql_auto_is_null",
  "sql_select_limit",
  "timestamp",
  "tx_read_only",
  "version",
  "sql_warnings",
  "max_user_connections",
  "init_connect",
  "license",
  "net_buffer_length",
  "system_time_zone",
  "query_cache_size",
  "query_cache_type",
  "sql_quote_show_create",
  "max_sp_recursion_depth",
  "sql_safe_updates",
  "concurrent_insert",
  "default_authentication_plugin",
  "disabled_storage_engines",
  "error_count",
  "general_log",
  "have_openssl",
  "have_profiling",
  "have_ssl",
  "hostname",
  "lc_messages",
  "local_infile",
  "lock_wait_timeout",
  "long_query_time",
  "max_connections",
  "max_execution_time",
  "protocol_version",
  "server_id",
  "ssl_ca",
  "ssl_capath",
  "ssl_cert",
  "ssl_cipher",
  "ssl_crl",
  "ssl_crlpath",
  "ssl_key",
  "time_format",
  "tls_version",
  "tmp_table_size",
  "tmpdir",
  "unique_checks",
  "version_compile_machine",
  "version_compile_os",
  "warning_count",
  "session_track_schema",
  "session_track_system_variables",
  "session_track_state_change",
  "have_query_cache",
  "query_cache_limit",
  "query_cache_min_res_unit",
  "query_cache_wlock_invalidate",
  "binlog_format",
  "binlog_checksum",
  "binlog_rows_query_log_events",
  "log_bin",
  "server_uuid",
  "default_storage_engine",
  "cte_max_recursion_depth",
  "regexp_stack_limit",
  "regexp_time_limit",
  "ob_interm_result_mem_limit",
  "ob_proxy_partition_hit",
  "ob_log_level",
  "ob_query_timeout",
  "ob_read_consistency",
  "ob_enable_transformation",
  "ob_trx_timeout",
  "ob_enable_plan_cache",
  "ob_enable_index_direct_select",
  "ob_proxy_set_trx_executed",
  "ob_enable_aggregation_pushdown",
  "ob_last_schema_version",
  "ob_global_debug_sync",
  "ob_proxy_global_variables_version",
  "ob_enable_show_trace",
  "ob_bnl_join_cache_size",
  "ob_proxy_user_privilege",
  "ob_org_cluster_id",
  "ob_plan_cache_percentage",
  "ob_plan_cache_evict_high_percentage",
  "ob_plan_cache_evict_low_percentage",
  "recyclebin",
  "ob_capability_flag",
  "is_result_accurate",
  "error_on_overlap_time",
  "ob_compatibility_mode",
  "ob_sql_work_area_percentage",
  "ob_safe_weak_read_snapshot",
  "ob_route_policy",
  "ob_enable_transmission_checksum",
  "foreign_key_checks",
  "ob_statement_trace_id",
  "ob_enable_truncate_flashback",
  "ob_tcp_invited_nodes",
  "sql_throttle_current_priority",
  "sql_throttle_priority",
  "sql_throttle_rt",
  "sql_throttle_cpu",
  "sql_throttle_io",
  "sql_throttle_network",
  "sql_throttle_logical_reads",
  "auto_increment_cache_size",
  "ob_enable_jit",
  "ob_temp_tablespace_size_percentage",
  "plugin_dir",
  "ob_sql_audit_percentage",
  "ob_enable_sql_audit",
  "optimizer_use_sql_plan_baselines",
  "optimizer_capture_sql_plan_baselines",
  "parallel_servers_target",
  "ob_early_lock_release",
  "ob_trx_idle_timeout",
  "block_encryption_mode",
  "nls_date_format",
  "nls_timestamp_format",
  "nls_timestamp_tz_format",
  "ob_reserved_meta_memory_percentage",
  "ob_check_sys_variable",
  "nls_language",
  "nls_territory",
  "nls_sort",
  "nls_comp",
  "nls_characterset",
  "nls_nchar_characterset",
  "nls_date_language",
  "nls_length_semantics",
  "nls_nchar_conv_excp",
  "nls_calendar",
  "nls_numeric_characters",
  "_nlj_batching_enabled",
  "tracefile_identifier",
  "_groupby_nopushdown_cut_ratio",
  "_px_broadcast_fudge_factor",
  "transaction_isolation",
  "ob_trx_lock_timeout",
  "validate_password_check_user_name",
  "validate_password_length",
  "validate_password_mixed_case_count",
  "validate_password_number_count",
  "validate_password_policy",
  "validate_password_special_char_count",
  "default_password_lifetime",
  "_ob_ols_policy_session_labels",
  "ob_trace_info",
  "_px_partition_scan_threshold",
  "_ob_px_bcast_optimization",
  "_ob_px_slave_mapping_threshold",
  "_enable_parallel_dml",
  "_px_min_granules_per_slave",
  "secure_file_priv",
  "plsql_warnings",
  "_enable_parallel_query",
  "_force_parallel_query_dop",
  "_force_parallel_dml_dop",
  "ob_pl_block_timeout",
  "transaction_read_only",
  "resource_manager_plan",
  "performance_schema",
  "nls_currency",
  "nls_iso_currency",
  "nls_dual_currency",
  "plsql_ccflags",
  "_ob_proxy_session_temporary_table_used",
  "_enable_parallel_ddl",
  "_force_parallel_ddl_dop",
  "cursor_sharing",
  "_optimizer_null_aware_antijoin",
  "_px_partial_rollup_pushdown",
  "_px_dist_agg_partial_rollup_pushdown",
  "_create_audit_purge_job",
  "_drop_audit_purge_job",
  "_set_purge_job_interval",
  "_set_purge_job_status",
  "_set_last_archive_timestamp",
  "_clear_last_archive_timestamp",
  "_aggregation_optimization_settings",
  "_px_shared_hash_join",
  "sql_notes",
  "innodb_strict_mode",
  "_windowfunc_optimization_settings",
  "ob_enable_rich_error_msg",
  "log_row_value_options",
  "ob_max_read_stale_time",
  "_optimizer_gather_stats_on_load",
  "_set_reverse_dblink_infos",
  "_force_order_preserve_set",
  "_show_ddl_in_compat_mode",
  "parallel_degree_policy",
  "parallel_degree_limit",
  "parallel_min_scan_time_threshold",
  "optimizer_dynamic_sampling",
  "runtime_filter_type",
  "runtime_filter_wait_time_ms",
  "runtime_filter_max_in_num",
  "runtime_bloom_filter_max_size",
  "optimizer_features_enable",
  "_ob_proxy_weakread_feedback",
  "ncharacter_set_connection",
  "automatic_sp_privileges",
  "privilege_features_enable",
  "_priv_control",
  "_enable_mysql_pl_priv_check"
};

bool ObSysVarFactory::sys_var_name_case_cmp(const char *name1, const ObString &name2)
{
  return name2.case_compare(name1) > 0;
}

ObSysVarClassType ObSysVarFactory::find_sys_var_id_by_name(const ObString &sys_var_name,
                                                           bool is_from_sys_table /*= false*/)
{
  int ret = OB_SUCCESS;
  ObSysVarClassType sys_var_id = SYS_VAR_INVALID;
  int64_t lower_idx = std::lower_bound(ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_NAME,
      ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_NAME + ObSysVarFactory::ALL_SYS_VARS_COUNT,
      sys_var_name, ObSysVarFactory::sys_var_name_case_cmp) -
      ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_NAME;
  if (OB_UNLIKELY(lower_idx < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid lower index", K(ret), K(sys_var_name), K(lower_idx), K(lbt()));
  } else if (OB_UNLIKELY(lower_idx > ObSysVarFactory::ALL_SYS_VARS_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid lower index", K(ret), K(sys_var_name), K(lower_idx),
              LITERAL_K(ObSysVarFactory::ALL_SYS_VARS_COUNT), K(lbt()));
  } else if (OB_UNLIKELY(ObSysVarFactory::ALL_SYS_VARS_COUNT == lower_idx)) {
    // std::lower_bound返回ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_NAME +
    // ObSysVarFactory::ALL_SYS_VARS_COUNT的地址，即是找不到，而不是出错
    ret = OB_SEARCH_NOT_FOUND;
  } else if (0 != sys_var_name.case_compare(
      ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_NAME[lower_idx])) {
    // 找不到
    ret = OB_SEARCH_NOT_FOUND;
  } else {
    sys_var_id = ObSysVarFactory::SYS_VAR_IDS_SORTED_BY_NAME[lower_idx]; // 找到了
  }
  if (OB_UNLIKELY(OB_SEARCH_NOT_FOUND == ret)) {
    if (is_from_sys_table) {
      LOG_INFO("new system variable is added , so can not found; don't worry", K(sys_var_name),
               K(lower_idx), LITERAL_K(ObSysVarFactory::ALL_SYS_VARS_COUNT), K(lbt()));
    } else {
      LOG_WARN("sys var name not found", K(sys_var_name), K(lower_idx),
               LITERAL_K(ObSysVarFactory::ALL_SYS_VARS_COUNT), K(lbt()));
    }
  }
  return sys_var_id;
}

int ObSysVarFactory::calc_sys_var_store_idx(ObSysVarClassType sys_var_id, int64_t &store_idx)
{
  int ret = OB_SUCCESS;
  int64_t real_idx = -1;
  int64_t var_id = static_cast<int64_t>(sys_var_id);
  if (ObSysVarsToIdxMap::has_invalid_sys_var_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("has invalid sys var id", K(ret), K(ObSysVarsToIdxMap::has_invalid_sys_var_id()));
  } else if (OB_UNLIKELY(var_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid sys var id", K(ret), K(var_id));
  } else {
    // 直接利用ObSysVarsIdToArrayIdx 索引数组查询到对应的store idx
    real_idx = ObSysVarsToIdxMap::get_store_idx(var_id);
    if (real_idx < 0) {
      ret = OB_SYS_VARS_MAYBE_DIFF_VERSION;
      LOG_WARN("invalid sys var id, maybe sys vars version is different", K(ret), K(var_id), K(real_idx),
          LITERAL_K(ObSysVarFactory::OB_SPECIFIC_SYS_VAR_ID_OFFSET),
          LITERAL_K(ObSysVarFactory::OB_SYS_VARS_COUNT));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    store_idx = real_idx;
  }
  return ret;
}

int ObSysVarFactory::calc_sys_var_store_idx_by_name(const common::ObString &sys_var_name,
                                                    int64_t &store_idx)
{
  int ret = OB_SUCCESS;
  ObSysVarClassType sys_var_id = find_sys_var_id_by_name(sys_var_name);
  if (OB_FAIL(calc_sys_var_store_idx(sys_var_id, store_idx))) {
    LOG_WARN("fail to calc sys var store idx", K(ret), K(sys_var_name), K(lbt()));
  }
  return ret;
}

bool ObSysVarFactory::is_valid_sys_var_store_idx(int64_t store_idx)
{
  return 0 <= store_idx && store_idx < ObSysVarFactory::ALL_SYS_VARS_COUNT;
}

int ObSysVarFactory::get_sys_var_name_by_id(ObSysVarClassType sys_var_id, ObString &sys_var_name)
{
  int ret = OB_SUCCESS;
  int64_t store_idx = -1;
  if (OB_FAIL(calc_sys_var_store_idx(sys_var_id, store_idx))) {
    LOG_WARN("fail to calc sys var store idx", K(ret), K(sys_var_id));
  } else {
    sys_var_name = ObString::make_string(ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_ID[store_idx]);
  }
  return ret;
}

const ObString ObSysVarFactory::get_sys_var_name_by_id(ObSysVarClassType sys_var_id)
{
  ObString sys_var_name;
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_sys_var_name_by_id(sys_var_id, sys_var_name))) {
    sys_var_name = ObString::make_string("invalid_sys_var_name");
    LOG_WARN("invalid sys var id", K(ret), K(sys_var_id));
  }
  return sys_var_name;
}

ObSysVarFactory::ObSysVarFactory(const int64_t tenant_id)
  : allocator_(ObMemAttr(tenant_id, ObModIds::OB_COMMON_SYS_VAR_FAC)),
    all_sys_vars_created_(false)
{
  MEMSET(store_, 0, sizeof(store_));
  MEMSET(store_buf_, 0, sizeof(store_buf_));
}

ObSysVarFactory::~ObSysVarFactory()
{
  destroy();
}

void ObSysVarFactory::destroy()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < ALL_SYS_VARS_COUNT; ++i) {
    if (OB_NOT_NULL(store_[i])) {
      store_[i]->~ObBasicSysVar();
      store_[i] = nullptr;
    }
    if (OB_NOT_NULL(store_buf_[i])) {
      store_buf_[i]->~ObBasicSysVar();
      store_buf_[i] = nullptr;
    }
  }
  allocator_.reset();
  all_sys_vars_created_ = false;
}

int ObSysVarFactory::free_sys_var(ObBasicSysVar *sys_var, int64_t sys_var_idx)
{
  int ret = OB_SUCCESS;
  OV (OB_NOT_NULL(sys_var));
  OV (is_valid_sys_var_store_idx(sys_var_idx));
  OV (sys_var == store_[sys_var_idx], OB_ERR_UNEXPECTED, sys_var, sys_var_idx);
  if (OB_NOT_NULL(store_buf_[sys_var_idx])) {
    OX (store_buf_[sys_var_idx]->~ObBasicSysVar());
    OX (allocator_.free(store_buf_[sys_var_idx]));
    OX (store_buf_[sys_var_idx] = nullptr);
  }
  OX (store_buf_[sys_var_idx] = store_[sys_var_idx]);
  OX (store_buf_[sys_var_idx]->clean_value());
  OX (store_[sys_var_idx] = nullptr);
  return ret;
}

int ObSysVarFactory::create_all_sys_vars()
{
  int ret = OB_SUCCESS;
  if (!all_sys_vars_created_) {
    int64_t store_idx = -1;
    ObBasicSysVar *sys_var_ptr = NULL;
    int64_t total_mem_size = 0
        + sizeof(ObSysVarAutoIncrementIncrement)
        + sizeof(ObSysVarAutoIncrementOffset)
        + sizeof(ObSysVarAutocommit)
        + sizeof(ObSysVarCharacterSetClient)
        + sizeof(ObSysVarCharacterSetConnection)
        + sizeof(ObSysVarCharacterSetDatabase)
        + sizeof(ObSysVarCharacterSetResults)
        + sizeof(ObSysVarCharacterSetServer)
        + sizeof(ObSysVarCharacterSetSystem)
        + sizeof(ObSysVarCollationConnection)
        + sizeof(ObSysVarCollationDatabase)
        + sizeof(ObSysVarCollationServer)
        + sizeof(ObSysVarInteractiveTimeout)
        + sizeof(ObSysVarLastInsertId)
        + sizeof(ObSysVarMaxAllowedPacket)
        + sizeof(ObSysVarSqlMode)
        + sizeof(ObSysVarTimeZone)
        + sizeof(ObSysVarTxIsolation)
        + sizeof(ObSysVarVersionComment)
        + sizeof(ObSysVarWaitTimeout)
        + sizeof(ObSysVarBinlogRowImage)
        + sizeof(ObSysVarCharacterSetFilesystem)
        + sizeof(ObSysVarConnectTimeout)
        + sizeof(ObSysVarDatadir)
        + sizeof(ObSysVarDebugSync)
        + sizeof(ObSysVarDivPrecisionIncrement)
        + sizeof(ObSysVarExplicitDefaultsForTimestamp)
        + sizeof(ObSysVarGroupConcatMaxLen)
        + sizeof(ObSysVarIdentity)
        + sizeof(ObSysVarLowerCaseTableNames)
        + sizeof(ObSysVarNetReadTimeout)
        + sizeof(ObSysVarNetWriteTimeout)
        + sizeof(ObSysVarReadOnly)
        + sizeof(ObSysVarSqlAutoIsNull)
        + sizeof(ObSysVarSqlSelectLimit)
        + sizeof(ObSysVarTimestamp)
        + sizeof(ObSysVarTxReadOnly)
        + sizeof(ObSysVarVersion)
        + sizeof(ObSysVarSqlWarnings)
        + sizeof(ObSysVarMaxUserConnections)
        + sizeof(ObSysVarInitConnect)
        + sizeof(ObSysVarLicense)
        + sizeof(ObSysVarNetBufferLength)
        + sizeof(ObSysVarSystemTimeZone)
        + sizeof(ObSysVarQueryCacheSize)
        + sizeof(ObSysVarQueryCacheType)
        + sizeof(ObSysVarSqlQuoteShowCreate)
        + sizeof(ObSysVarMaxSpRecursionDepth)
        + sizeof(ObSysVarSqlSafeUpdates)
        + sizeof(ObSysVarConcurrentInsert)
        + sizeof(ObSysVarDefaultAuthenticationPlugin)
        + sizeof(ObSysVarDisabledStorageEngines)
        + sizeof(ObSysVarErrorCount)
        + sizeof(ObSysVarGeneralLog)
        + sizeof(ObSysVarHaveOpenssl)
        + sizeof(ObSysVarHaveProfiling)
        + sizeof(ObSysVarHaveSsl)
        + sizeof(ObSysVarHostname)
        + sizeof(ObSysVarLcMessages)
        + sizeof(ObSysVarLocalInfile)
        + sizeof(ObSysVarLockWaitTimeout)
        + sizeof(ObSysVarLongQueryTime)
        + sizeof(ObSysVarMaxConnections)
        + sizeof(ObSysVarMaxExecutionTime)
        + sizeof(ObSysVarProtocolVersion)
        + sizeof(ObSysVarServerId)
        + sizeof(ObSysVarSslCa)
        + sizeof(ObSysVarSslCapath)
        + sizeof(ObSysVarSslCert)
        + sizeof(ObSysVarSslCipher)
        + sizeof(ObSysVarSslCrl)
        + sizeof(ObSysVarSslCrlpath)
        + sizeof(ObSysVarSslKey)
        + sizeof(ObSysVarTimeFormat)
        + sizeof(ObSysVarTlsVersion)
        + sizeof(ObSysVarTmpTableSize)
        + sizeof(ObSysVarTmpdir)
        + sizeof(ObSysVarUniqueChecks)
        + sizeof(ObSysVarVersionCompileMachine)
        + sizeof(ObSysVarVersionCompileOs)
        + sizeof(ObSysVarWarningCount)
        + sizeof(ObSysVarSessionTrackSchema)
        + sizeof(ObSysVarSessionTrackSystemVariables)
        + sizeof(ObSysVarSessionTrackStateChange)
        + sizeof(ObSysVarHaveQueryCache)
        + sizeof(ObSysVarQueryCacheLimit)
        + sizeof(ObSysVarQueryCacheMinResUnit)
        + sizeof(ObSysVarQueryCacheWlockInvalidate)
        + sizeof(ObSysVarBinlogFormat)
        + sizeof(ObSysVarBinlogChecksum)
        + sizeof(ObSysVarBinlogRowsQueryLogEvents)
        + sizeof(ObSysVarLogBin)
        + sizeof(ObSysVarServerUuid)
        + sizeof(ObSysVarDefaultStorageEngine)
        + sizeof(ObSysVarCteMaxRecursionDepth)
        + sizeof(ObSysVarRegexpStackLimit)
        + sizeof(ObSysVarRegexpTimeLimit)
        + sizeof(ObSysVarObIntermResultMemLimit)
        + sizeof(ObSysVarObProxyPartitionHit)
        + sizeof(ObSysVarObLogLevel)
        + sizeof(ObSysVarObQueryTimeout)
        + sizeof(ObSysVarObReadConsistency)
        + sizeof(ObSysVarObEnableTransformation)
        + sizeof(ObSysVarObTrxTimeout)
        + sizeof(ObSysVarObEnablePlanCache)
        + sizeof(ObSysVarObEnableIndexDirectSelect)
        + sizeof(ObSysVarObProxySetTrxExecuted)
        + sizeof(ObSysVarObEnableAggregationPushdown)
        + sizeof(ObSysVarObLastSchemaVersion)
        + sizeof(ObSysVarObGlobalDebugSync)
        + sizeof(ObSysVarObProxyGlobalVariablesVersion)
        + sizeof(ObSysVarObEnableShowTrace)
        + sizeof(ObSysVarObBnlJoinCacheSize)
        + sizeof(ObSysVarObProxyUserPrivilege)
        + sizeof(ObSysVarObOrgClusterId)
        + sizeof(ObSysVarObPlanCachePercentage)
        + sizeof(ObSysVarObPlanCacheEvictHighPercentage)
        + sizeof(ObSysVarObPlanCacheEvictLowPercentage)
        + sizeof(ObSysVarRecyclebin)
        + sizeof(ObSysVarObCapabilityFlag)
        + sizeof(ObSysVarIsResultAccurate)
        + sizeof(ObSysVarErrorOnOverlapTime)
        + sizeof(ObSysVarObCompatibilityMode)
        + sizeof(ObSysVarObSqlWorkAreaPercentage)
        + sizeof(ObSysVarObSafeWeakReadSnapshot)
        + sizeof(ObSysVarObRoutePolicy)
        + sizeof(ObSysVarObEnableTransmissionChecksum)
        + sizeof(ObSysVarForeignKeyChecks)
        + sizeof(ObSysVarObStatementTraceId)
        + sizeof(ObSysVarObEnableTruncateFlashback)
        + sizeof(ObSysVarObTcpInvitedNodes)
        + sizeof(ObSysVarSqlThrottleCurrentPriority)
        + sizeof(ObSysVarSqlThrottlePriority)
        + sizeof(ObSysVarSqlThrottleRt)
        + sizeof(ObSysVarSqlThrottleCpu)
        + sizeof(ObSysVarSqlThrottleIo)
        + sizeof(ObSysVarSqlThrottleNetwork)
        + sizeof(ObSysVarSqlThrottleLogicalReads)
        + sizeof(ObSysVarAutoIncrementCacheSize)
        + sizeof(ObSysVarObEnableJit)
        + sizeof(ObSysVarObTempTablespaceSizePercentage)
        + sizeof(ObSysVarPluginDir)
        + sizeof(ObSysVarObSqlAuditPercentage)
        + sizeof(ObSysVarObEnableSqlAudit)
        + sizeof(ObSysVarOptimizerUseSqlPlanBaselines)
        + sizeof(ObSysVarOptimizerCaptureSqlPlanBaselines)
        + sizeof(ObSysVarParallelServersTarget)
        + sizeof(ObSysVarObEarlyLockRelease)
        + sizeof(ObSysVarObTrxIdleTimeout)
        + sizeof(ObSysVarBlockEncryptionMode)
        + sizeof(ObSysVarNlsDateFormat)
        + sizeof(ObSysVarNlsTimestampFormat)
        + sizeof(ObSysVarNlsTimestampTzFormat)
        + sizeof(ObSysVarObReservedMetaMemoryPercentage)
        + sizeof(ObSysVarObCheckSysVariable)
        + sizeof(ObSysVarNlsLanguage)
        + sizeof(ObSysVarNlsTerritory)
        + sizeof(ObSysVarNlsSort)
        + sizeof(ObSysVarNlsComp)
        + sizeof(ObSysVarNlsCharacterset)
        + sizeof(ObSysVarNlsNcharCharacterset)
        + sizeof(ObSysVarNlsDateLanguage)
        + sizeof(ObSysVarNlsLengthSemantics)
        + sizeof(ObSysVarNlsNcharConvExcp)
        + sizeof(ObSysVarNlsCalendar)
        + sizeof(ObSysVarNlsNumericCharacters)
        + sizeof(ObSysVarNljBatchingEnabled)
        + sizeof(ObSysVarTracefileIdentifier)
        + sizeof(ObSysVarGroupbyNopushdownCutRatio)
        + sizeof(ObSysVarPxBroadcastFudgeFactor)
        + sizeof(ObSysVarTransactionIsolation)
        + sizeof(ObSysVarObTrxLockTimeout)
        + sizeof(ObSysVarValidatePasswordCheckUserName)
        + sizeof(ObSysVarValidatePasswordLength)
        + sizeof(ObSysVarValidatePasswordMixedCaseCount)
        + sizeof(ObSysVarValidatePasswordNumberCount)
        + sizeof(ObSysVarValidatePasswordPolicy)
        + sizeof(ObSysVarValidatePasswordSpecialCharCount)
        + sizeof(ObSysVarDefaultPasswordLifetime)
        + sizeof(ObSysVarObOlsPolicySessionLabels)
        + sizeof(ObSysVarObTraceInfo)
        + sizeof(ObSysVarPxPartitionScanThreshold)
        + sizeof(ObSysVarObPxBcastOptimization)
        + sizeof(ObSysVarObPxSlaveMappingThreshold)
        + sizeof(ObSysVarEnableParallelDml)
        + sizeof(ObSysVarPxMinGranulesPerSlave)
        + sizeof(ObSysVarSecureFilePriv)
        + sizeof(ObSysVarPlsqlWarnings)
        + sizeof(ObSysVarEnableParallelQuery)
        + sizeof(ObSysVarForceParallelQueryDop)
        + sizeof(ObSysVarForceParallelDmlDop)
        + sizeof(ObSysVarObPlBlockTimeout)
        + sizeof(ObSysVarTransactionReadOnly)
        + sizeof(ObSysVarResourceManagerPlan)
        + sizeof(ObSysVarPerformanceSchema)
        + sizeof(ObSysVarNlsCurrency)
        + sizeof(ObSysVarNlsIsoCurrency)
        + sizeof(ObSysVarNlsDualCurrency)
        + sizeof(ObSysVarPlsqlCcflags)
        + sizeof(ObSysVarObProxySessionTemporaryTableUsed)
        + sizeof(ObSysVarEnableParallelDdl)
        + sizeof(ObSysVarForceParallelDdlDop)
        + sizeof(ObSysVarCursorSharing)
        + sizeof(ObSysVarOptimizerNullAwareAntijoin)
        + sizeof(ObSysVarPxPartialRollupPushdown)
        + sizeof(ObSysVarPxDistAggPartialRollupPushdown)
        + sizeof(ObSysVarCreateAuditPurgeJob)
        + sizeof(ObSysVarDropAuditPurgeJob)
        + sizeof(ObSysVarSetPurgeJobInterval)
        + sizeof(ObSysVarSetPurgeJobStatus)
        + sizeof(ObSysVarSetLastArchiveTimestamp)
        + sizeof(ObSysVarClearLastArchiveTimestamp)
        + sizeof(ObSysVarAggregationOptimizationSettings)
        + sizeof(ObSysVarPxSharedHashJoin)
        + sizeof(ObSysVarSqlNotes)
        + sizeof(ObSysVarInnodbStrictMode)
        + sizeof(ObSysVarWindowfuncOptimizationSettings)
        + sizeof(ObSysVarObEnableRichErrorMsg)
        + sizeof(ObSysVarLogRowValueOptions)
        + sizeof(ObSysVarObMaxReadStaleTime)
        + sizeof(ObSysVarOptimizerGatherStatsOnLoad)
        + sizeof(ObSysVarSetReverseDblinkInfos)
        + sizeof(ObSysVarForceOrderPreserveSet)
        + sizeof(ObSysVarShowDdlInCompatMode)
        + sizeof(ObSysVarParallelDegreePolicy)
        + sizeof(ObSysVarParallelDegreeLimit)
        + sizeof(ObSysVarParallelMinScanTimeThreshold)
        + sizeof(ObSysVarOptimizerDynamicSampling)
        + sizeof(ObSysVarRuntimeFilterType)
        + sizeof(ObSysVarRuntimeFilterWaitTimeMs)
        + sizeof(ObSysVarRuntimeFilterMaxInNum)
        + sizeof(ObSysVarRuntimeBloomFilterMaxSize)
        + sizeof(ObSysVarOptimizerFeaturesEnable)
        + sizeof(ObSysVarObProxyWeakreadFeedback)
        + sizeof(ObSysVarNcharacterSetConnection)
        + sizeof(ObSysVarAutomaticSpPrivileges)
        + sizeof(ObSysVarPrivilegeFeaturesEnable)
        + sizeof(ObSysVarPrivControl)
        + sizeof(ObSysVarEnableMysqlPlPrivCheck)
        ;
    void *ptr = NULL;
    if (OB_ISNULL(ptr = allocator_.alloc(total_mem_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory", K(ret));
    } else {
      all_sys_vars_created_ = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarAutoIncrementIncrement())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarAutoIncrementIncrement", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_AUTO_INCREMENT_INCREMENT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarAutoIncrementIncrement));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarAutoIncrementOffset())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarAutoIncrementOffset", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_AUTO_INCREMENT_OFFSET))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarAutoIncrementOffset));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarAutocommit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarAutocommit", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_AUTOCOMMIT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarAutocommit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetClient())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetClient", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_CHARACTER_SET_CLIENT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCharacterSetClient));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetConnection())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetConnection", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_CHARACTER_SET_CONNECTION))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCharacterSetConnection));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetDatabase())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetDatabase", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_CHARACTER_SET_DATABASE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCharacterSetDatabase));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetResults())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetResults", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_CHARACTER_SET_RESULTS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCharacterSetResults));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetServer())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetServer", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_CHARACTER_SET_SERVER))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCharacterSetServer));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetSystem())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetSystem", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_CHARACTER_SET_SYSTEM))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCharacterSetSystem));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCollationConnection())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCollationConnection", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_COLLATION_CONNECTION))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCollationConnection));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCollationDatabase())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCollationDatabase", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_COLLATION_DATABASE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCollationDatabase));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCollationServer())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCollationServer", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_COLLATION_SERVER))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCollationServer));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarInteractiveTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarInteractiveTimeout", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_INTERACTIVE_TIMEOUT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarInteractiveTimeout));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLastInsertId())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLastInsertId", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_LAST_INSERT_ID))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarLastInsertId));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarMaxAllowedPacket())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarMaxAllowedPacket", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_MAX_ALLOWED_PACKET))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarMaxAllowedPacket));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlMode())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlMode", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_MODE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlMode));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTimeZone())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTimeZone", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_TIME_ZONE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarTimeZone));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTxIsolation())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTxIsolation", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_TX_ISOLATION))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarTxIsolation));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarVersionComment())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarVersionComment", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_VERSION_COMMENT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarVersionComment));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarWaitTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarWaitTimeout", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_WAIT_TIMEOUT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarWaitTimeout));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarBinlogRowImage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarBinlogRowImage", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_BINLOG_ROW_IMAGE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarBinlogRowImage));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetFilesystem())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetFilesystem", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_CHARACTER_SET_FILESYSTEM))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCharacterSetFilesystem));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarConnectTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarConnectTimeout", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_CONNECT_TIMEOUT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarConnectTimeout));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDatadir())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDatadir", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_DATADIR))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarDatadir));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDebugSync())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDebugSync", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_DEBUG_SYNC))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarDebugSync));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDivPrecisionIncrement())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDivPrecisionIncrement", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_DIV_PRECISION_INCREMENT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarDivPrecisionIncrement));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarExplicitDefaultsForTimestamp())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarExplicitDefaultsForTimestamp", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_EXPLICIT_DEFAULTS_FOR_TIMESTAMP))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarExplicitDefaultsForTimestamp));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarGroupConcatMaxLen())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarGroupConcatMaxLen", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_GROUP_CONCAT_MAX_LEN))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarGroupConcatMaxLen));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarIdentity())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarIdentity", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_IDENTITY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarIdentity));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLowerCaseTableNames())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLowerCaseTableNames", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_LOWER_CASE_TABLE_NAMES))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarLowerCaseTableNames));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNetReadTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNetReadTimeout", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NET_READ_TIMEOUT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNetReadTimeout));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNetWriteTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNetWriteTimeout", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NET_WRITE_TIMEOUT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNetWriteTimeout));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarReadOnly())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarReadOnly", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_READ_ONLY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarReadOnly));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlAutoIsNull())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlAutoIsNull", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_AUTO_IS_NULL))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlAutoIsNull));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlSelectLimit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlSelectLimit", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_SELECT_LIMIT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlSelectLimit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTimestamp())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTimestamp", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_TIMESTAMP))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarTimestamp));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTxReadOnly())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTxReadOnly", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_TX_READ_ONLY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarTxReadOnly));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarVersion())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarVersion", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_VERSION))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarVersion));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlWarnings())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlWarnings", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_WARNINGS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlWarnings));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarMaxUserConnections())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarMaxUserConnections", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_MAX_USER_CONNECTIONS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarMaxUserConnections));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarInitConnect())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarInitConnect", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_INIT_CONNECT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarInitConnect));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLicense())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLicense", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_LICENSE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarLicense));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNetBufferLength())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNetBufferLength", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NET_BUFFER_LENGTH))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNetBufferLength));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSystemTimeZone())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSystemTimeZone", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SYSTEM_TIME_ZONE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSystemTimeZone));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarQueryCacheSize())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarQueryCacheSize", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_QUERY_CACHE_SIZE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarQueryCacheSize));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarQueryCacheType())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarQueryCacheType", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_QUERY_CACHE_TYPE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarQueryCacheType));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlQuoteShowCreate())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlQuoteShowCreate", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_QUOTE_SHOW_CREATE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlQuoteShowCreate));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarMaxSpRecursionDepth())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarMaxSpRecursionDepth", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_MAX_SP_RECURSION_DEPTH))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarMaxSpRecursionDepth));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlSafeUpdates())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlSafeUpdates", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_SAFE_UPDATES))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlSafeUpdates));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarConcurrentInsert())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarConcurrentInsert", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_CONCURRENT_INSERT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarConcurrentInsert));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDefaultAuthenticationPlugin())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDefaultAuthenticationPlugin", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarDefaultAuthenticationPlugin));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDisabledStorageEngines())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDisabledStorageEngines", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_DISABLED_STORAGE_ENGINES))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarDisabledStorageEngines));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarErrorCount())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarErrorCount", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_ERROR_COUNT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarErrorCount));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarGeneralLog())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarGeneralLog", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_GENERAL_LOG))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarGeneralLog));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarHaveOpenssl())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarHaveOpenssl", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_HAVE_OPENSSL))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarHaveOpenssl));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarHaveProfiling())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarHaveProfiling", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_HAVE_PROFILING))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarHaveProfiling));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarHaveSsl())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarHaveSsl", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_HAVE_SSL))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarHaveSsl));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarHostname())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarHostname", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_HOSTNAME))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarHostname));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLcMessages())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLcMessages", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_LC_MESSAGES))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarLcMessages));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLocalInfile())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLocalInfile", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_LOCAL_INFILE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarLocalInfile));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLockWaitTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLockWaitTimeout", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_LOCK_WAIT_TIMEOUT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarLockWaitTimeout));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLongQueryTime())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLongQueryTime", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_LONG_QUERY_TIME))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarLongQueryTime));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarMaxConnections())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarMaxConnections", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_MAX_CONNECTIONS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarMaxConnections));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarMaxExecutionTime())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarMaxExecutionTime", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_MAX_EXECUTION_TIME))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarMaxExecutionTime));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarProtocolVersion())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarProtocolVersion", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_PROTOCOL_VERSION))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarProtocolVersion));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarServerId())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarServerId", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SERVER_ID))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarServerId));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslCa())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslCa", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SSL_CA))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSslCa));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslCapath())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslCapath", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SSL_CAPATH))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSslCapath));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslCert())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslCert", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SSL_CERT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSslCert));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslCipher())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslCipher", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SSL_CIPHER))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSslCipher));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslCrl())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslCrl", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SSL_CRL))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSslCrl));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslCrlpath())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslCrlpath", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SSL_CRLPATH))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSslCrlpath));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslKey())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslKey", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SSL_KEY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSslKey));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTimeFormat())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTimeFormat", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_TIME_FORMAT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarTimeFormat));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTlsVersion())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTlsVersion", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_TLS_VERSION))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarTlsVersion));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTmpTableSize())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTmpTableSize", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_TMP_TABLE_SIZE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarTmpTableSize));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTmpdir())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTmpdir", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_TMPDIR))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarTmpdir));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarUniqueChecks())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarUniqueChecks", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_UNIQUE_CHECKS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarUniqueChecks));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarVersionCompileMachine())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarVersionCompileMachine", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_VERSION_COMPILE_MACHINE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarVersionCompileMachine));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarVersionCompileOs())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarVersionCompileOs", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_VERSION_COMPILE_OS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarVersionCompileOs));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarWarningCount())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarWarningCount", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_WARNING_COUNT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarWarningCount));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSessionTrackSchema())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSessionTrackSchema", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SESSION_TRACK_SCHEMA))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSessionTrackSchema));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSessionTrackSystemVariables())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSessionTrackSystemVariables", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SESSION_TRACK_SYSTEM_VARIABLES))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSessionTrackSystemVariables));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSessionTrackStateChange())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSessionTrackStateChange", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SESSION_TRACK_STATE_CHANGE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSessionTrackStateChange));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarHaveQueryCache())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarHaveQueryCache", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_HAVE_QUERY_CACHE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarHaveQueryCache));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarQueryCacheLimit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarQueryCacheLimit", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_QUERY_CACHE_LIMIT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarQueryCacheLimit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarQueryCacheMinResUnit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarQueryCacheMinResUnit", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_QUERY_CACHE_MIN_RES_UNIT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarQueryCacheMinResUnit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarQueryCacheWlockInvalidate())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarQueryCacheWlockInvalidate", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_QUERY_CACHE_WLOCK_INVALIDATE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarQueryCacheWlockInvalidate));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarBinlogFormat())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarBinlogFormat", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_BINLOG_FORMAT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarBinlogFormat));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarBinlogChecksum())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarBinlogChecksum", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_BINLOG_CHECKSUM))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarBinlogChecksum));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarBinlogRowsQueryLogEvents())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarBinlogRowsQueryLogEvents", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_BINLOG_ROWS_QUERY_LOG_EVENTS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarBinlogRowsQueryLogEvents));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLogBin())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLogBin", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_LOG_BIN))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarLogBin));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarServerUuid())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarServerUuid", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SERVER_UUID))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarServerUuid));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDefaultStorageEngine())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDefaultStorageEngine", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_DEFAULT_STORAGE_ENGINE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarDefaultStorageEngine));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCteMaxRecursionDepth())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCteMaxRecursionDepth", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_CTE_MAX_RECURSION_DEPTH))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCteMaxRecursionDepth));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRegexpStackLimit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRegexpStackLimit", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_REGEXP_STACK_LIMIT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarRegexpStackLimit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRegexpTimeLimit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRegexpTimeLimit", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_REGEXP_TIME_LIMIT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarRegexpTimeLimit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObIntermResultMemLimit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObIntermResultMemLimit", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_INTERM_RESULT_MEM_LIMIT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObIntermResultMemLimit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObProxyPartitionHit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObProxyPartitionHit", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_PROXY_PARTITION_HIT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObProxyPartitionHit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObLogLevel())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObLogLevel", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_LOG_LEVEL))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObLogLevel));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObQueryTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObQueryTimeout", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_QUERY_TIMEOUT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObQueryTimeout));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObReadConsistency())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObReadConsistency", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_READ_CONSISTENCY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObReadConsistency));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableTransformation())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableTransformation", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_ENABLE_TRANSFORMATION))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObEnableTransformation));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObTrxTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObTrxTimeout", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_TRX_TIMEOUT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObTrxTimeout));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnablePlanCache())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnablePlanCache", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_ENABLE_PLAN_CACHE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObEnablePlanCache));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableIndexDirectSelect())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableIndexDirectSelect", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_ENABLE_INDEX_DIRECT_SELECT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObEnableIndexDirectSelect));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObProxySetTrxExecuted())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObProxySetTrxExecuted", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_PROXY_SET_TRX_EXECUTED))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObProxySetTrxExecuted));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableAggregationPushdown())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableAggregationPushdown", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_ENABLE_AGGREGATION_PUSHDOWN))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObEnableAggregationPushdown));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObLastSchemaVersion())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObLastSchemaVersion", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_LAST_SCHEMA_VERSION))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObLastSchemaVersion));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObGlobalDebugSync())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObGlobalDebugSync", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_GLOBAL_DEBUG_SYNC))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObGlobalDebugSync));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObProxyGlobalVariablesVersion())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObProxyGlobalVariablesVersion", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObProxyGlobalVariablesVersion));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableShowTrace())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableShowTrace", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_ENABLE_SHOW_TRACE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObEnableShowTrace));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObBnlJoinCacheSize())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObBnlJoinCacheSize", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_BNL_JOIN_CACHE_SIZE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObBnlJoinCacheSize));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObProxyUserPrivilege())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObProxyUserPrivilege", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_PROXY_USER_PRIVILEGE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObProxyUserPrivilege));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObOrgClusterId())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObOrgClusterId", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_ORG_CLUSTER_ID))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObOrgClusterId));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObPlanCachePercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObPlanCachePercentage", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_PLAN_CACHE_PERCENTAGE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObPlanCachePercentage));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObPlanCacheEvictHighPercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObPlanCacheEvictHighPercentage", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObPlanCacheEvictHighPercentage));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObPlanCacheEvictLowPercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObPlanCacheEvictLowPercentage", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObPlanCacheEvictLowPercentage));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRecyclebin())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRecyclebin", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_RECYCLEBIN))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarRecyclebin));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObCapabilityFlag())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObCapabilityFlag", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_CAPABILITY_FLAG))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObCapabilityFlag));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarIsResultAccurate())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarIsResultAccurate", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_IS_RESULT_ACCURATE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarIsResultAccurate));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarErrorOnOverlapTime())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarErrorOnOverlapTime", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_ERROR_ON_OVERLAP_TIME))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarErrorOnOverlapTime));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObCompatibilityMode())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObCompatibilityMode", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_COMPATIBILITY_MODE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObCompatibilityMode));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObSqlWorkAreaPercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObSqlWorkAreaPercentage", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObSqlWorkAreaPercentage));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObSafeWeakReadSnapshot())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObSafeWeakReadSnapshot", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObSafeWeakReadSnapshot));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObRoutePolicy())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObRoutePolicy", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_ROUTE_POLICY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObRoutePolicy));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableTransmissionChecksum())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableTransmissionChecksum", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_ENABLE_TRANSMISSION_CHECKSUM))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObEnableTransmissionChecksum));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarForeignKeyChecks())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarForeignKeyChecks", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_FOREIGN_KEY_CHECKS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarForeignKeyChecks));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObStatementTraceId())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObStatementTraceId", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_STATEMENT_TRACE_ID))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObStatementTraceId));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableTruncateFlashback())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableTruncateFlashback", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_ENABLE_TRUNCATE_FLASHBACK))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObEnableTruncateFlashback));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObTcpInvitedNodes())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObTcpInvitedNodes", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_TCP_INVITED_NODES))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObTcpInvitedNodes));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottleCurrentPriority())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottleCurrentPriority", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_CURRENT_PRIORITY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlThrottleCurrentPriority));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottlePriority())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottlePriority", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_PRIORITY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlThrottlePriority));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottleRt())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottleRt", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_RT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlThrottleRt));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottleCpu())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottleCpu", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_CPU))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlThrottleCpu));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottleIo())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottleIo", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_IO))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlThrottleIo));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottleNetwork())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottleNetwork", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_NETWORK))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlThrottleNetwork));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottleLogicalReads())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottleLogicalReads", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_LOGICAL_READS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlThrottleLogicalReads));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarAutoIncrementCacheSize())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarAutoIncrementCacheSize", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_AUTO_INCREMENT_CACHE_SIZE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarAutoIncrementCacheSize));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableJit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableJit", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_ENABLE_JIT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObEnableJit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObTempTablespaceSizePercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObTempTablespaceSizePercentage", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_TEMP_TABLESPACE_SIZE_PERCENTAGE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObTempTablespaceSizePercentage));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPluginDir())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPluginDir", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_PLUGIN_DIR))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarPluginDir));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObSqlAuditPercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObSqlAuditPercentage", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_SQL_AUDIT_PERCENTAGE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObSqlAuditPercentage));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableSqlAudit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableSqlAudit", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_ENABLE_SQL_AUDIT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObEnableSqlAudit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarOptimizerUseSqlPlanBaselines())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarOptimizerUseSqlPlanBaselines", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarOptimizerUseSqlPlanBaselines));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarOptimizerCaptureSqlPlanBaselines())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarOptimizerCaptureSqlPlanBaselines", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarOptimizerCaptureSqlPlanBaselines));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarParallelServersTarget())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarParallelServersTarget", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_PARALLEL_SERVERS_TARGET))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarParallelServersTarget));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEarlyLockRelease())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEarlyLockRelease", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_EARLY_LOCK_RELEASE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObEarlyLockRelease));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObTrxIdleTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObTrxIdleTimeout", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_TRX_IDLE_TIMEOUT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObTrxIdleTimeout));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarBlockEncryptionMode())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarBlockEncryptionMode", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_BLOCK_ENCRYPTION_MODE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarBlockEncryptionMode));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsDateFormat())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsDateFormat", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_DATE_FORMAT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsDateFormat));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsTimestampFormat())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsTimestampFormat", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_TIMESTAMP_FORMAT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsTimestampFormat));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsTimestampTzFormat())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsTimestampTzFormat", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsTimestampTzFormat));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObReservedMetaMemoryPercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObReservedMetaMemoryPercentage", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_RESERVED_META_MEMORY_PERCENTAGE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObReservedMetaMemoryPercentage));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObCheckSysVariable())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObCheckSysVariable", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_CHECK_SYS_VARIABLE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObCheckSysVariable));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsLanguage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsLanguage", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_LANGUAGE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsLanguage));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsTerritory())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsTerritory", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_TERRITORY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsTerritory));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsSort())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsSort", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_SORT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsSort));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsComp())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsComp", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_COMP))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsComp));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsCharacterset())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsCharacterset", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_CHARACTERSET))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsCharacterset));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsNcharCharacterset())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsNcharCharacterset", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_NCHAR_CHARACTERSET))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsNcharCharacterset));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsDateLanguage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsDateLanguage", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_DATE_LANGUAGE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsDateLanguage));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsLengthSemantics())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsLengthSemantics", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_LENGTH_SEMANTICS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsLengthSemantics));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsNcharConvExcp())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsNcharConvExcp", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_NCHAR_CONV_EXCP))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsNcharConvExcp));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsCalendar())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsCalendar", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_CALENDAR))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsCalendar));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsNumericCharacters())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsNumericCharacters", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_NUMERIC_CHARACTERS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsNumericCharacters));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNljBatchingEnabled())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNljBatchingEnabled", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__NLJ_BATCHING_ENABLED))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNljBatchingEnabled));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTracefileIdentifier())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTracefileIdentifier", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_TRACEFILE_IDENTIFIER))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarTracefileIdentifier));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarGroupbyNopushdownCutRatio())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarGroupbyNopushdownCutRatio", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarGroupbyNopushdownCutRatio));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPxBroadcastFudgeFactor())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPxBroadcastFudgeFactor", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__PX_BROADCAST_FUDGE_FACTOR))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarPxBroadcastFudgeFactor));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTransactionIsolation())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTransactionIsolation", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_TRANSACTION_ISOLATION))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarTransactionIsolation));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObTrxLockTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObTrxLockTimeout", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_TRX_LOCK_TIMEOUT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObTrxLockTimeout));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarValidatePasswordCheckUserName())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarValidatePasswordCheckUserName", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarValidatePasswordCheckUserName));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarValidatePasswordLength())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarValidatePasswordLength", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_LENGTH))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarValidatePasswordLength));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarValidatePasswordMixedCaseCount())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarValidatePasswordMixedCaseCount", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarValidatePasswordMixedCaseCount));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarValidatePasswordNumberCount())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarValidatePasswordNumberCount", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarValidatePasswordNumberCount));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarValidatePasswordPolicy())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarValidatePasswordPolicy", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_POLICY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarValidatePasswordPolicy));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarValidatePasswordSpecialCharCount())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarValidatePasswordSpecialCharCount", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarValidatePasswordSpecialCharCount));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDefaultPasswordLifetime())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDefaultPasswordLifetime", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_DEFAULT_PASSWORD_LIFETIME))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarDefaultPasswordLifetime));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObOlsPolicySessionLabels())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObOlsPolicySessionLabels", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__OB_OLS_POLICY_SESSION_LABELS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObOlsPolicySessionLabels));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObTraceInfo())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObTraceInfo", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_TRACE_INFO))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObTraceInfo));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPxPartitionScanThreshold())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPxPartitionScanThreshold", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__PX_PARTITION_SCAN_THRESHOLD))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarPxPartitionScanThreshold));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObPxBcastOptimization())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObPxBcastOptimization", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__OB_PX_BCAST_OPTIMIZATION))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObPxBcastOptimization));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObPxSlaveMappingThreshold())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObPxSlaveMappingThreshold", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__OB_PX_SLAVE_MAPPING_THRESHOLD))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObPxSlaveMappingThreshold));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarEnableParallelDml())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarEnableParallelDml", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__ENABLE_PARALLEL_DML))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarEnableParallelDml));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPxMinGranulesPerSlave())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPxMinGranulesPerSlave", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__PX_MIN_GRANULES_PER_SLAVE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarPxMinGranulesPerSlave));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSecureFilePriv())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSecureFilePriv", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SECURE_FILE_PRIV))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSecureFilePriv));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPlsqlWarnings())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPlsqlWarnings", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_PLSQL_WARNINGS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarPlsqlWarnings));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarEnableParallelQuery())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarEnableParallelQuery", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__ENABLE_PARALLEL_QUERY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarEnableParallelQuery));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarForceParallelQueryDop())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarForceParallelQueryDop", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__FORCE_PARALLEL_QUERY_DOP))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarForceParallelQueryDop));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarForceParallelDmlDop())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarForceParallelDmlDop", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__FORCE_PARALLEL_DML_DOP))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarForceParallelDmlDop));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObPlBlockTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObPlBlockTimeout", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_PL_BLOCK_TIMEOUT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObPlBlockTimeout));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTransactionReadOnly())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTransactionReadOnly", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_TRANSACTION_READ_ONLY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarTransactionReadOnly));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarResourceManagerPlan())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarResourceManagerPlan", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_RESOURCE_MANAGER_PLAN))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarResourceManagerPlan));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPerformanceSchema())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPerformanceSchema", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_PERFORMANCE_SCHEMA))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarPerformanceSchema));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsCurrency())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsCurrency", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_CURRENCY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsCurrency));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsIsoCurrency())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsIsoCurrency", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_ISO_CURRENCY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsIsoCurrency));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsDualCurrency())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsDualCurrency", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NLS_DUAL_CURRENCY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNlsDualCurrency));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPlsqlCcflags())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPlsqlCcflags", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_PLSQL_CCFLAGS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarPlsqlCcflags));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObProxySessionTemporaryTableUsed())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObProxySessionTemporaryTableUsed", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__OB_PROXY_SESSION_TEMPORARY_TABLE_USED))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObProxySessionTemporaryTableUsed));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarEnableParallelDdl())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarEnableParallelDdl", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__ENABLE_PARALLEL_DDL))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarEnableParallelDdl));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarForceParallelDdlDop())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarForceParallelDdlDop", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__FORCE_PARALLEL_DDL_DOP))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarForceParallelDdlDop));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCursorSharing())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCursorSharing", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_CURSOR_SHARING))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCursorSharing));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarOptimizerNullAwareAntijoin())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarOptimizerNullAwareAntijoin", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__OPTIMIZER_NULL_AWARE_ANTIJOIN))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarOptimizerNullAwareAntijoin));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPxPartialRollupPushdown())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPxPartialRollupPushdown", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__PX_PARTIAL_ROLLUP_PUSHDOWN))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarPxPartialRollupPushdown));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPxDistAggPartialRollupPushdown())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPxDistAggPartialRollupPushdown", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__PX_DIST_AGG_PARTIAL_ROLLUP_PUSHDOWN))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarPxDistAggPartialRollupPushdown));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCreateAuditPurgeJob())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCreateAuditPurgeJob", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__CREATE_AUDIT_PURGE_JOB))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarCreateAuditPurgeJob));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDropAuditPurgeJob())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDropAuditPurgeJob", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__DROP_AUDIT_PURGE_JOB))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarDropAuditPurgeJob));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSetPurgeJobInterval())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSetPurgeJobInterval", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__SET_PURGE_JOB_INTERVAL))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSetPurgeJobInterval));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSetPurgeJobStatus())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSetPurgeJobStatus", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__SET_PURGE_JOB_STATUS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSetPurgeJobStatus));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSetLastArchiveTimestamp())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSetLastArchiveTimestamp", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__SET_LAST_ARCHIVE_TIMESTAMP))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSetLastArchiveTimestamp));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarClearLastArchiveTimestamp())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarClearLastArchiveTimestamp", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__CLEAR_LAST_ARCHIVE_TIMESTAMP))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarClearLastArchiveTimestamp));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarAggregationOptimizationSettings())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarAggregationOptimizationSettings", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__AGGREGATION_OPTIMIZATION_SETTINGS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarAggregationOptimizationSettings));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPxSharedHashJoin())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPxSharedHashJoin", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__PX_SHARED_HASH_JOIN))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarPxSharedHashJoin));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlNotes())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlNotes", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_SQL_NOTES))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSqlNotes));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarInnodbStrictMode())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarInnodbStrictMode", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_INNODB_STRICT_MODE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarInnodbStrictMode));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarWindowfuncOptimizationSettings())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarWindowfuncOptimizationSettings", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__WINDOWFUNC_OPTIMIZATION_SETTINGS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarWindowfuncOptimizationSettings));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableRichErrorMsg())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableRichErrorMsg", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_ENABLE_RICH_ERROR_MSG))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObEnableRichErrorMsg));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLogRowValueOptions())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLogRowValueOptions", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_LOG_ROW_VALUE_OPTIONS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarLogRowValueOptions));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObMaxReadStaleTime())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObMaxReadStaleTime", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OB_MAX_READ_STALE_TIME))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObMaxReadStaleTime));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarOptimizerGatherStatsOnLoad())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarOptimizerGatherStatsOnLoad", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__OPTIMIZER_GATHER_STATS_ON_LOAD))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarOptimizerGatherStatsOnLoad));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSetReverseDblinkInfos())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSetReverseDblinkInfos", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__SET_REVERSE_DBLINK_INFOS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarSetReverseDblinkInfos));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarForceOrderPreserveSet())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarForceOrderPreserveSet", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__FORCE_ORDER_PRESERVE_SET))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarForceOrderPreserveSet));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarShowDdlInCompatMode())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarShowDdlInCompatMode", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__SHOW_DDL_IN_COMPAT_MODE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarShowDdlInCompatMode));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarParallelDegreePolicy())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarParallelDegreePolicy", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_PARALLEL_DEGREE_POLICY))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarParallelDegreePolicy));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarParallelDegreeLimit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarParallelDegreeLimit", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_PARALLEL_DEGREE_LIMIT))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarParallelDegreeLimit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarParallelMinScanTimeThreshold())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarParallelMinScanTimeThreshold", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_PARALLEL_MIN_SCAN_TIME_THRESHOLD))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarParallelMinScanTimeThreshold));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarOptimizerDynamicSampling())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarOptimizerDynamicSampling", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OPTIMIZER_DYNAMIC_SAMPLING))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarOptimizerDynamicSampling));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRuntimeFilterType())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRuntimeFilterType", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_RUNTIME_FILTER_TYPE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarRuntimeFilterType));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRuntimeFilterWaitTimeMs())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRuntimeFilterWaitTimeMs", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_RUNTIME_FILTER_WAIT_TIME_MS))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarRuntimeFilterWaitTimeMs));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRuntimeFilterMaxInNum())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRuntimeFilterMaxInNum", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_RUNTIME_FILTER_MAX_IN_NUM))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarRuntimeFilterMaxInNum));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRuntimeBloomFilterMaxSize())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRuntimeBloomFilterMaxSize", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_RUNTIME_BLOOM_FILTER_MAX_SIZE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarRuntimeBloomFilterMaxSize));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarOptimizerFeaturesEnable())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarOptimizerFeaturesEnable", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_OPTIMIZER_FEATURES_ENABLE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarOptimizerFeaturesEnable));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObProxyWeakreadFeedback())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObProxyWeakreadFeedback", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__OB_PROXY_WEAKREAD_FEEDBACK))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarObProxyWeakreadFeedback));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNcharacterSetConnection())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNcharacterSetConnection", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_NCHARACTER_SET_CONNECTION))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarNcharacterSetConnection));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarAutomaticSpPrivileges())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarAutomaticSpPrivileges", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_AUTOMATIC_SP_PRIVILEGES))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarAutomaticSpPrivileges));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPrivilegeFeaturesEnable())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPrivilegeFeaturesEnable", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_PRIVILEGE_FEATURES_ENABLE))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarPrivilegeFeaturesEnable));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPrivControl())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPrivControl", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__PRIV_CONTROL))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarPrivControl));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarEnableMysqlPlPrivCheck())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarEnableMysqlPlPrivCheck", K(ret));
      } else {
        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR__ENABLE_MYSQL_PL_PRIV_CHECK))] = sys_var_ptr;
        ptr = (void *)((char *)ptr + sizeof(ObSysVarEnableMysqlPlPrivCheck));
      }
    }

  }
  return ret;
}

int ObSysVarFactory::create_sys_var(ObIAllocator &allocator_, ObSysVarClassType sys_var_id,
                                        ObBasicSysVar *&sys_var_ptr)
{
  int ret = OB_SUCCESS;
  switch(sys_var_id) {
    case SYS_VAR_AUTO_INCREMENT_INCREMENT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarAutoIncrementIncrement)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarAutoIncrementIncrement)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarAutoIncrementIncrement())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarAutoIncrementIncrement", K(ret));
      }
      break;
    }
    case SYS_VAR_AUTO_INCREMENT_OFFSET: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarAutoIncrementOffset)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarAutoIncrementOffset)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarAutoIncrementOffset())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarAutoIncrementOffset", K(ret));
      }
      break;
    }
    case SYS_VAR_AUTOCOMMIT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarAutocommit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarAutocommit)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarAutocommit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarAutocommit", K(ret));
      }
      break;
    }
    case SYS_VAR_CHARACTER_SET_CLIENT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCharacterSetClient)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCharacterSetClient)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetClient())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetClient", K(ret));
      }
      break;
    }
    case SYS_VAR_CHARACTER_SET_CONNECTION: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCharacterSetConnection)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCharacterSetConnection)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetConnection())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetConnection", K(ret));
      }
      break;
    }
    case SYS_VAR_CHARACTER_SET_DATABASE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCharacterSetDatabase)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCharacterSetDatabase)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetDatabase())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetDatabase", K(ret));
      }
      break;
    }
    case SYS_VAR_CHARACTER_SET_RESULTS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCharacterSetResults)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCharacterSetResults)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetResults())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetResults", K(ret));
      }
      break;
    }
    case SYS_VAR_CHARACTER_SET_SERVER: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCharacterSetServer)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCharacterSetServer)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetServer())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetServer", K(ret));
      }
      break;
    }
    case SYS_VAR_CHARACTER_SET_SYSTEM: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCharacterSetSystem)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCharacterSetSystem)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetSystem())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetSystem", K(ret));
      }
      break;
    }
    case SYS_VAR_COLLATION_CONNECTION: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCollationConnection)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCollationConnection)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCollationConnection())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCollationConnection", K(ret));
      }
      break;
    }
    case SYS_VAR_COLLATION_DATABASE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCollationDatabase)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCollationDatabase)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCollationDatabase())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCollationDatabase", K(ret));
      }
      break;
    }
    case SYS_VAR_COLLATION_SERVER: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCollationServer)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCollationServer)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCollationServer())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCollationServer", K(ret));
      }
      break;
    }
    case SYS_VAR_INTERACTIVE_TIMEOUT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarInteractiveTimeout)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarInteractiveTimeout)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarInteractiveTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarInteractiveTimeout", K(ret));
      }
      break;
    }
    case SYS_VAR_LAST_INSERT_ID: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarLastInsertId)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarLastInsertId)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLastInsertId())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLastInsertId", K(ret));
      }
      break;
    }
    case SYS_VAR_MAX_ALLOWED_PACKET: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarMaxAllowedPacket)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarMaxAllowedPacket)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarMaxAllowedPacket())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarMaxAllowedPacket", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_MODE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlMode)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlMode)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlMode())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlMode", K(ret));
      }
      break;
    }
    case SYS_VAR_TIME_ZONE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarTimeZone)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarTimeZone)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTimeZone())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTimeZone", K(ret));
      }
      break;
    }
    case SYS_VAR_TX_ISOLATION: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarTxIsolation)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarTxIsolation)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTxIsolation())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTxIsolation", K(ret));
      }
      break;
    }
    case SYS_VAR_VERSION_COMMENT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarVersionComment)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarVersionComment)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarVersionComment())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarVersionComment", K(ret));
      }
      break;
    }
    case SYS_VAR_WAIT_TIMEOUT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarWaitTimeout)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarWaitTimeout)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarWaitTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarWaitTimeout", K(ret));
      }
      break;
    }
    case SYS_VAR_BINLOG_ROW_IMAGE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarBinlogRowImage)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarBinlogRowImage)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarBinlogRowImage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarBinlogRowImage", K(ret));
      }
      break;
    }
    case SYS_VAR_CHARACTER_SET_FILESYSTEM: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCharacterSetFilesystem)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCharacterSetFilesystem)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCharacterSetFilesystem())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCharacterSetFilesystem", K(ret));
      }
      break;
    }
    case SYS_VAR_CONNECT_TIMEOUT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarConnectTimeout)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarConnectTimeout)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarConnectTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarConnectTimeout", K(ret));
      }
      break;
    }
    case SYS_VAR_DATADIR: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarDatadir)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarDatadir)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDatadir())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDatadir", K(ret));
      }
      break;
    }
    case SYS_VAR_DEBUG_SYNC: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarDebugSync)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarDebugSync)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDebugSync())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDebugSync", K(ret));
      }
      break;
    }
    case SYS_VAR_DIV_PRECISION_INCREMENT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarDivPrecisionIncrement)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarDivPrecisionIncrement)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDivPrecisionIncrement())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDivPrecisionIncrement", K(ret));
      }
      break;
    }
    case SYS_VAR_EXPLICIT_DEFAULTS_FOR_TIMESTAMP: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarExplicitDefaultsForTimestamp)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarExplicitDefaultsForTimestamp)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarExplicitDefaultsForTimestamp())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarExplicitDefaultsForTimestamp", K(ret));
      }
      break;
    }
    case SYS_VAR_GROUP_CONCAT_MAX_LEN: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarGroupConcatMaxLen)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarGroupConcatMaxLen)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarGroupConcatMaxLen())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarGroupConcatMaxLen", K(ret));
      }
      break;
    }
    case SYS_VAR_IDENTITY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarIdentity)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarIdentity)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarIdentity())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarIdentity", K(ret));
      }
      break;
    }
    case SYS_VAR_LOWER_CASE_TABLE_NAMES: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarLowerCaseTableNames)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarLowerCaseTableNames)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLowerCaseTableNames())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLowerCaseTableNames", K(ret));
      }
      break;
    }
    case SYS_VAR_NET_READ_TIMEOUT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNetReadTimeout)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNetReadTimeout)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNetReadTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNetReadTimeout", K(ret));
      }
      break;
    }
    case SYS_VAR_NET_WRITE_TIMEOUT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNetWriteTimeout)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNetWriteTimeout)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNetWriteTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNetWriteTimeout", K(ret));
      }
      break;
    }
    case SYS_VAR_READ_ONLY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarReadOnly)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarReadOnly)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarReadOnly())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarReadOnly", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_AUTO_IS_NULL: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlAutoIsNull)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlAutoIsNull)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlAutoIsNull())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlAutoIsNull", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_SELECT_LIMIT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlSelectLimit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlSelectLimit)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlSelectLimit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlSelectLimit", K(ret));
      }
      break;
    }
    case SYS_VAR_TIMESTAMP: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarTimestamp)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarTimestamp)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTimestamp())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTimestamp", K(ret));
      }
      break;
    }
    case SYS_VAR_TX_READ_ONLY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarTxReadOnly)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarTxReadOnly)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTxReadOnly())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTxReadOnly", K(ret));
      }
      break;
    }
    case SYS_VAR_VERSION: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarVersion)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarVersion)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarVersion())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarVersion", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_WARNINGS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlWarnings)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlWarnings)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlWarnings())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlWarnings", K(ret));
      }
      break;
    }
    case SYS_VAR_MAX_USER_CONNECTIONS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarMaxUserConnections)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarMaxUserConnections)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarMaxUserConnections())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarMaxUserConnections", K(ret));
      }
      break;
    }
    case SYS_VAR_INIT_CONNECT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarInitConnect)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarInitConnect)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarInitConnect())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarInitConnect", K(ret));
      }
      break;
    }
    case SYS_VAR_LICENSE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarLicense)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarLicense)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLicense())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLicense", K(ret));
      }
      break;
    }
    case SYS_VAR_NET_BUFFER_LENGTH: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNetBufferLength)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNetBufferLength)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNetBufferLength())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNetBufferLength", K(ret));
      }
      break;
    }
    case SYS_VAR_SYSTEM_TIME_ZONE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSystemTimeZone)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSystemTimeZone)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSystemTimeZone())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSystemTimeZone", K(ret));
      }
      break;
    }
    case SYS_VAR_QUERY_CACHE_SIZE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarQueryCacheSize)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarQueryCacheSize)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarQueryCacheSize())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarQueryCacheSize", K(ret));
      }
      break;
    }
    case SYS_VAR_QUERY_CACHE_TYPE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarQueryCacheType)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarQueryCacheType)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarQueryCacheType())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarQueryCacheType", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_QUOTE_SHOW_CREATE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlQuoteShowCreate)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlQuoteShowCreate)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlQuoteShowCreate())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlQuoteShowCreate", K(ret));
      }
      break;
    }
    case SYS_VAR_MAX_SP_RECURSION_DEPTH: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarMaxSpRecursionDepth)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarMaxSpRecursionDepth)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarMaxSpRecursionDepth())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarMaxSpRecursionDepth", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_SAFE_UPDATES: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlSafeUpdates)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlSafeUpdates)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlSafeUpdates())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlSafeUpdates", K(ret));
      }
      break;
    }
    case SYS_VAR_CONCURRENT_INSERT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarConcurrentInsert)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarConcurrentInsert)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarConcurrentInsert())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarConcurrentInsert", K(ret));
      }
      break;
    }
    case SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarDefaultAuthenticationPlugin)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarDefaultAuthenticationPlugin)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDefaultAuthenticationPlugin())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDefaultAuthenticationPlugin", K(ret));
      }
      break;
    }
    case SYS_VAR_DISABLED_STORAGE_ENGINES: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarDisabledStorageEngines)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarDisabledStorageEngines)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDisabledStorageEngines())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDisabledStorageEngines", K(ret));
      }
      break;
    }
    case SYS_VAR_ERROR_COUNT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarErrorCount)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarErrorCount)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarErrorCount())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarErrorCount", K(ret));
      }
      break;
    }
    case SYS_VAR_GENERAL_LOG: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarGeneralLog)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarGeneralLog)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarGeneralLog())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarGeneralLog", K(ret));
      }
      break;
    }
    case SYS_VAR_HAVE_OPENSSL: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarHaveOpenssl)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarHaveOpenssl)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarHaveOpenssl())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarHaveOpenssl", K(ret));
      }
      break;
    }
    case SYS_VAR_HAVE_PROFILING: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarHaveProfiling)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarHaveProfiling)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarHaveProfiling())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarHaveProfiling", K(ret));
      }
      break;
    }
    case SYS_VAR_HAVE_SSL: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarHaveSsl)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarHaveSsl)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarHaveSsl())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarHaveSsl", K(ret));
      }
      break;
    }
    case SYS_VAR_HOSTNAME: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarHostname)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarHostname)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarHostname())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarHostname", K(ret));
      }
      break;
    }
    case SYS_VAR_LC_MESSAGES: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarLcMessages)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarLcMessages)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLcMessages())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLcMessages", K(ret));
      }
      break;
    }
    case SYS_VAR_LOCAL_INFILE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarLocalInfile)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarLocalInfile)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLocalInfile())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLocalInfile", K(ret));
      }
      break;
    }
    case SYS_VAR_LOCK_WAIT_TIMEOUT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarLockWaitTimeout)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarLockWaitTimeout)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLockWaitTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLockWaitTimeout", K(ret));
      }
      break;
    }
    case SYS_VAR_LONG_QUERY_TIME: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarLongQueryTime)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarLongQueryTime)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLongQueryTime())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLongQueryTime", K(ret));
      }
      break;
    }
    case SYS_VAR_MAX_CONNECTIONS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarMaxConnections)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarMaxConnections)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarMaxConnections())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarMaxConnections", K(ret));
      }
      break;
    }
    case SYS_VAR_MAX_EXECUTION_TIME: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarMaxExecutionTime)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarMaxExecutionTime)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarMaxExecutionTime())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarMaxExecutionTime", K(ret));
      }
      break;
    }
    case SYS_VAR_PROTOCOL_VERSION: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarProtocolVersion)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarProtocolVersion)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarProtocolVersion())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarProtocolVersion", K(ret));
      }
      break;
    }
    case SYS_VAR_SERVER_ID: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarServerId)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarServerId)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarServerId())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarServerId", K(ret));
      }
      break;
    }
    case SYS_VAR_SSL_CA: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSslCa)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSslCa)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslCa())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslCa", K(ret));
      }
      break;
    }
    case SYS_VAR_SSL_CAPATH: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSslCapath)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSslCapath)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslCapath())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslCapath", K(ret));
      }
      break;
    }
    case SYS_VAR_SSL_CERT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSslCert)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSslCert)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslCert())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslCert", K(ret));
      }
      break;
    }
    case SYS_VAR_SSL_CIPHER: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSslCipher)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSslCipher)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslCipher())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslCipher", K(ret));
      }
      break;
    }
    case SYS_VAR_SSL_CRL: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSslCrl)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSslCrl)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslCrl())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslCrl", K(ret));
      }
      break;
    }
    case SYS_VAR_SSL_CRLPATH: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSslCrlpath)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSslCrlpath)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslCrlpath())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslCrlpath", K(ret));
      }
      break;
    }
    case SYS_VAR_SSL_KEY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSslKey)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSslKey)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSslKey())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSslKey", K(ret));
      }
      break;
    }
    case SYS_VAR_TIME_FORMAT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarTimeFormat)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarTimeFormat)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTimeFormat())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTimeFormat", K(ret));
      }
      break;
    }
    case SYS_VAR_TLS_VERSION: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarTlsVersion)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarTlsVersion)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTlsVersion())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTlsVersion", K(ret));
      }
      break;
    }
    case SYS_VAR_TMP_TABLE_SIZE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarTmpTableSize)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarTmpTableSize)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTmpTableSize())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTmpTableSize", K(ret));
      }
      break;
    }
    case SYS_VAR_TMPDIR: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarTmpdir)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarTmpdir)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTmpdir())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTmpdir", K(ret));
      }
      break;
    }
    case SYS_VAR_UNIQUE_CHECKS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarUniqueChecks)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarUniqueChecks)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarUniqueChecks())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarUniqueChecks", K(ret));
      }
      break;
    }
    case SYS_VAR_VERSION_COMPILE_MACHINE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarVersionCompileMachine)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarVersionCompileMachine)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarVersionCompileMachine())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarVersionCompileMachine", K(ret));
      }
      break;
    }
    case SYS_VAR_VERSION_COMPILE_OS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarVersionCompileOs)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarVersionCompileOs)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarVersionCompileOs())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarVersionCompileOs", K(ret));
      }
      break;
    }
    case SYS_VAR_WARNING_COUNT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarWarningCount)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarWarningCount)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarWarningCount())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarWarningCount", K(ret));
      }
      break;
    }
    case SYS_VAR_SESSION_TRACK_SCHEMA: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSessionTrackSchema)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSessionTrackSchema)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSessionTrackSchema())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSessionTrackSchema", K(ret));
      }
      break;
    }
    case SYS_VAR_SESSION_TRACK_SYSTEM_VARIABLES: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSessionTrackSystemVariables)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSessionTrackSystemVariables)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSessionTrackSystemVariables())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSessionTrackSystemVariables", K(ret));
      }
      break;
    }
    case SYS_VAR_SESSION_TRACK_STATE_CHANGE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSessionTrackStateChange)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSessionTrackStateChange)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSessionTrackStateChange())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSessionTrackStateChange", K(ret));
      }
      break;
    }
    case SYS_VAR_HAVE_QUERY_CACHE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarHaveQueryCache)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarHaveQueryCache)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarHaveQueryCache())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarHaveQueryCache", K(ret));
      }
      break;
    }
    case SYS_VAR_QUERY_CACHE_LIMIT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarQueryCacheLimit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarQueryCacheLimit)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarQueryCacheLimit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarQueryCacheLimit", K(ret));
      }
      break;
    }
    case SYS_VAR_QUERY_CACHE_MIN_RES_UNIT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarQueryCacheMinResUnit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarQueryCacheMinResUnit)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarQueryCacheMinResUnit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarQueryCacheMinResUnit", K(ret));
      }
      break;
    }
    case SYS_VAR_QUERY_CACHE_WLOCK_INVALIDATE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarQueryCacheWlockInvalidate)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarQueryCacheWlockInvalidate)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarQueryCacheWlockInvalidate())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarQueryCacheWlockInvalidate", K(ret));
      }
      break;
    }
    case SYS_VAR_BINLOG_FORMAT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarBinlogFormat)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarBinlogFormat)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarBinlogFormat())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarBinlogFormat", K(ret));
      }
      break;
    }
    case SYS_VAR_BINLOG_CHECKSUM: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarBinlogChecksum)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarBinlogChecksum)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarBinlogChecksum())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarBinlogChecksum", K(ret));
      }
      break;
    }
    case SYS_VAR_BINLOG_ROWS_QUERY_LOG_EVENTS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarBinlogRowsQueryLogEvents)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarBinlogRowsQueryLogEvents)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarBinlogRowsQueryLogEvents())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarBinlogRowsQueryLogEvents", K(ret));
      }
      break;
    }
    case SYS_VAR_LOG_BIN: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarLogBin)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarLogBin)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLogBin())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLogBin", K(ret));
      }
      break;
    }
    case SYS_VAR_SERVER_UUID: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarServerUuid)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarServerUuid)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarServerUuid())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarServerUuid", K(ret));
      }
      break;
    }
    case SYS_VAR_DEFAULT_STORAGE_ENGINE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarDefaultStorageEngine)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarDefaultStorageEngine)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDefaultStorageEngine())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDefaultStorageEngine", K(ret));
      }
      break;
    }
    case SYS_VAR_CTE_MAX_RECURSION_DEPTH: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCteMaxRecursionDepth)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCteMaxRecursionDepth)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCteMaxRecursionDepth())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCteMaxRecursionDepth", K(ret));
      }
      break;
    }
    case SYS_VAR_REGEXP_STACK_LIMIT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarRegexpStackLimit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarRegexpStackLimit)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRegexpStackLimit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRegexpStackLimit", K(ret));
      }
      break;
    }
    case SYS_VAR_REGEXP_TIME_LIMIT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarRegexpTimeLimit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarRegexpTimeLimit)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRegexpTimeLimit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRegexpTimeLimit", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_INTERM_RESULT_MEM_LIMIT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObIntermResultMemLimit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObIntermResultMemLimit)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObIntermResultMemLimit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObIntermResultMemLimit", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_PROXY_PARTITION_HIT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObProxyPartitionHit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObProxyPartitionHit)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObProxyPartitionHit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObProxyPartitionHit", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_LOG_LEVEL: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObLogLevel)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObLogLevel)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObLogLevel())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObLogLevel", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_QUERY_TIMEOUT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObQueryTimeout)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObQueryTimeout)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObQueryTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObQueryTimeout", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_READ_CONSISTENCY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObReadConsistency)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObReadConsistency)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObReadConsistency())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObReadConsistency", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_ENABLE_TRANSFORMATION: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObEnableTransformation)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObEnableTransformation)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableTransformation())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableTransformation", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_TRX_TIMEOUT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObTrxTimeout)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObTrxTimeout)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObTrxTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObTrxTimeout", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_ENABLE_PLAN_CACHE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObEnablePlanCache)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObEnablePlanCache)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnablePlanCache())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnablePlanCache", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_ENABLE_INDEX_DIRECT_SELECT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObEnableIndexDirectSelect)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObEnableIndexDirectSelect)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableIndexDirectSelect())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableIndexDirectSelect", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_PROXY_SET_TRX_EXECUTED: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObProxySetTrxExecuted)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObProxySetTrxExecuted)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObProxySetTrxExecuted())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObProxySetTrxExecuted", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_ENABLE_AGGREGATION_PUSHDOWN: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObEnableAggregationPushdown)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObEnableAggregationPushdown)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableAggregationPushdown())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableAggregationPushdown", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_LAST_SCHEMA_VERSION: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObLastSchemaVersion)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObLastSchemaVersion)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObLastSchemaVersion())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObLastSchemaVersion", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_GLOBAL_DEBUG_SYNC: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObGlobalDebugSync)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObGlobalDebugSync)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObGlobalDebugSync())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObGlobalDebugSync", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObProxyGlobalVariablesVersion)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObProxyGlobalVariablesVersion)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObProxyGlobalVariablesVersion())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObProxyGlobalVariablesVersion", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_ENABLE_SHOW_TRACE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObEnableShowTrace)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObEnableShowTrace)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableShowTrace())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableShowTrace", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_BNL_JOIN_CACHE_SIZE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObBnlJoinCacheSize)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObBnlJoinCacheSize)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObBnlJoinCacheSize())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObBnlJoinCacheSize", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_PROXY_USER_PRIVILEGE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObProxyUserPrivilege)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObProxyUserPrivilege)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObProxyUserPrivilege())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObProxyUserPrivilege", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_ORG_CLUSTER_ID: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObOrgClusterId)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObOrgClusterId)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObOrgClusterId())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObOrgClusterId", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_PLAN_CACHE_PERCENTAGE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObPlanCachePercentage)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObPlanCachePercentage)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObPlanCachePercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObPlanCachePercentage", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObPlanCacheEvictHighPercentage)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObPlanCacheEvictHighPercentage)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObPlanCacheEvictHighPercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObPlanCacheEvictHighPercentage", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObPlanCacheEvictLowPercentage)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObPlanCacheEvictLowPercentage)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObPlanCacheEvictLowPercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObPlanCacheEvictLowPercentage", K(ret));
      }
      break;
    }
    case SYS_VAR_RECYCLEBIN: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarRecyclebin)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarRecyclebin)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRecyclebin())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRecyclebin", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_CAPABILITY_FLAG: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObCapabilityFlag)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObCapabilityFlag)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObCapabilityFlag())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObCapabilityFlag", K(ret));
      }
      break;
    }
    case SYS_VAR_IS_RESULT_ACCURATE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarIsResultAccurate)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarIsResultAccurate)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarIsResultAccurate())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarIsResultAccurate", K(ret));
      }
      break;
    }
    case SYS_VAR_ERROR_ON_OVERLAP_TIME: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarErrorOnOverlapTime)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarErrorOnOverlapTime)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarErrorOnOverlapTime())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarErrorOnOverlapTime", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_COMPATIBILITY_MODE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObCompatibilityMode)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObCompatibilityMode)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObCompatibilityMode())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObCompatibilityMode", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObSqlWorkAreaPercentage)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObSqlWorkAreaPercentage)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObSqlWorkAreaPercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObSqlWorkAreaPercentage", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObSafeWeakReadSnapshot)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObSafeWeakReadSnapshot)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObSafeWeakReadSnapshot())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObSafeWeakReadSnapshot", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_ROUTE_POLICY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObRoutePolicy)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObRoutePolicy)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObRoutePolicy())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObRoutePolicy", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_ENABLE_TRANSMISSION_CHECKSUM: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObEnableTransmissionChecksum)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObEnableTransmissionChecksum)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableTransmissionChecksum())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableTransmissionChecksum", K(ret));
      }
      break;
    }
    case SYS_VAR_FOREIGN_KEY_CHECKS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarForeignKeyChecks)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarForeignKeyChecks)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarForeignKeyChecks())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarForeignKeyChecks", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_STATEMENT_TRACE_ID: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObStatementTraceId)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObStatementTraceId)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObStatementTraceId())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObStatementTraceId", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_ENABLE_TRUNCATE_FLASHBACK: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObEnableTruncateFlashback)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObEnableTruncateFlashback)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableTruncateFlashback())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableTruncateFlashback", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_TCP_INVITED_NODES: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObTcpInvitedNodes)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObTcpInvitedNodes)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObTcpInvitedNodes())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObTcpInvitedNodes", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_THROTTLE_CURRENT_PRIORITY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlThrottleCurrentPriority)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlThrottleCurrentPriority)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottleCurrentPriority())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottleCurrentPriority", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_THROTTLE_PRIORITY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlThrottlePriority)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlThrottlePriority)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottlePriority())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottlePriority", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_THROTTLE_RT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlThrottleRt)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlThrottleRt)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottleRt())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottleRt", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_THROTTLE_CPU: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlThrottleCpu)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlThrottleCpu)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottleCpu())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottleCpu", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_THROTTLE_IO: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlThrottleIo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlThrottleIo)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottleIo())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottleIo", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_THROTTLE_NETWORK: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlThrottleNetwork)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlThrottleNetwork)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottleNetwork())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottleNetwork", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_THROTTLE_LOGICAL_READS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlThrottleLogicalReads)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlThrottleLogicalReads)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlThrottleLogicalReads())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlThrottleLogicalReads", K(ret));
      }
      break;
    }
    case SYS_VAR_AUTO_INCREMENT_CACHE_SIZE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarAutoIncrementCacheSize)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarAutoIncrementCacheSize)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarAutoIncrementCacheSize())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarAutoIncrementCacheSize", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_ENABLE_JIT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObEnableJit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObEnableJit)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableJit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableJit", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_TEMP_TABLESPACE_SIZE_PERCENTAGE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObTempTablespaceSizePercentage)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObTempTablespaceSizePercentage)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObTempTablespaceSizePercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObTempTablespaceSizePercentage", K(ret));
      }
      break;
    }
    case SYS_VAR_PLUGIN_DIR: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarPluginDir)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarPluginDir)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPluginDir())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPluginDir", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_SQL_AUDIT_PERCENTAGE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObSqlAuditPercentage)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObSqlAuditPercentage)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObSqlAuditPercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObSqlAuditPercentage", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_ENABLE_SQL_AUDIT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObEnableSqlAudit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObEnableSqlAudit)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableSqlAudit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableSqlAudit", K(ret));
      }
      break;
    }
    case SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarOptimizerUseSqlPlanBaselines)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarOptimizerUseSqlPlanBaselines)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarOptimizerUseSqlPlanBaselines())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarOptimizerUseSqlPlanBaselines", K(ret));
      }
      break;
    }
    case SYS_VAR_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarOptimizerCaptureSqlPlanBaselines)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarOptimizerCaptureSqlPlanBaselines)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarOptimizerCaptureSqlPlanBaselines())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarOptimizerCaptureSqlPlanBaselines", K(ret));
      }
      break;
    }
    case SYS_VAR_PARALLEL_SERVERS_TARGET: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarParallelServersTarget)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarParallelServersTarget)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarParallelServersTarget())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarParallelServersTarget", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_EARLY_LOCK_RELEASE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObEarlyLockRelease)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObEarlyLockRelease)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEarlyLockRelease())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEarlyLockRelease", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_TRX_IDLE_TIMEOUT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObTrxIdleTimeout)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObTrxIdleTimeout)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObTrxIdleTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObTrxIdleTimeout", K(ret));
      }
      break;
    }
    case SYS_VAR_BLOCK_ENCRYPTION_MODE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarBlockEncryptionMode)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarBlockEncryptionMode)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarBlockEncryptionMode())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarBlockEncryptionMode", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_DATE_FORMAT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsDateFormat)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsDateFormat)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsDateFormat())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsDateFormat", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_TIMESTAMP_FORMAT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsTimestampFormat)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsTimestampFormat)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsTimestampFormat())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsTimestampFormat", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsTimestampTzFormat)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsTimestampTzFormat)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsTimestampTzFormat())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsTimestampTzFormat", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_RESERVED_META_MEMORY_PERCENTAGE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObReservedMetaMemoryPercentage)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObReservedMetaMemoryPercentage)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObReservedMetaMemoryPercentage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObReservedMetaMemoryPercentage", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_CHECK_SYS_VARIABLE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObCheckSysVariable)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObCheckSysVariable)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObCheckSysVariable())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObCheckSysVariable", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_LANGUAGE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsLanguage)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsLanguage)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsLanguage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsLanguage", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_TERRITORY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsTerritory)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsTerritory)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsTerritory())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsTerritory", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_SORT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsSort)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsSort)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsSort())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsSort", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_COMP: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsComp)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsComp)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsComp())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsComp", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_CHARACTERSET: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsCharacterset)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsCharacterset)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsCharacterset())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsCharacterset", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_NCHAR_CHARACTERSET: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsNcharCharacterset)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsNcharCharacterset)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsNcharCharacterset())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsNcharCharacterset", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_DATE_LANGUAGE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsDateLanguage)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsDateLanguage)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsDateLanguage())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsDateLanguage", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_LENGTH_SEMANTICS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsLengthSemantics)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsLengthSemantics)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsLengthSemantics())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsLengthSemantics", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_NCHAR_CONV_EXCP: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsNcharConvExcp)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsNcharConvExcp)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsNcharConvExcp())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsNcharConvExcp", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_CALENDAR: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsCalendar)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsCalendar)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsCalendar())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsCalendar", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_NUMERIC_CHARACTERS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsNumericCharacters)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsNumericCharacters)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsNumericCharacters())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsNumericCharacters", K(ret));
      }
      break;
    }
    case SYS_VAR__NLJ_BATCHING_ENABLED: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNljBatchingEnabled)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNljBatchingEnabled)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNljBatchingEnabled())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNljBatchingEnabled", K(ret));
      }
      break;
    }
    case SYS_VAR_TRACEFILE_IDENTIFIER: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarTracefileIdentifier)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarTracefileIdentifier)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTracefileIdentifier())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTracefileIdentifier", K(ret));
      }
      break;
    }
    case SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarGroupbyNopushdownCutRatio)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarGroupbyNopushdownCutRatio)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarGroupbyNopushdownCutRatio())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarGroupbyNopushdownCutRatio", K(ret));
      }
      break;
    }
    case SYS_VAR__PX_BROADCAST_FUDGE_FACTOR: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarPxBroadcastFudgeFactor)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarPxBroadcastFudgeFactor)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPxBroadcastFudgeFactor())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPxBroadcastFudgeFactor", K(ret));
      }
      break;
    }
    case SYS_VAR_TRANSACTION_ISOLATION: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarTransactionIsolation)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarTransactionIsolation)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTransactionIsolation())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTransactionIsolation", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_TRX_LOCK_TIMEOUT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObTrxLockTimeout)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObTrxLockTimeout)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObTrxLockTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObTrxLockTimeout", K(ret));
      }
      break;
    }
    case SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarValidatePasswordCheckUserName)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarValidatePasswordCheckUserName)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarValidatePasswordCheckUserName())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarValidatePasswordCheckUserName", K(ret));
      }
      break;
    }
    case SYS_VAR_VALIDATE_PASSWORD_LENGTH: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarValidatePasswordLength)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarValidatePasswordLength)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarValidatePasswordLength())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarValidatePasswordLength", K(ret));
      }
      break;
    }
    case SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarValidatePasswordMixedCaseCount)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarValidatePasswordMixedCaseCount)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarValidatePasswordMixedCaseCount())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarValidatePasswordMixedCaseCount", K(ret));
      }
      break;
    }
    case SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarValidatePasswordNumberCount)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarValidatePasswordNumberCount)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarValidatePasswordNumberCount())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarValidatePasswordNumberCount", K(ret));
      }
      break;
    }
    case SYS_VAR_VALIDATE_PASSWORD_POLICY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarValidatePasswordPolicy)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarValidatePasswordPolicy)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarValidatePasswordPolicy())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarValidatePasswordPolicy", K(ret));
      }
      break;
    }
    case SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarValidatePasswordSpecialCharCount)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarValidatePasswordSpecialCharCount)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarValidatePasswordSpecialCharCount())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarValidatePasswordSpecialCharCount", K(ret));
      }
      break;
    }
    case SYS_VAR_DEFAULT_PASSWORD_LIFETIME: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarDefaultPasswordLifetime)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarDefaultPasswordLifetime)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDefaultPasswordLifetime())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDefaultPasswordLifetime", K(ret));
      }
      break;
    }
    case SYS_VAR__OB_OLS_POLICY_SESSION_LABELS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObOlsPolicySessionLabels)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObOlsPolicySessionLabels)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObOlsPolicySessionLabels())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObOlsPolicySessionLabels", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_TRACE_INFO: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObTraceInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObTraceInfo)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObTraceInfo())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObTraceInfo", K(ret));
      }
      break;
    }
    case SYS_VAR__PX_PARTITION_SCAN_THRESHOLD: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarPxPartitionScanThreshold)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarPxPartitionScanThreshold)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPxPartitionScanThreshold())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPxPartitionScanThreshold", K(ret));
      }
      break;
    }
    case SYS_VAR__OB_PX_BCAST_OPTIMIZATION: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObPxBcastOptimization)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObPxBcastOptimization)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObPxBcastOptimization())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObPxBcastOptimization", K(ret));
      }
      break;
    }
    case SYS_VAR__OB_PX_SLAVE_MAPPING_THRESHOLD: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObPxSlaveMappingThreshold)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObPxSlaveMappingThreshold)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObPxSlaveMappingThreshold())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObPxSlaveMappingThreshold", K(ret));
      }
      break;
    }
    case SYS_VAR__ENABLE_PARALLEL_DML: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarEnableParallelDml)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarEnableParallelDml)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarEnableParallelDml())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarEnableParallelDml", K(ret));
      }
      break;
    }
    case SYS_VAR__PX_MIN_GRANULES_PER_SLAVE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarPxMinGranulesPerSlave)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarPxMinGranulesPerSlave)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPxMinGranulesPerSlave())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPxMinGranulesPerSlave", K(ret));
      }
      break;
    }
    case SYS_VAR_SECURE_FILE_PRIV: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSecureFilePriv)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSecureFilePriv)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSecureFilePriv())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSecureFilePriv", K(ret));
      }
      break;
    }
    case SYS_VAR_PLSQL_WARNINGS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarPlsqlWarnings)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarPlsqlWarnings)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPlsqlWarnings())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPlsqlWarnings", K(ret));
      }
      break;
    }
    case SYS_VAR__ENABLE_PARALLEL_QUERY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarEnableParallelQuery)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarEnableParallelQuery)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarEnableParallelQuery())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarEnableParallelQuery", K(ret));
      }
      break;
    }
    case SYS_VAR__FORCE_PARALLEL_QUERY_DOP: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarForceParallelQueryDop)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarForceParallelQueryDop)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarForceParallelQueryDop())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarForceParallelQueryDop", K(ret));
      }
      break;
    }
    case SYS_VAR__FORCE_PARALLEL_DML_DOP: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarForceParallelDmlDop)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarForceParallelDmlDop)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarForceParallelDmlDop())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarForceParallelDmlDop", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_PL_BLOCK_TIMEOUT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObPlBlockTimeout)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObPlBlockTimeout)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObPlBlockTimeout())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObPlBlockTimeout", K(ret));
      }
      break;
    }
    case SYS_VAR_TRANSACTION_READ_ONLY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarTransactionReadOnly)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarTransactionReadOnly)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarTransactionReadOnly())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarTransactionReadOnly", K(ret));
      }
      break;
    }
    case SYS_VAR_RESOURCE_MANAGER_PLAN: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarResourceManagerPlan)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarResourceManagerPlan)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarResourceManagerPlan())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarResourceManagerPlan", K(ret));
      }
      break;
    }
    case SYS_VAR_PERFORMANCE_SCHEMA: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarPerformanceSchema)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarPerformanceSchema)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPerformanceSchema())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPerformanceSchema", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_CURRENCY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsCurrency)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsCurrency)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsCurrency())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsCurrency", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_ISO_CURRENCY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsIsoCurrency)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsIsoCurrency)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsIsoCurrency())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsIsoCurrency", K(ret));
      }
      break;
    }
    case SYS_VAR_NLS_DUAL_CURRENCY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNlsDualCurrency)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNlsDualCurrency)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNlsDualCurrency())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNlsDualCurrency", K(ret));
      }
      break;
    }
    case SYS_VAR_PLSQL_CCFLAGS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarPlsqlCcflags)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarPlsqlCcflags)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPlsqlCcflags())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPlsqlCcflags", K(ret));
      }
      break;
    }
    case SYS_VAR__OB_PROXY_SESSION_TEMPORARY_TABLE_USED: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObProxySessionTemporaryTableUsed)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObProxySessionTemporaryTableUsed)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObProxySessionTemporaryTableUsed())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObProxySessionTemporaryTableUsed", K(ret));
      }
      break;
    }
    case SYS_VAR__ENABLE_PARALLEL_DDL: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarEnableParallelDdl)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarEnableParallelDdl)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarEnableParallelDdl())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarEnableParallelDdl", K(ret));
      }
      break;
    }
    case SYS_VAR__FORCE_PARALLEL_DDL_DOP: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarForceParallelDdlDop)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarForceParallelDdlDop)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarForceParallelDdlDop())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarForceParallelDdlDop", K(ret));
      }
      break;
    }
    case SYS_VAR_CURSOR_SHARING: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCursorSharing)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCursorSharing)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCursorSharing())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCursorSharing", K(ret));
      }
      break;
    }
    case SYS_VAR__OPTIMIZER_NULL_AWARE_ANTIJOIN: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarOptimizerNullAwareAntijoin)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarOptimizerNullAwareAntijoin)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarOptimizerNullAwareAntijoin())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarOptimizerNullAwareAntijoin", K(ret));
      }
      break;
    }
    case SYS_VAR__PX_PARTIAL_ROLLUP_PUSHDOWN: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarPxPartialRollupPushdown)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarPxPartialRollupPushdown)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPxPartialRollupPushdown())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPxPartialRollupPushdown", K(ret));
      }
      break;
    }
    case SYS_VAR__PX_DIST_AGG_PARTIAL_ROLLUP_PUSHDOWN: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarPxDistAggPartialRollupPushdown)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarPxDistAggPartialRollupPushdown)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPxDistAggPartialRollupPushdown())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPxDistAggPartialRollupPushdown", K(ret));
      }
      break;
    }
    case SYS_VAR__CREATE_AUDIT_PURGE_JOB: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarCreateAuditPurgeJob)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarCreateAuditPurgeJob)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarCreateAuditPurgeJob())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarCreateAuditPurgeJob", K(ret));
      }
      break;
    }
    case SYS_VAR__DROP_AUDIT_PURGE_JOB: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarDropAuditPurgeJob)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarDropAuditPurgeJob)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarDropAuditPurgeJob())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarDropAuditPurgeJob", K(ret));
      }
      break;
    }
    case SYS_VAR__SET_PURGE_JOB_INTERVAL: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSetPurgeJobInterval)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSetPurgeJobInterval)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSetPurgeJobInterval())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSetPurgeJobInterval", K(ret));
      }
      break;
    }
    case SYS_VAR__SET_PURGE_JOB_STATUS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSetPurgeJobStatus)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSetPurgeJobStatus)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSetPurgeJobStatus())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSetPurgeJobStatus", K(ret));
      }
      break;
    }
    case SYS_VAR__SET_LAST_ARCHIVE_TIMESTAMP: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSetLastArchiveTimestamp)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSetLastArchiveTimestamp)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSetLastArchiveTimestamp())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSetLastArchiveTimestamp", K(ret));
      }
      break;
    }
    case SYS_VAR__CLEAR_LAST_ARCHIVE_TIMESTAMP: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarClearLastArchiveTimestamp)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarClearLastArchiveTimestamp)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarClearLastArchiveTimestamp())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarClearLastArchiveTimestamp", K(ret));
      }
      break;
    }
    case SYS_VAR__AGGREGATION_OPTIMIZATION_SETTINGS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarAggregationOptimizationSettings)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarAggregationOptimizationSettings)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarAggregationOptimizationSettings())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarAggregationOptimizationSettings", K(ret));
      }
      break;
    }
    case SYS_VAR__PX_SHARED_HASH_JOIN: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarPxSharedHashJoin)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarPxSharedHashJoin)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPxSharedHashJoin())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPxSharedHashJoin", K(ret));
      }
      break;
    }
    case SYS_VAR_SQL_NOTES: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSqlNotes)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSqlNotes)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSqlNotes())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSqlNotes", K(ret));
      }
      break;
    }
    case SYS_VAR_INNODB_STRICT_MODE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarInnodbStrictMode)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarInnodbStrictMode)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarInnodbStrictMode())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarInnodbStrictMode", K(ret));
      }
      break;
    }
    case SYS_VAR__WINDOWFUNC_OPTIMIZATION_SETTINGS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarWindowfuncOptimizationSettings)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarWindowfuncOptimizationSettings)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarWindowfuncOptimizationSettings())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarWindowfuncOptimizationSettings", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_ENABLE_RICH_ERROR_MSG: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObEnableRichErrorMsg)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObEnableRichErrorMsg)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObEnableRichErrorMsg())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObEnableRichErrorMsg", K(ret));
      }
      break;
    }
    case SYS_VAR_LOG_ROW_VALUE_OPTIONS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarLogRowValueOptions)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarLogRowValueOptions)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarLogRowValueOptions())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarLogRowValueOptions", K(ret));
      }
      break;
    }
    case SYS_VAR_OB_MAX_READ_STALE_TIME: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObMaxReadStaleTime)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObMaxReadStaleTime)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObMaxReadStaleTime())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObMaxReadStaleTime", K(ret));
      }
      break;
    }
    case SYS_VAR__OPTIMIZER_GATHER_STATS_ON_LOAD: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarOptimizerGatherStatsOnLoad)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarOptimizerGatherStatsOnLoad)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarOptimizerGatherStatsOnLoad())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarOptimizerGatherStatsOnLoad", K(ret));
      }
      break;
    }
    case SYS_VAR__SET_REVERSE_DBLINK_INFOS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarSetReverseDblinkInfos)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarSetReverseDblinkInfos)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarSetReverseDblinkInfos())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarSetReverseDblinkInfos", K(ret));
      }
      break;
    }
    case SYS_VAR__FORCE_ORDER_PRESERVE_SET: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarForceOrderPreserveSet)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarForceOrderPreserveSet)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarForceOrderPreserveSet())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarForceOrderPreserveSet", K(ret));
      }
      break;
    }
    case SYS_VAR__SHOW_DDL_IN_COMPAT_MODE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarShowDdlInCompatMode)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarShowDdlInCompatMode)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarShowDdlInCompatMode())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarShowDdlInCompatMode", K(ret));
      }
      break;
    }
    case SYS_VAR_PARALLEL_DEGREE_POLICY: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarParallelDegreePolicy)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarParallelDegreePolicy)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarParallelDegreePolicy())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarParallelDegreePolicy", K(ret));
      }
      break;
    }
    case SYS_VAR_PARALLEL_DEGREE_LIMIT: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarParallelDegreeLimit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarParallelDegreeLimit)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarParallelDegreeLimit())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarParallelDegreeLimit", K(ret));
      }
      break;
    }
    case SYS_VAR_PARALLEL_MIN_SCAN_TIME_THRESHOLD: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarParallelMinScanTimeThreshold)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarParallelMinScanTimeThreshold)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarParallelMinScanTimeThreshold())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarParallelMinScanTimeThreshold", K(ret));
      }
      break;
    }
    case SYS_VAR_OPTIMIZER_DYNAMIC_SAMPLING: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarOptimizerDynamicSampling)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarOptimizerDynamicSampling)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarOptimizerDynamicSampling())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarOptimizerDynamicSampling", K(ret));
      }
      break;
    }
    case SYS_VAR_RUNTIME_FILTER_TYPE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarRuntimeFilterType)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarRuntimeFilterType)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRuntimeFilterType())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRuntimeFilterType", K(ret));
      }
      break;
    }
    case SYS_VAR_RUNTIME_FILTER_WAIT_TIME_MS: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarRuntimeFilterWaitTimeMs)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarRuntimeFilterWaitTimeMs)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRuntimeFilterWaitTimeMs())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRuntimeFilterWaitTimeMs", K(ret));
      }
      break;
    }
    case SYS_VAR_RUNTIME_FILTER_MAX_IN_NUM: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarRuntimeFilterMaxInNum)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarRuntimeFilterMaxInNum)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRuntimeFilterMaxInNum())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRuntimeFilterMaxInNum", K(ret));
      }
      break;
    }
    case SYS_VAR_RUNTIME_BLOOM_FILTER_MAX_SIZE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarRuntimeBloomFilterMaxSize)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarRuntimeBloomFilterMaxSize)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarRuntimeBloomFilterMaxSize())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarRuntimeBloomFilterMaxSize", K(ret));
      }
      break;
    }
    case SYS_VAR_OPTIMIZER_FEATURES_ENABLE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarOptimizerFeaturesEnable)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarOptimizerFeaturesEnable)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarOptimizerFeaturesEnable())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarOptimizerFeaturesEnable", K(ret));
      }
      break;
    }
    case SYS_VAR__OB_PROXY_WEAKREAD_FEEDBACK: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarObProxyWeakreadFeedback)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarObProxyWeakreadFeedback)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarObProxyWeakreadFeedback())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarObProxyWeakreadFeedback", K(ret));
      }
      break;
    }
    case SYS_VAR_NCHARACTER_SET_CONNECTION: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarNcharacterSetConnection)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarNcharacterSetConnection)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarNcharacterSetConnection())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarNcharacterSetConnection", K(ret));
      }
      break;
    }
    case SYS_VAR_AUTOMATIC_SP_PRIVILEGES: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarAutomaticSpPrivileges)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarAutomaticSpPrivileges)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarAutomaticSpPrivileges())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarAutomaticSpPrivileges", K(ret));
      }
      break;
    }
    case SYS_VAR_PRIVILEGE_FEATURES_ENABLE: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarPrivilegeFeaturesEnable)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarPrivilegeFeaturesEnable)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPrivilegeFeaturesEnable())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPrivilegeFeaturesEnable", K(ret));
      }
      break;
    }
    case SYS_VAR__PRIV_CONTROL: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarPrivControl)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarPrivControl)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarPrivControl())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarPrivControl", K(ret));
      }
      break;
    }
    case SYS_VAR__ENABLE_MYSQL_PL_PRIV_CHECK: {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSysVarEnableMysqlPlPrivCheck)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(sizeof(ObSysVarEnableMysqlPlPrivCheck)));
      } else if (OB_ISNULL(sys_var_ptr = new (ptr)ObSysVarEnableMysqlPlPrivCheck())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to new ObSysVarEnableMysqlPlPrivCheck", K(ret));
      }
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid system variable id", K(ret), K(sys_var_id));
      break;
    }
  }
  return ret;
}

int ObSysVarFactory::create_sys_var(ObSysVarClassType sys_var_id, ObBasicSysVar *&sys_var)
{
  int ret = OB_SUCCESS;
  int64_t store_idx = -1;
  ObBasicSysVar *sys_var_ptr = NULL;
  if (OB_FAIL(calc_sys_var_store_idx(sys_var_id, store_idx))) {
    LOG_WARN("fail to calc sys var store idx", K(ret), K(sys_var_id));
  } else if (store_idx < 0 || store_idx >= ALL_SYS_VARS_COUNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected store idx", K(ret), K(store_idx), K(sys_var_id));
  } else if (OB_NOT_NULL(store_[store_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("store ptr shoule be null", K(ret), K(store_idx), K(sys_var_id));
  } else {
    if (OB_NOT_NULL(store_buf_[store_idx])) {
      sys_var_ptr = store_buf_[store_idx];
      store_buf_[store_idx] = nullptr;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(sys_var_ptr)) {
    if (OB_FAIL(create_sys_var(allocator_, sys_var_id, sys_var_ptr))) {
      LOG_WARN("fail to calc sys var", K(ret), K(sys_var_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(sys_var_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ret is OB_SUCCESS, but sys_var_ptr is NULL", K(ret), K(sys_var_id));
    } else if (OB_NOT_NULL(store_[store_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("store_[store_idx] should be NULL", K(ret), K(sys_var_id));
    } else {
      store_[store_idx] = sys_var_ptr;
      sys_var = sys_var_ptr;
    }
  }
  if (OB_FAIL(ret) && sys_var_ptr != nullptr) {
    sys_var_ptr->~ObBasicSysVar();
    sys_var_ptr = NULL;
  }
  return ret;
}

}
}
