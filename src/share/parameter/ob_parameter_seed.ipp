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

#include "share/parameter/ob_parameter_macro.h"

#ifndef OB_CLUSTER_PARAMETER
#define OB_CLUSTER_PARAMETER(args...)
#endif

#ifndef OB_TENANT_PARAMETER
#define OB_TENANT_PARAMETER(args...)
#endif

//// sstable config
// "/ob/storage/path/dir" means use local dir
// "ofs://0.0.0.0,1.1.1.1,2.2.2.2/dir" means use ofs dir
DEF_STR(data_dir, OB_CLUSTER_PARAMETER, "store", "the directory for the data file",
    ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::READONLY));
DEF_STR(redundancy_level, OB_CLUSTER_PARAMETER, "NORMAL",
    "EXTERNAL: use extrernal redundancy"
    "NORMAL: tolerate one disk failure"
    "HIGH: tolerate two disk failure if disk count is enough",
    ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE))
DEF_CAP(datafile_size, OB_CLUSTER_PARAMETER, "0", "[0M,)", "size of the data file. Range: [0, +∞)",
    ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(datafile_next, OB_CLUSTER_PARAMETER, "0", "[0M,)", "the auto extend step. Range: [0, +∞)",
    ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(datafile_maxsize, OB_CLUSTER_PARAMETER, "0", "[0M,)", "the auto extend max size. Range: [0, +∞)",
    ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_datafile_usage_upper_bound_percentage, OB_CLUSTER_PARAMETER, "90", "[5,99]",
    "the percentage of disk space usage upper bound to trigger datafile extend. Range: [5,99] in integer",
    ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_datafile_usage_lower_bound_percentage, OB_CLUSTER_PARAMETER, "10", "[5,99]",
    "the percentage of disk space usage lower bound to trigger datafile shrink. Range: [5,99] in integer",
    ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(datafile_disk_percentage, OB_CLUSTER_PARAMETER, "90", "[5,99]",
    "the percentage of disk space used by the data files. Range: [5,99] in integer",
    ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(memory_reserved, OB_CLUSTER_PARAMETER, "500M", "[10M,)",
    "the size of the system memory reserved for emergency internal use. "
    "Range: [10M, total size of memory]",
    ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//// observer config
DEF_STR_LIST(config_additional_dir, OB_CLUSTER_PARAMETER, "etc2;etc3", "additional directories of configure file",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(leak_mod_to_check, OB_CLUSTER_PARAMETER, "NONE", "the name of the module under memory leak checks",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(rpc_port, OB_CLUSTER_PARAMETER, "2500", "(1024,65536)",
    "the port number for RPC protocol. Range: (1024. 65536) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(mysql_port, OB_CLUSTER_PARAMETER, "2880", "(1024,65536)",
    "port number for mysql connection. Range: (1024, 65536) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(devname, OB_CLUSTER_PARAMETER, "bond0", "name of network adapter",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(zone, OB_CLUSTER_PARAMETER, "", "specifies the zone name",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(internal_sql_execute_timeout, OB_CLUSTER_PARAMETER, "30s", "[1000us, 10m]",
    "the number of microseconds an internal DML request is permitted to "
    "execute before it is terminated. Range: [1000us, 10m]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(net_thread_count, OB_CLUSTER_PARAMETER, "0", "[0,64]",
    "the number of rpc/mysql I/O threads for Libeasy. Range: [0, 64] in integer, 0 stands for max(6, CPU_NUM/8)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_INT(high_priority_net_thread_count, OB_CLUSTER_PARAMETER, "0", "[0,64]",
    "the number of rpc I/O threads for high priority messages, 0 means set off. Range: [0, 64] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_INT(tenant_task_queue_size, OB_CLUSTER_PARAMETER, "65536", "[1024,]",
    "the size of the task queue for each tenant. Range: [1024,+∞)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP_WITH_CHECKER(memory_limit, OB_CLUSTER_PARAMETER, "0", common::ObConfigMemoryLimitChecker, "[0M,)",
    "the size of the memory reserved for internal use(for testing purpose), 0 means follow memory_limit_percentage. "
    "Range: 0, [8G,)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(rootservice_memory_limit, OB_CLUSTER_PARAMETER, "2G", "[2G,)",
    "max memory size which can be used by rs tenant The default value is 2G. Range: [2G,)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(system_memory, OB_CLUSTER_PARAMETER, "30G", "[0M,)",
    "the memory reserved for internal use which cannot be allocated to any outer-tenant, "
    "and should be determined to guarantee every server functions normally. Range: [0M,)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(minor_warm_up_duration_time, OB_CLUSTER_PARAMETER, "30s", "[0s, 60m]",
    "warm up duration time for minor freeze. Range: [0s,60m]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(minor_deferred_gc_time, OB_CLUSTER_PARAMETER, "0s", "[0s, 24h]",
    "sstable deferred gc time after merge. Range: [0s,24h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_minor_deferred_gc_level, OB_CLUSTER_PARAMETER, "1", "[0, 1]",
    "minor deferred gc_level, 0 means defer gc L0, 1 means defer gc L0/L1",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(cpu_count, OB_CLUSTER_PARAMETER, "0", "[0,]",
    "the number of CPU\\'s in the system. "
    "If this parameter is set to zero, the number will be set according to sysconf; "
    "otherwise, this parameter is used. Range: [0,+∞) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(cpu_reserved, OB_CLUSTER_PARAMETER, "2", "[0,15]",
    "the number of CPU\\'s reserved for system usage. Range: [0, 15] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(trace_log_sampling_interval, OB_CLUSTER_PARAMETER, "10ms", "[0ms,]",
    "the time interval for periodically printing log info in trace log. "
    "When force_trace_log is set to FALSE, "
    "for each time interval specifies by sampling_trace_log_interval, "
    "logging info regarding 'slow query' and 'white list' will be printed out. "
    "Range: [0ms,+∞)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(trace_log_slow_query_watermark, OB_CLUSTER_PARAMETER, "1s", "[1ms,]",
    "the threshold of execution time (in milliseconds) of a query beyond "
    "which it is considered to be a \\'slow query\\'. Range: [1ms,+∞)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_one_phase_commit, OB_CLUSTER_PARAMETER, "False", "enable one phase commit optimization",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEPRECATED_DEF_BOOL(enable_pg, OB_CLUSTER_PARAMETER, "False", "open partition group",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_record_trace_log, OB_CLUSTER_PARAMETER, "True",
    "specifies whether to always record the trace log. The default value is True.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(system_trace_level, OB_CLUSTER_PARAMETER, "1", "[0,2]",
    "system trace log level, 0:none, 1:standard, 2:debug. "
    "The default log level for trace log is 1",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(max_string_print_length, OB_CLUSTER_PARAMETER, "500", "[0,]",
    "truncate very long string when printing to log file. Range: [0,]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(sql_audit_memory_limit, OB_CLUSTER_PARAMETER, "3G", "[64M,]",
    "the maximum size of the memory used by SQL audit virtual table "
    "when the function is turned on. The upper limit is 3G, with defaut "
    "10% of avaiable memory. Range: [64M, +∞]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(sql_audit_queue_size, OB_CLUSTER_PARAMETER, "10000000", "[10000,]",
    "the maximum number of the records in SQL audit virtual table. (don't use now)"
    "The default value is 10000000. Range: [10000, +∞], integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_sql_audit, OB_CLUSTER_PARAMETER, "true",
    "specifies whether SQL audit is turned on. "
    "The default value is TRUE. Value: TRUE: turned on FALSE: turned off",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_record_trace_id, OB_CLUSTER_PARAMETER, "true", "specifies whether record app trace id is turned on.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_rich_error_msg, OB_CLUSTER_PARAMETER, "false",
    "specifies whether add ip:port, time and trace id to user error message. "
    "The default value is FALSE.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(debug_sync_timeout, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "Enable the debug sync facility and "
    "optionally specify a default wait timeout in micro seconds. "
    "A zero value keeps the facility disabled, Range: [0, +∞]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(dead_socket_detection_timeout, OB_CLUSTER_PARAMETER, "10s", "[0s,2h]",
    "specify a tcp_user_timeout for RFC5482. "
    "A zero value makes the option disabled, Range: [0, 2h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_perf_event, OB_CLUSTER_PARAMETER, "True",
    "specifies whether to enable perf event feature. The default value is False.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_separate_sys_clog, OB_CLUSTER_PARAMETER, "False",
    "separate system and user commit log. The default value is False.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_upgrade_mode, OB_CLUSTER_PARAMETER, "False",
    "specifies whether upgrade mode is turned on. "
    "If turned on, daily merger and balancer will be disabled. "
    "Value: True: turned on; False: turned off;",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(schema_history_expire_time, OB_CLUSTER_PARAMETER, "7d", "[1m, 30d]",
    "the hour of expire time for schema history, from 1hour to 30days, "
    "with default 7days. Range: [1h, 30d]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(default_compress_func, OB_CLUSTER_PARAMETER, "zstd_1.3.8", common::ObConfigCompressFuncChecker,
    "default compress function name for create new table, "
    "values: none, lz4_1.0, snappy_1.0, zlib_1.0, zstd_1.0, zstd_1.3.8",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(default_row_format, OB_CLUSTER_PARAMETER, "compact", common::ObConfigRowFormatChecker,
    "default row format in mysql mode",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(default_compress, OB_CLUSTER_PARAMETER, "oltp", common::ObConfigCompressOptionChecker,
    "default compress strategy for create new table within oracle mode",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(_partition_balance_strategy, OB_CLUSTER_PARAMETER, "auto",
    common::ObConfigPartitionBalanceStrategyFuncChecker,
    "specifies the partition balance strategy. "
    "Value: [auto]: partition and shard amount with disk utilization strategy is used, "
    "Value: [standard]: partition amout with disk utilization stragegy is used, "
    "Value: [disk_utilization_only]: disk utilization strategy is used.",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(weak_read_version_refresh_interval, OB_CLUSTER_PARAMETER, "50ms", "[0ms,)",
    "the time interval to refresh cluster weak read version "
    "Range: [0ms, +∞)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// for observer, root_server_list in format ip1:rpc_port1:sql_port1;ip2:rpc_port2:sql_port2
DEF_STR_LIST(rootservice_list, OB_CLUSTER_PARAMETER, "",
    "a list of servers against which election candidate is checked for validation",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(cluster, OB_CLUSTER_PARAMETER, "obcluster", "Name of the cluster",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(cluster_id, OB_CLUSTER_PARAMETER, "0", "[1,4294901759]", "ID of the cluster",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(obconfig_url, OB_CLUSTER_PARAMETER, "", "URL for OBConfig service",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_syslog_file_compress, OB_CLUSTER_PARAMETER, "False",
    "specifies whether to compress archive log files"
    "Value: True:turned on; False: turned off",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(max_syslog_file_time, OB_CLUSTER_PARAMETER, "0s", "[0s, 3650d]",
    "specifies the maximum retention time of the log files. "
    "When this value is set to 0s, no log file will be removed due to time. "
    "with default 0s. Range: [0s, 3650d]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_LOG_LEVEL(syslog_level, OB_CLUSTER_PARAMETER, "INFO",
    "specifies the current level of logging. There are DEBUG, TRACE, INFO, WARN, USER_ERR, ERROR, six different log "
    "levels.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(syslog_io_bandwidth_limit, OB_CLUSTER_PARAMETER, "30MB",
    "Syslog IO bandwidth limitation, exceeding syslog would be truncated. Use 0 to disable ERROR log.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(max_syslog_file_count, OB_CLUSTER_PARAMETER, "0", "[0,]",
    "specifies the maximum number of the log files "
    "that can co-exist before the log file recycling kicks in. "
    "Each log file can occupy at most 256MB disk space. "
    "When this value is set to 0, no log file will be removed due to the file count. Range: [0, +∞) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_async_syslog, OB_CLUSTER_PARAMETER, "True",
    "specifies whether use async log for observer.log, elec.log and rs.log",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_syslog_wf, OB_CLUSTER_PARAMETER, "True",
    "specifies whether any log message with a log level higher than \\'WARN\\' "
    "would be printed into a separate file with a suffix of \\'wf\\'",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_syslog_recycle, OB_CLUSTER_PARAMETER, "False",
    "specifies whether log file recycling is turned on. "
    "Value: True:turned on; False: turned off",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(memory_limit_percentage, OB_CLUSTER_PARAMETER, "80", "[10, 90]",
    "the size of the memory reserved for internal use(for testing purpose). Range: [10, 90]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(cache_wash_threshold, OB_CLUSTER_PARAMETER, "4GB", "[0B,]",
    "size of remaining memory at which cache eviction will be triggered. Range: [0,+∞)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(memory_chunk_cache_size, OB_CLUSTER_PARAMETER, "0M", "[0M,]",
    "the maximum size of memory cached by memory chunk cache. Range: [0M,], 0 stands for adaptive",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(autoinc_cache_refresh_interval, OB_CLUSTER_PARAMETER, "3600s", "[100ms,]",
    "auto-increment service cache refresh sync_value in this interval, "
    "with default 3600s. Range: [100ms, +∞)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_sql_operator_dump, OB_CLUSTER_PARAMETER, "True",
    "specifies whether sql operators "
    "(sort/hash join/material/window function/interm result/...) allowed to write to disk",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_chunk_row_store_mem_limit, OB_CLUSTER_PARAMETER, "0B", "[0,]",
    "the maximum size of memory used by ChunkRowStore, 0 means follow operator's setting. Range: [0, +∞)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(tableapi_transport_compress_func, OB_CLUSTER_PARAMETER, "none",
    common::ObConfigCompressFuncChecker,
    "compressor used for tableAPI query result. Values: none, lz4_1.0, snappy_1.0, zlib_1.0, zstd_1.0 zstd 1.3.8",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_sort_area_size, OB_TENANT_PARAMETER, "128M", "[2M,]",
    "size of maximum memory that could be used by SORT. Range: [2M,+∞)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_hash_area_size, OB_TENANT_PARAMETER, "100M", "[4M,]",
    "size of maximum memory that could be used by HASH JOIN. Range: [4M,+∞)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(rebuild_replica_data_lag_threshold, OB_CLUSTER_PARAMETER, "0",
    "size of clog files that a replica lag behind leader to trigger rebuild, 0 means never trigger rebuild on purpose. "
    "Range: [0, +∞)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// TODO : application && default true
DEF_BOOL(_enable_static_typing_engine, OB_CLUSTER_PARAMETER, "True",
    "specifies whether static typing sql execution engine is activated",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_defensive_check, OB_CLUSTER_PARAMETER, "False",
    "specifies whether allow to do some defensive checks when the query is executed",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
//// tenant config
DEF_TIME_WITH_CHECKER(max_stale_time_for_weak_consistency, OB_TENANT_PARAMETER, "5s", common::ObConfigStaleTimeChecker,
    "[5s,)",
    "the max data stale time that cluster weak read version behind current timestamp,"
    "no smaller than weak_read_version_refresh_interval, range: [5s, +∞)",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_monotonic_weak_read, OB_TENANT_PARAMETER, "false",
    "specifies observer supportting atomicity and monotonic order read",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(tenant_cpu_variation_per_server, OB_CLUSTER_PARAMETER, "50", "[0,100]",
    "the percentage variation for any tenant\\'s CPU quota allocation on each observer. "
    "The default value is 50(%). Range: [0, 100] in percentage",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(system_cpu_quota, OB_CLUSTER_PARAMETER, "10", "[0,16]", "the number of vCPUs allocated to the clog tenant",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(server_cpu_quota_min, OB_CLUSTER_PARAMETER, "2.5", "[0,16]",
    "the number of minimal vCPUs allocated to the server tenant"
    "(a special internal tenant that exists on every observer). Range: [0, 16]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_DBL(server_cpu_quota_max, OB_CLUSTER_PARAMETER, "5", "[0,16]",
    "the number of maximal vCPUs allocated to the server tenant"
    "(a special internal tenant that exists on every observer). Range: [0, 16]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_DBL(election_cpu_quota, OB_CLUSTER_PARAMETER, "3", "[0,10]",
    "the number of vCPUs allocated to the \\'election\\' tenant. Range: [0,10] in integer",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(location_cache_cpu_quota, OB_CLUSTER_PARAMETER, "5", "[0,10]",
    "the number of vCPUs allocated for the requests regarding location "
    "info of the core tables. Range: [0,10] in integer",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(workers_per_cpu_quota, OB_CLUSTER_PARAMETER, "10", "[2,20]",
    "the ratio(integer) between the number of system allocated workers vs "
    "the maximum number of threads that can be scheduled concurrently. Range: [2, 20]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(large_query_worker_percentage, OB_CLUSTER_PARAMETER, "30", "[0,100]",
    "the percentage of the workers reserved to serve large query requests. "
    "Range: [0, 100] in percentage",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE))
DEF_TIME(large_query_threshold, OB_CLUSTER_PARAMETER, "5s", "[1ms,)",
    "threshold for execution time beyond "
    "which a request may be paused and rescheduled as a \\'large request\\'. Range: [1ms, +∞)",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(cpu_quota_concurrency, OB_CLUSTER_PARAMETER, "4", "[1,10]",
    "max allowed concurrency for 1 CPU quota. Range: [1,10]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(token_reserved_percentage, OB_CLUSTER_PARAMETER, "30", "[0,100]",
    "specifies the amount of token increase allocated to a tenant based on "
    "his consumption from the last round (without exceeding his upper limit). "
    "Range: [0, 100] in percentage",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_global_freeze_trigger, OB_CLUSTER_PARAMETER, "False",
    "specifies whether to trigger major freeze when global active memstore used reach "
    "global_freeze_trigger_percentage, "
    "Value:  True:turned on  False: turned off",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(global_major_freeze_residual_memory, OB_CLUSTER_PARAMETER, "40", "(0, 100)",
    "post global major freeze when observer memstore free memory(plus memory held by frozen memstore and blockcache) "
    "reach this limit. Rang:(0, 100)"
    "limit calc by (memory_limit - system_memory) * global_major_freeze_residual_memory/100",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(global_write_halt_residual_memory, OB_CLUSTER_PARAMETER, "30", "(0, 100)",
    "disable write to memstore when observer memstore free memory(plus memory held by blockcache) lower than this "
    "limit, Range: (0, 100)"
    "limit calc by (memory_limit - system_memory) * global_write_halt_residual_memory/100",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(px_workers_per_cpu_quota, OB_CLUSTER_PARAMETER, "10", "[0,20]",
    "the ratio(integer) between the number of system allocated px workers vs "
    "the maximum number of threads that can be scheduled concurrently. Range: [0, 20]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// tenant memtable consumption related
DEF_INT(memstore_limit_percentage, OB_CLUSTER_PARAMETER, "50", "(0, 100)",
    "used in calculating the value of MEMSTORE_LIMIT parameter: "
    "memstore_limit_percentage = memstore_limit / min_memory, min_memory, "
    "where MIN_MEMORY is determined when the tenant is created. Range: (0, 100)",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(freeze_trigger_percentage, OB_CLUSTER_PARAMETER, "50", "(0, 100)",
    "the threshold of the size of the mem store when freeze will be triggered. Range: (0, 100)",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(writing_throttling_trigger_percentage, OB_TENANT_PARAMETER, "100", "(0, 100]",
    "the threshold of the size of the mem store when writing_limit will be triggered. Rang:(0, 100]. setting 100 means "
    "turn off writing limit",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(writing_throttling_maximum_duration, OB_TENANT_PARAMETER, "1h", "[1s, 3d]",
    "maximum duration of writing throttling(in minutes), max value is 3 days",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(plan_cache_high_watermark, OB_CLUSTER_PARAMETER, "2000M",
    "(don't use now) memory usage at which plan cache eviction will be trigger immediately. Range: [0, +∞)",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(plan_cache_low_watermark, OB_CLUSTER_PARAMETER, "1500M",
    "(don't use now) memory usage at which plan cache eviction will be stopped. "
    "Range: [0, plan_cache_high_watermark)",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(plan_cache_evict_interval, OB_CLUSTER_PARAMETER, "1s", "[0s,)",
    "time interval for periodic plan cache eviction. Range: [0s, +∞)",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(max_px_worker_count, OB_CLUSTER_PARAMETER, "64", "[0,65535]",
    "maximum parallel execution worker count can be used for all parallel requests. "
    "Range: [0, 65535]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(default_progressive_merge_num, OB_TENANT_PARAMETER, "0", "[0,)",
    "default progressive_merge_num when tenant create table"
    "Range: [0,)",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_parallel_min_message_pool, OB_TENANT_PARAMETER, "400M", "[16M, 8G]",
    "DTL message buffer pool reserve the mininum size after extend the size. Range: [16M,8G]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(_parallel_server_sleep_time, OB_TENANT_PARAMETER, "1", "[0, 2000]",
    "sleep time between get channel data in millisecond. Range: [0, 2000]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(_px_max_message_pool_pct, OB_TENANT_PARAMETER, "40", "[0,90]",
    "The maxinum percent of tenant memory that DTL message buffer pool can alloc memory. Range: [0,90]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_px_message_compression, OB_TENANT_PARAMETER, "False",
    "Enable DTL send message with compression"
    "Value: True: enable compression False: disable compression",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_px_chunklist_count_ratio, OB_CLUSTER_PARAMETER, "1", "[1, 128]",
    "the ratio of the dtl buffer manager list. Range: [1, 128]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_force_hash_join_spill, OB_TENANT_PARAMETER, "False",
    "force hash join to dump after get all build hash table "
    "Value:  True:turned on  False: turned off",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_enable_hash_join_hasher, OB_TENANT_PARAMETER, "1", "[1, 7]",
    "which hash function to choose for hash join "
    "1: murmurhash, 2: crc, 4: xxhash",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_enable_hash_join_processor, OB_TENANT_PARAMETER, "7", "[1, 7]",
    "which path to process for hash join, default 7 to auto choose "
    "1: nest loop, 2: recursive, 4: in-memory",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_filter_push_down_storage, OB_TENANT_PARAMETER, "False",
    "Enable filter push down to storage"
    "Value:  True:turned on  False: turned off",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_WORK_AREA_POLICY(workarea_size_policy, OB_TENANT_PARAMETER, "AUTO",
    "policy used to size SQL working areas (MANUAL/AUTO)",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(_gts_core_num, OB_TENANT_PARAMETER, "0", "[0, 256]", "core num allocate for gts service. Range: [0, 256]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_create_table_partition_distribution_strategy, OB_CLUSTER_PARAMETER, "1", "[1, 2]",
    "which distribution strategy for create table partition "
    "1: distribute partition by remainder, 2: distribute partition by division",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(__enable_identical_partition_distribution, OB_CLUSTER_PARAMETER, "false",
    "Identify partition distribution methods "
    "true: partition leader/follow isomorphism, false: partition leader/follow isomerism",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_temporary_file_io_area_size, OB_TENANT_PARAMETER, "1", "[0, 50)",
    "memory buffer size of temporary file, as a percentage of total tenant memory. "
    "Range: [0, 50), percentage",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(__enable_small_tenant, OB_CLUSTER_PARAMETER, "true",
    "specify when small tenant configuration is on "
    "true: turn small tenant configuration on, false: turn small tenant configuration off",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
////  rootservice config
DEF_STR(all_cluster_list, OB_CLUSTER_PARAMETER, "", "a list of servers which access the same config_url",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_rootservice_standalone, OB_CLUSTER_PARAMETER, "False",
    "specifies whether the \\'SYS\\' tenant is allowed to occupy an observer exclusively, "
    "thus running in the \\'standalone\\' mode. Value:  True:turned on  False: turned off",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(lease_time, OB_CLUSTER_PARAMETER, "10s", "[1s, 5m]",
    "Lease for current heartbeat. If the root server does not received any heartbeat "
    "from an observer in lease_time seconds, that observer is considered to be offline. "
    "Not recommended for modification. Range: [1s, 5m]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(switchover_process_thread_count, OB_CLUSTER_PARAMETER, "6", "[1,1000]",
    "maximum of threads allowed for executing switchover task at rootserver. "
    "Range: [1, 1000]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(rootservice_async_task_thread_count, OB_CLUSTER_PARAMETER, "4", "[1,10]",
    "maximum of threads allowed for executing asynchronous task at rootserver. "
    "Range: [1, 10]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(rootservice_async_task_queue_size, OB_CLUSTER_PARAMETER, "16384", "[8,131072]",
    "the size of the queue for all asynchronous tasks at rootserver. "
    "Range: [8, 131072] in integer",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_sys_table_ddl, OB_CLUSTER_PARAMETER, "False",
    "specifies whether a \\'system\\' table is allowed be to created manually. "
    "Value: True: allowed; False: not allowed",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(auto_leader_switch_interval, OB_CLUSTER_PARAMETER, "30s", "[1s,)",
    "time interval for periodic leadership reorganization taking place. Range: [1s, +∞)",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(wait_leader_batch_count, OB_CLUSTER_PARAMETER, "1024", "[128, 5000]",
    "leader batch count everytime leader coordinator wait. Range: [128, 5000]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(merger_switch_leader_duration_time, OB_CLUSTER_PARAMETER, "3m", "[0s,30m]",
    "switch leader duration time for daily merge. Range: [0s,30m]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(merger_warm_up_duration_time, OB_CLUSTER_PARAMETER, "0s", "[0s,60m]",
    "warm up duration time for daily merge. Range: [0s,60m]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// NOTE: server_temporary_offline_time is discarded.
DEF_TIME(server_temporary_offline_time, OB_CLUSTER_PARAMETER, "60s", "[15s,)",
    "the time interval between two heartbeats beyond "
    "which a server is considered to be \\'temporarily\\' offline. Range: [15s, +∞)",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(server_permanent_offline_time, OB_CLUSTER_PARAMETER, "3600s", "[20s,)",
    "the time interval between any two heartbeats beyond "
    "which a server is considered to be \\'permanently\\' offline. Range: [20s,+∞)",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(migration_disable_time, OB_CLUSTER_PARAMETER, "3600s", "[1s,)",
    "the duration in which the observer stays in the \\'block_migrate_in\\' status, "
    "which means no partition is allowed to migrate into the server. Range: [1s, +∞)",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(server_check_interval, OB_CLUSTER_PARAMETER, "30s", "[1s,)",
    "the time interval between schedules of a task "
    "that examines the __all_server table. Range: [1s, +∞)",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(rootservice_ready_check_interval, OB_CLUSTER_PARAMETER, "3s", "[100000us, 1m]",
    "the interval between the schedule of the task "
    "that checks on the status of the ZONE during restarting. Range: [100000us, 1m]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(partition_table_scan_batch_count, OB_CLUSTER_PARAMETER, "999", "(0, 65536]",
    "the number of partition replication info "
    "that will be read by each request on the partition-related system tables "
    "during procedures such as load-balancing, daily merge, election and etc. "
    "Range: (0,65536]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_auto_leader_switch, OB_CLUSTER_PARAMETER, "True",
    "specifies whether partition leadership auto-switch is turned on. "
    "Value:  True:turned on;  False: turned off",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(replica_safe_remove_time, OB_CLUSTER_PARAMETER, "2h", "[1m,)",
    "the time interval that replica not existed has not been modified beyond "
    "which a replica is considered can be safely removed. Range: [1m,+∞)",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(partition_table_check_interval, OB_CLUSTER_PARAMETER, "30m", "[1m,)",
    "the time interval that observer remove replica "
    "which not exist in observer from partition table. Range: [1m,+∞)",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(get_leader_candidate_rpc_timeout, OB_CLUSTER_PARAMETER, "9s", "[2s, 180s]",
    "the time during a get leader candidate rpc request "
    "is permitted to execute before it is terminated. Range: [2s, 180s]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(min_observer_version, OB_CLUSTER_PARAMETER, "3.1.4", "the min observer version",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_ddl, OB_CLUSTER_PARAMETER, "True",
    "specifies whether DDL operation is turned on. "
    "Value:  True:turned on;  False: turned off",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(global_index_build_single_replica_timeout, OB_CLUSTER_PARAMETER, "48h", "[1h,)",
    "build single replica task timeout "
    "when rootservice schedule to build global index. Range: [1h,+∞)",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_major_freeze, OB_CLUSTER_PARAMETER, "True",
    "specifies whether major_freeze function is turned on. "
    "Value:  True:turned on;  False: turned off",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(ob_event_history_recycle_interval, OB_CLUSTER_PARAMETER, "7d", "[1d, 180d]",
    "the time to recycle event history. Range: [1d, 180d]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_recyclebin_object_purge_frequency, OB_CLUSTER_PARAMETER, "10m", "[0m,)",
    "the time to purge recyclebin. Range: [0m, +∞)",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(recyclebin_object_expire_time, OB_CLUSTER_PARAMETER, "0s", "[0s,)",
    "recyclebin object expire time, "
    "default 0 that means auto purge recyclebin off. Range: [0s, +∞)",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//// load balance config

DEF_INT(resource_soft_limit, OB_CLUSTER_PARAMETER, "50", "(0, 10000]",
    "Used along with resource_hard_limit in unit allocation. "
    "If server utilization is less than resource_soft_limit, a policy of \\'best fit\\' "
    "will be used for unit allocation; "
    "otherwise, a \\'least load\\' policy will be employed. "
    "Ultimately,system utilization should not be larger than resource_hard_limit. "
    "Range: (0,10000] in in",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(resource_hard_limit, OB_CLUSTER_PARAMETER, "100", "(0, 10000]",
    "Used along with resource_soft_limit in unit allocation. "
    "If server utilization is less than resource_soft_limit, "
    "a policy of \\'best fit\\' will be used for unit allocation; "
    "otherwise, a \\'least load\\' policy will be employed. "
    "Ultimately,system utilization should not be larger than resource_hard_limit. "
    "Range: (0,10000] in integer",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_rereplication, OB_CLUSTER_PARAMETER, "True",
    "specifies whether the partition auto-replication is turned on. "
    "Value:  True:turned on  False: turned off",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_rebalance, OB_CLUSTER_PARAMETER, "True",
    "specifies whether the partition load-balancing is turned on. "
    "Value:  True:turned on  False: turned off",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(balancer_idle_time, OB_CLUSTER_PARAMETER, "5m", "[10s,]",
    "the time interval between the schedules of the partition load-balancing task. "
    "Range: [10s, +∞)",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(balancer_tolerance_percentage, OB_CLUSTER_PARAMETER, "10", "[1, 100)",
    "specifies the tolerance (in percentage) of the unbalance of the disk space utilization "
    "among all units. The average disk space utilization is calculated by dividing "
    "the total space by the number of units. For example, say balancer_tolerance_percentage "
    "is set to 10 and a tenant has two units in the system, "
    "the average disk use for each unit should be about the same, "
    "thus 50% of the total value. Therefore, the system will start a rebalancing task "
    "when any unit\\'s disk space goes beyond +-10% of the average usage. "
    "Range: [1, 100) in percentage",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(balancer_emergency_percentage, OB_CLUSTER_PARAMETER, "80", "[1, 100]",
    "Unit load balance is disabled while zone is merging."
    "But for unit load above emergency percentage suituation, "
    "will always try migrate out partitions."
    "Range: [1, 100] in percentage",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(server_data_copy_in_concurrency, OB_CLUSTER_PARAMETER, "2", "[1,1000]",
    "the maximum number of partitions allowed to migrate to the server. "
    "Range: [1, 1000], integer",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(server_data_copy_out_concurrency, OB_CLUSTER_PARAMETER, "2", "[1,1000]",
    "the maximum number of partitions allowed to migrate from the server. "
    "Range: [1, 1000], integer",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(data_copy_concurrency, OB_CLUSTER_PARAMETER, "20", "[1,)",
    "the maximum number of the data replication tasks. "
    "Range: [1,∞) in integer",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(fast_recovery_concurrency, OB_CLUSTER_PARAMETER, "10", "[1,1000]",
    "the maximum number of the datafile fast recovery tasks. "
    "Range: [1,1000] in integer",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(balancer_task_timeout, OB_CLUSTER_PARAMETER, "20m", "[1s,)",
    "the time to execute the load-balancing task before it is terminated. Range: [1s, +∞)",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(balancer_timeout_check_interval, OB_CLUSTER_PARAMETER, "1m", "[1s,)",
    "the time interval between the schedules of the task that checks "
    "whether the partition load balancing task has timed-out. Range: [1s, +∞)",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(balancer_log_interval, OB_CLUSTER_PARAMETER, "1m", "[1s,)",
    "the time interval between logging the load-balancing task\\'s statistics. "
    "Range: [1s, +∞)",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_unit_balance_resource_weight, OB_CLUSTER_PARAMETER, "False",
    "specifies whether maual configed resource weight is turned on. "
    "Value:  True:turned on  False: turned off",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(unit_balance_resource_weight, OB_CLUSTER_PARAMETER, "",
    "the percentage variation for any tenant\\'s resource weight. "
    "The default value is empty. All weight must adds up to 100 if set",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(__balance_controller, OB_CLUSTER_PARAMETER, "",
    "specifies whether the balance events are turned on or turned off.",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(__min_full_resource_pool_memory, OB_CLUSTER_PARAMETER, "5368709120", "[268435456,)",
    "the min memory value which is specified for a full resource pool.",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(tenant_groups, OB_CLUSTER_PARAMETER, "", "specifies tenant groups for server balancer.",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(server_balance_critical_disk_waterlevel, OB_CLUSTER_PARAMETER, "80", "[0, 100]",
    "disk water level to determine server balance strategy",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(server_balance_disk_tolerance_percent, OB_CLUSTER_PARAMETER, "1", "[1, 100]",
    "specifies the tolerance (in percentage) of the unbalance of the disk space utilization "
    "among all servers. The average disk space utilization is calculated by dividing "
    "the total space by the number of servers. "
    "server balancer will start a rebalancing task "
    "when the deviation between the average usage and some server load is greater than this tolerance "
    "Range: [1, 100] in percentage",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(server_balance_cpu_mem_tolerance_percent, OB_CLUSTER_PARAMETER, "5", "[1, 100]",
    "specifies the tolerance (in percentage) of the unbalance of the cpu/memory utilization "
    "among all servers. The average cpu/memory utilization is calculated by dividing "
    "the total cpu/memory by the number of servers. "
    "server balancer will start a rebalancing task "
    "when the deviation between the average usage and some server load is greater than this tolerance "
    "Range: [1, 100] in percentage",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(balance_blacklist_retry_interval, OB_CLUSTER_PARAMETER, "30m", "[0s, 180m]",
    "balance task in black list, wait a little time to add",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(balance_blacklist_failure_threshold, OB_CLUSTER_PARAMETER, "5", "[0, 1000]",
    "a balance task failed count to be putted into blacklist",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(enable_sys_unit_standalone, OB_CLUSTER_PARAMETER, "False",
    "specifies whether sys unit standalone deployment is turned on. "
    "Value:  True:turned on  False: turned off",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//// daily merge  config
// set to disable if don't want major freeze launch auto
DEF_MOMENT(major_freeze_duty_time, OB_CLUSTER_PARAMETER, "02:00",
    "the start time of system daily merge procedure. Range: [00:00, 24:00)",
    ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_manual_merge, OB_CLUSTER_PARAMETER, "False",
    "specifies whether manual MERGE is turned on. Value: True:turned on  False: turned off",
    ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(zone_merge_concurrency, OB_CLUSTER_PARAMETER, "0", "[0,]",
    "the maximum number of zones "
    "which are allowed to be in the \\'MERGE\\' status concurrently. "
    "Range: [0,) in integer",
    ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(zone_merge_order, OB_CLUSTER_PARAMETER, "", "the order of zone start merge in daily merge",
    ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_merge_by_turn, OB_CLUSTER_PARAMETER, "False",
    "specifies whether merge tasks can be performed on different zones "
    "in an alternating fashion. Value: True:turned on; False: turned off",
    ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(zone_merge_timeout, OB_CLUSTER_PARAMETER, "3h", "[1s,)",
    "the time for each zone to finish its merge process before "
    "the root service no longer consider it as in \\'MERGE\\' state. Range: [1s, +∞)",
    ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(merger_check_interval, OB_CLUSTER_PARAMETER, "10m", "[10s, 60m]",
    "the time interval between the schedules of the task "
    "that checks on the progress of MERGE for each zone. Range: [10s, 60m]",
    ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(ignore_replica_checksum_error, OB_CLUSTER_PARAMETER, "False",
    "specifies whether error raised from the partition checksum validation can be ignored. "
    "Value: True:ignored; False: not ignored");
DEF_INT(merger_completion_percentage, OB_CLUSTER_PARAMETER, "100", "[5, 100]",
    "the merged partition count percentage and merged data size percentage "
    "when MERGE is completed. Range: [5,100]",
    ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(max_kept_major_version_number, OB_CLUSTER_PARAMETER, "2", "[1, 16]",
    "the maximum number of kept major versions. "
    "Range: [1, 16] in integer",
    ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
//// transaction config
DEF_TIME(trx_2pc_retry_interval, OB_CLUSTER_PARAMETER, "100ms", "[1ms, 5000ms]",
    "the time interval between the retries in case of failure "
    "during a transaction\\'s two-phase commit phase. Range: [1ms,5000ms]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(clog_sync_time_warn_threshold, OB_CLUSTER_PARAMETER, "1s", "[1ms, 10000ms]",
    "the time given to the commit log synchronization between a leader and its followers "
    "before a \\'warning\\' message is printed in the log file.  Range: [1ms,1000ms]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(row_purge_thread_count, OB_CLUSTER_PARAMETER, "4", "[1,64]",
    "maximum of threads allowed for executing row purge task. "
    "Range: [1, 64]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(row_compaction_update_limit, OB_CLUSTER_PARAMETER, "6", "[1, 6400]",
    "maximum update count before trigger row compaction. "
    "Range: [1, 64]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(ignore_replay_checksum_error, OB_CLUSTER_PARAMETER, "False",
    "specifies whether error raised from the memtable replay checksum validation can be ignored. "
    "Value: True:ignored; False: not ignored",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(_rpc_checksum, OB_CLUSTER_PARAMETER, "Force", common::ObConfigRpcChecksumChecker,
    "Force: always verify; "
    "Optional: verify when rpc_checksum non-zero; "
    "Disable: ignore verify",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE))
DEF_TIME(trx_try_wait_lock_timeout, OB_CLUSTER_PARAMETER, "0ms",
    "the time to wait on row lock acquiring before retry. Range: [0ms, 100ms]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(trx_force_kill_threshold, OB_CLUSTER_PARAMETER, "100ms", "[1ms, 10s]",
    "the time given to the transaction to execute when major freeze or switch leader "
    "before it will be killed. Range: [1ms, 10s]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(flush_log_at_trx_commit, OB_CLUSTER_PARAMETER, "1", "[0, 2]",
    "0: commit transactions without waiting clog write to buffer cache,"
    "1: commit transactions after clog flush to disk,"
    "2: commit transactions after clog write to buffer cache,"
    "Range: [0, 2]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(clog_disk_usage_limit_percentage, OB_CLUSTER_PARAMETER, "95", "[80, 100]",
    "maximum of clog disk usage percentage before stop submitting or receiving logs, should be greater than "
    "clog_disk_utilization_threshold. "
    "Range: [80, 100]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(clog_disk_utilization_threshold, OB_CLUSTER_PARAMETER, "80", "[10, 100)",
    "clog disk utilization threshold before reuse clog files, should be less than clog_disk_usage_limit_percentage. "
    "Range: [10, 100)",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(election_blacklist_interval, OB_CLUSTER_PARAMETER, "1800s", "[0s, 24h]",
    "If leader_revoke, this replica cannot be elected to leader in election_blacklist_interval"
    "The default value is 1800s. Range: [0s, 24h]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(gts_refresh_interval, OB_CLUSTER_PARAMETER, "100us", "[10us, 1s]",
    "The time interval to request gts for high availability gts source (abbr: ha_gts_source)"
    "with default 100us. Range: [10us, 1s]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(clog_max_unconfirmed_log_count, OB_TENANT_PARAMETER, "1500", "[100, 50000]",
    "maximum of unconfirmed logs in clog module. "
    "Range: [100, 50000]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_ob_clog_timeout_to_force_switch_leader, OB_CLUSTER_PARAMETER, "10s", "[0s, 60m]",
    "When log sync is blocking, leader need wait this interval before revoke."
    "The default value is 0s, use 0s to close this function. Range: [0s, 60m]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_ob_clog_disk_buffer_cnt, OB_CLUSTER_PARAMETER, "64", "[1, 2000]", "clog disk buffer cnt. Range: [1, 2000]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_ob_trans_rpc_timeout, OB_CLUSTER_PARAMETER, "3s", "[0s, 3600s]",
    "transaction rpc timeout(s). Range: [0s, 3600s]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_early_lock_release, OB_TENANT_PARAMETER, "False", "enable early lock release",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//__enable_block_receiving_clog is obsolete
// DEF_BOOL(__enable_block_receiving_clog, OB_CLUSTER_PARAMETER, "True",
//         "If this option is set to true, block receiving clog for slave replicas when too much clog is waiting for
//         beening submited to replaying. The default is true", ObParameterAttr(Section::TRANS, Source::DEFAULT,
//         EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_trx_commit_retry_interval, OB_CLUSTER_PARAMETER, "200ms", "[100ms,)",
    "transaction commit retry interval. Range: [100ms,)",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_ob_get_gts_ahead_interval, OB_CLUSTER_PARAMETER, "0s", "[0s, 1s]", "get gts ahead interval. Range: [0s, 1s]",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_ob_enable_log_replica_strict_recycle_mode, OB_CLUSTER_PARAMETER, "True",
    "enable log replica strict recycle mode",
    ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//// rpc config
DEF_TIME(rpc_timeout, OB_CLUSTER_PARAMETER, "2s",
    "the time during which a RPC request is permitted to execute before it is terminated",
    ObParameterAttr(Section::RPC, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//// location cache config
DEF_INT(location_cache_priority, OB_CLUSTER_PARAMETER, "1000", "[1,)",
    "priority of location cache among all system caching service. Range: [1, +∞) in integer",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(location_cache_expire_time, OB_CLUSTER_PARAMETER, "600s", "[1s,)",
    "the expiration time for a partition location info in partition location cache. "
    "Not recommended for modification. Range: [1s, +∞)",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(virtual_table_location_cache_expire_time, OB_CLUSTER_PARAMETER, "8s", "[1s,)",
    "expiration time for virtual table location info in partiton location cache. "
    "Range: [1s, +∞)",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(location_refresh_thread_count, OB_CLUSTER_PARAMETER, "4", "(1,64]",
    "the number of threads "
    "that fetch the partition location information from the root service. Range: (1, 64]",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(force_refresh_location_cache_threshold, OB_CLUSTER_PARAMETER, "100", "[1,)",
    "location cache refresh threshold which use sql method in one second. Range: [1, +∞) in integer",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(force_refresh_location_cache_interval, OB_CLUSTER_PARAMETER, "2h", "[1s,)",
    "the max interval for refresh location cache by sql. Range: [1s, +∞)",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(location_fetch_concurrency, OB_CLUSTER_PARAMETER, "20", "[1, 1000]",
    "the maximum number of the tasks "
    "which fetch the partition location information concurrently. Range: [1, 1000]",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(location_cache_refresh_min_interval, OB_CLUSTER_PARAMETER, "100ms", "[0s,)",
    "the time interval in which no request for location cache renewal will be executed. "
    "The default value is 100 milliseconds. [0s, +∞)",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(location_cache_refresh_rpc_timeout, OB_CLUSTER_PARAMETER, "500ms", "[1ms,)",
    "The timeout used for refreshing location cache by RPC. Range: [1ms, +∞)",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(location_cache_refresh_sql_timeout, OB_CLUSTER_PARAMETER, "1s", "[1ms,)",
    "The timeout used for refreshing location cache by SQL. Range: [1ms, +∞)",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(all_server_list, OB_CLUSTER_PARAMETER, "", "all server addr in cluster",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_auto_refresh_location_cache, OB_CLUSTER_PARAMETER, "True", "enable auto refresh location",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(auto_refresh_location_cache_rate_limit, OB_CLUSTER_PARAMETER, "1000", "[1, 100000]",
    "Maximum number of partitions to refresh location automatically per second",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(auto_broadcast_location_cache_rate_limit, OB_CLUSTER_PARAMETER, "1000", "[1, 100000]",
    "Maximum number of partitions to broadcast location per second",
    ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//// cache config
DEF_INT(clog_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)", "clog cache priority. Range: [1, )",
    ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(index_clog_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)", "index clog cache priority. Range: [1, )",
    ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(user_tab_col_stat_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)",
    "user tab col stat cache priority. Range: [1, )",
    ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(index_cache_priority, OB_CLUSTER_PARAMETER, "10", "[1,)", "index cache priority. Range: [1, )",
    ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(index_info_block_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)",
    "index info block cache priority. Range: [1, )",
    ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(user_block_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)", "user block cache priority. Range: [1, )",
    ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(user_row_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)", "user row cache priority. Range: [1, )",
    ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(bf_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)", "bf cache priority. Range: [1, )",
    ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(bf_cache_miss_count_threshold, OB_CLUSTER_PARAMETER, "100", "[0,)",
    "bf cache miss count threshold, 0 means disable bf cache. Range: [0, )",
    ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(fuse_row_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)", "fuse row cache priority. Range: [1, )",
    ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// background limit config
DEF_INT(sys_bkgd_io_low_percentage, OB_CLUSTER_PARAMETER, "0", "[0,100]",
    "the low disk io percentage of sys io, sys io can use at least low percentage,"
    "when the value is 0, it will automatically set low limit for SATA and SSD disk to "
    "guarantee at least 128MB disk bandwidth. "
    "Range: [0,100]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(sys_bkgd_io_high_percentage, OB_CLUSTER_PARAMETER, "90", "[1,100]",
    "the high disk io percentage of sys io, sys io can use at most high percentage. "
    "Range: [1,100]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(sys_cpu_limit_trigger, OB_CLUSTER_PARAMETER, "80", "[50,)",
    "when the cpu usage percentage exceed the trigger, will limit the sys cpu usage "
    "Range: [50,)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_ob_sys_high_load_per_cpu_threshold, OB_CLUSTER_PARAMETER, "0", "[0,1000]",
    "when the load perf cpu exceed the trigger, will limit the background dag task"
    "Range: [0,1000]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(user_iort_up_percentage, OB_CLUSTER_PARAMETER, "100", "[0,)",
    "variable to control sys io, the percentage of use io rt can raise "
    "Range: [0,)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_data_storage_io_timeout, OB_CLUSTER_PARAMETER, "120s", "[5s,600s]",
    "io timeout for data storage, Range [5s,600s]. "
    "The default value is 120s",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(data_storage_warning_tolerance_time, OB_CLUSTER_PARAMETER, "30s", "[10s,300s]",
    "time to tolerate disk read failure, after that, the disk status will be set warning. Range [10s,300s]. The "
    "default value is 30s",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME_WITH_CHECKER(data_storage_error_tolerance_time, OB_CLUSTER_PARAMETER, "300s",
    common::ObDataStorageErrorToleranceTimeChecker, "[10s,7200s]",
    "time to tolerate disk read failure, after that, the disk status will be set error. Range [10s,7200s]. The default "
    "value is 300s",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(data_disk_usage_limit_percentage, OB_CLUSTER_PARAMETER, "90", "[50,100]",
    "the safe use percentage of data disk"
    "Range: [50,100] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(sys_bkgd_net_percentage, OB_CLUSTER_PARAMETER, "60", "[0,100]",
    "the net percentage of sys background net. Range: [0, 100] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT_WITH_CHECKER(disk_io_thread_count, OB_CLUSTER_PARAMETER, "8", common::ObConfigEvenIntChecker, "[2,32]",
    "The number of io threads on each disk. The default value is 8. Range: [2,32] in even integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_io_callback_thread_count, OB_CLUSTER_PARAMETER, "8", "[1,64]",
    "The number of io callback threads. The default value is 8. Range: [1,64] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_large_query_io_percentage, OB_CLUSTER_PARAMETER, "0", "[0,100]",
    "the max percentage of io resource for big queries. Range: [0,100] in integer. Especially, 0 means unlimited. The "
    "default value is 0.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_parallel_minor_merge, OB_TENANT_PARAMETER, "False",
    "specifies whether to enable parallel minor merge. "
    "Value: True:turned on;  False: turned off",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(merge_thread_count, OB_CLUSTER_PARAMETER, "0", "[0,256]",
    "the current work thread num of daily merge. Range: [0,256] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(minor_merge_concurrency, OB_CLUSTER_PARAMETER, "0", "[0,64]",
    "the current work thread num of minor merge. Range: [0,64] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_mini_merge_concurrency, OB_CLUSTER_PARAMETER, "5", "[0,64]",
    "the current work thread num of mini merge. Range: [0,64] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(restore_concurrency, OB_CLUSTER_PARAMETER, "0", "[0,512]",
    "the current work thread num of restore macro block. Range: [0,512] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(merge_stat_sampling_ratio, OB_CLUSTER_PARAMETER, "100", "[0,100]",
    "column stats sampling ratio daily merge. Range: [0,100] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(minor_freeze_times, OB_CLUSTER_PARAMETER, "100", "[0, 65535]",
    "specifies how many minor freezes should be triggered between two major freezes. Range: [0, 65535]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(minor_compact_trigger, OB_CLUSTER_PARAMETER, "2", "[0,16]", "minor_compact_trigger, Range: [0,16] in integer",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_compaction_diagnose, OB_CLUSTER_PARAMETER, "False",
    "enable compaction diagnose function"
    "Value:  True:turned on;  False: turned off",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_private_buffer_size, OB_CLUSTER_PARAMETER, "2M",
    "[0B,)"
    "the trigger remaining data size within transaction for immediate logging, 0B represents not trigger immediate "
    "logging"
    "Range: [0B, total size of memory]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_fast_commit, OB_CLUSTER_PARAMETER, "False",
    "whether to enable fast commit strategy"
    "Value:  True:turned on;  False: turned off",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_sparse_row, OB_CLUSTER_PARAMETER, "False",
    "whether enable using sparse row in SSTable"
    "Value:  True:turned on;  False: turned off",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_minor_compaction_amplification_factor, OB_CLUSTER_PARAMETER, "0", "[0,100]",
    "the L1 compaction write amplification factor, 0 means default 25, Range: [0,100] in integer",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_minor_compaction_interval, OB_CLUSTER_PARAMETER, "0s", "[0s,30m]",
    "the time interval to start next minor compaction, Range: [0s,30m]"
    "Range: [0s, 30m)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_follower_replica_merge_level, OB_CLUSTER_PARAMETER, "2", "[0,3]",
    "the merge level for d replica migrate, 0: nomore any compaction; 1: self minor; 2: self major; 3: major+minor"
    "Range: [0,3] in integer",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(major_compact_trigger, OB_CLUSTER_PARAMETER, "5", "[0,65535]",
    "major_compact_trigger alias to minor_freeze_times, Range: [0,65535] in integer",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_ob_enable_fast_freeze, OB_TENANT_PARAMETER, "True",
    "specifies whether fast freeze for elr table is enabled"
    "Value: True:turned on;  False: turned off",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_ob_elr_fast_freeze_threshold, OB_CLUSTER_PARAMETER, "500000",
    "[10000,)"
    "per row update counts threshold to trigger minor freeze for tables with ELR optimization",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_ob_minor_merge_schedule_interval, OB_CLUSTER_PARAMETER, "20s", "[3s,3m]",
    "the time interval to schedule minor mrerge, Range: [3s,3m]"
    "Range: [3s, 3m]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(sys_bkgd_migration_retry_num, OB_CLUSTER_PARAMETER, "3", "[3,100]",
    "retry num limit during migration. Range: [3, 100] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(sys_bkgd_migration_change_member_list_timeout, OB_CLUSTER_PARAMETER, "1h", "[0s,24h]",
    "the timeout for migration change member list retry. "
    "The default value is 1h. Range: [0s,24h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_election_group, OB_CLUSTER_PARAMETER, "True",
    "specifies whether election group is turned on. "
    "Value:  True:turned on;  False: turned off",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_trans_ctx_size_limit, OB_TENANT_PARAMETER, "True",
    "specifies whether trans ctx size limit is turned on or not. "
    "Value: True:turned on;  False: turned off",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// Tablet config
DEF_CAP_WITH_CHECKER(tablet_size, OB_CLUSTER_PARAMETER, "128M", common::ObConfigTabletSizeChecker,
    "default tablet size, has to be a multiple of 2M",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// PartitionTable config
DEF_INT(meta_table_read_write_mode, OB_CLUSTER_PARAMETER, "2", "[0,2]",
    "meta table read write mode. Range: [0,2] in integer. "
    "0 : read write __all_meta_table "
    "1 : read write __all_meta_table while write __all_tenant_meta_table "
    "2 : read write __all_tenant_meta_table "
    "The default value is 0",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(builtin_db_data_verify_cycle, OB_CLUSTER_PARAMETER, "20", "[0, 360]",
    "check cycle of db data. Range: [0, 360] in integer. Unit: day. "
    "0: check nothing. "
    "1-360: check all data every specified days. "
    "The default value is 20. "
    "The real check cycle maybe longer than the specified value for insuring performance.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(micro_block_merge_verify_level, OB_CLUSTER_PARAMETER, "2", "[0,3]",
    "specify what kind of verification should be done when merging micro block. "
    "0 : no verification will be done "
    "1 : verify encoding algorithm, encoded micro block will be read to ensure data is correct "
    "2 : verify encoding and compression algorithm, besides encoding verification, compressed block will be "
    "decompressed to ensure data is correct"
    "3 : verify encoding, compression algorithm and lost write protect",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_migrate_block_verify_level, OB_CLUSTER_PARAMETER, "1", "[0,2]",
    "specify what kind of verification should be done when migrating macro block. "
    "0 : no verification will be done "
    "1 : physical verification"
    "2 : logical verification",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_cache_wash_interval, OB_CLUSTER_PARAMETER, "200ms", "[1ms, 1m]", "specify interval of cache background wash",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_max_partition_cnt_per_server, OB_CLUSTER_PARAMETER, "500000", "[10000, 500000]",
    "specify max partition count on one observer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// for test
DEF_BOOL(__schema_split_mode, OB_CLUSTER_PARAMETER, "True", "determinate if observer start with schema split mode.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

#ifdef CLOG_MODULE_TEST
DEF_INT(module_test_clog_cache_max_hit_rate, OB_CLUSTER_PARAMETER, "100", "[0,100]",
    "module test: clog cache max hit rate, 0 means disable clog cache. Range: [0,100]",
    ObParameterAttr(Section::CLOG, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(module_test_clog_packet_loss_rate, OB_CLUSTER_PARAMETER, "0", "[0, 100]",
    "module test: clog packet loss rate, 0 means no packet loss deliberately. Range: [0,100]",
    ObParameterAttr(Section::CLOG, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(module_test_clog_ignore_log_id_in_election_priority, OB_CLUSTER_PARAMETER, "False",
    "module test: on_get_election_priority whether refer to log id",
    ObParameterAttr(Section::CLOG, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(module_test_clog_delay_submit, OB_CLUSTER_PARAMETER, "False",
    "module test: delay submiting operation by 50 ms, one percent. simulate logs out of order",
    ObParameterAttr(Section::CLOG, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(module_test_clog_mem_alloc_thrashing, OB_CLUSTER_PARAMETER, "False",
    "module test: clog allocator will fail one per mille deliberately if true",
    ObParameterAttr(Section::CLOG, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(module_test_clog_flush_delay, OB_CLUSTER_PARAMETER, "False",
    "module test: flush will delay random ([0~10ms]) if true",
    ObParameterAttr(Section::CLOG, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
#endif  // CLOG_MODULE_TEST

// TODO : remove
DEF_CAP(multiblock_read_size, OB_CLUSTER_PARAMETER, "128K", "[0K,2M]",
    "multiple block batch read size in one read io request. Range: [0K,2M]",
    ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(multiblock_read_gap_size, OB_CLUSTER_PARAMETER, "0K", "[0K,2M]",
    "max gap size in one read io request, gap means blocks that hit in block cache. Range: [0K,2M]",
    ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// TODO : to be remove
DEF_CAP(dtl_buffer_size, OB_CLUSTER_PARAMETER, "64K", "[4K,2M]", "to be removed",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// TODO : to be remove
DEF_CAP(px_task_size, OB_CLUSTER_PARAMETER, "2M", "[2M,)", "to be removed",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// TODO : to be removed
DEF_BOOL(enable_smooth_leader_switch, OB_CLUSTER_PARAMETER, "true", "to be removed",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(migrate_concurrency, OB_CLUSTER_PARAMETER, "10", "[0,64]",
    "set concurrency of migration, set upper limit to migrate_concurrency and set lower limit to "
    "migrate_concurrency/2, range: [0,64]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_max_elr_dependent_trx_count, OB_CLUSTER_PARAMETER, "0", "[0,)", "max elr dependent transaction count",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_INT(skip_report_pg_backup_task_table_id, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "skip_report_pg_backup_task table id"
    "Range: [0,) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(fake_archive_log_checkpoint_ts, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "fake_archive_log_checkpoint_ts"
    "Range: [0,) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(fake_archive_log_status, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "fake_archive_log_status"
    "Range: [0,) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(fake_once_init_restore_macro_index_store, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "0 for not fake, 1 for major, 2 for minor"
    "Range: [0,) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(enable_disk_error_test, OB_CLUSTER_PARAMETER, "False",
    "when disk error test mode is enabled, observer send disk error "
    "status to rs in lease request",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(skip_update_estimator, OB_CLUSTER_PARAMETER, "False", "skip update_estimator during daily merge. ",
    ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(with_new_migrated_replica, OB_CLUSTER_PARAMETER, "False",
    "it is new migrated replica, can not be removed by old migration. ",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(sys_bkgd_migration_large_task_threshold, OB_CLUSTER_PARAMETER, "2h", "[0s,24h]",
    "the timeout for migration change member list retry. "
    "The default value is 2h. Range: [0s,24h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(fail_write_checkpoint_alert_interval, OB_CLUSTER_PARAMETER, "6h", "[0s,24h]",
    "fail_write_checkpoint_alert_interval. "
    "The default value is 6h. Range: [0s,24h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(retry_write_checkpoint_min_interval, OB_CLUSTER_PARAMETER, "1h", "[0s,24h]",
    "retry_write_checkpoint_min_interval. "
    "The default value is 1h. Range: [0s,24h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(macro_block_hold_alert_time, OB_CLUSTER_PARAMETER, "12h", "[0s,24h]",
    "macro_block_hold_alert_time. "
    "The default value is 12h. Range: [0s,24h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(macro_block_builder_errsim_flag, OB_CLUSTER_PARAMETER, "0", "[0,100]",
    "macro_block_builder_errsim_flag"
    "Range: [0,100] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(mark_and_sweep_interval, OB_CLUSTER_PARAMETER, "60s", "[0s,24h]",
    "mark_and_sweep_interval. "
    "The default value is 60s. Range: [0s,24h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(skip_balance_disk_limit, OB_CLUSTER_PARAMETER, "False", "skip_dbalance_isk_limit. ",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(pwrite_errsim, OB_CLUSTER_PARAMETER, "False", "pwrite_errsim",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(fake_disk_error, OB_CLUSTER_PARAMETER, "False", "fake_disk_error",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(fake_remove_member_error, OB_CLUSTER_PARAMETER, "0", "[0,100]",
    "fake_remove_member_error"
    "Range: [0,100] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(fake_add_member_error, OB_CLUSTER_PARAMETER, "0", "[0,100]",
    "fake_remove_member_error"
    "Range: [0,100] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(fake_wait_batch_member_chagne_done, OB_CLUSTER_PARAMETER, "0", "[0,100]",
    "fake_wait_batch_member_chagne_done"
    "Range: [0,100] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(backup_backup_archive_log_batch_count, OB_CLUSTER_PARAMETER, "0", "[1,1024]",
    "backup_backup_archive_log_batch_count"
    "Range: [1,1024] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(backup_backup_archivelog_retry_interval, OB_CLUSTER_PARAMETER, "0", "[0s, 1h]",
    "control backup backup archivelog retry interval. "
    "Range: [0s, 1h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(backup_backupset_batch_count, OB_CLUSTER_PARAMETER, "0", "[1,1024]",
    "backup_backupset_batch_count"
    "Range: [1,1024] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(backup_backupset_retry_interval, OB_CLUSTER_PARAMETER, "0", "[0s, 1h]",
    "control backup backupset retry interval. "
    "Range: [0s, 1h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(max_wait_batch_member_change_done_us, OB_CLUSTER_PARAMETER, "300s", "[0s,24h]",
    "max_wait_batch_member_change_done_us. "
    "The default value is 300s. Range: [0s,24h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(allow_major_sstable_merge, OB_CLUSTER_PARAMETER, "True", "allow_major_sstable_merge. ",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(trigger_reuse_table_in_table_mgr, OB_CLUSTER_PARAMETER, "False", "trigger_reuse_table_in_table_mgr",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(fake_backup_date_for_incremental_backup, OB_CLUSTER_PARAMETER, "False",
    "incremental backup use fake backup date",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_CAP(slog_size, OB_CLUSTER_PARAMETER, "256M", "[0M,256M]", "size of the slog file. Range: [0, 256M]",
    ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(fake_replay_start_point, OB_CLUSTER_PARAMETER, "False", "fake_replay_start_point",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(allow_batch_remove_member_during_change_replica, OB_CLUSTER_PARAMETER, "True",
    "allow_batch_remove_member_during_change_replica",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(allow_batch_change_member, OB_CLUSTER_PARAMETER, "True", "allow_batch_change_member",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(schema_drop_gc_delay_time, OB_CLUSTER_PARAMETER, "1800s", "[0s, 24h]",
    "max delay time for schema_drop gc."
    "The default value is 1800s. Range: [0s, 24h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(_ob_enable_rebuild_on_purpose, OB_CLUSTER_PARAMETER, "False",
    "whether open rebuild test mode in observer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(_max_migration_status_count, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "the max count of __all_virtual_partition_migration_status"
    "Range: [0,max) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(_max_block_per_backup_task, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "the max macro block count per backup sub task"
    "Range: [0,max) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(_max_backup_piece_id, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "the max backup log archive piece id that the RS can switch to, Range: [0,max) in integer, 0 means that RS can "
    "switch to any piece",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(ilog_flush_trigger_time, OB_CLUSTER_PARAMETER, "1800s", "[0s, 24h]",
    "max trigger time for ilog flush."
    "The default value is 1800s. Range: [0s, 24h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(enable_ilog_recycle, OB_CLUSTER_PARAMETER, "true", "switch for ilog recycling",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(enable_block_log_archive, OB_CLUSTER_PARAMETER, "False", "switch for blcoking log_archive",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(skip_buffer_minor_merge, OB_CLUSTER_PARAMETER, "False", "switch for buffer minor merge",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(skip_update_storage_info, OB_CLUSTER_PARAMETER, "False", "switch for update storage info",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(mock_oss_delete, OB_CLUSTER_PARAMETER, "False", "mock device touch and delete",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(mock_nfs_device_touch, OB_CLUSTER_PARAMETER, "False", "mock device touch and delete",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(incremental_backup_limit, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "the max count of incremental backup"
    "Range: [0,max) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(backup_lease_takeover_time, OB_CLUSTER_PARAMETER, "10s", "[1s, 5m]",
    "Lease Takeover Time for Rootserver Backup heartbeat. Range: [1s, 5m]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(fake_report_server_tenant_start_ts, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "for test start ts rollback"
    "Range: [0,max) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_INT(_backup_pg_retry_max_count, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "the max of one pg backup retry count, Range: [0,max) in integer, 0 means that pg backup no need retry",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_INT(_backup_pg_max_batch_count, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "the max of pg backup batch count, Range: [0,max) in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

#ifdef TRANS_MODULE_TEST
DEF_INT(module_test_trx_memory_errsim_percentage, OB_CLUSTER_PARAMETER, "0", "[0, 100]",
    "the percentage of memory errsim. Rang:[0, 100]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(module_test_trx_fake_commit, OB_TENANT_PARAMETER, "false", "module test: transaction fake commit",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
#endif

DEF_CAP(sql_work_area, OB_TENANT_PARAMETER, "1G", "[10M,)", "Work area memory limitation for tenant",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_CAP(_max_trx_size, OB_CLUSTER_PARAMETER, "100G", "[0B,)",
    "the limit for memstore memory used per partition involved in each database transaction",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(__easy_memory_limit, OB_CLUSTER_PARAMETER, "4G", "[1G,)",
    "max memory size which can be used by libeasy. The default value is 4G. Range: [1G,)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(stack_size, OB_CLUSTER_PARAMETER, "1M", "[512K, 20M]",
    "the size of routine execution stack"
    "Range: [512K, 20M]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_INT(__easy_memory_reserved_percentage, OB_CLUSTER_PARAMETER, "0", "[0,100]",
    "the percentage of easy memory reserved size. The default value is 0. Range: [0,100]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_px_max_pipeline_depth, OB_CLUSTER_PARAMETER, "2", "[2,3]",
    "max parallel execution pipeline depth, "
    "range: [2,3]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// ssl
DEF_BOOL(ssl_client_authentication, OB_CLUSTER_PARAMETER, "False",
    "enable server SSL support. Takes effect after ca/cert/key file is configured correctly. ",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_alter_column_mode, OB_CLUSTER_PARAMETER, "2", "[0,2]",
    "use to check if alter column is allowed. "
    "0: always forbid; 1: always allow; 2: forbid when major freeze is not finished; "
    "Range: [0,2] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(use_ipv6, OB_CLUSTER_PARAMETER, "False", "Whether this server uses ipv6 address",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));

DEF_TIME(_ob_ddl_timeout, OB_CLUSTER_PARAMETER, "1000s", "[1s,)",
    "the config parameter of ddl timeout"
    "Range: [1s, +∞)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// backup
DEF_STR(backup_dest, OB_CLUSTER_PARAMETER, "", "backup dest",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(backup_backup_dest, OB_CLUSTER_PARAMETER, "", "backup backup dest",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(backup_dest_option, OB_CLUSTER_PARAMETER, "", "backup_dest_option",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(backup_backup_dest_option, OB_CLUSTER_PARAMETER, "", "backup_backup_dest_option",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_LOG_ARCHIVE_OPTIONS_WITH_CHECKER(backup_log_archive_option, OB_CLUSTER_PARAMETER, "OPTIONAL COMPRESSION=ENABLE",
    common::ObConfigLogArchiveOptionsChecker, "backup log archive option, support MANDATORY/OPTIONAL, COMPRESSION",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(backup_concurrency, OB_CLUSTER_PARAMETER, "0", "[0,100]",
    "backup concurrency limit. "
    "Range: [0, 100] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_CAP(backup_net_limit, OB_CLUSTER_PARAMETER, "0M", "[0M,)",
    "backup net limit for whole cluster"
    "Range: [0M, max)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(enable_log_archive, OB_CLUSTER_PARAMETER, "False", "control if enable log archive",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(log_restore_concurrency, OB_CLUSTER_PARAMETER, "10", "[1,]",
    "concurrency for log restoring"
    "Range: [1, ] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(log_archive_concurrency, OB_CLUSTER_PARAMETER, "0", "[0,]",
    "concurrency  for log_archive_sender and log_archive_spiter"
    "Range: [0, ] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// DEF_INT(_log_archive_task_ratio, OB_CLUSTER_PARAMETER, "4", "[0,100]",
//        "to add"
//        "Range: [0, 100] in integer",
//        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(log_archive_checkpoint_interval, OB_CLUSTER_PARAMETER, "120s", "[5s, 1h]",
    "control interval of generating log archive checkpoint for cold partition"
    "Range: [5s, 1h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_CAP(log_archive_batch_buffer_limit, OB_CLUSTER_PARAMETER, "1G", "[4M,)",
    "batch buffer limit for log archive, capacity smaller than 1G only for mini mode, "
    "Range: [4M, max)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(gc_wait_archive, OB_CLUSTER_PARAMETER, "True",
    "control whether partition GC need to wait for all partition log be archived",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(backup_recovery_window, OB_CLUSTER_PARAMETER, "0", "[0,)",
    "backup expired day limit, 0 means not expired"
    "Range: [0, max) in time interval, for example 7d",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(backup_region, OB_CLUSTER_PARAMETER, "", common::ObConfigBackupRegionChecker,
    "user suggest backup region", ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(backup_zone, OB_CLUSTER_PARAMETER, "", common::ObConfigBackupRegionChecker,
    "user suggest backup zone, format like z1,z2;z3,z4;z5",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(auto_delete_expired_backup, OB_CLUSTER_PARAMETER, "False", "control if auto delete expired backup",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_auto_update_reserved_backup_timestamp, OB_CLUSTER_PARAMETER, "False",
    "control if auto update reserved backup timestamp.True means only updating needed backup data file "
    "timestamp which usually used in OSS without delete permission.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_backup_retry_timeout, OB_CLUSTER_PARAMETER, "10m", "[10s, 1h]",
    "control backup retry timeout. "
    "Range: [10s, 1h]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(ob_enable_batched_multi_statement, OB_TENANT_PARAMETER, "False", "enable use of batched multi statement",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_single_zone_deployment_on, OB_CLUSTER_PARAMETER, "False",
    "specify cluster deployment mode, "
    "True: single zone deployment; False: multiple zone deploment, Default: False",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::READONLY));
DEF_BOOL(_enable_ha_gts_full_service, OB_CLUSTER_PARAMETER, "False",
    "enable ha gts full service, default false for general oceanbase version, true for tpcc version. ",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::READONLY));
DEF_INT(_bloom_filter_ratio, OB_CLUSTER_PARAMETER, "35", "[0, 100]",
    "the px bloom filter false-positive rate.the default value is 1, range: [0,100]",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_restore_idle_time, OB_CLUSTER_PARAMETER, "1m", "[10s,]",
    "the time interval between the schedules of physical restore task. "
    "Range: [10s, +∞)",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_backup_idle_time, OB_CLUSTER_PARAMETER, "5m", "[10s,]",
    "the time interval between the schedules of physical backup task. "
    "Range: [10s, +∞)",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_ob_enable_prepared_statement, OB_CLUSTER_PARAMETER, "False", "control if enable prepared statement",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(_upgrade_stage, OB_CLUSTER_PARAMETER, "NONE", common::ObConfigUpgradeStageChecker,
    "specifies the upgrade stage. "
    "NONE: in non upgrade stage, "
    "PREUPGRADE: in pre upgrade stage, "
    "DBUPGRADE: in db uprade stage, "
    "POSTUPGRADE: in post upgrade stage. ",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::READONLY));
DEF_STR_WITH_CHECKER(_ob_plan_cache_gc_strategy, OB_CLUSTER_PARAMETER, "REPORT", common::ObConfigPlanCacheGCChecker,
    "OFF: disabled, "
    "REPORT: check leaked cache object infos only, "
    "AUTO: check and release leaked cache obj.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_plan_cache_mem_diagnosis, OB_CLUSTER_PARAMETER, "False",
    "wether turn plan cache ref count diagnosis on",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_clog_aggregation_buffer_amount, OB_TENANT_PARAMETER, "0", "[0, 128]", "the amount of clog aggregation buffer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(schema_history_recycle_interval, OB_CLUSTER_PARAMETER, "10m", "[0s,]",
    "the time interval between the schedules of schema history recyle task. "
    "Range: [0s, +∞)",
    ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_flush_clog_aggregation_buffer_timeout, OB_TENANT_PARAMETER, "0ms", "[0ms, 100ms]",
    "the timeout for flushing clog aggregation buffer. Range: [0ms, 100ms]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_oracle_priv_check, OB_CLUSTER_PARAMETER, "False", "whether turn on oracle privilege check ",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// for pl/sql compiler
DEF_STR(plsql_ccflags, OB_TENANT_PARAMETER, "",
    "provides a mechanism that allows PL/SQL programmers to control"
    "conditional compilation of each PL/SQL library unit independently",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(plsql_code_type, OB_TENANT_PARAMETER, "native", "specifies the compilation mode for PL/SQL library units",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(plsql_debug, OB_TENANT_PARAMETER, "False",
    "specifies whether or not PL/SQL library units will be compiled for debugging",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(plsql_optimize_level, OB_TENANT_PARAMETER, "1",
    "specifies the optimization level that will be used to"
    "compile PL/SQL library units. The higher the setting of this parameter, the more effort"
    "the compiler makes to optimize PL/SQL library units",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(plsql_v2_compatibility, OB_TENANT_PARAMETER, "False",
    "allows some abnormal behavior that Version 8 disallows, not available",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(plsql_warnings, OB_TENANT_PARAMETER, "DISABLE::ALL",
    "enables or disables the reporting of warning messages by the"
    "PL/SQL compiler, and specifies which warning messages to show as errors",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// for bloom filter
DEF_BOOL(_bloom_filter_enabled, OB_TENANT_PARAMETER, "True", "enable join bloom filter",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(use_large_pages, OB_CLUSTER_PARAMETER, "false", common::ObConfigUseLargePagesChecker,
    "used to manage the database's use of large pages, "
    "values: false, true, only",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));

DEF_STR(ob_ssl_invited_common_names, OB_TENANT_PARAMETER, "NONE",
    "when server use ssl, use it to control client identity with ssl subject common name. default NONE",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR(ssl_external_kms_info, OB_CLUSTER_PARAMETER, "",
    "when using the external key management center for ssl, "
    "this parameter will store some key management information",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(_ob_ssl_invited_nodes, OB_CLUSTER_PARAMETER, "NONE",
    "when rpc need use ssl, we will use it to store invited server ipv4 during grayscale change."
    "when it is finish, it can use ALL instead of all server ipv4",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_max_schema_slot_num, OB_CLUSTER_PARAMETER, "128", "[2,8192]",
    "the max schema slot number for each tenant, "
    "Range: [2, 8192] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_fulltext_index, OB_CLUSTER_PARAMETER, "False", "enable full text index",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT_WITH_CHECKER(_ob_query_rate_limit, OB_TENANT_PARAMETER, "-1", common::ObConfigQueryRateLimitChecker,
    "the maximum throughput allowed for a tenant per observer instance",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_xa_gc_timeout, OB_CLUSTER_PARAMETER, "24h", "[1s,)",
    "specifies the threshold value for a xa record to be considered as obsolete",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_xa_gc_interval, OB_CLUSTER_PARAMETER, "1h", "[1s,)", "specifies the scan interval of the gc worker",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// enable easy-level keepalive
DEF_BOOL(_enable_easy_keepalive, OB_CLUSTER_PARAMETER, "False", "enable keepalive for each TCP connection.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(open_cursors, OB_TENANT_PARAMETER, "50", "[0,65535]",
    "specifies the maximum number of open cursors a session can have at once."
    "can use this parameter to prevent a session from opening an excessive number of cursors."
    "Range: [0, 65535] in integer",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(enable_tcp_keepalive, OB_CLUSTER_PARAMETER, "true",
    "enable TCP keepalive for the TCP connection of sql protocol. Take effect for "
    "new established connections.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(tcp_keepidle, OB_CLUSTER_PARAMETER, "7200s", "[1s,]",
    "The time (in seconds) the connection needs to remain idle before TCP "
    "starts sending keepalive probe. Take effect for new established connections. "
    "Range: [1s, +∞]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(tcp_keepintvl, OB_CLUSTER_PARAMETER, "6s", "[1s,]",
    "The time (in seconds) between individual keepalive probes. Take effect for new "
    "established connections. Range: [1s, +∞]",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(tcp_keepcnt, OB_CLUSTER_PARAMETER, "10", "[1,]",
    "The maximum number of keepalive probes TCP should send before dropping "
    "the connection. Take effect for new established connections. Range: [1,+∞)",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(ilog_index_expire_time, OB_CLUSTER_PARAMETER, "7d", "[0s, 60d]",
    "specifies the expire time of ilog_index, can use this parameter to limit the"
    "memory usage of file_id_cache",
    ObParameterAttr(Section::CLOG, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// auto drop restoring tenant if physical restore fails
DEF_BOOL(_auto_drop_tenant_if_restore_failed, OB_CLUSTER_PARAMETER, "True",
    "auto drop restoring tenant if physical restore fails",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(ob_proxy_readonly_transaction_routing_policy, OB_TENANT_PARAMETER, "true",
    "Proxy route policy for readonly sql",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_block_file_punch_hole, OB_CLUSTER_PARAMETER, "False",
    "specifies whether to punch whole when free blocks in block_file",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_ob_enable_px_for_inner_sql, OB_CLUSTER_PARAMETER, "true",
    "specifies whether inner sql uses px. "
    "The default value is TRUE. Value: TRUE: turned on FALSE: turned off",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// ttl
DEF_STR_WITH_CHECKER(kv_ttl_duty_duration, OB_TENANT_PARAMETER, "[0:00:00, 24:00:00]", common::ObTTLDutyDurationChecker,
    "ttl background task working time duration"
    "begin_time or end_time in Range [0:00:00, 24:00:00]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(kv_ttl_history_recycle_interval, OB_TENANT_PARAMETER, "7d", "[1d, 180d]",
    "the time to recycle ttl history. Range: [1d, 180d]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_kv_ttl, OB_TENANT_PARAMETER, "False", "specifies whether ttl task is enbled",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// query response time
DEF_BOOL(query_response_time_stats, OB_TENANT_PARAMETER, "False",
    "Enable or disable QUERY_RESPONSE_TIME statistics collecting"
    "The default value is False. Value: TRUE: turned on FALSE: turned off",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(query_response_time_flush, OB_TENANT_PARAMETER, "False",
    "Flush QUERY_RESPONSE_TIME table and re-read query_response_time_range_base"
    "The default value is False. Value: TRUE: trigger flush FALSE: do not trigger",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(query_response_time_range_base, OB_TENANT_PARAMETER, "10", "[2,10000]",
    "Select base of log for QUERY_RESPONSE_TIME ranges. WARNING: variable change takes affect only after flush."
    "The default value is False. Value: TRUE: trigger flush FALSE: do not trigger",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
