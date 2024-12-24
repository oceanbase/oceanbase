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
// background information about disk space configuration
// ObServerUtils::get_data_disk_info_in_config()
DEF_CAP(datafile_size, OB_CLUSTER_PARAMETER, "0M", "[0M,)", "size of the data file. Range: [0, +∞)",
        ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(datafile_next, OB_CLUSTER_PARAMETER, "0", "[0M,)", "the auto extend step. Range: [0, +∞)",
        ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(datafile_maxsize, OB_CLUSTER_PARAMETER, "0", "[0M,)", "the auto extend max size. Range: [0, +∞)",
        ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(datafile_disk_percentage, OB_CLUSTER_PARAMETER, "0", "[0,99]",
        "the percentage of disk space used by the data files. Range: [0,99] in integer",
        ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_datafile_usage_upper_bound_percentage, OB_CLUSTER_PARAMETER, "90", "[5,99]",
        "the percentage of disk space usage upper bound to trigger datafile extend. Range: [5,99] in integer",
        ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_datafile_usage_lower_bound_percentage, OB_CLUSTER_PARAMETER, "10", "[5,99]",
        "the percentage of disk space usage lower bound to trigger datafile shrink. Range: [5,99] in integer",
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
DEF_INT(rpc_port, OB_CLUSTER_PARAMETER, "2882", "(1024,65536)",
        "the port number for RPC protocol. Range: (1024, 65536) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(mysql_port, OB_CLUSTER_PARAMETER, "2881", "(1024,65536)",
        "port number for mysql connection. Range: (1024, 65536) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(devname, OB_CLUSTER_PARAMETER, "bond0", "name of network adapter",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::READONLY));
DEF_STR(zone, OB_CLUSTER_PARAMETER, "", "specifies the zone name",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(ob_startup_mode, OB_CLUSTER_PARAMETER, "NORMAL", "specifies the observer startup mode",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::READONLY));
DEF_TIME(internal_sql_execute_timeout, OB_CLUSTER_PARAMETER, "30s", "[1000us, 1h]",
         "the number of microseconds an internal DML request is permitted to "
         "execute before it is terminated. Range: [1000us, 1h]",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(net_thread_count, OB_CLUSTER_PARAMETER, "0", "[0,64]",
        "the number of rpc/mysql I/O threads for Libeasy. Range: [0, 64] in integer, 0 stands for max(6, CPU_NUM/8)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_INT(high_priority_net_thread_count, OB_CLUSTER_PARAMETER, "0", "[0,64]",
        "the number of rpc I/O threads for high priority messages, 0 means set off. Range: [0, 64] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_INT(tenant_task_queue_size, OB_CLUSTER_PARAMETER, "16384", "[1024,]",
        "the size of the task queue for each tenant. Range: [1024,+∞)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
_DEF_PARAMETER_SCOPE_CHECKER_EASY(private, Capacity, memory_limit, OB_CLUSTER_PARAMETER, "0M",
        common::ObConfigMemoryLimitChecker, "[0M,)",
        "the size of the memory reserved for internal use(for testing purpose), 0 means follow memory_limit_percentage. Range: 0, [1G,).",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
_DEF_PARAMETER_SCOPE_RANGE_EASY(private, Capacity, system_memory, OB_CLUSTER_PARAMETER, "0M", "[0M,)",
        "the memory reserved for internal use which cannot be allocated to any outer-tenant, "
        "and should be determined to guarantee every server functions normally. Range: [0M,)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(cpu_count, OB_CLUSTER_PARAMETER, "0", "[0,]",
        "the number of CPU\\'s in the system. "
        "If this parameter is set to zero, the number will be set according to sysconf; "
        "otherwise, this parameter is used. Range: [0,+∞) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(trace_log_slow_query_watermark, OB_CLUSTER_PARAMETER, "1s", "[1ms,]",
        "the threshold of execution time (in milliseconds) of a query beyond "
        "which it is considered to be a \\'slow query\\'. Range: [1ms,+∞)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_record_trace_log, OB_CLUSTER_PARAMETER, "True",
         "specifies whether to always record the trace log. The default value is True.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(max_string_print_length, OB_CLUSTER_PARAMETER, "500", "[0,]",
        "truncate very long string when printing to log file. Range:[0,]",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_sql_audit, OB_CLUSTER_PARAMETER, "true",
         "specifies whether SQL audit is turned on. "
         "The default value is TRUE. Value: TRUE: turned on FALSE: turned off",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_record_trace_id, OB_CLUSTER_PARAMETER, "False",
         "specifies whether record app trace id is turned on.",
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
DEF_TIME(dead_socket_detection_timeout, OB_CLUSTER_PARAMETER, "3s", "[0s,2h]",
         "specify a tcp_user_timeout for RFC5482. "
         "A zero value makes the option disabled, Range: [0, 2h]",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_perf_event, OB_CLUSTER_PARAMETER, "True",
         "specifies whether to enable perf event feature. The default value is True.",
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

DEF_STR_WITH_CHECKER(_publish_schema_mode, OB_TENANT_PARAMETER, "BEST_EFFORT",
                     common::ObConfigPublishSchemaModeChecker,
                     "specify the inspection of schema synchronous status after ddl transaction commits"
                     "values: BEST_EFFORT, ASYNC",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(default_compress_func, OB_CLUSTER_PARAMETER, "zstd_1.3.8",
                     common::ObConfigCompressFuncChecker,
                     "default compress function name for create new table, "
                     "values: none, lz4_1.0, snappy_1.0, zstd_1.0, zstd_1.3.8",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(default_row_format, OB_CLUSTER_PARAMETER, "dynamic",
                     common::ObConfigRowFormatChecker,
                     "default row format in mysql mode",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(default_compress, OB_CLUSTER_PARAMETER, "archive",
                     common::ObConfigCompressOptionChecker,
                     "default compress strategy for create new table within oracle mode",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(default_table_store_format, OB_TENANT_PARAMETER, "row",
                     common::ObConfigTableStoreFormatChecker,
                     "Specify the default storage format of creating table: row, column, compound format of row and column"
                     "values: row, column, compound",
                     ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_no_logging, OB_TENANT_PARAMETER, "False",
         "set true to skip writing ddl clog when all server using the same oss in shared storage mode",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(storage_rowsets_size, OB_TENANT_PARAMETER, "8192", "(0,1048576]",
        "the row number processed by vectorized storage engine within one batch in column storage. Range: (0,1048576]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(weak_read_version_refresh_interval, OB_CLUSTER_PARAMETER, "100ms", "[50ms,)",
         "the time interval to refresh cluster weak read version "
         "Range: [50ms, +∞)",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// for observer, root_server_list in format ip1:rpc_port1:sql_port1;ip2:rpc_port2:sql_port2
DEF_STR_LIST(rootservice_list, OB_CLUSTER_PARAMETER, "",
             "a list of servers against which election candidate is checked for validation",
             ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(cluster, OB_CLUSTER_PARAMETER, "obcluster", "Name of the cluster",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// drc复制的时候,使用的ob_org_cluster_id的范围是[0xffff0000,0xffffffff],
// 而用户的sql写入到clog中的cluster_id的范围必须不能在[0xffff0000,0xffffffff]之内,
// 用户的sql写入到clog中的cluster_id就是由系统配置项中的cluster_id决定的,
// 因此这里的系统配置项中的cluster_id的范围为[1, 0xffff0000 - 1],也就是[1,4294901759]。
// cluser_id的默认值为0,不在它的值域[1,4294901759]之内,也就是说,
// 在检查ObServerConfig对象的合法性之前必须要为cluster_id赋一个[1,4294901759]范围内的值。
DEF_INT(cluster_id, OB_CLUSTER_PARAMETER, "0", "[1,4294901759]", "ID of the cluster",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(obconfig_url, OB_CLUSTER_PARAMETER, "", "URL for OBConfig service",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_LOG_LEVEL(syslog_level, OB_CLUSTER_PARAMETER, "WDIAG", "specifies the current level of logging. There are DEBUG, TRACE, WDIAG, EDIAG, INFO, WARN, ERROR, seven different log levels.",
              ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(alert_log_level, OB_CLUSTER_PARAMETER, "INFO",
                     common::ObConfigAlertLogLevelChecker,
                     "specifies the current level of alert log. There are INFO, WARN, ERROR, three different log levels.",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(syslog_io_bandwidth_limit, OB_CLUSTER_PARAMETER, "30MB",
        "Syslog IO bandwidth limitation, exceeding syslog would be truncated. Use 0 to disable ERROR log.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(diag_syslog_per_error_limit, OB_CLUSTER_PARAMETER, "200", "[0,]",
        "DIAG syslog limitation for each error per second, exceeding syslog would be truncated",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT_WITH_CHECKER(max_syslog_file_count, OB_CLUSTER_PARAMETER, "0",
                     common::ObConfigMaxSyslogFileCountChecker,
                     "specifies the maximum number of the log files "
                     "that can co-exist before the log file recycling kicks in. "
                     "Each log file can occupy at most 256MB disk space. "
                     "When this value is set to 0, no log file will be removed. Range: [0, +∞) in integer",
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
         "Value: True：turned on; False: turned off",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(syslog_disk_size, OB_CLUSTER_PARAMETER, "0M", "[0M,)",
        "the size of disk space used by the syslog files. Range: [0, +∞)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(syslog_compress_func, OB_CLUSTER_PARAMETER, "none",
                     common::ObConfigSyslogCompressFuncChecker,
                     "compress function name for syslog files, "
                     "values: none, zstd_1.0, zstd_1.3.8",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT_WITH_CHECKER(syslog_file_uncompressed_count, OB_CLUSTER_PARAMETER, "0",
                     common::ObConfigSyslogFileUncompressedCountChecker,
                     "specifies the minimum number of the syslog files that will not be compressed. "
                     "Each syslog file can occupy at most 256MB disk space. "
                     "When this value is set to 0, all syslog file may be compressed. Range: [0, +∞) in integer",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(memory_limit_percentage, OB_CLUSTER_PARAMETER, "80", "[10, 95]",
        "the size of the memory reserved for internal use(for testing purpose). Range: [10, 95]",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(cache_wash_threshold, OB_CLUSTER_PARAMETER, "4GB", "[0B,]",
        "size of remaining memory at which cache eviction will be triggered. Range: [0,+∞)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(memory_chunk_cache_size, OB_CLUSTER_PARAMETER, "0M", "[0M,]", "the maximum size of memory cached by memory chunk cache. Range: [0M,], 0 stands for adaptive",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(autoinc_cache_refresh_interval, OB_CLUSTER_PARAMETER, "3600s", "[100ms,]",
         "auto-increment service cache refresh sync_value in this interval, "
         "with default 3600s. Range: [100ms, +∞)",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_sql_operator_dump, OB_CLUSTER_PARAMETER, "True", "specifies whether sql operators "
         "(sort/hash join/material/window function/interm result/...) allowed to write to disk",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_chunk_row_store_mem_limit, OB_CLUSTER_PARAMETER, "0M", "[0M,]",
        "the maximum size of memory used by ChunkRowStore, 0 means follow operator's setting. Range: [0, +∞)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(kv_transport_compress_func, OB_TENANT_PARAMETER, "none",
                     common::ObConfigCompressFuncChecker,
                     "compressor used for tableAPI query result. Values: none, lz4_1.0, snappy_1.0, zlib_1.0, zstd_1.0 zstd 1.3.8",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(kv_transport_compress_threshold, OB_TENANT_PARAMETER, "10K","[0B,]",
        "Together with the configuration item kv_transport_compress_func, it is used to specify the minimum threshold size of the OBKV query result set that needs to be compressed. Range: [0, +∞)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_sort_area_size, OB_TENANT_PARAMETER, "32M", "[2M,]",
        "size of maximum memory that could be used by SORT. Range: [2M,+∞)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_hash_area_size, OB_TENANT_PARAMETER, "32M", "[4M,]",
        "size of maximum memory that could be used by HASH JOIN. Range: [4M,+∞)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//
DEF_BOOL(_enable_partition_level_retry, OB_CLUSTER_PARAMETER, "True",
         "specifies whether allow the partition level retry when the leader changes",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
//
DEF_INT_WITH_CHECKER(_enable_defensive_check, OB_CLUSTER_PARAMETER, "1",
                     common::ObConfigEnableDefensiveChecker,
                     "specifies whether allow to do some defensive checks when the query is executed, "
                     "0 means defensive check is disabled, "
                     "1 means normal defensive check is enabled, "
                     "2 means more strict defensive check is enabled, such as check partition id validity",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_delay_resource_recycle_after_correctness_issue, OB_CLUSTER_PARAMETER, "False",
         "whether hinder the recycling of the log resources and the sstable resources under correctness issues",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//
DEF_BOOL(_sql_insert_multi_values_split_opt, OB_CLUSTER_PARAMETER, "True",
         "True means that the split + batch optimization for inserting multiple rows of the insert values ​​statement can be done",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_auto_drop_recovering_auxiliary_tenant, OB_CLUSTER_PARAMETER, "True",
         "control whether to delete auxiliary tenant after recovering tables failed",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(recover_table_concurrency, OB_TENANT_PARAMETER, "0", "[0,16]",
        "The maximum number of tables that can be recovered concurrently during the cross-tenant table import stage of tables recovery."
        "Range: [0,16] in integer. Especially, 0 means default value",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(recover_table_dop, OB_TENANT_PARAMETER, "0", "[0,)",
        "The maximum degree of parallel of the single table recovery during the cross-tenant table import stage of tables recovery."
        "Range: [0,) in integer. Especially, 0 means default value",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_min_malloc_sample_interval, OB_CLUSTER_PARAMETER, "16", "[1, 10000]",
        "the min malloc times between two samples, "
        "which is not more than _max_malloc_sample_interval. "
        "10000 means not to sample any malloc, Range: [1, 10000]",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_max_malloc_sample_interval, OB_CLUSTER_PARAMETER, "256", "[1, 10000]",
        "the max malloc times between two samples, "
        "which is not less than _min_malloc_sample_interval. "
        "1 means to sample all malloc, Range: [1, 10000]",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_values_table_folding, OB_CLUSTER_PARAMETER, "True",
         "whether enable values statement folds self params",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//
DEF_STR_WITH_CHECKER(spill_compression_codec, OB_TENANT_PARAMETER, "NONE",
        common::ObConfigSQLSpillCompressionCodecChecker,
        "specific the compression algorithm type to compress the spilled data in temp block store "\
        "during the sql execution phase. "\
        "The supported compression codecs are: ZSTD, LZ4, SNAPPY, ZLIB. NONE means no compression."\
        "The default value is NONE.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//// tenant config
DEF_TIME_WITH_CHECKER(max_stale_time_for_weak_consistency, OB_TENANT_PARAMETER, "5s",
                      common::ObConfigStaleTimeChecker,
                      "[5s,)",
                      "the max data stale time that cluster weak read version behind current timestamp,"
                      "no smaller than weak_read_version_refresh_interval, range: [5s, +∞)",
                      ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_monotonic_weak_read, OB_TENANT_PARAMETER, "false",
         "specifies observer supportting atomicity and monotonic order read",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(server_cpu_quota_min, OB_CLUSTER_PARAMETER, "0", "[0,16]",
        "the number of minimal vCPUs allocated to the server tenant"
        "(a special internal tenant that exists on every observer). 0 stands for adaptive. Range: [0, 16]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(server_cpu_quota_max, OB_CLUSTER_PARAMETER, "0", "[0,16]",
        "the number of maximal vCPUs allocated to the server tenant"
        "(a special internal tenant that exists on every observer). 0 stands for adaptive. Range: [0, 16]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP_WITH_CHECKER(_hidden_sys_tenant_memory, OB_CLUSTER_PARAMETER, "0M",
        common::ObConfigTenantMemoryChecker, "[0M,)",
        "the size of the memory reserved for hidden sys tenant, 0M means follow the adjusting value.",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP_WITH_CHECKER(_ss_hidden_sys_tenant_data_disk_size, OB_CLUSTER_PARAMETER, "0M",
        common::ObConfigTenantDataDiskChecker, "[0M,)",
        "the size of the data disk reserved for hidden sys tenant in shared storage mode, 0M means follow the adjusting value.",
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
        "the percentage of the workers reserved to serve large query request. "
        "Range: [0, 100] in percentage",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE))
DEF_TIME(large_query_threshold, OB_CLUSTER_PARAMETER, "5s", "[0ms,)",
         "threshold for execution time beyond "
         "which a request may be paused and rescheduled as a \\'large request\\', 0ms means disable \\'large request\\'. Range: [0ms, +∞)",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_ob_max_thread_num, OB_CLUSTER_PARAMETER, "0", "[0,10000)",
         "ob max thread number "
         "upper limit of observer thread count. Range: [0, 10000), 0 means no limit.",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(cpu_quota_concurrency, OB_TENANT_PARAMETER, "4", "[1,20]",
        "max allowed concurrency for 1 CPU quota. Range: [1,20]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(px_workers_per_cpu_quota, OB_TENANT_PARAMETER, "10", "[0,20]",
        "the ratio(integer) between the number of system allocated px workers vs "
        "the maximum number of threads that can be scheduled concurrently. Range: [0, 20]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(undo_retention, OB_TENANT_PARAMETER, "1800", "[0, 4294967295]",
        "the low threshold value of undo retention. The system retains undo for at least the time specified in this config when active txn protection is banned. Range: [0, 4294967295]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_mvcc_gc_using_min_txn_snapshot, OB_TENANT_PARAMETER, "True",
        "specifies enable mvcc gc using active txn snapshot",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_rowsets_enabled, OB_TENANT_PARAMETER, "True",
         "specifies whether vectorized sql execution engine is activated",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_rowsets_target_maxsize, OB_TENANT_PARAMETER, "524288", "[262144, 8388608]",
        "the size of the memory reserved for vectorized sql engine. Range: [262144, 8388608]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_rowsets_max_rows, OB_TENANT_PARAMETER, "256", "[0, 65535]",
        "the row number processed by vectorized sql engine within one batch. Range: [0, 65535]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(_ctx_memory_limit, OB_TENANT_PARAMETER, "",
        common::ObCtxMemoryLimitChecker,
        "specifies tenant ctx memory limit.",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_convert_real_to_decimal, OB_TENANT_PARAMETER, "False",
         "specifies whether convert column type float(M,D), double(M,D) to decimal(M,D) in DDL",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_decimal_int_type, OB_TENANT_PARAMETER, "True",
         "specifies wether use decimal_int type as backend for decimal values",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_ob_enable_dynamic_worker, OB_TENANT_PARAMETER, "True",
         "specifies whether worker count increases when all workers were in blocking.",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_optimizer_ads_time_limit, OB_TENANT_PARAMETER, "10", "[0, 300]",
        "the maximum optimizer dynamic sampling time limit. Range: [0, 300]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_hash_join_enabled, OB_TENANT_PARAMETER, "True",
         "enable/disable hash join",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_optimizer_sortmerge_join_enabled, OB_TENANT_PARAMETER, "True",
         "enable/disable merge join",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
//
DEF_BOOL(_nested_loop_join_enabled, OB_TENANT_PARAMETER, "True",
         "enable/disable nested loop join",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
//
DEF_BOOL(_enable_enum_set_subschema, OB_TENANT_PARAMETER, "True",
         "Specifies whether to enable the enum/set extended type info is stored as subschema "
         "and to activate the related new type cast logic behavior.",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_min_const_integer_precision, OB_TENANT_PARAMETER, "1", "[1, 20]",
        "the minimum precision of integer constant",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_lob_rowsets_max_rows, OB_TENANT_PARAMETER, "65535", "[1, 65535]",
        "max batch size of physical plan with lob data",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(_use_hash_rollup, OB_TENANT_PARAMETER, "AUTO",
        common::ObConfigEnableHashRollupChecker,
        "policy of hash based rollup plan:"\
        "AUTO: hash rollup plan is up to optimizer;"\
        "FORCED: hash rollup plan is used by default;"\
        "DISABLED: hash rollup plan is disabled;",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// tenant memtable consumption related
DEF_INT(memstore_limit_percentage, OB_CLUSTER_PARAMETER, "0", "[0, 100)",
        "used in calculating the value of MEMSTORE_LIMIT parameter: "
        "memstore_limit_percentage = memstore_limit / memory_size, "
        "where MEMORY_SIZE is determined when the tenant is created. Range: [0, 100). "
        "1. the system will use memstore_limit_percentage if only memstore_limit_percentage is set."
        "2. the system will use _memstore_limit_percentage if both memstore_limit_percentage and "
        "_memstore_limit_percentage is set."
        "3. the system will adjust automatically if both memstore_limit_percentage and "
        "_memstore_limit_percentage set to 0(by default).",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_memstore_limit_percentage, OB_TENANT_PARAMETER, "0", "[0, 100)",
        "used in calculating the value of MEMSTORE_LIMIT parameter: "
        "_memstore_limit_percentage = memstore_limit / memory_size, "
        "where MEMORY_SIZE is determined when the tenant is created. Range: [0, 100). "
        "1. the system will use memstore_limit_percentage if only memstore_limit_percentage is set."
        "2. the system will use _memstore_limit_percentage if both memstore_limit_percentage and "
        "_memstore_limit_percentage is set."
        "3. the system will adjust automatically if both memstore_limit_percentage and "
        "_memstore_limit_percentage set to 0(by default).",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(freeze_trigger_percentage, OB_TENANT_PARAMETER, "20", "(0, 100)",
        "the threshold of the size of the mem store when freeze will be triggered. Rang:(0,100)",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(writing_throttling_trigger_percentage, OB_TENANT_PARAMETER, "60", "(0, 100]",
          "the threshold of the size of the mem store when writing_limit will be triggered. Rang:(0,100]. setting 100 means turn off writing limit",
          ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(data_disk_write_limit_percentage, OB_CLUSTER_PARAMETER, "0", "[0, 100)",
        "used to stop user write operations. "
        "When the user data disk reaches this watermark, SQL requests will report that the disk is full. "
        "The configuration should be greater than data_disk_usage_limit_percentage, "
        "with the recommended setting being: (1 - memstore_limit_size / data_disk_size) * 100%",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_tx_share_memory_limit_percentage, OB_TENANT_PARAMETER, "0", "[0, 100)",
        "Used to control the percentage of tenant memory limit that multiple modules in the transaction layer can collectively use. "
        "This primarily includes user data (MemTable), transaction data (TxData), etc. "
        "When it is set to the default value of 0, it represents dynamic adaptive behavior, "
        "which will be adjusted dynamically based on memstore_limit_percentage. The adjustment rule is: "
        " _tx_share_memory_limit_percentage = memstore_limit_percentage + 10. "
        "Range: [0, 100)",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_tx_data_memory_limit_percentage, OB_TENANT_PARAMETER, "20", "(0, 100)",
        "used to control the upper limit percentage of memory resources that the TxData module can use. "
        "Range:(0, 100)",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_mds_memory_limit_percentage, OB_TENANT_PARAMETER, "10", "(0, 100)",
        "Used to control the upper limit percentage of memory resources that the Mds module can use. "
        "Range:(0, 100)",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(writing_throttling_maximum_duration, OB_TENANT_PARAMETER, "2h", "[1s, 3d]",
          "maximum duration of writting throttling(in minutes), max value is 3 days",
          ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(plan_cache_evict_interval, OB_CLUSTER_PARAMETER, "5s", "[0s,)",
         "time interval for periodic plan cache eviction. Range: [0s, +∞)",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(default_progressive_merge_num, OB_TENANT_PARAMETER, "0", "[0,)",
         "default progressive_merge_num when tenant create table"
         "Range:[0,)",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_parallel_min_message_pool, OB_TENANT_PARAMETER, "16M", "[16M, 8G]",
        "DTL message buffer pool reserve the mininum size after extend the size. Range: [16M,8G]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(_parallel_server_sleep_time, OB_TENANT_PARAMETER, "1", "[0, 2000]",
        "sleep time between get channel data in millisecond. Range: [0, 2000]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_DBL(_px_max_message_pool_pct, OB_TENANT_PARAMETER, "40", "[0,90]",
        "The maxinum percent of tenant memory that DTL message buffer pool can alloc memory. Range: [0,90]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_px_message_compression, OB_TENANT_PARAMETER, "True",
        "Enable DTL send message with compression"
        "Value: True: enable compression False: disable compression",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_px_chunklist_count_ratio, OB_CLUSTER_PARAMETER, "1", "[1, 128]",
        "the ratio of the dtl buffer manager list. Range: [1, 128]",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_sqlexec_disable_hash_based_distagg_tiv, OB_TENANT_PARAMETER, "False",
         "disable hash based distinct aggregation in the second stage of three stage aggregation for gby queries"
         "Value:  True:turned on  False: turned off",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_force_hash_groupby_dump, OB_TENANT_PARAMETER, "False",
         "force hash groupby to dump"
         "Value:  True:turned on  False: turned off",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
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
DEF_INT(_pushdown_storage_level, OB_TENANT_PARAMETER, "4", "[0, 4]",
        "the level of storage pushdown. Range: [0, 4] "
        "0: disabled, 1:blockscan, 2: blockscan & filter, 3: blockscan & filter & aggregate, 4: blockscan & filter & aggregate & group by",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_WORK_AREA_POLICY(workarea_size_policy, OB_TENANT_PARAMETER, "AUTO", "policy used to size SQL working areas (MANUAL/AUTO)",
              ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_temporary_file_io_area_size, OB_TENANT_PARAMETER, "1", "[0, 50)",
         "memory buffer size of temporary file, as a percentage of total tenant memory. "
         "Range: [0, 50), percentage",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_temporary_file_meta_memory_limit_percentage, OB_TENANT_PARAMETER, "0", "[0,100]",
        "The memory limit of temporary file meta, and the value is a percentage of the tenant's memory. "
        "The default value is 70. For compatibility, 0 is 70% of tenant memory."
        "Range: [0, 100], percentage",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_storage_meta_memory_limit_percentage, OB_TENANT_PARAMETER, "20", "[0, 50)",
         "maximum memory for storage meta, as a percentage of total tenant memory. "
         "Range: [0, 50), percentage, 0 means no limit to storage meta memory",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_max_tablet_cnt_per_gb, OB_TENANT_PARAMETER, "20000", "[1000, 50000)",
         "The maximum number of tablets supported per 1GB of memory by tenant unit. Range: [1000, 50000)",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_filter_reordering, OB_TENANT_PARAMETER, "True",
         "enable filter reordering in storage engine",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
////  rootservice config
DEF_TIME(lease_time, OB_CLUSTER_PARAMETER, "10s", "[1s, 5m]",
         "Lease for current heartbeat. If the root server does not received any heartbeat "
         "from an observer in lease_time seconds, that observer is considered to be offline. "
         "Not recommended for modification. Range: [1s, 5m]",
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
DEF_TIME(server_permanent_offline_time, OB_CLUSTER_PARAMETER, "3600s", "[20s,)",
         "the time interval between any two heartbeats beyond "
         "which a server is considered to be \\'permanently\\' offline. Range: [20s,+∞)",
         ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(migration_disable_time, OB_CLUSTER_PARAMETER, "3600s", "[1s,)",
         "the duration in which the observer stays in the \\'block_migrate_in\\' status, "
         "which means it is not allowed to migrate into the server. Range: [1s, +∞)",
         ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(server_check_interval, OB_CLUSTER_PARAMETER, "30s", "[1s,)",
         "the time interval between schedules of a task "
         "that examines the __all_server table. Range: [1s, +∞)",
         ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(rootservice_ready_check_interval, OB_CLUSTER_PARAMETER, "3s", "[100000us, 1m]",
         "the interval between the schedule of the rootservice restart task while restart failed "
         "Range: [100000us, 1m]",
         ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(tablet_meta_table_scan_batch_count, OB_CLUSTER_PARAMETER, "999", "(0, 65536]",
        "the number of tablet replica info "
        "that will be read by each request on the tablet-related system tables "
        "during procedures such as load-balancing, daily merge, election and etc. "
        "Range:(0,65536]",
        ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(ls_meta_table_check_interval, OB_CLUSTER_PARAMETER, "1s", "[1ms,)",
         "the time interval that observer compares ls meta table with local ls replica info "
         "and make adjustments to ensure the correctness of ls meta table. Range: [1ms,+∞)",
         ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(tablet_meta_table_check_interval, OB_CLUSTER_PARAMETER, "30m", "[1m,)",
         "the time interval that observer compares tablet meta table with local ls replica info "
         "and make adjustments to ensure the correctness of tablet meta table. Range: [1m,+∞)",
         ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(min_observer_version, OB_CLUSTER_PARAMETER, "4.3.5.0", "the min observer version",
        ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_VERSION(compatible, OB_TENANT_PARAMETER, "4.3.5.0", "compatible version for persisted data",
            ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_ddl, OB_CLUSTER_PARAMETER, "True", "specifies whether DDL operation is turned on. "
         "Value:  True:turned on;  False: turned off",
         ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_parallel_table_creation, OB_TENANT_PARAMETER, "True", "specifies whether create table parallelly. "
         "Value:  True: create table parallelly;  False: create table serially",
         ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_major_freeze, OB_CLUSTER_PARAMETER, "True", "specifies whether major_freeze function is turned on. "
         "Value:  True:turned on;  False: turned off",
         ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
TEMP_DEF_BOOL(v4.3, enable_crazy_medium_compaction, OB_CLUSTER_PARAMETER, "False",
              "enables triggering medium compaction repeatly. The default value is False.",
              ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
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

DEF_MODE_WITH_PARSER(_parallel_ddl_control, OB_TENANT_PARAMETER, "",
        common::ObParallelDDLControlParser,
        "switch for parallel capability of parallel DDL",
        ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// ========================= LogService Config Begin =====================

DEF_CAP(log_disk_size, OB_CLUSTER_PARAMETER, "0M", "[0M,)",
        "the size of disk space used by the log files. Range: [0, +∞)",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(log_disk_percentage, OB_CLUSTER_PARAMETER, "0", "[0,99]",
        "the percentage of disk space used by the log files. Range: [0,99] in integer;"
        "only effective when parameter log_disk_size is 0;"
        "when log_disk_percentage is 0:"
        " a) if the data and the log are on the same disk, means log_disk_percentage = 30"
        " b) if the data and the log are on the different disks, means log_disk_perecentage = 90",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(log_transport_compress_all, OB_TENANT_PARAMETER, "False",
         "If this option is set to true, use compression for log transport. "
         "The default is false(no compression)",
         ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(log_transport_compress_func, OB_TENANT_PARAMETER, "lz4_1.0",
                     common::ObConfigCompressFuncChecker,
                     "compressor used for log transport. Values: none, lz4_1.0, zstd_1.0, zstd_1.3.8",
                     ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(log_storage_compress_all, OB_TENANT_PARAMETER, "False",
         "specifies whether to compress logs before storing. The default is false(no compression)",
         ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(log_storage_compress_func, OB_TENANT_PARAMETER, "lz4_1.0",
                     common::ObConfigPerfCompressFuncChecker,
                     "specifies the algorithms used for log storage compression. Values: lz4_1.0, zstd_1.0, zstd_1.3.8",
                     ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//DEF_BOOL(enable_log_archive, OB_CLUSTER_PARAMETER, "False",
//         "control if enable log archive",
//         ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(log_restore_concurrency, OB_TENANT_PARAMETER, "0", "[0, 100]",
        "log restore concurrency, for both the restore tenant and standby tenant. "
        "If the value is default 0, the database will automatically calculate the number of restore worker threads "
        "based on the tenant specification, which is tenant max_cpu; otherwise set the the worker count equals to the value."
        "Range: [0, 100] in integer",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(log_archive_concurrency, OB_TENANT_PARAMETER, "0", "[0, 100]",
        "log archive concurrency, for both archive fetcher and sender. "
        "If the value is default 0, the database will automatically calculate the number of archive worker threads "
        "based on the tenant specification, which is tenant max_cpu divided by 4; otherwise set the the worker count equals to the value."
        "Range: [0, 100] in integer",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(log_disk_utilization_limit_threshold, OB_TENANT_PARAMETER, "95",
        "[80, 100]",
        "maximum of log disk usage percentage before stop submitting or receiving logs, "
        "should be bigger than log_disk_utilization_threshold. "
        "Range: [80, 100]",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(log_disk_utilization_threshold, OB_TENANT_PARAMETER,"80",
        "[10, 100)",
        "log disk utilization threshold before reuse log files, "
        "should be smaller than log_disk_utilization_limit_threshold. "
        "Range: [10, 100)",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(log_disk_throttling_percentage, OB_TENANT_PARAMETER, "60",
        "[40, 100]",
        "the threshold of the size of the log disk when writing_limit will be triggered. Rang:[40,100]. setting 100 means turn off writing limit",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(log_disk_throttling_maximum_duration, OB_TENANT_PARAMETER, "2h", "[1s, 3d]",
          "maximum duration of log disk throttling, that is the time remaining until the log disk space is exhausted after log disk throttling triggered.",
          ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(log_storage_warning_tolerance_time, OB_CLUSTER_PARAMETER, "5s",
        "[1s,300s]",
        "time to tolerate log disk io delay, after that, the disk status will be set warning. "
        "Range: [1s,300s]",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(ls_gc_delay_time, OB_TENANT_PARAMETER, "0s",
        "[0s,)",
        "The max delay time for ls gc when log archive is off. The default value is 0s. Range: [0s, +∞). "
        "The ls delay deletion mechanism will no longer take effect when the tenant is dropped.",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(standby_db_fetch_log_rpc_timeout, OB_TENANT_PARAMETER, "15s",
        "[2s,)",
        "The threshold for detecting the RPC timeout for the standby tenant to fetch log from the log restore source tenant. "
        "When the rpc timeout, the log transport service switches to another server of the log restore source tenant to fetch logs. "
        "Range: [2s, +∞)",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(archive_lag_target, OB_TENANT_PARAMETER, "120s",
        "[0ms,7200s]",
        "The lag target of the log archive. The log archive target affects not only the backup availability, "
        "but also the lag of the standby database based on archive. Values larger than 7200s are not reasonable lag. "
        "The typical value is 120s. Extremely low values can result in high IOPS, which is not optimal for object storage; "
        "such values can also affect the performance of the database. The value 0ms means to archive as soon as possible. "
        "Range: [0ms,7200s]",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_log_writer_parallelism, OB_TENANT_PARAMETER, "3",
       "[1,8]",
       "the number of parallel log writer threads that can be used to write redo log entries to disk. ",
       ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));

DEF_TIME(_ls_gc_wait_readonly_tx_time, OB_TENANT_PARAMETER, "24h",
        "[0s,)",
        "The maximum waiting time for residual read-only transaction before executing log stream garbage collecting。The default value is 24h. Range: [0s,  +∞)."
        "Log stream garbage collecting will no longer wait for readonly transaction when the tenant is dropped. ",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(standby_db_preferred_upstream_log_region, OB_TENANT_PARAMETER, "",
       "The preferred upstream log region for Standby db. "
       "The Standby db will give priority to the preferred upstream log region to fetch log. "
       "For high availability，the Standby db will also switch to the other region "
       "when the preferred upstream log region can not fetch log because of exception etc.",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_log_cache, OB_TENANT_PARAMETER, "True",
         "specifies whether allow to fill log kv cache. "
         "Value:  True:turned on  False: turned off",
         ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_ob_enable_standby_db_parallel_log_transport, OB_TENANT_PARAMETER, "True",
        "Specifies whether the parallel log transport protocol is enabled on the standby database. "
        "The parallel log transport protocol is enabled only if this parameter is true and "
        "the primary database is compatible with the parallel log transport protocol.",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(arbitration_degradation_policy, OB_CLUSTER_PARAMETER, "LS_POLICY",
        common::ObConfigDegradationPolicyChecker,
        "specifies the degradation policy, whether to check network connectivity with RS before arbitration degrades. "
        "Value: LS_POLICY, CLUSTER_POLICY "
        "LS_POLICY: default policy. "
        "CLUSTER_POLICY: check network connectivity with RS before arbitration degrades. Do not degrade when not connected. "
        "Then, switch log stream leaders to the replicas which are connected with RS.",
        ObParameterAttr(Section::LOGSERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// ========================= LogService Config End   =====================
DEF_INT(resource_hard_limit, OB_CLUSTER_PARAMETER, "100", "[100, 10000]",
        "system utilization should not be large than resource_hard_limit",
        ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_rereplication, OB_CLUSTER_PARAMETER, "True",
         "specifies whether the auto-replication is turned on. "
         "Value:  True:turned on  False: turned off",
         ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_rebalance, OB_TENANT_PARAMETER, "True",
         "specifies whether the tenant load-balancing is turned on. "
         "Value:  True:turned on  False: turned off",
         ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_transfer, OB_TENANT_PARAMETER, "True",
         "controls whether transfers are allowed in the tenant. "
         "This config does not take effect when enable_rebalance is disabled. "
         "Value:  True:turned on  False:turned off",
         ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(balancer_idle_time, OB_TENANT_PARAMETER, "10s", "[10s,]",
         "the time interval between the schedules of the tenant load-balancing task. "
         "Range: [10s, +∞)",
         ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(partition_balance_schedule_interval, OB_TENANT_PARAMETER, "2h", "[0s,]",
         "the time interval between generate partition balance task. "
         "The value should be no less than balancer_idle_time to enable partition balance. "
         "Default value 2h and the value 0s means disable partition balance. "
         "Range: [0s, +∞)",
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
DEF_TIME(balancer_task_timeout, OB_CLUSTER_PARAMETER, "20m", "[1s,)",
         "the time to execute the load-balancing task before it is terminated. Range: [1s, +∞)",
         ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(balancer_log_interval, OB_CLUSTER_PARAMETER, "1m", "[1s,)",
         "the time interval between logging the load-balancing task\\'s statistics. "
         "Range: [1s, +∞)",
         ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(__balance_controller, OB_CLUSTER_PARAMETER, "",
        "specifies whether the balance events are turned on or turned off.",
        ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(__min_full_resource_pool_memory, OB_CLUSTER_PARAMETER, "5368709120", "[1073741824,)",
        "the min memory value which is specified for a full resource pool.",
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

DEF_TIME(_lcl_op_interval, OB_CLUSTER_PARAMETER, "30ms", "[0ms, 1s]",
         "Scan interval for every detector node, smaller interval support larger deadlock scale, but cost more system resource. "
         "0ms means disable deadlock, default value is 30ms. Range:[0ms, 1s]",
         ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(enable_sys_unit_standalone, OB_CLUSTER_PARAMETER, "False",
         "specifies whether sys unit standalone deployment is turned on. "
         "Value:  True:turned on  False: turned off",
         ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(replica_parallel_migration_mode, OB_TENANT_PARAMETER, "auto",
        common::ObConfigReplicaParallelMigrationChecker,
        "specify the strategy for parallel migration of LS replicas. "
        "'auto' means to allow parallel migration of LS replica of standby tenant "
        "and prohibit the parallel migration of LS replica of primary tenant. "
        "'on' means to allow parallel migration of LS replica of primary tenant and standby tenant. "
        "'off' means to prohibit parallel migration of LS replica of primary tenant and standby tenant",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//// daily merge  config
// set to disable if don't want major freeze launch auto
DEF_MOMENT(major_freeze_duty_time, OB_TENANT_PARAMETER, "02:00",
           "the start time of system daily merge procedure. Range: [00:00, 24:00)",
           ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(merger_check_interval, OB_TENANT_PARAMETER, "10m", "[10s, 60m]",
         "the time interval between the schedules of the task "
         "that checks on the progress of MERGE for each zone. Range: [10s, 60m]",
         ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
//// transaction config
DEF_TIME(trx_2pc_retry_interval, OB_CLUSTER_PARAMETER, "100ms", "[1ms, 5000ms]",
         "the time interval between the retries in case of failure "
         "during a transaction\\'s two-phase commit phase. Range: [1ms,5000ms]",
         ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(clog_sync_time_warn_threshold, OB_CLUSTER_PARAMETER, "100ms", "[1ms, 10000ms]",
         "the time given to the commit log synchronization between a leader and its followers "
         "before a \\'warning\\' message is printed in the log file.  Range: [1ms,1000ms]",
         ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(row_compaction_update_limit, OB_CLUSTER_PARAMETER, "6", "[1, 6400]",
        "maximum update count before trigger row compaction. "
        "Range: [1, 6400]",
        ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(ignore_replay_checksum_error, OB_CLUSTER_PARAMETER, "False",
         "specifies whether error raised from the memtable replay checksum validation can be ignored. "
         "Value: True:ignored; False: not ignored",
         ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(_rpc_checksum, OB_CLUSTER_PARAMETER, "Force",
                     common::ObConfigRpcChecksumChecker,
                     "Force: always verify; " \
                     "Optional: verify when rpc_checksum non-zero; " \
                     "Disable: ignore verify",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE))

DEF_TIME(_ob_trans_rpc_timeout, OB_CLUSTER_PARAMETER, "3s", "[0s, 3600s]",
         "transaction rpc timeout(s). Range: [0s, 3600s]",
         ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_early_lock_release, OB_TENANT_PARAMETER, "True",
         "enable early lock release",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_tx_result_retention, OB_TENANT_PARAMETER, "300", "[0, 36000]",
        "The tx data can be recycled after at least _tx_result_retention seconds. "
        "Range: [0, 36000]",
        ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_parallel_redo_logging, OB_CLUSTER_PARAMETER, "True",
         "enable parallel write redo log.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_parallel_redo_logging_trigger, OB_CLUSTER_PARAMETER, "16M", "[0B,)",
        "size of single transaction's pending redo log to trigger parallel writes redo log. "
        "Range: [0B,+∞)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_ob_get_gts_ahead_interval, OB_CLUSTER_PARAMETER, "0s", "[0s, 1s]",
         "get gts ahead interval. Range: [0s, 1s]",
         ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_tx_debug_level, OB_TENANT_PARAMETER, "0", "[0, 10]",
        "the debug level of transaction module. Range: [0, 10] in integer.",
        ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//// rpc config
DEF_TIME(rpc_timeout, OB_CLUSTER_PARAMETER, "2s",
         "the time during which a RPC request is permitted to execute before it is terminated",
         ObParameterAttr(Section::RPC, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_stream_rpc_max_wait_timeout, OB_TENANT_PARAMETER, "1200s", "[1s,)",
         "the maximum timeout for a tenant worker thread to wait for the next request while processing streaming RPC",
         ObParameterAttr(Section::RPC, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_pkt_nio, OB_CLUSTER_PARAMETER, "True",
         "enable pkt-nio, the new RPC framework"
         "Value:  True:turned on;  False: turned off",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(rpc_memory_limit_percentage, OB_TENANT_PARAMETER, "0", "[0,100]",
         "maximum memory for rpc in a tenant, as a percentage of total tenant memory, "
         "and 0 means no limit to rpc memory",
        ObParameterAttr(Section::RPC, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_max_rpc_packet_size, OB_CLUSTER_PARAMETER, "2047MB", "[2M,2047M]",
        "the max rpc packet size when sending RPC or responding RPC results",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(standby_fetch_log_bandwidth_limit, OB_CLUSTER_PARAMETER, "0MB", "[0M,10000G]",
        "the max bandwidth in bytes per second that can be occupied by the sum of the synchronizing log from primary cluster of all servers in the standby cluster",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_server_standby_fetch_log_bandwidth_limit, OB_CLUSTER_PARAMETER, "0MB", "[0M,1000G]",
        "the max bandwidth in bytes per second that can be occupied by the synchronizing log from primary cluster of a server in standby cluster",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//// location cache config
DEF_TIME(virtual_table_location_cache_expire_time, OB_CLUSTER_PARAMETER, "8s", "[1s,)",
         "expiration time for virtual table location info in partition location cache. "
         "Range: [1s, +∞)",
         ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(location_refresh_thread_count, OB_CLUSTER_PARAMETER, "2", "(1,64]",
        "the number of threads for fetching location cache in the background. Range: (1, 64]",
        ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(location_fetch_concurrency, OB_CLUSTER_PARAMETER, "20", "[1, 1000]",
        "the maximum number of the tasks for fetching location cache concurrently. Range: [1, 1000]",
        ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(location_cache_refresh_min_interval, OB_CLUSTER_PARAMETER, "100ms", "[0s,)",
         "the time interval in which no request for location cache renewal will be executed. "
         "The default value is 100 milliseconds. [0s, +∞)",
         ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(all_server_list, OB_CLUSTER_PARAMETER, "",
        "all server addr in cluster",
        ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(location_cache_refresh_rpc_timeout, OB_CLUSTER_PARAMETER, "500ms", "[1ms,)",
        "The timeout used for refreshing location cache by RPC. Range: [1ms, +∞)",
        ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(location_cache_refresh_sql_timeout, OB_CLUSTER_PARAMETER, "1s", "[1ms,)",
        "The timeout used for refreshing location cache by SQL. Range: [1ms, +∞)",
        ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_auto_refresh_tablet_location_interval, OB_CLUSTER_PARAMETER, "10m", "[0s,)",
        "Polling period of auto refresh tablet location service. "
        "When the value is 0, it means shutting down related service. Range: [0s, +∞)",
        ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_auto_broadcast_tablet_location_rate_limit, OB_CLUSTER_PARAMETER, "10000", "[0, 100000]",
        "Maximum number of tablets broadcasted per second by a single observer. When the value is 0, it means shutting down related logic.",
        ObParameterAttr(Section::LOCATION_CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//// cache config
DEF_INT(tablet_ls_cache_priority, OB_CLUSTER_PARAMETER, "1000", "[1,)", "tablet ls cache priority. Range:[1, )",
        ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(opt_tab_stat_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)", "tab stat cache priority. Range:[1, )",
        ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(index_block_cache_priority, OB_CLUSTER_PARAMETER, "10", "[1,)", "index cache priority. Range:[1, )",
        ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(user_block_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)", "user block cache priority. Range:[1, )",
        ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(user_row_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)", "user row cache priority. Range:[1, )",
        ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(bf_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)", "bf cache priority. Range:[1, )",
        ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(bf_cache_miss_count_threshold, OB_CLUSTER_PARAMETER, "100", "[0,)", "bf cache miss count threshold, 0 means disable bf cache. Range:[0, )",
        ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(fuse_row_cache_priority, OB_CLUSTER_PARAMETER, "1", "[1,)", "fuse row cache priority. Range:[1, )", ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(storage_meta_cache_priority, OB_CLUSTER_PARAMETER, "10", "[1,)", "storage meta cache priority. Range:[1, )",
        ObParameterAttr(Section::CACHE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// shared storage local disk cache config
DEF_INT(_ss_major_compaction_prewarm_level, OB_TENANT_PARAMETER, "0", "[0, 2]",
        "the level of major compaction prewarm in shared storage mode. Range: [0, 2] "
        "0: prewarm both meta and data, 1: only prewarm meta, 2: prewarm none of meta and data. "
        "this prewarm level should be set before one round of major compaction.",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_ss_replica_prewarm, OB_TENANT_PARAMETER, "True",
         "specifies whether enable replica prewarm in shared storage mode. "
         "Value: True:turned on;  False: turned off",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_ss_micro_cache_memory_percentage, OB_TENANT_PARAMETER, "20", "[1,50]",
        "the percentage of tenant memory size used by microblock_cache in shared_stoarge mode, 0 means follow the "
        "adjusting value. Range: [1, 50]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
//background limit config
DEF_TIME(_data_storage_io_timeout, OB_CLUSTER_PARAMETER, "10s", "[1s,600s]",
        "io timeout for data storage, Range [1s,600s]. "
        "The default value is 10s",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_object_storage_io_timeout, OB_TENANT_PARAMETER, "20s", "[1s,1200s]",
        "io timeout for object storage, Range [1s,1200s]. "
        "The default value is 20s",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(data_storage_warning_tolerance_time, OB_CLUSTER_PARAMETER, "5s", "[1s,300s]",
        "time to tolerate disk read failure, after that, the disk status will be set warning. Range [1s,300s]. The default value is 5s",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME_WITH_CHECKER(data_storage_error_tolerance_time, OB_CLUSTER_PARAMETER, "300s", common::ObDataStorageErrorToleranceTimeChecker, "[10s,7200s]",
        "time to tolerate disk read failure, after that, the disk status will be set error. Range [10s,7200s]. The default value is 300s",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(data_disk_usage_limit_percentage, OB_CLUSTER_PARAMETER, "90", "[50,100]",
        "the safe use percentage of data disk"
        "Range: [50,100] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(sys_bkgd_net_percentage, OB_CLUSTER_PARAMETER, "60", "[0,100]",
        "the net percentage of sys background net. Range: [0, 100] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT_WITH_CHECKER(disk_io_thread_count, OB_CLUSTER_PARAMETER, "8",
                     common::ObConfigEvenIntChecker,
                     "[2,32]",
                     "The number of io threads on each disk. The default value is 8. Range: [2,32] in even integer",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(sync_io_thread_count, OB_CLUSTER_PARAMETER, "0",
        "[0,1024]",
        "The number of io threads for synchronizing request on each device. The default value is 0. Range: [0,1024] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_io_callback_thread_count, OB_TENANT_PARAMETER, "0", "[0,64]",
        "The number of io callback threads. The default value is 0. Range: [0,64] in integer. If not specified, The number of threads is dynamically configured according to the memory size",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_parallel_minor_merge, OB_TENANT_PARAMETER, "True",
         "specifies whether enable parallel minor merge. "
         "Value: True:turned on;  False: turned off",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_adaptive_compaction, OB_TENANT_PARAMETER, "True",
         "specifies whether allow adaptive compaction schedule and information collection",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(compaction_dag_cnt_limit, OB_TENANT_PARAMETER, "15000", "[10000,200000]",
        "the compaction dag count limit. Range: [10000,200000] in integer. default value is 15000",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(compaction_schedule_tablet_batch_cnt, OB_TENANT_PARAMETER, "50000", "[10000,200000]",
        "the batch size when scheduling tablet to execute compaction task. Range: [10000,200000] in integer. default value is 50000",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(compaction_low_thread_score, OB_TENANT_PARAMETER, "0", "[0,100]",
        "the current work thread score of low priority compaction. Range: [0,100] in integer. Especially, 0 means default value",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(compaction_mid_thread_score, OB_TENANT_PARAMETER, "0", "[0,100]",
        "the current work thread score of middle priority compaction. Range: [0,100] in integer. Especially, 0 means default value",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(compaction_high_thread_score, OB_TENANT_PARAMETER, "0", "[0,100]",
        "the current work thread score of high priority compaction. Range: [0,100] in integer. Especially, 0 means default value",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(ha_high_thread_score, OB_TENANT_PARAMETER, "0", "[0,100]",
        "the current work thread score of high availability high thread. Range: [0,100] in integer. Especially, 0 means default value",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(ha_mid_thread_score, OB_TENANT_PARAMETER, "0", "[0,100]",
        "the current work thread score of high availability mid thread. Range: [0,100] in integer. Especially, 0 means default value",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(ha_low_thread_score, OB_TENANT_PARAMETER, "0", "[0,100]",
        "the current work thread score of high availability low thread. Range: [0,100] in integer. Especially, 0 means default value",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(ddl_thread_score, OB_TENANT_PARAMETER, "0", "[0,100]",
        "the current work thread score of ddl thread. Range: [0,100] in integer. Especially, 0 means default value",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(minor_compact_trigger, OB_TENANT_PARAMETER, "2", "[0,16]",
        "minor_compact_trigger, Range: [0,16] in integer",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_compaction_diagnose, OB_CLUSTER_PARAMETER, "False",
         "enable compaction diagnose function"
         "Value:  True:turned on;  False: turned off",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(_force_skip_encoding_partition_id, OB_CLUSTER_PARAMETER, "",
        "force the specified partition to major without encoding row store, only for emergency!",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_private_buffer_size, OB_CLUSTER_PARAMETER, "16K", "[0B,)"
         "the trigger remaining data size within transaction for immediate logging, 0B represents not trigger immediate logging"
         "Range: [0B, total size of memory]",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_fast_commit_callback_count, OB_CLUSTER_PARAMETER, "10000", "[0,)"
        "trigger max callback count allowed within transaction for durable callback checkpoint, 0 represents not allow durable callback"
        "Range: [0, not limited callback count",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_minor_compaction_amplification_factor, OB_TENANT_PARAMETER, "0", "[0,100]",
        "thre L1 compaction write amplification factor, 0 means default 25, Range: [0,100] in integer",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(major_compact_trigger, OB_TENANT_PARAMETER, "0", "[0,65535]",
        "specifies how many minor freeze should be triggered between two major freeze, Range: [0,65535] in integer",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(ob_compaction_schedule_interval, OB_TENANT_PARAMETER, "120s", "[3s,5m]",
         "the time interval to schedule compaction, Range: [3s,5m]"
         "Range: [3s, 5m]",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_ob_elr_fast_freeze_threshold, OB_CLUSTER_PARAMETER, "500000", "[10000,)",
         "per row update counts threshold to trigger minor freeze for tables with ELR optimization",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_ob_enable_fast_freeze, OB_TENANT_PARAMETER, "True",
         "specifies whether the tenant's fast freeze is enabled"
         "Value: True:turned on;  False: turned off",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_adaptive_merge_schedule, OB_TENANT_PARAMETER, "True",
         "specifies whether the tenant's adaptive merge scheduling is enabled"
         "Value: True:turned on;  False: turned off",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(sys_bkgd_migration_retry_num, OB_CLUSTER_PARAMETER, "3", "[3,100]",
        "retry num limit during migration. Range: [3, 100] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(sys_bkgd_migration_change_member_list_timeout, OB_CLUSTER_PARAMETER, "20s", "[0s,24h]",
         "the timeout for migration change member list retry. "
         "The default value is 20s. Range: [0s,24h]",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//Tablet config
DEF_CAP_WITH_CHECKER(tablet_size, OB_CLUSTER_PARAMETER, "128M",
                     common::ObConfigTabletSizeChecker,
                     "default tablet size, has to be a multiple of 2M",
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
        "2 : verify encoding and compression algorithm, besides encoding verification, compressed block will be decompressed to ensure data is correct"
        "3 : verify encoding, compression algorithm and lost write protect",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_migrate_block_verify_level, OB_CLUSTER_PARAMETER, "1", "[0,2]",
        "specify what kind of verification should be done when migrating macro block. "
        "0 : no verification will be done "
        "1 : physical verification"
        "2 : logical verification",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_cache_wash_interval, OB_CLUSTER_PARAMETER, "200ms", "[1ms, 3s]",
        "specify interval of cache background wash",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_max_ls_cnt_per_server, OB_TENANT_PARAMETER, "0", "[0, 1024]",
        "specify max ls count of one tenant on one observer."
        "WARNING: Modifying this can potentially destabilize the cluster. It is strongly advised to avoid making such changes as they are unlikely to be necessary."
        "0: the cluster will adapt the max ls number according to the memory size of tenant itself",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_CAP(_io_read_batch_size, OB_TENANT_PARAMETER, "0K", "[0K,16M]", "Maximum batch size in one read io request. Range:[0K,16M]",
        ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_io_read_redundant_limit_percentage, OB_TENANT_PARAMETER, "0", "[0, 99]",
        "Maximum percentage of redundant size in one read io request, redundant data means blocks in the middle of the batch that hit in cache or filtered by skipping index but must be read. Range:[0,99]",
        ObParameterAttr(Section::SSTABLE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// TODO bin.lb: to be remove
DEF_CAP(dtl_buffer_size, OB_CLUSTER_PARAMETER, "64K", "[4K,2M]", "to be removed",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// TODO bin.lb: to be remove
DEF_CAP(px_task_size, OB_CLUSTER_PARAMETER, "2M", "[1K,)", "to be removed",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_max_elr_dependent_trx_count, OB_CLUSTER_PARAMETER, "0", "[0,)", "max elr dependent transaction count",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_INT_LIST(errsim_ddl_sim_point_random_control, OB_TENANT_PARAMETER, "",
        "the ddl sim point param in errsim mode, once set successfully, no more modifications"
        "format like: seed; point_count_per_task",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_INT_LIST(errsim_ddl_sim_point_fixed_list, OB_TENANT_PARAMETER, "",
        "the fixed ddl sim point list in errsim mode, "
        "format like: point_id_1; point_id_2; point_id_3",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_INT(errsim_max_backup_retry_count, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "max backup retry count in errsim mode"
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_TIME(errsim_max_backup_meta_retry_time_interval, OB_CLUSTER_PARAMETER, "10s", "[1s,5m]",
        "max backup meta retry time interval in errsim mode"
        "Range: [1s, 5m]",
        ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_INT(errsim_storage_meta_macro_ids_threshold, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "the threshold value that macro block list is stored by linked way in storage meta. "
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(errsim_tablet_batch_count, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "batch tablet count when in errsim mode"
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(errsim_large_co_sstable_threshold, OB_CLUSTER_PARAMETER, "0", "[0, 1966080]",
        "large_co_sstable size threshold (byte) when in errsim mode"
        "Range: [0, 1966080] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(errsim_backup_ls_id, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "the ls id that backup want to insert error"
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(errsim_backup_tablet_id, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "the tablet id that backup want to insert error"
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(errsim_backup_task_id, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "the task id that backup want to insert error"
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(errsim_transfer_ls_id, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "the ls id that transfer want to insert error"
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(skip_report_pg_backup_task_table_id, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "skip_report_pg_backup_task table id"
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
ERRSIM_DEF_BOOL(skip_update_estimator, OB_CLUSTER_PARAMETER, "False",
         "skip update_estimator during daily merge. ",
         ObParameterAttr(Section::DAILY_MERGE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(with_new_migrated_replica, OB_CLUSTER_PARAMETER, "False",
         "it is new migrated replica, can not be removed by old migration. ",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(sys_bkgd_migration_large_task_threshold, OB_CLUSTER_PARAMETER, "2h", "[0s,24h]",
         "the timeout for migration change member list retry. "
         "The default value is 2h. Range: [0s,24h]",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(macro_block_builder_errsim_flag, OB_CLUSTER_PARAMETER, "0", "[0,100]",
        "macro_block_builder_errsim_flag"
        "Range: [0,100] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(skip_balance_disk_limit, OB_CLUSTER_PARAMETER, "False",
         "skip_dbalance_isk_limit. ",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(pwrite_errsim, OB_CLUSTER_PARAMETER, "False",
         "pwrite_errsim",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_BOOL(fake_disk_error, OB_CLUSTER_PARAMETER, "False",
         "fake_disk_error",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(_max_block_per_backup_task, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "the max macro block count per backup sub task"
        "Range: [0,max) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(backup_lease_takeover_time, OB_CLUSTER_PARAMETER, "10s", "[1s, 5m]",
         "Lease Takeover Time for Rootserver Backup heartbeat. Range: [1s, 5m]",
         ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_TIME(trigger_auto_backup_delete_interval, OB_CLUSTER_PARAMETER, "1h", "[1s,)",
         "trigger auto backup delete interval."
         "The default value is 1h. Range: [1s,)",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_INT(errsim_max_restore_retry_count, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "max restore retry count in errsim mode"
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_TIME(errsim_ddl_major_delay_time, OB_CLUSTER_PARAMETER, "0s", "[0s,3600s]",
        "ddl create major sstable delay time in errsim mode, Range [0s,3600s]. "
        "The default value is 120s",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(errsim_test_tablet_id, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "test tablet id in errsim mode, for error injection"
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_INT(errsim_backup_table_list_batch_size, OB_CLUSTER_PARAMETER, "20000", "[1,)",
        "batch size of table list items in errsim mode"
        "Range: [1,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_BOOL(enable_quick_restore_remove_backup_dest_test, OB_CLUSTER_PARAMETER, "False",
         "when remove backup dest test mode is enabled, io from backup will "
         "resturn OB_EAGAIN",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

#ifdef TRANS_MODULE_TEST
DEF_INT(module_test_trx_memory_errsim_percentage, OB_CLUSTER_PARAMETER, "0", "[0, 100]",
        "the percentage of memory errsim. Rang:[0,100]",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
#endif

DEF_CAP(sql_work_area, OB_TENANT_PARAMETER, "1G", "[10M,)",
        "Work area memory limitation for tenant",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_CAP(__easy_memory_limit, OB_CLUSTER_PARAMETER, "4G", "[256M,)",
        "max memory size which can be used by libeasy. The default value is 4G. Range: [256M,)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(stack_size, OB_CLUSTER_PARAMETER, "512K", "[512K, 20M]",
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
//ssl
DEF_BOOL(ssl_client_authentication, OB_CLUSTER_PARAMETER, "False",
         "enable server SSL support. Takes effect after ca/cert/key file is configured correctly. ",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(use_ipv6, OB_CLUSTER_PARAMETER, "False",
         "Whether this server uses ipv6 address",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));

//audit 审计功能相关
DEF_BOOL(audit_sys_operations, OB_TENANT_PARAMETER, "False",
         "whether trace sys user operations",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(audit_trail, OB_TENANT_PARAMETER, "None",
         common::ObConfigAuditTrailChecker,
         "enables or disables database auditing, support NONE;OS;DB;DB,EXTENDED;DB_EXTENDED",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(audit_log_enable, OB_TENANT_PARAMETER, "False",
         "whether enable audit log",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(audit_log_buffer_size, OB_TENANT_PARAMETER, "16M", "[16M,)"
         "the buffer size of async audit log"
         "Range: [16M, total size of memory]",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(audit_log_compression, OB_TENANT_PARAMETER, "NONE",
         common::ObConfigAuditLogCompressionChecker,
         "the type of compression for the audit log file, values: NONE, ZSTD",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(audit_log_path, OB_TENANT_PARAMETER, "",
         common::ObConfigAuditLogPathChecker,
         "the directory of the audit log",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(audit_log_format, OB_TENANT_PARAMETER, "CSV",
         common::ObConfigAuditLogFormatChecker,
         "the audit log file format, values: CSV",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(audit_log_max_size, OB_TENANT_PARAMETER, "0M", "[0M,)",
         "the maximum combined size of the audit log files",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(audit_log_prune_seconds, OB_TENANT_PARAMETER, "0", "[0,)",
         "the number of seconds after which audit log files become subject to pruning, range: [0,)",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(audit_log_query_sql, OB_TENANT_PARAMETER, "ALL",
         common::ObConfigAuditLogQuerySQLChecker,
         "how to record the query sql. "
         "ALL: record the original query sql. "
         "DESENSITIVE: record the desensitive query sql. "
         "NONE: not to record query sql.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(audit_log_rotate_on_size, OB_TENANT_PARAMETER, "256M", "[0,)"
         "whenever a write to the audit log file causes its size to exceed the config value, "
         "it will be renamed and a new audit log file using is opened, range: [0,)",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(audit_log_strategy, OB_TENANT_PARAMETER, "ASYNCHRONOUS",
         common::ObConfigAuditLogStrategyChecker,
         "the logging method used by the audit log plugin. "
         "ASYNCHRONOUS: Log asynchronously. Wait for space in the output buffer. "
         "PERFORMANCE: Log asynchronously. Drop requests when there is insufficient buffer. "
         "SYNCHRONOUS: Log synchronously.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//ddl 超时时间
DEF_TIME(_ob_ddl_timeout, OB_CLUSTER_PARAMETER, "1000s", "[1s,)",
         "the config parameter of ddl timeout"
         "Range: [1s, +∞)",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// backup 备份恢复相关的配置
DEF_CAP(backup_data_file_size, OB_TENANT_PARAMETER, "4G", "[512M,4G]",
        "backup data file size. "
        "Range: [512M, 4G] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_backup_task_keep_alive_interval, OB_TENANT_PARAMETER, "10s", "[1s,)",
         "control backup task keep alive interval"
         "Range: [1s, +∞)",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_backup_task_keep_alive_timeout, OB_TENANT_PARAMETER, "10m", "[1s,)",
         "control backup task keep alive timeout"
         "Range: [1s, +∞)",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));


DEF_BOOL(ob_enable_batched_multi_statement, OB_TENANT_PARAMETER, "False",
         "enable use of batched multi statement",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

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
DEF_BOOL(_ob_enable_prepared_statement, OB_CLUSTER_PARAMETER, "True",
         "control if enable prepared statement",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(_upgrade_stage, OB_CLUSTER_PARAMETER, "NONE",
        common::ObConfigUpgradeStageChecker,
        "specifies the upgrade stage. "
        "NONE: in non upgrade stage, "
        "PREUPGRADE: in pre upgrade stage, "
        "DBUPGRADE: in db uprade stage, "
        "POSTUPGRADE: in post upgrade stage. ",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::READONLY));
DEF_STR_WITH_CHECKER(_ob_plan_cache_gc_strategy, OB_CLUSTER_PARAMETER, "REPORT",
         common::ObConfigPlanCacheGCChecker,
         "OFF: disabled, "
         "REPORT: check leaked cache object infos only, "
         "AUTO: check and release leaked cache obj.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_plan_cache_mem_diagnosis, OB_CLUSTER_PARAMETER, "False",
         "wether turn plan cache ref count diagnosis on",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR(external_kms_info, OB_TENANT_PARAMETER, "",
        "when using the external key management center, "
        "this parameter will store some key management information",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR(tde_method, OB_TENANT_PARAMETER, "none",
        "none : transparent encryption is none, none means cannot use tde, "
        "internal : transparent encryption is in the form of internal tables, "
        "bkmi : transparent encryption is in the form of external bkmi",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(schema_history_recycle_interval, OB_CLUSTER_PARAMETER, "10m", "[0s,]",
         "the time interval between the schedules of schema history recyle task. "
         "Range: [0s, +∞)",
         ObParameterAttr(Section::LOAD_BALANCE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_oracle_priv_check, OB_CLUSTER_PARAMETER, "True",
         "whether turn on oracle privilege check ",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(plsql_code_type, OB_TENANT_PARAMETER, "native",
        "specifies the compilation mode for PL/SQL library units",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(plsql_debug, OB_TENANT_PARAMETER, "False",
         "specifies whether or not PL/SQL library units will be compiled for debugging",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(plsql_v2_compatibility, OB_TENANT_PARAMETER, "False",
         "allows to control store routine compile action at DDL stage",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(sts_credential, OB_TENANT_PARAMETER, "",
        common::ObConfigSTScredentialChecker,
        "STS credential for object storage, "
        "values: sts_url=xxx&sts_ak=xxx&sts_sk=xxx",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// for bloom filter
DEF_BOOL(_bloom_filter_enabled, OB_TENANT_PARAMETER, "True",
         "enable join bloom filter",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(use_large_pages, OB_CLUSTER_PARAMETER, "false",
                     common::ObConfigUseLargePagesChecker,
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
DEF_STR_WITH_CHECKER(_audit_mode, OB_TENANT_PARAMETER, "NONE",
        common::ObConfigAuditModeChecker,
        "specifies audit mode,"
        "NONE: close audit,"
        "MYSQL: use mysql audit"
        "ORACLE: use oracle audit",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_max_schema_slot_num, OB_TENANT_PARAMETER, "128", "[2,256]",
        "the max schema slot number for multi-version schema memory management, "
        "Range: [2, 256] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_add_fulltext_index_to_existing_table, OB_CLUSTER_PARAMETER, "False",
         "enable create fulltext index after table is created",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT_WITH_CHECKER(_ob_query_rate_limit, OB_TENANT_PARAMETER, "-1",
        common::ObConfigQueryRateLimitChecker,
        "the maximun throughput allowed for a tenant per observer instance",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_xa_gc_timeout, OB_CLUSTER_PARAMETER, "24h", "[1s,)",
        "specifies the threshold value for a xa record to be considered as obsolete",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_xa_gc_interval, OB_CLUSTER_PARAMETER, "1h", "[1s,)",
        "specifies the scan interval of the gc worker",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_easy_keepalive, OB_CLUSTER_PARAMETER, "True",
         "enable keepalive for each TCP connection.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_ob_ratelimit, OB_CLUSTER_PARAMETER, "False",
         "enable ratelimit between regions for RPC connection.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(ob_ratelimit_stat_period, OB_CLUSTER_PARAMETER, "1s", "[100ms,]",
         "the time interval to update observer's maximum bandwidth to a certain region. ",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(open_cursors, OB_TENANT_PARAMETER, "50", "[0,65535]",
        "specifies the maximum number of open cursors a session can have at once."
        "can use this parameter to prevent a session from opening an excessive number of cursors."
        "Range: [0, 65535] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_enhanced_cursor_validation, OB_TENANT_PARAMETER, "False",
        "enable enhanced cursor validation, which let cursor can be fetched after transaction committed"
        " if it has not read uncommitted data.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_px_batch_rescan, OB_TENANT_PARAMETER, "True",
         "enable px batch rescan for nlj or subplan filter",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_spf_batch_rescan, OB_TENANT_PARAMETER, "False",
         "enable das batch rescan for subplan filter",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_das_keep_order, OB_TENANT_PARAMETER, "True",
         "enable das keep order optimization",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_nlj_spf_use_rich_format, OB_TENANT_PARAMETER, "True",
         "enable nlj and spf use rich format",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_parallel_max_active_sessions, OB_TENANT_PARAMETER, "0", "[0,]",
        "max active parallel sessions allowed for tenant. Range: [0,+∞)",
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
DEF_INT(_send_bloom_filter_size, OB_CLUSTER_PARAMETER, "1024", "[1,]",
         "Set send bloom filter slice size"
         "Range: [1, +∞)",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_px_ordered_coord, OB_CLUSTER_PARAMETER, "false",
         "enable px task ordered coord",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(connection_control_failed_connections_threshold, OB_TENANT_PARAMETER, "0", "[0,2147483647]",
        "The number of consecutive failed connection attempts permitted to accounts"
        "before the server adds a delay for subsequent connection attempts",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(connection_control_min_connection_delay, OB_TENANT_PARAMETER, "1000", "[1000,2147483647]",
        "The minimum delay in milliseconds for server response to failed connection attempts, "
        "if connection_control_failed_connections_threshold is greater than zero.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(connection_control_max_connection_delay, OB_TENANT_PARAMETER, "2147483647", "[1000,2147483647]",
        "The maximum delay in milliseconds for server response to failed connection attempts, "
        "if connection_control_failed_connections_threshold is greater than zero",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(ob_proxy_readonly_transaction_routing_policy, OB_TENANT_PARAMETER, "false",
         "Proxy route policy for readonly sql: whether regard begining read only stmts as in transaction",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(job_queue_processes, OB_TENANT_PARAMETER, "1000", "[0,16384]",
        "specifies the maximum number of job slaves per instance that can be created "
        "for the execution of DBMS_JOB jobs and Oracle Scheduler (DBMS_SCHEDULER) jobs.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_resource_limit_spec, OB_CLUSTER_PARAMETER, "False",
         "specify whether resource limit check is turned on",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(_resource_limit_spec, OB_CLUSTER_PARAMETER, "auto", common::ObConfigResourceLimitSpecChecker,
        "this parameter encodes some resource limit parameters to json",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_resource_limit_max_session_num, OB_TENANT_PARAMETER, "0", "[0,1000000]",
        "the maximum number of sessions that can be created concurrently",
        ObParameterAttr(Section::RESOURCE_LIMIT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(_px_bloom_filter_group_size, OB_TENANT_PARAMETER, "auto", common::ObConfigPxBFGroupSizeChecker,
         "specifies the px bloom filter each group size in sending to the other sqc"
         "Range: [1, +∞) or auto, the default value is auto",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(enable_sql_extension, OB_TENANT_PARAMETER, "False",
         "specifies whether to allow use some oracle mode features in mysql mode",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_block_file_punch_hole, OB_CLUSTER_PARAMETER, "False",
         "specifies whether to punch whole when free blocks in block_file",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_trace_session_leak, OB_CLUSTER_PARAMETER, "False",
         "specifies whether to enable tracing session leak",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_ob_enable_fast_parser, OB_CLUSTER_PARAMETER, "True",
         "control if enable fast parser",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_ob_obj_dep_maint_task_interval, OB_CLUSTER_PARAMETER, "1ms", "[0,10s]",
         "The execution interval of the task of maintaining the dependency of the object. "\
         "Range: [0, 10s]",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_newsort, OB_CLUSTER_PARAMETER, "True",
         "control if enable encode sort",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_session_context_size, OB_CLUSTER_PARAMETER, "10000", "[0, 2147483647]",
         "limits the total number of (namespace, attribute) pairs "
         "used by all application contexts in the user session.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::READONLY));
DEF_INT(_px_object_sampling, OB_TENANT_PARAMETER, "200", "[1, 100000]"
        "parallel query sampling for base objects (100000 = 100%)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_follower_snapshot_read_retry_duration, OB_TENANT_PARAMETER, "0ms", "[0ms,]",
         "the waiting time after the first judgment failure of strong reading on follower"
         "Range: [0ms, +∞)",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(default_auto_increment_mode, OB_TENANT_PARAMETER, "order",
        common::ObAutoIncrementModeChecker, "specifies default auto-increment mode, default is 'order'",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(_trace_control_info, OB_TENANT_PARAMETER, "{\"type_t\":{\"level\":1,\"sample_pct\":\"0.10\",\"record_policy\":\"SAMPLE_AND_SLOW_QUERY\"}}",
        "persistent control information for full-link trace",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_print_sample_ppm, OB_TENANT_PARAMETER, "0", "[0, 1000000]",
        "In the full link diagnosis, control the frequency of printing traces to the log (unit is ppm, parts per million).",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(ob_query_switch_leader_retry_timeout, OB_TENANT_PARAMETER, "0ms", "[0ms,]",
         "max time spend on retry caused by leader swith or network disconnection"
         "Range: [0ms, +∞)",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(default_enable_extended_rowid, OB_TENANT_PARAMETER, "false",
         "specifies whether to create table as extended rowid mode or not",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_new_sql_nio, OB_CLUSTER_PARAMETER, "true",
"specifies whether SQL serial network is turned on. Turned on to support mysql_send_long_data"
"The default value is FALSE. Value: TRUE: turned on FALSE: turned off",
ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_BOOL(_enable_parallel_das_dml, OB_TENANT_PARAMETER, "False",
         "By default, the das service is allowed to use multiple threads to submit das tasks",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_insertup_replace_gts_opt, OB_TENANT_PARAMETER, "True",
         "By default, insert/replace ... values statement can use gts optimization",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// Add a config to enable use das if the sql statement has variable assignment
DEF_BOOL(_enable_var_assign_use_das, OB_TENANT_PARAMETER, "False",
         "enable use das if the sql statement has variable assignment",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// query response time
DEF_BOOL(query_response_time_stats, OB_TENANT_PARAMETER, "True",
    "Enable or disable QUERY_RESPONSE_TIME statistics collecting"
    "The default value is True. Value: TRUE: turned on FALSE: turned off",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(query_response_time_flush, OB_TENANT_PARAMETER, "False",
    "Flush QUERY_RESPONSE_TIME table and re-read query_response_time_range_base"
    "The default value is False. Value: TRUE: trigger flush FALSE: do not trigger",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(query_response_time_range_base, OB_TENANT_PARAMETER, "10", "[2,10000]",
    "Select base of log for QUERY_RESPONSE_TIME ranges. WARNING: variable change takes affect only after flush."
    "The default value is 10. Range: [2,10000]. ",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(arbitration_timeout, OB_TENANT_PARAMETER, "5s", "[3s,]",
        "The timeout before automatically degrading when arbitration member exists. Range: [3s,+∞]",
        ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_ignore_system_memory_over_limit_error, OB_CLUSTER_PARAMETER, "False",
         "When the hold of observer tenant is over the system_memory, print ERROR with False, or WARN with True",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_STR(backup_errsim_task_server_addr, OB_CLUSTER_PARAMETER, "",
        "the server addr that backup task send to when in errsim mode",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
#ifdef OB_USE_ASAN
DEF_BOOL(enable_asan_for_memory_context, OB_CLUSTER_PARAMETER, "False",
        "when use ob_asan, user can specifies whether to allow ObAsanAllocator(default is ObAllocator as allocator of MemoryContext",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
#endif
DEF_INT(sql_login_thread_count, OB_CLUSTER_PARAMETER, "0", "[0,32]",
        "the number of threads for sql login request. Range: [0, 32] in integer, 0 stands for use default thread count defined in TG."
        "the default thread count for login request in TG is normal:6 mini-mode:2",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(tenant_sql_login_thread_count, OB_TENANT_PARAMETER, "0", "[0,32]",
        "the number of threads for sql login request of each tenant. Range: [0, 32] in integer, 0 stands for unit_min_cpu",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(tenant_sql_net_thread_count, OB_TENANT_PARAMETER, "0", "[0, 64]",
        "the number of mysql I/O threads for a tenant. Range: [0, 64] in integer, 0 stands for unit_min_cpu",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(sql_net_thread_count, OB_CLUSTER_PARAMETER, "0", "[0,64]",
        "the number of global mysql I/O threads. Range: [0, 64] in integer, "
        "default value is 0, 0 stands for old value GCONF.net_thread_count",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_tenant_sql_net_thread, OB_CLUSTER_PARAMETER, "True",
        "Dispatch mysql request to each tenant with True, or disable with False",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(_endpoint_tenant_mapping, OB_CLUSTER_PARAMETER, "",
        "This parameter will store the mapping of endpoint and tenant",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
#ifndef ENABLE_SANITY
#else
DEF_STR_LIST(sanity_whitelist, OB_CLUSTER_PARAMETER, "", "vip who wouldn't leading to coredump",
             ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_tenant_leak_memory_protection, OB_CLUSTER_PARAMETER, "False", "protect unfreed objects while deletes tenant",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
#endif
DEF_TIME(_advance_checkpoint_timeout, OB_CLUSTER_PARAMETER, "30m", "[10s,180m]",
         "the timeout for backup/migrate advance checkpoint Range: [10s,180m]",
         ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(errsim_allow_force_archive_threshold, OB_CLUSTER_PARAMETER, "600s", "[1s,2h]",
                "force stop archive threshold, Range: [1s,2h]",
                ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//transfer
DEF_TIME(_transfer_start_rpc_timeout, OB_TENANT_PARAMETER, "10s", "[1ms,600s]",
        "transfer start status rpc check some status ready timeout, Range [1ms,600s]. "
        "The default value is 10s",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_transfer_finish_trans_timeout, OB_TENANT_PARAMETER, "10s", "[1s,600s]",
        "transfer finish transaction timeout, Range [1s,600s]. "
        "The default value is 10s",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_transfer_start_trans_timeout, OB_TENANT_PARAMETER, "1s", "[1ms,600s]",
        "transfer start transaction timeout, Range [1ms,600s]. "
        "The default value is 1s",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_transfer_start_retry_count, OB_TENANT_PARAMETER, "3", "[0,64]",
        "the number of transfer start retry. Range: [0, 64] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));


DEF_TIME(_transfer_service_wakeup_interval, OB_TENANT_PARAMETER, "5m", "[1s,5m]",
        "transfer service wakeup interval in errsim mode. "
        "Range: [1s, 5m]",
        ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_transfer_process_lock_tx_timeout, OB_TENANT_PARAMETER, "100s", "[30s,)",
        "transaction timeout for locking and unlocking transfer task. "
        "Range: [30s, +∞)",
        ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_transfer_task_tablet_count_threshold, OB_TENANT_PARAMETER, "100", "(0,)",
        "Threshold for the count of tablets that can be processed by a transfer task. "
        "Range: (0, +∞)",
        ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_active_txn_transfer, OB_TENANT_PARAMETER, "True",
        "Specifies whether support transfer active tx",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_transfer_task_retry_interval, OB_TENANT_PARAMETER, "1m", "[0s,)",
        "Retry interval after transfer task failure. "
        "Range: [0s, +∞). Default: 1m",
        ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// end of transfer

DEF_TIME(dump_data_dictionary_to_log_interval, OB_TENANT_PARAMETER, "24h", "(0s,]",
         "data dictionary dump to log(SYS LS) interval"
        "Range: (0s,+∞)",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(errsim_migration_tablet_id, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "the tablet id that migration want to insert error"
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(errsim_max_ddl_sstable_count, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "max ddl sstable count in errsim mode"
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(errsim_max_ddl_block_count, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "max ddl block count in errsim mode"
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_STR(errsim_migration_src_server_addr, OB_CLUSTER_PARAMETER, "",
        "the server dest ls choose as src when in errsim mode",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_STR(errsim_migration_dest_server_addr, OB_CLUSTER_PARAMETER, "",
        "the migration dest in errsim mode",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(enable_cgroup, OB_CLUSTER_PARAMETER, "True",
         "when set to false, cgroup will not init; when set to true but cgroup root dir is not ready, print ERROR",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_global_background_resource_isolation, OB_CLUSTER_PARAMETER, "False",
         "When set to false, foreground and background tasks are isolated within the tenant; When set to true, isolate background tasks individually upon tenant-level",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_DBL(global_background_cpu_quota, OB_CLUSTER_PARAMETER, "-1", "[-1,)",
         "When enable_global_background_resource_isolation is True, specify the number of vCPUs allocated to the background tasks"
         "-1 for the CPU is not limited by the cgroup",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_user_defined_rewrite_rules, OB_TENANT_PARAMETER, "False",
         "specify whether the user defined rewrite rules are enabled. "
         "Value: True: enable  False: disable",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_ob_plan_cache_auto_flush_interval, OB_CLUSTER_PARAMETER, "0s", "[0s,)",
         "time interval for auto periodic flush plan cache. Range: [0s, +∞)",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(errsim_migration_ls_id, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "errsim migration ls id. Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_STR(errsim_transfer_backfill_server_addr, OB_CLUSTER_PARAMETER, "",
        "the server addr that transfer backfill forbid to execute to when in errsim mode",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_ob_enable_direct_load, OB_CLUSTER_PARAMETER, "True",
         "Enable or disable direct path load",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_dblink, OB_CLUSTER_PARAMETER, "True",
         "Enable or disable dblink",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(_display_mysql_version, OB_CLUSTER_PARAMETER, "5.7.25", common::ObMySQLVersionLengthChecker,
        "dynamic mysql version of mysql mode observer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_px_join_skew_handling, OB_TENANT_PARAMETER, "False",
        "enables skew handling for parallel joins. The  default value is True.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_px_join_skew_minfreq, OB_TENANT_PARAMETER, "30", "[1,100]",
        "sets minimum frequency(%) for skewed value for parallel joins. Range: [1, 100] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_protocol_diagnose, OB_CLUSTER_PARAMETER, "True",
        "enables protocol layer diagnosis. The default value is False.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_transaction_internal_routing, OB_TENANT_PARAMETER, "True",
         "enable SQLs of transaction routed to any servers in the cluster on demand",
         ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_wait_remote_lock, OB_TENANT_PARAMETER, "True",
         "enable remote execution wait in lock wait mgr when lock conflict occurs",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(_load_tde_encrypt_engine, OB_CLUSTER_PARAMETER, "NONE",
        "load the engine that meet the security classification requirement to encrypt data.  default NONE",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(local_ip, OB_CLUSTER_PARAMETER, "", "the IP address of the machine on which the ObServer will be installed",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::READONLY));
DEF_INT(observer_id, OB_CLUSTER_PARAMETER, "0", "[1, 18446744073709551615]",
        "the unique id that been assigned by rootservice for each observer in cluster, "
        "default: 0 (invalid id), Range: [1, 18446744073709551615]",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::READONLY));
DEF_INT(_pipelined_table_function_memory_limit, OB_TENANT_PARAMETER, "524288000", "[1024,18446744073709551615]",
        "pipeline table function result set memory size limit. default 524288000 (500M), Range: [1024,18446744073709551615]",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_balance_kill_transaction, OB_TENANT_PARAMETER, "False",
        "Specifies whether balance should actively kill transaction",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_balance_kill_transaction_threshold, OB_TENANT_PARAMETER, "100ms", "[1ms, 60s]",
         "the time given to the transaction to execute when do balance"
         "before it will be killed. Range: [1ms, 60s]",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_balance_wait_killing_transaction_end_threshold, OB_TENANT_PARAMETER, "100ms", "[10ms, 60s]",
         "the threshold for waiting time after killing transactions until they end."
         "Range: [10ms, 60s]",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_hgby_skew_detection, OB_TENANT_PARAMETER, "True",
         "specifies whether hgby skew detection is enabled",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_px_fast_reclaim, OB_CLUSTER_PARAMETER, "True",
        "Enable the fast reclaim function through PX tasks deteting for survival by detect manager. The default value is True.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_pdml_thread_cache_size, OB_CLUSTER_PARAMETER, "2M", "[1B,)",
        "The cache size of per pdml thread."
        "Range:[1B, )",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_hgby_llc_ndv_adaptive, OB_TENANT_PARAMETER, "True",
         "specifies whether llc ndv adptive is activated",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_reserved_user_dcl_restriction, OB_CLUSTER_PARAMETER, "False",
         "specifies whether to forbid non-reserved user to modify reserved users",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(rpc_client_authentication_method, OB_CLUSTER_PARAMETER, "NONE",
        common::ObRpcClientAuthMethodChecker,
        "specifies rpc client authentication method. "
        "NONE: without authentication. "
        "SSL_NO_ENCRYPT: authentication by SSL handshake but not encrypt the communication channel. "
        "SSL_IO: authentication by SSL handshake and encrypt the communication channel",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(rpc_server_authentication_method, OB_CLUSTER_PARAMETER, "ALL",
        common::ObRpcServerAuthMethodChecker,
        "specifies rpc server authentication method. "
        "ALL: support all authentication methods. "
        "NONE: without authentication. "
        "SSL_NO_ENCRYPT: authentication by SSL handshake but not encrypt the communication channel. "
        "SSL_IO: authentication by SSL handshake and encrypt the communication channel",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_backtrace_function, OB_CLUSTER_PARAMETER, "True",
         "Decide whether to let the backtrace function take effect",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_dblink_reuse_connection, OB_TENANT_PARAMETER, "True",
         "specifies whether dblink reuse connection in a session",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_with_subquery, OB_TENANT_PARAMETER, "0", "[0,2]",
        "WITH subquery transformation,0: optimizer,1: materialize,2: inline",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_max_dblink_conn_per_observer, OB_TENANT_PARAMETER, "256", "[0,)",
        "The maximum limit on the number of connections that can be opened simultaneously for a specific observer for any DBLink, default value is 256",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_xsolapi_generate_with_clause, OB_TENANT_PARAMETER, "True",
        "OLAP API generates WITH clause",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_optimizer_group_by_placement, OB_TENANT_PARAMETER, "True",
        "enable group by placement transform rule",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_complex_cbqt_table_num, OB_TENANT_PARAMETER, "10", "[0,)",
        "cost-based transform will be disabled when table count in a single stmt exceeds threshold",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_wait_interval_after_parallel_ddl, OB_TENANT_PARAMETER, "30s", "[0s,)",
        "time interval for waiting other servers to refresh schema after parallel ddl is done",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_rebuild_replica_log_lag_threshold, OB_TENANT_PARAMETER, "0M", "[0M,)",
        "size of clog files that a replica lag behind leader to trigger rebuild, 0 means never trigger rebuild on purpose. Range: [0, +∞)",
        ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_in_range_optimization, OB_TENANT_PARAMETER, "True",
        "Enable extract query range optimization for in predicate",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_force_explict_500_malloc, OB_CLUSTER_PARAMETER, "False",
         "Force 500 memory for explicit allocation",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(range_optimizer_max_mem_size, OB_TENANT_PARAMETER, "128M", "[0M,)",
        "to limit the memory consumption for the query range optimizer. Range: [0M,+∞), 0 stands for unlimited",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_schema_memory_recycle_interval, OB_CLUSTER_PARAMETER, "15m", "[0s,)",
        "the time interval between the schedules of schema memory recycle task. "
        "0 means only turn off gc current allocator, "
        "and other schema memory recycle task's interval will be 15mins. "
        "Range [0s,)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
#ifdef ENABLE_500_MEMORY_LIMIT
DEF_BOOL(_enable_system_tenant_memory_limit, OB_CLUSTER_PARAMETER, "True",
         "specifies whether allowed to limit the memory of tenant 500",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_system_tenant_limit_mode, OB_CLUSTER_PARAMETER, "1", "[0,2]",
        "specifies the limit mode for the memory hold of system tenant, "
        "0: not limit the memory hold of system tenant, "
        "1: only limit the DEFAULT_CTX_ID memory of system tenant, "
        "2: besides limit the DEFAULT_CTX_ID memory, the total hold of system tenant is also limited.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
#endif
DEF_BOOL(_force_malloc_for_absent_tenant, OB_CLUSTER_PARAMETER, "False",
         "force malloc even if tenant does not exist in observer",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(errsim_transfer_backfill_error_time, OB_TENANT_PARAMETER, "0", "[0s,1h]",
        "the duration of the error happened to transfer backfill. "
        "Range: [0s, 1h] in duration",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_stall_threshold_for_dynamic_worker, OB_TENANT_PARAMETER, "3ms", "[0ms,)",
        "threshold of dynamic worker works",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_optimizer_better_inlist_costing, OB_TENANT_PARAMETER, "True",
        "enable improved costing of index access using in-list(s)",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_optimizer_skip_scan_enabled, OB_TENANT_PARAMETER, "False",
        "enable/disable index skip scan",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_ls_migration_wait_completing_timeout, OB_TENANT_PARAMETER, "30m", "[60s,)",
        "the wait timeout in ls complete migration phase",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// column store section
DEF_BOOL(_enable_column_store, OB_TENANT_PARAMETER, "True",
        "enable the column store format in storage engine",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_skip_index, OB_TENANT_PARAMETER, "True",
        "enable the skip index in storage engine",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(_ob_ddl_temp_file_compress_func, OB_TENANT_PARAMETER, "AUTO",
        common::ObConfigTempStoreFormatChecker,
        "specific compression in ObTempBlockStore."\
        "AUTO: use compression algorithm from table schema;"\
        "ZSTD: use ZSTD compression algorithm;"\
        "LZ4: use LZ4 compression algorithm;"\
        "NONE: do not use compression.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_prefetch_limiting, OB_TENANT_PARAMETER, "False",
         "enable limiting memory in prefetch for single query",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR(_storage_leak_check_mod, OB_CLUSTER_PARAMETER, "", "set leak check mod in storage",
     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_ha_tablet_info_batch_count, OB_TENANT_PARAMETER, "0", "[0,]",
        "the number of tablet replica info sent by on rpc for ha. Range: [0, +∞) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_ha_rpc_timeout, OB_TENANT_PARAMETER, "0", "[0,120s]",
         "the rpc timeout for storage high availability. Range:[0, 120s]",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(_ob_ash_size, OB_CLUSTER_PARAMETER, "0M", "[0,1G]",
        "to limit the memory size for ash buffer. Range: [0,1G] 0 means using default ash size"
        ", 30MB in normal case, 10M in mini mode",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_ob_ash_enable, OB_CLUSTER_PARAMETER, "True",
         "enable active session history",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_ob_ash_disk_write_enable, OB_CLUSTER_PARAMETER, "True",
         "enable active session history early flush",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_ob_sqlstat_enable, OB_TENANT_PARAMETER, "True", "enable/disable sql stat",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_inner_session_mgr, OB_TENANT_PARAMETER, "True", "enable/disable inner session mgr",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_trace_tablet_leak, OB_TENANT_PARAMETER, "False",
        "enable t3m tablet leak checker. The default value is False",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_BOOL(enable_auto_split, OB_TENANT_PARAMETER, "False",
         "if the auto-partition clause is not used"
         "this config judge whether to enable auto-partition for creating table.",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(auto_split_tablet_size, OB_TENANT_PARAMETER, "128M", "[128M,)",
        "when create an auto-partitioned table in \"create table\" syntax or "
        "modify a table as an auto-partitioned table in \"alter table\" syntax,"
        "if the splitting threshold of tablet size is not setted,"
        "this config will be setted as the threshold of the table."
        "Note that the modification of this config will not affect the created auto-partitioned table."
        "Range: [128M, +∞)",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_inlist_rewrite_threshold, OB_TENANT_PARAMETER, "1000", "[1, 2147483647]"
        "specifies transform how much const params in IN list to values table",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// for set errsim module types, format like transfer;migration
ERRSIM_DEF_STR_LIST(errsim_module_types, OB_TENANT_PARAMETER, "",
             "set module list for errsim error",
             ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_DBL(errsim_module_error_percentage, OB_TENANT_PARAMETER, "0", "[0,100]",
        "the percentage of the error happened to errsim module. "
        "Range: [0, 100] in percentage",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE))

ERRSIM_DEF_BOOL(block_transfer_out_replay, OB_TENANT_PARAMETER, "False",
         "errsim to block transfer out clog replay",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_BOOL(allow_transfer_backfill, OB_TENANT_PARAMETER, "True",
          "errsim to allow transfer backfill",
          ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// ttl
DEF_STR_WITH_CHECKER(kv_ttl_duty_duration, OB_TENANT_PARAMETER, "", common::ObTTLDutyDurationChecker,
    "ttl background task working time duration"
    "begin_time or end_time in Range, e.g., [23:00:00, 24:00:00]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(kv_ttl_history_recycle_interval, OB_TENANT_PARAMETER, "7d", "[1d, 180d]",
    "the time to recycle ttl history. Range: [1d, 180d]",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_kv_ttl, OB_TENANT_PARAMETER, "False",
    "specifies whether ttl task is enbled",
    ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(ttl_thread_score, OB_TENANT_PARAMETER, "0", "[0,100]",
        "the current work thread score of ttl thread. Range: [0,100] in integer. Especially, 0 means default value",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_persistent_compiled_routine, OB_CLUSTER_PARAMETER, "true",
         "specifies whether the feature of storeing dll to disk is turned on. "
         "The default value is TRUE. Value: TRUE: turned on FALSE: turned off",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(sql_protocol_min_tls_version, OB_CLUSTER_PARAMETER, "none",
                     common::ObConfigSQLTlsVersionChecker,
                     "SQL SSL control options, used to specify the minimum SSL/TLS version number. "
                     "values: none, TLSv1, TLSv1.1, TLSv1.2, TLSv1.3",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(shared_log_retention, OB_TENANT_PARAMETER, "1d", "[0s,7d]",
        "Retention time of log files on shared storage",
        ObParameterAttr(Section::TRANS, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// obkv
DEF_MODE_WITH_PARSER(_obkv_feature_mode, OB_CLUSTER_PARAMETER, "", common::ObKvFeatureModeParser,
    "_obkv_feature_mode is a option list to control specified OBKV features on/off.",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_standby_max_replay_gap_time, OB_TENANT_PARAMETER, "900s", "[10s,)",
        "The difference in replayable_scn between log streams on standby tenants is not greater than "
        "_standby_max_replay_gap_time, and the gap between sync_scn and replayable_scn of each log stream "
        "is kept reasonably small. Range: [10s, )",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(kv_hbase_client_scanner_timeout_period, OB_TENANT_PARAMETER, "60000", "(0,)",
    "OBKV Hbase client scanner query timeout, which unit is milliseconds. Range: (0, +∞) in integer. Especially, 60000 means default value",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));


DEF_BOOL(_enable_optimizer_qualify_filter, OB_TENANT_PARAMETER, "True",
        "Enable extracting qualify filters for window function",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_range_extraction_for_not_in, OB_TENANT_PARAMETER, "True",
        "Enable extract query range for not in predicate",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_new_query_range_extraction, OB_TENANT_PARAMETER, "True",
    "decide whether use new algorithm to extract query range.",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_ha_diagnose_history_recycle_interval, OB_CLUSTER_PARAMETER, "7d", "[2m, 180d]",
         "The recycle interval time of diagnostic history data. Range: [2m, 180d]",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(errsim_transfer_diagnose_server_wait_time, OB_CLUSTER_PARAMETER, "5m", "[1s,1h]",
        "the wakeup wait time of the transfer diagnose server. "
        "Range: [0s, 1h]",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_STR(palf_inject_receive_log_error_zone, OB_CLUSTER_PARAMETER, "", "specifies the zone name that palf module want to inject error when receive log",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_STR(migrate_check_member_list_error_zone, OB_CLUSTER_PARAMETER, "", "specifies the zone name that migrate want to inject error when change member list",
    ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// index usage
DEF_BOOL(_iut_enable, OB_TENANT_PARAMETER, "True",
        "specifies whether allow the index table usage start monitoring.",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_iut_max_entries, OB_TENANT_PARAMETER, "30000", "[0,]",
        "maximum of index entries to be monitoring.",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE))

DEF_STR_WITH_CHECKER(_iut_stat_collection_type, OB_TENANT_PARAMETER, "SAMPLED",
    common::ObConfigIndexStatsModeChecker,
    "specify index table usage stat collection type, values: SAMPLED, ALL",
    ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(optimizer_index_cost_adj, OB_TENANT_PARAMETER, "0", "[0,100]",
        "adjust costing of index scan",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_rpc_authentication_bypass, OB_CLUSTER_PARAMETER, "True",
        "specifies whether allow OMS service to connect "
        "cluster and provide service when rpc authentication is turned on.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_checkpoint_diagnose_preservation_count, OB_TENANT_PARAMETER, "100", "[0,800]",
        "the count of checkpoint diagnose info preservation",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_preserve_order_for_pagination, OB_TENANT_PARAMETER, "False",
        "enable preserver order for limit",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(max_partition_num, OB_TENANT_PARAMETER, "8192", "[8192, 65536]",
        "set max partition num in mysql mode",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(json_document_max_depth, OB_TENANT_PARAMETER, "100", "[100,1024]",
        "maximum nesting depth allowed in a JSON document",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_multimodel_memory_trace_level, OB_TENANT_PARAMETER, "0", "[0,100)",
        "Multi-mode memory tracking mechanism",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_INT(errsim_backup_task_batch_size, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "the batch size backup task receive in errsim mode"
        "Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// automatically faststack
DEF_INT(_faststack_req_queue_size_threshold, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "When the size of the req_queue reaches this threshold, the obstack will be "
        "collected automatically. Default: 0, means set off. Range: [0, +∞)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_TIME(_faststack_min_interval, OB_CLUSTER_PARAMETER, "30m", "[1s,)",
        "Minimum interval for OBServer to automatically collect the obstack. "
        "Default: 30min. Range: [1s,+∞)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(choose_migration_source_policy, OB_TENANT_PARAMETER, "region",
        common::ObConfigMigrationChooseSourceChecker,
        "the policy of choose source in migration and add replica. 'idc' means firstly choose follower replica of the same idc as source, "
        "'region' means firstly choose follower replica of the same region as source",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_query_record_size_limit, OB_TENANT_PARAMETER, "65536", "[0, 67108864] in integer",
        "set sql_audit and plan stat query sql size. Range: [0,67108864] in integer in integer.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_choose_migration_source_policy, OB_TENANT_PARAMETER, "True",
        "Control whether to use chose_migration_source_policy. "
        "If the value of configure is false, it will not use chose_migration_source_policy and choose replica with the largest checkpoint scn as the source.",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_global_enable_rich_vector_format, OB_CLUSTER_PARAMETER, "True",
         "Control whether use rich vector format in vectorization engine",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_allow_skip_replay_redo_after_detete_tablet, OB_TENANT_PARAMETER, "FALSE",
         "allow skip replay invalid redo log after tablet delete transaction is committed."
         "The default value is FALSE. Value: TRUE means we allow skip replaying this invalid redo log, False means we do not alow such behavior.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

//check os params
DEF_BOOL(strict_check_os_params, OB_CLUSTER_PARAMETER, "False",
         "A switch that determines whether to enable strict OS parameter check mode, defaulting to true and can be set to false to bypass strict checks."
         "Value: True: allowed; False: allowed but not suggested",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_BOOL(_enable_tree_based_io_scheduler, OB_CLUSTER_PARAMETER, "True",
         "A switch that allows enabling the tree-based IO scheduler."
         "Value: True: allowed; False: disabled",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(enable_ob_error_msg_style, OB_CLUSTER_PARAMETER, "True",
         "A switch that determines whether to use the ORA-xx or OBE-xx error code format for ORA error codes, with a default value of True to use the OBE-xx format."
         "The default value is True. Value: False means we use the ORA-xx format, True means we use the OBE-xx format.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::STATIC_EFFECTIVE));
DEF_BOOL(_enable_memleak_light_backtrace, OB_CLUSTER_PARAMETER, "True",
        "specifies whether allow memleak to get the backtrace of malloc by light_backtrace",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_dbms_lob_partial_update, OB_TENANT_PARAMETER, "False",
         "Enable the capability of dbms_lob to perform partial updates on LOB",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_dbms_job_package, OB_CLUSTER_PARAMETER, "True",
         "Control whether can use DBMS_JOB package.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// for shared storage mode
DEF_TIME(_ss_new_leader_overwrite_delay, OB_TENANT_PARAMETER, "60s", "[1s,)",
         "the delay time for new leader to write ss obj, Range: [1s, )"
         "Range: [1s, )",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_ss_deleted_tablet_gc_time, OB_TENANT_PARAMETER, "5d", "[0,365d]",
         "the GC time for deleted tablet in shared dir,"
         "Range: [0,365d]",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_TIME(_ss_old_ver_retention_time, OB_TENANT_PARAMETER, "5d", "[0,365d]",
         "the retention time of old compaction version data in shared dir,"
         "Range: [0,365d]",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_ss_migration_prewarm, OB_TENANT_PARAMETER, "True",
         "Control whether open migration prewarm",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// obkv feature switch
DEF_BOOL(_enable_kv_feature, OB_CLUSTER_PARAMETER, "True",
         "Enable or disable OBKV feature.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_CAP(lob_enable_block_cache_threshold, OB_TENANT_PARAMETER, "256K", "[0B, 512M]",
        "For outrow-stored LOBs, if the length is less than or equal to that threshold, "
        "they can be admitted into the block cache to speed up the next query.",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(_ob_pl_compile_max_concurrency, OB_CLUSTER_PARAMETER, "4", "[0,)",
        "The maximum number of threads that an observer node can compile PL concurrently.",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_compatible_monotonic, OB_CLUSTER_PARAMETER, "True",
        "Control whether to enable cross-server monotonic compatible(data_version) mode",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(sql_plan_management_mode, OB_TENANT_PARAMETER, "Disable",
                     common::ObSqlPlanManagementModeChecker,
                     "Specifies how spm work."
                     "\"Disable\" represent disable spm (default value)."
                     "\"OnlineEvolve\" represent evolve plan online.",
                     ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(default_load_mode, OB_TENANT_PARAMETER, "DISABLED",
                     common::ObDefaultLoadModeChecker,
                     "Specifies default load data path."
                     "\"DISABLED\" represent load data not in direct load path (default value)."
                     "\"FULL_DIRECT_WRITE\" represent load data in full direct load path with insert semantics."
                     "\"INC_DIRECT_WRITE\" represent load data in inc direct load path with insert semantics."
                     "\"INC_REPLACE_DIRECT_WRITE\" represent load data in inc direct load path with replace semantics.",
                     ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(direct_load_allow_fallback, OB_TENANT_PARAMETER, "False",
        "Control whether an error is reported when direct load of the derivative operation scenario is not supported.",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
ERRSIM_DEF_TIME(errsim_delay_gc_interval, OB_CLUSTER_PARAMETER, "30m", "[0,)",
        "delay gc interval in errsim mode"
         "Range: [0, +∞)",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
// regexp engine
DEF_STR_WITH_CHECKER(_regex_engine, OB_TENANT_PARAMETER, "ICU",
                     common::ObConfigRegexpEngineChecker,
                     "specifies the regexp engine. Values: ICU(International Components for Unicode), Hyperscan",
                     ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_preset_runtime_bloom_filter_size, OB_CLUSTER_PARAMETER, "False",
         "Whether build runtime bloom filter with row count estimated by optimizor."
         "Value:  True:turned on  False: turned off",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_check_trigger_const_variables_assign, OB_TENANT_PARAMETER, "True",
        "Used to control whether an error is reported when assigning a value to a const variable in a trigger under an Oracle tenant",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_CAP(errsim_max_key_set_size, OB_CLUSTER_PARAMETER, "2M", "[0M,)",
        "max key set size, it used to test migrating warmup rpc reconnect"
        "Range: [0M,)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_unit_gc_wait, OB_CLUSTER_PARAMETER, "True",
         "Used to control enable or disable the unit smooth gc feature, enabled by default.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

// obkv group commit
DEF_INT(kv_group_commit_batch_size, OB_TENANT_PARAMETER, "1", "(0,)",
        "Used to specify the batch size of each group commit batch in OBKV."
        " Values: 1 means sinlge operaion in each batch, equally to disable group commit."
        " When batch size is greater than 1, it means group commit is enable and used as its batch size. ",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_STR_WITH_CHECKER(kv_group_commit_rw_mode, OB_TENANT_PARAMETER, "ALL",
        common::ObConfigKvGroupCommitRWModeChecker,
        "Used to specify the read/write operation types when group commit is enable. "
        "Values: 'ALL' means enable all operations, 'READ' mean only enable read operation in group commit, 'WRITE'  means only write operations in group commit.",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(ob_vector_memory_limit_percentage, OB_TENANT_PARAMETER, "0",
        "[0,100)",
        "Used to control the upper limit percentage of memory resources that the vector_index module can use. Range:[0, 100)",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_STR_WITH_CHECKER(ob_storage_s3_url_encode_type, OB_CLUSTER_PARAMETER, "default",
                     common::ObConfigS3URLEncodeTypeChecker,
                     "Determines the URL encoding method for S3 requests."
                     "\"default\": Uses the S3 standard URL encoding method."
                     "\"compliantRfc3986Encoding\": Uses URL encoding that adheres to the RFC 3986 standard.",
                     ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_BOOL(_enable_drop_and_add_index, OB_TENANT_PARAMETER, "False",
         "it specifies that whether we can drop and add index in single statement",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_dop_of_collect_external_table_statistics, OB_TENANT_PARAMETER, "0", "[0,)",
        "parallelism of pull statistics of external table",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(_max_partition_count_to_collect_statistic, OB_TENANT_PARAMETER, "5", "[0,)",
        "force odps external table to using block granule iterator when count of partition is under this config",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_INT(ob_encoding_granularity, OB_TENANT_PARAMETER, "65536", "[8192, 1048576]",
        "Maximum rows for encoding in one micro block. Range:[8192,1048576]",
        ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_partition_wise_plan_enabled, OB_TENANT_PARAMETER, "True",
         "enable/disable optimizer partition wise plan",
         ObParameterAttr(Section::TENANT, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_INT(errsim_rebuild_ls_id, OB_CLUSTER_PARAMETER, "0", "[0,)",
        "rebuild ls id. Range: [0,) in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_STR(errsim_rebuild_addr, OB_CLUSTER_PARAMETER, "",
        "rebuild addr (ip:port)",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

ERRSIM_DEF_BOOL(enable_parallel_migration, OB_CLUSTER_PARAMETER, "False",
         "turn on parallel migration, observer preferentially choose same zone as src",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));

DEF_BOOL(_enable_adaptive_auto_dop, OB_CLUSTER_PARAMETER, "False",
         "Enable or disable adaptive auto dop feature.",
         ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(query_memory_limit_percentage, OB_TENANT_PARAMETER, "50", "[0,100]",
        "the percentage of tenant memory that can be used by a single SQL. The default value is 50. Range: [0,100]",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
DEF_INT(package_state_sync_max_size, OB_TENANT_PARAMETER, "8192", "[0, 16777216]",
        "the max sync size of single package state that can sync package var value. If over it, package state will not sync package var value. Range: [0, 16777216] in integer",
        ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
