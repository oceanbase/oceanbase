/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * LogFetcher Config module
 */

#ifndef OCEANBASE_LOG_FETCHER_CONFIG_H__
#define OCEANBASE_LOG_FETCHER_CONFIG_H__

#include <map>
#include "share/ob_define.h"
#include "share/parameter/ob_parameter_macro.h"
#include "share/config/ob_common_config.h"    // ObInitConfigContainer

////////////// Define member variables of type INT, no limit on maximum value //////////////
// DEF: default value
// MIN: minimum value
//
// Note: DEF, MIN must be literal values, not variable names
#define T_DEF_INT_INFT(name, SCOPE, DEF, MIN, NOTE) \
    public: \
      static const int64_t default_##name = (DEF); \
      DEF_INT(name, SCOPE, #DEF, "[" #MIN ",]", NOTE);

////////////// Define INT type member variable //////////////
// DEF: default value
// MIN: minimum value
// MAX: maximum value
//
// Note: DEF, MIN, MAX must be literal values, not variable names
#define T_DEF_INT(name, SCOPE, DEF, MIN, MAX, NOTE) \
    public: \
      static const int64_t default_##name = (DEF); \
      static const int64_t max_##name = (MAX); \
      DEF_INT(name, SCOPE, #DEF, "[" #MIN "," #MAX "]", NOTE);

////////////// Define INT type member variable //////////////
// DEF: default value, 0 or 1
//
// Note: DEF must be a literal value, not a variable name
#define T_DEF_BOOL(name, SCOPE, DEF, NOTE) \
    public: \
      static const int64_t default_##name = DEF; \
      DEF_INT(name, SCOPE, #DEF, "[0,1]", NOTE);

namespace oceanbase
{
namespace logfetcher
{
class ObLogFetcherConfig : public common::ObBaseConfig
{
  typedef std::map<std::string, std::string> ConfigMap;

public:
  ObLogFetcherConfig() : is_inited_(false)
  {
  }

  virtual ~ObLogFetcherConfig() { destroy(); }

  int init();
  void destroy();

public:
  int load_from_map(const ConfigMap &configs,
      const int64_t version = 0,
      const bool check_name = false);

public:

#ifdef OB_CLUSTER_PARAMETER
#undef OB_CLUSTER_PARAMETER
#endif
#define OB_CLUSTER_PARAMETER(args...) args
  // Log_level=INFO in the startup scenario, and then optimize the schema to WARN afterwards
  DEF_STR(init_log_level, OB_CLUSTER_PARAMETER, "ALL.*:INFO;PALF.*:WARN;SHARE.SCHEMA:INFO", "log level: DEBUG, TRACE, INFO, WARN, USER_ERR, ERROR");
  DEF_STR(log_level, OB_CLUSTER_PARAMETER, "ALL.*:INFO;PALF.*:WARN;SHARE.SCHEMA:WARN", "log level: DEBUG, TRACE, INFO, WARN, USER_ERR, ERROR");
  ////////////////////////////// Fetcher config //////////////////////////////
  //
  // ------------------------------------------------------------------------
  //          Configurations that do not support dynamic changes
  // ------------------------------------------------------------------------
  // fetching log mode of libobcdc
  // 1. integrated: integrated fetch mode, fetch log from observer, don't perceive archivelog
  // 2. direct: direct fetch mode, direct fetch log from archive, need to perceive the destination of archivelog.
  DEF_STR(fetching_log_mode, OB_CLUSTER_PARAMETER, "integrated", "libobcdc fetching mode");
  // the destination of archive log.
  DEF_STR(archive_dest, OB_CLUSTER_PARAMETER, "|", "the location of archive log");

  T_DEF_INT_INFT(io_thread_num, OB_CLUSTER_PARAMETER, 4, 1, "io thread number");
  T_DEF_INT(idle_pool_thread_num, OB_CLUSTER_PARAMETER, 1, 1, 32, "idle pool thread num");
  T_DEF_INT(dead_pool_thread_num, OB_CLUSTER_PARAMETER, 1, 1, 32, "dead pool thread num");
  T_DEF_INT(cdc_read_archive_log_concurrency, OB_CLUSTER_PARAMETER, 4, 1, 64, "log external storage handler thread num");
  T_DEF_INT(stream_worker_thread_num, OB_CLUSTER_PARAMETER, 4, 1, 64, "stream worker thread num");
  T_DEF_INT(start_lsn_locator_thread_num, OB_CLUSTER_PARAMETER, 1, 1, 32, "start lsn locator thread num");
  T_DEF_INT_INFT(start_lsn_locator_locate_count, OB_CLUSTER_PARAMETER, 1, 1, "start lsn locator locate count");
  // Whether to skip the starting lsn positioning result consistency check, i.e. whether there is a positioning log bias scenario
  T_DEF_BOOL(skip_start_lsn_locator_result_consistent_check, OB_CLUSTER_PARAMETER, 0, "0:disabled, 1:enabled");
  T_DEF_INT_INFT(svr_stream_cached_count, OB_CLUSTER_PARAMETER, 16, 1, "cached svr stream object count");
  T_DEF_INT_INFT(fetch_stream_cached_count, OB_CLUSTER_PARAMETER, 16, 1, "cached fetch stream object count");

  // region
  DEF_STR(region, OB_CLUSTER_PARAMETER, "default_region", "OB region");

  // Number of globally cached RPC results
  T_DEF_INT_INFT(rpc_result_cached_count, OB_CLUSTER_PARAMETER, 16, 1, "cached rpc result object count");

  // Number of active ls count in memory
  // This value can be used as a reference for the number of data structure objects cached at the ls level
  T_DEF_INT_INFT(active_ls_count, OB_CLUSTER_PARAMETER, 100, 1, "active ls count in memory");

  // Maximum number of ls currently supported
  T_DEF_INT_INFT(ls_count_upper_limit, OB_CLUSTER_PARAMETER, 10000, 1, "max ls count supported");

  // ------------------------------------------------------------------------
  //              configurations which supports dynamically modify
  // ------------------------------------------------------------------------
  T_DEF_INT_INFT(start_lsn_locator_rpc_timeout_sec, OB_CLUSTER_PARAMETER, 60, 1,
      "start lsn locator rpc timeout in seconds");
  T_DEF_INT_INFT(start_lsn_locator_batch_count, OB_CLUSTER_PARAMETER, 5, 1, "start lsn locator batch count");

  // server blacklist, default is|,means no configuration, support configuration single/multiple servers
  // Single: SEVER_IP1:PORT1
  // Multiple: SEVER_IP1:PORT1|SEVER_IP2:PORT2|SEVER_IP3:PORT3
  DEF_STR(server_blacklist, OB_CLUSTER_PARAMETER, "|", "server black list");
  DEF_STR(sql_server_blacklist, OB_CLUSTER_PARAMETER, "|", "sql server black list");

  T_DEF_INT_INFT(fetch_log_rpc_timeout_sec, OB_CLUSTER_PARAMETER, 15, 1, "fetch log rpc timeout in seconds");

  // Upper limit of progress difference between partitions, in seconds
  T_DEF_INT_INFT(progress_limit_sec_for_dml, OB_CLUSTER_PARAMETER, 300, 1, "dml progress limit in seconds");

  // When all servers are added to the blacklist because of exceptions, the LS FetchCtx is dispatched into IDEL Pool mode.
  // If the RS servers continues to be disconnected, we cannot refresh new server list for FetchCtx by SQL. So The LS FetchCtx cannot fetch log.
  // If set enable_continue_use_cache_server_list is true, we can continue use cache server to fetch log.
  // A means of fault tolerance for LDG
  T_DEF_BOOL(enable_continue_use_cache_server_list, OB_CLUSTER_PARAMETER, 0, "0:disabled, 1:enabled");

  T_DEF_INT_INFT(progress_limit_sec_for_ddl, OB_CLUSTER_PARAMETER, 3600, 1, "ddl progress limit in seconds");

  // LS fetch progress update timeout in seconds
  // If the logs are not fetched after a certain period of time, the stream will be cut
  T_DEF_INT_INFT(ls_fetch_progress_update_timeout_sec, OB_CLUSTER_PARAMETER, 15, 1, "logstream fetch progress update timeout in seconds");

  T_DEF_INT_INFT(log_router_background_refresh_interval_sec, OB_CLUSTER_PARAMETER, 10, 1,
                 "log_route_service background_refresh_time in seconds");
	// cache update interval of sys table __all_server
  T_DEF_INT_INFT(all_server_cache_update_interval_sec, OB_CLUSTER_PARAMETER, 5, 1,
			           "__all_server table cache update internal in seconds");

	// cache update interval of sys table __all_zone
  T_DEF_INT_INFT(all_zone_cache_update_interval_sec, OB_CLUSTER_PARAMETER, 5, 1,
			           "__all_zone table cache update internal in seconds");

  // pause fetcher
  T_DEF_BOOL(pause_fetcher, OB_CLUSTER_PARAMETER, 0, "0:disabled, 1:enabled");

  // Maximum number of tasks supported by the timer
  T_DEF_INT_INFT(timer_task_count_upper_limit, OB_CLUSTER_PARAMETER, 1024, 1, "max timer task count");
  // Timer task timing time
  T_DEF_INT_INFT(timer_task_wait_time_msec, OB_CLUSTER_PARAMETER, 100, 1, "timer task wait time in milliseconds");

  // the upper limit observer takes  for the log rpc processing time
  // Print RPC chain statistics logs if this limit is exceeded
  T_DEF_INT_INFT(rpc_process_handler_time_upper_limit_msec, OB_CLUSTER_PARAMETER, 200, 1,
      "observer fetch log rpc process handler timer upper limit");

  // Survival time of server to blacklist, in seconds
  T_DEF_INT_INFT(blacklist_survival_time_sec, OB_CLUSTER_PARAMETER, 30, 1, "blacklist-server surival time in seconds");

  // The maximum time the server can be blacklisted, in minutes
  T_DEF_INT_INFT(blacklist_survival_time_upper_limit_min, OB_CLUSTER_PARAMETER, 1, 1, "blacklist-server survival time upper limit in minute");

  // The server is blacklisted in the logstream, based on the time of the current server service logstream - to decide whether to penalize the survival time
  // When the service time is less than a certain interval, a doubling-live-time policy is adopted
  // Unit: minutes
  T_DEF_INT_INFT(blacklist_survival_time_penalty_period_min, OB_CLUSTER_PARAMETER, 1, 1, "blacklist survival time punish interval in minute");

  // Blacklist history expiration time, used to delete history
  T_DEF_INT_INFT(blacklist_history_overdue_time_min, OB_CLUSTER_PARAMETER, 30, 10, "blacklist history overdue in minute");

  // Clear blacklist history period, unit: minutes
  T_DEF_INT_INFT(blacklist_history_clear_interval_min, OB_CLUSTER_PARAMETER, 20, 10, "blacklist history clear interval in minute");

  // Check the need for active cut-off cycles, in minutes
  T_DEF_INT_INFT(check_switch_server_interval_sec, OB_CLUSTER_PARAMETER, 60, 1, "check switch server interval in seconds");

  // Print the number of LSs with the slowest progress of the Fetcher module
  T_DEF_INT_INFT(print_fetcher_slowest_ls_num, OB_CLUSTER_PARAMETER, 10, 1, "print fetcher slowest ls num");

  // Maximum number of RPC results per RPC
  T_DEF_INT_INFT(rpc_result_count_per_rpc_upper_limit, OB_CLUSTER_PARAMETER, 16, 1,
      "max rpc result count per rpc");

  // Whether to print RPC processing information
  // Print every RPC processing
  // No printing by default
  T_DEF_BOOL(print_rpc_handle_info, OB_CLUSTER_PARAMETER, 0, "0:disabled, 1:enabled");
  T_DEF_BOOL(print_stream_dispatch_info, OB_CLUSTER_PARAMETER, 0, "0:disabled, 1:enabled");

  // ------------------------------------------------------------------------
  // Print logstream heartbeat information
  T_DEF_BOOL(print_ls_heartbeat_info, OB_CLUSTER_PARAMETER, 0, "0:disabled, 1:enabled");
  // Print logstream service information
  T_DEF_BOOL(print_ls_serve_info, OB_CLUSTER_PARAMETER, 0, "0:disabled, 1:enabled");
  // Print the svr list of each logstream update, off by default
  T_DEF_BOOL(print_ls_server_list_update_info, OB_CLUSTER_PARAMETER, 0, "0:disabled, 1:enabled");
  // Switch: Whether to enable SSL authentication: including MySQL and RPC
  // Disabled by default
  T_DEF_BOOL(ssl_client_authentication, OB_CLUSTER_PARAMETER, 0, "0:disabled, 1:enabled");

  // SSL external kms info
  // 1. Local file mode: ssl_external_kms_info=file
  // 2. BKMI mode: ssl_external_kms_info=hex(...)
  DEF_STR(ssl_external_kms_info, OB_CLUSTER_PARAMETER, "|", "ssl external kms info");


  ////////////////////////////// Test mode //////////////////////////////
  // Test mode, used only in obtest and other test tool scenarios
  T_DEF_BOOL(test_mode_on, OB_CLUSTER_PARAMETER, 0, "0:disabled, 1:enabled");

  // if force fetch archive is on, cdc service will seek archive for all rpc request unconditionally
  T_DEF_BOOL(test_mode_force_fetch_archive, OB_CLUSTER_PARAMETER, 0, "0:disabled, 1:enabled");

  // enable test_mode_switch_fetch_mode to test whether cdc service can fetch log correctly when switching fetch mode
  T_DEF_BOOL(test_mode_switch_fetch_mode, OB_CLUSTER_PARAMETER, 0, "0:disabled 1:enabled");

  // The number of times sqlServer cannot get the rs list in test mode
  T_DEF_INT_INFT(test_mode_block_sqlserver_count, OB_CLUSTER_PARAMETER, 0, 0,
      "mock times of con't get rs list under test mode");

  ///////////////////////////////////////////////////////////////////////

#undef OB_CLUSTER_PARAMETER

private:
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFetcherConfig);
};

} // namespace logfetcher
} // namespace oceanbase
#endif /* OCEANBASE_LOG_FETCHER_CONFIG_H__ */
