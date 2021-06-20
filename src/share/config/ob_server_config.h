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

#ifndef OCEANBASE_SHARE_CONFIG_OB_SERVER_CONFIG_H_
#define OCEANBASE_SHARE_CONFIG_OB_SERVER_CONFIG_H_

#include "share/config/ob_common_config.h"
#include "share/config/ob_system_config.h"

namespace oceanbase {
namespace common {
class ObISQLClient;
const char* const ZONE_MERGE_ORDER = "zone_merge_order";
const char* const ZONE_MERGE_CURRENCNCY = "zone_merge_concurrency";
const char* const UNIT_BALANCE_RESOURCE_WEIGHT = "unit_balance_resource_weight";
const char* const MIN_OBSERVER_VERSION = "min_observer_version";
const char* const __BALANCE_CONTROLLER = "__balance_controller";
const char* const __MIN_FULL_RESOURCE_POOL_MEMORY = "__min_full_resource_pool_memory";
const char* const __SCHEMA_SPLIT_MODE = "__schema_split_mode";
const char* const TENANT_GROUPS = "tenant_groups";
const char* const SERVER_BALANCE_CRITICAL_DISK_WATERLEVEL = "server_balance_critical_disk_waterlevel";
const char* const ENABLE_REBALANCE = "enable_rebalance";
const char* const PARALLEL_MAX_SERVERS = "parallel_max_servers";
const char* const ENABLE_REREPLICATION = "enable_rereplication";
const char* const MERGER_CHECK_INTERVAL = "merger_check_interval";
const char* const ENABLE_MAJOR_FREEZE = "enable_major_freeze";
const char* const ENABLE_DDL = "enable_ddl";
const char* const ENABLE_AUTO_LEADER_SWITCH = "enable_auto_leader_switch";
const char* const MINOR_FREEZE_TIMES = "minor_freeze_times";
const char* const MAJOR_COMPACT_TRIGGER = "major_compact_trigger";
const char* const ENABLE_PERF_EVENT = "enable_perf_event";
const char* const ENABLE_SQL_AUDIT = "enable_sql_audit";
const char* const CONFIG_TRUE_VALUE = "1";
const char* const CONFIG_FALSE_VALUE = "0";
const char* const OBCONFIG_URL = "obconfig_url";
const char* const _SCHEMA_HISTORY_RECYCLE_INTERVAL = "_schema_history_recycle_interval";
const char* const _RECYCLEBIN_OBJECT_PURGE_FREQUENCY = "_recyclebin_object_purge_frequency";
const char* const TDE_MODE = "tde_mode";
const char* const EXTERNAL_KMS_INFO = "external_kms_info";
const char* const SSL_EXTERNAL_KMS_INFO = "ssl_external_kms_info";
const char* const ENABLE_ONE_PHASE_COMMIT = "enable_one_phase_commit";
const char* const CLOG_DISK_USAGE_LIMIT_PERCENTAGE = "clog_disk_usage_limit_percentage";
const char* const CLOG_DISK_UTILIZATION_THRESHOLD = "clog_disk_utilization_threshold";

class ObServerConfig : public ObCommonConfig {
public:
  int init(const ObSystemConfig& config);
  static ObServerConfig& get_instance();

  // read all config from system_config_
  virtual int read_config();

  // check if all config is validated
  virtual int check_all() const;
  // check some special settings strictly
  int strict_check_special() const;
  // print all config to log file
  void print() const;

  // server memory limit
  int64_t get_server_memory_limit();
  // server memory available for normal tenants
  int64_t get_server_memory_avail();
  // server momory reserved for internal usage.
  int64_t get_reserved_server_memory();

  // get DEFAULT_MIN_SYS_MEMORY/DEFAULT_MAX_SYS_MEMORY by these interface
  int64_t get_min_sys_tenant_memory();
  int64_t get_max_sys_tenant_memory();
  // get or possible update sql_audit_memory_limit
  int64_t get_or_update_sql_audit_memory_limit();
  int64_t get_log_archive_concurrency() const;
  int64_t get_log_restore_concurrency() const;

  int64_t get_global_freeze_trigger_percentage()
  {
    return 100 - global_major_freeze_residual_memory;
  }
  int64_t get_global_memstore_limit_percentage()
  {
    return 100 - global_write_halt_residual_memory;
  }
  virtual ObServerRole get_server_type() const
  {
    return common::OB_SERVER;
  }
  virtual bool is_debug_sync_enabled() const
  {
    return static_cast<int64_t>(debug_sync_timeout) > 0;
  }
  virtual bool is_manual_merge_enabled()
  {
    return in_major_version_upgrade_mode() || enable_manual_merge;
  }
  virtual bool is_rebalance_enabled()
  {
    return !in_major_version_upgrade_mode() && enable_rebalance;
  }
  virtual bool is_rereplication_enabled()
  {
    return !in_major_version_upgrade_mode() && enable_rereplication;
  }

  virtual double user_location_cpu_quota() const
  {
    return location_cache_cpu_quota;
  }
  virtual double sys_location_cpu_quota() const
  {
    return std::max(1., user_location_cpu_quota() / 2);
  }
  virtual double root_location_cpu_quota() const
  {
    return 1.;
  }
  virtual double core_location_cpu_quota() const
  {
    return 1.;
  }

  bool is_sql_operator_dump_enabled() const
  {
    return enable_sql_operator_dump;
  }
  bool enable_static_engine_for_query() const;

  bool is_major_version_upgrade() const
  {
    return false;
  }
  bool in_major_version_upgrade_mode() const
  {
    return in_upgrade_mode() && is_major_version_upgrade();
  }
  bool enable_new_major() const
  {
    return true;
  }
  int check_and_refresh_major_compact_trigger();
  bool in_upgrade_mode() const;

  // Compatibility requirements, compatible with the old SPFILE format
  int deserialize_with_compat(const char* buf, const int64_t data_len, int64_t& pos);
  OB_UNIS_VERSION(1);

public:
  int64_t disk_actual_space_;
  ObAddr self_addr_;

public:
///////////////////////////////////////////////////////////////////////////////
// use MACRO 'OB_CLUSTER_PARAMETER' to define new cluster parameters
// in ob_parameter_seed.ipp:
///////////////////////////////////////////////////////////////////////////////
#undef OB_CLUSTER_PARAMETER
#define OB_CLUSTER_PARAMETER(args...) args
#include "share/parameter/ob_parameter_seed.ipp"
#undef OB_CLUSTER_PARAMETER

protected:
  ObServerConfig();
  virtual ~ObServerConfig();
  const ObSystemConfig* system_config_;
  static const int16_t OB_CONFIG_MAGIC = static_cast<int16_t>(0XBCDE);
  static const int16_t OB_CONFIG_VERSION = 1;

private:
  DISALLOW_COPY_AND_ASSIGN(ObServerConfig);
};
}  // namespace common
}  // namespace oceanbase

#define GCONF (::oceanbase::common::ObServerConfig::get_instance())

#endif  // OCEANBASE_SHARE_CONFIG_OB_SERVER_CONFIG_H_
