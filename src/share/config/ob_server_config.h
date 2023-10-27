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

namespace oceanbase
{
namespace unittest
{
  class ObSimpleClusterTestBase;
  class ObMultiReplicaTestBase;
}
namespace common
{
class ObISQLClient;
const char* const MIN_OBSERVER_VERSION = "min_observer_version";
const char* const __BALANCE_CONTROLLER = "__balance_controller";
const char* const __MIN_FULL_RESOURCE_POOL_MEMORY = "__min_full_resource_pool_memory";
const char* const SERVER_BALANCE_CRITICAL_DISK_WATERLEVEL = "server_balance_critical_disk_waterlevel";
const char* const ENABLE_REBALANCE = "enable_rebalance";
const char* const ENABLE_REREPLICATION = "enable_rereplication";
const char* const MERGER_CHECK_INTERVAL = "merger_check_interval";
const char* const ENABLE_MAJOR_FREEZE = "enable_major_freeze";
const char* const ENABLE_DDL = "enable_ddl";
const char* const ENABLE_AUTO_LEADER_SWITCH = "enable_auto_leader_switch";
const char* const MAJOR_COMPACT_TRIGGER = "major_compact_trigger";
const char* const ENABLE_PERF_EVENT = "enable_perf_event";
const char* const ENABLE_SQL_AUDIT = "enable_sql_audit";
const char *const OB_STR_TRC_CONTROL_INFO = "_trace_control_info";
const char* const CONFIG_TRUE_VALUE_BOOL = "1";
const char* const CONFIG_FALSE_VALUE_BOOL = "0";
const char* const CONFIG_TRUE_VALUE_STRING = "true";
const char* const CONFIG_FALSE_VALUE_STRING = "false";
const char* const OBCONFIG_URL = "obconfig_url";
const char* const SCHEMA_HISTORY_RECYCLE_INTERVAL = "schema_history_recycle_interval";
const char* const _RECYCLEBIN_OBJECT_PURGE_FREQUENCY = "_recyclebin_object_purge_frequency";
const char* const TDE_METHOD = "tde_method";
const char* const EXTERNAL_KMS_INFO = "external_kms_info";
const char* const SSL_EXTERNAL_KMS_INFO = "ssl_external_kms_info";
const char* const CLUSTER_ID = "cluster_id";
const char* const CLUSTER_NAME = "cluster";
const char* const FREEZE_TRIGGER_PERCENTAGE = "freeze_trigger_percentage";
const char* const WRITING_THROTTLEIUNG_TRIGGER_PERCENTAGE = "writing_throttling_trigger_percentage";
const char* const COMPATIBLE = "compatible";
const char* const WEAK_READ_VERSION_REFRESH_INTERVAL = "weak_read_version_refresh_interval";
const char* const PARTITION_BALANCE_SCHEDULE_INTERVAL = "partition_balance_schedule_interval";
const char* const BALANCER_IDLE_TIME = "balancer_idle_time";
const char* const LOG_DISK_UTILIZATION_LIMIT_THRESHOLD = "log_disk_utilization_limit_threshold";
const char* const LOG_DISK_THROTTLING_PERCENTAGE = "log_disk_throttling_percentage";

class ObServerMemoryConfig;

class ObServerConfig : public ObCommonConfig
{
public:
  friend class ObServerMemoryConfig;
  int init(const ObSystemConfig &config);
  static ObServerConfig &get_instance();

  // read all config from system_config_
  virtual int read_config();

  // check if all config is validated
  virtual int check_all() const;
  // check some special settings strictly
  int strict_check_special() const;
  // print all config to log file
  void print() const;

  double get_sys_tenant_default_min_cpu();
  double get_sys_tenant_default_max_cpu();

  virtual ObServerRole get_server_type() const { return common::OB_SERVER; }
  virtual bool is_debug_sync_enabled() const { return static_cast<int64_t>(debug_sync_timeout) > 0; }
  virtual bool is_rereplication_enabled() { return !in_major_version_upgrade_mode() && enable_rereplication; }

  virtual double user_location_cpu_quota() const { return location_cache_cpu_quota; }
  virtual double sys_location_cpu_quota() const { return std::max(1., user_location_cpu_quota() / 2); }
  virtual double root_location_cpu_quota() const { return 1.; }
  virtual double core_location_cpu_quota() const { return 1.; }

  bool is_sql_operator_dump_enabled() const { return enable_sql_operator_dump; }

  bool enable_defensive_check() const
  {
    int64_t v = _enable_defensive_check;
    return v > 0;
  }

  bool enable_strict_defensive_check() const
  {
    int64_t v = _enable_defensive_check;
    return v == 2;
  }

  // false for 1.4.2 -> 1.4.3
  // true for 1.3.4 -> 1.4.2
  // 大版本升级期间关闭冻结合并、迁移复制等系统功能
  // 小版本支持灰度升级，不关闭这些功能，且需要支持升级回滚
  bool is_major_version_upgrade() const { return false; }
  bool in_major_version_upgrade_mode() const { return in_upgrade_mode() && is_major_version_upgrade(); }
  bool enable_new_major() const {  return true; }
  bool in_upgrade_mode() const;
  bool in_dbupgrade_stage() const;
  bool is_valid() const { return  system_config_!= NULL; };
  int64_t get_current_version() { return system_config_->get_version(); }

  // 兼容性需求，兼容老的SPFILE格式
  int deserialize_with_compat(const char *buf, const int64_t data_len, int64_t &pos);
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
  const ObSystemConfig *system_config_;
  static const int16_t OB_CONFIG_MAGIC = static_cast<int16_t>(0XBCDE);
  static const int16_t OB_CONFIG_VERSION = 1;

private:
  DISALLOW_COPY_AND_ASSIGN(ObServerConfig);
};

class ObServerMemoryConfig
{
public:
  enum DependentMemConfig {
    MEMORY_LIMIT,
    SYSTEM_MEMORY,
  };
  enum AdaptiveMemConfig {
    ADAPTIVE_SYSTEM_MEMORY,
    ADAPTIVE_HIDDEN_SYS_MEMORY,
  };
  friend class unittest::ObSimpleClusterTestBase;
  friend class unittest::ObMultiReplicaTestBase;
  ObServerMemoryConfig();
  static ObServerMemoryConfig &get_instance();
  int reload_config(const ObServerConfig& server_config);
  int64_t get_server_memory_limit() { return memory_limit_; }
  int64_t get_reserved_server_memory() { return system_memory_; }
  int64_t get_server_memory_avail() { return memory_limit_ - system_memory_; }
  int64_t get_hidden_sys_memory() { return hidden_sys_memory_; }
  //the extra_memory just used by real sys when non_mini_mode
  int64_t get_extra_memory();
  void check_500_tenant_hold(bool ignore_error);

#ifdef ENABLE_500_MEMORY_LIMIT
  int set_500_tenant_limit(const int64_t limit_mode);
#endif
private:
  int64_t get_adaptive_memory_config(const int64_t memory_size,
                                     DependentMemConfig dep_mem_config,
                                     AdaptiveMemConfig adap_mem_config);
private:
  int64_t memory_limit_;
  int64_t system_memory_;
  int64_t hidden_sys_memory_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObServerMemoryConfig);
};
}
}

#define GCONF (::oceanbase::common::ObServerConfig::get_instance())
#define GMEMCONF (::oceanbase::common::ObServerMemoryConfig::get_instance())
#endif // OCEANBASE_SHARE_CONFIG_OB_SERVER_CONFIG_H_
