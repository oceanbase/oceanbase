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

#ifndef OCEANBASE_SHARE_OB_ALL_CLUSTER_PROXY_H_
#define OCEANBASE_SHARE_OB_ALL_CLUSTER_PROXY_H_
#include "share/ob_zone_info.h"
#include "share/ob_web_service_root_addr.h"
#include "lib/string/ob_fixed_length_string.h"
#include "share/ob_cluster_type.h"  // ObClusterType
namespace oceanbase {
namespace share {
#define SWITCH_TIMSTAMP_MASK 0x3FF
class ObClusterInfo {
  OB_UNIS_VERSION(1);

public:
  static const int64_t MAX_PASSWD_LENGTH = common::OB_MAX_PASSWORD_LENGTH;
  typedef common::ObFixedLengthString<common::OB_MAX_USERNAME_LENGTH> UserNameString;
  typedef common::ObFixedLengthString<MAX_PASSWD_LENGTH> PassWdString;
  enum PersistentSwitchOverStatus  // Persistent switchover state maintained by RS
  {
    P_SWITCHOVER_INVALID = 0,
    P_SWITCHOVER_NORMAL,
    P_SWITCHOVER_SWITCHING,
    P_FAILOVER_FLASHBACK,
    P_FAILOVER_CLEANUP,
    P_MAX_STATUS
  };

  // Switchover status of external display
  enum InMemorySwitchOverStatus {
    // invalid status
    I_INVALID = 0,

    // can not switchover to primary or standby
    I_NOT_ALLOW,

    // only in primary cluster, the cluster can switchover to standby
    I_TO_STANDBY,

    // only in standby cluster, the cluster can switchover to primary
    I_TO_PRIMARY,

    // the intermediate status of primary cluster switchover to standby,
    // can not provide write service.
    I_SWITCHOVER_SWITCHING,

    // the intermediate status of standby cluster failover to primary cluster.
    // cluster is flashback to a consistent state, need to retry failover until success.
    // the status can not provide write service.
    I_FAILOVER_FLASHBACK,

    // the intermediate status of standby cluster failover to primary cluster.
    // cluster is doing cleanup, remove unused schema.
    // the status can not do DDL
    I_FAILOVER_CLEANUP,

    I_MAX_STATUS
  };

  ObClusterInfo()
      : cluster_id_(-1),
        cluster_type_(common::INVALID_CLUSTER_TYPE),
        login_name_(),
        login_passwd_(),
        switchover_status_(P_SWITCHOVER_INVALID),
        cluster_status_(common::INVALID_CLUSTER_STATUS),
        switch_timestamp_(0),
        is_sync_(false),
        gc_snapshot_ts_(common::OB_INVALID_VERSION),
        protection_mode_(common::MAXIMUM_PERFORMANCE_MODE),
        version_(common::OB_INVALID_VERSION),
        protection_level_(common::MAXIMUM_PERFORMANCE_LEVEL)
  {}
  ~ObClusterInfo()
  {}
  ObClusterInfo(const ObClusterInfo& other);
  ObClusterInfo& operator=(const ObClusterInfo& other);

  void reset();
  int assign(const ObClusterInfo& other);
  bool operator!=(const ObClusterInfo& other) const;
  bool is_valid() const;
  static int str_to_in_memory_switchover_status(const common::ObString& status_str, InMemorySwitchOverStatus& status);
  static const char* in_memory_switchover_status_to_str(const InMemorySwitchOverStatus& status);
  static const char* persistent_switchover_status_to_str(const PersistentSwitchOverStatus& status);
  static bool is_primary_cluster(const ObClusterInfo& cluster_info);

  static int64_t generate_switch_timestamp(const int64_t switch_timestamp);
  static int64_t get_pure_switch_timestamp(const int64_t switch_timestamp);
  int inc_switch_timestamp();
  int64_t get_switch_timestamp() const
  {
    return switch_timestamp_;
  }
  int64_t atomic_get_switch_timestamp() const
  {
    return ATOMIC_LOAD(&switch_timestamp_);
  }
  void atomic_set_switch_timestamp(const int64_t switchover_epoch)
  {
    ATOMIC_SET(&switch_timestamp_, switchover_epoch);
  }
  void set_switch_timestamp(const int64_t switchover_epoch)
  {
    switch_timestamp_ = switchover_epoch;
  }
  bool is_less_than(const int64_t switch_timestamp) const;
  int64_t atomic_get_version() const
  {
    return ATOMIC_LOAD(&version_);
  }
  void set_cluster_info_version(const int64_t version)
  {
    version_ = version;
  }
  TO_STRING_KV(K_(cluster_id), K_(cluster_type), K_(login_name), K_(login_passwd), "switchover_status",
      persistent_switchover_status_to_str(switchover_status_), K_(cluster_status), K_(switch_timestamp), K_(is_sync),
      K_(gc_snapshot_ts), K_(protection_mode), K_(version), K_(protection_level));

public:
  static const char* IN_MEMORY_SWITCHOVER_STATUS_ARRAY[];
  static const char* PERSISTENT_SWITCHOVER_STATUS_ARRAY[];
  static const char* CLUSTER_STATUS_ARRAY[];
  static const int64_t MAX_CHANGE_TIMES = 1000;
  int64_t cluster_id_;
  common::ObClusterType cluster_type_;
  UserNameString login_name_;
  PassWdString login_passwd_;
  PersistentSwitchOverStatus switchover_status_;
  common::ObClusterStatus cluster_status_;

private:
  // It can avoid the backoff of switching state, and can switch 1000 times at most in one switching process;
  // The last 10 bits are used to indicate the number of state changes in a switching process; each state change will
  // inc_switch_timestamp
  int64_t switch_timestamp_;

public:
  bool is_sync_;  // the cluster is sync with primary
public:
  int64_t gc_snapshot_ts_;
  common::ObProtectionMode protection_mode_;
  int64_t version_;  // Mark the change of each variable in cluster info to avoid state fallback
  common::ObProtectionLevel protection_level_;
};

class ObClusterInfoProxy {
public:
  ObClusterInfoProxy();
  virtual ~ObClusterInfoProxy();
  // the cluster's create timestamp
  static int load_cluster_create_timestamp(common::ObISQLClient& sql_proxy, int64_t& cluster_create_ts);
  static int load(common::ObISQLClient& sql_proxy, ObClusterInfo& cluster_info);
  static int update(
      common::ObISQLClient& sql_proxy, const ObClusterInfo& cluster_info, const bool with_login_info = false);

private:
  static const char* OB_ALL_CLUSTER_INFO_TNAME;
  static const char* LOGIN_NAME;
  static const char* LOGIN_PASSWD;
  static const char* LOGIN_PASSWD_LENGTH;
  static const char* CLUSTER_TYPE;
  static const char* SWITCHOVER_STATUS;
  static const char* SWITCHOVER_TIMESTAMP;
  static const char* CLUSTER_STATUS;
  static const char* ENCRYPTION_KEY;
  static const char* PROTECTION_MODE;
  static const char* VERSION;
  static const char* PROTECTION_LEVEL;
};
}  // namespace share
}  // namespace oceanbase
#endif
