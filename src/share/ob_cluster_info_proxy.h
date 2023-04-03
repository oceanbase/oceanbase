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
#include "share/ob_cluster_role.h"            // ObClusterRole
namespace oceanbase
{
namespace share
{
#define SWITCH_TIMSTAMP_MASK 0x3FF
class ObClusterInfo
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t MAX_PASSWD_LENGTH = common::OB_MAX_PASSWORD_LENGTH;
  typedef common::ObFixedLengthString<common::OB_MAX_USERNAME_LENGTH> UserNameString;
  typedef common::ObFixedLengthString<MAX_PASSWD_LENGTH> PassWdString;
  enum PersistentSwitchOverStatus  //Persistent switchover state maintained by RS
  {
    P_SWITCHOVER_INVALID = 0,
    P_SWITCHOVER_NORMAL = 1,
    P_SWITCHOVER_SWITCHING = 2,
    P_FAILOVER_FLASHBACK = 3,//no use any more
    P_FAILOVER_CLEANUP = 4,
    P_FAILOVER_FLASHBACK_INNER = 5,
    P_FAILOVER_FLASHBACK_USER = 6,
    P_FAILOVER_FLASHBACK_INNER_PRE = 7,
    P_FAILOVER_FLASHBACK_CLEANUP = 8,
    P_FAILOVER_FLASHBACK_USER_PRE = 9,
    P_DISCONNECT = 10,
    P_MAX_STATUS
  };

  /* The process of FAILOVER FLASHBACK
    1. Construct all tenant’s failover_scn
    2. Keep the SCN needed by flashback to avoid recycling clog before replaying it 
    3. Prepare partitions to do flashback for tenant's inner table
      a) Write cutdata clog
      b) Wait flashback info dump finished
      c) Wait the restore status of replicas are in cutdata status
    4. Do flashback tenant's inner table partitions, send cutdata task and wait cutdata task finished 
    5. Do Preparations to do user table flashback
    6. Prepare partitions to do flashback for tenant's user table
      a) Write cutdata clog
      b) Wait flashback info dump finished
      c) Wait the restore status of replicas are in cutdata status
    7. Do flashback tenant's user table partitions, send cutdata task and wait cutdata task finished 

    The following table shows what will do in each stage of FAILOVER FLASHBACK
      +-------------------------------------+--------------+
      | status                              | process      |
      +-------------------------------------+--------------+
      | P_FAILOVER_FLASHBACK                | 1-2          |
      +-------------------------------------+--------------+
      | P_FAILOVER_FLASHBACK_INNER_PRE      | 3            |
      +-------------------------------------+--------------+
      | P_FAILOVER_FLASHBACK_INNER          | 4            |
      +-------------------------------------+--------------+
      | P_FAILOVER_FLASHBACK_CLEANUP        | 5            |
      +-------------------------------------+--------------+
      | P_FAILOVER_FLASHBACK_USER_PRE       | 6            |
      +-------------------------------------+--------------+
      | P_FAILOVER_FLASHBACK_USER           | 7            |
      +-------------------------------------+--------------+
  */

  //Switchover status of external display
  enum InMemorySwitchOverStatus
  {
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
    
    I_DISCONNECT,

    I_MAX_STATUS
  };

  ObClusterInfo() : cluster_id_(-1), cluster_role_(common::INVALID_CLUSTER_ROLE),
    switchover_status_(P_SWITCHOVER_INVALID), cluster_status_(common::INVALID_CLUSTER_STATUS),
    switch_timestamp_(0), is_sync_(false),
    protection_mode_(common::INVALID_PROTECTION_MODE), version_(common::OB_INVALID_VERSION),
    protection_level_(common::INVALID_PROTECTION_LEVEL) {}
  ObClusterInfo(const common::ObClusterStatus cluster_status,
                const common::ObClusterRole cluster_role,
                const PersistentSwitchOverStatus switchover_status,
                const int64_t cluster_id,
                const int64_t version)
  {
    reset();
    cluster_status_ = cluster_status;
    cluster_role_ = cluster_role;
    switchover_status_ = switchover_status;
    cluster_id_ = cluster_id;
    version_ = version;

  }
  ~ObClusterInfo() {}
  ObClusterInfo(const ObClusterInfo &other);
  ObClusterInfo &operator =(const ObClusterInfo &other);

  void reset();
  int assign(const ObClusterInfo &other);
  bool operator !=(const ObClusterInfo &other) const;
  bool is_valid() const;
  static int str_to_in_memory_switchover_status(const common::ObString &status_str,
                                                InMemorySwitchOverStatus &status);
  static const char* in_memory_switchover_status_to_str(const InMemorySwitchOverStatus &status);
  static const char* persistent_switchover_status_to_str(const PersistentSwitchOverStatus &status);
  static bool is_in_failover(const PersistentSwitchOverStatus &status);

  static int64_t generate_switch_timestamp(const int64_t switch_timestamp);
  static int64_t get_pure_switch_timestamp(const int64_t switch_timestamp);
  static bool is_failover_flashback(const PersistentSwitchOverStatus switchover_status)
  {
    return is_failover_flashback_inner(switchover_status)
           || is_failover_flashback_user(switchover_status);
  }

  static bool is_failover_flashback_inner(const PersistentSwitchOverStatus switchover_status)
  {
    return P_FAILOVER_FLASHBACK_INNER_PRE == switchover_status
           || P_FAILOVER_FLASHBACK_INNER == switchover_status;
  }

  static bool is_failover_flashback_user(const PersistentSwitchOverStatus switchover_status)
  {
    return P_FAILOVER_FLASHBACK_USER_PRE == switchover_status
           || P_FAILOVER_FLASHBACK_USER == switchover_status
           || P_FAILOVER_FLASHBACK_CLEANUP == switchover_status;
  }

  int inc_switch_timestamp();
  int64_t get_switch_timestamp() const { return switch_timestamp_; }
  int64_t atomic_get_switch_timestamp() const { return ATOMIC_LOAD(&switch_timestamp_); }
  void atomic_set_switch_timestamp(const int64_t switchover_epoch)
  { ATOMIC_SET(&switch_timestamp_, switchover_epoch); }
  void set_switch_timestamp(const int64_t switchover_epoch)
  { switch_timestamp_ = switchover_epoch; }
  bool is_less_than(const int64_t switch_timestamp) const;
  int64_t atomic_get_version() const { return ATOMIC_LOAD(&version_); }
  void set_cluster_info_version(const int64_t version)
  {
    version_ = version;
  }

  TO_STRING_KV(K_(cluster_id), K_(cluster_role),
               "switchover_status", persistent_switchover_status_to_str(switchover_status_),
               K_(cluster_status), K_(switch_timestamp), K_(is_sync),
               K_(protection_mode), K_(version), K_(protection_level));
public: // TODO public -> private
  static const char* IN_MEMORY_SWITCHOVER_STATUS_ARRAY[];
  static const char* PERSISTENT_SWITCHOVER_STATUS_ARRAY[];
  static const char* CLUSTER_STATUS_ARRAY[];
  static const int64_t MAX_CHANGE_TIMES = 1000;
  int64_t cluster_id_;
  common::ObClusterRole cluster_role_;
  PersistentSwitchOverStatus switchover_status_;
  common::ObClusterStatus cluster_status_;
private:
  //It can avoid the backoff of switching state, and can switch 1000 times at most in one switching process;
  //The last 10 bits are used to indicate the number of state changes in a switching process; each state change will inc_switch_timestamp
  int64_t switch_timestamp_;

public:
  bool is_sync_;//the cluster is sync with primary
public:
  common::ObProtectionMode protection_mode_;
  int64_t version_;//Mark the change of each variable in cluster info to avoid state fallback
  common::ObProtectionLevel protection_level_;
};

class ObClusterInfoProxy
{
public:
  ObClusterInfoProxy();
  virtual ~ObClusterInfoProxy();
  // the cluster's create timestamp
  static int load_cluster_create_timestamp(common::ObISQLClient &sql_proxy,
      int64_t &cluster_create_ts);
  static int load(common::ObISQLClient &sql_proxy, ObClusterInfo &cluster_info,
                  const bool for_update = false);
  static int update(common::ObISQLClient &sql_proxy, const ObClusterInfo &cluster_info);
private:
  static const char* OB_ALL_CLUSTER_INFO_TNAME;
  static const char* CLUSTER_ROLE;
  static const char* SWITCHOVER_STATUS;
  static const char* SWITCHOVER_TIMESTAMP;
  static const char* CLUSTER_STATUS;
  static const char* ENCRYPTION_KEY;
  static const char* PROTECTION_MODE;
  static const char* VERSION;
  static const char* PROTECTION_LEVEL;
};
} //end share
} //end oceanbase
#endif

