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

#ifndef _OCEABASE_OBSERVER_OB_SERVER_STRUCT_H_
#define _OCEABASE_OBSERVER_OB_SERVER_STRUCT_H_

// DON'T INCLUDE ANY OCEANBASE HEADER EXCEPT FROM LIB DIRECTORY
#include "share/ob_lease_struct.h"
#include "lib/net/ob_addr.h"
#include "share/part/ob_part_mgr.h"
#include "share/ob_cluster_info_proxy.h"
#include "share/ob_cluster_type.h"  // ObClusterType
#include "share/ob_rpc_struct.h"

namespace oceanbase {
namespace common {
class ObServerConfig;
class ObConfigManager;
class ObMySQLProxy;
class ObTimer;
class ObIDataAccessService;
class ObMysqlRandom;
}  // end of namespace common

namespace obrpc {
class ObSrvRpcProxy;
class ObCommonRpcProxy;
class ObLoadDataRpcProxy;
}  // namespace obrpc

namespace share {
class ObResourcePlanManager;
class ObPartitionTableOperator;
class ObRemotePartitionTableOperator;
class ObRsMgr;
class ObPartitionLocationCache;
class ObRemoteSqlProxy;
class ObSchemaStatusProxy;
namespace schema {
class ObMultiVersionSchemaService;
}  // end of namespace schema
}  // end of namespace share

namespace rootserver {
class ObRootService;
class ObInZoneMaster;
}  // end of namespace rootserver

namespace sql {
class ObSQLSessionMgr;
class ObQueryExecCtxMgr;
class ObSql;
class ObExecutorRpcImpl;
}  // end of namespace sql

namespace storage {
class ObPartitionService;
class ObPtfMgr;
}  // namespace storage

namespace transaction {
class ObGlobalTimestampService;
class ObIWeakReadService;
}  // namespace transaction

namespace obmysql {
class ObDiag;
}  // end of namespace obmysql

namespace omt {
class ObMultiTenant;
class ObCgroupCtrl;
}  // namespace omt

namespace observer {
class ObService;
class ObVTIterCreator;

class ObServerOptions {
public:
  ObServerOptions()
      : rpc_port_(0),
        elect_port_(0),
        mysql_port_(0),
        home_(NULL),
        zone_(NULL),
        nodaemon_(false),
        optstr_(NULL),
        devname_(NULL),
        rs_list_(NULL),
        appname_(NULL),
        cluster_id_(common::OB_INVALID_CLUSTER_ID),
        data_dir_(NULL),
        mode_(NULL),
        log_level_(0),
        use_ipv6_(false),
        flashback_scn_(0)
  {}
  ObServerOptions(int rpc_port, int elect_port, int mysql_port, const char* home, const char* zone, bool nodaemon,
      const char* optstr, const char* devname, const char* rs_list, const char* appname, int64_t cluster_id,
      const char* data_dir, int8_t log_level, const char* mode, bool use_ipv6, int64_t flashback_scn)
  {
    rpc_port_ = rpc_port;
    elect_port_ = elect_port;
    mysql_port_ = mysql_port;
    home_ = home;
    zone_ = zone;
    nodaemon_ = nodaemon;
    optstr_ = optstr;
    devname_ = devname;
    rs_list_ = rs_list;
    appname_ = appname;
    cluster_id_ = cluster_id;
    data_dir_ = data_dir;
    mode_ = mode;
    log_level_ = log_level;
    use_ipv6_ = use_ipv6;
    flashback_scn_ = flashback_scn;
  }
  virtual ~ObServerOptions()
  {}

  int rpc_port_;
  int elect_port_;
  int mysql_port_;
  const char* home_;
  const char* zone_;
  bool nodaemon_;
  const char* optstr_;
  const char* devname_;
  const char* rs_list_;
  const char* appname_;
  int64_t cluster_id_;
  const char* data_dir_;
  const char* mode_;
  int8_t log_level_;
  bool use_ipv6_;
  int64_t flashback_scn_;
};

enum ObServerMode {
  INVALID_MODE = 0,
  NORMAL_MODE,
  PHY_FLASHBACK_MODE,
  PHY_FLASHBACK_VERIFY_MODE,
};

enum ObServiceStatus { SS_INIT, SS_STARTING, SS_SERVING, SS_STOPPING, SS_STOPPED };

struct ObGlobalContext {
  common::ObAddr self_addr_;
  rootserver::ObRootService* root_service_;
  rootserver::ObInZoneMaster* in_zone_master_;
  observer::ObService* ob_service_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObPartMgr* part_mgr_;
  common::ObServerConfig* config_;
  common::ObConfigManager* config_mgr_;
  share::ObPartitionTableOperator* pt_operator_;
  share::ObRemotePartitionTableOperator* remote_pt_operator_;
  obrpc::ObSrvRpcProxy* srv_rpc_proxy_;
  obrpc::ObCommonRpcProxy* rs_rpc_proxy_;
  obrpc::ObLoadDataRpcProxy* load_data_proxy_;
  sql::ObExecutorRpcImpl* executor_rpc_;
  common::ObMySQLProxy* sql_proxy_;
  share::ObRemoteSqlProxy* remote_sql_proxy_;
  common::ObDbLinkProxy* dblink_proxy_;
  share::ObRsMgr* rs_mgr_;
  storage::ObPartitionService* par_ser_;
  transaction::ObGlobalTimestampService* gts_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  common::ObIDataAccessService* vt_par_ser_;
  sql::ObSQLSessionMgr* session_mgr_;
  sql::ObQueryExecCtxMgr* query_ctx_mgr_;
  sql::ObSql* sql_engine_;
  omt::ObMultiTenant* omt_;
  ObVTIterCreator* vt_iter_creator_;
  share::ObPartitionLocationCache* location_cache_;
  int64_t start_time_;
  int64_t* merged_version_;
  int64_t* global_last_merged_version_;
  int64_t* warm_up_start_time_;
  uint64_t server_id_;
  ObServiceStatus status_;
  ObServerMode mode_;
  share::RSServerStatus rs_server_status_;
  int64_t start_service_time_;
  common::ObString* sort_dir_;
  obmysql::ObDiag* diag_;
  common::ObMysqlRandom* scramble_rand_;
  omt::ObCgroupCtrl* cgroup_ctrl_;
  bool inited_;
  int64_t split_schema_version_;
  // the start schema_version that sys can read and write __all_table_v2/__all_table_history_v2
  int64_t split_schema_version_v2_;
  transaction::ObIWeakReadService* weak_read_service_;
  share::ObSchemaStatusProxy* schema_status_proxy_;
  int64_t flashback_scn_;
  int64_t ssl_key_expired_time_;
  // the cluster_id of the strongly synchronized standby cluster
  int64_t sync_standby_cluster_id_;
  // the redo_transport_option of the strongly synchronized standby cluster
  share::ObRedoTransportOption sync_standby_redo_options_;
  // the number of partitions that failed to receive
  // the acks from strongly synchronized standby cluster during timeout
  volatile int64_t sync_timeout_partition_cnt_;

  ObGlobalContext()
  {
    MEMSET(this, 0, sizeof(*this));
    init();
  }
  ObGlobalContext& operator=(const ObGlobalContext& other);
  void init();
  bool is_inited() const
  {
    return inited_;
  }
  // Refer to the high availability zone design document
  bool is_observer() const;
  bool is_standby_cluster_and_started()
  {
    return is_observer() && is_standby_cluster() && has_start_service();
  }
  bool is_started_and_can_weak_read()
  {
    return is_observer() && has_start_service();
  }
  bool is_schema_splited() const
  {
    return common::OB_INVALID_VERSION != split_schema_version_;
  }
  void set_split_schema_version(int64_t new_split_schema_version);
  void set_split_schema_version_v2(int64_t new_split_schema_version);
  int64_t get_switch_epoch2() const;
  int64_t get_pure_switch_epoch() const;
  bool is_primary_cluster() const;
  bool is_standby_cluster() const;
  common::ObClusterType get_cluster_type() const;
  share::ServerServiceStatus get_server_service_status() const;
  bool can_be_parent_cluster() const;
  bool can_do_leader_takeover() const;
  bool is_in_primary_switching_state() const;
  void get_cluster_type_and_status(
      common::ObClusterType& cluster_type, share::ServerServiceStatus& server_status) const;
  bool is_in_standby_switching_state() const;
  int64_t get_cluster_idx() const;
  bool is_in_standby_active_state() const;
  bool is_in_phy_fb_mode() const;
  bool is_in_phy_fb_verify_mode() const;
  void set_cluster_idx(const int64_t cluster_idx);
  bool is_in_flashback_state() const;
  bool is_in_cleanup_state() const;
  bool is_in_invalid_state() const;
  bool is_in_disabled_state() const;
  bool need_sync_to_standby() const
  {
    return false;
  }
  // retrun whether the protection mode of cluster is max_availability
  // it is used to judge need revoke for clog
  bool is_in_max_availability_mode() const
  {
    return false;
  }
  // Determine whether the standby database is in strong synchronization state,
  // and whether it needs to accept strong synchronization logs
  bool is_sync_level_on_standby()
  {
    return false;
  }
  int64_t get_cluster_info_version() const
  {
    return 0;
  }
  common::ObProtectionMode get_protection_mode()
  {
    return common::MAXIMUM_PERFORMANCE_MODE;
  }
  int get_sync_standby_cluster_list(common::ObIArray<int64_t>& sync_standby);
  int get_sync_standby_cluster_id(int64_t& sync_cluster_id);
  void set_upgrade_stage(obrpc::ObUpgradeStage upgrade_stage)
  {
    upgrade_stage_ = upgrade_stage;
  }
  obrpc::ObUpgradeStage get_upgrade_stage()
  {
    return upgrade_stage_;
  }
  DECLARE_TO_STRING;
  const share::ObClusterInfo& get_cluster_info() const
  {
    return cluster_info_;
  }
  void inc_sync_timeout_partition_cnt()
  {
    ATOMIC_INC(&sync_timeout_partition_cnt_);
  }
  void reset_sync_timeout_partition_cnt()
  {
    ATOMIC_SET(&sync_timeout_partition_cnt_, 0);
  }
  // Retruns the number of partitions that failed to receive
  // the acks from strongly synchronized standby cluster during timeout
  int64_t get_sync_timeout_partition_cnt() const
  {
    return ATOMIC_LOAD(&sync_timeout_partition_cnt_);
  }
  // Returns the timeout of the strongly synchronized standby cluster
  int64_t get_sync_standby_net_timeout()
  {
    return ATOMIC_LOAD(&sync_standby_redo_options_.net_timeout_);
  }

private:
  common::SpinRWLock cluster_info_rwlock_;
  share::ObClusterInfo cluster_info_;
  int64_t cluster_idx_;  // Internal identification of cluster
  volatile int64_t server_status_;
  bool has_start_service() const
  {
    return 0 < start_service_time_;
  }

  obrpc::ObUpgradeStage upgrade_stage_;
};

struct ObThreadContext {
  const ObGlobalContext* gctx_;
};

ObGlobalContext& global_context();

struct ObUseWeakGuard {
  ObUseWeakGuard();
  ~ObUseWeakGuard();
  static bool did_use_weak();

private:
  struct TSIUseWeak {
    bool inited_;
    bool did_use_weak_;
    TSIUseWeak() : inited_(false), did_use_weak_(false)
    {}
  };
};
}  // end of namespace observer
}  // end of namespace oceanbase

#define GCTX (::oceanbase::observer::global_context())

#endif /* _OCEABASE_OBSERVER_OB_SERVER_STRUCT_H_ */
