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

#ifndef _OCEABASE_SHARE_OB_SERVER_STRUCT_H_
#define _OCEABASE_SHARE_OB_SERVER_STRUCT_H_

// DON'T INCLUDE ANY OCEANBASE HEADER EXCEPT FROM LIB DIRECTORY
#include "share/ob_lease_struct.h"
#include "lib/net/ob_addr.h"
#include "share/ob_cluster_role.h"              // ObClusterRole
#include "share/ob_rpc_struct.h"
#include "share/ob_server_status.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
namespace common
{
class ObServerConfig;
class ObConfigManager;
class ObMySQLProxy;
class ObTimer;
class ObITabletScan;
class ObMysqlRandom;
} // end of namespace common

namespace obrpc
{
class ObSrvRpcProxy;
class ObStorageRpcProxy;
class ObCommonRpcProxy;
class ObLoadDataRpcProxy;
class ObDBMSJobRpcProxy;
class ObBatchRpc;
class ObInnerSQLRpcProxy;
class ObDBMSSchedJobRpcProxy;
class ObExtenralTableRpcProxy;
} // end of namespace rpc

namespace rootserver
{
class ObRootService;
class ObInZoneMaster;
} // end of namespace rootserver

namespace sql
{
class ObSQLSessionMgr;
class ObSql;
class ObExecutorRpcImpl;
class ObDataAccessService;
class ObConnectResourceMgr;
} // end of namespace sql

namespace pl
{
class ObPL;
}

namespace storage
{
class ObPtfMgr;
class ObLocalityManager;
}

namespace transaction
{
class ObIWeakReadService;
}

namespace obmysql
{
class ObDiag;
} // end of namespace obmysql

namespace omt
{
class ObMultiTenant;
}

namespace logservice
{
class ObServerLogBlockMgr;
}

#ifdef OB_BUILD_ARBITRATION
namespace arbserver
{
class ObArbGarbageCollectService;
}
#endif

namespace observer
{
class ObService;
class ObVTIterCreator;
class ObTableService;
class ObSrvNetworkFrame;
class ObIDiskReport;
class ObResourceInnerSQLConnectionPool;
class ObStartupAccelTaskHandler;
} // end of namespace observer

namespace plugin
{
class ObPluginMgr;
}

namespace share
{
class ObResourcePlanManager;
class ObLSTableOperator;
class ObTabletTableOperator;
class ObRsMgr;
class ObLocationService;
class ObSchemaStatusProxy;
class ObRatelimitMgr;
class ObAliveServerTracer;
class ObCgroupCtrl;
class ObWorkloadRepositoryService;

namespace schema
{
class ObMultiVersionSchemaService;
} // end of namespace schema

class ObServerOptions
{
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
      startup_mode_(NULL),
      log_level_(0),
      use_ipv6_(false),
      flashback_scn_(0),
      local_ip_(NULL),
      plugins_load_(NULL)
  {
  }
  ObServerOptions(int rpc_port,
                  int elect_port,
                  int mysql_port,
                  const char *home,
                  const char *zone,
                  bool nodaemon,
                  const char *optstr,
                  const char *devname,
                  const char *rs_list,
                  const char *appname,
                  int64_t cluster_id,
                  const char *data_dir,
                  const char *shared_storage_info,
                  int8_t log_level,
                  const char *mode,
                  bool use_ipv6,
                  int64_t flashback_scn,
                  const char *local_ip,
                  const char *plugins_load)
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
    startup_mode_ = mode;
    log_level_ = log_level;
    use_ipv6_ = use_ipv6;
    flashback_scn_ = flashback_scn;
    local_ip_ = local_ip;
    plugins_load_ = plugins_load;
  }
  virtual ~ObServerOptions() {}

  int rpc_port_;
  int elect_port_;
  int mysql_port_;
  const char *home_;
  const char *zone_;
  bool nodaemon_;
  const char *optstr_;
  const char *devname_;
  const char *rs_list_;
  const char *appname_;
  int64_t cluster_id_;
  const char *data_dir_;
  const char *startup_mode_;
  int8_t log_level_;
  bool use_ipv6_;
  int64_t flashback_scn_;
  const char *local_ip_;
  const char *plugins_load_;
};

struct ObGlobalContext
{
  common::ObAddrWithSeq self_addr_seq_;
  rootserver::ObRootService *root_service_;
  rootserver::ObInZoneMaster *in_zone_master_;
  observer::ObService *ob_service_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObServerConfig *config_;
  common::ObConfigManager *config_mgr_;
  share::ObLSTableOperator *lst_operator_;
  share::ObTabletTableOperator *tablet_operator_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;
  obrpc::ObStorageRpcProxy *storage_rpc_proxy_;
  obrpc::ObDBMSJobRpcProxy *dbms_job_rpc_proxy_;
  obrpc::ObInnerSQLRpcProxy *inner_sql_rpc_proxy_;
  obrpc::ObDBMSSchedJobRpcProxy *dbms_sched_job_rpc_proxy_;
  obrpc::ObCommonRpcProxy *rs_rpc_proxy_;
  obrpc::ObLoadDataRpcProxy *load_data_proxy_;
  sql::ObExecutorRpcImpl *executor_rpc_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObMySQLProxy *ddl_sql_proxy_;
  common::ObOracleSqlProxy *ddl_oracle_sql_proxy_;
  common::ObDbLinkProxy *dblink_proxy_;
  observer::ObResourceInnerSQLConnectionPool *res_inner_conn_pool_;
  share::ObRsMgr *rs_mgr_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  common::ObITabletScan *vt_par_ser_;
  common::ObITabletScan *et_access_service_;
  sql::ObSQLSessionMgr *session_mgr_;
  sql::ObSql *sql_engine_;
  pl::ObPL *pl_engine_;
  omt::ObMultiTenant *omt_;
  observer::ObVTIterCreator *vt_iter_creator_;
  share::ObLocationService *location_service_;
  int64_t start_time_;
  int64_t *warm_up_start_time_;
  ObServiceStatus status_;
  ObServerMode startup_mode_;
  share::RSServerStatus rs_server_status_;
  int64_t start_service_time_;
  obmysql::ObDiag *diag_;
  common::ObMysqlRandom *scramble_rand_;
  observer::ObTableService *table_service_;
  share::ObCgroupCtrl *cgroup_ctrl_;
  observer::ObSrvNetworkFrame *net_frame_;
  share::ObRatelimitMgr *rl_mgr_;
  obrpc::ObBatchRpc *batch_rpc_;
  share::ObAliveServerTracer *server_tracer_;
  observer::ObIDiskReport *disk_reporter_;
  logservice::ObServerLogBlockMgr *log_block_mgr_;
#ifdef OB_BUILD_ARBITRATION
  arbserver::ObArbGarbageCollectService *arb_gcs_;
#endif

  bool inited_;
  transaction::ObIWeakReadService *weak_read_service_;
  share::ObSchemaStatusProxy *schema_status_proxy_;
  int64_t flashback_scn_;
  int64_t ssl_key_expired_time_;
  sql::ObConnectResourceMgr* conn_res_mgr_;
  storage::ObLocalityManager *locality_manager_;
  obrpc::ObExtenralTableRpcProxy *external_table_proxy_;
  share::ObWorkloadRepositoryService *wr_service_;
  observer::ObStartupAccelTaskHandler* startup_accel_handler_;
  bool in_bootstrap_;
  plugin::ObPluginMgr *plugin_mgr_ = nullptr;

  static ObGlobalContext& get_instance();
  void init();
  bool is_inited() const { return inited_; }
  // Refer to the high availability zone design document
  //
  bool is_observer() const;
  common::ObClusterRole get_cluster_role() const;
  share::ServerServiceStatus get_server_service_status() const;
  /*
  Returns a globally unique, monotonically increasing server ID.
  This ID is unique across the lifetime of the cluster and will not be reused.
  */
  inline uint64_t get_server_id() const { return ATOMIC_LOAD(&server_id_); }
  inline void set_server_id(const uint64_t id) { ATOMIC_SET(&server_id_, id); }
  /*
  Returns a currently unique server index within the cluster.
  This index is unique among current servers in the cluster, but may be reused if a server is removed from the cluster.

  When server ID has size limitation, like only 12 bits are allocated for server ID in session_id
  to ensure its uniqueness, which implies the server ID cannot be greater than or equal to 4096,
  we should consider using server index instead of server ID.
  However, using server index requires a guarantee that no remnants of an old server remain after it is deleted.
  For example, no sessions from the deleted server should exist anymore. If remnants persist after server deletion,
  using server index is not permitted, otherwise, correctness issues may arise.
  In such cases, you need to carefully consider how to resolve this problem by yourself.
  */
  uint64_t get_server_index() const;
  void set_upgrade_stage(obrpc::ObUpgradeStage upgrade_stage) { upgrade_stage_ = upgrade_stage; }
  obrpc::ObUpgradeStage get_upgrade_stage() { return upgrade_stage_; }
  DECLARE_TO_STRING;
  // instead of self_addr_
  const ObAddr &self_addr() const { return self_addr_seq_.get_addr(); }
  const int64_t &self_seq() const { return self_addr_seq_.get_seq(); }
  bool is_shared_storage_mode() const
  {
#ifdef OB_BUILD_SHARED_STORAGE
    return (ObServerMode::SHARED_STORAGE_MODE == startup_mode_);
#else
    return false;
#endif
  }
private:
  ObGlobalContext() { MEMSET(this, 0, sizeof(*this)); init(); }
  ObGlobalContext(const ObGlobalContext &other);
  ObGlobalContext &operator=(const ObGlobalContext &other);
  volatile int64_t server_status_;
  bool has_start_service() const { return 0 < start_service_time_; }

  obrpc::ObUpgradeStage upgrade_stage_;
  uint64_t server_id_;
};

} // end of namespace share
} // end of namespace oceanbase

#define GCTX (::oceanbase::share::ObGlobalContext::get_instance())

#endif /* _OCEABASE_SHARE_OB_SERVER_STRUCT_H_ */
