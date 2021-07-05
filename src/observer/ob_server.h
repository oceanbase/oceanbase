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

#ifndef _OCEABASE_OBSERVER_OB_SERVER_H_
#define _OCEABASE_OBSERVER_OB_SERVER_H_

#include "lib/signal/ob_signal_worker.h"
#include "lib/random/ob_mysql_random.h"

#include "share/stat/ob_user_tab_col_statistics.h"
#include "share/stat/ob_opt_stat_service.h"

#include "sql/ob_sql.h"
#include "sql/engine/cmd/ob_load_data_rpc.h"
#include "sql/ob_query_exec_ctx_mgr.h"

#include "storage/transaction/ob_weak_read_service.h"  // ObWeakReadService
#include "storage/ob_long_ops_monitor.h"

#include "rootserver/ob_root_service.h"

#include "observer/mysql/ob_diag.h"

#include "observer/omt/ob_cgroup_ctrl.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_worker_processor.h"

#include "observer/virtual_table/ob_virtual_data_access_service.h"

#include "observer/ob_signal_handle.h"
#include "observer/ob_tenant_duty_task.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_cache_size_calculator.h"
#include "observer/ob_srv_network_frame.h"
#include "observer/ob_service.h"
#include "observer/ob_server_reload_config.h"
#include "observer/ob_root_service_monitor.h"

namespace oceanbase {
namespace omt {
class ObTenantTimezoneMgr;
}
namespace observer {
// This the class definition of ObAddr which responds the server
// itself. It's designed as a singleton in program. This class is
// structure aggregated but not logical processing. Please don't put
// cumbersome logical processes into it. Here's a typical usage:
//
//   ObServer server;
//   server.init(...);
//   server.set_xxx(...);
//   server.start(); // blocked only program is coming to stop
//   server.destory();
//
class ObServer {
public:
  static const int64_t DEFAULT_ETHERNET_SPEED = 1000 / 8 * 1024 * 1024;  // default 125m/s  1000Mbit
  static ObServer& get_instance();

public:
  int init(const ObServerOptions& opts, const ObPLogWriterCfg& log_cfg);
  void destroy();

  // Start OceanBase server, this function is blocked after invoking
  // until the server itself stops it.
  int start();
  int wait();
  void set_stop();

public:
  // Refer to ObPurgeCompletedMonitorInfoTask
  class ObCTASCleanUpTask : public common::ObTimerTask {
  public:
    ObCTASCleanUpTask();
    virtual ~ObCTASCleanUpTask()
    {}
    int init(ObServer* observer, int tg_id);
    void destroy();
    virtual void runTimerTask() override;

  private:
    const static int64_t CLEANUP_INTERVAL = 60L * 1000L * 1000L;  // 60s
    ObServer* obs_;
    bool is_inited_;
  };

  class ObRefreshTimeTask : public common::ObTimerTask {
  public:
    ObRefreshTimeTask();
    virtual ~ObRefreshTimeTask()
    {}
    int init(ObServer* observer, int tg_id);
    void destroy();
    virtual void runTimerTask() override;

  private:
    const static int64_t REFRESH_INTERVAL = 60L * 60L * 1000L * 1000L;  // 1hr
    ObServer* obs_;
    bool is_inited_;
  };

  class ObRefreshTime {
  public:
    explicit ObRefreshTime(ObServer* obs) : obs_(obs)
    {}
    virtual ~ObRefreshTime()
    {}
    bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo* sess_info);

  private:
    ObServer* obs_;
    DISALLOW_COPY_AND_ASSIGN(ObRefreshTime);
  };

  class ObCTASCleanUp {
  public:
    explicit ObCTASCleanUp(ObServer* obs, bool drop_flag) : obs_(obs), drop_flag_(drop_flag)
    {}
    virtual ~ObCTASCleanUp()
    {}
    bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo* sess_info);
    inline void set_session_id(uint64_t session_id)
    {
      session_id_ = session_id;
    }
    inline uint64_t get_session_id()
    {
      return session_id_;
    }
    inline void set_schema_version(int64_t schema_version)
    {
      schema_version_ = schema_version;
    }
    inline int64_t get_schema_version()
    {
      return schema_version_;
    }
    inline void set_drop_flag(bool drop_flag)
    {
      drop_flag_ = drop_flag;
    }
    inline bool get_drop_flag()
    {
      return drop_flag_;
    }
    inline void set_cleanup_type(int type)
    {
      cleanup_rule_type_ = type;
    }
    inline int get_cleanup_type()
    {
      return cleanup_rule_type_;
    }
    enum CLEANUP_RULE {
      CTAS_RULE,           // Query cleanup rules for table creation
      TEMP_TAB_RULE,       // Cleanup rules for temporary tables (direct connection)
      TEMP_TAB_PROXY_RULE  // Temporary table cleanup rules (PROXY)
    };

  private:
    ObServer* obs_;
    uint64_t session_id_;     // Determine whether the sesion_id of the table schema needs to be dropped
    int64_t schema_version_;  // Determine whether the version number of the table schema that needs to be dropped
    bool drop_flag_;          // Do you need a drop table
    int cleanup_rule_type_;   // According to the temporary table rules or query table building rules
    DISALLOW_COPY_AND_ASSIGN(ObCTASCleanUp);
  };
  storage::ObPartitionService& get_partition_service();
  share::schema::ObMultiVersionSchemaService& get_schema_service()
  {
    return schema_service_;
  }
  ObInOutBandwidthThrottle& get_bandwidth_throttle()
  {
    return bandwidth_throttle_;
  }
  const common::ObAddr& get_self() const
  {
    return self_addr_;
  }
  const ObGlobalContext& get_gctx() const
  {
    return gctx_;
  }
  ObGlobalContext& get_gctx()
  {
    return gctx_;
  }
  ObSrvNetworkFrame& get_net_frame()
  {
    return net_frame_;
  }
  int reload_config();
  bool is_log_dir_empty() const
  {
    return is_log_dir_empty_;
  }
  sql::ObSQLSessionMgr& get_sql_session_mgr()
  {
    return session_mgr_;
  }
  rootserver::ObRootService& get_root_service()
  {
    return root_service_;
  }
  obrpc::ObCommonRpcProxy& get_common_rpc_proxy()
  {
    return rs_rpc_proxy_;
  }
  common::ObMySQLProxy& get_mysql_proxy()
  {
    return sql_proxy_;
  }

private:
  int stop();

private:
  ObServer();
  ~ObServer();

  int init_config();
  int init_tz_info_mgr();
  int init_pre_setting();
  int init_network();
  int init_interrupt();
  int init_multi_tenant();
  int init_sql_proxy();
  int init_io();
  int init_restore_ctx();
  int init_schema();
  int init_inner_table_monitor();
  int init_autoincrement_service();
  int init_global_kvcache();
  int init_global_session_info();
  int init_ob_service();
  int init_root_service();
  int init_sql();
  int init_sql_runner();
  int init_sequence();
  int init_pl();
  int init_global_context();
  int init_cluster_version();
  int init_ts_mgr();
  int init_gts();
  int start_gts();
  int stop_gts();
  int wait_gts();
  int init_gts_cache_mgr();
  int init_storage();
  int init_bandwidth_throttle();
  int init_gc_partition_adapter();
  int reload_bandwidth_throttle_limit();
  int init_loaddata_global_stat();

  int clean_up_invalid_tables();
  int init_ctas_clean_up_task();  // Regularly clean up the residuals related to querying and building tables and
                                  // temporary tables
  int refresh_temp_table_sess_active_time();
  int init_refresh_active_time_task();  // Regularly update the sess_active_time of the temporary table created by the
                                        // proxy connection sess
  int set_running_mode();
  int check_server_can_start_service();

public:
  volatile bool need_ctas_cleanup_;  // true: ObCTASCleanUpTask should traverse all table schemas to find the one need
                                     // be dropped
private:
  // thread to deal signals
  ObSignalHandle signal_handle_;

  // self addr
  common::ObAddr self_addr_;
  bool stop_;
  volatile bool has_stopped_;
  bool has_destroy_;
  ObServerOptions opts_;
  // The network framework in OceanBase is all defined at ObServerNetworkFrame.
  ObSrvNetworkFrame net_frame_;
  obrpc::ObBatchRpc batch_rpc_;

  ObInnerSQLConnectionPool sql_conn_pool_;
  sqlclient::ObDbLinkConnectionPool dblink_conn_pool_;
  ObRestoreCtx restore_ctx_;

  // The two proxies by which local OceanBase server has ability to
  // communicate with other server.
  obrpc::ObSrvRpcProxy srv_rpc_proxy_;
  obrpc::ObCommonRpcProxy rs_rpc_proxy_;
  common::ObMySQLProxy sql_proxy_;
  share::ObRemoteServerProvider remote_server_provider_;
  share::ObRemoteSqlProxy remote_sql_proxy_;
  common::ObDbLinkProxy dblink_proxy_;
  obrpc::ObExecutorRpcProxy executor_proxy_;
  sql::ObExecutorRpcImpl executor_rpc_;
  obrpc::ObInterruptRpcProxy interrupt_proxy_;  // global interrupt
  obrpc::ObLoadDataRpcProxy load_data_proxy_;

  // The OceanBase configuration relating to.
  common::ObServerConfig& config_;
  ObServerReloadConfig reload_config_;
  common::ObConfigManager config_mgr_;
  omt::ObTenantConfigMgr& tenant_config_mgr_;
  omt::ObTenantTimezoneMgr& tenant_timezone_mgr_;

  // data stat. related classes.
  common::ObColStatService user_col_stat_service_;
  common::ObTableStatService user_table_stat_service_;
  common::ObOptStatService opt_stat_service_;

  // The Oceanbase schema relating to.
  share::schema::ObMultiVersionSchemaService& schema_service_;

  // The Oceanbase rootserver manager
  share::ObRsMgr rs_mgr_;

  // The SQL Engine
  sql::ObSql sql_engine_;

  // The Oceanbase partition table relating to
  share::ObPartitionTableOperator pt_operator_;
  share::ObRemotePartitionTableOperator remote_pt_operator_;
  share::ObLocationFetcher location_fetcher_;
  share::ObAliveServerTracer server_tracer_;
  share::ObPartitionLocationCache location_cache_;

  // storage related
  storage::ObPartitionComponentFactory partition_cfy_;
  transaction::ObGtsResponseRpc gts_response_rpc_;
  transaction::ObGlobalTimestampService gts_;
  common::ObInOutBandwidthThrottle bandwidth_throttle_;
  int64_t sys_bkgd_net_percentage_;
  int64_t ethernet_speed_;

  // sql session_mgr
  sql::ObSQLSessionMgr session_mgr_;
  sql::ObQueryExecCtxMgr query_ctx_mgr_;

  // All operations and processing logic relating to root server is
  // defined in root_service_.
  rootserver::ObRootService root_service_;
  // Start && stop root service.

  ObRootServiceMonitor root_service_monitor_;

  // All operations and processing logic relating to ob server is
  // defined in oceanbase_service_.
  ObService ob_service_;

  // multi tenant interface object
  omt::ObWorkerProcessor procor_;
  omt::ObMultiTenant multi_tenant_;

  // virtual table related
  ObVirtualDataAccessService vt_data_service_;

  // calculate suitable cache size
  ObCacheSizeCalculator cache_size_calculator_;

  // Weakly Consistent Read Service
  transaction::ObWeakReadService weak_read_service_;

  // Tenant isolation resource management
  omt::ObCgroupCtrl cgroup_ctrl_;

  // gctx, aka global context, stores pointers to objects or services
  // which should share with all, or in part, of classes using in
  // observer. The whole pointers stored in gctx wouldn't be changed
  // once they're assigned. So other class can only get a reference of
  // constant gctx.
  ObGlobalContext gctx_;

  // observer start time
  int64_t start_time_;
  // zone merged version
  int64_t zone_merged_version_;
  // global last merged version
  int64_t global_last_merged_version_;
  int64_t warm_up_start_time_;
  common::ObString sort_dir_;
  obmysql::ObDiag diag_;
  common::ObMysqlRandom scramble_rand_;
  ObTenantDutyTask duty_task_;
  ObTenantSqlMemoryTimerTask sql_mem_task_;
  storage::ObPurgeCompletedMonitorInfoTask long_ops_task_;
  ObCTASCleanUpTask ctas_clean_up_task_;        // repeat & no retry
  ObRefreshTimeTask refresh_active_time_task_;  // repeat & no retry
  blocksstable::ObStorageEnv storage_env_;
  share::ObSchemaStatusProxy schema_status_proxy_;
  ObSignalWorker sig_worker_;

  bool is_log_dir_empty_;
};  // end of class ObServer

inline ObServer& ObServer::get_instance()
{
  static ObServer THE_ONE;
  return THE_ONE;
}

}  // end of namespace observer
}  // end of namespace oceanbase

#define OBSERVER (::oceanbase::observer::ObServer::get_instance())
#define MYADDR (OBSERVER.get_self())

#endif /* _OCEABASE_OBSERVER_OB_SERVER_H_ */
