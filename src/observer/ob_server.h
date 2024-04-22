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

#include <sys/statvfs.h>
#include "lib/signal/ob_signal_worker.h"
#include "lib/net/ob_net_util.h"
#include "lib/random/ob_mysql_random.h"
#include "lib/container/ob_iarray.h"


#include "share/stat/ob_opt_stat_service.h"
#include "share/ratelimit/ob_rl_mgr.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/tablet/ob_tablet_table_operator.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_alive_server_tracer.h"
#include "diagnose/lua/ob_lua_handler.h"

#include "sql/ob_sql.h"
#include "sql/engine/cmd/ob_load_data_rpc.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/session/ob_user_resource_mgr.h"
#include "sql/executor/ob_executor_rpc_impl.h"

#include "pl/ob_pl.h"

#include "storage/tx/wrs/ob_weak_read_service.h"         // ObWeakReadService
#include "storage/tx/wrs/ob_black_list.h"

#include "rootserver/ob_root_service.h"

#include "observer/mysql/ob_diag.h"

#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_worker_processor.h"

#include "observer/virtual_table/ob_virtual_data_access_service.h"

#include "observer/ob_signal_handle.h"
#include "observer/ob_tenant_duty_task.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_resource_inner_sql_connection_pool.h"
#include "observer/ob_srv_network_frame.h"
#include "observer/ob_service.h"
#include "observer/ob_server_reload_config.h"
#include "observer/ob_root_service_monitor.h"
#include "observer/table/ob_table_service.h"
#include "observer/dbms_job/ob_dbms_job_rpc_proxy.h"
#include "observer/ob_inner_sql_rpc_proxy.h"
#include "observer/ob_startup_accel_task_handler.h"
#include "share/ls/ob_ls_table_operator.h" // for ObLSTableOperator
#include "storage/ob_locality_manager.h"
#include "storage/ddl/ob_ddl_heart_beat_task.h"

#include "storage/ob_disk_usage_reporter.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_rpc_proxy.h"
#include "logservice/ob_server_log_block_mgr.h"
#ifdef OB_BUILD_ARBITRATION
#include "logservice/arbserver/ob_arb_srv_garbage_collect_service.h"
#include "logservice/arbserver/ob_arb_server_timer.h"
#endif

#include "share/table/ob_table_rpc_proxy.h"
#include "share/wr/ob_wr_service.h"

#include "sql/engine/table/ob_external_table_access_service.h"
#include "share/external_table/ob_external_table_file_rpc_proxy.h"

namespace oceanbase
{
namespace omt
{
class ObTenantTimezoneMgr;
}
namespace share
{
class ObAliveServerTracer;
}
namespace observer
{
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
class ObServer
{
public:
  static const int64_t DEFAULT_ETHERNET_SPEED = 10000 / 8 * 1024 * 1024; // change from default 125m/s  1000Mbit to 1250MBps 10000Mbit
  static const int64_t DISK_USAGE_REPORT_INTERVAL = 1000L * 1000L * 300L; // 5min
  static const uint64_t DEFAULT_CPU_FREQUENCY = 2500 * 1000; // 2500 * 1000 khz
  static ObServer &get_instance();

public:
  int init(const ObServerOptions &opts, const ObPLogWriterCfg &log_cfg);
  void destroy();

  // Start OceanBase server, this function is blocked after invoking
  // until the server itself stops it.
  int start();
  int wait();
  void prepare_stop();
  bool is_prepare_stopped();
  void set_stop();
  bool is_stopped();

public:
  //Refer to ObPurgeCompletedMonitorInfoTask
  class ObCTASCleanUpTask: public common::ObTimerTask
  {
  public:
    ObCTASCleanUpTask();
    virtual ~ObCTASCleanUpTask() {}
    int init(ObServer *observer, int tg_id);
    void destroy();
    virtual void runTimerTask() override;
  private:
    const static int64_t CLEANUP_INTERVAL = 60L * 1000L * 1000L;//60s
    ObServer *obs_;
    bool is_inited_;
  };

  class ObRefreshTimeTask: public common::ObTimerTask
  {
  public:
    ObRefreshTimeTask();
    virtual ~ObRefreshTimeTask() {}
    int init(ObServer *observer, int tg_id);
    void destroy();
    virtual void runTimerTask() override;
  private:
    const static int64_t REFRESH_INTERVAL = 60L * 60L * 1000L * 1000L;//1hr
    ObServer *obs_;
    bool is_inited_;
  };

  class ObRefreshNetworkSpeedTask: public common::ObTimerTask
  {
  public:
    ObRefreshNetworkSpeedTask();
    virtual ~ObRefreshNetworkSpeedTask() {}
    int init(ObServer *observer, int tg_id);
    void destroy();
    virtual void runTimerTask() override;
  private:
    const static int64_t REFRESH_INTERVAL = 1L * 1000L * 1000L;//1hr
    ObServer *obs_;
    bool is_inited_;
  };

  class ObRefreshCpuFreqTimeTask: public common::ObTimerTask
  {
  public:
    ObRefreshCpuFreqTimeTask();
    virtual ~ObRefreshCpuFreqTimeTask() {}
    int init(ObServer *observer, int tg_id);
    void destroy();
    virtual void runTimerTask() override;
  private:
    const static int64_t REFRESH_INTERVAL = 10 * 1000L * 1000L;//10s
    ObServer *obs_;
    bool is_inited_;
  };

  class ObRefreshIOCalibrationTimeTask: public common::ObTimerTask
  {
  public:
    ObRefreshIOCalibrationTimeTask();
    virtual ~ObRefreshIOCalibrationTimeTask() {}
    int init(ObServer *observer, int tg_id);
    void destroy();
    virtual void runTimerTask() override;
  private:
    const static int64_t REFRESH_INTERVAL = 10 * 1000L * 1000L;//10s
    ObServer *obs_;
    int tg_id_;
    bool is_inited_;
  };

  class ObRefreshTime {
  public:
    explicit ObRefreshTime(ObServer *obs): obs_(obs){}
    virtual ~ObRefreshTime(){}
    bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info);
  private:
    ObServer *obs_;
    DISALLOW_COPY_AND_ASSIGN(ObRefreshTime);
  };

  class ObCTASCleanUp
  {
  public:
    explicit ObCTASCleanUp(ObServer *obs, bool drop_flag): obs_(obs), session_id_(0), 
              schema_version_(0), drop_flag_(drop_flag), cleanup_rule_type_(0) {}
    virtual ~ObCTASCleanUp(){}
    bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info);
    inline void set_session_id(uint64_t  session_id) {session_id_ = session_id; }
    inline uint64_t get_session_id() { return session_id_; }
    inline void set_schema_version(int64_t  schema_version) {schema_version_ = schema_version; }
    inline int64_t get_schema_version() { return schema_version_; }
    inline void set_drop_flag(bool drop_flag) { drop_flag_ = drop_flag; }
    inline bool get_drop_flag() { return drop_flag_; }
    inline void set_cleanup_type(int type) { cleanup_rule_type_ = type; }
    inline int get_cleanup_type() { return cleanup_rule_type_; }
    enum CLEANUP_RULE
    {
      CTAS_RULE,          //Query cleanup rules for table creation
      TEMP_TAB_RULE,      //Cleanup rules for temporary tables (direct connection)
      TEMP_TAB_PROXY_RULE //Temporary table cleanup rules (PROXY)
    };
  private:
    ObServer *obs_;
    uint64_t session_id_;      //Determine whether the sesion_id of the table schema needs to be dropped
    int64_t schema_version_;  //Determine whether the version number of the table schema that needs to be dropped
    bool drop_flag_;           //Do you need a drop table
    int cleanup_rule_type_;    //According to the temporary table rules or query table building rules
    DISALLOW_COPY_AND_ASSIGN(ObCTASCleanUp);
  };
  share::schema::ObMultiVersionSchemaService &get_schema_service() { return schema_service_; }
  ObInOutBandwidthThrottle &get_bandwidth_throttle() { return bandwidth_throttle_; }
  uint64_t get_cpu_frequency_khz() { return cpu_frequency_; }
  int64_t get_network_speed() const { return ethernet_speed_; }
  const common::ObAddr &get_self() const { return self_addr_; }
  const ObGlobalContext &get_gctx() const { return gctx_; }
  ObGlobalContext &get_gctx() { return gctx_; }
  ObSrvNetworkFrame& get_net_frame() { return net_frame_; }
  share::ObRatelimitMgr& get_rl_mgr() { return rl_mgr_; }
  int reload_config();
  bool is_log_dir_empty() const { return is_log_dir_empty_; }
  sql::ObSQLSessionMgr &get_sql_session_mgr() { return session_mgr_; }
  rootserver::ObRootService &get_root_service() { return root_service_; }
  obrpc::ObCommonRpcProxy &get_common_rpc_proxy() { return rs_rpc_proxy_; }
  common::ObMySQLProxy &get_mysql_proxy() { return sql_proxy_; }
  int64_t get_start_time() const { return start_time_; }
  sql::ObConnectResourceMgr& get_conn_res_mgr() { return conn_res_mgr_; }
  obrpc::ObTableRpcProxy &get_table_rpc_proxy() { return table_rpc_proxy_; }
  share::ObLocationService &get_location_service() { return location_service_; }
  bool is_arbitration_mode() const;
private:
  int stop();

private:
  ObServer();
  ~ObServer();

  int init_config();
  int init_opts_config(bool has_config_file); // init configs from command line
  int init_local_ip_and_devname();
  int init_self_addr();
  int init_config_module();
  int init_tz_info_mgr();
  int init_pre_setting();
  int init_network();
  int init_interrupt();
  int init_zlib_lite_compressor();
  int init_multi_tenant();
  int init_sql_proxy();
  int init_io();
  int init_restore_ctx();
  int init_schema();
  int init_inner_table_monitor();
  int init_autoincrement_service();
  int init_tablet_autoincrement_service();
  int init_global_kvcache();
  int init_global_session_info();
  int init_ob_service();
  int init_root_service();
  int init_sql();
  int init_sql_runner();
  int init_sequence();
  int init_pl();
  int init_global_context();
  int init_version();
  int init_ts_mgr();
  int init_px_target_mgr();
  int init_storage();
  int init_tx_data_cache();
  int init_log_kv_cache();
  int init_gc_partition_adapter();
  int init_loaddata_global_stat();
  int init_bandwidth_throttle();
  int init_table_lock_rpc_client();
  int start_log_mgr();
  int stop_log_mgr();
  int reload_bandwidth_throttle_limit(int64_t network_speed);
  int get_network_speed_from_sysfs(int64_t &network_speed);
  int get_network_speed_from_config_file(int64_t &network_speed);
  int refresh_network_speed();
  int refresh_cpu_frequency();
  int refresh_io_calibration();
  int clean_up_invalid_tables();
  int clean_up_invalid_tables_by_tenant(const uint64_t tenant_id);
  int init_ctas_clean_up_task(); //Regularly clean up the residuals related to querying and building tables and temporary tables
  int init_redef_heart_beat_task();
  int init_ddl_heart_beat_task_container();
  int refresh_temp_table_sess_active_time();
  int init_refresh_active_time_task(); //Regularly update the sess_active_time of the temporary table created by the proxy connection sess
  int init_refresh_network_speed_task();
  int init_refresh_cpu_frequency();
  int init_refresh_io_calibration();
  int set_running_mode();
  void check_user_tenant_schema_refreshed(const common::ObIArray<uint64_t> &tenant_ids, const int64_t expire_time);
  void check_log_replay_over(const common::ObIArray<uint64_t> &tenant_ids, const int64_t expire_time);
  int try_update_hidden_sys();
  int check_if_multi_tenant_synced();
  int check_if_schema_ready();
  int check_if_timezone_usable();
  int parse_mode();
  void deinit_zlib_lite_compressor();

  // ------------------------------- arb server start ------------------------------------
  int start_sig_worker_and_handle();
  int init_server_in_arb_mode();
  int start_server_in_arb_mode();
  int stop_server_in_arb_mode();
  int wait_server_in_arb_mode();
  int destroy_server_in_arb_mode();
  // ------------------------------- arb server end --------------------------------------

public:
  volatile bool need_ctas_cleanup_; //true: ObCTASCleanUpTask should traverse all table schemas to find the one need be dropped
private:
  //thread to deal signals
  char sig_buf_[sizeof(ObSignalWorker) + sizeof(ObSignalHandle)] __attribute__((__aligned__(16)));
  ObSignalWorker *sig_worker_;
  ObSignalHandle *signal_handle_;

  // self addr
  common::ObAddr self_addr_;
  bool prepare_stop_;
  bool stop_;
  volatile bool has_stopped_;
  bool has_destroy_;
  ObServerOptions opts_;
  // The network framework in OceanBase is all defined at ObServerNetworkFrame.
  ObSrvNetworkFrame net_frame_;
  obrpc::ObBatchRpc batch_rpc_;
  share::ObRatelimitMgr rl_mgr_;

  ObInnerSQLConnectionPool sql_conn_pool_;
  ObInnerSQLConnectionPool ddl_conn_pool_;
  sqlclient::ObDbLinkConnectionPool dblink_conn_pool_;
  ObResourceInnerSQLConnectionPool res_inner_conn_pool_;
  ObRestoreCtx restore_ctx_;

  // The two proxies by which local OceanBase server has ability to
  // communicate with other server.
  obrpc::ObSrvRpcProxy srv_rpc_proxy_;
  obrpc::ObStorageRpcProxy storage_rpc_proxy_;
  obrpc::ObCommonRpcProxy rs_rpc_proxy_;
  common::ObMySQLProxy sql_proxy_;
  common::ObMySQLProxy ddl_sql_proxy_;
  common::ObOracleSqlProxy ddl_oracle_sql_proxy_;
  common::ObDbLinkProxy dblink_proxy_;
  obrpc::ObExecutorRpcProxy executor_proxy_;
  sql::ObExecutorRpcImpl executor_rpc_;
  obrpc::ObDBMSJobRpcProxy dbms_job_rpc_proxy_;
  obrpc::ObInnerSQLRpcProxy inner_sql_rpc_proxy_;
  obrpc::ObDBMSSchedJobRpcProxy dbms_sched_job_rpc_proxy_;
  obrpc::ObInterruptRpcProxy interrupt_proxy_; // global interrupt
  obrpc::ObLoadDataRpcProxy load_data_proxy_;
  obrpc::ObTableRpcProxy table_rpc_proxy_;
  obrpc::ObExtenralTableRpcProxy external_table_proxy_;

  // The OceanBase configuration relating to.
  common::ObServerConfig &config_;
  ObServerReloadConfig reload_config_;
  common::ObConfigManager config_mgr_;
  omt::ObTenantConfigMgr &tenant_config_mgr_;
  omt::ObTenantTimezoneMgr &tenant_timezone_mgr_;

  // The Oceanbase schema relating to.
  share::schema::ObMultiVersionSchemaService &schema_service_;

  // The Oceanbase rootserver manager
  share::ObRsMgr rs_mgr_;

  // The SQL Engine
  sql::ObSql sql_engine_;

  // The PL Engine
  pl::ObPL pl_engine_;

  // The Oceanbase partition table relating to
  share::ObLSTableOperator lst_operator_;
  share::ObTabletTableOperator tablet_operator_;
  share::ObAliveServerTracer server_tracer_;
  share::ObLocationService location_service_;

  // storage related
  common::ObInOutBandwidthThrottle bandwidth_throttle_;
  int64_t sys_bkgd_net_percentage_;
  int64_t ethernet_speed_;
  uint64_t cpu_frequency_;

  // sql session_mgr
  sql::ObSQLSessionMgr session_mgr_;

  // All operations and processing logic relating to root server is
  // defined in root_service_.
  rootserver::ObRootService root_service_;
  // Start && stop root service.

  ObRootServiceMonitor root_service_monitor_;

  // All operations and processing logic relating to ob server is
  // defined in oceanbase_service_.
  ObService ob_service_;

  omt::ObMultiTenant multi_tenant_;

  // virtual table related
  ObVirtualDataAccessService vt_data_service_;
  // external table
  ObExternalTableAccessService et_access_service_;

  // Weakly Consistent Read Service
  transaction::ObWeakReadService  weak_read_service_;
  // blacklist service
  transaction::ObBLService &bl_service_;
  // table service
  ObTableService table_service_;

  // Tenant isolation resource management
  share::ObCgroupCtrl cgroup_ctrl_;

  // gctx, aka global context, stores pointers to objects or services
  // which should share with all, or in part, of classes using in
  // observer. The whole pointers stored in gctx wouldn't be changed
  // once they're assigned. So other class can only get a reference of
  // constant gctx.
  ObGlobalContext gctx_;

  //observer start time
  int64_t start_time_;
  int64_t warm_up_start_time_;
  obmysql::ObDiag diag_;
  common::ObMysqlRandom scramble_rand_;
  ObTenantDutyTask duty_task_;
  ObTenantSqlMemoryTimerTask sql_mem_task_;
  ObCTASCleanUpTask ctas_clean_up_task_;     // repeat & no retry
  ObRedefTableHeartBeatTask redef_table_heart_beat_task_;
  ObRefreshTimeTask refresh_active_time_task_; // repeat & no retry
  ObRefreshNetworkSpeedTask refresh_network_speed_task_; // repeat & no retry
  ObRefreshCpuFreqTimeTask refresh_cpu_frequency_task_;
  ObRefreshIOCalibrationTimeTask refresh_io_calibration_task_; // retry to success & no repeat
  blocksstable::ObStorageEnv storage_env_;
  share::ObSchemaStatusProxy schema_status_proxy_;

  // for locality
  ObLocalityManager locality_manager_;

  bool is_log_dir_empty_;
  sql::ObConnectResourceMgr conn_res_mgr_;
  diagnose::ObUnixDomainListener unix_domain_listener_;
  ObDiskUsageReportTask disk_usage_report_task_;

  logservice::ObServerLogBlockMgr log_block_mgr_;
#ifdef OB_BUILD_ARBITRATION
  arbserver::ObArbGarbageCollectService arb_gcs_;
  arbserver::ObArbServerTimer arb_timer_;
#endif
  share::ObWorkloadRepositoryService wr_service_;

  // This handler is used to process tasks during startup. it can speed up the startup process.
  // If you have tasks that need to be processed in parallel, you can use this handler,
  // but please note that this handler will be destroyed after observer startup.
  ObStartupAccelTaskHandler startup_accel_handler_;
}; // end of class ObServer

inline ObServer &ObServer::get_instance()
{
  static ObServer THE_ONE;
  return THE_ONE;
}

} // end of namespace observer
} // end of namespace oceanbase

#define OBSERVER (::oceanbase::observer::ObServer::get_instance())
#define MYADDR (OBSERVER.get_self())

#endif /* _OCEABASE_OBSERVER_OB_SERVER_H_ */
