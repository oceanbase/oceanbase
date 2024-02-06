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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_SERVICE_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_SERVICE_H_

#include "lib/net/ob_addr.h"
#include "lib/thread/ob_work_queue.h"

#include "share/ob_common_rpc_proxy.h"
#include "share/ob_tenant_id_schema_version.h"
#include "share/ob_inner_config_root_addr.h"
#include "share/ob_root_addr_agent.h"
#include "share/ob_web_service_root_addr.h"
#include "share/ob_unit_replica_counter.h"
#include "share/ob_ls_id.h"

#include "rpc/ob_packet.h"
#include "observer/ob_restore_ctx.h"
#include "rootserver/restore/ob_restore_scheduler.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_all_server_task.h"
#include "rootserver/ob_all_server_checker.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_root_minor_freeze.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_vtable_location_getter.h"
#include "rootserver/ob_root_balancer.h"
#include "rootserver/ob_system_admin_util.h"
#include "rootserver/ob_root_inspection.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_rs_thread_checker.h"
#include "rootserver/ob_snapshot_info_manager.h"
#include "rootserver/ob_upgrade_storage_format_version_executor.h"
#include "rootserver/ob_upgrade_executor.h"
#include "rootserver/ob_upgrade_storage_format_version_executor.h"
#include "rootserver/ob_create_inner_schema_executor.h"
#include "rootserver/ob_update_rs_list_task.h"
#include "rootserver/ob_schema_history_recycler.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "share/ls/ob_ls_info.h"
#include "share/ls/ob_ls_table_operator.h"
#include "rootserver/ob_disaster_recovery_task_mgr.h"
#include "rootserver/ob_disaster_recovery_task_executor.h"
#include "rootserver/ob_empty_server_checker.h"
#include "rootserver/ob_lost_replica_checker.h"
#include "rootserver/ob_server_zone_op_service.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "rootserver/ob_rs_master_key_manager.h"
#endif

namespace oceanbase
{

namespace common
{
class ObPacket;
class ObServerConfig;
class ObConfigManager;
class ObMySQLProxy;
class ObRequestTZInfoResult;
class ObRequestTZInfoArg;
class ObString;
}

namespace share
{

namespace status
{
enum ObRootServiceStatus;
}
class ObAutoincrementService;
namespace schema
{
class ObMultiVersionSchemaService;
class ObTenantSchema;
class ObDatabaseSchema;
class ObTablegroupSchema;
class ObTableSchema;
class ObSchemaGetterGuard;
}
class ObLSTableOperator;
class ObRsMgr;
}

namespace obrpc
{
class ObSrvRpcProxy;
class ObCommonRpcProxy;
struct ObGetSwitchoverStatusRes;
}
namespace storage
{
class ObMajorFreeze;
}

namespace rootserver
{
class ObRsStatus
{
public:
  ObRsStatus() : rs_status_(share::status::INIT) {}
  virtual ~ObRsStatus() {}
  int set_rs_status(const share::status::ObRootServiceStatus status);
  share::status::ObRootServiceStatus get_rs_status() const;
  bool need_do_restart() const;
  bool can_start_service() const;
  bool is_start() const;
  bool is_stopping() const;
  bool is_full_service() const;
  bool in_service() const;
  bool is_need_stop() const;
  int revoke_rs();
  int try_set_stopping();
private:
  common::SpinRWLock lock_;
  share::status::ObRootServiceStatus rs_status_;
};
// Root Service Entry Class
class ObRootService
{
public:
  friend class TestRootServiceCreateTable_check_rs_capacity_Test;
  friend class ObTenantWrsTask;
  class ObStartStopServerTask : public share::ObAsyncTask
  {
  public:
    ObStartStopServerTask(ObRootService &root_service,
                          const common::ObAddr &server,
                          const bool start)
      : root_service_(root_service), server_(server), start_(start) {}
    virtual ~ObStartStopServerTask() {}
    virtual int process();
    virtual int64_t get_deep_copy_size() const;
    share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const;
  private:
    ObRootService &root_service_;
    const common::ObAddr server_;
    const bool start_;
  };

  class ObOfflineServerTask : public share::ObAsyncTask
  {
  public:
    ObOfflineServerTask(ObRootService &root_service,
                          const common::ObAddr &server)
      : root_service_(root_service), server_(server) { set_retry_times(0); /*not repeat*/ }
    virtual ~ObOfflineServerTask() {}
    virtual int process();
    virtual int64_t get_deep_copy_size() const;
    share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const;
  private:
    ObRootService &root_service_;
    const common::ObAddr server_;
  };

  class ObRefreshServerTask : public common::ObAsyncTimerTask
  {
  public:
    const static int64_t REFRESH_SERVER_INTERVAL = 1L * 1000 * 1000; // 1 second
    explicit ObRefreshServerTask(ObRootService &root_service);
    virtual ~ObRefreshServerTask() {};
    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
    virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  private:
    ObRootService &root_service_;
    DISALLOW_COPY_AND_ASSIGN(ObRefreshServerTask);
  };

  class ObStatusChangeCallback : public ObIStatusChangeCallback
  {
  public:
    explicit ObStatusChangeCallback(ObRootService &root_service);
    virtual ~ObStatusChangeCallback();

    virtual int wakeup_balancer() override;
    virtual int wakeup_daily_merger() override;
    virtual int on_server_status_change(const common::ObAddr &server) override;
    virtual int on_start_server(const common::ObAddr &server) override;
    virtual int on_stop_server(const common::ObAddr &server) override;
    virtual int on_offline_server(const common::ObAddr &server) override;

  private:
    ObRootService &root_service_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObStatusChangeCallback);
  };

  class ObServerChangeCallback : public ObIServerChangeCallback
  {
  public:
    explicit ObServerChangeCallback(ObRootService &root_service) : root_service_(root_service) {}
    virtual ~ObServerChangeCallback() {}

    virtual int on_server_change() override;

  private:
    ObRootService &root_service_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObServerChangeCallback);
  };


  class ObRestartTask : public common::ObAsyncTimerTask
  {
  public:
    explicit ObRestartTask(ObRootService &root_service);
    virtual ~ObRestartTask();

    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
    virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  private:
    ObRootService &root_service_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObRestartTask);
  };

  class ObReportCoreTableReplicaTask : public common::ObAsyncTimerTask
  {
  public:
    explicit ObReportCoreTableReplicaTask(ObRootService &root_service);
    virtual ~ObReportCoreTableReplicaTask() {}
  public:
    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
    virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  private:
    ObRootService &root_service_;
    DISALLOW_COPY_AND_ASSIGN(ObReportCoreTableReplicaTask);
  };

  class ObReloadUnitManagerTask : public common::ObAsyncTimerTask
  {
  public:
    explicit ObReloadUnitManagerTask(ObRootService &root_service, ObUnitManager &unit_manager);
    virtual ~ObReloadUnitManagerTask() {}
  public:
    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
    virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  private:
    ObRootService &root_service_;
    ObUnitManager &unit_manager_;
    DISALLOW_COPY_AND_ASSIGN(ObReloadUnitManagerTask);
  };

  class ObLoadDDLTask : public common::ObAsyncTimerTask
  {
  public:
    explicit ObLoadDDLTask(ObRootService &root_service);
    virtual ~ObLoadDDLTask() = default;
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
    virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  private:
    ObRootService &root_service_;
  };

  class ObRefreshIOCalibrationTask : public common::ObAsyncTimerTask
  {
  public:
    explicit ObRefreshIOCalibrationTask(ObRootService &root_service);
    virtual ~ObRefreshIOCalibrationTask() = default;
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
    virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  private:
    ObRootService &root_service_;
  };

  class ObUpdateAllServerConfigTask : public common::ObAsyncTimerTask
  {
  public:
    const static int64_t RETRY_INTERVAL = 600 * 1000L * 1000L;  // 10min
    explicit ObUpdateAllServerConfigTask(ObRootService &root_service);
    virtual ~ObUpdateAllServerConfigTask() {}
  public:
    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
    virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  private:
    ObRootService &root_service_;
    DISALLOW_COPY_AND_ASSIGN(ObUpdateAllServerConfigTask);
  };

  class ObSelfCheckTask : public common::ObAsyncTimerTask
  {
  public:
    explicit ObSelfCheckTask(ObRootService &root_service);
    virtual ~ObSelfCheckTask() {};
    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
    virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  private:
    void print_error_log();
  private:
    ObRootService &root_service_;
    DISALLOW_COPY_AND_ASSIGN(ObSelfCheckTask);
  };

  class RsListChangeCb: public share::ObIRsListChangeCb
  {
  public:
    explicit RsListChangeCb(ObRootService &rs)
        :rs_(rs) {}
    virtual ~RsListChangeCb() {}
    // interface of ObIRsListChangeCb
    virtual int submit_update_rslist_task(const bool force_update = false) override { return rs_.submit_update_rslist_task(force_update); }
    virtual int submit_report_replica() override { return rs_.report_replica(); }
    virtual int submit_report_replica(const int64_t tenant_id, const share::ObLSID &ls_id) override
    { return rs_.report_single_replica(tenant_id, ls_id); }
  private:
    ObRootService &rs_;
    DISALLOW_COPY_AND_ASSIGN(RsListChangeCb);
  };

  class ObMinorFreezeTask : public share::ObAsyncTask
  {
  public:
    explicit ObMinorFreezeTask(const obrpc::ObRootMinorFreezeArg &arg) : arg_(arg) {}
    virtual ~ObMinorFreezeTask() {}
    virtual int process();
    virtual int64_t get_deep_copy_size() const;
    share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const;
  private:
    obrpc::ObRootMinorFreezeArg arg_;
  };
  class ObTenantGlobalContextCleanTimerTask : private common::ObTimerTask
  {
  public:
    explicit ObTenantGlobalContextCleanTimerTask(ObRootService &root_service);
    virtual ~ObTenantGlobalContextCleanTimerTask() {};
    int schedule(int tg_id);
  private:
    void runTimerTask() override;
  private:
    static constexpr int64_t SCHEDULE_PERIOD = 3600 * 1000L * 1000L; // 1h
    ObRootService &root_service_;
  };

public:
  ObRootService();
  virtual ~ObRootService();
  void reset_fail_count();
  void update_fail_count(int ret);
  int fake_init(common::ObServerConfig &config, common::ObConfigManager &config_mgr,
           obrpc::ObSrvRpcProxy &rpc_proxy, obrpc::ObCommonRpcProxy &common_proxy,
           common::ObAddr &self, common::ObMySQLProxy &sql_proxy, share::ObRsMgr &rs_mgr,
           share::schema::ObMultiVersionSchemaService *schema_mgr_,
	   share::ObLSTableOperator &lst_operator_);

  int init(common::ObServerConfig &config, common::ObConfigManager &config_mgr,
           obrpc::ObSrvRpcProxy &rpc_proxy, obrpc::ObCommonRpcProxy &common_proxy,
           common::ObAddr &self, common::ObMySQLProxy &sql_proxy,
           observer::ObRestoreCtx &restore_ctx,
           share::ObRsMgr &rs_mgr, share::schema::ObMultiVersionSchemaService *schema_mgr_,
	   share::ObLSTableOperator &lst_operator_);
  inline bool is_inited() const { return inited_; }
  void destroy();

  // add virtual make the following functions mockable
  virtual int start_service();
  int revoke_rs();
  bool is_need_stop() const;
  virtual int stop_service();
  virtual int stop();
  virtual void wait();
  virtual bool in_service() const;
  bool need_do_restart() const;
  int set_rs_status(const share::status::ObRootServiceStatus status);
  virtual bool is_full_service() const;
  virtual bool is_major_freeze_done() const { return is_full_service(); }
  virtual bool is_ddl_allowed() const { return is_full_service(); }
  bool can_start_service() const;
  bool is_stopping() const;
  bool is_start() const;
  share::status::ObRootServiceStatus get_status() const;
  bool in_debug() const { return debug_; }
  void set_debug() { debug_ = true; }
  int reload_config();
  virtual bool check_config(const ObConfigItem &item, const char *&err_info);
  // misc get functions
  share::ObLSTableOperator &get_lst_operator() { return *lst_operator_; }
  share::schema::ObMultiVersionSchemaService &get_schema_service() { return *schema_service_; }
  ObServerManager &get_server_mgr() { return server_manager_; }
  ObZoneManager &get_zone_mgr() { return zone_manager_; }
  ObUnitManager &get_unit_mgr() { return unit_manager_; }
  ObRootBalancer &get_root_balancer() { return root_balancer_; }
  ObRootInspection &get_root_inspection() { return root_inspection_; }
  common::ObMySQLProxy &get_sql_proxy() { return sql_proxy_; }
  common::ObOracleSqlProxy &get_oracle_sql_proxy() { return oracle_sql_proxy_; }
  obrpc::ObCommonRpcProxy &get_common_rpc_proxy() { return common_proxy_; }
  obrpc::ObSrvRpcProxy &get_rpc_proxy() { return rpc_proxy_; }
  common::ObWorkQueue &get_task_queue() { return task_queue_; }
  common::ObWorkQueue &get_inspect_task_queue() { return inspect_task_queue_; }
  common::ObServerConfig *get_server_config() { return config_; }
  ObDDLScheduler &get_ddl_task_scheduler() { return ddl_scheduler_; }
  int64_t get_core_meta_table_version() { return core_meta_table_version_; }
  ObSchemaHistoryRecycler &get_schema_history_recycler() { return schema_history_recycler_; }
  ObRootMinorFreeze &get_root_minor_freeze() { return root_minor_freeze_; }
  int admin_clear_balance_task(const obrpc::ObAdminClearBalanceTaskArg &arg);
  int generate_user(const ObClusterRole &cluster_role, const char* usr_name, const char* user_passwd);
  int check_server_have_enough_resource_for_delete_server(
      const ObIArray<ObAddr> &servers,
      const ObZone &zone);

  // not implemented rpc, helper function for rs rpc processor define.
  int not_implement();

  int execute_bootstrap(const obrpc::ObBootstrapArg &arg);
#ifdef OB_BUILD_TDE_SECURITY
  int check_sys_tenant_initial_master_key_valid();
#endif

  int check_config_result(const char *name, const char *value);
  int check_ddl_allowed();

  int renew_lease(const share::ObLeaseRequest &lease_request,
                  share::ObLeaseResponse &lease_response);
  int report_sys_ls(const share::ObLSReplica &replica);
  int remove_sys_ls(const obrpc::ObRemoveSysLsArg &arg);
  int fetch_location(const obrpc::ObFetchLocationArg &arg,
                     obrpc::ObFetchLocationResult &res);
  int merge_finish(const obrpc::ObMergeFinishArg &arg);

  // 4.0 backup
  // balance over
  int receive_backup_over(const obrpc::ObBackupTaskRes &res);
  int receive_backup_clean_over(const obrpc::ObBackupTaskRes &res);

  int broadcast_ds_action(const obrpc::ObDebugSyncActionArg &arg);
  int check_dangling_replica_finish(const obrpc::ObCheckDanglingReplicaFinishArg &arg);
  int fetch_alive_server(const obrpc::ObFetchAliveServerArg &arg,
      obrpc::ObFetchAliveServerResult &result);
  int get_tenant_schema_versions(const obrpc::ObGetSchemaArg &arg,
                                 obrpc::ObTenantSchemaVersions &tenant_schema_versions);

  // ddl related
  int create_resource_unit(const obrpc::ObCreateResourceUnitArg &arg);
  int alter_resource_unit(const obrpc::ObAlterResourceUnitArg &arg);
  int drop_resource_unit(const obrpc::ObDropResourceUnitArg &arg);
  int create_resource_pool(const obrpc::ObCreateResourcePoolArg &arg);
  int alter_resource_pool(const obrpc::ObAlterResourcePoolArg &arg);
  int drop_resource_pool(const obrpc::ObDropResourcePoolArg &arg);
  int split_resource_pool(const obrpc::ObSplitResourcePoolArg &arg);
  int merge_resource_pool(const obrpc::ObMergeResourcePoolArg &arg);
  int alter_resource_tenant(const obrpc::ObAlterResourceTenantArg &arg);
  int create_tenant(const obrpc::ObCreateTenantArg &arg, obrpc::UInt64 &tenant_id);
  int create_tenant_end(const obrpc::ObCreateTenantEndArg &arg);
  int commit_alter_tenant_locality(const rootserver::ObCommitAlterTenantLocalityArg &arg);
  int drop_tenant(const obrpc::ObDropTenantArg &arg);
  int flashback_tenant(const obrpc::ObFlashBackTenantArg &arg);
  int purge_tenant(const obrpc::ObPurgeTenantArg &arg);
  int modify_tenant(const obrpc::ObModifyTenantArg &arg);
  int lock_tenant(const obrpc::ObLockTenantArg &arg);
  int modify_system_variable(const obrpc::ObModifySysVarArg &arg);
  int add_system_variable(const obrpc::ObAddSysVarArg &arg);
  int create_database(const obrpc::ObCreateDatabaseArg &arg, obrpc::UInt64 &db_id);
  int create_tablegroup(const obrpc::ObCreateTablegroupArg &arg, obrpc::UInt64 &tg_id);
  int handle_security_audit(const obrpc::ObSecurityAuditArg &arg);
  int parallel_create_table(const obrpc::ObCreateTableArg &arg, obrpc::ObCreateTableRes &res);
  int create_table(const obrpc::ObCreateTableArg &arg, obrpc::ObCreateTableRes &res);
  int alter_database(const obrpc::ObAlterDatabaseArg &arg);
  int alter_table(const obrpc::ObAlterTableArg &arg, obrpc::ObAlterTableRes &res);
  int start_redef_table(const obrpc::ObStartRedefTableArg &arg, obrpc::ObStartRedefTableRes &res);
  int copy_table_dependents(const obrpc::ObCopyTableDependentsArg &arg);
  int finish_redef_table(const obrpc::ObFinishRedefTableArg &arg);
  int abort_redef_table(const obrpc::ObAbortRedefTableArg &arg);
  int update_ddl_task_active_time(const obrpc::ObUpdateDDLTaskActiveTimeArg &arg);
  int create_hidden_table(const obrpc::ObCreateHiddenTableArg &arg, obrpc::ObCreateHiddenTableRes &res);
  /**
   * For recover restore table ddl, data insert into the target table is selected from another tenant.
   * The function is used to create a hidden target table without any change on the source table,
   * and then register a recover task into ddl task queue to finish the all procedures.
   * The format about the command is,
   * alter system recover table test.t1 to tenant backup_oracle_tenant from '$ARCHIVE_FILES_PATH' with 'pool_list=small_pool_0&primary_zone=z1' remap table test.t1:recover_test.t3;
  */
  int recover_restore_table_ddl(const obrpc::ObRecoverRestoreTableDDLArg &arg);
  int execute_ddl_task(const obrpc::ObAlterTableArg &arg, common::ObSArray<uint64_t> &obj_ids);
  int cancel_ddl_task(const obrpc::ObCancelDDLTaskArg &arg);
  int alter_tablegroup(const obrpc::ObAlterTablegroupArg &arg);
  int maintain_obj_dependency_info(const obrpc::ObDependencyObjDDLArg &arg);
  int rename_table(const obrpc::ObRenameTableArg &arg);
  int truncate_table(const obrpc::ObTruncateTableArg &arg, obrpc::ObDDLRes &res);
  int truncate_table_v2(const obrpc::ObTruncateTableArg &arg, obrpc::ObDDLRes &res);
  int create_index(const obrpc::ObCreateIndexArg &arg, obrpc::ObAlterTableRes &res);
  int drop_table(const obrpc::ObDropTableArg &arg, obrpc::ObDDLRes &res);
  int drop_database(const obrpc::ObDropDatabaseArg &arg, obrpc::ObDropDatabaseRes &drop_database_res);
  int drop_tablegroup(const obrpc::ObDropTablegroupArg &arg);
  int drop_index(const obrpc::ObDropIndexArg &arg, obrpc::ObDropIndexRes &res);
  int rebuild_index(const obrpc::ObRebuildIndexArg &arg, obrpc::ObAlterTableRes &res);
  //the interface only for switchover: execute skip check enable_ddl
  int flashback_index(const obrpc::ObFlashBackIndexArg &arg);
  int purge_index(const obrpc::ObPurgeIndexArg &arg);
  int create_table_like(const obrpc::ObCreateTableLikeArg &arg);
  int refresh_config();
  int root_minor_freeze(const obrpc::ObRootMinorFreezeArg &arg);
  int update_index_status(const obrpc::ObUpdateIndexStatusArg &arg);
  int purge_table(const obrpc::ObPurgeTableArg &arg);
  int flashback_table_from_recyclebin(const obrpc::ObFlashBackTableFromRecyclebinArg &arg);
  int flashback_table_to_time_point(const obrpc::ObFlashBackTableToScnArg &arg);
  int purge_database(const obrpc::ObPurgeDatabaseArg &arg);
  int flashback_database(const obrpc::ObFlashBackDatabaseArg &arg);
  int check_tenant_in_alter_locality(const uint64_t tenant_id, bool &in_alter_locality);

  int create_restore_point(const obrpc::ObCreateRestorePointArg &arg);
  int drop_restore_point(const obrpc::ObDropRestorePointArg &arg);

  //for inner table monitor, purge in fixed time
  int purge_expire_recycle_objects(const obrpc::ObPurgeRecycleBinArg &arg, obrpc::Int64 &affected_rows);
  int calc_column_checksum_repsonse(const obrpc::ObCalcColumnChecksumResponseArg &arg);
  int build_ddl_single_replica_response(const obrpc::ObDDLBuildSingleReplicaResponseArg &arg);
  int optimize_table(const obrpc::ObOptimizeTableArg &arg);

  //----Functions for managing privileges----
  int create_user(obrpc::ObCreateUserArg &arg,
                  common::ObSArray<int64_t> &failed_index);
  int drop_user(const obrpc::ObDropUserArg &arg,
                common::ObSArray<int64_t> &failed_index);
  int rename_user(const obrpc::ObRenameUserArg &arg,
                  common::ObSArray<int64_t> &failed_index);
  int set_passwd(const obrpc::ObSetPasswdArg &arg);
  int grant(const obrpc::ObGrantArg &arg);
  int revoke_user(const obrpc::ObRevokeUserArg &arg);
  int lock_user(const obrpc::ObLockUserArg &arg, common::ObSArray<int64_t> &failed_index);
  int revoke_database(const obrpc::ObRevokeDBArg &arg);
  int revoke_table(const obrpc::ObRevokeTableArg &arg);
  int revoke_syspriv(const obrpc::ObRevokeSysPrivArg &arg);
  int alter_user_profile(const obrpc::ObAlterUserProfileArg &arg);
  int alter_role(const obrpc::ObAlterRoleArg &arg);
  //----End of functions for managing privileges----

  //----Functions for managing outlines----
  int create_outline(const obrpc::ObCreateOutlineArg &arg);
  int alter_outline(const obrpc::ObAlterOutlineArg &arg);
  int drop_outline(const obrpc::ObDropOutlineArg &arg);
  //----End of functions for managing outlines----

  //----Functions for managing schema revise----
  int schema_revise(const obrpc::ObSchemaReviseArg &arg);
  //----End of functions for managing schema revise----

  //----Functions for managing UDF----
  int create_user_defined_function(const obrpc::ObCreateUserDefinedFunctionArg &arg);
  int drop_user_defined_function(const obrpc::ObDropUserDefinedFunctionArg &arg);
  //----End of functions for managing UDF----

  //----Functions for managing routines----
  int create_routine(const obrpc::ObCreateRoutineArg &arg);
  int drop_routine(const obrpc::ObDropRoutineArg &arg);
  int alter_routine(const obrpc::ObCreateRoutineArg &arg);
  //----End of functions for managing routines----

  //----Functions for managing routines----
  int create_udt(const obrpc::ObCreateUDTArg &arg);
  int drop_udt(const obrpc::ObDropUDTArg &arg);
  //----End of functions for managing routines----

  //----Functions for managing dblinks----
  int create_dblink(const obrpc::ObCreateDbLinkArg &arg);
  int drop_dblink(const obrpc::ObDropDbLinkArg &arg);
  //----End of functions for managing dblinks----

  //----Functions for managing synonyms----
  int create_synonym(const obrpc::ObCreateSynonymArg &arg);
  int drop_synonym(const obrpc::ObDropSynonymArg &arg);
  //----End of functions for managing synonyms----

#ifdef OB_BUILD_SPM
  //----Functions for managing plan_baselines----
  int accept_plan_baseline(const obrpc::ObModifyPlanBaselineArg &arg);
  int cancel_evolve_task(const obrpc::ObModifyPlanBaselineArg &arg);
  int admin_load_baseline(const obrpc::ObLoadPlanBaselineArg &arg);
  int admin_load_baseline_v2(const obrpc::ObLoadPlanBaselineArg &arg, obrpc::ObLoadBaselineRes &res);
  // int drop_plan_baseline(const obrpc::ObDropPlanBaselineArg &arg);
  //----End of functions for managing plan_baselines----
#endif

  //----Functions for sync rewrite rules----
  int admin_sync_rewrite_rules(const obrpc::ObSyncRewriteRuleArg &arg);
  //----End of functions for sync rewrite rules----

  //----Functions for managing package----
  int create_package(const obrpc::ObCreatePackageArg &arg);
  int alter_package(const obrpc::ObAlterPackageArg &arg);
  int drop_package(const obrpc::ObDropPackageArg &arg);
  //----End of functions for managing package----

  //----Functions for managing trigger----
  int create_trigger(const obrpc::ObCreateTriggerArg &arg);
  int alter_trigger(const obrpc::ObAlterTriggerArg &arg);
  int drop_trigger(const obrpc::ObDropTriggerArg &arg);
  //----End of functions for managing trigger----

  //----Functions for managing sequence----
  // create alter drop actions all in one, avoid noodle-like code
  int do_sequence_ddl(const obrpc::ObSequenceDDLArg &arg);
  //----End of functions for managing sequence----

  //----Functions for managing context----
  // create alter drop actions all in one, avoid noodle-like code
  int do_context_ddl(const obrpc::ObContextDDLArg &arg);
  //----End of functions for managing context----

   //----Functions for managing keystore----
  int do_keystore_ddl(const obrpc::ObKeystoreDDLArg &arg);
  //----End of functions for managing keystore----

  //----Functions for managing label security policies----
  int handle_label_se_policy_ddl(const obrpc::ObLabelSePolicyDDLArg &arg);
  int handle_label_se_component_ddl(const obrpc::ObLabelSeComponentDDLArg &arg);
  int handle_label_se_label_ddl(const obrpc::ObLabelSeLabelDDLArg &arg);
  int handle_label_se_user_level_ddl(const obrpc::ObLabelSeUserLevelDDLArg &arg);
  //----End of functions for managing label security policies----
  // for tablespace
  int do_tablespace_ddl(const obrpc::ObTablespaceDDLArg &arg);

  //----Functions for managing profile----
  int do_profile_ddl(const obrpc::ObProfileDDLArg &arg);
  //----End of functions for managing sequence----

  //----Functions for directory object----
  int create_directory(const obrpc::ObCreateDirectoryArg &arg);
  int drop_directory(const obrpc::ObDropDirectoryArg &arg);
  //----End of functions for directory object----

  //----Functions for managing row level security----
  int handle_rls_policy_ddl(const obrpc::ObRlsPolicyDDLArg &arg);
  int handle_rls_group_ddl(const obrpc::ObRlsGroupDDLArg &arg);
  int handle_rls_context_ddl(const obrpc::ObRlsContextDDLArg &arg);
  //----End of functions for managing row level security----

  // server related
  int load_server_manager();
  ObStatusChangeCallback &get_status_change_cb() { return status_change_cb_; }
  int add_server(const obrpc::ObAdminServerArg &arg);
  int add_server_for_bootstrap_in_version_smaller_than_4_2_0(
      const common::ObAddr &server,
      const common::ObZone &zone);
  int delete_server(const obrpc::ObAdminServerArg &arg);
  int cancel_delete_server(const obrpc::ObAdminServerArg &arg);
  int start_server(const obrpc::ObAdminServerArg &arg);
  int stop_server(const obrpc::ObAdminServerArg &arg);
  // Check if all ls has leader
  // @param [in] print_str: string of operation. Used to print LOG_USER_ERROR "'print_str' not allowed".
  int check_all_ls_has_leader(const char *print_str);
  // zone related
  int add_zone(const obrpc::ObAdminZoneArg &arg);
  int delete_zone(const obrpc::ObAdminZoneArg &arg);
  int start_zone(const obrpc::ObAdminZoneArg &arg);
  int stop_zone(const obrpc::ObAdminZoneArg &arg);
  int alter_zone(const obrpc::ObAdminZoneArg &arg);
  int check_can_stop(
      const common::ObZone &zone,
      const common::ObIArray<common::ObAddr> &servers,
      const bool is_stop_zone);
  // Check if all ls has leader, enough member and if log is in sync.
  // @param [in] to_stop_servers: server_list to be stopped.
  // @param [in] skip_log_sync_check: whether skip log_sync check.
  // @param [in] print_str: string of operation. Used to print LOG_USER_ERROR "'print_str' not allowed".
  // @return: OB_SUCCESS if all check is passed.
  //          OB_OP_NOT_ALLOW if ls doesn't have leader/enough member or ls' log is not in sync.
  int check_majority_and_log_in_sync(
      const ObIArray<ObAddr> &to_stop_servers,
      const bool skip_log_sync_check,
      const char *print_str);

  // system admin command (alter system ...)
  int admin_switch_replica_role(const obrpc::ObAdminSwitchReplicaRoleArg &arg);
  int admin_switch_rs_role(const obrpc::ObAdminSwitchRSRoleArg &arg);
  int admin_drop_replica(const obrpc::ObAdminDropReplicaArg &arg);
  int admin_change_replica(const obrpc::ObAdminChangeReplicaArg &arg);
  int admin_migrate_replica(const obrpc::ObAdminMigrateReplicaArg &arg);
  int admin_report_replica(const obrpc::ObAdminReportReplicaArg &arg);
  int admin_recycle_replica(const obrpc::ObAdminRecycleReplicaArg &arg);
  int admin_merge(const obrpc::ObAdminMergeArg &arg);
  int admin_recovery(const obrpc::ObAdminRecoveryArg &arg);
  int admin_clear_roottable(const obrpc::ObAdminClearRoottableArg &arg);
  int admin_refresh_schema(const obrpc::ObAdminRefreshSchemaArg &arg);
  int admin_set_config(obrpc::ObAdminSetConfigArg &arg);
  int admin_clear_location_cache(const obrpc::ObAdminClearLocationCacheArg &arg);
  int admin_refresh_memory_stat(const obrpc::ObAdminRefreshMemStatArg &arg);
  int admin_wash_memory_fragmentation(const obrpc::ObAdminWashMemFragmentationArg &arg);
  int admin_refresh_io_calibration(const obrpc::ObAdminRefreshIOCalibrationArg &arg);
  int admin_reload_unit();
  int admin_reload_server();
  int admin_reload_zone();
  int admin_clear_merge_error(const obrpc::ObAdminMergeArg &arg);
#ifdef OB_BUILD_ARBITRATION
  int admin_add_arbitration_service(const obrpc::ObAdminAddArbitrationServiceArg &arg);
  int admin_remove_arbitration_service(const obrpc::ObAdminRemoveArbitrationServiceArg &arg);
  int admin_replace_arbitration_service(const obrpc::ObAdminReplaceArbitrationServiceArg &arg);
  int remove_cluster_info_from_arb_server(const obrpc::ObRemoveClusterInfoFromArbServerArg &arg);
#endif
  int admin_migrate_unit(const obrpc::ObAdminMigrateUnitArg &arg);
  int admin_upgrade_virtual_schema();
  int run_job(const obrpc::ObRunJobArg &arg);
  int run_upgrade_job(const obrpc::ObUpgradeJobArg &arg);
  int upgrade_table_schema(const obrpc::ObUpgradeTableSchemaArg &arg);
  int admin_flush_cache(const obrpc::ObAdminFlushCacheArg &arg);
  int admin_upgrade_cmd(const obrpc::Bool &arg);
  int admin_rolling_upgrade_cmd(const obrpc::ObAdminRollingUpgradeArg &arg);
  int admin_set_tracepoint(const obrpc::ObAdminSetTPArg &arg);
  int admin_set_backup_config(const obrpc::ObAdminSetConfigArg &arg);
  /* physical restore */
  int physical_restore_tenant(const obrpc::ObPhysicalRestoreTenantArg &arg, obrpc::Int64 &job_id);
  int check_restore_tenant_valid(const share::ObPhysicalRestoreJob &job_info,
      share::schema::ObSchemaGetterGuard &guard);
  int rebuild_index_in_restore(const obrpc::ObRebuildIndexInRestoreArg &arg);
  /*-----------------*/
  int refresh_time_zone_info(const obrpc::ObRefreshTimezoneArg &arg);
  int request_time_zone_info(const common::ObRequestTZInfoArg &arg, common::ObRequestTZInfoResult &result);
  // async tasks and callbacks
  // @see ObStatusChangeCallback
  int submit_update_all_server_task(const common::ObAddr &server);
  int submit_start_server_task(const common::ObAddr &server);
  int submit_stop_server_task(const common::ObAddr &server);
  int submit_offline_server_task(const common::ObAddr &server);
  int submit_report_core_table_replica_task();
  int submit_reload_unit_manager_task();
  int report_replica();
  int report_single_replica(const int64_t tenant_id, const share::ObLSID &ls_id);
  // @see RsListChangeCb
  int submit_update_rslist_task(const bool force_update = false);
  int submit_upgrade_task(const obrpc::ObUpgradeJobArg &arg);
  int submit_upgrade_storage_format_version_task();
  int submit_create_inner_schema_task();
  int submit_update_all_server_config_task();
  int submit_max_availability_mode_task(const common::ObProtectionLevel level, const int64_t cluster_version);

  int submit_ddl_single_replica_build_task(share::ObAsyncTask &task);
  int check_weak_read_version_refresh_interval(int64_t refresh_interval, bool &valid);
  // may modify arg before taking effect
  int set_config_pre_hook(obrpc::ObAdminSetConfigArg &arg);
  // arg is readonly after take effect
  int set_config_post_hook(const obrpc::ObAdminSetConfigArg &arg);

  // @see ObRestartTask
  int after_restart();
  int do_after_full_service();
  int schedule_restart_timer_task(const int64_t delay);
  int schedule_self_check_task();
  int schedule_temporary_offline_timer_task();
  // @see ObCheckServerTask
  int schedule_check_server_timer_task();
  // @see ObRefreshServerTask
  int schedule_refresh_server_timer_task(const int64_t delay);
  int schedule_primary_cluster_inspection_task();
  int schedule_recyclebin_task(int64_t delay);
  // @see ObInspector
  int schedule_inspector_task();
  int schedule_update_rs_list_task();
  int schedule_update_all_server_config_task();
  //update statistic cache
  int update_stat_cache(const obrpc::ObUpdateStatCacheArg &arg);

  int schedule_load_ddl_task();
  int schedule_refresh_io_calibration_task();
  // ob_admin command, must be called in ddl thread
  int force_create_sys_table(const obrpc::ObForceCreateSysTableArg &arg);
  int force_set_locality(const obrpc::ObForceSetLocalityArg &arg);
  int generate_stop_server_log_in_sync_dest_server_array(
      const common::ObIArray<common::ObAddr> &alive_server_array,
      const common::ObIArray<common::ObAddr> &excluded_server_array,
      common::ObIArray<common::ObAddr> &dest_server_array);
  int log_nop_operation(const obrpc::ObDDLNopOpreatorArg &arg);
  int broadcast_schema(const obrpc::ObBroadcastSchemaArg &arg);
  ObDDLService &get_ddl_service() { return ddl_service_; }
  ObDDLScheduler &get_ddl_scheduler() { return ddl_scheduler_; }
  int get_recycle_schema_versions(
      const obrpc::ObGetRecycleSchemaVersionsArg &arg,
      obrpc::ObGetRecycleSchemaVersionsResult &result);
  int handle_archive_log(const obrpc::ObArchiveLogArg &arg);
  int handle_backup_database(const obrpc::ObBackupDatabaseArg &arg);
  int handle_backup_manage(const obrpc::ObBackupManageArg &arg);
  int handle_backup_delete(const obrpc::ObBackupCleanArg &arg);
  int handle_delete_policy(const obrpc::ObDeletePolicyArg &arg);
  int handle_validate_database(const obrpc::ObBackupManageArg &arg);
  int handle_validate_backupset(const obrpc::ObBackupManageArg &arg);
  int handle_cancel_validate(const obrpc::ObBackupManageArg &arg);
  int handle_recover_table(const obrpc::ObRecoverTableArg &arg);
  int disaster_recovery_task_reply(const obrpc::ObDRTaskReplyResult &arg);
  int standby_upgrade_virtual_schema(const obrpc::ObDDLNopOpreatorArg &arg);
  int check_backup_scheduler_working(obrpc::Bool &is_working);
  int send_physical_restore_result(const obrpc::ObPhysicalRestoreResult &res);
  int get_is_in_bootstrap(bool &is_bootstrap) const;
  int purge_recyclebin_objects(int64_t purge_each_time);
  int flush_opt_stat_monitoring_info(const obrpc::ObFlushOptStatArg &arg);
  int update_rslist();
  int recompile_all_views_batch(const obrpc::ObRecompileAllViewsBatchArg &arg);
  int try_add_dep_infos_for_synonym_batch(const obrpc::ObTryAddDepInofsForSynonymBatchArg &arg);
#ifdef OB_BUILD_TDE_SECURITY
  int handle_get_root_key(const obrpc::ObRootKeyArg &arg, obrpc::ObRootKeyResult &result);
  int get_root_key_from_obs(const obrpc::ObRootKeyArg &arg, obrpc::ObRootKeyResult &result);
#endif
private:
#ifdef OB_BUILD_TDE_SECURITY
  int try_check_encryption_zone_cond(
      const obrpc::ObAdminZoneArg &arg);
#endif
  int check_parallel_ddl_conflict(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const obrpc::ObDDLArg &arg);
  int fetch_sys_tenant_ls_info();
  // create system table in mysql backend for debugging mode.
  int init_debug_database();
  int do_restart();
  int refresh_server(const bool fast_recover, const bool need_retry);
  int refresh_schema(const bool fast_recover);
  int init_sequence_id();
  int start_timer_tasks();
  int stop_timer_tasks();
  int request_heartbeats();
  int self_check();
  int update_all_server_and_rslist();
  int init_sys_admin_ctx(ObSystemAdminCtx &ctx);
  int set_cluster_version();
  bool is_replica_count_reach_rs_limit(int64_t replica_count) { return replica_count > OB_MAX_CLUSTER_REPLICA_COUNT; }
  int update_all_server_config();
  int generate_table_schema_in_tenant_space(
      const obrpc::ObCreateTableArg &arg,
      share::schema::ObTableSchema &table_schema);
  int clear_special_cluster_schema_status();
  int check_tenant_gts_config(const int64_t tenant_id, bool &tenant_gts_config_ok,
                              share::schema::ObSchemaGetterGuard &schema_guard);
  int check_database_config(const int64_t tenant_id, bool &db_config_ok,
                            share::schema::ObSchemaGetterGuard &schema_guard);
  int check_table_config(const int64_t tenant_id, bool &table_config_ok, bool &table_split_ok,
                         share::schema::ObSchemaGetterGuard &schema_guard,
                         const int64_t snapshot_schema_version);
  int check_tablegroup_config(const int64_t tenant_id, bool &tablegroup_config_ok,
                              bool &tablegroup_split_ok,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              const int64_t snapshot_schema_version);
  int check_restore_tenant_after_major_freeze(ObArray<ObString> &not_allow_reasons,
                                              const int64_t snapshot_schema_version,
                                              const ObArray<uint64_t> &tenant_ids);
  int get_tenants_created_after_snapshot(const int64_t snapshot_schema_version,
                                         ObArray<uint64_t> &tenant_ids);
  int query_ddl_table_after_major_freeze(int &row_cnt, int64_t &schema_version_cursor,
                                         ObArray<uint64_t> &tenant_ids);
  bool continue_check(const int ret);
  int handle_backup_database_cancel(const obrpc::ObBackupManageArg &arg);
  inline static bool cmp_tenant_id(const uint64_t lhs, const uint64_t tenant_id) {
    return lhs < tenant_id;
  }
  int handle_cancel_backup_backup(const obrpc::ObBackupManageArg &arg);
  int handle_cancel_all_backup_force(const obrpc::ObBackupManageArg &arg);
  int clean_global_context();

  bool is_sys_tenant(const common::ObString &tenant_name);
  int table_allow_ddl_operation(const obrpc::ObAlterTableArg &arg);
  int get_table_schema(uint64_t tenant_id,
                       const common::ObString &database_name,
                       const common::ObString &table_name,
                       const bool is_index,
                       const int64_t session_id,
                       const share::schema::ObTableSchema *&table_schema);
  int update_baseline_schema_version();
  int finish_bootstrap();
  void construct_lease_expire_time(
       const share::ObLeaseRequest &lease_request,
       share::ObLeaseResponse &lease_response,
       const share::ObServerStatus &server_status);
  void update_cpu_quota_concurrency_in_memory_();
  int set_cpu_quota_concurrency_config_();
  int try_notify_switch_leader(const obrpc::ObNotifySwitchLeaderArg::SwitchLeaderComment &comment);

  int precheck_interval_part(const obrpc::ObAlterTableArg &arg);
  int old_add_server(const obrpc::ObAdminServerArg &arg);
  int old_delete_server(const obrpc::ObAdminServerArg &arg);
  int old_cancel_delete_server(const obrpc::ObAdminServerArg &arg);

  int parallel_ddl_pre_check_(const uint64_t tenant_id);
private:
  static const int64_t OB_MAX_CLUSTER_REPLICA_COUNT = 10000000;
  static const int64_t OB_ROOT_SERVICE_START_FAIL_COUNT_UPPER_LIMIT = 5;
  bool inited_;
  volatile bool server_refreshed_; // server manager reload and force request heartbeat
  // use mysql server backend for debug.
  bool debug_;

  common::ObAddr self_addr_;
  common::ObServerConfig *config_;
  common::ObConfigManager *config_mgr_;

  obrpc::ObSrvRpcProxy rpc_proxy_;
  obrpc::ObCommonRpcProxy common_proxy_;
  common::ObMySQLProxy sql_proxy_;
  common::ObOracleSqlProxy oracle_sql_proxy_;
  observer::ObRestoreCtx *restore_ctx_;
  share::ObRsMgr *rs_mgr_;
  share::schema::ObMultiVersionSchemaService *schema_service_;

  // server manager related
  ObStatusChangeCallback status_change_cb_;
  ObServerChangeCallback server_change_callback_;
  ObServerManager server_manager_;
  ObHeartbeatChecker hb_checker_;
  ObAllServerChecker server_checker_;
  RsListChangeCb rs_list_change_cb_;
  ObServerZoneOpService server_zone_op_service_;

  // minor freeze
  ObRootMinorFreeze root_minor_freeze_;

  // partition table related
  share::ObLSTableOperator *lst_operator_;

  ObZoneManager zone_manager_;

  // ddl related
  ObDDLService ddl_service_;
  ObUnitManager unit_manager_;

  ObRootBalancer root_balancer_;

  // empty server checker
  ObEmptyServerChecker empty_server_checker_;

  //check lost LS replica
  ObLostReplicaChecker lost_replica_checker_;
  // thread checker
  ObRsThreadChecker thread_checker_;

  // virtual table related
  ObVTableLocationGetter vtable_location_getter_;

  share::ObUnifiedAddrAgent *addr_agent_;

  // upgrade inspection
  ObRootInspection root_inspection_;

  // upgrade post job
  ObUpgradeExecutor upgrade_executor_;

  // upgrade storage format version
  ObUpgradeStorageFormatVersionExecutor upgrade_storage_format_executor_;

  // create inner oracle role(for upgrade)
  ObCreateInnerSchemaExecutor create_inner_schema_executor_;

  // avoid concurrent run of do_restart and bootstrap
  common::ObLatch bootstrap_lock_;

  common::SpinRWLock broadcast_rs_list_lock_;

  // the single task queue for all async tasks and timer tasks
  common::ObWorkQueue task_queue_;
  common::ObWorkQueue inspect_task_queue_;

  // async timer tasks
  ObRestartTask restart_task_;  // not repeat & no retry
  ObRefreshServerTask refresh_server_task_;  // not repeat & no retry
  ObCheckServerTask check_server_task_;      // repeat & no retry
  ObSelfCheckTask self_check_task_;  //repeat to succeed & no retry
  ObLoadDDLTask load_ddl_task_; // repeat to succeed & no retry
  ObRefreshIOCalibrationTask refresh_io_calibration_task_; // retry to succeed & no repeat
  share::ObEventTableClearTask event_table_clear_task_;  // repeat & no retry

  ObInspector inspector_task_;     // repeat & no retry
  ObPurgeRecyclebinTask purge_recyclebin_task_;     // not repeat & no retry
  // for set_config
  ObLatch set_config_lock_;

  ObDDLScheduler ddl_scheduler_;
  share::ObDDLReplicaBuilder ddl_builder_;
  ObSnapshotInfoManager snapshot_manager_;
  int64_t core_meta_table_version_;
  ObUpdateRsListTimerTask update_rs_list_timer_task_;
  ObUpdateAllServerConfigTask update_all_server_config_task_;
  int64_t baseline_schema_version_;

  int64_t start_service_time_;
  ObRsStatus rs_status_;

  int64_t fail_count_;
  ObSchemaHistoryRecycler schema_history_recycler_;
#ifdef OB_BUILD_TDE_SECURITY
  // master key manager
  ObRsMasterKeyManager master_key_mgr_;
#endif
  // Disaster Recovery related
  ObDRTaskExecutor disaster_recovery_task_executor_;
  ObDRTaskMgr disaster_recovery_task_mgr_;
  // application context
  ObTenantGlobalContextCleanTimerTask global_ctx_task_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRootService);
};
} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ROOT_SERVICE_H_
