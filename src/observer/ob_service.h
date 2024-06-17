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

#ifndef OCEANBASE_OBSERVER_OB_SERVICE_H_
#define OCEANBASE_OBSERVER_OB_SERVICE_H_


#include "share/ls/ob_ls_table.h"
#include "share/ob_all_server_tracer.h"
#include "observer/ob_lease_state_mgr.h"
#include "observer/ob_heartbeat.h"
#include "observer/ob_server_schema_updater.h"
#include "observer/ob_rpc_processor_simple.h"
#include "observer/ob_uniq_task_queue.h"
#include "observer/report/ob_i_meta_report.h"
#include "observer/report/ob_ls_table_updater.h"
#include "observer/report/ob_tablet_table_updater.h"
#include "observer/report/ob_server_meta_table_checker.h" // ObServerMetaTableChecker

namespace oceanbase
{
namespace share
{
class ObIAliveServerTracer;
struct ObTabletReplicaChecksumItem;
}
namespace storage
{
struct ObFrozenStatus;
class ObLS;
}
namespace observer
{
class ObServer;
class ObServerInstance;
class ObRemoteLocationGetter;

class ObSchemaReleaseTimeTask: public common::ObTimerTask
{
public:
  ObSchemaReleaseTimeTask();
  virtual ~ObSchemaReleaseTimeTask() {}
  int init(ObServerSchemaUpdater &schema_updater, int tg_id);
  void destroy();
  virtual void runTimerTask() override;
private:
  int schedule_();
private:
  ObServerSchemaUpdater *schema_updater_;
  bool is_inited_;
};

class ObRemoteMasterRsUpdateTask : public common::ObTimerTask
{
public:
  ObRemoteMasterRsUpdateTask(const ObGlobalContext &gctx);
  virtual ~ObRemoteMasterRsUpdateTask() {}
  int init(int tg_id);
  void destroy() {}
  virtual void runTimerTask() override;
private:
  const static int64_t REFRESH_INTERVAL = 10L * 60L * 1000L * 1000L; // 10min
  const ObGlobalContext &gctx_;
  bool is_inited_;
};

class ObService : public ObIMetaReport
{
public:
  explicit ObService(const ObGlobalContext &gctx);
  virtual ~ObService();

  int init(common::ObMySQLProxy &sql_proxy,
           share::ObIAliveServerTracer &server_tracer);
  int start();
  void set_stop();
  void stop();
  void wait();
  int destroy();

  //fill_tablet_replica: to build a tablet replica locally
  // @params[in] tenant_id: tablet belongs to which tenant
  // @params[in] ls_id: tablet belongs to which log stream
  // @params[in] tablet_id: the tablet to build
  // @params[out] tablet_replica: infos about this tablet replica
  // @params[out] tablet_checksum: infos about this tablet data/column checksum
  // @params[in] need_checksum: whether to fill tablet_checksum
  // ATTENTION: If ls not exist, then OB_LS_NOT_EXIST
  //            If tablet not exist on that ls, then OB_TABLET_NOT_EXIST
  int fill_tablet_report_info(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      share::ObTabletReplica &tablet_replica,
      share::ObTabletReplicaChecksumItem &tablet_checksum,
      const bool need_checksum = true);

  int detect_master_rs_ls(const obrpc::ObDetectMasterRsArg &arg,
                       obrpc::ObDetectMasterRsLSResult &result);
  int fill_ls_replica(const uint64_t tenant_id,
                              const share::ObLSID &ls_id,
                              share::ObLSReplica &replica);
  int update_baseline_schema_version(const int64_t schema_version);
  virtual const common::ObAddr &get_self_addr();
  //////////////////////////////// ObIMetaReport interfaces ////////////////////////////////
  virtual int submit_ls_update_task(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id) override;

  ////////////////////////////////////////////////////////////////
  int check_frozen_scn(const obrpc::ObCheckFrozenScnArg &arg);
  int get_min_sstable_schema_version(
      const obrpc::ObGetMinSSTableSchemaVersionArg &arg,
      obrpc::ObGetMinSSTableSchemaVersionRes &result);
  // ObRpcSwitchSchemaP @RS DDL
  int switch_schema(const obrpc::ObSwitchSchemaArg &arg, obrpc::ObSwitchSchemaResult &result);
  int calc_column_checksum_request(const obrpc::ObCalcColumnChecksumRequestArg &arg, obrpc::ObCalcColumnChecksumRequestRes &res);
  int build_ddl_single_replica_request(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg, obrpc::ObDDLBuildSingleReplicaRequestResult &res);
  int check_and_cancel_ddl_complement_data_dag(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg, bool &is_dag_exist);
  int stop_partition_write(const obrpc::Int64 &switchover_timestamp, obrpc::Int64 &result);
  int check_partition_log(const obrpc::Int64 &switchover_timestamp, obrpc::Int64 &result);
  int get_wrs_info(const obrpc::ObGetWRSArg &arg, obrpc::ObGetWRSResult &result);
  int broadcast_consensus_version(
      const obrpc::ObBroadcastConsensusVersionArg &arg,
      obrpc::ObBroadcastConsensusVersionRes &result);
  ////////////////////////////////////////////////////////////////
  // ObRpcFetchSysLSP @RS load balance
  int fetch_sys_ls(share::ObLSReplica &replica);
  int backup_ls_data(const obrpc::ObBackupDataArg &arg);
  int backup_completing_log(const obrpc::ObBackupComplLogArg &arg);
  int backup_build_index(const obrpc::ObBackupBuildIdxArg &arg);
  int check_backup_dest_connectivity(const obrpc::ObCheckBackupConnectivityArg &arg);
  int backup_meta(const obrpc::ObBackupMetaArg &arg);
  int check_backup_task_exist(const obrpc::ObBackupCheckTaskArg &arg, bool &res);
  int check_sys_task_exist(const share::ObTaskId &arg, bool &res);
  int check_migrate_task_exist(const share::ObTaskId &arg, bool &res);
  int delete_backup_ls_task(const obrpc::ObLSBackupCleanArg &arg);
  int notify_archive(const obrpc::ObNotifyArchiveArg &arg);
  int report_backup_over(const obrpc::ObBackupTaskRes &res);
  int report_backup_clean_over(const obrpc::ObBackupTaskRes &res);

  int get_ls_sync_scn(const obrpc::ObGetLSSyncScnArg &arg,
                           obrpc::ObGetLSSyncScnRes &result);
  int force_set_ls_as_single_replica(const obrpc::ObForceSetLSAsSingleReplicaArg &arg);
  int refresh_tenant_info(const obrpc::ObRefreshTenantInfoArg &arg,
                          obrpc::ObRefreshTenantInfoRes &result);
  int get_ls_replayed_scn(const obrpc::ObGetLSReplayedScnArg &arg,
                          obrpc::ObGetLSReplayedScnRes &result);
  int estimate_partition_rows(const obrpc::ObEstPartArg &arg,
                              obrpc::ObEstPartRes &res) const;
  int estimate_tablet_block_count(const obrpc::ObEstBlockArg &arg,
                                  obrpc::ObEstBlockRes &res) const;
  int update_tenant_info_cache(const obrpc::ObUpdateTenantInfoCacheArg &arg,
                                  obrpc::ObUpdateTenantInfoCacheRes &result);
  ////////////////////////////////////////////////////////////////
  // ObRpcMinorFreezeP @RS minor freeze
  int minor_freeze(const obrpc::ObMinorFreezeArg &arg,
                   obrpc::Int64 &result);
  // ObRpcTabletMajorFreezeP @RS tablet major freeze
  int tablet_major_freeze(const obrpc::ObTabletMajorFreezeArg &arg,
                   obrpc::Int64 &result);
  // ObRpcCheckSchemaVersionElapsedP @RS global index builder
  int check_schema_version_elapsed(
      const obrpc::ObCheckSchemaVersionElapsedArg &arg,
      obrpc::ObCheckSchemaVersionElapsedResult &result);
  // ObRpcGetChecksumCalSnapshotP
  int check_modify_time_elapsed(
      const obrpc::ObCheckModifyTimeElapsedArg &arg,
      obrpc::ObCheckModifyTimeElapsedResult &result);

  int check_ddl_tablet_merge_status(
    const obrpc::ObDDLCheckTabletMergeStatusArg &arg,
    obrpc::ObDDLCheckTabletMergeStatusResult &result);

  ////////////////////////////////////////////////////////////////
  // ObRpcBatchSwitchRsLeaderP @RS leader coordinator & admin
  int batch_switch_rs_leader(const ObAddr &arg);
  // ObRpcGetPartitionCountP @RS leader coordinator
  int get_partition_count(obrpc::ObGetPartitionCountResult &result);

  ////////////////////////////////////////////////////////////////
  // ObRpcBootstrapP @RS bootstrap
  int bootstrap(const obrpc::ObBootstrapArg &arg);
  // ObRpcCheckServerForAddingServerP @RS add server
  int check_server_for_adding_server(
      const obrpc::ObCheckServerForAddingServerArg &arg,
      obrpc::ObCheckServerForAddingServerResult &result);
  // ObRpcGetServerStatusP @RS
  int get_server_resource_info(const obrpc::ObGetServerResourceInfoArg &arg, obrpc::ObGetServerResourceInfoResult &result);
  int get_server_resource_info(share::ObServerResourceInfo &resource_info);
  static int get_build_version(share::ObServerInfoInTable::ObBuildVersion &build_version);
  // log stream replica task related
  static int do_remove_ls_paxos_replica(const obrpc::ObLSDropPaxosReplicaArg &arg);
  static int do_remove_ls_nonpaxos_replica(const obrpc::ObLSDropNonPaxosReplicaArg &arg);
  static int do_add_ls_replica(const obrpc::ObLSAddReplicaArg &arg);
  // ObRpcIsEmptyServerP @RS bootstrap
  int is_empty_server(const obrpc::ObCheckServerEmptyArg &arg, obrpc::Bool &is_empty);
  // ObRpcCheckDeploymentModeP
  int check_deployment_mode_match(const obrpc::ObCheckDeploymentModeArg &arg, obrpc::Bool &match);
  int get_leader_locations(
      const obrpc::ObGetLeaderLocationsArg &arg,
      obrpc::ObGetLeaderLocationsResult &result);
  int batch_broadcast_schema(
      const obrpc::ObBatchBroadcastSchemaArg &arg,
      obrpc::ObBatchBroadcastSchemaResult &result);

  ////////////////////////////////////////////////////////////////
#ifdef OB_BUILD_TDE_SECURITY
  int wait_master_key_in_sync(const obrpc::ObWaitMasterKeyInSyncArg &wms_in_sync_arg);
  int trigger_tenant_config(const obrpc::ObWaitMasterKeyInSyncArg &wms_in_sync_arg);
  int do_wait_master_key_in_sync(
      const common::ObIArray<std::pair<uint64_t, uint64_t> > &got_version_array);
  int convert_tenant_max_key_version(
      const common::ObIArray<std::pair<uint64_t, share::ObLeaseResponse::TLRpKeyVersion> > &,
      common::ObIArray<std::pair<uint64_t, uint64_t> > &);
#endif
  // ObReportReplicaP @RS::admin to report replicas
  int report_replica();
  int load_leader_cluster_login_info();
  // ObRecycleReplicaP @RS::admin to recycle replicas
  int recycle_replica();
  // ObClearLocationCacheP @RS::admin to clear location cache
  int clear_location_cache();
  // ObDropReplicaP @RS::admin to drop replica
  int set_ds_action(const obrpc::ObDebugSyncActionArg &arg);
  // ObRequestHeartbeatP @RS::admin to cancel delete server
  int request_heartbeat(share::ObLeaseRequest &lease_requeset);
  int report_replica(const obrpc::ObReportSingleReplicaArg &arg);
  // ObSyncPartitionTableP @RS empty_server_checker
  int sync_partition_table(const obrpc::Int64 &arg);
  // ObRpcSetTPP @RS::admin to set tracepoint
  int set_tracepoint(const obrpc::ObAdminSetTPArg &arg);
  int cancel_sys_task(const share::ObTaskId &task_id);
  int refresh_memory_stat();
  int wash_memory_fragmentation();
  int broadcast_rs_list(const obrpc::ObRsListArg &arg);
  ////////////////////////////////////////////////////////////////
  // misc functions

  int get_all_partition_status(int64_t &inactive_num, int64_t &total_num) const;
  int get_root_server_status(obrpc::ObGetRootserverRoleResult &get_role_result);
  int refresh_sys_tenant_ls();

  int get_tenant_refreshed_schema_version(
      const obrpc::ObGetTenantSchemaVersionArg &arg,
      obrpc::ObGetTenantSchemaVersionResult &result);
  int submit_async_refresh_schema_task(const uint64_t tenant_id, const int64_t schema_version);
  int renew_in_zone_hb(const share::ObInZoneHbRequest &arg,
                       share::ObInZoneHbResponse &result);
  int init_tenant_config(
      const obrpc::ObInitTenantConfigArg &arg,
      obrpc::ObInitTenantConfigRes &result);
  int handle_heartbeat(
      const share::ObHBRequest &hb_request,
      share::ObHBResponse &hb_response);
  int ob_admin_unlock_member_list(
      const obrpc::ObAdminUnlockMemberListOpArg &arg);
  int check_server_empty(bool &server_empty);

private:
  int get_role_from_palf_(
      logservice::ObLogService &log_service,
      const share::ObLSID &ls_id,
      common::ObRole &role,
      int64_t &proposal_id);
  int inner_fill_tablet_info_(
      const int64_t tenant_id,
      const ObTabletID &tablet_id,
      storage::ObLS *ls,
      share::ObTabletReplica &tablet_replica,
      share::ObTabletReplicaChecksumItem &tablet_checksum,
      const bool need_checksum);
  int register_self();

  int handle_server_freeze_req_(const obrpc::ObMinorFreezeArg &arg);
  int handle_tenant_freeze_req_(const obrpc::ObMinorFreezeArg &arg);
  int handle_ls_freeze_req_(const obrpc::ObMinorFreezeArg &arg);
  int tenant_freeze_(const uint64_t tenant_id);
  int ls_freeze_(const uint64_t tenant_id, const share::ObLSID &ls_id, const common::ObTabletID &tablet_id);
  int generate_master_rs_ls_info_(
      const share::ObLSReplica &cur_leader,
      share::ObLSInfo &ls_info);
private:
  bool inited_;
  bool in_register_process_;
  bool service_started_;
  volatile bool stopped_;

  ObServerSchemaUpdater schema_updater_;

  //lease
  ObLeaseStateMgr lease_state_mgr_;
  ObHeartBeatProcess heartbeat_process_;
  const ObGlobalContext &gctx_;
  // server tracer task
  share::ObServerTraceTask server_trace_task_;
  ObSchemaReleaseTimeTask schema_release_task_;
  ObRefreshSchemaStatusTimerTask schema_status_task_;
  ObRemoteMasterRsUpdateTask remote_master_rs_update_task_;
  // report
  ObLSTableUpdater ls_table_updater_;
  ObServerMetaTableChecker meta_table_checker_;
};

}//end namespace observer
}//end namespace oceanbase
#endif
