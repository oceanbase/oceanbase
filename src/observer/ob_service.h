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

#include "common/ob_i_rs_cb.h"
#include "share/partition_table/ob_ipartition_table.h"
#include "storage/ob_i_partition_report.h"
#include "storage/ob_all_server_tracer.h"
#include "observer/ob_lease_state_mgr.h"
#include "observer/ob_heartbeat.h"
#include "observer/ob_partition_table_updater.h"
#include "observer/ob_sstable_checksum_updater.h"
#include "observer/ob_server_schema_updater.h"
#include "observer/ob_pg_partition_meta_table_updater.h"
#include "observer/ob_rpc_processor_simple.h"
#include "observer/ob_index_status_reporter.h"
#include "observer/ob_rebuild_flag_reporter.h"
#include "observer/ob_partition_table_checker.h"
#include "observer/ob_uniq_task_queue.h"
#include "share/backup/ob_pg_backup_task_updater.h"
#include "share/backup/ob_tenant_backup_task_updater.h"
#include "share/backup/ob_pg_validate_task_updater.h"
#include "share/backup/ob_tenant_validate_task_updater.h"

namespace oceanbase {
namespace share {
class ObSSTableDataChecksumItem;
class ObSSTableColumnChecksumItem;
class ObPGPartitionMTUpdateItem;
}  // namespace share
namespace storage {
class ObFrozenStatus;
class ObServerTraceTask;
}  // namespace storage
namespace observer {
class ObServer;
class ObServerInstance;
class ObRemoteLocationGetter;

class ObSchemaReleaseTimeTask : public common::ObTimerTask {
public:
  ObSchemaReleaseTimeTask();
  virtual ~ObSchemaReleaseTimeTask()
  {}
  int init(ObServerSchemaUpdater& schema_updater, int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  const static int64_t REFRESH_INTERVAL = 30L * 60L * 1000L * 1000L;  // 30min
  ObServerSchemaUpdater* schema_updater_;
  bool is_inited_;
};

class ObService : public ObIPartitionReplicaFiller,
                  public share::ObIPartPropertyGetter,
                  public storage::ObIPartitionReport {
public:
  explicit ObService(const ObGlobalContext& gctx);
  virtual ~ObService();

  int init(common::ObMySQLProxy& sql_proxy);
  int start();
  void set_stop();
  void stop();
  void wait();
  int destroy();

  virtual int get_role(const common::ObPartitionKey& part_key, common::ObRole& role) override;
  int batch_get_role(const obrpc::ObBatchGetRoleArg& arg, obrpc::ObBatchGetRoleResult& result);
  // ObIPartPropertyGetter interface
  virtual int get_leader_member(
      const common::ObPartitionKey& part_key, common::ObIArray<common::ObAddr>& member_list) override;
  virtual int get_member_list_and_leader(
      const common::ObPartitionKey& part_key, obrpc::ObMemberListAndLeaderArg& arg) override;
  virtual int get_member_list_and_leader_v2(
      const common::ObPartitionKey& part_key, obrpc::ObGetMemberListAndLeaderResult& arg) override;
  int batch_get_member_list_and_leader(const obrpc::ObLocationRpcRenewArg& arg, obrpc::ObLocationRpcRenewResult& res);
  int update_baseline_schema_version(const int64_t schema_version);
  // ObIPartitionReplicaFiller interface
  virtual int fill_partition_replica(const common::ObPGKey& pg_key, share::ObPartitionReplica& replica) override;
  virtual int fill_partition_replica(storage::ObIPartitionGroup* part, share::ObPartitionReplica& replica);
  int get_pg_key(const common::ObPartitionKey& pkey, common::ObPGKey& pg_key) const;
  virtual const common::ObAddr& get_self_addr();
  virtual int fill_checksum(const common::ObPartitionKey& pkey, const uint64_t sstable_id, const int sstable_type,
      const ObSSTableChecksumUpdateType update_type,
      common::ObIArray<share::ObSSTableDataChecksumItem>& data_checksum_items,
      common::ObIArray<share::ObSSTableColumnChecksumItem>& column_checksum_items);
  int fill_partition_table_update_task(const common::ObPartitionKey& pkey, share::ObPGPartitionMTUpdateItem& item);

  ////////////////////////////////////////////////////////////////
  // ObIPartitionReport interface
  virtual int submit_pt_update_task(const common::ObPartitionKey& part_key, const bool need_report_checksum = true,
      const bool with_role = false) override;
  virtual int submit_pt_update_role_task(const common::ObPartitionKey& part_key) override;
  virtual void submit_pg_pt_update_task(const common::ObPartitionArray& pg_partitions) override;
  virtual int submit_checksum_update_task(const common::ObPartitionKey& part_key, const uint64_t sstable_id,
      const int sstable_type, const ObSSTableChecksumUpdateType update_type = ObSSTableChecksumUpdateType::UPDATE_ALL,
      const bool task_need_batch = true) override;
  virtual int pt_sync_update(const common::ObPartitionKey& part_key) override;
  // ObIPartitionReport interface
  virtual int report_merge_finished(const int64_t frozen_version) override;
  // ObIPartitionReport interface
  virtual int report_merge_error(const common::ObPartitionKey& part_key, const int error_code) override;
  // ObIPartitionReport interface
  virtual int report_local_index_build_complete(const common::ObPartitionKey& part_key, const uint64_t index_id,
      const share::schema::ObIndexStatus index_status, const int32_t ret_code) override;
  // ObIPartitionReport interface
  virtual int report_rebuild_replica(const common::ObPartitionKey& part_key, const common::ObAddr& server,
      const storage::ObRebuildSwitch& rebuild_switch) override;
  virtual int report_rebuild_replica_async(const common::ObPartitionKey& part_key, const common::ObAddr& server,
      const storage::ObRebuildSwitch& rebuild_switch) override;
  virtual int update_pg_backup_task_info(
      const common::ObIArray<share::ObPGBackupTaskInfo>& pg_task_info_array) override;
  // ObIPartitionReport interface
  int submit_pt_remove_task(const common::ObPartitionKey& part_key) override;

  ////////////////////////////////////////////////////////////////
  int reach_partition_limit(const obrpc::ObReachPartitionLimitArg& arg);
  int check_frozen_version(const obrpc::ObCheckFrozenVersionArg& arg);
  int get_min_sstable_schema_version(
      const obrpc::ObGetMinSSTableSchemaVersionArg& arg, obrpc::ObGetMinSSTableSchemaVersionRes& result);
  // ObRpcSwitchSchemaP @RS DDL
  int switch_schema(const obrpc::ObSwitchSchemaArg& arg);
  // ObRpcCreatePartitionP @RS DDL
  int create_partition(const obrpc::ObCreatePartitionArg& arg);
  // ObRpcCreatePartitionBatchP @RS DDL
  int create_partition_batch(
      const obrpc::ObCreatePartitionBatchArg& batch_arg, obrpc::ObCreatePartitionBatchRes& batch_res);
  int sync_report_replica_info(const int64_t data_version, const obrpc::ObCreatePartitionArg& first,
      const obrpc::ObCreatePartitionBatchArg& batch_arg);
  int check_unique_index_request(const obrpc::ObCheckUniqueIndexRequestArg& arg);
  int calc_column_checksum_request(const obrpc::ObCalcColumnChecksumRequestArg& arg);
  int split_partition(const obrpc::ObSplitPartitionArg& arg, obrpc::ObSplitPartitionResult& is_succ);
  int batch_set_member_list(const obrpc::ObBatchStartElectionArg& arg, obrpc::Int64& result);
  int batch_wait_leader(const obrpc::ObBatchCheckLeaderArg& arg, obrpc::ObBatchCheckRes& result);
  int batch_write_cutdata_clog(const obrpc::ObBatchWriteCutdataClogArg& arg, obrpc::ObBatchCheckRes& result);
  int stop_partition_write(const obrpc::Int64& switchover_timestamp, obrpc::Int64& result);
  int check_partition_log(const obrpc::Int64& switchover_timestamp, obrpc::Int64& result);
  int get_wrs_info(const obrpc::ObGetWRSArg& arg, obrpc::ObGetWRSResult& result);
  int check_single_replica_major_sstable_exist(const obrpc::ObCheckSingleReplicaMajorSSTableExistArg& arg);
  int check_all_replica_major_sstable_exist(const obrpc::ObCheckAllReplicaMajorSSTableExistArg& arg);
  int check_single_replica_major_sstable_exist(const obrpc::ObCheckSingleReplicaMajorSSTableExistArg& arg,
      obrpc::ObCheckSingleReplicaMajorSSTableExistResult& res);
  int check_all_replica_major_sstable_exist(
      const obrpc::ObCheckAllReplicaMajorSSTableExistArg& arg, obrpc::ObCheckAllReplicaMajorSSTableExistResult& res);

  ////////////////////////////////////////////////////////////////
  // ObRpcFetchRootPartitionP @RS load balance
  int fetch_root_partition(share::ObPartitionReplica& replica);
  // ObRpcAddReplicaP @RS load balance
  int add_replica(const obrpc::ObAddReplicaArg& arg, const share::ObTaskId& task_id);
  // ObRpcRemoveReplicaP @RS load balance
  int remove_non_paxos_replica(const obrpc::ObRemoveNonPaxosReplicaArg& arg);
  int remove_replica(const obrpc::ObRemoveNonPaxosReplicaArg& arg);
  // ObRpcRemoveMemberP @RS load balance
  int remove_member(const obrpc::ObMemberChangeArg& arg);
  int modify_quorum(const obrpc::ObModifyQuorumArg& arg);
  int restore_replica(const obrpc::ObRestoreReplicaArg& arg, const share::ObTaskId& task_id);
  int physical_restore_replica(const obrpc::ObPhyRestoreReplicaArg& arg, const share::ObTaskId& task_id);
  int get_tenant_log_archive_status(
      const share::ObGetTenantLogArchiveStatusArg& arg, share::ObTenantLogArchiveStatusWrapper& result);
  int copy_sstable_batch(const obrpc::ObCopySSTableBatchArg& arg);
  // ObRpcMigrateReplicaP @RS load balance
  int migrate_replica(const obrpc::ObMigrateReplicaArg& arg, const share::ObTaskId& task_id);
  int rebuild_replica(const obrpc::ObRebuildReplicaArg& arg, const share::ObTaskId& task_id);
  // ObRpcChangeReplicaP @RS load balance
  int change_replica(const obrpc::ObChangeReplicaArg& arg, const share::ObTaskId& task_id);
  // ObRpcAddReplicaBatchP @RS load balance
  int add_replica_batch(const obrpc::ObAddReplicaBatchArg& arg);
  // ObRpcRemoveReplicaBatchP @RS load balance
  int remove_non_paxos_replica_batch(
      const obrpc::ObRemoveNonPaxosReplicaBatchArg& arg, obrpc::ObRemoveNonPaxosReplicaBatchResult& result);
  // ObRpcRemoveMemberBatchP @RS load balance
  int remove_member_batch(const obrpc::ObMemberChangeBatchArg& arg, obrpc::ObMemberChangeBatchResult& result);
  // ObRpcModifyQuorumBatchP @RS load balance
  int modify_quorum_batch(const obrpc::ObModifyQuorumBatchArg& arg, obrpc::ObModifyQuorumBatchResult& result);
  // ObRpcMigrateReplicaBatchP @RS load balance
  int migrate_replica_batch(const obrpc::ObMigrateReplicaBatchArg& arg);
  int rebuild_replica_batch(const obrpc::ObRebuildReplicaBatchArg& arg);
  // ObRpcChangeReplicaArgP @RS load balance
  int backup_replica_batch(const obrpc::ObBackupBatchArg& arg);
  // ObRpcAddReplicaBatchP @RS load balance
  int standby_cutdata_batch_task(const obrpc::ObStandbyCutDataBatchTaskArg& arg);

  int validate_backup_batch(const obrpc::ObValidateBatchArg& arg);

  int change_replica_batch(const obrpc::ObChangeReplicaBatchArg& arg);
  int check_sys_task_exist(const share::ObTaskId& arg, bool& res);
  int check_migrate_task_exist(const share::ObTaskId& arg, bool& res);
  // ObGetMemberListP @ObPartitionService::handle_add_replica_callback
  int get_member_list(const common::ObPartitionKey& partition_key, obrpc::ObServerList& members);

  int estimate_partition_rows(const obrpc::ObEstPartArg& arg, obrpc::ObEstPartRes& res) const;

  ////////////////////////////////////////////////////////////////
  // ObRpcMinorFreezeP @RS minor freeze
  int minor_freeze(const obrpc::ObMinorFreezeArg& arg, obrpc::Int64& result);
  // ObRpcCheckSchemaVersionElapsedP @RS global index builder
  int check_schema_version_elapsed(
      const obrpc::ObCheckSchemaVersionElapsedArg& arg, obrpc::ObCheckSchemaVersionElapsedResult& result);
  // ObRpcGetChecksumCalSnapshotP
  int check_ctx_create_timestamp_elapsed(
      const obrpc::ObCheckCtxCreateTimestampElapsedArg& arg, obrpc::ObCheckCtxCreateTimestampElapsedResult& result);

  ////////////////////////////////////////////////////////////////
  // ObRpcSwitchLeaderP @RS leader coordinator & admin
  int switch_leader(const obrpc::ObSwitchLeaderArg& arg);
  // ObRpcBatchSwitchRsLeaderP @RS leader coordinator & admin
  int batch_switch_rs_leader(const ObAddr& arg);
  // ObRpcSwitchLeaderListP @RS leader coordinator
  int switch_leader_list(const obrpc::ObSwitchLeaderListArg& arg);
  // ObRpcGetLeaderCandidatesP @RS leader coordinator & admin
  int get_leader_candidates(const obrpc::ObGetLeaderCandidatesArg& arg, obrpc::ObGetLeaderCandidatesResult& result);
  int get_leader_candidates_v2(
      const obrpc::ObGetLeaderCandidatesV2Arg& arg, obrpc::ObGetLeaderCandidatesResult& result);
  // ObRpcGetPartitionCountP @RS leader coordinator
  int get_partition_count(obrpc::ObGetPartitionCountResult& result);

  ////////////////////////////////////////////////////////////////
  // ObRpcBootstrapP @RS bootstrap
  int bootstrap(const obrpc::ObBootstrapArg& arg);
  // ObRpcIsEmptyServerP @RS bootstrap
  int is_empty_server(const obrpc::ObCheckServerEmptyArg& arg, obrpc::Bool& is_empty);
  // ObRpcCheckDeploymentModeP
  int check_deployment_mode_match(const obrpc::ObCheckDeploymentModeArg& arg, obrpc::Bool& match);
  // ObBroadcastSysSchemaP @RS bootstrap
  int broadcast_sys_schema(const common::ObSArray<share::schema::ObTableSchema>& table_schemas);

  ////////////////////////////////////////////////////////////////
  int get_partition_stat(obrpc::ObPartitionStatList& partition_stat_list);
  // ObReportReplicaP @RS::admin to report replicas
  int report_replica();
  int load_leader_cluster_login_info();
  // ObRecycleReplicaP @RS::admin to recycle replicas
  int recycle_replica();
  // ObClearLocationCacheP @RS::admin to clear location cache
  int clear_location_cache();
  // ObDropReplicaP @RS::admin to drop replica
  int drop_replica(const obrpc::ObDropReplicaArg& arg);
  // ObSetDSActionP @RS::broadcast_debug_sync_action & ObDebugSync::add_debug_sync
  int set_ds_action(const obrpc::ObDebugSyncActionArg& arg);
  // ObRequestHeartbeatP @RS::admin to cancel delete server
  int request_heartbeat(share::ObLeaseRequest& lease_requeset);
  int update_cluster_info(obrpc::ObClusterInfoArg& cluster_info);
  // ObCheckPartitionTableP @RS::admin to check partition table
  int check_partition_table();
  int report_replica(const obrpc::ObReportSingleReplicaArg& arg);
  // ObSyncPartitionTableP @RS empty_server_checker
  int sync_partition_table(const obrpc::Int64& arg);
  int sync_pg_partition_table(const obrpc::Int64& arg);
  // ObCheckDanglingReplicaExistP @RS meta_table_migrator
  int check_dangling_replica_exist(const obrpc::Int64& arg);
  // ObRpcSetTPP @RS::admin to set tracepoint
  int set_tracepoint(const obrpc::ObAdminSetTPArg& arg);
  // for ObPartitionService::check_mc_allowed_by_server_lease
  int get_server_heartbeat_expire_time(int64_t& lease_expire_time);
  bool is_heartbeat_expired() const;
  bool is_svr_lease_valid() const;
  int cancel_sys_task(const share::ObTaskId& task_id);
  int refresh_memory_stat();
  int broadcast_rs_list(const obrpc::ObRsListArg& arg);
  ////////////////////////////////////////////////////////////////
  // misc functions
  int64_t get_partition_table_updater_user_queue_size() const;
  int64_t get_partition_table_updater_sys_queue_size() const;
  int64_t get_partition_table_updater_core_queue_size() const;

  int get_all_partition_status(int64_t& inactive_num, int64_t& total_num) const;
  int get_root_server_status(obrpc::ObGetRootserverRoleResult& get_role_result);
  int get_tenant_group_string(common::ObString& ttg_string);
  int refresh_core_partition();

  int get_tenant_refreshed_schema_version(
      const obrpc::ObGetTenantSchemaVersionArg& arg, obrpc::ObGetTenantSchemaVersionResult& result);
  int get_master_root_server(obrpc::ObGetRootserverRoleResult& result);
  int check_physical_flashback_succ(
      const obrpc::ObCheckPhysicalFlashbackArg& arg, obrpc::ObPhysicalFlashbackResultArg& result);
  int submit_async_refresh_schema_task(const uint64_t tenant_id, const int64_t schema_version);
  int renew_in_zone_hb(const share::ObInZoneHbRequest& arg, share::ObInZoneHbResponse& result);
  int pre_process_server_reply(const obrpc::ObPreProcessServerReplyArg& arg);

private:
  int register_self();
  int check_server_empty(const obrpc::ObCheckServerEmptyArg& arg, const bool wait_log_scan, bool& server_empty);
  int schedule_pt_check_task();
  int check_partition_need_update_pt_(const obrpc::ObCreatePartitionBatchArg& batch_arg,
      obrpc::ObCreatePartitionBatchRes& batch_res, bool& need_update);

private:
  bool inited_;
  bool in_register_process_;
  bool service_started_;
  volatile bool stopped_;

  ObServerSchemaUpdater schema_updater_;

  ObPartitionTableUpdater partition_table_updater_;
  ObIndexStatusUpdater index_updater_;
  ObSSTableChecksumUpdater checksum_updater_;
  ObUniqTaskQueue<ObIndexStatusReporter, ObIndexStatusUpdater> index_status_report_queue_;
  ObRebuildFlagUpdater rebuild_updater_;
  ObUniqTaskQueue<ObRebuildFlagReporter, ObRebuildFlagUpdater> rebuild_flag_report_queue_;

  // partition table checker
  ObPartitionTableChecker pt_checker_;

  // lease
  ObLeaseStateMgr lease_state_mgr_;
  ObHeartBeatProcess heartbeat_process_;
  const ObGlobalContext& gctx_;
  // server tracer task
  storage::ObServerTraceTask server_trace_task_;
  ObSchemaReleaseTimeTask schema_release_task_;
  share::ObTenantBackupTaskUpdater tenant_backup_task_updater_;
  share::ObPGBackupTaskUpdater pg_backup_task_updater_;
  share::ObTenantValidateTaskUpdater tenant_validate_task_updater_;
  share::ObPGValidateTaskUpdater pg_validate_task_updater_;
  ObRefreshSchemaStatusTimerTask schema_status_task_;
};

}  // end namespace observer
}  // end namespace oceanbase
#endif
