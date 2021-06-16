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

#ifndef _OCEABASE_COMMON_OB_SRV_RPC_PROXY_H_
#define _OCEABASE_COMMON_OB_SRV_RPC_PROXY_H_

#include "sql/engine/cmd/ob_kill_session_arg.h"
#include "storage/ob_partition_service_rpc.h"

namespace oceanbase {
namespace obrpc {

class ObSrvRpcProxy : public ObRpcProxy {
public:
  DEFINE_TO(ObSrvRpcProxy);

  // special usage when can't deliver request.
  RPC_S(PR5 nouse, OB_ERROR_PACKET);

  RPC_S(PR5 set_config, OB_SET_CONFIG, (common::ObString));
  RPC_S(PR5 get_config, OB_GET_CONFIG, common::ObString);
  RPC_S(PR1 get_diagnose_args, OB_GET_DIAGNOSE_ARGS, common::ObString);

  // @author
  // notify server to add tenant temporary, that server will have
  // minimal quota for a while.
  // ARG: tenant_id
  // RES: NULL
  // RET: OB_SUCCESS add tenant successfully
  //      OTHER other error
  RPC_S(PR5 add_tenant_tmp, OB_ADD_TENANT_TMP, (UInt64));

  RPC_AP(PR5 reach_partition_limit, OB_REACH_PARTITION_LIMIT, (obrpc::ObReachPartitionLimitArg));
  RPC_AP(
      PR5 notify_tenant_server_unit_resource, OB_NOTIFY_TENANT_SERVER_UNIT_RESOURCE, (obrpc::TenantServerUnitConfig));
  RPC_AP(PR5 create_partition_batch, OB_CREATE_PARTITION_BATCH, (ObCreatePartitionBatchArg), ObCreatePartitionBatchRes);
  RPC_AP(PR5 batch_split_partition, OB_BATCH_SPLIT_PARTITION, (ObSplitPartitionArg), ObSplitPartitionResult);
  RPC_AP(PR5 create_partition, OB_CREATE_PARTITION, (ObCreatePartitionArg));
  RPC_AP(PR5 get_wrs_info, OB_GET_WRS_INFO, (ObGetWRSArg), ObGetWRSResult);
  RPC_AP(PR5 stop_write, OB_PARTITION_STOP_WRITE, (obrpc::Int64), obrpc::Int64);
  RPC_AP(PR5 check_log, OB_PARTITION_CHECK_LOG, (obrpc::Int64), obrpc::Int64);
  RPC_AP(PR5 batch_set_member_list, OB_BATCH_START_ELECTION, (ObBatchStartElectionArg), obrpc::Int64);
  RPC_AP(PR5 batch_wait_leader, OB_BATCH_WAIT_LEADER, (ObBatchCheckLeaderArg), ObBatchCheckRes);
  RPC_AP(PR5 batch_write_cutdata_clog, OB_BATCH_WRITE_CUTDATA_CLOG, (ObBatchWriteCutdataClogArg), ObBatchCheckRes);
  RPC_AP(PR5 batch_flashback, OB_BATCH_FLASHBACK, (obrpc::ObBatchFlashbackArg), obrpc::Int64);
  RPC_AP(PR5 batch_prepare_flashback, OB_BATCH_PREPARE_FLASHBACK, (obrpc::ObBatchFlashbackArg), obrpc::Int64);
  RPC_AP(PR5 check_frozen_version, OB_CHECK_FROZEN_VERSION, (obrpc::ObCheckFrozenVersionArg));
  RPC_AP(PR5 get_min_sstable_schema_version, OB_GET_MIN_SSTABLE_SCHEMA_VERSION,
      (obrpc::ObGetMinSSTableSchemaVersionArg), obrpc::ObGetMinSSTableSchemaVersionRes);
  RPC_S(PR5 fetch_root_partition, OB_FETCH_ROOT_PARTITION, share::ObPartitionReplica);
  RPC_S(PR5 broadcast_rs_list, OB_BROADCAST_RS_LIST, (obrpc::ObRsListArg));
  RPC_S(PR5 add_replica, OB_ADD_REPLICA, (ObAddReplicaArg));
  RPC_S(PR5 get_tenant_log_archive_status, OB_GET_TENANT_LOG_ARCHIVE_STATUS, (share::ObGetTenantLogArchiveStatusArg),
      share::ObTenantLogArchiveStatusWrapper);
  RPC_S(PR5 remove_non_paxos_replica, OB_REMOVE_NON_PAXOS_REPLICA, (ObRemoveNonPaxosReplicaArg));
  RPC_S(PR5 remove_member, OB_REMOVE_MEMBER, (ObMemberChangeArg));
  RPC_S(PR5 migrate_replica, OB_MIGRATE_REPLICA, (ObMigrateReplicaArg));
  RPC_S(PR5 change_replica, OB_CHANGE_REPLICA, (ObChangeReplicaArg));
  RPC_S(PR5 rebuild_replica, OB_REBUILD_REPLICA, (ObRebuildReplicaArg));
  RPC_S(PR5 restore_replica, OB_RESTORE_REPLICA, (ObRestoreReplicaArg));
  RPC_S(PR5 physical_restore_replica, OB_PHYSICAL_RESTORE_REPLICA, (ObPhyRestoreReplicaArg));
  RPC_S(PR5 split_partition, OB_SPLIT_PARTITION, (ObSplitPartitionArg), ObSplitPartitionResult);
  // Rpc bulk interface for replication, migration, etc.
  RPC_S(PR5 add_replica_batch, OB_ADD_REPLICA_BATCH, (ObAddReplicaBatchArg));
  RPC_S(PR5 remove_non_paxos_replica_batch, OB_REMOVE_NON_PAXOS_REPLICA_BATCH, (ObRemoveNonPaxosReplicaBatchArg),
      ObRemoveNonPaxosReplicaBatchResult);
  RPC_S(PR5 remove_member_batch, OB_REMOVE_MEMBER_BATCH, (ObMemberChangeBatchArg), ObMemberChangeBatchResult);
  RPC_S(PR5 migrate_replica_batch, OB_MIGRATE_REPLICA_BATCH, (ObMigrateReplicaBatchArg));
  RPC_S(PR5 standby_cutdata_batch_task, OB_STANDBY_CUTDATA_BATCH_TASK, (ObStandbyCutDataBatchTaskArg));
  RPC_S(PR5 change_replica_batch, OB_CHANGE_REPLICA_BATCH, (ObChangeReplicaBatchArg));
  RPC_S(PR5 rebuild_replica_batch, OB_REBUILD_REPLICA_BATCH, (ObRebuildReplicaBatchArg));
  RPC_S(PR5 copy_sstable_batch, OB_COPY_SSTABLE_BATCH, (ObCopySSTableBatchArg));
  RPC_S(PR5 check_migrate_task_exist, OB_CHECK_MIGRATE_TASK_EXIST, (share::ObTaskId), obrpc::Bool);
  RPC_S(PR5 modify_quorum_batch, OB_MODIFY_QUORUM_BATCH, (ObModifyQuorumBatchArg), ObModifyQuorumBatchResult);
  RPC_S(PR5 backup_replica_batch, OB_BACKUP_REPLICA_BATCH, (ObBackupBatchArg));
  RPC_S(PR5 check_backup_task_exist, OB_CHECK_BACKUP_TASK_EXIST, (int64_t), obrpc::Bool);
  RPC_S(PR5 validate_backup_batch, OB_VALIDATE_BACKUP_BATCH, (ObValidateBatchArg));

  RPC_AP(PR5 minor_freeze, OB_MINOR_FREEZE, (ObMinorFreezeArg), obrpc::Int64);
  RPC_AP(PR5 prepare_major_freeze, OB_PREPARE_MAJOR_FREEZE, (ObMajorFreezeArg), obrpc::Int64);
  RPC_AP(PR5 commit_major_freeze, OB_COMMIT_MAJOR_FREEZE, (ObMajorFreezeArg), obrpc::Int64);
  RPC_AP(PR5 abort_major_freeze, OB_ABORT_MAJOR_FREEZE, (ObMajorFreezeArg), obrpc::Int64);
  RPC_AP(PR5 check_schema_version_elapsed, OB_CHECK_SCHEMA_VERSION_ELAPSED, (ObCheckSchemaVersionElapsedArg),
      ObCheckSchemaVersionElapsedResult);
  RPC_AP(PR5 check_ctx_create_timestamp_elapsed, OB_CHECK_CTX_CREATE_TIMESTAMP_ELAPSED,
      (ObCheckCtxCreateTimestampElapsedArg), ObCheckCtxCreateTimestampElapsedResult);

  RPC_S(PR5 get_member_list, OB_GET_MEMBER_LIST, (common::ObPartitionKey), ObServerList);
  RPC_S(PR5 switch_leader, OB_SWITCH_LEADER, (ObSwitchLeaderArg));
  RPC_S(PR5 batch_switch_rs_leader, OB_BATCH_SWITCH_RS_LEADER, (ObAddr));
  RPC_S(PR5 switch_leader_list, OB_SWITCH_LEADER_LIST, (ObSwitchLeaderListArg));
  RPC_AP(PR5 switch_leader_list_async, OB_SWITCH_LEADER_LIST_ASYNC, (ObSwitchLeaderListArg), obrpc::Int64);
  RPC_S(PR5 get_leader_candidates, OB_GET_LEADER_CANDIDATES, (ObGetLeaderCandidatesArg), ObGetLeaderCandidatesResult);
  RPC_AP(PR5 get_leader_candidates_async, OB_GET_LEADER_CANDIDATES_ASYNC, (ObGetLeaderCandidatesArg),
      ObGetLeaderCandidatesResult);
  RPC_AP(PR5 get_leader_candidates_async_v2, OB_GET_LEADER_CANDIDATES_ASYNC_V2, (ObGetLeaderCandidatesV2Arg),
      ObGetLeaderCandidatesResult);
  RPC_S(PR5 get_partition_count, OB_GET_PARTITION_COUNT, ObGetPartitionCountResult);
  RPC_S(PR5 switch_schema, OB_SWITCH_SCHEMA, (ObSwitchSchemaArg));
  RPC_S(PR5 refresh_memory_stat, OB_REFRESH_MEMORY_STAT);
  RPC_S(PR5 bootstrap, OB_BOOTSTRAP, (ObBootstrapArg));
  RPC_S(PR5 is_empty_server, OB_IS_EMPTY_SERVER, (ObCheckServerEmptyArg), Bool);
  RPC_S(PR5 check_deployment_mode_match, OB_CHECK_DEPLOYMENT_MODE, (ObCheckDeploymentModeArg), Bool);
  RPC_S(PR5 get_partition_stat, OB_GET_PARTITION_STAT, ObPartitionStatList);
  RPC_S(PR5 report_replica, OB_REPORT_REPLICA);
  RPC_S(PR5 report_single_replica, OB_REPORT_SINGLE_REPLICA, (ObReportSingleReplicaArg));
  RPC_S(PR5 recycle_replica, OB_RECYCLE_REPLICA);
  RPC_S(PR5 drop_replica, OB_DROP_REPLICA, (ObDropReplicaArg));
  RPC_S(PR5 clear_location_cache, OB_CLEAR_LOCATION_CACHE);
  RPC_S(PR5 refresh_sync_value, OB_REFRESH_SYNC_VALUE, (ObAutoincSyncArg));
  RPC_S(PR5 sync_auto_increment, OB_SYNC_AUTO_INCREMENT, (ObAutoincSyncArg));
  RPC_S(PR5 clear_autoinc_cache, OB_CLEAR_AUTOINC_CACHE, (ObAutoincSyncArg));
  RPC_S(PR5 dump_memtable, OB_DUMP_MEMTABLE, (common::ObPartitionKey));
  RPC_S(PR5 force_set_as_single_replica, OB_FORCE_SET_AS_SINGLE_REPLICA, (common::ObPartitionKey));
  RPC_S(PR5 force_set_replica_num, OB_FORCE_SET_REPLICA_NUM, (ObSetReplicaNumArg));
  RPC_S(PR5 force_reset_parent, OB_FORCE_RESET_PARENT, (common::ObPartitionKey));
  RPC_S(PR5 force_set_parent, OB_FORCE_SET_PARENT, (ObSetParentArg));
  RPC_S(PR5 halt_all_prewarming, OB_FORCE_PURGE_MEMTABLE);
  RPC_AP(PR5 halt_all_prewarming_async, OB_FORCE_PURGE_MEMTABLE_ASYNC, (obrpc::UInt64));
  RPC_S(PR5 set_debug_sync_action, OB_SET_DS_ACTION, (obrpc::ObDebugSyncActionArg));
  RPC_S(PR5 request_heartbeat, OB_REQUEST_HEARTBEAT, share::ObLeaseRequest);
  RPC_S(PR5 update_cluster_info, OB_UPDATE_CLUSTER_INFO, (obrpc::ObClusterInfoArg));
  RPC_S(PR5 broadcast_sys_schema, OB_BROADCAST_SYS_SCHEMA, (common::ObSArray<share::schema::ObTableSchema>));
  RPC_S(PR5 check_partition_table, OB_CHECK_PARTITION_TABLE);
  RPC_S(PR5 update_baseline_schema_version, OB_UPDATE_BASELINE_SCHEMA_VERSION, (obrpc::Int64));
  RPC_S(PR3 get_member_list_and_leader, OB_GET_MEMBER_LIST_AND_LEADER, (common::ObPartitionKey),
      obrpc::ObMemberListAndLeaderArg);
  RPC_S(PR3 get_member_list_and_leader_v2, OB_GET_MEMBER_LIST_AND_LEADER_V2, (common::ObPartitionKey),
      obrpc::ObGetMemberListAndLeaderResult);
  RPC_AP(PR3 batch_get_member_list_and_leader, OB_BATCH_GET_MEMBER_LIST_AND_LEADER, (obrpc::ObLocationRpcRenewArg),
      obrpc::ObLocationRpcRenewResult);
  RPC_AP(PR3 batch_get_role, OB_BATCH_GET_ROLE, (obrpc::ObBatchGetRoleArg), obrpc::ObBatchGetRoleResult);
  RPC_AP(PR5 check_has_need_offline_replica, OB_CHECK_NEED_OFFLINE_REPLICA, (obrpc::ObTenantSchemaVersions),
      obrpc::ObGetPartitionCountResult);
  RPC_AP(PR5 check_flashback_info_dump, OB_CHECK_FLASHBACK_INFO_DUMP, (obrpc::ObCheckFlashbackInfoArg),
      obrpc::ObCheckFlashbackInfoResult);
  RPC_S(PR5 sync_partition_table, OB_SYNC_PARTITION_TABLE, (obrpc::Int64));
  RPC_S(PR5 sync_pg_partition_table, OB_SYNC_PG_PARTITION_TABLE, (obrpc::Int64));
  RPC_S(PR5 flush_cache, OB_FLUSH_CACHE, (ObFlushCacheArg));
  RPC_S(PR5 set_tracepoint, OB_SET_TP, (obrpc::ObAdminSetTPArg));
  RPC_S(PR5 kill_session, OB_KILL_SESSION, (sql::ObKillSessionArg));
  RPC_S(PR5 cancel_sys_task, OB_CANCEL_SYS_TASK, (obrpc::ObCancelTaskArg));
  RPC_S(PR5 set_disk_valid, OB_SET_DISK_VALID, (ObSetDiskValidArg));
  RPC_S(PR5 add_disk, OB_ADD_DISK, (ObAdminAddDiskArg));
  RPC_S(PR5 drop_disk, OB_DROP_DISK, (ObAdminDropDiskArg));
  RPC_S(PR5 load_baseline, OB_LOAD_BASELINE, (ObLoadBaselineArg));
  RPC_S(PR5 force_switch_ilog_file, OB_FORCE_SWITCH_ILOG_FILE, (ObForceSwitchILogFileArg));
  RPC_S(PR5 check_dangling_replica_exist, OB_CHECK_DANGLING_REPLICA_EXIST, (obrpc::Int64));
  RPC_S(PR5 force_set_all_as_single_replica, OB_FORCE_SET_ALL_AS_SINGLE_REPLICA, (ObForceSetAllAsSingleReplicaArg));
  RPC_S(PR5 force_set_server_list, OB_FORCE_SET_SERVER_LIST, (ObForceSetServerListArg));
  RPC_S(PR5 check_unique_index_request, OB_CHECK_UNIQUE_INDEX_REQUEST, (ObCheckUniqueIndexRequestArg));
  RPC_S(PR5 calc_column_checksum_request, OB_CALC_COLUMN_CHECKSUM_REQUEST, (ObCalcColumnChecksumRequestArg));
  RPC_S(PR5 check_single_replica_major_sstable_exist, obrpc::OB_CHECK_SINGLE_REPLICA_MAJOR_SSTABLE_EXIST,
      (obrpc::ObCheckSingleReplicaMajorSSTableExistArg));
  RPC_S(PR5 check_all_replica_major_sstable_exist, obrpc::OB_CHECK_ALL_REPLICA_MAJOR_SSTABLE_EXIST,
      (obrpc::ObCheckAllReplicaMajorSSTableExistArg));
  RPC_S(PR5 check_single_replica_major_sstable_exist_with_time,
      obrpc::OB_CHECK_SINGLE_REPLICA_MAJOR_SSTABLE_EXIST_WITH_TIME, (obrpc::ObCheckSingleReplicaMajorSSTableExistArg),
      obrpc::ObCheckSingleReplicaMajorSSTableExistResult);
  RPC_S(PR5 check_all_replica_major_sstable_exist_with_time, obrpc::OB_CHECK_ALL_REPLICA_MAJOR_SSTABLE_EXIST_WITH_TIME,
      (obrpc::ObCheckAllReplicaMajorSSTableExistArg), obrpc::ObCheckAllReplicaMajorSSTableExistResult);
  RPC_S(PR5 query_max_decided_trans_version, OB_QUERY_MAX_DECIDED_TRANS_VERSION, (ObQueryMaxDecidedTransVersionRequest),
      ObQueryMaxDecidedTransVersionResponse);
  RPC_S(
      PR5 query_is_valid_member, OB_QUERY_IS_VALID_MEMBER, (ObQueryIsValidMemberRequest), ObQueryIsValidMemberResponse);
  RPC_S(PR5 query_max_flushed_ilog_id, OB_QUERY_MAX_FLUSHED_ILOG_ID, (ObQueryMaxFlushedILogIdRequest),
      ObQueryMaxFlushedILogIdResponse);
  RPC_S(PR5 force_remove_replica, OB_FORCE_REMOVE_REPLICA, (common::ObPartitionKey));
  RPC_S(PRD force_create_sys_table, OB_FORCE_CREATE_SYS_TABLE, (ObForceCreateSysTableArg));
  RPC_S(PRD force_set_locality, OB_FORCE_SET_LOCALITY, (ObForceSetLocalityArg));
  RPC_S(PR5 force_disable_blacklist, OB_FORCE_DISABLE_BLACKLIST);
  RPC_S(PR5 force_enable_blacklist, OB_FORCE_ENABLE_BLACKLIST);
  RPC_S(PR5 force_clear_srv_blacklist, OB_FORCE_CLEAR_BLACKLIST);

  RPC_S(PR5 update_local_stat_cache, obrpc::OB_SERVER_UPDATE_STAT_CACHE, (ObUpdateStatCacheArg));

  // The optimizer estimates the number of rows
  RPC_S(PR5 estimate_partition_rows, OB_ESTIMATE_PARTITION_ROWS, (ObEstPartArg), ObEstPartRes);

  RPC_AP(PR1 ha_gts_ping_request, OB_HA_GTS_PING_REQUEST, (ObHaGtsPingRequest), ObHaGtsPingResponse);
  RPC_AP(PR1 ha_gts_get_request, OB_HA_GTS_GET_REQUEST, (ObHaGtsGetRequest));
  RPC_AP(PR1 ha_gts_get_response, OB_HA_GTS_GET_RESPONSE, (ObHaGtsGetResponse));
  RPC_AP(PR1 ha_gts_heartbeat, OB_HA_GTS_HEARTBEAT, (ObHaGtsHeartbeat));
  RPC_S(PR1 ha_gts_update_meta, OB_HA_GTS_UPDATE_META, (ObHaGtsUpdateMetaRequest), ObHaGtsUpdateMetaResponse);
  RPC_S(PR1 ha_gts_change_member, OB_HA_GTS_CHANGE_MEMBER, (ObHaGtsChangeMemberRequest), ObHaGtsChangeMemberResponse);
  RPC_AP(PR5 batch_set_member_list, OB_BATCH_SET_MEMBER_LIST, (ObSetMemberListBatchArg), ObCreatePartitionBatchRes);
  RPC_S(PR5 get_tenant_refreshed_schema_version, OB_GET_TENANT_REFRESHED_SCHEMA_VERSION, (ObGetTenantSchemaVersionArg),
      ObGetTenantSchemaVersionResult);
  RPC_S(PR5 get_remote_tenant_group_string, OB_GET_REMOTE_TENANT_GROUP_STRING, common::ObString);
  RPC_S(PR5 update_tenant_memory, OB_UPDATE_TENANT_MEMORY, (obrpc::ObTenantMemoryArg));
  RPC_S(PR5 check_physical_flashback_succ, OB_CHECK_PHYSICAL_FLASHBACK_SUCC, (obrpc::ObCheckPhysicalFlashbackArg),
      obrpc::ObPhysicalFlashbackResultArg);
  RPC_S(PR5 renew_in_zone_hb, OB_RENEW_IN_ZONE_HB, (share::ObInZoneHbRequest), share::ObInZoneHbResponse);
  RPC_AP(PR5 batch_get_protection_level, OB_BATCH_GET_PROTECTION_LEVEL, (ObBatchCheckLeaderArg), ObBatchCheckRes);
  RPC_S(PR5 kill_part_trans_ctx, OB_KILL_PART_TRANS_CTX, (obrpc::ObKillPartTransCtxArg));
};  // end of class ObSrvRpcProxy

}  // namespace obrpc
}  // end of namespace oceanbase

#endif /* _OCEABASE_COMMON_OB_SRV_RPC_PROXY_H_ */
