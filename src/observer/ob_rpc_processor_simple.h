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

#ifndef _OCEABASE_OBSERVER_OB_RPC_PROCESSOR_SIMPLE_H_
#define _OCEABASE_OBSERVER_OB_RPC_PROCESSOR_SIMPLE_H_

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_common_rpc_proxy.h"

#define OB_DEFINE_PROCESSOR(cls, pcode, pname) \
  class pname : public obrpc::ObRpcProcessor<obrpc::Ob##cls##RpcProxy::ObRpc<pcode> >

#define OB_DEFINE_PROCESSOR_S(cls, pcode, pname)              \
  OB_DEFINE_PROCESSOR(cls, obrpc::pcode, pname)               \
  {                                                           \
  public:                                                     \
    explicit pname(const ObGlobalContext& gctx) : gctx_(gctx) \
    {}                                                        \
                                                              \
  protected:                                                  \
    int process();                                            \
                                                              \
  private:                                                    \
    const ObGlobalContext& gctx_;                             \
  }

#define OB_DEFINE_PROCESSOR_SM(cls, pcode, pname)             \
  OB_DEFINE_PROCESSOR(cls, obrpc::pcode, pname)               \
  {                                                           \
  public:                                                     \
    explicit pname(const ObGlobalContext& gctx) : gctx_(gctx) \
    {}                                                        \
                                                              \
  protected:                                                  \
    int process();                                            \
    int after_process();                                      \
                                                              \
  private:                                                    \
    const ObGlobalContext& gctx_;                             \
  }

#define RPC_PROCESSOR_X(pcode, pname)           \
  OB_DEFINE_PROCESSOR(Srv, obrpc::pcode, pname) \
  {                                             \
  public:                                       \
    explicit pname(int ret) : ret_(ret)         \
    {}                                          \
                                                \
  protected:                                    \
    int process();                              \
    int deserialize();                          \
                                                \
  private:                                      \
    int ret_;                                   \
  }

namespace oceanbase {
namespace observer {

OB_DEFINE_PROCESSOR(Srv, obrpc::OB_GET_DIAGNOSE_ARGS, ObGetDiagnoseArgsP)
{
public:
  ObGetDiagnoseArgsP() : pwbuf_(), passwd_(), argsbuf_()
  {
    passwd_.assign_buffer(pwbuf_, sizeof(pwbuf_));
  }

protected:
  int process();

private:
  char pwbuf_[64];
  common::ObString passwd_;
  char argsbuf_[1024];
};

class ObGlobalContext;

RPC_PROCESSOR_X(OB_ERROR_PACKET, ObErrorP);

OB_DEFINE_PROCESSOR_S(Common, OB_GET_ROLE, ObRpcGetRoleP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SET_CONFIG, ObRpcSetConfigP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_CONFIG, ObRpcGetConfigP);
OB_DEFINE_PROCESSOR_S(Srv, OB_ADD_TENANT_TMP, ObRpcAddTenantTmpP);
OB_DEFINE_PROCESSOR_S(Srv, OB_NOTIFY_TENANT_SERVER_UNIT_RESOURCE, ObRpcNotifyTenantServerUnitResourceP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REACH_PARTITION_LIMIT, ObReachPartitionLimitP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_FROZEN_VERSION, ObCheckFrozenVersionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_MIN_SSTABLE_SCHEMA_VERSION, ObGetMinSSTableSchemaVersionP);

// oceanbase service provied
OB_DEFINE_PROCESSOR_S(Srv, OB_CREATE_PARTITION, ObRpcCreatePartitionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CREATE_PARTITION_BATCH, ObRpcCreatePartitionBatchP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FETCH_ROOT_PARTITION, ObRpcFetchRootPartitionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BROADCAST_RS_LIST, ObRpcBroadcastRsListP);
OB_DEFINE_PROCESSOR_S(Srv, OB_ADD_REPLICA, ObRpcAddReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_TENANT_LOG_ARCHIVE_STATUS, ObRpcGetTenantLogArchiveStatusP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REMOVE_NON_PAXOS_REPLICA, ObRpcRemoveNonPaxosReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REMOVE_MEMBER, ObRpcRemoveMemberP);
OB_DEFINE_PROCESSOR_S(Srv, OB_MIGRATE_REPLICA, ObRpcMigrateReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REBUILD_REPLICA, ObRpcRebuildReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_RESTORE_REPLICA, ObRpcRestoreReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_PHYSICAL_RESTORE_REPLICA, ObRpcPhysicalRestoreReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHANGE_REPLICA, ObRpcChangeReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_ADD_REPLICA_BATCH, ObRpcAddReplicaBatchP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REMOVE_NON_PAXOS_REPLICA_BATCH, ObRpcRemoveNonPaxosReplicaBatchP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REMOVE_MEMBER_BATCH, ObRpcRemoveMemberBatchP);
OB_DEFINE_PROCESSOR_S(Srv, OB_MIGRATE_REPLICA_BATCH, ObRpcMigrateReplicaBatchP);
OB_DEFINE_PROCESSOR_S(Srv, OB_STANDBY_CUTDATA_BATCH_TASK, ObRpcStandbyCutdataBatchTaskP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REBUILD_REPLICA_BATCH, ObRpcRebuildReplicaBatchP);
OB_DEFINE_PROCESSOR_S(Srv, OB_COPY_SSTABLE_BATCH, ObRpcCopySSTableBatchP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHANGE_REPLICA_BATCH, ObRpcChangeReplicaBatchP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_MIGRATE_TASK_EXIST, ObRpcCheckMigrateTaskExistP);
OB_DEFINE_PROCESSOR_S(Srv, OB_MINOR_FREEZE, ObRpcMinorFreezeP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_SCHEMA_VERSION_ELAPSED, ObRpcCheckSchemaVersionElapsedP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_CTX_CREATE_TIMESTAMP_ELAPSED, ObRpcCheckCtxCreateTimestampElapsedP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_MEMBER_LIST, ObRpcGetMemberListP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SWITCH_LEADER, ObRpcSwitchLeaderP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_SWITCH_RS_LEADER, ObRpcBatchSwitchRsLeaderP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SWITCH_LEADER_LIST, ObRpcSwitchLeaderListP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SWITCH_LEADER_LIST_ASYNC, ObRpcSwitchLeaderListAsyncP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_LEADER_CANDIDATES, ObRpcGetLeaderCandidatesP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_LEADER_CANDIDATES_ASYNC, ObRpcGetLeaderCandidatesAsyncP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_LEADER_CANDIDATES_ASYNC_V2, ObRpcGetLeaderCandidatesAsyncV2P);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_PARTITION_COUNT, ObRpcGetPartitionCountP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SWITCH_SCHEMA, ObRpcSwitchSchemaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REFRESH_MEMORY_STAT, ObRpcRefreshMemStatP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BOOTSTRAP, ObRpcBootstrapP);
OB_DEFINE_PROCESSOR_S(Srv, OB_IS_EMPTY_SERVER, ObRpcIsEmptyServerP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_DEPLOYMENT_MODE, ObRpcCheckDeploymentModeP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_PARTITION_STAT, ObRpcGetPartitionStatP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REFRESH_SYNC_VALUE, ObRpcSyncAutoincValueP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CLEAR_AUTOINC_CACHE, ObRpcClearAutoincCacheP);
OB_DEFINE_PROCESSOR_S(Srv, OB_DUMP_MEMTABLE, ObDumpMemtableP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_SET_AS_SINGLE_REPLICA, ObForceSetAsSingleReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_REMOVE_REPLICA, ObForceRemoveReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_SET_REPLICA_NUM, ObForceSetReplicaNumP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_RESET_PARENT, ObForceResetParentP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_SET_PARENT, ObForceSetParentP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_PURGE_MEMTABLE, ObHaltPrewarmP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_PURGE_MEMTABLE_ASYNC, ObHaltPrewarmAsyncP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_SWITCH_ILOG_FILE, ObForceSwitchILogFileP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_SET_ALL_AS_SINGLE_REPLICA, ObForceSetAllAsSingleReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_MODIFY_QUORUM_BATCH, ObRpcModifyQuorumBatchP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BACKUP_REPLICA_BATCH, ObRpcBackupReplicaBatchP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_SET_SERVER_LIST, ObForceSetServerListP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_BACKUP_TASK_EXIST, ObRpcCheckBackupTaskExistP);
OB_DEFINE_PROCESSOR_S(Common, OB_CHECK_BACKUP_SCHEDULER_WORKING, ObRpcCheckBackupSchuedulerWorkingP);
OB_DEFINE_PROCESSOR_S(Srv, OB_VALIDATE_BACKUP_BATCH, ObRpcValidateBackupBatchP);

OB_DEFINE_PROCESSOR_S(Srv, OB_REPORT_REPLICA, ObReportReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REPORT_SINGLE_REPLICA, ObReportSingleReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_RECYCLE_REPLICA, ObRecycleReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CLEAR_LOCATION_CACHE, ObClearLocationCacheP);
OB_DEFINE_PROCESSOR_S(Srv, OB_DROP_REPLICA, ObDropReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SET_DS_ACTION, ObSetDSActionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REQUEST_HEARTBEAT, ObRequestHeartbeatP);
OB_DEFINE_PROCESSOR_S(Srv, OB_UPDATE_CLUSTER_INFO, ObUpdateClusterInfoP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BROADCAST_SYS_SCHEMA, ObBroadcastSysSchemaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_PARTITION_TABLE, ObCheckPartitionTableP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SYNC_PARTITION_TABLE, ObSyncPartitionTableP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SYNC_PG_PARTITION_TABLE, ObSyncPGPartitionMTP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_MEMBER_LIST_AND_LEADER, ObRpcGetMemberListAndLeaderP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_MEMBER_LIST_AND_LEADER_V2, ObRpcGetMemberListAndLeaderV2P);
OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_GET_MEMBER_LIST_AND_LEADER, ObRpcBatchGetMemberListAndLeaderP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_GET_ROLE, ObRpcBatchGetRoleP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_GET_PROTECTION_LEVEL, ObRpcBatchGetProtectionLevelP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_NEED_OFFLINE_REPLICA, ObRpcCheckNeedOffineReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_FLASHBACK_INFO_DUMP, ObRpcCheckFlashbackInfoDumpP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FLUSH_CACHE, ObFlushCacheP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SET_TP, ObRpcSetTPP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CANCEL_SYS_TASK, ObCancelSysTaskP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SET_DISK_VALID, ObSetDiskValidP);
OB_DEFINE_PROCESSOR_S(Srv, OB_ADD_DISK, ObAddDiskP);
OB_DEFINE_PROCESSOR_S(Srv, OB_DROP_DISK, ObDropDiskP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_DANGLING_REPLICA_EXIST, ObCheckDanglingReplicaExistP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_UNIQUE_INDEX_REQUEST, ObCheckUniqueIndexRequestP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CALC_COLUMN_CHECKSUM_REQUEST, ObCalcColumnChecksumRequestP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SPLIT_PARTITION, ObRpcSplitPartitionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_SPLIT_PARTITION, ObRpcBatchSplitPartitionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_SINGLE_REPLICA_MAJOR_SSTABLE_EXIST, ObCheckSingleReplicaMajorSSTableExistP);
OB_DEFINE_PROCESSOR_S(
    Srv, OB_CHECK_SINGLE_REPLICA_MAJOR_SSTABLE_EXIST_WITH_TIME, ObCheckSingleReplicaMajorSSTableExistWithTimeP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_ALL_REPLICA_MAJOR_SSTABLE_EXIST, ObCheckAllReplicaMajorSSTableExistP);
OB_DEFINE_PROCESSOR_S(
    Srv, OB_CHECK_ALL_REPLICA_MAJOR_SSTABLE_EXIST_WITH_TIME, ObCheckAllReplicaMajorSSTableExistWithTimeP);
OB_DEFINE_PROCESSOR_S(Srv, OB_QUERY_MAX_DECIDED_TRANS_VERSION, ObQueryMaxDecidedTransVersionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_QUERY_IS_VALID_MEMBER, ObQueryIsValidMemberP);
OB_DEFINE_PROCESSOR_S(Srv, OB_QUERY_MAX_FLUSHED_ILOG_ID, ObQueryMaxFlushedILogIdP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_DISABLE_BLACKLIST, ObForceDisableBlacklistP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_ENABLE_BLACKLIST, ObForceEnableBlacklistP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_CLEAR_BLACKLIST, ObForceClearBlacklistP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_WAIT_LEADER, ObBatchWaitLeaderP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_WRITE_CUTDATA_CLOG, ObBatchWriteCutdataClogP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_START_ELECTION, ObBatchTranslatePartitionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_PARTITION_CHECK_LOG, ObCheckPartitionLogP);
OB_DEFINE_PROCESSOR_S(Srv, OB_PARTITION_STOP_WRITE, ObStopPartitionWriteP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SERVER_UPDATE_STAT_CACHE, ObUpdateLocalStatCacheP);
OB_DEFINE_PROCESSOR_S(Srv, OB_ESTIMATE_PARTITION_ROWS, ObEstimatePartitionRowsP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_WRS_INFO, ObGetWRSInfoP);
OB_DEFINE_PROCESSOR_S(Srv, OB_HA_GTS_PING_REQUEST, ObHaGtsPingRequestP);
OB_DEFINE_PROCESSOR_S(Srv, OB_HA_GTS_GET_REQUEST, ObHaGtsGetRequestP);
OB_DEFINE_PROCESSOR_S(Srv, OB_HA_GTS_GET_RESPONSE, ObHaGtsGetResponseP);
OB_DEFINE_PROCESSOR_S(Srv, OB_HA_GTS_HEARTBEAT, ObHaGtsHeartbeatP);
OB_DEFINE_PROCESSOR_S(Srv, OB_HA_GTS_UPDATE_META, ObHaGtsUpdateMetaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_HA_GTS_CHANGE_MEMBER, ObHaGtsChangeMemberP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_SET_MEMBER_LIST, ObSetMemberListBatchP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_TENANT_REFRESHED_SCHEMA_VERSION, ObGetTenantSchemaVersionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_REMOTE_TENANT_GROUP_STRING, ObGetRemoteTenantGroupStringP);
OB_DEFINE_PROCESSOR_S(Srv, OB_UPDATE_TENANT_MEMORY, ObUpdateTenantMemoryP);
OB_DEFINE_PROCESSOR_S(Common, OB_GET_MASTER_RS, ObRpcGetMasterRSP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_PHYSICAL_FLASHBACK_SUCC, ObCheckPhysicalFlashbackSUCCP);
OB_DEFINE_PROCESSOR_S(Srv, OB_RENEW_IN_ZONE_HB, ObRenewInZoneHbP);
OB_DEFINE_PROCESSOR_S(Srv, OB_KILL_PART_TRANS_CTX, ObKillPartTransCtxP);
}  // end of namespace observer
}  // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OB_RPC_PROCESSOR_SIMPLE_H_ */
