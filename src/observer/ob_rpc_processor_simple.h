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
#include "rpc/obrpc/ob_rpc_packet.h"

#define OB_DEFINE_PROCESSOR(cls, pcode, pname)                          \
  class pname : public obrpc::ObRpcProcessor<                           \
    obrpc::Ob ## cls ## RpcProxy::ObRpc<pcode> >

#define OB_DEFINE_PROCESSOR_S(cls, pcode, pname)        \
  OB_DEFINE_PROCESSOR(cls, obrpc::pcode, pname)         \
  {                                                     \
  public:                                               \
    explicit pname(const ObGlobalContext &gctx)         \
        : gctx_(gctx)                                   \
    {}                                                  \
  protected: int process();                             \
  private:                                              \
    const ObGlobalContext &gctx_ __maybe_unused;        \
  }

#define OB_DEFINE_PROCESSOR_SM(cls, pcode, pname)       \
  OB_DEFINE_PROCESSOR(cls, obrpc::pcode, pname)         \
  {                                                     \
  public:                                               \
    explicit pname(const ObGlobalContext &gctx)         \
        : gctx_(gctx)                                   \
    {}                                                  \
  protected: int process(); int after_process(int error_code);        \
  private:                                              \
    const ObGlobalContext &gctx_;                       \
  }

#define RPC_PROCESSOR_X(pcode, pname)           \
  OB_DEFINE_PROCESSOR(Srv, obrpc::pcode, pname) \
  {                                             \
  public:                                       \
    explicit pname(int ret) : ret_(ret) {}  \
  protected:                                    \
    int process();                              \
    int deserialize() override;                         \
private:                                                \
    int ret_;                                           \
  }

#define OB_DEFINE_PROCESSOR_OBADMIN(cls, pcode, pname)        \
  OB_DEFINE_PROCESSOR(cls, obrpc::pcode, pname)         \
  {                                                     \
  public:                                               \
    explicit pname(const ObGlobalContext &gctx)         \
        : gctx_(gctx)                                   \
    {}                                                  \
  protected: int process();                             \
  int before_process() { return req_->is_from_unix_domain()? OB_SUCCESS : OB_NOT_SUPPORTED;} \
  private:                                              \
    const ObGlobalContext &gctx_ __maybe_unused;        \
  }

namespace oceanbase
{
namespace observer
{

OB_DEFINE_PROCESSOR(Srv, obrpc::OB_GET_DIAGNOSE_ARGS, ObGetDiagnoseArgsP)
{
public:
  ObGetDiagnoseArgsP()
      : pwbuf_(), passwd_(), argsbuf_()
  {
    passwd_.assign_buffer(pwbuf_, sizeof (pwbuf_));
  }

protected:
  int process();
  int before_process() { return req_->is_from_unix_domain()? OB_SUCCESS : OB_NOT_SUPPORTED;}

private:
  char pwbuf_[64];
  common::ObString passwd_;
  char argsbuf_[1024];
};


struct ObGlobalContext;

RPC_PROCESSOR_X(OB_ERROR_PACKET, ObErrorP);

OB_DEFINE_PROCESSOR_S(Srv, OB_GET_ROOT_SERVER_ROLE, ObRpcGetRoleP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_SET_CONFIG, ObRpcSetConfigP);

OB_DEFINE_PROCESSOR_S(Srv, OB_GET_CONFIG, ObRpcGetConfigP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_SET_TENANT_CONFIG, ObRpcSetTenantConfigP);
OB_DEFINE_PROCESSOR_S(Srv, OB_NOTIFY_TENANT_SERVER_UNIT_RESOURCE, ObRpcNotifyTenantServerUnitResourceP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_FROZEN_SCN, ObCheckFrozenVersionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_MIN_SSTABLE_SCHEMA_VERSION, ObGetMinSSTableSchemaVersionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_INIT_TENANT_CONFIG, ObInitTenantConfigP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_LEADER_LOCATIONS, ObGetLeaderLocationsP);
OB_DEFINE_PROCESSOR_S(Srv, OB_NOTIFY_SWITCH_LEADER, ObRpcNotifySwitchLeaderP);

// oceanbase service provied
OB_DEFINE_PROCESSOR_S(Srv, OB_FETCH_SYS_LS, ObRpcFetchSysLSP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BROADCAST_RS_LIST, ObRpcBroadcastRsListP);
OB_DEFINE_PROCESSOR_S(Srv, OB_MINOR_FREEZE, ObRpcMinorFreezeP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_SCHEMA_VERSION_ELAPSED, ObRpcCheckSchemaVersionElapsedP);
OB_DEFINE_PROCESSOR_S(Srv, OB_DDL_BUILD_SINGLE_REPLICA_REQUEST, ObRpcBuildDDLSingleReplicaRequestP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FETCH_TABLET_AUTOINC_SEQ_CACHE, ObRpcFetchTabletAutoincSeqCacheP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_GET_TABLET_AUTOINC_SEQ, ObRpcBatchGetTabletAutoincSeqP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_SET_TABLET_AUTOINC_SEQ, ObRpcBatchSetTabletAutoincSeqP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_MODIFY_TIME_ELAPSED, ObRpcCheckCtxCreateTimestampElapsedP);
OB_DEFINE_PROCESSOR_S(Srv, OB_UPDATE_BASELINE_SCHEMA_VERSION, ObRpcUpdateBaselineSchemaVersionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SWITCH_LEADER, ObRpcSwitchLeaderP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_BATCH_SWITCH_RS_LEADER, ObRpcBatchSwitchRsLeaderP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_PARTITION_COUNT, ObRpcGetPartitionCountP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SWITCH_SCHEMA, ObRpcSwitchSchemaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REFRESH_MEMORY_STAT, ObRpcRefreshMemStatP);
OB_DEFINE_PROCESSOR_S(Srv, OB_WASH_MEMORY_FRAGMENTATION, ObRpcWashMemFragmentationP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BOOTSTRAP, ObRpcBootstrapP);
OB_DEFINE_PROCESSOR_S(Srv, OB_IS_EMPTY_SERVER, ObRpcIsEmptyServerP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_SERVER_FOR_ADDING_SERVER, ObRpcCheckServerForAddingServerP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_DEPLOYMENT_MODE, ObRpcCheckDeploymentModeP);
OB_DEFINE_PROCESSOR_S(Srv, OB_NOTIFY_CREATE_TENANT_USER_LS, ObRpcCreateTenantUserLSP);
#ifdef OB_BUILD_TDE_SECURITY
OB_DEFINE_PROCESSOR_S(Srv, OB_WAIT_MASTER_KEY_IN_SYNC, ObRpcWaitMasterKeyInSyncP);
#endif
OB_DEFINE_PROCESSOR_S(Srv, OB_REFRESH_SYNC_VALUE, ObRpcSyncAutoincValueP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CLEAR_AUTOINC_CACHE, ObRpcClearAutoincCacheP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_DUMP_MEMTABLE, ObDumpMemtableP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_DUMP_TX_DATA_MEMTABLE, ObDumpTxDataMemtableP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_DUMP_SINGLE_TX_DATA, ObDumpSingleTxDataP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_FORCE_PURGE_MEMTABLE, ObHaltPrewarmP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FORCE_PURGE_MEMTABLE_ASYNC, ObHaltPrewarmAsyncP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_FORCE_SWITCH_ILOG_FILE, ObForceSwitchILogFileP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_FORCE_SET_ALL_AS_SINGLE_REPLICA, ObForceSetAllAsSingleReplicaP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_FORCE_SET_SERVER_LIST, ObForceSetServerListP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_BACKUP_TASK_EXIST, ObRpcCheckBackupTaskExistP);
OB_DEFINE_PROCESSOR_S(Common, OB_CHECK_BACKUP_SCHEDULER_WORKING, ObRpcCheckBackupSchuedulerWorkingP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BACKUP_LS_DATA, ObRpcBackupLSDataP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BACKUP_COMPL_LOG, ObRpcBackupLSComplLOGP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BACKUP_BUILD_INDEX, ObRpcBackupBuildIndexP);
OB_DEFINE_PROCESSOR_S(Srv, OB_DELETE_BACKUP_LS_TASK, ObRpcBackupLSCleanP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BACKUP_META, ObRpcBackupMetaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_NOTIFY_CREATE_DUPLICATE_LS, ObRpcCreateDuplicateLSP);
OB_DEFINE_PROCESSOR_S(Srv, OB_DELETE_BACKUP_LS_TASK_RES, ObRpcBackupCleanLSResP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BACKUP_LS_DATA_RES, ObRpcBackupLSDataResP);
OB_DEFINE_PROCESSOR_S(Srv, OB_NOTIFY_ARCHIVE, ObRpcNotifyArchiveP);

OB_DEFINE_PROCESSOR_S(Srv, OB_LS_MIGRATE_REPLICA, ObRpcLSMigrateReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_LS_ADD_REPLICA, ObRpcLSAddReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_LS_TYPE_TRANSFORM, ObRpcLSTypeTransformP);
OB_DEFINE_PROCESSOR_S(Srv, OB_LS_REMOVE_PAXOS_REPLICA, ObRpcLSRemovePaxosReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_LS_REMOVE_NONPAXOS_REPLICA, ObRpcLSRemoveNonPaxosReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_LS_MODIFY_PAXOS_REPLICA_NUMBER, ObRpcLSModifyPaxosReplicaNumberP);
OB_DEFINE_PROCESSOR_S(Srv, OB_LS_CHECK_DR_TASK_EXIST, ObRpcLSCheckDRTaskExistP);
#ifdef OB_BUILD_ARBITRATION
OB_DEFINE_PROCESSOR_S(Srv, OB_ADD_ARB, ObRpcAddArbP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REMOVE_ARB, ObRpcRemoveArbP);
#endif
OB_DEFINE_PROCESSOR_S(Srv, OB_REPORT_REPLICA, ObReportReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_RECYCLE_REPLICA, ObRecycleReplicaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CLEAR_LOCATION_CACHE, ObClearLocationCacheP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SET_DS_ACTION, ObSetDSActionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REQUEST_HEARTBEAT, ObRequestHeartbeatP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REFRESH_IO_CALIBRATION, ObRefreshIOCalibrationP);
OB_DEFINE_PROCESSOR_S(Srv, OB_EXECUTE_IO_BENCHMARK, ObExecuteIOBenchmarkP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SYNC_PARTITION_TABLE, ObSyncPartitionTableP);
OB_DEFINE_PROCESSOR_S(Srv, OB_DETECT_MASTER_RS_LS, ObRpcDetectMasterRsLSP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FLUSH_CACHE, ObFlushCacheP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SET_TP, ObRpcSetTPP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CANCEL_SYS_TASK, ObCancelSysTaskP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SET_DISK_VALID, ObSetDiskValidP);
OB_DEFINE_PROCESSOR_S(Srv, OB_ADD_DISK, ObAddDiskP);
OB_DEFINE_PROCESSOR_S(Srv, OB_DROP_DISK, ObDropDiskP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CALC_COLUMN_CHECKSUM_REQUEST, ObCalcColumnChecksumRequestP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_FORCE_DISABLE_BLACKLIST, ObForceDisableBlacklistP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_FORCE_ENABLE_BLACKLIST, ObForceEnableBlacklistP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_FORCE_CLEAR_BLACKLIST, ObForceClearBlacklistP);
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
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_TENANT_REFRESHED_SCHEMA_VERSION, ObGetTenantSchemaVersionP);
OB_DEFINE_PROCESSOR_OBADMIN(Srv, OB_UPDATE_TENANT_MEMORY, ObUpdateTenantMemoryP);
OB_DEFINE_PROCESSOR_S(Srv, OB_RENEW_IN_ZONE_HB, ObRenewInZoneHbP);
OB_DEFINE_PROCESSOR_S(Srv, OB_PRE_PROCESS_SERVER, ObPreProcessServerP);
#ifdef OB_BUILD_TDE_SECURITY
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_MASTER_KEY, ObGetMasterKeyP);
OB_DEFINE_PROCESSOR_S(Srv, OB_RESTORE_KEY, ObRestoreKeyP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SET_ROOT_KEY, ObSetRootKeyP);
#endif
OB_DEFINE_PROCESSOR_S(Srv, OB_HANDLE_PART_TRANS_CTX, ObHandlePartTransCtxP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SERVER_FLUSH_OPT_STAT_MONITORING_INFO, ObFlushLocalOptStatMonitoringInfoP);
#ifdef OB_BUILD_TDE_SECURITY
OB_DEFINE_PROCESSOR_S(Srv, OB_DUMP_TENANT_CACHE_MASTER_KEY, ObDumpTenantCacheMasterKeyP);
#endif
OB_DEFINE_PROCESSOR_S(Srv, OB_SET_MEMBER_LIST, ObRpcSetMemberListP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CREATE_LS, ObRpcCreateLSP);
#ifdef OB_BUILD_ARBITRATION
OB_DEFINE_PROCESSOR_S(Srv, OB_CREATE_ARB, ObRpcCreateArbP);
OB_DEFINE_PROCESSOR_S(Srv, OB_DELETE_ARB, ObRpcDeleteArbP);
#endif
OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_BROADCAST_SCHEMA, ObBatchBroadcastSchemaP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REMOTE_WRITE_DDL_REDO_LOG, ObRpcRemoteWriteDDLRedoLogP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REMOTE_WRITE_DDL_COMMIT_LOG, ObRpcRemoteWriteDDLCommitLogP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_LS_CAN_OFFLINE, ObRpcCheckLSCanOfflineP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CLEAN_SEQUENCE_CACHE, ObCleanSequenceCacheP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REGISTER_TX_DATA, ObRegisterTxDataP);
OB_DEFINE_PROCESSOR_S(Srv, OB_QUERY_LS_IS_VALID_MEMBER, ObQueryLSIsValidMemberP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECKPOINT_SLOG, ObCheckpointSlogP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHECK_BACKUP_DEST_CONNECTIVITY, ObRpcCheckBackupDestConnectivityP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_LS_ACCESS_MODE, ObRpcGetLSAccessModeP);
OB_DEFINE_PROCESSOR_S(Srv, OB_CHANGE_LS_ACCESS_MODE, ObRpcChangeLSAccessModeP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_LS_SYNC_SCN, ObRpcGetLSSyncScnP);
OB_DEFINE_PROCESSOR_S(Srv, OB_LOG_FORCE_SET_LS_AS_SINGLE_REPLICA, ObForceSetLSAsSingleReplicaP);
#ifdef OB_BUILD_SPM
OB_DEFINE_PROCESSOR_S(Srv, OB_SERVER_ACCEPT_PLAN_BASELINE, ObServerAcceptPlanBaselineP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SERVER_CANCEL_EVOLVE_TASK, ObServerCancelEvolveTaskP);
OB_DEFINE_PROCESSOR_S(Srv, OB_LOAD_BASELINE, ObLoadBaselineP);
OB_DEFINE_PROCESSOR_S(Srv, OB_LOAD_BASELINE_V2, ObLoadBaselineV2P);
#endif
OB_DEFINE_PROCESSOR_S(Srv, OB_ESTIMATE_TABLET_BLOCK_COUNT, ObEstimateTabletBlockCountP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GEN_UNIQUE_ID, ObRpcGenUniqueIDP);
OB_DEFINE_PROCESSOR_S(Srv, OB_START_TRANSFER_TASK, ObRpcStartTransferTaskP);
OB_DEFINE_PROCESSOR_S(Srv, OB_FINISH_TRANSFER_TASK, ObRpcFinishTransferTaskP);
OB_DEFINE_PROCESSOR_S(Srv, OB_DDL_CHECK_TABLET_MERGE_STATUS, ObRpcDDLCheckTabletMergeStatusP);
OB_DEFINE_PROCESSOR_S(Srv, OB_REFRESH_TENANT_INFO, ObRefreshTenantInfoP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SYNC_REWRITE_RULES, ObSyncRewriteRulesP);
OB_DEFINE_PROCESSOR_SM(Srv, OB_SESS_INFO_VERIFICATION, ObSessInfoVerificationP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SEND_HEARTBEAT, ObRpcSendHeartbeatP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_SERVER_RESOURCE_INFO, ObRpcGetServerResourceInfoP);
OB_DEFINE_PROCESSOR_S(Srv, OB_UPDATE_TENANT_INFO_CACHE, ObUpdateTenantInfoCacheP);
OB_DEFINE_PROCESSOR_S(Srv, OB_BROADCAST_CONSENSUS_VERSION, ObBroadcastConsensusVersionP);
OB_DEFINE_PROCESSOR_S(Srv, OB_GET_LS_REPLAYED_SCN, ObRpcGetLSReplayedScnP);
OB_DEFINE_PROCESSOR_S(Srv, OB_TABLE_TTL, ObTenantTTLP);
OB_DEFINE_PROCESSOR_S(Srv, OB_HA_UNLOCK_MEMBER_LIST, ObAdminUnlockMemberListP);

} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OB_RPC_PROCESSOR_SIMPLE_H_ */
