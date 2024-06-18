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

#include "share/interrupt/ob_interrupt_rpc_proxy.h"
#include "observer/ob_srv_xlator.h"

#include "share/ob_tenant_mgr.h"
#include "share/schema/ob_schema_service_rpc_proxy.h"
#include "share/ratelimit/ob_rl_rpc.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "share/rpc/ob_batch_processor.h"
#include "share/rpc/ob_blacklist_req_processor.h"
#include "share/rpc/ob_blacklist_resp_processor.h"
#include "share/deadlock/ob_deadlock_detector_rpc.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "sql/engine/cmd/ob_kill_executor.h"
#include "sql/engine/cmd/ob_load_data_rpc.h"
#include "sql/engine/px/ob_px_rpc_processor.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_process.h"
#include "sql/das/ob_das_id_rpc.h"
#include "sql/dtl/ob_dtl_rpc_processor.h"
#include "storage/tablelock/ob_table_lock_rpc_processor.h"
#include "sql/engine/px/ob_px_bloom_filter.h"
#include "storage/tx/ob_trans_rpc.h"
#include "storage/tx/ob_gts_rpc.h"
#include "storage/tx/ob_gti_rpc.h"
#include "storage/tx/ob_dup_table_rpc.h"
#include "storage/tx/ob_ts_response_handler.h"
#include "storage/tx/wrs/ob_weak_read_service_rpc_define.h"  // weak_read_service
#include "observer/ob_rpc_processor_simple.h"
#include "observer/ob_srv_task.h"

#include "observer/table/ob_table_rpc_processor.h"
#include "observer/table/ob_table_execute_processor.h"
#include "observer/table/ob_table_batch_execute_processor.h"
#include "observer/table/ob_table_query_processor.h"
#include "observer/table/ob_table_query_and_mutate_processor.h"
#include "observer/table/ob_table_query_async_processor.h"
#include "observer/table/ob_table_direct_load_processor.h"
#include "storage/ob_storage_rpc.h"

#include "logservice/restoreservice/ob_log_restore_rpc_define.h"
#include "rootserver/freeze/ob_major_freeze_rpc_define.h"        // ObTenantMajorFreezeP
#include "storage/tx/ob_xa_rpc.h"

#include "observer/table_load/ob_table_load_rpc_processor.h"
#include "observer/table_load/resource/ob_table_load_resource_processor.h"
#include "observer/net/ob_net_endpoint_ingress_rpc_processor.h"
#include "share/wr/ob_wr_snapshot_rpc_processor.h"

using namespace oceanbase;
using namespace oceanbase::observer;
using namespace oceanbase::lib;
using namespace oceanbase::rpc;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::transaction::tablelock;
using namespace oceanbase::obrpc;
using namespace oceanbase::obmysql;
using namespace oceanbase::share;

void oceanbase::observer::init_srv_xlator_for_partition(ObSrvRpcXlator *xlator) {
  // RPC_PROCESSOR(ObGetMemberListP, gctx_.par_ser_);
//  RPC_PROCESSOR(ObReachPartitionLimitP, gctx_);
  // RPC_PROCESSOR(ObPTSAddMemberP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObPTSRemoveMemberP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObPTSRemoveReplicaP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObIsMemberChangeDoneP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObWarmUpRequestP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObBatchRemoveMemberP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObBatchAddMemberP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObBatchMemberChangeDoneP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObFetchMacroBlockOldP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObDumpMemtableP, gctx_);
  RPC_PROCESSOR(ObDumpTxDataMemtableP, gctx_);
  RPC_PROCESSOR(ObDumpSingleTxDataP, gctx_);
  RPC_PROCESSOR(ObForceSwitchILogFileP, gctx_);
  RPC_PROCESSOR(ObForceSetAllAsSingleReplicaP, gctx_);
  RPC_PROCESSOR(ObForceSetLSAsSingleReplicaP, gctx_);
  // RPC_PROCESSOR(ObSplitDestPartitionRequestP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObReplicaSplitProgressRequestP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObCheckMemberMajorSSTableEnoughP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObBatchRemoveReplicaP, gctx_.par_ser_);
  RPC_PROCESSOR(ObForceDisableBlacklistP, gctx_);
  RPC_PROCESSOR(ObForceEnableBlacklistP, gctx_);
  RPC_PROCESSOR(ObForceClearBlacklistP, gctx_);
  RPC_PROCESSOR(ObAddDiskP, gctx_);
  RPC_PROCESSOR(ObDropDiskP, gctx_);
  RPC_PROCESSOR(ObForceSetServerListP, gctx_);
#ifdef OB_BUILD_TDE_SECURITY
  RPC_PROCESSOR(ObGetMasterKeyP, gctx_);
  RPC_PROCESSOR(ObRestoreKeyP, gctx_);
  RPC_PROCESSOR(ObSetRootKeyP, gctx_);
  RPC_PROCESSOR(ObCloneKeyP, gctx_);
  RPC_PROCESSOR(ObTrimKeyListP, gctx_);
#endif
  RPC_PROCESSOR(ObHandlePartTransCtxP, gctx_);
#ifdef OB_BUILD_TDE_SECURITY
  RPC_PROCESSOR(ObDumpTenantCacheMasterKeyP, gctx_);
#endif
  RPC_PROCESSOR(ObRpcSetMemberListP, gctx_);
  RPC_PROCESSOR(ObRpcCreateLSP, gctx_);
#ifdef OB_BUILD_ARBITRATION
  RPC_PROCESSOR(ObRpcCreateArbP, gctx_);
  RPC_PROCESSOR(ObRpcDeleteArbP, gctx_);
#endif
  RPC_PROCESSOR(ObRpcCheckLSCanOfflineP, gctx_);
  RPC_PROCESSOR(ObCleanSequenceCacheP, gctx_);
  RPC_PROCESSOR(ObRegisterTxDataP, gctx_);
  RPC_PROCESSOR(ObRpcGetLSAccessModeP, gctx_);
  RPC_PROCESSOR(ObRpcChangeLSAccessModeP, gctx_);
  RPC_PROCESSOR(ObTabletLocationReceiveP, gctx_);
  RPC_PROCESSOR(ObForceSetTenantLogDiskP, gctx_);
  RPC_PROCESSOR(ObForceDumpServerUsageP, gctx_);
}

void oceanbase::observer::init_srv_xlator_for_migrator(ObSrvRpcXlator *xlator) {
  // RPC_PROCESSOR(ObFetchPartitionInfoP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObFetchTableInfoP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObFetchLogicBaseMetaP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  // RPC_PROCESSOR(ObFetchPhysicalBaseMetaP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  // RPC_PROCESSOR(ObFetchLogicRowP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  // RPC_PROCESSOR(ObFetchLogicDataChecksumP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  // RPC_PROCESSOR(ObFetchLogicDataChecksumSliceP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  // RPC_PROCESSOR(ObFetchLogicRowSliceP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  // RPC_PROCESSOR(ObFetchMacroBlockP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  // RPC_PROCESSOR(ObFetchOFSMacroBlockP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  // RPC_PROCESSOR(ObFetchPartitionGroupInfoP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObFetchPGPartitioninfoP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  // RPC_PROCESSOR(ObCheckMemberPGMajorSSTableEnoughP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObFetchReplicaInfoP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObSuspendPartitionP, gctx_.par_ser_);
  // RPC_PROCESSOR(ObHandoverPartitionP, gctx_.par_ser_);
}

void oceanbase::observer::init_srv_xlator_for_migration(ObSrvRpcXlator *xlator)
{
  RPC_PROCESSOR(ObHAFetchMacroBlockP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObFetchTabletInfoP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObFetchSSTableInfoP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObFetchLSInfoP);
  RPC_PROCESSOR(ObFetchLSMetaInfoP);
  RPC_PROCESSOR(ObFetchLSMemberListP);
  RPC_PROCESSOR(ObFetchSSTableMacroInfoP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObStorageFetchLSViewP, gctx_.bandwidth_throttle_);

  // restore
  RPC_PROCESSOR(ObNotifyRestoreTabletsP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObInquireRestoreP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObUpdateLSMetaP, gctx_.bandwidth_throttle_);

  //transfer
  RPC_PROCESSOR(ObCheckStartTransferTabletsP);
  RPC_PROCESSOR(ObGetLSActiveTransCountP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObGetTransferStartScnP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObFetchLSReplayScnP);
  RPC_PROCESSOR(ObCheckTransferTabletsBackfillP);
  RPC_PROCESSOR(ObStorageGetConfigVersionAndTransferScnP);
  RPC_PROCESSOR(ObStorageSubmitTxLogP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObStorageGetTransferDestPrepareSCNP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObStorageLockConfigChangeP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObStorageUnlockConfigChangeP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObStorageGetLogConfigStatP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObStorageWakeupTransferServiceP, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObFetchLSMemberAndLearnerListP);
  RPC_PROCESSOR(ObAdminUnlockMemberListP, gctx_);
}

void oceanbase::observer::init_srv_xlator_for_others(ObSrvRpcXlator *xlator) {
  RPC_PROCESSOR(ObGtsP);
  RPC_PROCESSOR(ObGtsErrRespP);
  RPC_PROCESSOR(ObGtiP);
  RPC_PROCESSOR(ObDASIDP);

  // Weakly Consistent Read Related
  RPC_PROCESSOR(ObWrsGetClusterVersionP, gctx_.weak_read_service_);
  RPC_PROCESSOR(ObWrsClusterHeartbeatP, gctx_.weak_read_service_);

  // update optimizer statistic
  RPC_PROCESSOR(ObUpdateLocalStatCacheP, gctx_);
  RPC_PROCESSOR(ObInitSqcP, gctx_);
  RPC_PROCESSOR(ObInitTaskP, gctx_);
  RPC_PROCESSOR(ObInitFastSqcP, gctx_);
  RPC_PROCESSOR(ObPxP2pDhMsgP, gctx_);
  RPC_PROCESSOR(ObPxP2pDhClearMsgP, gctx_);
  RPC_PROCESSOR(ObPxTenantTargetMonitorP, gctx_);
  RPC_PROCESSOR(ObPxCleanDtlIntermResP, gctx_);
  // SQL Estimate
  RPC_PROCESSOR(ObEstimatePartitionRowsP, gctx_);

  // table api
  RPC_PROCESSOR(ObTableLoginP, gctx_);
  RPC_PROCESSOR(ObTableApiExecuteP, gctx_);
  RPC_PROCESSOR(ObTableBatchExecuteP, gctx_);
  RPC_PROCESSOR(ObTableQueryP, gctx_);
  RPC_PROCESSOR(ObTableQueryAndMutateP, gctx_);
  RPC_PROCESSOR(ObTableQueryAsyncP, gctx_);
  RPC_PROCESSOR(ObTableDirectLoadP, gctx_);
  RPC_PROCESSOR(ObTenantTTLP, gctx_);

  // HA GTS
  RPC_PROCESSOR(ObHaGtsPingRequestP, gctx_);
  RPC_PROCESSOR(ObHaGtsGetRequestP, gctx_);
  RPC_PROCESSOR(ObHaGtsGetResponseP, gctx_);
  RPC_PROCESSOR(ObHaGtsHeartbeatP, gctx_);
  RPC_PROCESSOR(ObHaGtsUpdateMetaP, gctx_);
  RPC_PROCESSOR(ObHaGtsChangeMemberP, gctx_);

  // XA transaction rpc
  RPC_PROCESSOR(ObXAPrepareP);
  RPC_PROCESSOR(ObXAStartP);
  RPC_PROCESSOR(ObXAStartResponseP);
  RPC_PROCESSOR(ObXAEndP);
  RPC_PROCESSOR(ObXAStartStmtP);
  RPC_PROCESSOR(ObXAStartStmtResponseP);
  RPC_PROCESSOR(ObXAEndStmtP);
  RPC_PROCESSOR(ObXACommitP);
  RPC_PROCESSOR(ObXAHbReqP);
  RPC_PROCESSOR(ObXAHbRespP);
  RPC_PROCESSOR(ObXARollbackP);
  RPC_PROCESSOR(ObXATerminateP);

  // DeadLock rpc
  RPC_PROCESSOR(ObDeadLockCollectInfoMessageP, gctx_);
  RPC_PROCESSOR(ObDetectorLCLMessageP, gctx_);
  RPC_PROCESSOR(ObDeadLockNotifyParentMessageP, gctx_);

  // table lock rpc
  RPC_PROCESSOR(ObTableLockTaskP, gctx_);
  RPC_PROCESSOR(ObHighPriorityTableLockTaskP, gctx_);
  RPC_PROCESSOR(ObOutTransLockTableP, gctx_);
  RPC_PROCESSOR(ObOutTransUnlockTableP, gctx_);
  RPC_PROCESSOR(ObBatchLockTaskP, gctx_);
  RPC_PROCESSOR(ObHighPriorityBatchLockTaskP, gctx_);
  RPC_PROCESSOR(ObAdminRemoveLockP);
  RPC_PROCESSOR(ObAdminUpdateLockP);

  // Region Ratelimit rpc
  RPC_PROCESSOR(ObRLGetRegionBWP, gctx_);

  // flush opt stat monitoring info rpc
  RPC_PROCESSOR(ObFlushLocalOptStatMonitoringInfoP, gctx_);
  // send bloom filter
  RPC_PROCESSOR(ObSendBloomFilterP);
  // GC check member list
  RPC_PROCESSOR(ObQueryLSIsValidMemberP, gctx_);

  // Remote fetch log rpc
  RPC_PROCESSOR(ObRemoteFetchLogP);

  // tenant major freeze
  RPC_PROCESSOR(ObTenantMajorFreezeP);
  RPC_PROCESSOR(ObTenantAdminMergeP);

  // checkpoint slog rpc
  RPC_PROCESSOR(ObCheckpointSlogP, gctx_);

  // check connectivity
  RPC_PROCESSOR(ObRpcCheckBackupDestConnectivityP, gctx_);

  // global auto increment service rpc
  RPC_PROCESSOR(ObGAISNextAutoIncP);
  RPC_PROCESSOR(ObGAISCurrAutoIncP);
  RPC_PROCESSOR(ObGAISPushAutoIncP);
  RPC_PROCESSOR(ObGAISClearAutoIncCacheP);
  RPC_PROCESSOR(ObGAISNextSequenceP);

#ifdef OB_BUILD_SPM
  // sql plan baseline
  RPC_PROCESSOR(ObServerAcceptPlanBaselineP, gctx_);
  RPC_PROCESSOR(ObServerCancelEvolveTaskP, gctx_);
  RPC_PROCESSOR(ObLoadBaselineP, gctx_);
  RPC_PROCESSOR(ObLoadBaselineV2P, gctx_);
#endif

  //sql optimizer estimate tablet block count
  RPC_PROCESSOR(ObEstimateTabletBlockCountP, gctx_);

  // lob
  RPC_PROCESSOR(ObLobQueryP, gctx_.bandwidth_throttle_);
  //standby switchover/failover
  RPC_PROCESSOR(ObRpcGetLSSyncScnP, gctx_);
  RPC_PROCESSOR(ObRefreshTenantInfoP, gctx_);
  RPC_PROCESSOR(ObRpcGetLSReplayedScnP, gctx_);
  RPC_PROCESSOR(ObUpdateTenantInfoCacheP, gctx_);

  RPC_PROCESSOR(ObSyncRewriteRulesP, gctx_);

  RPC_PROCESSOR(ObNetEndpointRegisterP, gctx_);
  RPC_PROCESSOR(ObNetEndpointPredictIngressP, gctx_);
  RPC_PROCESSOR(ObNetEndpointSetIngressP, gctx_);
  RPC_PROCESSOR(ObRpcGetTenantResP, gctx_);

  // session info verification
  RPC_PROCESSOR(ObSessInfoVerificationP, gctx_);
  RPC_PROCESSOR(ObBroadcastConsensusVersionP, gctx_);

  // direct load
  RPC_PROCESSOR(ObDirectLoadControlP, gctx_);
  // direct load resource
  RPC_PROCESSOR(ObDirectLoadResourceP, gctx_);

  // wr
  RPC_PROCESSOR(ObWrAsyncSnapshotTaskP, gctx_);
  RPC_PROCESSOR(ObWrAsyncPurgeSnapshotTaskP, gctx_);
  RPC_PROCESSOR(ObWrSyncUserSubmitSnapshotTaskP, gctx_);
  RPC_PROCESSOR(ObWrSyncUserModifySettingsTaskP, gctx_);

  // kill client session
  RPC_PROCESSOR(ObKillClientSessionP, gctx_);
  // client session create time
  RPC_PROCESSOR(ObClientSessionConnectTimeP, gctx_);
  // limit calculator
  RPC_PROCESSOR(ObResourceLimitCalculatorP, gctx_);

  // ddl
  RPC_PROCESSOR(ObRpcCheckandCancelDDLComplementDagP, gctx_);

  RPC_PROCESSOR(ObGAISBroadcastAutoIncCacheP);
}
