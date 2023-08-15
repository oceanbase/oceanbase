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
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "share/rpc/ob_batch_processor.h"
#include "share/rpc/ob_blacklist_req_processor.h"
#include "share/rpc/ob_blacklist_resp_processor.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "sql/engine/cmd/ob_kill_executor.h"
#include "sql/engine/cmd/ob_load_data_rpc.h"
#include "sql/engine/px/ob_px_rpc_processor.h"
#include "sql/dtl/ob_dtl_rpc_processor.h"
#include "storage/tx/ob_trans_rpc.h"
#include "storage/tx/ob_gts_rpc.h"
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

using namespace oceanbase;
using namespace oceanbase::observer;
using namespace oceanbase::lib;
using namespace oceanbase::rpc;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::obrpc;
using namespace oceanbase::obmysql;

void oceanbase::observer::init_srv_xlator_for_storage(ObSrvRpcXlator *xlator) {
    // oceanbase service provied
    RPC_PROCESSOR(ObGetWRSInfoP, gctx_);
    RPC_PROCESSOR(ObStopPartitionWriteP, gctx_);
    RPC_PROCESSOR(ObCheckPartitionLogP, gctx_);
    RPC_PROCESSOR(ObRpcFetchSysLSP, gctx_);
    //RPC_PROCESSOR(ObRpcRemoveReplicaP, gctx_);
    RPC_PROCESSOR(ObRpcMinorFreezeP, gctx_);
    RPC_PROCESSOR(ObRpcCheckSchemaVersionElapsedP, gctx_);
    RPC_PROCESSOR(ObRpcCheckCtxCreateTimestampElapsedP, gctx_);
    RPC_PROCESSOR(ObRpcUpdateBaselineSchemaVersionP, gctx_);
    RPC_PROCESSOR(ObRpcSwitchLeaderP, gctx_);
    RPC_PROCESSOR(ObRpcBatchSwitchRsLeaderP, gctx_);
    RPC_PROCESSOR(ObRpcGetPartitionCountP, gctx_);
    RPC_PROCESSOR(ObRpcSwitchSchemaP, gctx_);
    RPC_PROCESSOR(ObRpcRefreshMemStatP, gctx_);
    RPC_PROCESSOR(ObRpcWashMemFragmentationP, gctx_);
    RPC_PROCESSOR(ObRpcBootstrapP, gctx_);
    RPC_PROCESSOR(ObRpcIsEmptyServerP, gctx_);
    RPC_PROCESSOR(ObRpcCheckServerForAddingServerP, gctx_);
    RPC_PROCESSOR(ObRpcCheckDeploymentModeP, gctx_);
#ifdef OB_BUILD_TDE_SECURITY
    RPC_PROCESSOR(ObRpcWaitMasterKeyInSyncP, gctx_);
#endif
    RPC_PROCESSOR(ObRpcSyncAutoincValueP, gctx_);
    RPC_PROCESSOR(ObRpcClearAutoincCacheP, gctx_);
    RPC_PROCESSOR(ObReportReplicaP, gctx_);
    RPC_PROCESSOR(ObRecycleReplicaP, gctx_);
    RPC_PROCESSOR(ObClearLocationCacheP, gctx_);
    RPC_PROCESSOR(ObSetDSActionP, gctx_);
    RPC_PROCESSOR(ObRefreshIOCalibrationP, gctx_);
    RPC_PROCESSOR(ObExecuteIOBenchmarkP, gctx_);
    RPC_PROCESSOR(ObSyncPartitionTableP, gctx_);
    RPC_PROCESSOR(ObCancelSysTaskP, gctx_);
    RPC_PROCESSOR(ObCalcColumnChecksumRequestP, gctx_);
    RPC_PROCESSOR(ObRpcBackupLSDataP, gctx_);
    RPC_PROCESSOR(ObRpcBackupLSComplLOGP, gctx_);
    RPC_PROCESSOR(ObRpcBackupBuildIndexP, gctx_);
    RPC_PROCESSOR(ObRpcBackupLSCleanP, gctx_);
    RPC_PROCESSOR(ObRpcBackupMetaP, gctx_);
    RPC_PROCESSOR(ObRpcBackupLSDataResP, gctx_);
    RPC_PROCESSOR(ObRpcBackupCleanLSResP, gctx_);
    RPC_PROCESSOR(ObRpcNotifyArchiveP, gctx_);

    RPC_PROCESSOR(ObRpcCheckBackupTaskExistP, gctx_);
    RPC_PROCESSOR(ObRenewInZoneHbP, gctx_);
    RPC_PROCESSOR(ObPreProcessServerP, gctx_);
    RPC_PROCESSOR(ObRpcBroadcastRsListP, gctx_);
    RPC_PROCESSOR(ObRpcBuildDDLSingleReplicaRequestP, gctx_);
    RPC_PROCESSOR(ObRpcFetchTabletAutoincSeqCacheP, gctx_);
    RPC_PROCESSOR(ObRpcBatchGetTabletAutoincSeqP, gctx_);
    RPC_PROCESSOR(ObRpcBatchSetTabletAutoincSeqP, gctx_);
    RPC_PROCESSOR(ObRpcRemoteWriteDDLRedoLogP, gctx_);
    RPC_PROCESSOR(ObRpcRemoteWriteDDLCommitLogP, gctx_);
    RPC_PROCESSOR(ObRpcLSMigrateReplicaP, gctx_);
    RPC_PROCESSOR(ObRpcLSAddReplicaP, gctx_);
    RPC_PROCESSOR(ObRpcLSTypeTransformP, gctx_);
    RPC_PROCESSOR(ObRpcLSRemovePaxosReplicaP, gctx_);
    RPC_PROCESSOR(ObRpcLSRemoveNonPaxosReplicaP, gctx_);
    RPC_PROCESSOR(ObRpcLSModifyPaxosReplicaNumberP, gctx_);
    RPC_PROCESSOR(ObRpcLSCheckDRTaskExistP, gctx_);
    RPC_PROCESSOR(ObRpcCreateTenantUserLSP, gctx_);
    RPC_PROCESSOR(ObRpcGenUniqueIDP, gctx_);
    RPC_PROCESSOR(ObRpcStartTransferTaskP, gctx_);
    RPC_PROCESSOR(ObRpcFinishTransferTaskP, gctx_);
#ifdef OB_BUILD_ARBITRATION
    RPC_PROCESSOR(ObRpcAddArbP, gctx_);
    RPC_PROCESSOR(ObRpcRemoveArbP, gctx_);
#endif
    RPC_PROCESSOR(ObRpcDDLCheckTabletMergeStatusP, gctx_);
    RPC_PROCESSOR(ObRpcCreateDuplicateLSP, gctx_);
}
