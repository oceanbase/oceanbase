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
#include "storage/transaction/ob_trans_rpc.h"
#include "storage/transaction/ob_gts_rpc.h"
#include "storage/transaction/ob_dup_table_rpc.h"
#include "storage/transaction/ob_gts_response_handler.h"
#include "storage/transaction/ob_weak_read_service_rpc_define.h"  // weak_read_service
#include "election/ob_election_rpc.h"
#include "clog/ob_log_rpc_processor.h"
#include "clog/ob_log_external_rpc.h"
#include "clog/ob_clog_sync_rpc.h"
#include "observer/ob_rpc_processor_simple.h"
#include "observer/ob_srv_task.h"

using namespace oceanbase;
using namespace oceanbase::observer;
using namespace oceanbase::lib;
using namespace oceanbase::rpc;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::clog;
using namespace oceanbase::obrpc;
using namespace oceanbase::obmysql;

void oceanbase::observer::init_srv_xlator_for_storage(ObSrvRpcXlator* xlator)
{
  // oceanbase service provied
  RPC_PROCESSOR(ObRpcCreatePartitionP, gctx_);
  RPC_PROCESSOR(ObRpcCreatePartitionBatchP, gctx_);
  RPC_PROCESSOR(ObGetWRSInfoP, gctx_);
  RPC_PROCESSOR(ObStopPartitionWriteP, gctx_);
  RPC_PROCESSOR(ObCheckPartitionLogP, gctx_);
  RPC_PROCESSOR(ObRpcFetchRootPartitionP, gctx_);
  RPC_PROCESSOR(ObRpcAddReplicaP, gctx_);
  // RPC_PROCESSOR(ObRpcRemoveReplicaP, gctx_);
  RPC_PROCESSOR(ObRpcRemoveNonPaxosReplicaP, gctx_);
  RPC_PROCESSOR(ObRpcRemoveMemberP, gctx_);
  RPC_PROCESSOR(ObRpcMigrateReplicaP, gctx_);
  RPC_PROCESSOR(ObRpcRebuildReplicaP, gctx_);
  RPC_PROCESSOR(ObRpcChangeReplicaP, gctx_);
  RPC_PROCESSOR(ObRpcAddReplicaBatchP, gctx_);
  RPC_PROCESSOR(ObRpcRemoveNonPaxosReplicaBatchP, gctx_);
  RPC_PROCESSOR(ObRpcRemoveMemberBatchP, gctx_);
  RPC_PROCESSOR(ObRpcModifyQuorumBatchP, gctx_);
  RPC_PROCESSOR(ObRpcMigrateReplicaBatchP, gctx_);
  RPC_PROCESSOR(ObRpcStandbyCutdataBatchTaskP, gctx_);
  RPC_PROCESSOR(ObRpcRebuildReplicaBatchP, gctx_);
  RPC_PROCESSOR(ObRpcCopySSTableBatchP, gctx_);
  RPC_PROCESSOR(ObRpcChangeReplicaBatchP, gctx_);
  RPC_PROCESSOR(ObRpcCheckMigrateTaskExistP, gctx_);
  RPC_PROCESSOR(ObRpcMinorFreezeP, gctx_);
  RPC_PROCESSOR(ObRpcCheckSchemaVersionElapsedP, gctx_);
  RPC_PROCESSOR(ObRpcCheckCtxCreateTimestampElapsedP, gctx_);
  RPC_PROCESSOR(ObRpcGetMemberListP, gctx_);
  RPC_PROCESSOR(ObRpcSwitchLeaderP, gctx_);
  RPC_PROCESSOR(ObRpcBatchSwitchRsLeaderP, gctx_);
  RPC_PROCESSOR(ObRpcSwitchLeaderListP, gctx_);
  RPC_PROCESSOR(ObRpcSwitchLeaderListAsyncP, gctx_);
  RPC_PROCESSOR(ObRpcGetLeaderCandidatesP, gctx_);
  RPC_PROCESSOR(ObRpcGetLeaderCandidatesAsyncP, gctx_);
  RPC_PROCESSOR(ObRpcGetLeaderCandidatesAsyncV2P, gctx_);
  RPC_PROCESSOR(ObRpcGetPartitionCountP, gctx_);
  RPC_PROCESSOR(ObRpcSwitchSchemaP, gctx_);
  RPC_PROCESSOR(ObRpcRefreshMemStatP, gctx_);
  RPC_PROCESSOR(ObRpcBootstrapP, gctx_);
  RPC_PROCESSOR(ObRpcIsEmptyServerP, gctx_);
  RPC_PROCESSOR(ObRpcCheckDeploymentModeP, gctx_);
  RPC_PROCESSOR(ObRpcGetPartitionStatP, gctx_);
  RPC_PROCESSOR(ObRpcSyncAutoincValueP, gctx_);
  RPC_PROCESSOR(ObRpcClearAutoincCacheP, gctx_);
  RPC_PROCESSOR(ObReportReplicaP, gctx_);
  RPC_PROCESSOR(ObReportSingleReplicaP, gctx_);
  RPC_PROCESSOR(ObRecycleReplicaP, gctx_);
  RPC_PROCESSOR(ObClearLocationCacheP, gctx_);
  RPC_PROCESSOR(ObDropReplicaP, gctx_);
  RPC_PROCESSOR(ObSetDSActionP, gctx_);
  RPC_PROCESSOR(ObBroadcastSysSchemaP, gctx_);
  RPC_PROCESSOR(ObCheckPartitionTableP, gctx_);
  RPC_PROCESSOR(ObSyncPartitionTableP, gctx_);
  RPC_PROCESSOR(ObSyncPGPartitionMTP, gctx_);
  RPC_PROCESSOR(ObRpcGetMemberListAndLeaderP, gctx_);
  RPC_PROCESSOR(ObRpcGetMemberListAndLeaderV2P, gctx_);
  RPC_PROCESSOR(ObRpcBatchGetMemberListAndLeaderP, gctx_);
  RPC_PROCESSOR(ObCancelSysTaskP, gctx_);
  RPC_PROCESSOR(ObCheckDanglingReplicaExistP, gctx_);
  RPC_PROCESSOR(ObRpcSplitPartitionP, gctx_);
  RPC_PROCESSOR(ObRpcBatchSplitPartitionP, gctx_);
  RPC_PROCESSOR(ObCheckUniqueIndexRequestP, gctx_);
  RPC_PROCESSOR(ObCheckSingleReplicaMajorSSTableExistP, gctx_);
  RPC_PROCESSOR(ObCheckAllReplicaMajorSSTableExistP, gctx_);
  RPC_PROCESSOR(ObCheckSingleReplicaMajorSSTableExistWithTimeP, gctx_);
  RPC_PROCESSOR(ObCheckAllReplicaMajorSSTableExistWithTimeP, gctx_);
  RPC_PROCESSOR(ObCalcColumnChecksumRequestP, gctx_);
  RPC_PROCESSOR(ObUpdateClusterInfoP, gctx_);
  RPC_PROCESSOR(ObSetMemberListBatchP, gctx_);
  RPC_PROCESSOR(ObGetRemoteTenantGroupStringP, gctx_);
  RPC_PROCESSOR(ObRpcGetTenantLogArchiveStatusP, gctx_);
  RPC_PROCESSOR(ObCheckPhysicalFlashbackSUCCP, gctx_);
  RPC_PROCESSOR(ObRpcBackupReplicaBatchP, gctx_);
  RPC_PROCESSOR(ObRpcCheckBackupTaskExistP, gctx_);
  RPC_PROCESSOR(ObRenewInZoneHbP, gctx_);
  RPC_PROCESSOR(ObRpcBroadcastRsListP, gctx_);
  RPC_PROCESSOR(ObRpcBatchGetProtectionLevelP, gctx_);
  RPC_PROCESSOR(ObRpcValidateBackupBatchP, gctx_);
}
