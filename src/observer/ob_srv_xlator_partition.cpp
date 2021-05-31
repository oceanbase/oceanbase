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

void oceanbase::observer::init_srv_xlator_for_partition(ObSrvRpcXlator* xlator)
{
  RPC_PROCESSOR(ObGetMemberListP, gctx_.par_ser_);
  RPC_PROCESSOR(ObReachPartitionLimitP, gctx_);
  RPC_PROCESSOR(ObPTSAddMemberP, gctx_.par_ser_);
  RPC_PROCESSOR(ObPTSRemoveMemberP, gctx_.par_ser_);
  RPC_PROCESSOR(ObPTSRemoveReplicaP, gctx_.par_ser_);
  RPC_PROCESSOR(ObIsMemberChangeDoneP, gctx_.par_ser_);
  RPC_PROCESSOR(ObWarmUpRequestP, gctx_.par_ser_);
  RPC_PROCESSOR(ObBatchRemoveMemberP, gctx_.par_ser_);
  RPC_PROCESSOR(ObBatchAddMemberP, gctx_.par_ser_);
  RPC_PROCESSOR(ObBatchMemberChangeDoneP, gctx_.par_ser_);
  RPC_PROCESSOR(ObFetchMacroBlockOldP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObDumpMemtableP, gctx_);
  RPC_PROCESSOR(ObForceSetAsSingleReplicaP, gctx_);
  RPC_PROCESSOR(ObForceSetReplicaNumP, gctx_);
  RPC_PROCESSOR(ObHaltPrewarmP, gctx_);
  RPC_PROCESSOR(ObHaltPrewarmAsyncP, gctx_);
  RPC_PROCESSOR(ObForceResetParentP, gctx_);
  RPC_PROCESSOR(ObForceSetParentP, gctx_);
  RPC_PROCESSOR(ObForceSwitchILogFileP, gctx_);
  RPC_PROCESSOR(ObForceSetAllAsSingleReplicaP, gctx_);
  RPC_PROCESSOR(ObSplitDestPartitionRequestP, gctx_.par_ser_);
  RPC_PROCESSOR(ObReplicaSplitProgressRequestP, gctx_.par_ser_);
  RPC_PROCESSOR(ObCheckMemberMajorSSTableEnoughP, gctx_.par_ser_);
  RPC_PROCESSOR(ObBatchRemoveReplicaP, gctx_.par_ser_);
  RPC_PROCESSOR(ObForceRemoveReplicaP, gctx_);
  RPC_PROCESSOR(ObForceDisableBlacklistP, gctx_);
  RPC_PROCESSOR(ObForceEnableBlacklistP, gctx_);
  RPC_PROCESSOR(ObForceClearBlacklistP, gctx_);
  RPC_PROCESSOR(ObBatchTranslatePartitionP, gctx_);
  RPC_PROCESSOR(ObBatchWaitLeaderP, gctx_);
  RPC_PROCESSOR(ObBatchWriteCutdataClogP, gctx_);
  RPC_PROCESSOR(ObAddDiskP, gctx_);
  RPC_PROCESSOR(ObDropDiskP, gctx_);
  RPC_PROCESSOR(ObForceSetServerListP, gctx_);
  RPC_PROCESSOR(ObKillPartTransCtxP, gctx_);
  RPC_PROCESSOR(ObRpcCheckNeedOffineReplicaP, gctx_);
}

void oceanbase::observer::init_srv_xlator_for_migrator(ObSrvRpcXlator* xlator)
{
  RPC_PROCESSOR(ObFetchPartitionInfoP, gctx_.par_ser_);
  RPC_PROCESSOR(ObFetchTableInfoP, gctx_.par_ser_);
  RPC_PROCESSOR(ObFetchLogicBaseMetaP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObFetchPhysicalBaseMetaP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObFetchLogicRowP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObFetchLogicDataChecksumP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObFetchLogicDataChecksumSliceP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObFetchLogicRowSliceP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObFetchMacroBlockP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObFetchPartitionGroupInfoP, gctx_.par_ser_);
  RPC_PROCESSOR(ObFetchPGPartitioninfoP, gctx_.par_ser_, gctx_.bandwidth_throttle_);
  RPC_PROCESSOR(ObCheckMemberPGMajorSSTableEnoughP, gctx_.par_ser_);
  RPC_PROCESSOR(ObFetchReplicaInfoP, gctx_.par_ser_);
}

void oceanbase::observer::init_srv_xlator_for_others(ObSrvRpcXlator* xlator)
{
  RPC_PROCESSOR(ObGtsP, gctx_);
  RPC_PROCESSOR(ObGtsErrRespP, gctx_);
  // Weakly Consistent Read Related
  RPC_PROCESSOR(ObWrsGetClusterVersionP, gctx_.weak_read_service_);
  RPC_PROCESSOR(ObWrsClusterHeartbeatP, gctx_.weak_read_service_);

  RPC_PROCESSOR(ObQueryMaxDecidedTransVersionP, gctx_);
  RPC_PROCESSOR(ObQueryIsValidMemberP, gctx_);
  RPC_PROCESSOR(ObQueryMaxFlushedILogIdP, gctx_);

  // update optimizer statistic
  RPC_PROCESSOR(ObUpdateLocalStatCacheP, gctx_);
  RPC_PROCESSOR(ObInitSqcP, gctx_);
  RPC_PROCESSOR(ObInitTaskP, gctx_);
  RPC_PROCESSOR(ObInitFastSqcP, gctx_);
  // SQL Estimate
  RPC_PROCESSOR(ObEstimatePartitionRowsP, gctx_);

  // HA GTS
  RPC_PROCESSOR(ObHaGtsPingRequestP, gctx_);
  RPC_PROCESSOR(ObHaGtsGetRequestP, gctx_);
  RPC_PROCESSOR(ObHaGtsGetResponseP, gctx_);
  RPC_PROCESSOR(ObHaGtsHeartbeatP, gctx_);
  RPC_PROCESSOR(ObHaGtsUpdateMetaP, gctx_);
  RPC_PROCESSOR(ObHaGtsChangeMemberP, gctx_);

  // XA transaction rpc
  RPC_PROCESSOR(ObXAPrepareP, gctx_);
  RPC_PROCESSOR(ObXAEndTransP, gctx_);
  RPC_PROCESSOR(ObXASyncStatusP, gctx_);
  RPC_PROCESSOR(ObXASyncStatusResponseP, gctx_);
  RPC_PROCESSOR(ObXAMergeStatusP, gctx_);
  RPC_PROCESSOR(ObXAHbReqP, gctx_);
  RPC_PROCESSOR(ObXAHbRespP, gctx_);
}
