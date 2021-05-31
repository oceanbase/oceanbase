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
#include "sql/executor/ob_remote_executor_processor.h"
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

void oceanbase::observer::init_srv_xlator_for_sys(ObSrvRpcXlator* xlator)
{
  RPC_PROCESSOR(ObRpcGetRoleP, gctx_);
  RPC_PROCESSOR(ObRpcBatchGetRoleP, gctx_);
  RPC_PROCESSOR(ObRpcGetMasterRSP, gctx_);
  RPC_PROCESSOR(ObRpcSetConfigP, gctx_);
  RPC_PROCESSOR(ObRpcGetConfigP, gctx_);
  RPC_PROCESSOR(ObRpcAddTenantTmpP, gctx_);
  RPC_PROCESSOR(ObRpcNotifyTenantServerUnitResourceP, gctx_);
  RPC_PROCESSOR(ObCheckFrozenVersionP, gctx_);
  RPC_PROCESSOR(ObGetDiagnoseArgsP);
  RPC_PROCESSOR(ObGetMinSSTableSchemaVersionP, gctx_);

  // interrupt
  RPC_PROCESSOR(obrpc::ObInterruptProcessor);

  // DTL
  RPC_PROCESSOR(dtl::ObDtlSendMessageP);
  RPC_PROCESSOR(dtl::ObDtlBCSendMessageP);

  // tenant manager
  RPC_PROCESSOR(ObTenantMgrP, gctx_.rs_rpc_proxy_, gctx_.rs_mgr_, gctx_.par_ser_);
  // session
  RPC_PROCESSOR(ObRpcKillSessionP, gctx_);

  // BatchRpc
  RPC_PROCESSOR(ObBatchP, gctx_.par_ser_);
  // server blacklist
  RPC_PROCESSOR(ObBlacklistReqP);
  RPC_PROCESSOR(ObBlacklistRespP);

  // election provided
  RPC_PROCESSOR(ObElectionP, gctx_.par_ser_);
  RPC_PROCESSOR(ObRequestHeartbeatP, gctx_);
  RPC_PROCESSOR(ObFlushCacheP, gctx_);
  RPC_PROCESSOR(ObUpdateTenantMemoryP, gctx_);

  // backup
  RPC_PROCESSOR(ObRpcCheckBackupSchuedulerWorkingP, gctx_);
}

void oceanbase::observer::init_srv_xlator_for_schema_test(ObSrvRpcXlator* xlator)
{
  // RPC_PROCESSOR(ObGetLatestSchemaVersionP, gctx_.schema_service_);
  RPC_PROCESSOR(ObGetAllSchemaP, gctx_.schema_service_);
  RPC_PROCESSOR(ObRpcSetTPP, gctx_);
  RPC_PROCESSOR(ObSetDiskValidP, gctx_);
}

void oceanbase::observer::init_srv_xlator_for_transaction(ObSrvRpcXlator* xlator)
{
  // transaction provided
  RPC_PROCESSOR(ObTransP, gctx_.par_ser_);
  RPC_PROCESSOR(ObTransRespP, gctx_.par_ser_);
  RPC_PROCESSOR(ObDupTableLeaseRequestMsgP, gctx_);
  RPC_PROCESSOR(ObDupTableLeaseResponseMsgP, gctx_);
  RPC_PROCESSOR(ObRedoLogSyncRequestP, gctx_);
  RPC_PROCESSOR(ObRedoLogSyncResponseP, gctx_);
}

void oceanbase::observer::init_srv_xlator_for_clog(ObSrvRpcXlator* xlator)
{
  // clog provided
  RPC_PROCESSOR(ObLogRpcProcessor, gctx_.par_ser_);
  RPC_PROCESSOR(ObLogReqStartLogIdByTsProcessor, gctx_.par_ser_);
  RPC_PROCESSOR(ObLogReqStartPosByLogIdProcessor, gctx_.par_ser_);
  RPC_PROCESSOR(ObLogExternalFetchLogProcessor, gctx_.par_ser_);
  RPC_PROCESSOR(ObLogReqHeartbeatInfoProcessor, gctx_.par_ser_);
  RPC_PROCESSOR(ObLogReqStartLogIdByTsProcessorWithBreakpoint, gctx_.par_ser_);
  RPC_PROCESSOR(ObLogReqStartPosByLogIdProcessorWithBreakpoint, gctx_.par_ser_);
  RPC_PROCESSOR(ObLogOpenStreamProcessor, gctx_.par_ser_);
  RPC_PROCESSOR(ObLogStreamFetchLogProcessor, gctx_.par_ser_);
  RPC_PROCESSOR(ObLogLeaderHeartbeatProcessor, gctx_.par_ser_);

  RPC_PROCESSOR(ObLogGetMCTsProcessor, gctx_.par_ser_);
  RPC_PROCESSOR(ObLogGetMcCtxArrayProcessor, gctx_.par_ser_);
  RPC_PROCESSOR(ObLogGetPriorityArrayProcessor, gctx_.par_ser_);
  RPC_PROCESSOR(ObLogGetRemoteLogProcessor, gctx_.par_ser_);
}

void oceanbase::observer::init_srv_xlator_for_executor(ObSrvRpcXlator* xlator)
{
  // executor

  RPC_PROCESSOR(ObRpcRemoteExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcDistExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcTaskCompleteP, gctx_);
  RPC_PROCESSOR(ObRpcTaskNotifyFetchP, gctx_);
  RPC_PROCESSOR(ObRpcTaskFetchResultP, gctx_);
  RPC_PROCESSOR(ObRpcTaskFetchIntermResultP, gctx_);
  RPC_PROCESSOR(ObRpcTaskKillP, gctx_);
  RPC_PROCESSOR(ObRpcCloseResultP, gctx_);
  RPC_PROCESSOR(ObRpcAPDistExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcMiniTaskExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcAPMiniDistExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcBKGDDistExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcBKGDTaskCompleteP, gctx_);
  RPC_PROCESSOR(ObRpcLoadDataTaskExecuteP, gctx_);
  RPC_PROCESSOR(ObFetchIntermResultItemP, gctx_);
  RPC_PROCESSOR(ObRpcLoadDataShuffleTaskExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcLoadDataInsertTaskExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcAPPingSqlTaskP, gctx_);
  RPC_PROCESSOR(ObRpcRemoteSyncExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcRemoteASyncExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcRemotePostResultP, gctx_);
  RPC_PROCESSOR(ObCheckBuildIndexTaskExistP, gctx_);
}
