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
#include "sql/das/ob_das_rpc_processor.h"
#include "storage/tx/ob_trans_rpc.h"
#include "storage/tx/ob_gts_rpc.h"
// #include "storage/tx/ob_dup_table_rpc.h"
#include "storage/tx/ob_dup_table_base.h"
#include "storage/tx/ob_ts_response_handler.h"
#include "storage/tx/wrs/ob_weak_read_service_rpc_define.h"  // weak_read_service
#include "observer/ob_rpc_processor_simple.h"
#include "observer/ob_inner_sql_rpc_processor.h"
#include "observer/ob_srv_task.h"
#include "logservice/palf/log_rpc_processor.h"
#include "logservice/logrpc/ob_log_rpc_processor.h"
#include "logservice/cdcservice/ob_cdc_rpc_processor.h"

#include "observer/table/ob_table_rpc_processor.h"
#include "observer/table/ob_table_execute_processor.h"
#include "observer/table/ob_table_batch_execute_processor.h"
#include "observer/table/ob_table_query_processor.h"
#include "observer/table/ob_table_query_and_mutate_processor.h"

#include "observer/dbms_job/ob_dbms_job_rpc_processor.h"
#include "storage/tx_storage/ob_tenant_freezer_rpc.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_rpc_processor.h"
#include "share/detect/ob_detect_rpc_processor.h"

#include "share/external_table/ob_external_table_file_rpc_processor.h"

using namespace oceanbase;
using namespace oceanbase::observer;
using namespace oceanbase::lib;
using namespace oceanbase::rpc;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::obrpc;
using namespace oceanbase::obmysql;

void oceanbase::observer::init_srv_xlator_for_sys(ObSrvRpcXlator *xlator) {
  RPC_PROCESSOR(ObRpcGetRoleP, gctx_);
  RPC_PROCESSOR(ObRpcDetectMasterRsLSP, gctx_);
  RPC_PROCESSOR(ObRpcSetConfigP, gctx_);
  RPC_PROCESSOR(ObRpcGetConfigP, gctx_);
  RPC_PROCESSOR(ObRpcSetTenantConfigP, gctx_);
  RPC_PROCESSOR(ObRpcNotifyTenantServerUnitResourceP, gctx_);
  RPC_PROCESSOR(ObCheckFrozenVersionP, gctx_);
  RPC_PROCESSOR(ObGetDiagnoseArgsP);
  RPC_PROCESSOR(ObGetMinSSTableSchemaVersionP, gctx_);
  RPC_PROCESSOR(ObInitTenantConfigP, gctx_);
  RPC_PROCESSOR(ObGetLeaderLocationsP, gctx_);
  RPC_PROCESSOR(ObBatchBroadcastSchemaP, gctx_);
  RPC_PROCESSOR(ObRpcSendHeartbeatP, gctx_);
  RPC_PROCESSOR(ObRpcNotifySwitchLeaderP, gctx_);

  // interrupt
  RPC_PROCESSOR(obrpc::ObInterruptProcessor);

  // DTL
  RPC_PROCESSOR(dtl::ObDtlSendMessageP);
  RPC_PROCESSOR(dtl::ObDtlBCSendMessageP);

  // tenant freezer
  RPC_PROCESSOR(ObTenantFreezerP);
  //session
  RPC_PROCESSOR(ObRpcKillSessionP, gctx_);

  // BatchRpc
  RPC_PROCESSOR(ObBatchP);
  // server blacklist
  RPC_PROCESSOR(ObBlacklistReqP);
  RPC_PROCESSOR(ObBlacklistRespP);

  RPC_PROCESSOR(ObDetectRpcP);

  // election provided
//  RPC_PROCESSOR(ObElectionP);
  RPC_PROCESSOR(ObRequestHeartbeatP, gctx_);
  RPC_PROCESSOR(ObFlushCacheP, gctx_);
  RPC_PROCESSOR(ObUpdateTenantMemoryP, gctx_);

  //backup
  RPC_PROCESSOR(ObRpcCheckBackupSchuedulerWorkingP, gctx_);

  //dbms_job
  RPC_PROCESSOR(ObRpcRunDBMSJobP, gctx_);

  // inner sql
  RPC_PROCESSOR(ObInnerSqlRpcP, gctx_);

  //dbms_scheduler
  RPC_PROCESSOR(ObRpcRunDBMSSchedJobP, gctx_);

  RPC_PROCESSOR(ObRpcGetServerResourceInfoP, gctx_);
}

void oceanbase::observer::init_srv_xlator_for_schema_test(ObSrvRpcXlator *xlator) {
  //RPC_PROCESSOR(ObGetLatestSchemaVersionP, gctx_.schema_service_);
  RPC_PROCESSOR(ObGetAllSchemaP, gctx_.schema_service_);
  RPC_PROCESSOR(ObRpcSetTPP, gctx_);
  RPC_PROCESSOR(ObSetDiskValidP, gctx_);
}

void oceanbase::observer::init_srv_xlator_for_transaction(ObSrvRpcXlator *xlator) {
  // transaction provided
  RPC_PROCESSOR(ObTxCommitP);
  RPC_PROCESSOR(ObTxCommitRespP);
  RPC_PROCESSOR(ObTxAbortP);
  RPC_PROCESSOR(ObTxRollbackSPP);
  RPC_PROCESSOR(ObTxRollbackSPRespP);
  RPC_PROCESSOR(ObTxKeepaliveP);
  RPC_PROCESSOR(ObTxKeepaliveRespP);
  //for dup_table
  // RPC_PROCESSOR(ObDupTableLeaseRequestMsgP, gctx_);
  // RPC_PROCESSOR(ObDupTableLeaseResponseMsgP, gctx_);
  // RPC_PROCESSOR(ObRedoLogSyncRequestP, gctx_);
  // RPC_PROCESSOR(ObRedoLogSyncResponseP, gctx_);
  RPC_PROCESSOR(ObDupTableLeaseRequestP);
  RPC_PROCESSOR(ObDupTableTsSyncRequestP);
  RPC_PROCESSOR(ObDupTableTsSyncResponseP);
  RPC_PROCESSOR(ObDupTableBeforePrepareRequestP);
  // RPC_PROCESSOR(ObPreCommitRequestP, gctx_);
  // RPC_PROCESSOR(ObPreCommitResponseP, gctx_);
  // for xa
  RPC_PROCESSOR(ObTxSubPrepareP);
  RPC_PROCESSOR(ObTxSubPrepareRespP);
  RPC_PROCESSOR(ObTxSubCommitP);
  RPC_PROCESSOR(ObTxSubCommitRespP);
  RPC_PROCESSOR(ObTxSubRollbackP);
  RPC_PROCESSOR(ObTxSubRollbackRespP);
  // for standby
  RPC_PROCESSOR(ObTxAskStateP);
  RPC_PROCESSOR(ObTxAskStateRespP);
  RPC_PROCESSOR(ObTxCollectStateP);
  RPC_PROCESSOR(ObTxCollectStateRespP);
  // for tx free route
  RPC_PROCESSOR(ObTxFreeRouteCheckAliveP);
  RPC_PROCESSOR(ObTxFreeRouteCheckAliveRespP);
  RPC_PROCESSOR(ObTxFreeRoutePushStateP);
  // for tx state check of 4377
  RPC_PROCESSOR(ObAskTxStateFor4377P);
}

void oceanbase::observer::init_srv_xlator_for_clog(ObSrvRpcXlator *xlator) {
  // clog provided
  /*
  RPC_PROCESSOR(ObLogRpcProcessor);
  RPC_PROCESSOR(ObLogReqStartLogIdByTsProcessor);
  RPC_PROCESSOR(ObLogExternalFetchLogProcessor);
  RPC_PROCESSOR(ObLogReqStartLogIdByTsProcessorWithBreakpoint);
  RPC_PROCESSOR(ObLogOpenStreamProcessor);
  RPC_PROCESSOR(ObLSFetchLogProcessor);
  RPC_PROCESSOR(ObLogLeaderHeartbeatProcessor);

  RPC_PROCESSOR(ObLogGetMCTsProcessor);
  RPC_PROCESSOR(ObLogGetMcCtxArrayProcessor);
  RPC_PROCESSOR(ObLogGetPriorityArrayProcessor);
  RPC_PROCESSOR(ObLogGetRemoteLogProcessor);
  RPC_PROCESSOR(ObLogGetPhysicalRestoreStateProcessor);
  */
}

void oceanbase::observer::init_srv_xlator_for_logservice(ObSrvRpcXlator *xlator)
{
  RPC_PROCESSOR(logservice::LogMembershipChangeP);
  RPC_PROCESSOR(logservice::LogGetPalfStatReqP);
#ifdef OB_BUILD_ARBITRATION
  RPC_PROCESSOR(logservice::LogServerProbeP);
#endif
  RPC_PROCESSOR(logservice::LogChangeAccessModeP);
  RPC_PROCESSOR(logservice::LogFlashbackMsgP);
}

void oceanbase::observer::init_srv_xlator_for_palfenv(ObSrvRpcXlator *xlator)
{
  RPC_PROCESSOR(palf::LogPushReqP);
  RPC_PROCESSOR(palf::LogPushRespP);
  RPC_PROCESSOR(palf::LogFetchReqP);
  RPC_PROCESSOR(palf::LogBatchFetchRespP);
  RPC_PROCESSOR(palf::LogPrepareReqP);
  RPC_PROCESSOR(palf::LogPrepareRespP);
  RPC_PROCESSOR(palf::LogChangeConfigMetaReqP);
  RPC_PROCESSOR(palf::LogChangeConfigMetaRespP);
  RPC_PROCESSOR(palf::LogChangeModeMetaReqP);
  RPC_PROCESSOR(palf::LogChangeModeMetaRespP);
  RPC_PROCESSOR(palf::LogNotifyRebuildReqP);
  RPC_PROCESSOR(palf::CommittedInfoP);
  RPC_PROCESSOR(palf::LogLearnerReqP);
  RPC_PROCESSOR(palf::LogRegisterParentReqP);
  RPC_PROCESSOR(palf::LogRegisterParentRespP);
  RPC_PROCESSOR(palf::ElectionPrepareRequestMsgP);
  RPC_PROCESSOR(palf::ElectionPrepareResponseMsgP);
  RPC_PROCESSOR(palf::ElectionAcceptRequestMsgP);
  RPC_PROCESSOR(palf::ElectionAcceptResponseMsgP);
  RPC_PROCESSOR(palf::ElectionChangeLeaderMsgP);
  RPC_PROCESSOR(palf::LogGetMCStP);
  RPC_PROCESSOR(palf::LogGetStatP);
  RPC_PROCESSOR(palf::LogNotifyFetchLogReqP);
}

void oceanbase::observer::init_srv_xlator_for_cdc(ObSrvRpcXlator *xlator)
{
  RPC_PROCESSOR(ObCdcLSReqStartLSNByTsP);
  RPC_PROCESSOR(ObCdcLSFetchLogP);
  RPC_PROCESSOR(ObCdcLSFetchMissingLogP);
}

void oceanbase::observer::init_srv_xlator_for_executor(ObSrvRpcXlator *xlator) {
  // executor
  RPC_PROCESSOR(ObRpcRemoteExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcLoadDataTaskExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcLoadDataShuffleTaskExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcLoadDataInsertTaskExecuteP, gctx_);
  RPC_PROCESSOR(ObRpcRemoteSyncExecuteP, gctx_);
  RPC_PROCESSOR(ObDASSyncAccessP, gctx_);
  RPC_PROCESSOR(ObDASSyncFetchP);
  RPC_PROCESSOR(ObDASAsyncEraseP);
  RPC_PROCESSOR(ObRpcEraseIntermResultP, gctx_);
  RPC_PROCESSOR(ObDASAsyncAccessP, gctx_);
  RPC_PROCESSOR(ObFlushExternalTableKVCacheP);
  RPC_PROCESSOR(ObAsyncLoadExternalTableFileListP);
}
