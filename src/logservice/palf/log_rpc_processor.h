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

#ifndef OCEANBASE_LOGSERVICE_LOG_RPC_PROCESSOR_
#define OCEANBASE_LOGSERVICE_LOG_RPC_PROCESSOR_

#include "log_req.h"
#include "lib/ob_errno.h"
#include "lib/function/ob_function.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "log_rpc_macros.h"
#include "log_rpc_proxy.h"
#include "log_req.h"
#include "log_rpc_packet.h"
#include "log_request_handler.h"
#include "election/message/election_message.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"

namespace oceanbase
{
namespace palf
{
class IPalfEnvImpl;
int __get_palf_env_impl(uint64_t tenant_id, IPalfEnvImpl *&palf_env_impl, const bool need_check_tenant_id);

DEFINE_RPC_PROCESSOR(LogPushReqP,
                     obrpc::LogRpcProxyV2,
                     LogPushReq,
                     obrpc::OB_LOG_PUSH_REQ);

DEFINE_RPC_PROCESSOR(LogPushRespP,
                     obrpc::LogRpcProxyV2,
                     LogPushResp,
                     obrpc::OB_LOG_PUSH_RESP);

DEFINE_RPC_PROCESSOR(LogFetchReqP,
                     obrpc::LogRpcProxyV2,
                     LogFetchReq,
                     obrpc::OB_LOG_FETCH_REQ);

DEFINE_RPC_PROCESSOR(LogBatchFetchRespP,
                     obrpc::LogRpcProxyV2,
                     LogBatchFetchResp,
                     obrpc::OB_LOG_BATCH_FETCH_RESP);

DEFINE_RPC_PROCESSOR(LogPrepareReqP,
                     obrpc::LogRpcProxyV2,
                     LogPrepareReq,
                     obrpc::OB_LOG_PREPARE_REQ);

DEFINE_RPC_PROCESSOR(LogPrepareRespP,
                     obrpc::LogRpcProxyV2,
                     LogPrepareResp,
                     obrpc::OB_LOG_PREPARE_RESP);

DEFINE_RPC_PROCESSOR(LogChangeConfigMetaReqP,
                     obrpc::LogRpcProxyV2,
                     LogChangeConfigMetaReq,
                     obrpc::OB_LOG_CHANGE_CONFIG_META_REQ);

DEFINE_RPC_PROCESSOR(LogChangeConfigMetaRespP,
                     obrpc::LogRpcProxyV2,
                     LogChangeConfigMetaResp,
                     obrpc::OB_LOG_CHANGE_CONFIG_META_RESP);

DEFINE_RPC_PROCESSOR(LogChangeModeMetaReqP,
                     obrpc::LogRpcProxyV2,
                     LogChangeModeMetaReq,
                     obrpc::OB_LOG_CHANGE_MODE_META_REQ);

DEFINE_RPC_PROCESSOR(LogChangeModeMetaRespP,
                     obrpc::LogRpcProxyV2,
                     LogChangeModeMetaResp,
                     obrpc::OB_LOG_CHANGE_MODE_META_RESP);

DEFINE_RPC_PROCESSOR(LogNotifyRebuildReqP,
                     obrpc::LogRpcProxyV2,
                     NotifyRebuildReq,
                     obrpc::OB_LOG_NOTIFY_REBUILD_REQ);

DEFINE_RPC_PROCESSOR(LogNotifyFetchLogReqP,
                     obrpc::LogRpcProxyV2,
                     NotifyFetchLogReq,
                     obrpc::OB_LOG_NOTIFY_FETCH_LOG);

DEFINE_RPC_PROCESSOR(LogLearnerReqP,
                     obrpc::LogRpcProxyV2,
                     LogLearnerReq,
                     obrpc::OB_LOG_LEARNER_REQ);

DEFINE_RPC_PROCESSOR(LogRegisterParentReqP,
                     obrpc::LogRpcProxyV2,
                     LogRegisterParentReq,
                     obrpc::OB_LOG_REGISTER_PARENT_REQ);

DEFINE_RPC_PROCESSOR(LogRegisterParentRespP,
                     obrpc::LogRpcProxyV2,
                     LogRegisterParentResp,
                     obrpc::OB_LOG_REGISTER_PARENT_RESP);

DEFINE_ELECTION_RPC_PROCESSOR(ElectionPrepareRequestMsgP,
                              obrpc::LogRpcProxyV2,
                              election::ElectionPrepareRequestMsg,
                              obrpc::OB_LOG_ELECTION_PREPARE_REQUEST);

DEFINE_ELECTION_RPC_PROCESSOR(ElectionPrepareResponseMsgP,
                              obrpc::LogRpcProxyV2,
                              election::ElectionPrepareResponseMsg,
                              obrpc::OB_LOG_ELECTION_PREPARE_RESPONSE);

DEFINE_ELECTION_RPC_PROCESSOR(ElectionAcceptRequestMsgP,
                              obrpc::LogRpcProxyV2,
                              election::ElectionAcceptRequestMsg,
                              obrpc::OB_LOG_ELECTION_ACCEPT_REQUEST);

DEFINE_ELECTION_RPC_PROCESSOR(ElectionAcceptResponseMsgP,
                              obrpc::LogRpcProxyV2,
                              election::ElectionAcceptResponseMsg,
                              obrpc::OB_LOG_ELECTION_ACCEPT_RESPONSE);

DEFINE_ELECTION_RPC_PROCESSOR(ElectionChangeLeaderMsgP,
                              obrpc::LogRpcProxyV2,
                              election::ElectionChangeLeaderMsg,
                              obrpc::OB_LOG_ELECTION_CHANGE_LEADER_REQUEST);

DEFINE_RPC_PROCESSOR(CommittedInfoP,
                     obrpc::LogRpcProxyV2,
                     CommittedInfo,
                     obrpc::OB_LOG_COMMITTED_INFO);

DEFINE_SYNC_RPC_PROCESSOR(LogGetMCStP,
                          obrpc::LogRpcProxyV2,
                          LogGetMCStReq,
                          LogGetMCStResp,
                          obrpc::OB_LOG_GET_MC_ST);

DEFINE_SYNC_RPC_PROCESSOR(LogGetStatP,
                          obrpc::LogRpcProxyV2,
                          LogGetStatReq,
                          LogGetStatResp,
                          obrpc::OB_LOG_GET_STAT);

#ifdef OB_BUILD_ARBITRATION
DEFINE_SYNC_RPC_PROCESSOR(ObRpcGetArbMemberInfoP,
                          obrpc::LogRpcProxyV2,
                          LogGetArbMemberInfoReq,
                          LogGetArbMemberInfoResp,
                          obrpc::OB_LOG_GET_ARB_MEMBER_INFO);
#endif
} // end namespace palf
} // end namespace oceanbase

#endif
