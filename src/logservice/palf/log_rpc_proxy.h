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

#ifndef OCEANBASE_LOGSERVICE_LOG_RPC_PROXY_
#define OCEANBASE_LOGSERVICE_LOG_RPC_PROXY_

#include "rpc/obrpc/ob_rpc_proxy.h"                             // ObRpcProxy
#include "observer/ob_server_struct.h"                          // GCONF
#include "log_rpc_macros.h"                                  // RPC Macros
#include "log_rpc_packet.h"                                  // LogRpcPacket
#include "log_req.h"                                         // LogPushReq...
#include "log_meta_info.h"
#include "election/message/election_message.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"

namespace oceanbase
{
namespace obrpc
{
class LogRpcProxyV2 : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(LogRpcProxyV2);
  void set_group_id(int32_t group_id);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogPushReq,
                                  OB_LOG_PUSH_REQ);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogPushResp,
                                  OB_LOG_PUSH_RESP);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogFetchReq,
                                  OB_LOG_FETCH_REQ);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogBatchFetchResp,
                                  OB_LOG_BATCH_FETCH_RESP);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogPrepareReq,
                                  OB_LOG_PREPARE_REQ);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogPrepareResp,
                                  OB_LOG_PREPARE_RESP);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogChangeConfigMetaReq,
                                  OB_LOG_CHANGE_CONFIG_META_REQ);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogChangeConfigMetaResp,
                                  OB_LOG_CHANGE_CONFIG_META_RESP);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogChangeModeMetaReq,
                                  OB_LOG_CHANGE_MODE_META_REQ);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogChangeModeMetaResp,
                                  OB_LOG_CHANGE_MODE_META_RESP);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  NotifyRebuildReq,
                                  OB_LOG_NOTIFY_REBUILD_REQ);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  NotifyFetchLogReq,
                                  OB_LOG_NOTIFY_FETCH_LOG);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogRegisterParentReq,
                                  OB_LOG_REGISTER_PARENT_REQ);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogRegisterParentResp,
                                  OB_LOG_REGISTER_PARENT_RESP);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  LogLearnerReq,
                                  OB_LOG_LEARNER_REQ);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR3,
                                  CommittedInfo,
                                  OB_LOG_COMMITTED_INFO);
  // for election
  DECLARE_RPC_PROXY_POST_FUNCTION(PR2,
                                  election::ElectionPrepareRequestMsg,
                                  OB_LOG_ELECTION_PREPARE_REQUEST);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR2,
                                  election::ElectionPrepareResponseMsg,
                                  OB_LOG_ELECTION_PREPARE_RESPONSE);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR2,
                                  election::ElectionAcceptRequestMsg,
                                  OB_LOG_ELECTION_ACCEPT_REQUEST);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR2,
                                  election::ElectionAcceptResponseMsg,
                                  OB_LOG_ELECTION_ACCEPT_RESPONSE);
  DECLARE_RPC_PROXY_POST_FUNCTION(PR2,
                                  election::ElectionChangeLeaderMsg,
                                  OB_LOG_ELECTION_CHANGE_LEADER_REQUEST);
  DECLARE_SYNC_RPC_PROXY_POST_FUNCTION(PR5,
                                       get_mc_st,
                                       LogGetMCStReq,
                                       LogGetMCStResp,
                                       OB_LOG_GET_MC_ST);
  DECLARE_SYNC_RPC_PROXY_POST_FUNCTION(PR5,
                                       get_log_stat,
                                       LogGetStatReq,
                                       LogGetStatResp,
                                       OB_LOG_GET_STAT);
#ifdef OB_BUILD_ARBITRATION
  DECLARE_SYNC_RPC_PROXY_POST_FUNCTION(PR5,
                                       get_remote_arb_member_info,
                                       LogGetArbMemberInfoReq,
                                       LogGetArbMemberInfoResp,
                                       OB_LOG_GET_ARB_MEMBER_INFO);
#endif
};
} // end namespace obrpc
} // end namespace oceanbase

#endif
