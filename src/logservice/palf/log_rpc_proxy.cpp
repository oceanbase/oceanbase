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

#include "log_rpc_proxy.h"                                 // LogRpcProxy
#include "share/rc/ob_tenant_base.h"
#include "observer/ob_srv_network_frame.h"

namespace oceanbase
{
namespace obrpc
{
template <obrpc::ObRpcPacketCode pcode>
class LogRpcCB: public LogRpcProxyV2::AsyncCB<pcode>
{
public:
  LogRpcCB() {}
  ~LogRpcCB() {}
  void set_args(const typename LogRpcProxyV2::AsyncCB<pcode>::Request &args) {
    UNUSED(args);
  }
  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const {
    void *buf = alloc(sizeof (*this));
    LogRpcCB<pcode> *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) LogRpcCB<pcode>();
    }
    return newcb;
  }
  int process() {
    const common::ObAddr &dst = LogRpcProxyV2::AsyncCB<pcode>::dst_;
    obrpc::ObRpcResultCode &rcode = LogRpcProxyV2::AsyncCB<pcode>::rcode_;

    if (common::OB_SUCCESS != rcode.rcode_) {
      PALF_LOG_RET(WARN, rcode.rcode_, "there is an rpc error in logservice",
          K(rcode), K(dst), K(pcode));
    } else {
      // do nothing
    }

    return common::OB_SUCCESS;
  }
  void on_timeout() {
    const common::ObAddr &dst = LogRpcProxyV2::AsyncCB<pcode>::dst_;
    //PALF_LOG(WARN, "logservice rpc timeout", K(dst));
  }
};

void LogRpcProxyV2::set_group_id(int32_t group_id) {
  group_id_ = group_id;
  if (share::OBCG_ELECTION == group_id) {
    const rpc::frame::ObReqTransport* high_prio_transport = NULL;
    if (OB_ISNULL(GCTX.net_frame_)) {
    } else {
      high_prio_transport = GCTX.net_frame_->get_high_prio_req_transport();
      if (NULL != high_prio_transport) {
        transport_ = high_prio_transport;
      }
    }
  }
}

DEFINE_RPC_PROXY_POST_FUNCTION(LogPushReq,
                               OB_LOG_PUSH_REQ);
DEFINE_RPC_PROXY_POST_FUNCTION(LogPushResp,
                               OB_LOG_PUSH_RESP);
DEFINE_RPC_PROXY_POST_FUNCTION(LogFetchReq,
                               OB_LOG_FETCH_REQ);
DEFINE_RPC_PROXY_POST_FUNCTION(LogBatchFetchResp,
                               OB_LOG_BATCH_FETCH_RESP);
DEFINE_RPC_PROXY_POST_FUNCTION(LogPrepareReq,
                               OB_LOG_PREPARE_REQ);
DEFINE_RPC_PROXY_POST_FUNCTION(LogPrepareResp,
                               OB_LOG_PREPARE_RESP);
DEFINE_RPC_PROXY_POST_FUNCTION(LogChangeConfigMetaReq,
                               OB_LOG_CHANGE_CONFIG_META_REQ);
DEFINE_RPC_PROXY_POST_FUNCTION(LogChangeConfigMetaResp,
                               OB_LOG_CHANGE_CONFIG_META_RESP);
DEFINE_RPC_PROXY_POST_FUNCTION(LogChangeModeMetaReq,
                               OB_LOG_CHANGE_MODE_META_REQ);
DEFINE_RPC_PROXY_POST_FUNCTION(LogChangeModeMetaResp,
                               OB_LOG_CHANGE_MODE_META_RESP);
DEFINE_RPC_PROXY_POST_FUNCTION(LogRegisterParentReq, OB_LOG_REGISTER_PARENT_REQ);
DEFINE_RPC_PROXY_POST_FUNCTION(LogRegisterParentResp, OB_LOG_REGISTER_PARENT_RESP);
DEFINE_RPC_PROXY_POST_FUNCTION(LogLearnerReq, OB_LOG_LEARNER_REQ);
DEFINE_RPC_PROXY_ELECTION_POST_FUNCTION(election::ElectionPrepareRequestMsg,
                                        OB_LOG_ELECTION_PREPARE_REQUEST);
DEFINE_RPC_PROXY_ELECTION_POST_FUNCTION(election::ElectionPrepareResponseMsg,
                                        OB_LOG_ELECTION_PREPARE_RESPONSE);
DEFINE_RPC_PROXY_ELECTION_POST_FUNCTION(election::ElectionAcceptRequestMsg,
                                        OB_LOG_ELECTION_ACCEPT_REQUEST);
DEFINE_RPC_PROXY_ELECTION_POST_FUNCTION(election::ElectionAcceptResponseMsg,
                                        OB_LOG_ELECTION_ACCEPT_RESPONSE);
DEFINE_RPC_PROXY_ELECTION_POST_FUNCTION(election::ElectionChangeLeaderMsg,
                                        OB_LOG_ELECTION_CHANGE_LEADER_REQUEST);
DEFINE_RPC_PROXY_POST_FUNCTION(NotifyRebuildReq,
                               OB_LOG_NOTIFY_REBUILD_REQ);
DEFINE_RPC_PROXY_POST_FUNCTION(CommittedInfo, OB_LOG_COMMITTED_INFO);
DEFINE_RPC_PROXY_POST_FUNCTION(NotifyFetchLogReq,
                               OB_LOG_NOTIFY_FETCH_LOG);
DEFINE_SYNC_RPC_PROXY_POST_FUNCTION(get_mc_st,
                                    LogGetMCStReq,
                                    LogGetMCStResp);
DEFINE_SYNC_RPC_PROXY_POST_FUNCTION(get_log_stat,
                                    LogGetStatReq,
                                    LogGetStatResp);
#ifdef OB_BUILD_ARBITRATION
DEFINE_SYNC_RPC_PROXY_POST_FUNCTION(get_remote_arb_member_info,
                                    LogGetArbMemberInfoReq,
                                    LogGetArbMemberInfoResp);
#endif
} // end namespace obrpc
} // end namespace oceanbase
