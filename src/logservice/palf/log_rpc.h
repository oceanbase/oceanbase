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

#ifndef OCEANBASE_LOGSERVICE_LOG_RPC_
#define OCEANBASE_LOGSERVICE_LOG_RPC_

#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"            // IS_NOT_INIT
#include "lib/net/ob_addr.h"                       // ObAddr
#include "rpc/obrpc/ob_rpc_packet.h"               // ObRpcPacketCode
#include "log_rpc_macros.h"                        // MACROS...
#include "log_rpc_packet.h"                        // LogRpcPacketImpl
#include "log_rpc_proxy.h"                         // LogRpcProxyV2
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "share/rpc/ob_batch_rpc.h"

namespace oceanbase
{
namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}
namespace obrpc
{
class LogRpcProxyV2;
}
namespace common
{
class ObAddr;
}
namespace palf
{
// If you want to add a RPC, you just only need to:
//
// 1. Add two macros respectively in log_rpc_proxy.* to define an interface
//    which used to define an interface to be called by ObRpcProxy, the first
//    argument is the priority of rpc, the secondly is parameter type, the lastly
//    is the unique ObRpcPacketCode;
//
// 2. Add two macros respectively in log_rpc_processor.* to define an interface
//          you just only need do something as follow:
//    LOG_PREPARE_REQ;
//
// 2. Add a LogPreapreMetaReq in log_req.h;
//
// 3. Add DECLARE_RPC_PROXY_POST_FUNCTION(PR3, LogPrepareReq, LOG_PREPARE_REQ)
//    and DEFINE_RPC_PROXY_POST_FUNCTION(LogRpcProxyV2, , LogPrepareLogReq, LOG_PREPARE_REQ)
//    respectively in log_rpc_proxy.h and log_rpc_proxy.cpp.
//    NB: LogRpc will used this interface like as(caller doesn't care about detail impl):
//    LogRpcProxyV2::post_packet(log_rpc_packet)
//
// 4. Add DECLARE_RPC_PROCESSOR(LogPrepareReqP, LogRpcProxyV2, LOG_PUSH_REQ)
//    and DEFINE_RPC_PROCESSOR(LogPrepareReqP, LogPrepareReq) respectively
//    in log_rpc_processor.h and log_rpc_processor.cpp.
//
//    NB: Above processor used to LogPacketHandler
class LogRpc {
public:
  LogRpc();
  ~LogRpc();
  int init(const common::ObAddr &self,
           const int64_t cluster_id,
           const int64_t tenant_id,
           rpc::frame::ObReqTransport *transport,
           obrpc::ObBatchRpc *batch_rpc);
  void destroy();
  int update_transport_compress_options(const PalfTransportCompressOptions &compress_opt);
  const PalfTransportCompressOptions& get_compress_opts() const;
  template<class ReqType,
           typename std::enable_if<(!std::is_same<ReqType, LogBatchPushReq>::value &&
                                    !std::is_same<ReqType, LogBatchPushResp>::value), bool>::type=true>
  int post_request(const common::ObAddr &server,
                   const int64_t palf_id,
                   const ReqType &req)
  {
    int ret = common::OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (false == server.is_valid()
               || false == is_valid_palf_id(palf_id)
               || false == req.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      LogRpcPacketImpl<ReqType> packet(self_, palf_id, req);
      ret = rpc_proxy_.post_packet(server, packet, tenant_id_, options_);
      PALF_LOG(TRACE, "post_packet finished", K(ret), K(server), K(tenant_id_));
    }
    return ret;
  }

  template<class ReqType, class RespType>
  int post_sync_request(const common::ObAddr &server,
                        const int64_t palf_id,
                        const int64_t timeout_us,
                        const ReqType &req,
                        RespType &resp)
  {
    int ret = common::OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (false == server.is_valid()
               || false == is_valid_palf_id(palf_id)
               || false == req.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      LogRpcPacketImpl<ReqType> req_packet(self_, palf_id, req);
      LogRpcPacketImpl<RespType> resp_packet(server, palf_id, resp);
      ret = rpc_proxy_.post_sync_packet(server, tenant_id_, options_, timeout_us, req_packet, resp_packet);
      resp = resp_packet.req_;
      PALF_LOG(TRACE, "post_sync_request", K(tenant_id_), K(palf_id), K(req), K(resp));
    }
    return ret;
  }

  // BatchRPC
  template<class ReqType,
           typename std::enable_if<(std::is_same<ReqType, LogBatchPushReq>::value ||
                                    std::is_same<ReqType, LogBatchPushResp>::value), bool>::type=true>
  int post_request(const common::ObAddr &server,
                   const int64_t palf_id,
                   const ReqType &req)
  {
    int ret = common::OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (false == server.is_valid()
               || false == is_valid_palf_id(palf_id)
               || false == req.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_ISNULL(batch_rpc_)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "batch_rpc_ is nullptr, unexpected error", K(ret), K(server), K(palf_id), KP(batch_rpc_), K(req));
    } else {
      const int64_t LOG_BATCH_SUB_TYPE = (std::is_same<ReqType, LogBatchPushReq>::value)? \
          LOG_BATCH_PUSH_LOG_REQ: LOG_BATCH_PUSH_LOG_RESP;
      ret = batch_rpc_->post(tenant_id_,
                             server,
                             cluster_id_,
                             obrpc::CLOG_BATCH_REQ,
                             LOG_BATCH_SUB_TYPE,
                             share::ObLSID(palf_id),
                             req);
      PALF_LOG(TRACE, "post batch rpc finished", K(ret), K(server), K(tenant_id_));
    }
    return ret;
  }

  TO_STRING_KV(K_(self), K_(is_inited));
private:
  ObAddr self_;
  obrpc::LogRpcProxyV2 rpc_proxy_;
  mutable ObSpinLock opt_lock_;
  PalfTransportCompressOptions options_;
  obrpc::ObBatchRpc *batch_rpc_;
  int64_t tenant_id_;
  int64_t cluster_id_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase

#endif
