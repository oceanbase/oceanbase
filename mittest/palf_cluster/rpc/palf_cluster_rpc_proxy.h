/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_PALF_CLUSTER_LOG_RPC_PROXY_
#define OCEANBASE_PALF_CLUSTER_LOG_RPC_PROXY_

#include "palf_cluster_rpc_req.h"
#include "rpc/obrpc/ob_rpc_proxy.h"                             // ObRpcProxy

namespace oceanbase
{
namespace obrpc
{

class PalfClusterRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(PalfClusterRpcProxy);
  RPC_AP(PR3 send_submit_log_cmd, OB_LOG_SUBMIT_LOG_CMD,
      (palfcluster::SubmitLogCmd));
  RPC_AP(PR3 send_submit_log_resp, OB_LOG_SUBMIT_LOG_CMD_RESP,
      (palfcluster::SubmitLogCmdResp));
  RPC_AP(PR3 send_create_replica_cmd, OB_LOG_CREATE_REPLICA_CMD,
      (palfcluster::LogCreateReplicaCmd));
};

template <obrpc::ObRpcPacketCode pcode>
class ObLogRpcCB: public PalfClusterRpcProxy::AsyncCB<pcode>
{
public:
  ObLogRpcCB() {}
  ~ObLogRpcCB() {}
  void set_args(const typename PalfClusterRpcProxy::AsyncCB<pcode>::Request &args) {
    UNUSED(args);
  }
  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const {
    void *buf = alloc(sizeof (*this));
    ObLogRpcCB<pcode> *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObLogRpcCB<pcode>();
    }
    return newcb;
  }
  int process() {
    const common::ObAddr &dst = PalfClusterRpcProxy::AsyncCB<pcode>::dst_;
    obrpc::ObRpcResultCode &rcode = PalfClusterRpcProxy::AsyncCB<pcode>::rcode_;
    int ret = rcode.rcode_;

    if (common::OB_SUCCESS != rcode.rcode_) {
      PALF_LOG(WARN, "there is an rpc error in logservice", K(rcode), K(dst), K(pcode));
    } else {
      // do nothing
    }

    return common::OB_SUCCESS;
  }
  void on_timeout() {
    const common::ObAddr &dst = PalfClusterRpcProxy::AsyncCB<pcode>::dst_;
    //PALF_LOG(WARN, "logservice rpc timeout", K(dst));
  }
};
} // end namespace obrpc
} // end namespace oceanbase

#endif
