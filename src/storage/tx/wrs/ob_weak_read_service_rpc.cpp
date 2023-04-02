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

#define USING_LOG_PREFIX  TRANS

#include "share/ob_errno.h"
#include "ob_weak_read_service_rpc.h"

#include "rpc/obrpc/ob_rpc_result_code.h"     // ObRpcResultCode

namespace oceanbase
{
using namespace obrpc;
using namespace common;

namespace transaction
{

ObWrsRpc::ObWrsRpc() :
    inited_(false),
    proxy_(),
    cluster_heartbeat_cb_()
{
}

int ObWrsRpc::init(const rpc::frame::ObReqTransport *transport, ObIWeakReadService &wrs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(transport)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(proxy_.init(transport))) {
    LOG_WARN("init rpc proxy fail", KR(ret), K(transport));
  } else {
    cluster_heartbeat_cb_.init(&wrs);
    inited_ = true;
  }
  return ret;
}

int ObWrsRpc::get_cluster_version(const common::ObAddr &server, const uint64_t tenant_id,
    const obrpc::ObWrsGetClusterVersionRequest &req,
    obrpc::ObWrsGetClusterVersionResponse &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else {
    ret = proxy_.to(server)
          .trace_time(true)
          .max_process_handler_time(MAX_RPC_PROCESS_HANDLER_TIME)
          .by(tenant_id)
          .get_weak_read_cluster_version(req, res);
  }
  return ret;
}

int ObWrsRpc::post_cluster_heartbeat(const common::ObAddr &server,
    const uint64_t tenant_id,
    const obrpc::ObWrsClusterHeartbeatRequest &req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else {
    ret = proxy_.to(server)
          .trace_time(true)
          .max_process_handler_time(MAX_RPC_PROCESS_HANDLER_TIME)
          .by(tenant_id)
          .post_weak_read_cluster_heartbeat(req, &cluster_heartbeat_cb_);
  }
  return ret;
}

////////////////////// ObWrsRpc::ClusterHeartbeatCB ////////////////////////


rpc::frame::ObReqTransport::AsyncCB *ObWrsRpc::ClusterHeartbeatCB::clone(const rpc::frame::SPAlloc &alloc) const
{
  void *buf = NULL;
  ClusterHeartbeatCB *cb = NULL;
  if (OB_ISNULL(buf = alloc(sizeof(*this)))) {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "allocate memory for AsyncCB fail", K(buf));
  } else if (OB_ISNULL(cb = new(buf) ClusterHeartbeatCB())) {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "construct ClusterHeartbeatCB fail", K(cb), K(buf));
  } else {
    *cb = *this;
  }
  return cb;
}

int ObWrsRpc::ClusterHeartbeatCB::process()
{
  // process rcode from server
  return do_process_(rcode_);
}

int ObWrsRpc::ClusterHeartbeatCB::do_process_(const ObRpcResultCode &rcode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wrs_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("wrs is NULL", K(wrs_), KR(ret));
  } else {
    wrs_->process_cluster_heartbeat_rpc_cb(tenant_id_, rcode, result_, dst_);
  }
  return ret;
}

void ObWrsRpc::ClusterHeartbeatCB::on_timeout()
{
  ObRpcResultCode rcode;
  rcode.rcode_ = OB_TIMEOUT;
  (void)snprintf(rcode.msg_, sizeof(rcode.msg_), "weak read service cluster heartbeat rpc timeout, "
      "tenant_id=%lu, svr=%s, timeout=%ld, send_ts=%ld",
      tenant_id_, to_cstring(dst_), timeout_, send_ts_);
  do_process_(rcode);
}

void ObWrsRpc::ClusterHeartbeatCB::on_invalid()
{
  ObRpcResultCode rcode;
  // invalid rpc packet, decode fail
  rcode.rcode_ = OB_RPC_PACKET_INVALID;
  (void)snprintf(rcode.msg_, sizeof(rcode.msg_),
      "weak read service cluster heartbeat rpc response packet is invalid"
      "tenant_id=%lu, svr=%s, timeout=%ld", tenant_id_, to_cstring(dst_), timeout_);
  do_process_(rcode);
}

}
}
