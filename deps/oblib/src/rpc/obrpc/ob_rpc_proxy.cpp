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

#define USING_LOG_PREFIX RPC_OBRPC
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"

#include "lib/worker.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/trace/ob_trace.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace obrpc
{
ObRpcProxy::NoneT None;
}; // end namespace obrpc
}; // end namespace oceanbase

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;
using namespace oceanbase::rpc::frame;

ObAddr ObRpcProxy::myaddr_;


Handle::Handle()
    : has_more_(false),
      dst_(),
      sessid_(0L),
      opts_(),
      transport_(NULL),
      proxy_(),
      pcode_(OB_INVALID_RPC_CODE),
      first_pkt_id_(INVALID_RPC_PKT_ID)
{}

int ObRpcProxy::init(const ObReqTransport *transport,
    const oceanbase::common::ObAddr &dst)
{
  return init(transport, ObRpcNetHandler::CLUSTER_ID, dst);
}

int ObRpcProxy::init(const ObReqTransport *transport,
    const int64_t src_cluster_id,
    const oceanbase::common::ObAddr &dst)
{
  int ret = OB_SUCCESS;

  if (init_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Rpc proxy not inited", K(ret));
  } else if (OB_ISNULL(transport)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(transport));
  } else {
    transport_ = transport;
    src_cluster_id_ = src_cluster_id;
    dst_ = dst;
    init_ = true;
    int enable_poc_rpc = atoi(getenv("enable_poc_rpc")?:"1");
    if (enable_poc_rpc > 0) {
      transport_impl_ = 1;
    } else {
      transport_impl_ = 0;
    }
  }

  return ret;
}

int ObRpcProxy::rpc_post(
    ObRpcPacketCode pcode,
    ObReqTransport::AsyncCB *cb,
    const ObRpcOpts &opts)
{
  int ret = OB_SUCCESS;

  if (!active_) {
    ret = OB_INACTIVE_RPC_PROXY;
    LOG_WARN("Rpc proxy is inactive", K(ret));
  }
  POC_RPC_INTERCEPT(post, dst_, pcode, None, cb, opts);

  int64_t pos = 0;
  const int64_t payload = calc_payload_size(0);
  ObReqTransport::Request req;
  if (OB_FAIL(ret)) {
  } else if (payload > get_max_rpc_packet_size()) {
    ret = OB_RPC_PACKET_TOO_LONG;
    LOG_WARN("obrpc packet payload execced its limit",
        K(ret), K(payload), "limit", get_max_rpc_packet_size());
  } else if (OB_ISNULL(transport_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("transport shoul not be NULL", K(ret));
  } else if (OB_FAIL(create_request(pcode, *transport_, req, dst_,
      payload, timeout_, opts.local_addr_, do_ratelimit_, is_bg_flow_,
      opts.ssl_invited_nodes_, cb))) {
    LOG_WARN("create request fail", K(ret));
  } else if (OB_ISNULL(req.pkt()) || OB_ISNULL(req.buf())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("request packet or req buf is NULL", K(ret),
             "packet", req.pkt(), "buf", req.buf());
  } else if (OB_FAIL(fill_extra_payload(req, payload, pos))) {
    LOG_WARN("fill extra payload fail", K(ret), K(pos), K(payload));
  } else {
    req.set_async();
    if (OB_FAIL(init_pkt(req.pkt(), pcode, opts, NULL == cb))) {
      LOG_WARN("Init pkt error", K(ret));
    } else if (OB_FAIL(transport_->post(req))) {
      req.destroy();
      LOG_WARN("post packet fail", K(pcode), K(ret));
    } else {
      //do nothing
    }
  }

  return ret;
}

const ObRpcResultCode &ObRpcProxy::get_result_code() const
{
  return rcode_;
}

int ObRpcProxy::init_pkt(
    ObRpcPacket *pkt, ObRpcPacketCode pcode, const ObRpcOpts &opts,
    const bool unneed_response) const
{
  int ret = OB_SUCCESS;
  const uint64_t* trace_id = common::ObCurTraceId::get();
  if (OB_ISNULL(trace_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Trace id should not be NULL", K(ret), K(trace_id));
  } else if (OB_ISNULL(pkt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input pkt is NULL", K(ret), K(pkt));
  } else if (0 == trace_id[0]) {
    common::ObCurTraceId::init(myaddr_);
    pkt->set_trace_id(common::ObCurTraceId::get());
    common::ObCurTraceId::reset();
  } else {
    pkt->set_trace_id(common::ObCurTraceId::get());
  }

#ifdef ERRSIM
  pkt->set_module_type(THIS_WORKER.get_module_type());
#endif

  if (OB_SUCC(ret)) {
    pkt->set_pcode(pcode);
    //Assign a channel id to this new packet
    uint32_t new_chid = ATOMIC_AAF(&ObRpcPacket::global_chid, 1);
    pkt->set_chid(new_chid);
    pkt->set_timeout(timeout_);
    pkt->set_priority(opts.pr_);
    pkt->set_session_id(0);
    int8_t log_level = common::ObThreadLogLevelUtils::get_level();
    // for log level compatibility, disable thread log level while upgrading.
    if (OB_LOGGER.is_info_as_wdiag()) {
      log_level = OB_LOG_LEVEL_NONE;
    }
    pkt->set_log_level(log_level);
    pkt->set_tenant_id(tenant_id_);
    pkt->set_priv_tenant_id(priv_tenant_id_);
    pkt->set_timestamp(ObTimeUtility::current_time());
    pkt->set_dst_cluster_id(dst_cluster_id_);
    // For request, src_cluster_id must be the cluster_id of this cluster, directly hard-coded
    pkt->set_src_cluster_id(src_cluster_id_);
    pkt->set_unis_version(0);
    if (0 != get_group_id()) {
      pkt->set_group_id(get_group_id());
    } else if (this_worker().get_group_id() == 100) {
      pkt->set_group_id(0);
    } else {
      pkt->set_group_id(this_worker().get_group_id());
    }
    if (need_increment_request_level(pcode)) {
      if (this_worker().get_worker_level() == INT32_MAX) { // The inner sql request is not sent from the tenant thread, so the worker level is still the initial value, given
                                                   // inner sql a special nesting level
        pkt->set_request_level(5);
      } else {
        pkt->set_request_level(this_worker().get_curr_request_level() + 1);
      }
    } else {
      // When request_level <2 is still processed according to the original tenant thread, so internal requests can also be set to 0
      pkt->set_request_level(0);
    }
    pkt->calc_checksum();
    if (unneed_response) {
      pkt->set_unneed_response();
    }
    if (do_ratelimit_ && transport_->ratelimit_enabled()) {
      pkt->enable_ratelimit();
      if (is_bg_flow_) {
        pkt->set_background_flow();
      }
    }
  }
  return ret;
}

int ObRpcProxy::send_request(
    const ObReqTransport::Request &req,
    ObReqTransport::Result &result) const
{
  LOG_DEBUG("send ob_request: ", K(req));
  int ret = OB_SUCCESS;
  if (OB_ISNULL(transport_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("transport_ should not be NULL", K(ret));
  } else {
    const int64_t timeout_ms = req.timeout();
    const ObRpcPacketCode pcode = req.const_pkt().get_pcode();
    const int64_t size = req.const_pkt().get_encoded_size();

    // notify omt that maybe I'd begin to wait
    this_worker().sched_wait();

    {
      ObWaitEventGuard wait_guard(ObWaitEventIds::SYNC_RPC, timeout_ms, pcode, size, 0);
      ret = transport_->send(req, result);
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(result.pkt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("Result packet should not be NULL", K(ret));
      } else if (OB_FAIL(result.pkt()->verify_checksum())) {
        LOG_WARN("verity response packet checksum fail",
            K(ret), K(*result.pkt()));
      } else {
      }
    }

    // notify omt that maybe my waiting is done
    this_worker().sched_run();
  }

  return ret;
}

int ObRpcProxy::log_user_error_and_warn(const ObRpcResultCode &rcode) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != rcode.rcode_)) {
    FORWARD_USER_ERROR(rcode.rcode_, rcode.msg_);
  }
  for (int i = 0; OB_SUCC(ret) && i < rcode.warnings_.count(); ++i) {
    const common::ObWarningBuffer::WarningItem warning_item = rcode.warnings_.at(i);
    if (ObLogger::USER_WARN == warning_item.log_level_) {
      FORWARD_USER_WARN(warning_item.code_, warning_item.msg_);
    } else if (ObLogger::USER_NOTE == warning_item.log_level_) {
      FORWARD_USER_NOTE(warning_item.code_, warning_item.msg_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown log type", K(ret));
    }
  }
  return ret;
}

int64_t ObRpcProxy::calc_payload_size(int64_t user_payload)
{
  int64_t payload = user_payload;
  if (!g_runtime_enabled) {
    payload += ObIRpcExtraPayload::instance().get_serialize_size();
  } else {
    ObRuntimeContext *ctx = &(get_ob_runtime_context());
    if (ctx != nullptr) {
      payload += ctx->get_serialize_size();
    }
  }
  if (OBTRACE->is_inited()) {
    payload += OBTRACE->get_serialize_size();
  }
  return payload;
}

int ObRpcProxy::fill_extra_payload(
    ObReqTransport::Request &req, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (!g_runtime_enabled) {
    if (OB_FAIL(common::serialization::encode(
                    req.buf(), len, pos, ObIRpcExtraPayload::instance()))) {
      LOG_WARN("serialize debug sync actions fail", K(ret), K(pos), K(len));
    }
  } else {
    ObRuntimeContext *ctx = &(get_ob_runtime_context());
    if (ctx != nullptr) {
      if (OB_FAIL(common::serialization::encode(
                      req.buf(), len, pos, *ctx))) {
        LOG_WARN("serialize context fail", K(ret), K(pos), K(len));
      } else {
        req.pkt()->set_has_context();
        req.pkt()->set_disable_debugsync();
      }
    }
  }
  if (OBTRACE->is_inited() && OB_SUCC(ret)) {
    if (OB_FAIL(common::serialization::encode(req.buf(), len, pos, *OBTRACE))) {
      LOG_WARN("serialize failed", K(ret), K(len), K(pos));
    } else {
      req.pkt()->set_has_trace_info();
    }
  }
  return ret;
}

int ObRpcProxy::create_request(
    const obrpc::ObRpcPacketCode pcode,
    const rpc::frame::ObReqTransport &transport,
    rpc::frame::ObReqTransport::Request &req, const common::ObAddr &addr,
    int64_t size, int64_t timeout,
    const common::ObAddr &local_addr,
    bool do_ratelimit,
    uint32_t is_bg_flow,
    const common::ObString &ob_ssl_invited_nodes,
    const rpc::frame::ObReqTransport::AsyncCB *cb)
{
  PCodeGuard pcode_guard(pcode);
  return transport.create_request(req, addr, size, timeout, local_addr, do_ratelimit, is_bg_flow, ob_ssl_invited_nodes, cb);
}

void ObRpcProxy::set_handle_attr(Handle* handle, const ObRpcPacketCode& pcode, const ObRpcOpts& opts, bool is_stream_next, int64_t session_id, int64_t pkt_id, int64_t send_ts) {
  if (handle) {
    handle->pcode_ = pcode;
    handle->opts_ = opts;
    handle->has_more_ = is_stream_next;
    handle->sessid_ = session_id;
    handle->dst_ = dst_;
    handle->proxy_ = *this;
    handle->do_ratelimit_ = do_ratelimit_;
    handle->is_bg_flow_ = is_bg_flow_;
    handle->transport_ = NULL;
    if (is_stream_next) {
      handle->first_pkt_id_ = pkt_id;
      stream_rpc_register(pkt_id, send_ts);
    }
  }
}
