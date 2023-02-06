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

#include "lib/oblog/ob_log_module.h"
#include "observer/ob_server_struct.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/serialization.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "ob_rl_rpc.h"
#include "ob_rl_mgr.h"

namespace oceanbase {
namespace share {

using namespace oceanbase::obrpc;
using namespace oceanbase::rpc::frame;

OB_SERIALIZE_MEMBER(ObRlGetRegionBWRequest, addr_);
OB_SERIALIZE_MEMBER(ObRlGetRegionBWResponse, addr_, ob_region_bw_list_);

int ObRLGetRegionBWCallback::init(ObRatelimitMgr *rl_mgr, common::ObAddr &self_addr)
{
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(ERROR, "ObRLGetRegionBWCallback inited twice");
  } else {
    rl_mgr_ = rl_mgr;
    self_addr_ = self_addr;
    is_inited_ = true;
    OB_LOG(INFO, "ObRLGetRegionBWCallback inited");
  }
  return ret;
}

void ObRLGetRegionBWCallback::destroy()
{
  is_inited_ = false;
}

oceanbase::rpc::frame::ObReqTransport::AsyncCB* ObRLGetRegionBWCallback::clone(
    const oceanbase::rpc::frame::SPAlloc &alloc) const
{
  void *buf = NULL;
  ObRLGetRegionBWCallback *newcb = NULL;
  if (IS_NOT_INIT) {
    OB_LOG_RET(ERROR, OB_NOT_INIT, "ObRLGetRegionBWCallback not inited.");
  } else {
    buf = alloc(sizeof (*this));
    if (OB_ISNULL(buf)) {
      OB_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "Failed to alloc memory for ObRLGetRegionBWCallback clone", KP(buf));
    } else {
      newcb = new (buf) ObRLGetRegionBWCallback();
      newcb->dst_server_idx_ = dst_server_idx_;
      newcb->dst_server_addr_ = dst_server_addr_;
      newcb->rl_mgr_ = rl_mgr_;
      newcb->is_inited_ = is_inited_;
    }
  }
  return newcb;
}

int ObRLGetRegionBWCallback::process()
{
  int ret = OB_SUCCESS;
  ObRlGetRegionBWResponse &response = result_;
  ObRegionBW *region_bw = NULL;
  ObRegionBwSEArray region_bw_list;

  OB_LOG(DEBUG, "receive RPC response.", K(rcode_.rcode_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(ERROR, "ObRLGetRegionBWCallback not inited.");
  } else if (OB_SUCCESS != rcode_.rcode_) {
    OB_LOG(ERROR, "rpc returned error.", K(rcode_.rcode_));
    ret = rcode_.rcode_;
  }
  if (OB_SUCCESS == ret) {
    OB_LOG(DEBUG, "call get_server_bw_rpc_cb.", K(rcode_.rcode_), K(response.addr_), K(dst_server_addr_), K(dst_server_idx_));
    if (OB_FAIL(rl_mgr_->get_server_bw_rpc_cb(response.addr_, dst_server_idx_, response.ob_region_bw_list_))) {
      OB_LOG(ERROR, "failed to call get_server_bw_rpc_cb.");
    }
  } else {
    if (OB_FAIL(rl_mgr_->get_server_bw_rpc_cb(response.addr_, dst_server_idx_, region_bw_list))) {
      OB_LOG(ERROR, "failed to call get_server_bw_rpc_cb.");
    }
  }
  return ret;
}

int ObRLGetRegionBWCallback::on_error(int easy_err)
{
  int ret = OB_SUCCESS;
  ObRegionBwSEArray region_bw_list;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObRLGetRegionBWCallback not inited.");
  } else {
    if (easy_err == EASY_TIMEOUT) {
      OB_LOG(WARN, "rpc timed out", K(easy_err));
    } else {
      OB_LOG(WARN, "receive RPC response with error", K(easy_err));
    }
    if (OB_FAIL(rl_mgr_->get_server_bw_rpc_cb(dst_server_addr_, dst_server_idx_, region_bw_list))) {
      OB_LOG(WARN, "failed to call get_server_bw_rpc_cb.");
    }
  }
  return ret;
}

void ObRLGetRegionBWCallback::on_invalid()
{
  int ret = OB_SUCCESS;
  ObRegionBwSEArray region_bw_list;

  if (IS_NOT_INIT) {
    OB_LOG(WARN, "ObRLGetRegionBWCallback not inited.");
  } else {
    OB_LOG(WARN, "receive RPC response with invalid error.");
    if (OB_FAIL(rl_mgr_->get_server_bw_rpc_cb(dst_server_addr_, dst_server_idx_, region_bw_list))) {
      OB_LOG(WARN, "failed to call get_server_bw_rpc_cb.");
    }
  }
}

void ObRLGetRegionBWCallback::set_args(const Request &arg)
{
  UNUSED(arg);
}

void ObRLGetRegionBWCallback::set_dst_addr(common::ObAddr &dst_addr)
{
  dst_server_addr_ = dst_addr;
}

void ObRLGetRegionBWCallback::set_dst_server_idx(int dst_server_idx)
{
  dst_server_idx_ = dst_server_idx;
}

ObRLGetRegionBWP::ObRLGetRegionBWP(const observer::ObGlobalContext &global_ctx)
{
   global_ctx_ = &global_ctx;
   net_ = global_ctx.net_frame_->get_net_easy();
   rl_mgr_ = global_ctx.rl_mgr_;
}

int ObRLGetRegionBWP::process()
{
  int ret = OB_SUCCESS;
  int64_t bw = 0, max_bw = 0;
  ObRlGetRegionBWRequest  &request  = arg_;
  ObRlGetRegionBWResponse &response = result_;
  const ObRegionBwStatSEArray &region_list = rl_mgr_->get_peer_region_bw_list();
  OB_LOG(DEBUG, "Received RPC request", K(request.addr_));
  response.addr_ = request.addr_;
  for (int i = 0; i < region_list.count(); i++) {
    if (OB_FAIL(net_->get_easy_region_latest_bw(region_list[i].region_.ptr(), &bw, &max_bw))) {
      OB_LOG(WARN, "failed to call get_easy_region_latest_bw.", K(ret), K(region_list[i].region_));
    } else {
      OB_LOG(DEBUG, "add the bandwidth of current server to region. ", K(i), K(region_list[i].region_), K(bw), K(max_bw));
      response.ob_region_bw_list_.push_back(ObRegionBW(region_list[i].region_, bw, max_bw));
      OB_LOG(DEBUG, "add the bandwidth of current server to region. ", K(i), K(response.ob_region_bw_list_[i].region_),
          K(response.ob_region_bw_list_[i].bw_), K(response.ob_region_bw_list_[i].max_bw_));
    }
  }
  OB_LOG(DEBUG, "RPC request-handling done", K(ret));
  return ret;
};

ObRatelimitRPC::ObRatelimitRPC()
{
  gctx_ = NULL;
  net_  = NULL;
  rl_mgr_ = NULL;
  is_inited_ = false;
}

int ObRatelimitRPC::get_server_bw(common::ObAddr &ob_addr, int server_idx)
{
  int ret = common::OB_SUCCESS;
  ObRlGetRegionBWRequest request;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(ERROR, "ObRatelimitRPC not inited.");
  } else {
    request.addr_ = ob_addr;
    rl_rpc_cb_.set_dst_addr(ob_addr);
    rl_rpc_cb_.set_dst_server_idx(server_idx);
    if (OB_FAIL(rl_rpc_proxy_.to(ob_addr)
                             .dst_cluster_id(dst_cluster_id_)
                             .by(tenant_id_)
                             .as(OB_SYS_TENANT_ID)
                             .timeout(RL_RPC_TIMEOUT)
                             .get_region_bw(request, &rl_rpc_cb_))) {
      OB_LOG(ERROR, "failed to do rpc call.", K(ret));
    }
  }
  return ret;
}

int ObRatelimitRPC::init(common::ObAddr& self_addr,
                         ObNetEasy *net,
                         observer::ObGlobalContext *gctx,
                         rpc::frame::ObReqTransport *transport,
                         ObRatelimitMgr *rl_mgr)
{
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(ERROR, "ObRatelimitRPC inited twice.");
  } else if (OB_ISNULL(net) || OB_ISNULL(gctx) || OB_ISNULL(transport) || OB_ISNULL(rl_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(ERROR, "invalid argument", KP(net), KP(transport), KP(rl_mgr));
  } else {
    net_ = net;
    gctx_ = gctx;
    rl_mgr_ = rl_mgr;
    self_addr_ = self_addr;
    tenant_id_ = OB_SYS_TENANT_ID;
    dst_cluster_id_ = obrpc::ObRpcNetHandler::CLUSTER_ID;
    rl_rpc_cb_.init(rl_mgr, self_addr);
    rl_rpc_proxy_.init(transport, self_addr);
    is_inited_ = true;
  }
  return ret;
}

void ObRatelimitRPC::destroy()
{
  rl_rpc_cb_.destroy();
  is_inited_ = false;
}
} // namespace share
} // namespace oceanbase
