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

#include "observer/net/ob_rpc_reverse_keepalive.h"
#include "rpc/obrpc/ob_rpc_reverse_keepalive_struct.h"
#define USING_LOG_PREFIX RPC_FRAME
namespace oceanbase
{
namespace obrpc
{
int ObRpcReverseKeepaliveArg::assign(const ObRpcReverseKeepaliveArg &other)
{
  int ret = OB_SUCCESS;
  dst_ = other.dst_;
  first_send_ts_ = other.first_send_ts_;
  pkt_id_ = other.pkt_id_;
  return ret;
}
OB_SERIALIZE_MEMBER(ObRpcReverseKeepaliveArg, dst_, first_send_ts_, pkt_id_);
OB_SERIALIZE_MEMBER(ObRpcReverseKeepaliveResp, ret_);

ObRpcReverseKeepAliveService rpc_reverse_keepalive_instance;

int ObRpcReverseKeepAliveService::init(ObSrvRpcProxy *srv_rpc_proxy)
{
  int ret = OB_SUCCESS;
  rpc_proxy_ = srv_rpc_proxy;
  if (OB_FAIL(rpc_pkt_id_map_.init("RpcKeepalive"))) {
    LOG_ERROR("init rpc pkt_id map failed");
  } else {
    init_ = true;
  }
  return ret;
}
int ObRpcReverseKeepAliveService::destroy()
{
  if (init_) {
    rpc_pkt_id_map_.destroy();
  }
  return OB_SUCCESS;
}
int ObRpcReverseKeepAliveService::receiver_probe(const ObRpcReverseKeepaliveArg &arg)
{
  int ret = OB_SUCCESS;
  if OB_ISNULL(rpc_proxy_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc reverse keepalive rpc proxy is null");
  } else {
    const int64_t rpc_timeout = GCONF.rpc_timeout;
    ObAddr addr = arg.dst_;
    int64_t tenant_id = ob_get_tenant_id();
    ObRpcReverseKeepaliveResp res;
    ret = rpc_proxy_->to(addr).by(tenant_id).as(OB_SYS_TENANT_ID).timeout(rpc_timeout).rpc_reverse_keepalive(arg, res);
    if (OB_SUCC(ret)) {
      ret = res.ret_;
    }
    if OB_FAIL(ret) {
      LOG_WARN("send reverse keepalive failed", K(tenant_id), K(arg));
    }
  }
  return ret;
}
int ObRpcReverseKeepAliveService::sender_register(const int64_t pkt_id, int64_t send_time_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!init_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rpc reverse keepalive service is not inited");
  } else {
    RpcPktID key(pkt_id);
    if (OB_FAIL(rpc_pkt_id_map_.insert(key, send_time_us))) {
      LOG_WARN("register pkt_id failed", K(key), K(send_time_us));
    }
  }
  return ret;
}
int ObRpcReverseKeepAliveService::sender_unregister(const int64_t pkt_id)
{
  int ret = OB_SUCCESS;
  RpcPktID key(pkt_id);
  if (OB_UNLIKELY(!init_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rpc reverse keepalive service is not inited", K(pkt_id));
  } else if (OB_FAIL(rpc_pkt_id_map_.erase(key))) {
    LOG_WARN("unregister pkt_id failed", K(pkt_id));
  }
  return ret;
}
int ObRpcReverseKeepAliveService::check_status(const int64_t send_time_us, const int64_t pkt_id)
{
  int ret = OB_SUCCESS;
  int64_t time_us = 0;
  RpcPktID key(pkt_id);
  if (OB_UNLIKELY(!init_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rpc reverse keepalive service is not inited", K(pkt_id));
  } else if OB_FAIL(rpc_pkt_id_map_.get(key, time_us)) {
    LOG_INFO("orignal rpc has released", K(ret), K(pkt_id));
  } else if (time_us > send_time_us) {
    ret = OB_HASH_NOT_EXIST;
    LOG_WARN("send_ts check failed, client has been restarted and the pkt_id is reused", K(time_us), K(send_time_us), K(pkt_id));
  }
  return ret;
}
void stream_rpc_register(const int64_t pkt_id, int64_t send_time_us)
{
  IGNORE_RETURN rpc_reverse_keepalive_instance.sender_register(pkt_id, send_time_us);
}
void stream_rpc_unregister(const int64_t pkt_id)
{
  IGNORE_RETURN rpc_reverse_keepalive_instance.sender_unregister(pkt_id);
}
int stream_rpc_reverse_probe(const ObRpcReverseKeepaliveArg& reverse_keepalive_arg)
{
  return rpc_reverse_keepalive_instance.receiver_probe(reverse_keepalive_arg);
}

}; // end namespace obrpc

namespace observer
{
int ObRpcReverseKeepaliveP::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rpc reverse keepalive arg invalid", K(arg_));
  } else {
    result_.ret_ = obrpc::rpc_reverse_keepalive_instance.check_status(arg_.first_send_ts_, arg_.pkt_id_);
  }
  return ret;
}
}; // end of namespace observer
}; // end namespace oceanbase
