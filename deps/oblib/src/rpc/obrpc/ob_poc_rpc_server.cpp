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

#include "rpc/obrpc/ob_poc_rpc_server.h"
#include "lib/oblog/ob_log_module.h"
#include "rpc/obrpc/ob_net_keepalive.h"

#define rk_log_macro(level, ret, format, ...) _OB_LOG_RET(level, ret, "PNIO " format, ##__VA_ARGS__)
#include "lib/lock/ob_futex.h"
extern "C" {
#include "rpc/pnio/interface/group.h"
#include "ussl-hook.h"
};
#include "rpc/obrpc/ob_rpc_endec.h"
#define cfgi(k, v) atoi(getenv(k)?:v)

namespace oceanbase
{
namespace obrpc
{
extern const int easy_head_size;
ObPocRpcServer global_poc_server;
ObListener* global_ob_listener;
bool __attribute__((weak)) enable_pkt_nio(bool start_as_client) {
  UNUSED(start_as_client);
  return false;
}
int64_t  __attribute__((weak)) get_max_rpc_packet_size() {
  return OB_MAX_RPC_PACKET_LENGTH;
}
}; // end namespace obrpc
}; // end namespace oceanbase

using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::rpc;

frame::ObReqDeliver* global_deliver;
int ObPocServerHandleContext::create(int64_t resp_id, const char* buf, int64_t sz, ObRequest*& req)
{
  int ret = OB_SUCCESS;
  ObPocServerHandleContext* ctx = NULL;
  ObRpcPacket tmp_pkt;
  ObTimeGuard timeguard("rpc_request_create", 200 * 1000);
  const int64_t alloc_payload_sz = sz;
  if (OB_FAIL(tmp_pkt.decode(buf, sz))) {
    RPC_LOG(ERROR, "decode packet fail", K(ret));
  } else {
    ObCurTraceId::set(tmp_pkt.get_trace_id());
    obrpc::ObRpcPacketCode pcode = tmp_pkt.get_pcode();
    auto &set = obrpc::ObRpcPacketSet::instance();
    const char* pcode_label = set.label_of_idx(set.idx_of_pcode(pcode));
    const int64_t pool_size = sizeof(ObPocServerHandleContext) + sizeof(ObRequest) + sizeof(ObRpcPacket) + alloc_payload_sz;
    int64_t tenant_id = tmp_pkt.get_tenant_id();
    if (OB_UNLIKELY(tmp_pkt.get_group_id() == OBCG_ELECTION)) {
      tenant_id = OB_SERVER_TENANT_ID;
    }
    timeguard.click();
    ObRpcMemPool* pool = ObRpcMemPool::create(tenant_id, pcode_label, pool_size);
    void *temp = NULL;

#ifdef ERRSIM
    THIS_WORKER.set_module_type(tmp_pkt.get_module_type());
#endif

    if (OB_ISNULL(pool)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      RPC_LOG(WARN, "create memory pool failed", K(tenant_id), K(pcode_label));
    } else if (OB_ISNULL(temp = pool->alloc(sizeof(ObPocServerHandleContext) + sizeof(ObRequest)))){
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      RPC_LOG(WARN, "pool allocate memory failed", K(tenant_id), K(pcode_label));
    } else {
      int64_t resp_expired_abs_us = ObTimeUtility::current_time() + tmp_pkt.get_timeout();
      ctx = new(temp)ObPocServerHandleContext(*pool, resp_id, resp_expired_abs_us);
      ctx->set_peer_unsafe();
      req = new(ctx + 1)ObRequest(ObRequest::OB_RPC, ObRequest::TRANSPORT_PROTO_POC);
      timeguard.click();
      ObRpcPacket* pkt = (ObRpcPacket*)pool->alloc(sizeof(ObRpcPacket) + alloc_payload_sz);
      if (NULL == pkt) {
        RPC_LOG(WARN, "pool allocate rpc packet memory failed", K(tenant_id), K(pcode_label));
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(reinterpret_cast<void *>(pkt), reinterpret_cast<void *>(&tmp_pkt), sizeof(ObRpcPacket));
        const char* packet_data = NULL;
        if (alloc_payload_sz > 0) {
          packet_data = reinterpret_cast<char *>(pkt + 1);
          MEMCPY(const_cast<char*>(packet_data), tmp_pkt.get_cdata(), tmp_pkt.get_clen());
        } else {
          packet_data = tmp_pkt.get_cdata();
        }
        int64_t receive_ts = ObTimeUtility::current_time();
        pkt->set_receive_ts(receive_ts);
        pkt->set_content(packet_data, tmp_pkt.get_clen());
        req->set_server_handle_context(ctx);
        req->set_packet(pkt);
        req->set_receive_timestamp(pkt->get_receive_ts());
        req->set_request_arrival_time(pkt->get_receive_ts());
        req->set_arrival_push_diff(common::ObTimeUtility::current_time());

        const int64_t fly_ts = receive_ts - pkt->get_timestamp();
        if (fly_ts > oceanbase::common::OB_MAX_PACKET_FLY_TS && TC_REACH_TIME_INTERVAL(100 * 1000)) {
          RPC_LOG(WARN, "PNIO packet wait too much time between proxy and server_cb", "pcode", pkt->get_pcode(),
                  "fly_ts", fly_ts, "send_timestamp", pkt->get_timestamp());
        }
      }
    }
  }
  return ret;
}

void ObPocServerHandleContext::resp(ObRpcPacket* pkt)
{
  int ret = OB_SUCCESS;
  int sys_err = 0;
  char reserve_buf[2048]; // reserve stack memory for response packet buf
  char* buf = reserve_buf;
  int64_t sz = 0;
  if (NULL == pkt) {
    // do nothing
  } else if (OB_FAIL(rpc_encode_ob_packet(pool_, pkt, buf, sz, sizeof(reserve_buf)))) {
    RPC_LOG(WARN, "rpc_encode_ob_packet fail", KP(pkt), K(sz));
    buf = NULL;
    sz = 0;
  }
  if ((sys_err = pn_resp(resp_id_, buf, sz, resp_expired_abs_us_)) != 0) {
    RPC_LOG(WARN, "pn_resp fail", K(resp_id_), K(sys_err));
  }
}

int ObPocServerHandleContext::resp_error(uint64_t resp_id, int err_code, const char* b, const int64_t sz)
{
  int ret = OB_SUCCESS;
  ObRpcResultCode rcode;
  rcode.rcode_ = err_code;
  char tmp_buf[sizeof(rcode)];
  int64_t pos = 0;
  if (OB_FAIL(rcode.serialize(tmp_buf, sizeof(tmp_buf), pos))) {
    RPC_LOG(ERROR, "serialize rcode fail", K(pos));
  } else {
    ObRpcPacket res_pkt;
    res_pkt.set_content(tmp_buf, pos);
    if (b != NULL && sz > 0) {
      int tmp_ret = OB_SUCCESS;
      ObRpcPacket recv_pkt;
      if (OB_TMP_FAIL(recv_pkt.decode(b, sz))) {
        RPC_LOG_RET(ERROR, tmp_ret, "decode packet fail");
      } else {
        res_pkt.set_pcode(recv_pkt.get_pcode());
        res_pkt.set_chid(recv_pkt.get_chid());
        res_pkt.set_trace_id(recv_pkt.get_trace_id());
        res_pkt.set_dst_cluster_id(recv_pkt.get_src_cluster_id());
        int64_t receive_ts = ObTimeUtility::current_time();
        res_pkt.set_request_arrival_time(receive_ts);
#ifdef ERRSIM
        res_pkt.set_module_type(recv_pkt.get_module_type());
#endif
      }
    }
    res_pkt.set_resp();
    res_pkt.set_unis_version(0);
    res_pkt.calc_checksum();
    ObRpcMemPool pool;
    ObPocServerHandleContext dummy(pool, resp_id, OB_INVALID_TIMESTAMP);
    dummy.resp(&res_pkt);
  }
  return ret;
}

void ObPocServerHandleContext::set_peer_unsafe()
{
  struct sockaddr_storage sock_addr;
  if (0 == pn_get_peer(resp_id_, &sock_addr)) {
    if (AF_INET == sock_addr.ss_family) {
      struct sockaddr_in *sin = reinterpret_cast<struct sockaddr_in *>(&sock_addr);
      peer_.set_ipv4_addr(ntohl(sin->sin_addr.s_addr), ntohs(sin->sin_port));
    } else if (AF_INET6 == sock_addr.ss_family) {
      struct sockaddr_in6 *sin6 = reinterpret_cast<struct sockaddr_in6 *>(&sock_addr);
      peer_.set_ipv6_addr(&sin6->sin6_addr.s6_addr, ntohs(sin6->sin6_port));
    }
  }
}

ObAddr ObPocServerHandleContext::get_peer()
{
  return peer_;
}

int serve_cb(int grp, const char* b, int64_t sz, uint64_t resp_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTimeGuard timeguard("rpc_serve_cb", 200 * 1000);
  if (NULL == b || sz <= easy_head_size) {
    tmp_ret = OB_INVALID_DATA;
    RPC_LOG(WARN, "rpc request is invalid", K(tmp_ret), K(b), K(sz));
    b = NULL;
    sz = 0;
  } else {
    b = b + easy_head_size;
    sz = sz - easy_head_size;
    ObRequest* req = NULL;
    if (OB_TMP_FAIL(ObPocServerHandleContext::create(resp_id, b, sz, req))) {
      RPC_LOG(WARN, "created req is null", K(tmp_ret), K(sz), K(resp_id));
    } else {
      timeguard.click();
      global_deliver->deliver(*req);
    }
  }
  if (OB_SUCCESS != tmp_ret) {
    if (OB_TMP_FAIL(ObPocServerHandleContext::resp_error(resp_id, tmp_ret, b, sz))) {
      int sys_err = 0;
      if ((sys_err = pn_resp(resp_id, NULL, 0, OB_INVALID_TIMESTAMP)) != 0) {
        RPC_LOG(WARN, "pn_resp fail", K(resp_id), K(sys_err));
      }
    }
  }
  ObCurTraceId::reset();
  return ret;
}

int ObPocRpcServer::start(int port, int net_thread_count, frame::ObReqDeliver* deliver)
{
  int ret = OB_SUCCESS;
  // init pkt-nio framework
  int lfd = -1;
  int rl_net_thread_count = max(1, net_thread_count/4);
  if ((lfd = pn_listen(port, serve_cb)) == -1) {
    ret = OB_SERVER_LISTEN_ERROR;
    RPC_LOG(ERROR, "pn_listen failed", K(ret));
  } else {
    ATOMIC_STORE(&global_deliver, deliver);
    if (2 == net_thread_count) {
      net_thread_count = 1;
    }
    int count = 0;
    if ((count = pn_provision(lfd, DEFAULT_PNIO_GROUP, net_thread_count)) != net_thread_count) {
      ret = OB_ERR_SYS;
      RPC_LOG(WARN, "pn_provision error", K(count), K(net_thread_count));
    } else if((count = pn_provision(lfd, RATELIMIT_PNIO_GROUP, rl_net_thread_count)) != rl_net_thread_count) {
      ret = OB_ERR_SYS;
      RPC_LOG(WARN, "pn_provision for RATELIMIT_PNIO_GROUP error", K(count), K(rl_net_thread_count));
    } else {
      has_start_ = true;
    }
  }
  return ret;
}

int ObPocRpcServer::start_net_client(int net_thread_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(has_start_)) {
    RPC_LOG(WARN, "client has already started!");
  } else {
    int count = 0;
    const int listen_none = -1;
    if ((count = pn_provision(listen_none, DEFAULT_PNIO_GROUP, net_thread_count)) != net_thread_count) {
      ret = OB_ERR_SYS;
      RPC_LOG(WARN, "pn_provision error", K(count), K(net_thread_count));
    } else if((count = pn_provision(listen_none, RATELIMIT_PNIO_GROUP, net_thread_count)) != net_thread_count) {
      ret = OB_ERR_SYS;
      RPC_LOG(WARN, "pn_provision for RATELIMIT_PNIO_GROUP error", K(count), K(net_thread_count));
    } else {
      has_start_ = true;
      start_as_client_ = true;
    }
  }
  return ret;
}

void ObPocRpcServer::stop()
{
  for (uint64_t gid = 1; gid < END_GROUP; gid++) {
    pn_stop(gid);
  }
}

void ObPocRpcServer::wait()
{
  for (uint64_t gid = 1; gid < END_GROUP; gid++) {
    pn_wait(gid);
  }
}

int ObPocRpcServer::update_tcp_keepalive_params(int64_t user_timeout) {
  int ret = OB_SUCCESS;
  if (pn_set_keepalive_timeout(user_timeout) != user_timeout) {
    ret = OB_INVALID_ARGUMENT;
    RPC_LOG(WARN, "invalid user_timeout", K(user_timeout));
  }
  return ret;
}

int ObPocRpcServer::update_server_standby_fetch_log_bandwidth_limit(int64_t value) {
  int ret = OB_SUCCESS;
  int tmp_err = -1;
  tmp_err = pn_ratelimit(RATELIMIT_PNIO_GROUP, value);
  if (tmp_err != 0) {
    ret = OB_INVALID_ARGUMENT;
    RPC_LOG(WARN, "invalid bandwidth limit value", K(value));
  }
  return ret;
}

int64_t ObPocRpcServer:: get_ratelimit() {
  return pn_get_ratelimit(RATELIMIT_PNIO_GROUP);
}
uint64_t ObPocRpcServer::get_ratelimit_rxbytes() {
  return pn_get_rxbytes(RATELIMIT_PNIO_GROUP);
}
bool ObPocRpcServer::client_use_pkt_nio() {
  return has_start() && enable_pkt_nio(start_as_client_);
}

extern "C" {
void* pkt_nio_malloc(int64_t sz, const char* label) {
  ObMemAttr attr(OB_SERVER_TENANT_ID, label, ObCtxIds::PKT_NIO);
  SET_USE_500(attr);
  return oceanbase::common::ob_malloc(sz, attr);
}
void pkt_nio_free(void *ptr) {
  oceanbase::common::ob_free(ptr);
}
bool server_in_black(struct sockaddr* sa) {
  easy_addr_t ez_addr;
  easy_inet_atoe(sa, &ez_addr);
  return ObNetKeepAlive::get_instance().in_black(ez_addr);
}
int dispatch_to_ob_listener(int accept_fd) {
  int ret = -1;
  if (OB_NOT_NULL(ATOMIC_LOAD(&oceanbase::obrpc::global_ob_listener))) {
    ret = oceanbase::obrpc::global_ob_listener->do_one_event(accept_fd);
  }
  return ret;
}
int tranlate_to_ob_error(int err) {
  int ret = OB_SUCCESS;
  if (PNIO_OK == err) {
  } else if (PNIO_STOPPED == err) {
    ret = OB_RPC_SEND_ERROR;
  } else if (PNIO_LISTEN_ERROR == err) {
    ret = OB_SERVER_LISTEN_ERROR;
  } else if (ENOMEM == err || -ENOMEM == err) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (EINVAL == err || -EINVAL == err) {
    ret = OB_INVALID_ARGUMENT;
  } else if (EIO == err || -EIO == err) {
    ret = OB_IO_ERROR;
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}
#define PKT_NIO_MALLOC(sz, label)  pkt_nio_malloc(sz, label)
#define PKT_NIO_FREE(ptr)   pkt_nio_free(ptr)
#define SERVER_IN_BLACK(sa) server_in_black(sa)
#define DISPATCH_EXTERNAL(accept_fd) dispatch_to_ob_listener(accept_fd)
#include "rpc/pnio/pkt-nio.c"
};
