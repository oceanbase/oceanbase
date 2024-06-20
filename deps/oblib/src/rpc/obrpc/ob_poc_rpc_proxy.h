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

#ifndef OCEANBASE_OBRPC_OB_POC_RPC_PROXY_H_
#define OCEANBASE_OBRPC_OB_POC_RPC_PROXY_H_
#include "rpc/obrpc/ob_rpc_endec.h"
#include "rpc/frame/ob_req_transport.h"
#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_stat.h"
#include "rpc/obrpc/ob_poc_rpc_server.h"

extern "C" {
#include "rpc/pnio/interface/group.h"
}
namespace oceanbase
{
namespace obrpc
{
class ObSyncRespCallback
{
public:
  ObSyncRespCallback(ObRpcMemPool& pool)
    : pkt_nio_cb_(NULL), pool_(pool), resp_(NULL), sz_(0), cond_(0), send_ret_(common::OB_SUCCESS), gtid_(0), pkt_id_(0){}
  ~ObSyncRespCallback() {}
  void* alloc(int64_t sz) { return pool_.alloc(sz); }
  int handle_resp(int io_err, const char* buf, int64_t sz);
  int wait(const int64_t wait_timeout_us, const int64_t pcode, const int64_t req_sz);
  const char* get_resp(int64_t& sz) {
    sz = sz_;
    return resp_;
  }
  static int client_cb(void* arg, int io_err, const char* b, int64_t sz) {
    int ret = ((ObSyncRespCallback*)arg)->handle_resp(io_err, b, sz);
    return ret;
  }
private:
  void* pkt_nio_cb_;
  ObRpcMemPool& pool_;
  char* resp_;
  int64_t sz_;
  int cond_;
  int send_ret_;
public:
  uint64_t gtid_;
  uint32_t pkt_id_;
};

typedef rpc::frame::ObReqTransport::AsyncCB UAsyncCB;
class Handle;
class ObAsyncRespCallback
{
public:
  ObAsyncRespCallback(ObRpcMemPool& pool, UAsyncCB* ucb): pkt_nio_cb_(NULL), pool_(pool), ucb_(ucb) {}
  ~ObAsyncRespCallback() {}
  // static ObAsyncRespCallback* create(ObRpcMemPool& pool, UAsyncCB* ucb);
  static int create(ObRpcMemPool& pool, UAsyncCB* ucb, ObAsyncRespCallback*& ret_cb);
  UAsyncCB* get_ucb() { return ucb_; }
  int handle_resp(int io_err, const char* buf, int64_t sz);
  static int client_cb(void* arg, int io_error, const char* b, int64_t sz) {
    int ret = common::OB_SUCCESS;
    if (arg != NULL) {
      ret = ((ObAsyncRespCallback*)arg)->handle_resp(io_error, b, sz);
    }
    return ret;
  }

private:
  void* pkt_nio_cb_;
  ObRpcMemPool& pool_;
  UAsyncCB* ucb_;
public:
  uint64_t gtid_;
  uint32_t pkt_id_;
};

void init_ucb(ObRpcProxy& proxy, UAsyncCB* ucb, const common::ObAddr& addr, int64_t send_ts, int64_t payload_sz);

template<typename UCB, typename Input>
    void set_ucb_args(UCB* ucb, const Input& args)
{
  ucb->set_args(args);
}

template<typename NoneType>
    void set_ucb_args(UAsyncCB* ucb, const NoneType& none)
{
  UNUSED(ucb);
  UNUSED(none);
}

class ObPocClientStub
{
public:
  ObPocClientStub() {}
  ~ObPocClientStub() {}
  static int64_t get_proxy_timeout(ObRpcProxy& proxy);
  static void set_rcode(ObRpcProxy& proxy, const ObRpcResultCode& rcode);
  static int check_blacklist(const common::ObAddr& addr);
  static void set_handle(ObRpcProxy& proxy, Handle* handle, const ObRpcPacketCode& pcode, const ObRpcOpts& opts, bool is_stream_next, int64_t session_id, int64_t pkt_id, int64_t send_ts);
  static int32_t get_proxy_group_id(ObRpcProxy& proxy);
  static uint8_t balance_assign_tidx()
  {
    static uint8_t s_rpc_tidx CACHE_ALIGNED;
    return ATOMIC_FAA(&s_rpc_tidx, 1);
  }
  static int translate_io_error(int io_err);
  template<typename Input, typename Output>
  int send(ObRpcProxy& proxy, const common::ObAddr& addr, ObRpcPacketCode pcode, const Input& args, Output& out, Handle* handle, const ObRpcOpts& opts) {
    int sys_err = 0;
    int ret = common::OB_SUCCESS;
    const int64_t start_ts = common::ObTimeUtility::current_time();
    int64_t src_tenant_id = ob_get_tenant_id();
    if (get_proxy_group_id(proxy) == ObPocServerHandleContext::OBCG_ELECTION) {
      src_tenant_id = OB_SERVER_TENANT_ID;
    }
    auto &set = obrpc::ObRpcPacketSet::instance();
    const char* pcode_label = set.name_of_idx(set.idx_of_pcode(pcode));
    ObRpcMemPool pool(src_tenant_id, pcode_label);
    ObSyncRespCallback cb(pool);
    char* req = NULL;
    int64_t req_sz = 0;
    const char* resp = NULL;
    int64_t resp_sz = 0;
    ObRpcPacket resp_pkt;
    ObRpcResultCode rcode;
    sockaddr_storage sock_addr;
    uint8_t thread_id = balance_assign_tidx();
    uint64_t pnio_group_id = ObPocRpcServer::DEFAULT_PNIO_GROUP;
    // TODO:@fangwu.lcc map proxy.group_id_ to pnio_group_id
    if (OB_LS_FETCH_LOG2 == pcode) {
      pnio_group_id = ObPocRpcServer::RATELIMIT_PNIO_GROUP;
    }
    {
      lib::Thread::RpcGuard guard(addr, pcode);
      if (OB_FAIL(rpc_encode_req(proxy, pool, pcode, args, opts, req, req_sz, false))) {
        RPC_LOG(WARN, "rpc encode req fail", K(ret));
      } else if(OB_FAIL(check_blacklist(addr))) {
        RPC_LOG(WARN, "check_blacklist failed", K(ret));
      } else {
        const pn_pkt_t pkt = {
          req,
          req_sz,
          start_ts + get_proxy_timeout(proxy),
          static_cast<int16_t>(set.idx_of_pcode(pcode)),
          ObSyncRespCallback::client_cb,
          &cb
        };
        cb.gtid_ = (pnio_group_id<<32) + thread_id;
        if (0 != (sys_err = pn_send((pnio_group_id<<32) + thread_id, addr.to_sockaddr(&sock_addr), &pkt, &cb.pkt_id_))) {
          ret = translate_io_error(sys_err);
          RPC_LOG(WARN, "pn_send fail", K(sys_err), K(addr), K(pcode));
        }
      }
      if (OB_SUCC(ret)) {
        EVENT_INC(RPC_PACKET_OUT);
        EVENT_ADD(RPC_PACKET_OUT_BYTES, req_sz);
        int64_t timeout = get_proxy_timeout(proxy);
        if (OB_FAIL(cb.wait(timeout, pcode, req_sz))) {
          RPC_LOG(WARN, "sync rpc execute fail", K(ret), K(addr), K(pcode), K(timeout));
        } else if (NULL == (resp = cb.get_resp(resp_sz))) {
          ret = common::OB_ERR_UNEXPECTED;
          RPC_LOG(WARN, "sync rpc execute success but resp is null", K(ret), K(addr), K(pcode), K(timeout));
        } else if (OB_FAIL(rpc_decode_resp(resp, resp_sz, out, resp_pkt, rcode))) {
          RPC_LOG(WARN, "execute rpc fail", K(addr), K(pcode), K(ret), K(timeout));
        }
      }
    }
    if (rcode.rcode_ != OB_DESERIALIZE_ERROR) {
      int wb_ret = OB_SUCCESS;
      if (common::OB_SUCCESS != (wb_ret = log_user_error_and_warn(rcode))) {
        RPC_OBRPC_LOG(WARN, "fail to log user error and warn", K(ret), K(wb_ret), K((rcode)));
      }
      set_rcode(proxy, rcode);
      if (OB_SUCC(ret) && handle) {
        int64_t pkt_id = static_cast<int64_t>(cb.pkt_id_);
        set_handle(proxy, handle, pcode, opts, resp_pkt.is_stream_next(), resp_pkt.get_session_id(), pkt_id, start_ts);
      }
    }
    rpc::RpcStatPiece piece;
    piece.size_ = req_sz;
    piece.time_ = ObTimeUtility::current_time() - start_ts;
    if (OB_FAIL(ret)) {
      piece.failed_ = true;
      if (OB_TIMEOUT == ret) {
        piece.is_timeout_ = true;
      }
    }
    RPC_STAT(pcode, src_tenant_id, piece);
    return ret;
  }
  template<typename Input, typename UCB>
  int post(ObRpcProxy& proxy, const common::ObAddr& addr, ObRpcPacketCode pcode, const Input& args, UCB* ucb, const ObRpcOpts& opts) {
    int sys_err = 0;
    int ret = common::OB_SUCCESS;
    const int64_t start_ts = common::ObTimeUtility::current_time();
    ObRpcMemPool* pool = NULL;
    uint64_t pnio_group_id = ObPocRpcServer::DEFAULT_PNIO_GROUP;
    char rpc_timeguard_str[ObPocRpcServer::RPC_TIMEGUARD_STRING_SIZE] = {'\0'};
    ObTimeGuard timeguard("poc_rpc_post", 10 * 1000);
    // TODO:@fangwu.lcc map proxy.group_id_ to pnio_group_id
    if (OB_LS_FETCH_LOG2 == pcode) {
      pnio_group_id = ObPocRpcServer::RATELIMIT_PNIO_GROUP;
    }
    uint8_t thread_id = balance_assign_tidx();
    int64_t src_tenant_id = ob_get_tenant_id();
    if (get_proxy_group_id(proxy) == ObPocServerHandleContext::OBCG_ELECTION) {
      src_tenant_id = OB_SERVER_TENANT_ID;
    }
    const int init_alloc_sz = 0;
    auto &set = obrpc::ObRpcPacketSet::instance();
    const char* pcode_label = set.name_of_idx(set.idx_of_pcode(pcode));
    ObAsyncRespCallback* cb = NULL;
    if (NULL == (pool = ObRpcMemPool::create(src_tenant_id, pcode_label, init_alloc_sz))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      char* req = NULL;
      int64_t req_sz = 0;
      uint32_t* pkt_id_ptr = NULL;
      timeguard.click();
      if (OB_FAIL(rpc_encode_req(proxy, *pool, pcode, args, opts, req, req_sz, NULL == ucb))) {
        RPC_LOG(WARN, "rpc encode req fail", K(ret));
      } else if(OB_FAIL(check_blacklist(addr))) {
        RPC_LOG(WARN, "check_blacklist failed", K(addr));
      } else if (FALSE_IT(timeguard.click())) {
      } else if (OB_FAIL(ObAsyncRespCallback::create(*pool, ucb, cb))) {
        RPC_LOG(WARN, "create ObAsyncRespCallback failed", K(ucb));
      } else if (OB_NOT_NULL(cb)) {
        auto newcb = reinterpret_cast<UCB*>(cb->get_ucb());
        if (newcb) {
          set_ucb_args(newcb, args);
          init_ucb(proxy, cb->get_ucb(), addr, start_ts, req_sz);
        }
        cb->gtid_ = (pnio_group_id<<32) + thread_id;
        pkt_id_ptr = &cb->pkt_id_;
      }
      IGNORE_RETURN snprintf(rpc_timeguard_str, sizeof(rpc_timeguard_str), "sz=%ld,pcode=%x,id=%ld", req_sz, pcode, src_tenant_id);
      timeguard.click(rpc_timeguard_str);
      if (OB_SUCC(ret)) {
        sockaddr_storage sock_addr;
        const pn_pkt_t pkt = {
          req,
          req_sz,
          start_ts + get_proxy_timeout(proxy),
          static_cast<int16_t>(set.idx_of_pcode(pcode)),
          ObAsyncRespCallback::client_cb,
          cb
        };
        if (0 != (sys_err = pn_send(
            (pnio_group_id<<32) + thread_id,
            addr.to_sockaddr(&sock_addr),
            &pkt,
            pkt_id_ptr))) {
          ret = translate_io_error(sys_err);
          RPC_LOG(WARN, "pn_send fail", K(sys_err), K(addr), K(pcode));
        } else {
          EVENT_INC(RPC_PACKET_OUT);
          EVENT_ADD(RPC_PACKET_OUT_BYTES, req_sz);
        }
      }
    }
    if (NULL != pool) {
      if (ret != OB_SUCCESS || cb == NULL) {
        // if ucb is null, the ObAsyncRespCallback::create will return OB_SUCCESS and cb will set to null, in this case we should release pool in place
        pool->destroy();
      }
    }
    return ret;
  }

  int log_user_error_and_warn(const ObRpcResultCode &rcode) const;
};

extern ObPocClientStub global_poc_client;
#define POC_RPC_INTERCEPT(func, args...) if (transport_impl_ == rpc::ObRequest::TRANSPORT_PROTO_POC && global_poc_server.client_use_pkt_nio()) return global_poc_client.func(*this, args);
}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_POC_RPC_PROXY_H_ */
