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
#include "rpc/obrpc/ob_nio_interface.h"
#include "rpc/obrpc/ob_rpc_endec.h"
#include "rpc/frame/ob_req_transport.h"
#include "rpc/ob_request.h"

namespace oceanbase
{
namespace obrpc
{
class ObSyncRespCallback: public IRespHandler
{
public:
  ObSyncRespCallback(ObRpcMemPool& pool): pool_(pool), resp_(NULL), sz_(0) {}
  virtual ~ObSyncRespCallback() {}
  void* alloc(int64_t sz) { return pool_.alloc(sz); }
  int handle_resp(int io_err, char* buf, int64_t sz);
  char* get_resp(int64_t& sz) {
    sz = sz_;
    return resp_;
  }
private:
  ObRpcMemPool& pool_;
  char* resp_;
  int64_t sz_;
};

typedef rpc::frame::ObReqTransport::AsyncCB UAsyncCB;
class ObAsyncRespCallback: public IRespHandler
{
public:
  ObAsyncRespCallback(ObRpcMemPool& pool, UAsyncCB& ucb): pool_(pool), ucb_(ucb) {}
  ~ObAsyncRespCallback() {}
  static ObAsyncRespCallback* create(ObRpcMemPool& pool, UAsyncCB& ucb);
  UAsyncCB& get_ucb() { return ucb_; }
  void* alloc(int64_t sz) { return pool_.alloc(sz); }
  int handle_resp(int io_err, char* buf, int64_t sz);
private:
  ObRpcMemPool& pool_;
  UAsyncCB& ucb_;
};

void init_ucb(ObRpcProxy& proxy, UAsyncCB& ucb, const common::ObAddr& addr, int64_t send_ts, int64_t payload_sz);

template<typename UCB, typename Input>
    void set_ucb_args(UCB& ucb, const Input& args)
{
  ucb.set_args(args);
}

template<typename NoneType>
    void set_ucb_args(UAsyncCB& ucb, const NoneType& none)
{
  UNUSED(ucb);
  UNUSED(none);
}

class ObPocClientStub
{
public:
  ObPocClientStub(ObINio& nio): nio_(nio) {}
  ~ObPocClientStub() {}
  template<typename Input, typename Output>
      int send(ObRpcProxy& proxy, const common::ObAddr& addr, ObRpcPacketCode pcode, const Input& args, Output& out, const ObRpcOpts& opts) {
    int ret = common::OB_SUCCESS;
    ObRpcMemPool pool;
    ObSyncRespCallback cb(pool);
    char* req = NULL;
    int64_t req_sz = 0;
    char* resp = NULL;
    int64_t resp_sz = 0;
    int resp_ret = common::OB_SUCCESS;
    if (OB_FAIL(rpc_encode_req(proxy, pool, pcode, args, opts, req, req_sz))) {
      RPC_LOG(WARN, "rpc encode req fail", K(ret));
    } else if (OB_FAIL(nio_.post(addr, req, req_sz, &cb))) {
      RPC_LOG(WARN, "nio post fail", K(ret));
    } else if (NULL == (resp = cb.get_resp(resp_sz))) {
      ret = common::OB_ERR_UNEXPECTED;
      RPC_LOG(WARN, "get NULL response buffer", K(ret));
    } else if (OB_FAIL(rpc_decode_resp(resp, resp_sz, out))) {
      RPC_LOG(WARN, "rpc decode response fail", K(ret));
    } else {
      if (common::OB_SUCCESS != resp_ret) {
        ret = resp_ret;
        RPC_LOG(WARN, "rpc execute fail", K(ret));
      }
    }
    return ret;
  }
  template<typename Input, typename UCB>
      int post(ObRpcProxy& proxy, const common::ObAddr& addr, ObRpcPacketCode pcode, const Input& args, UCB& ucb, const ObRpcOpts& opts) {
    int ret = common::OB_SUCCESS;
    const int64_t start_ts = common::ObTimeUtility::current_time();
    ObRpcMemPool* pool = NULL;
    if (NULL == (pool = ObRpcMemPool::create(sizeof(ObAsyncRespCallback) + sizeof(UCB)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ObAsyncRespCallback* cb = NULL;
      char* req = NULL;
      int64_t req_sz = 0;
      if (OB_FAIL(rpc_encode_req(proxy, *pool, pcode, args, opts, req, req_sz))) {
        RPC_LOG(WARN, "rpc encode req fail", K(ret));
      } else if (NULL == (cb = ObAsyncRespCallback::create(*pool, ucb))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        init_ucb(proxy, cb->get_ucb(), addr, start_ts, req_sz);
        set_ucb_args(ucb, args);
        if (OB_FAIL(nio_.post(addr, req, req_sz, cb))) {
          RPC_LOG(WARN, "nio post fail", K(ret));
        }
      }
    }
    if (common::OB_SUCCESS != ret && NULL != pool) {
      pool->destroy();
    }
    return ret;
  }

private:
  ObINio& nio_;
};

extern ObPocClientStub global_poc_client;
#define POC_RPC_INTERCEPT(func, args...) if (transport_impl_ == rpc::ObRequest::TRANSPORT_PROTO_POC) return global_poc_client.func(*this, args);
}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_POC_RPC_PROXY_H_ */
