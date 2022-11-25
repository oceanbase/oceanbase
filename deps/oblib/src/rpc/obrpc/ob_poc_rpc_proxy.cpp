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

#include "rpc/obrpc/ob_poc_rpc_proxy.h"
#include "rpc/obrpc/ob_poc_rpc_server.h"
#include "rpc/obrpc/ob_rpc_proxy.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace obrpc
{

int ObSyncRespCallback::handle_resp(int io_err, char* buf, int64_t sz)
{
  int ret = OB_SUCCESS;
  if (0 == io_err) {
    resp_ = buf;
    sz_ = sz;
  }
  return ret;
}

class ObPocSPAlloc: public rpc::frame::SPAlloc
{
public:
  ObPocSPAlloc(ObRpcMemPool& pool): pool_(pool) {}
  virtual ~ObPocSPAlloc() {}
  void* alloc(int64_t sz) const {
    return pool_.alloc(sz);
  }
private:
  ObRpcMemPool& pool_;
};

ObAsyncRespCallback* ObAsyncRespCallback::create(ObRpcMemPool& pool, UAsyncCB& ucb)
{
  int ret = OB_SUCCESS;
  ObPocSPAlloc sp_alloc(pool);
  UAsyncCB* cb = NULL;
  ObAsyncRespCallback* pcb = NULL;
  if (NULL == (pcb = (ObAsyncRespCallback*)pool.alloc(sizeof(ObAsyncRespCallback)))) {
    RPC_LOG(WARN, "alloc resp callback fail", K(ret));
  } else if (NULL == (cb = ucb.clone(sp_alloc))) {
    RPC_LOG(WARN, "ucb.clone fail", K(ret));
  } else {
    new(pcb)ObAsyncRespCallback(pool, *cb);
  }
  return pcb;
}

int ObAsyncRespCallback::handle_resp(int io_err, char* buf, int64_t sz)
{
  int ret = OB_SUCCESS;
  ObRpcPacket* ret_pkt = NULL;
  if (0 != io_err) {
    ucb_.set_error(io_err);
    ucb_.on_error(io_err);
  } else if (NULL == buf) {
    ucb_.on_timeout();
  } else if (OB_FAIL(rpc_decode_ob_packet(pool_, buf, sz, ret_pkt))) {
    ucb_.on_invalid();
    RPC_LOG(WARN, "rpc_decode_ob_packet fail", K(ret));
  } else if (OB_FAIL(ucb_.decode(ret_pkt))) {
    ucb_.on_invalid();
    RPC_LOG(WARN, "ucb.decode fail", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ucb_.process())) {
      RPC_LOG(WARN, "ucb.process fail", K(tmp_ret));
    }
  }
  return ret;
}

void init_ucb(ObRpcProxy& proxy, UAsyncCB& ucb, const common::ObAddr& dest, int64_t send_ts, int64_t payload_sz)
{
  ucb.set_dst(dest);
  ucb.set_tenant_id(proxy.get_tenant());
  ucb.set_timeout(proxy.timeout());
  ucb.set_send_ts(send_ts);
  ucb.set_payload(payload_sz);
}
ObPocClientStub global_poc_client(global_poc_server.get_nio());
}; // end namespace obrpc
}; // end namespace oceanbase
