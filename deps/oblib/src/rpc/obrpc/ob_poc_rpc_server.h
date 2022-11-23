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

#ifndef OCEANBASE_OBRPC_OB_POC_RPC_SERVER_H_
#define OCEANBASE_OBRPC_OB_POC_RPC_SERVER_H_
#include "rpc/obrpc/ob_poc_nio.h"
#include "rpc/obrpc/ob_rpc_mem_pool.h"
#include "rpc/ob_request.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/obrpc/ob_listener.h"

namespace oceanbase
{
namespace obrpc
{

class ObPocServerHandleContext
{
public:
  ObPocServerHandleContext(ObINio& nio, ObRpcMemPool& pool, uint64_t resp_id):
      nio_(nio), pool_(pool), resp_id_(resp_id)
  {}
  ~ObPocServerHandleContext() {
    destroy();
  }
  static rpc::ObRequest* create(ObINio& nio, int64_t resp_id, char* buf, int64_t sz);
  void destroy() { pool_.destroy(); }
  void resp(ObRpcPacket* pkt);
  void* alloc(int64_t sz) { return pool_.alloc(sz); }
private:
  ObINio& nio_;
  ObRpcMemPool& pool_;
  int64_t resp_id_;
};

class ObPocServerReqHandler: public IReqHandler
{
public:
  ObPocServerReqHandler(): deliver_(NULL), nio_(NULL) {}
  ~ObPocServerReqHandler() {}
  void init(rpc::frame::ObReqDeliver* deliver, ObINio* nio) {
    deliver_ = deliver;
    nio_ = nio;
  }
  int handle_req(int64_t resp_id, char* buf, int64_t sz) {
    rpc::ObRequest* req = ObPocServerHandleContext::create(*nio_, resp_id, buf, sz);
    return deliver_->deliver(*req);
  }
private:
  rpc::frame::ObReqDeliver* deliver_;
  ObINio* nio_;
};

class ObPocRpcServer
{

public:
  ObPocRpcServer() {}
  ~ObPocRpcServer() {}
  int start(int port, rpc::frame::ObReqDeliver* deliver);
  void stop() {}
  ObPocNio& get_nio() { return nio_; }
private:
  ObPocNio nio_;
  ObListener listener_;
  ObPocServerReqHandler server_req_handler_;
};

extern ObPocRpcServer global_poc_server;

}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_POC_RPC_SERVER_H_ */

