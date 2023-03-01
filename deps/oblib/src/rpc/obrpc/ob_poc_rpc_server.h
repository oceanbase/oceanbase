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
  ObPocServerHandleContext( ObRpcMemPool& pool, uint64_t resp_id):
      pool_(pool), resp_id_(resp_id)
  {}
  ~ObPocServerHandleContext() {
    destroy();
  }
  static int create(int64_t resp_id, const char* buf, int64_t sz, rpc::ObRequest*& req);
  void destroy() { pool_.destroy(); }
  void resp(ObRpcPacket* pkt);
  void* alloc(int64_t sz) { return pool_.alloc(sz); }
private:
  ObRpcMemPool& pool_;
  uint64_t resp_id_;
};


class ObPocRpcServer
{

public:
  ObPocRpcServer() : has_start_(false){}
  ~ObPocRpcServer() {}
  int start(int port, int net_thread_count, rpc::frame::ObReqDeliver* deliver);
  void stop() {}
  bool has_start() {return has_start_;}
  int update_tcp_keepalive_params(int64_t user_timeout);
  bool client_use_pkt_nio();
private:
  bool has_start_;
};

extern ObPocRpcServer global_poc_server;
extern ObListener* global_ob_listener;

}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_POC_RPC_SERVER_H_ */

