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

enum {
  INVALID_RPC_PKT_ID = -1
};
struct ObRpcReverseKeepaliveArg;
class ObPocServerHandleContext
{
public:
  enum {
    OBCG_ELECTION = 2
  }; // same as src/share/resource_manager/ob_group_list.h
  ObPocServerHandleContext(ObRpcMemPool& pool, uint64_t resp_id, int64_t resp_expired_abs_us):
      pool_(pool), resp_id_(resp_id), resp_expired_abs_us_(resp_expired_abs_us), peer_()
  {}
  ~ObPocServerHandleContext() {
    destroy();
  }
  static int create(int64_t resp_id, const char* buf, int64_t sz, rpc::ObRequest*& req);
  void destroy() { pool_.destroy(); }
  void resp(ObRpcPacket* pkt);
  static int resp_error(uint64_t resp_id, int err_code, const char* b, const int64_t sz);
  ObAddr get_peer();
  void set_peer_unsafe(); // This function can only be called from the pnio thread.
  void* alloc(int64_t sz) { return pool_.alloc(sz); }
  void set_resp_expired_time(int64_t ts) { resp_expired_abs_us_ = ts; }
  int64_t get_resp_expired_time() { return resp_expired_abs_us_; }
private:
  ObRpcMemPool& pool_;
  uint64_t resp_id_;
  int64_t resp_expired_abs_us_;
  ObAddr peer_;
};


class ObPocRpcServer
{

public:
  enum {
    DEFAULT_PNIO_GROUP = 1,
    RATELIMIT_PNIO_GROUP = 2,
    END_GROUP
  };
  enum { RPC_TIMEGUARD_STRING_SIZE = 64};
  ObPocRpcServer() : has_start_(false), start_as_client_(false){}
  ~ObPocRpcServer() {}
  int start(int port, int net_thread_count, rpc::frame::ObReqDeliver* deliver);
  int start_net_client(int net_thread_count);
  void stop();
  void wait();
  void destroy();
  bool has_start() {return has_start_;}
  int update_tcp_keepalive_params(int64_t user_timeout);
  int update_server_standby_fetch_log_bandwidth_limit(int64_t value);
  bool client_use_pkt_nio();
  int64_t get_ratelimit();
  uint64_t get_ratelimit_rxbytes();
private:
  bool has_start_;
  bool start_as_client_;
};

extern ObPocRpcServer global_poc_server;
extern ObListener* global_ob_listener;
void stream_rpc_register(const int64_t pkt_id, int64_t send_time_us);
void stream_rpc_unregister(const int64_t pkt_id);
int stream_rpc_reverse_probe(const ObRpcReverseKeepaliveArg& reverse_keepalive_arg);
int64_t get_max_rpc_packet_size();
extern "C" {
  int dispatch_to_ob_listener(int accept_fd);
  int tranlate_to_ob_error(int err);
}
}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_POC_RPC_SERVER_H_ */

