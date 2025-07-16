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
#ifndef OCEANBASE_TEST_NET_UTIL_H_
#define OCEANBASE_TEST_NET_UTIL_H_
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <unistd.h>
#include "observer/ob_srv_deliver.h"
#include "rpc/frame/ob_req_qhandler.h"
#include "rpc/obrpc/ob_poc_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_server_struct.h"
#include "observer/ob_srv_xlator.h"
#include "observer/ob_rpc_processor_simple.h"
#include "observer/ob_server_struct.h"
#include "lib/oblog/ob_log_module.h"

extern "C" void ussl_stop();
extern "C" void ussl_wait();

using namespace oceanbase::rpc::frame;
namespace oceanbase
{
namespace obrpc
{
bool enable_pkt_nio(bool start_as_client) {
  return true;
}
int find_port(int start_port = 20000)
{
    int sock_fd = -1;
    int port = -1;
    if ((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        // failed to create socket
    } else {
        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        for (port = start_port; port <= 65535; ++port) {
          serv_addr.sin_port = htons(port);
          serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
          if (bind(sock_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) == 0) {
              close(sock_fd);
              break;
          }
        }
        close(sock_fd);
    }
    return port;
}
class ObTestRpcDeliver
  : public rpc::frame::ObReqQDeliver
{
public:
  ObTestRpcDeliver(ObiReqQHandler &qhandler) : ObReqQDeliver(qhandler),
    qth_("worker") {};
  int deliver(rpc::ObRequest &req) override final
  {
    int ret = OB_SUCCESS;
    ret = qth_.queue_.push(&req, observer::ObSrvDeliver::MAX_QUEUE_LEN);
    return ret;
  }
  int init()
  {
    int ret = OB_SUCCESS;
    int tg_id = lib::TGDefIDs::COMMON_THREAD_POOL;
    if (OB_FAIL(qth_.init())) {
      LOG_WARN("init qthread failed");
    } else {
      qth_.queue_.set_qhandler(&qhandler_);
    }
    if (OB_SUCC(ret)) {
      qth_.tg_id_ = tg_id;
      ret = TG_SET_RUNNABLE_AND_START(tg_id, qth_.thread_);
      LOG_INFO("create qthread", K(tg_id), K(ret));
    }
    return ret;
  }
  void stop() override final
  {};
  observer::QueueThread qth_;
};

class ObTestRpcServerCtx {
public:
  ObTestRpcServerCtx():
    translator_(share::ObGlobalContext::get_instance()),
    request_qhandler_(translator_),
    deliver_(request_qhandler_) {}
  observer::ObSrvXlator translator_;
  rpc::frame::ObReqQHandler request_qhandler_;
  ObTestRpcDeliver deliver_;
  int init()
  {
    int ret = deliver_.init();
    return ret;
  }
};

struct TestRpcRequest {
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(payload));
  common::ObString payload_;
};
OB_SERIALIZE_MEMBER(TestRpcRequest, payload_);
class ObTestRpcProxy : public ObRpcProxy {
public:
  DEFINE_TO(ObTestRpcProxy);
  RPC_AP(PR5 async_rpc_test, OB_TEST2_PCODE, (TestRpcRequest), obrpc::Int64);
};
class ObTestRpcCb : public obrpc::ObTestRpcProxy::AsyncCB<OB_TEST2_PCODE> {
public:
  ObTestRpcCb() : success_count_(0), timeout_count_(0) {}
  int process() override
  {
    LOG_INFO("ObTestRpcCb get response", K_(result), K_(rcode));
    if (OB_SUCCESS == rcode_.rcode_) {
      ATOMIC_FAA(&success_count_, 1);
    }
    int ret = OB_SUCCESS;
    return ret;
  }
  void on_timeout() override
  {
    LOG_INFO("ObTestRpcCb timeout", K_(result), K(get_error()));
    ATOMIC_FAA(&timeout_, 1);
  }
  void set_args(const Request &arg) override
  {
    UNUSED(arg);
  }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const override
  {
  UNUSED(alloc);
  return const_cast<rpc::frame::ObReqTransport::AsyncCB *>(
      static_cast<const rpc::frame::ObReqTransport::AsyncCB * const>(this));
  }
  int64_t success_count_;
  int64_t timeout_count_;
};

} // end namespace obrpc

} // end namespace oceanbase
#endif
