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

#define USING_LOG_PREFIX SERVER
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "observer/net/ob_rpc_reverse_keepalive.h"
#include "rpc/obrpc/ob_rpc_reverse_keepalive_struct.h"
#include "rpc/frame/ob_req_deliver.h"
#include "lib/net/ob_net_util.h"

#define private public

using namespace oceanbase::rpc::frame;
namespace oceanbase
{
namespace obrpc
{
class TestRpcReverseKeepAliveService : public testing::Test
{
public:
  TestRpcReverseKeepAliveService()
  {}
  virtual ~TestRpcReverseKeepAliveService()
  {}
  static int find_port(int start_port = 20000)
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
};
class ObTestRpcQHandler
    : public rpc::frame::ObiReqQHandler
{
public:
  ObTestRpcQHandler() {}
  int onThreadCreated(obsys::CThread *) override final { return OB_SUCCESS; };
  int onThreadDestroy(obsys::CThread *) override final { return OB_SUCCESS; };
  bool handlePacketQueue(rpc::ObRequest *req, void *args) override final
  {
    ObIAllocator &alloc = THIS_WORKER.get_sql_arena_allocator();
    const observer::ObGlobalContext gctx;
    ObReqProcessor *processor = NULL;
    ObRpcPacketCode pcode = reinterpret_cast<const obrpc::ObRpcPacket &>(req->get_packet()).get_pcode();
    if (OB_RPC_REVERSE_KEEPALIVE == pcode) {
      processor = OB_NEWx(observer::ObRpcReverseKeepaliveP, &alloc, gctx);
    }
    if (OB_NOT_NULL(processor)) {
      processor->init();
      processor->set_ob_request(*req);
      processor->run();
      processor->~ObReqProcessor();
      THIS_WORKER.get_sql_arena_allocator().free(processor);
      processor = NULL;
    }
    return true;
  }

};
class ObTestRpcDeliver
  : public rpc::frame::ObReqQDeliver
{
public:
  ObTestRpcDeliver() : ObReqQDeliver(qhandler_) {};
  int deliver(rpc::ObRequest &req) override final
  {
    int ret = OB_SUCCESS;
    LOG_INFO("deliver rpc request", K(req));
    qhandler_.handlePacketQueue(&req, NULL);
    return ret;
  }
  int init() {return OB_SUCCESS;}
  void stop() override final
  {};
  ObTestRpcQHandler qhandler_;
};
bool enable_pkt_nio(bool start_as_client) {
  return true;
}

TEST_F(TestRpcReverseKeepAliveService, reverse_keepalive_service)
{
  // init
  int ret = common::OB_SUCCESS;
  obrpc::ObSrvRpcProxy srv_rpc_proxy;
  ObReqTransport dummy_transport(NULL, NULL);
  srv_rpc_proxy.init(&dummy_transport);
  ObTestRpcDeliver deliver;
  int port = find_port();
  ObAddr dst;
  uint32_t ip_value = 0;
  if (OB_FAIL(obsys::ObNetUtil::get_local_addr_ipv4("eth0", ip_value))
    && OB_FAIL(obsys::ObNetUtil::get_local_addr_ipv4("bond0", ip_value))) {
    dst.set_ip_addr("127.0.0.1", port);
  } else {
    ip_value = ntohl(ip_value);
    dst.set_ipv4_addr(ip_value, port);
    LOG_INFO("get local ip", K(dst));
  }

  // test
  ret = rpc_reverse_keepalive_instance.init(&srv_rpc_proxy);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = global_poc_server.start(port, 1, &deliver);
  ASSERT_EQ(ret, OB_SUCCESS);

  uint32_t pkt_id = 1;
  stream_rpc_register(pkt_id, ObTimeUtility::current_time());
  ObRpcReverseKeepaliveArg a(dst, ObTimeUtility::current_time(), pkt_id);
  ret = stream_rpc_reverse_probe(a);
  ASSERT_EQ(ret, OB_SUCCESS);
  stream_rpc_unregister(pkt_id);
  ret = stream_rpc_reverse_probe(a);
  ASSERT_EQ(ret, OB_ENTRY_NOT_EXIST);

  pkt_id = 2;
  int64_t send_time_us = ObTimeUtility::current_time();
  stream_rpc_register(pkt_id, send_time_us + 100000);
  ObRpcReverseKeepaliveArg a2(dst, send_time_us, pkt_id);
  ret = stream_rpc_reverse_probe(a2);
  ASSERT_EQ(ret, OB_HASH_NOT_EXIST);

  rpc_reverse_keepalive_instance.destroy();
}

} // end namespace obrpc
} // end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_reverse_keepalive.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
