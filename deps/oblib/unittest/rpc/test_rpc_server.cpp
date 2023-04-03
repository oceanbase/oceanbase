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

#define USING_LOG_PREFIX RPC_TEST

#include <gtest/gtest.h>
#include "rpc/obrpc/ob_rpc_request.h"
#include "rpc/frame/ob_net_easy.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "rpc/ob_rpc_define.h"
#include "common/data_buffer.h"
#include "lib/random/ob_random.h"

using namespace oceanbase;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace std;

#define IO_CNT 1
#define SEND_CNT 1
#define ERROR_MSG "Common error"

static constexpr int64_t cluster_id = 1000;
static bool change_src_cluster_id = false;

class TestProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(TestProxy);

  RPC_S(@PR5 test, OB_TEST_PCODE);
  //RPC_S(@PR5 test2, OB_TEST2_PCODE, (int64_t), int64_t);
  RPC_S(@PR5 test2, OB_TEST2_PCODE, (uint64_t));
  RPC_S(@PR5 test_cluster_id, OB_TEST3_PCODE);
};

class MyProcessor
    : public TestProxy::Processor<OB_TEST_PCODE>
{
protected:
  int process()
  {
    static int64_t rpc_count_ = 0;
    EXPECT_FALSE(req_ == NULL);
    EXPECT_EQ(ObRequest::OB_RPC, req_->get_type());
    EXPECT_GT(ObTimeUtility::current_time(), get_send_timestamp());
    EXPECT_LT(ObTimeUtility::current_time() - get_send_timestamp(), 10000);
    if (0 == rpc_count_) {
      EXPECT_LT(get_receive_timestamp() - get_send_timestamp(), 3000);
    } else {
      EXPECT_LT(get_receive_timestamp() - get_send_timestamp(), 2000);
    }
    EXPECT_LT(get_run_timestamp() - get_receive_timestamp(), 10000);

    //LOG_USER_ERROR(OB_ERROR);
    //LOG_USER_WARN(OB_ERROR);
    ++rpc_count_;
    return OB_ERROR;
  }
};

class MyProcessor2
    : public TestProxy::Processor<OB_TEST2_PCODE>
{
protected:
  int process()
  {
    const ObRpcPacket &pkt = dynamic_cast<const ObRpcPacket&>(req_->get_packet());
    EXPECT_EQ(arg_, pkt.get_priv_tenant_id());
    return OB_SUCCESS;
  }
};

class MyProcessor3
    : public TestProxy::Processor<OB_TEST3_PCODE>
{
protected:
  int process()
  {
    const ObRpcPacket &pkt = dynamic_cast<const ObRpcPacket&>(req_->get_packet());
    EXPECT_EQ(cluster_id, pkt.get_dst_cluster_id());
    EXPECT_EQ(cluster_id, pkt.get_src_cluster_id());
    // It is not easy to simulate the exchange of rpc between two clusters on one side
    //Here, src_cluster_id is forcibly modified in the process to achieve a similar effect
    if (change_src_cluster_id) {
      const_cast<ObRpcPacket&>(pkt).set_src_cluster_id(cluster_id + 1);
    }
    return OB_SUCCESS;
  }
};

class ObTestDeliver
    : public rpc::frame::ObReqDeliver
{
public:
  int init() { return 0; }

  int deliver(rpc::ObRequest &req)
  {
    LOG_INFO("request", K(req));
    /*
      ObDataBuffer buf(new char[2048], 2048);
      ObRpcResultCode rcode;
      rcode.rcode_ = OB_SUCCESS;

      // LOG_INFO("server process", K(req));
      obrpc::ObRpcPacketCode pkc =
      reinterpret_cast<const obrpc::ObRpcPacket&>(req.get_packet()).get_pcode();
      if (OB_TEST2_PCODE == pkc) {
      ObRpcRequest &rpc_req = static_cast<ObRpcRequest &>(req);
      EXPECT_TRUE(0 == rpc_req.response_success<int64_t>(&buf, 987654321, rcode, 1));
      } else {
      MyProcessor mp;
      mp.init();
      mp.set_ob_request(req);
      mp.set_buffer(&buf);
      mp.run();
    */
    const ObRpcPacket &pkt = dynamic_cast<const ObRpcPacket&>(req.get_packet());

    switch (pkt.get_pcode()) {
      case OB_TEST_PCODE: {
        MyProcessor mp;
        mp.init();
        mp.set_ob_request(req);
        mp.run();
      } break;
      // case OB_TEST2_PCODE: {
      //   MyProcessor2 mp;
      //   mp.init();
      //   mp.set_ob_request(req);
      //   mp.run();
      // } break;
      case OB_TEST3_PCODE: {
        MyProcessor3 mp;
        mp.init();
        mp.set_ob_request(req);
        mp.run();
      } break;
      default:
        break;
    }

    EXPECT_TRUE(is_io_thread());
    // req.get_request()->opacket = const_cast<rpc::ObPacket*>(&req.get_packet());
    // req.set_request_rtcode(EASY_OK);
    // easy_request_wakeup(req.get_request());
    return 0;
  }

  void stop() {}
};

class TestRpcServer
    : public ::testing::Test
{
public:
  TestRpcServer()
      : handler_(server_), transport_(NULL)
  {}

  void SetUp() override
  {
    ObNetOptions opts;
    opts.rpc_io_cnt_ = IO_CNT;
    net_.init(opts);
    PORT = static_cast<int32_t>(rand.get(3000, 5000));
    while (OB_SUCCESS != net_.add_rpc_listen(PORT, handler_, transport_)) {
      PORT = static_cast<int32_t>(rand.get(3000, 5000));
    }
    EXPECT_EQ(OB_SUCCESS, net_.start());
    ObRpcNetHandler::CLUSTER_ID = cluster_id;
  }

  void TearDown() override
  {
    net_.stop();
  }

protected:
  rpc::frame::ObNetEasy net_;
  obrpc::ObRpcHandler handler_;
  ObTestDeliver server_;
  rpc::frame::ObReqTransport *transport_;
  ObRandom rand;
  int32_t PORT;
};

TEST_F(TestRpcServer, TestName)
{
  EXPECT_FALSE(is_io_thread());

  ObAddr dst(ObAddr::IPV4, "127.0.0.1", PORT);
  TestProxy proxy;
  proxy.init(transport_, dst);
  int cnt = 300;
  while (cnt--) {
    EXPECT_EQ(OB_ERROR, proxy.timeout(12340).test());
  }

  ObAddr dst2(ObAddr::IPV4, "127.0.0.1", PORT+1);
  EXPECT_EQ(OB_TIMEOUT, proxy.to(dst2).test());

  // EXPECT_EQ(OB_SUCCESS, proxy.to(dst).by(2).as(10).test2(10));
  // EXPECT_EQ(OB_SUCCESS, proxy.to(dst).by(2).test2(OB_INVALID_TENANT_ID));
}

TEST_F(TestRpcServer, TestClusterId)
{
  EXPECT_FALSE(is_io_thread());

  ObAddr dst(ObAddr::IPV4, "127.0.0.1", PORT);
  TestProxy proxy;
  proxy.init(transport_, dst);
  EXPECT_EQ(OB_TIMEOUT, proxy.dst_cluster_id(cluster_id + 1).test_cluster_id());
  EXPECT_EQ(OB_SUCCESS, proxy.dst_cluster_id(cluster_id).test_cluster_id());
  EXPECT_EQ(OB_SUCCESS, proxy.test_cluster_id());
  // Verify that src_cluster_id will be used as the cluster_id of the return package
  change_src_cluster_id = true;
  EXPECT_EQ(OB_TIMEOUT, proxy.test_cluster_id());
}

/*
  TEST_F(TestRpcServer, test_rpc_request)
  {
  ObAddr dst(ObAddr::IPV4, "127.0.0.1", PORT);
  TestProxy proxy;
  proxy.init(transport_, dst);
  int64_t res = 0;
  int64_t cnt =300;
  while (cnt--) {
  proxy.timeout(12340).test2(cnt, res);
  EXPECT_TRUE(res == 987654321);
  }
  }
*/


int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  ob_get_tsi_warning_buffer()->set_warn_log_on(true);
  return RUN_ALL_TESTS();
}
