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
#include "rpc/ob_rpc_define.h"
#include "lib/coro/testing.h"
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

class TestProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(TestProxy);

  RPC_S(@PR5 test, OB_TEST_PCODE);
  //RPC_S(@PR5 test2, OB_TEST2_PCODE, (int64_t), int64_t);
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
    // EXPECT_GT(ObTimeUtility::current_time(), get_send_timestamp());
    // EXPECT_LT(ObTimeUtility::current_time() - get_send_timestamp(), 10000);
    // if (0 == rpc_count_) {
    //   EXPECT_LT(get_receive_timestamp() - get_send_timestamp(), 3000);
    // } else {
    //   EXPECT_LT(get_receive_timestamp() - get_send_timestamp(), 2000);
    // }
    // EXPECT_LT(get_run_timestamp() - get_receive_timestamp(), 10000);

    // LOG_USER_ERROR(OB_ERROR);
    // LOG_USER_WARN(OB_ERROR);
    ++rpc_count_;
    return OB_ERROR;
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
    const ObRpcPacket &pkt = dynamic_cast<const ObRpcPacket&>(req.get_packet());

    switch (pkt.get_pcode()) {
      case OB_TEST_PCODE: {
        MyProcessor mp;
        mp.init();
        mp.set_ob_request(req);
        mp.run();
      } break;
      default:
        break;
    }
    EXPECT_TRUE(is_io_thread());
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
  int cnt = 1000;
  while (cnt--) {
    EXPECT_EQ(OB_ERROR, proxy.timeout(12340).test());
  }

  ObAddr dst2(ObAddr::IPV4, "127.0.0.1", PORT+1);
  EXPECT_EQ(OB_TIMEOUT, proxy.to(dst2).test());
}

TEST_F(TestRpcServer, CR)
{
  ObAddr dst(ObAddr::IPV4, "127.0.0.1", PORT);
  TestProxy proxy;
  proxy.init(transport_, dst);
  cotesting::FlexPool([proxy, dst] {
    auto cnt = 1000;
    while (cnt--) {
      EXPECT_EQ(OB_ERROR, proxy.to(dst).timeout(20000).test());
    }
  }, 4, 10).start();
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  ob_get_tsi_warning_buffer()->set_warn_log_on(true);
  return RUN_ALL_TESTS();
}
