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
#include "rpc/ob_request.h"
#include "rpc/frame/ob_net_easy.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_session_handler.h"
#include "common/data_buffer.h"
#include "lib/random/ob_random.h"

using namespace oceanbase;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace std;


#define IO_CNT 1
#define SEND_CNT 1
#define ERROR_MSG "abbcced"
#define MAX_NUM 1000

#if 0
class TestProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(TestProxy);

  RPC_SS(@PR5 test, OB_TEST_PCODE, (int64_t), int64_t);
};

class MyProcessor
    : public TestProxy::Processor<OB_TEST_PCODE>
{
protected:
  int process()
  {
    int ret = OB_SUCCESS;
    const int64_t cnt = arg_;
    result_ = 0;
    while (++result_ < cnt) {
      if (OB_FAIL(flush())) {
        LOG_INFO("flush fail", K(ret));
        break;
      }
    }
    return OB_SUCCESS;
  }
  int after_process()
  {
    ::usleep(100);
    return OB_SUCCESS;
  }
};

class QHandler
    : public ObiReqQHandler
{
public:
  QHandler()
  {
    mp_.init();
    mp_.set_session_handler(shandler_);
  }

  virtual int onThreadCreated(obsys::CThread *) {
    return OB_SUCCESS;
  }
  virtual int onThreadDestroy(obsys::CThread *) {
    return OB_SUCCESS;
  }

  bool handlePacketQueue(ObRequest *req, void *)
  {
    const ObRpcPacket &pkt = reinterpret_cast<const ObRpcPacket&>(req->get_packet());
    if (!pkt.is_stream()) {
      LOG_INFO("not stream");
      mp_.set_ob_request(*req);
      mp_.run();
    } else {
      if (!shandler_.wakeup_next_thread(*req)) {
        easy_request_wakeup(req->get_request());
      }
    }
    return true;
  }

private:
  MyProcessor mp_;
  ObRpcSessionHandler shandler_;
  ObRpcReqContext ctx_;
};

class ObTestDeliver
    : public rpc::frame::ObReqDeliver
{
public:
  int init() {
    queue_.set_qhandler(&handler_);
    queue_.get_thread().set_thread_count(2);
    queue_.get_thread().start();
    return 0;
  }

  int deliver(rpc::ObRequest &req)
  {
    queue_.push(&req, 10);
    return 0;
  }

  void stop()
  {
    queue_.get_thread().stop();
    queue_.get_thread().wait();
  }

protected:
  ObReqQueueThread queue_;
  QHandler handler_;
};

class TestRpcServer
    : public ::testing::Test
{
public:
  TestRpcServer()
      :  port_(3100), handler_(server_), transport_(NULL)
  {
  }

  virtual void SetUp()
  {
    server_.init();

    ObNetOptions opts;
    opts.rpc_io_cnt_ = IO_CNT;
    net_.init(opts);
    port_ = static_cast<int32_t>(rand.get(3000, 5000));
    while (OB_SUCCESS != net_.add_rpc_listen(port_, handler_, transport_)) {
      port_ = static_cast<int32_t>(rand.get(3000, 5000));
    }
    net_.start();
  }

  virtual void TearDown()
  {
    net_.stop();
    net_.wait();
    server_.stop();
  }

  int send(const char *buf, int len)
  {
    ObReqTransport::Request<ObRpcPacket> req;
    ObReqTransport::Result<ObRpcPacket> res;
    ObAddr dst(ObAddr::IPV4, "127.0.0.1", port_);
    int64_t payload = len;
    transport_->create_request(req, dst, payload, 3000000);
    memcpy(req.buf(), buf, len);
    return transport_->send(req, res);
  }

protected:
  int port_;
  rpc::frame::ObNetEasy net_;
  obrpc::ObRpcHandler handler_;
  ObTestDeliver server_;
  rpc::frame::ObReqTransport *transport_;
  ObRandom rand;
};

class MySSHandle
    : public TestProxy::SSHandle<OB_TEST_PCODE>
{
public:
  void inc_sessid()
  {
    sessid_++;
  }
  void dec_sessid()
  {
    sessid_--;
  }
  void set_timeout(int64_t timeout)
  {
    proxy_.set_timeout(timeout);
  }
};

TEST_F(TestRpcServer, StreamRPCChaos)
{
  ObAddr dst(ObAddr::IPV4, "127.0.0.1", port_);
  TestProxy proxy;
  proxy.init(transport_, dst);

  MySSHandle handle;
  int64_t num = 0;
  proxy.test(MAX_NUM, num, handle);
  handle.set_timeout(1000*1000);
  while (handle.has_more()) {
    int64_t oldnum = num;
    ASSERT_EQ(OB_SUCCESS, handle.get_more(num));
    EXPECT_EQ(num, oldnum + 1);
    if (num >= MAX_NUM / 2 && num <= MAX_NUM / 2 + 1) {
      handle.inc_sessid();
      EXPECT_EQ(OB_TIMEOUT, handle.get_more(num));
      handle.dec_sessid();
    }
  }
}

TEST_F(TestRpcServer, StreamRPCAbort)
{
  ObAddr dst(ObAddr::IPV4, "127.0.0.1", port_);
  TestProxy proxy;
  proxy.init(transport_, dst);

  MySSHandle handle;
  int64_t num = 0;
  proxy.test(MAX_NUM, num, handle);
  while (handle.has_more()) {
    int64_t oldnum = num;
    ASSERT_EQ(OB_SUCCESS, handle.get_more(num));
    EXPECT_EQ(num, oldnum + 1);
    if (num >= MAX_NUM / 2) {
      int ret = handle.abort();
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  EXPECT_EQ(MAX_NUM / 2, num);
}

TEST_F(TestRpcServer, StreamRPC)
{
  ObAddr dst(ObAddr::IPV4, "127.0.0.1", port_);
  TestProxy proxy;
  proxy.init(transport_, dst);

  TestProxy::SSHandle<OB_TEST_PCODE> handle;
  int64_t num = 0;
  proxy.test(MAX_NUM, num, handle);
  while (handle.has_more()) {
    int64_t oldnum = num;
    LOG_INFO("get more", K(num));
    EXPECT_EQ(OB_SUCCESS, handle.get_more(num));
    EXPECT_EQ(num, oldnum + 1);
  }
  if (handle.has_more()) {
    handle.abort();
  }
  EXPECT_EQ(MAX_NUM, num);
}
#endif

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
