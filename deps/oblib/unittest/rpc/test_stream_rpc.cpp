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
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_session_handler.h"
#include "rpc/frame/ob_req_qhandler.h"
#include "rpc/obrpc/ob_rpc_req_context.h"
#include "rpc/frame/ob_net_easy.h"
#include "rpc/ob_request.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "lib/net/ob_net_util.h"
#include "test_obrpc_util.h"

using namespace oceanbase;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace std;


#define IO_CNT 1
#define SEND_CNT 1
#define ERROR_MSG "abbcced"
#define AFTER_RPCESS_TIME_US 100
#define MAX_NUM 1000
namespace oceanbase {
namespace obrpc {
bool __attribute__((weak)) stream_rpc_update_timeout()
{
  return true;
}
bool enable_pkt_nio(bool start_as_client) {
  return true;
}
int pnio_server_ref;
int global_rpc_port;
extern void __attribute__((weak)) response_rpc_error_packet(ObRequest* req, int ret);
} // end of namespace obrpc
} // end of namespace oceanbase

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
    const int32_t cnt = arg_ & 0xffffffff;
    int32_t process_time_us = arg_ >> 32;
    LOG_INFO("process", K(cnt), K(process_time_us), K(THIS_WORKER.get_timeout_remain()));
    result_ = 0;
    while (++result_ < cnt && OB_SUCCESS == ret) {
      if(process_time_us > 0) {
        ob_usleep(process_time_us); // Simulate that the server is processing
      }
      if (THIS_WORKER.is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("check timeout fail", K(ret));
      } else if (OB_FAIL(flush(THIS_WORKER.get_timeout_remain()))) {
        LOG_INFO("flush fail", K(ret));
        break;
      } else {
        int64_t remain = THIS_WORKER.get_timeout_remain();
        LOG_INFO("process after flush", K(ret), K(remain), K(result_));
      }
    }
    return ret;
  }
  int after_process()
  {
    ::usleep(AFTER_RPCESS_TIME_US);
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
      THIS_WORKER.set_timeout_ts(req->get_receive_timestamp() + pkt.get_timeout());
      mp_.run();
    } else {
      if (!shandler_.wakeup_next_thread(*req)) {
        RPC_REQ_OP.response_result(req, NULL);
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "wakeup failed");
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
    if (global_rpc_port == 0) {
      global_rpc_port = static_cast<int32_t>(rand.get(3000, 5000));
    }
    port_ = global_rpc_port;
    if (ATOMIC_FAA(&pnio_server_ref, 1) == 0) {
      global_poc_server.start(port_, 4, &server_);
    }
    uint32_t ip_value = 0;
    if (0 == (ip_value = obsys::ObNetUtil::get_local_addr_ipv4("eth0"))
      && 0 == (ip_value = obsys::ObNetUtil::get_local_addr_ipv4("bond0"))) {
      local_addr_.set_ip_addr("127.0.0.1", port_);
    } else {
      ip_value = ntohl(ip_value);
      local_addr_.set_ipv4_addr(ip_value, port_);
      LOG_INFO("get local ip", K(local_addr_));
    }
    ObReqTransport dummy_transport(NULL, NULL);
    transport_ = &dummy_transport;
  }

  virtual void TearDown()
  {
    server_.stop();
    if (ATOMIC_AAF(&pnio_server_ref, -1) == 0) {
      global_poc_server.destroy();
    }
  }

protected:
  int port_;
  rpc::frame::ObNetEasy net_;
  obrpc::ObRpcHandler handler_;
  ObTestDeliver server_;
  rpc::frame::ObReqTransport *transport_;
  ObRandom rand;
  ObAddr local_addr_;
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
  TestProxy proxy;
  proxy.init(transport_, local_addr_);

  MySSHandle handle;
  int64_t num = 0;
  proxy.timeout(1000*1000).test(MAX_NUM, num, handle);
  while (handle.has_more()) {
    int64_t oldnum = num;
    int rt = 0;
    LOG_INFO("StreamRPCChaos", K(num));
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
  TestProxy proxy;
  proxy.init(transport_, local_addr_);

  MySSHandle handle;
  int64_t num = 0;
  proxy.test(MAX_NUM, num, handle);
  while (handle.has_more()) {
    int64_t oldnum = num;
    ASSERT_EQ(OB_SUCCESS, handle.get_more(num));
    EXPECT_EQ(num, oldnum + 1);
    if (num >= MAX_NUM / 2) {
      int ret = handle.abort();
      ASSERT_EQ(OB_ITER_END, ret);
    }
  }
  EXPECT_EQ(MAX_NUM / 2, num);
}

TEST_F(TestRpcServer, StreamRPC)
{
  TestProxy proxy;
  proxy.init(transport_, local_addr_);

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

TEST_F(TestRpcServer, StreamRPCTimeout)
{
  // test send stream rpc without reset_timeout
  TestProxy proxy;
  proxy.init(transport_, local_addr_);
  TestProxy::SSHandle<OB_TEST_PCODE> handle;
  int64_t num = 0;
  int64_t process_time = 200 * 1000; // 200ms
  int wait_time = 3000 * 1000; // 3s
  int32_t expect_succ_num = wait_time / (process_time + AFTER_RPCESS_TIME_US);
  int32_t actual_succ_num = 0;
  int64_t arg = (process_time << 32) | MAX_NUM;
  EXPECT_EQ(OB_SUCCESS, proxy.timeout(wait_time).test(arg, num, handle));
  actual_succ_num ++;
  int ret = OB_SUCCESS;
  while (handle.has_more() && OB_SUCCESS == ret) {
    int64_t oldnum = num;
    ret = handle.get_more(num);
    LOG_INFO("StreamRPCTimeout", K(num), K(ret));
    if (OB_SUCCESS == ret) {
      actual_succ_num ++;
    }
  }
  EXPECT_EQ(OB_TIMEOUT, ret);
  EXPECT_EQ(actual_succ_num, expect_succ_num);
  if (handle.has_more()) {
    handle.abort();
  }
}

TEST_F(TestRpcServer, StreamRPCResetTimeout)
{
  // test send stream rpc with reset_timeout
  TestProxy proxy;
  proxy.init(transport_, local_addr_);

  TestProxy::SSHandle<OB_TEST_PCODE> handle;
  int64_t num = 0;
  int64_t process_time = 1000 * 1000; // 1s
  int send_num = 4;
  int64_t arg = (process_time << 32) | send_num;
  EXPECT_EQ(OB_SUCCESS, proxy.timeout(2000000).test(arg, num, handle));
  int ret = OB_SUCCESS;
  while (handle.has_more() && OB_SUCCESS == ret) {
    int64_t oldnum = num;
    LOG_INFO("StreamRPCResetTimeout", K(num));
    handle.reset_timeout();
    ret = handle.get_more(num);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(num, oldnum + 1);
  }
  EXPECT_EQ(send_num, num);
  if (handle.has_more()) {
    handle.abort();
  }
}

int main(int argc, char *argv[])
{
  OB_LOGGER.set_log_level("TRACE");
  OB_LOGGER.set_file_name("test_stream_rpc.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
