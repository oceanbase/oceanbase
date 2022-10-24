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

#include "storage/tx/ob_trans_rpc.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_clock_generator.h"
#include "share/ob_common_rpc_proxy.h"
#include "lib/container/ob_array_iterator.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
using namespace storage;
using namespace obrpc;
using namespace share;
namespace unittest
{

class MockObTransRpcProxy : public ObTransRpcProxy
{
};

class TestObTransRpc : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}

  // define port
  static const int32_t PORT = 8080;
  // ObAddr using ipv4 for test
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;
  // ip address for test
  static const char *LOCAL_IP;
};
const char *TestObTransRpc::LOCAL_IP = "127.0.0.1";
transaction::ObTransService trans_service;

// memory allocation and collection of TransRpcTask in local rpc
TEST_F(TestObTransRpc, trans_rpc_task_alloc_free)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  /*
  TransRpcTask *rpc_task = TransRpcTaskFactory::alloc();
  EXPECT_TRUE(NULL != rpc_task);
  TransRpcTaskFactory::release(rpc_task);
  */
}

TEST_F(TestObTransRpc, trans_rpc_init_invalid_args)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObAddr observer(TestObTransRpc::IP_TYPE, TestObTransRpc::LOCAL_IP, TestObTransRpc::PORT);
  ObAddr addr;
  ObTxCommitMsg commit_msg;
  ObTransRpc rpc;
  obrpc::ObBatchRpc *batch_rpc = new obrpc::ObBatchRpc();
  rpc::frame::ObReqTransport *transport = NULL;
  // rpc without init
  EXPECT_EQ(OB_NOT_INIT, rpc.start());
  EXPECT_EQ(OB_NOT_INIT, rpc.post_msg(addr, commit_msg));

  EXPECT_EQ(OB_INVALID_ARGUMENT, rpc.init(&trans_service, transport, observer, batch_rpc));
  transport = (rpc::frame::ObReqTransport*)0x01;
  EXPECT_EQ(OB_SUCCESS, rpc.init(&trans_service, transport, observer, batch_rpc));
  EXPECT_EQ(OB_INIT_TWICE, rpc.init(&trans_service, transport, observer, batch_rpc));

  // rpc without start
  EXPECT_EQ(OB_NOT_RUNNING, rpc.post_msg(addr, commit_msg));

  // repeat start
  EXPECT_EQ(OB_SUCCESS, rpc.start());
  EXPECT_EQ(OB_ERR_UNEXPECTED, rpc.start());

  // todo 4.0:rpc task
  // TransRpcTask *trans_task = new TransRpcTask();
  // EXPECT_TRUE(NULL != trans_task);

  EXPECT_EQ(OB_INVALID_ARGUMENT, rpc.post_msg(addr, commit_msg));
  EXPECT_EQ(OB_INVALID_ARGUMENT, rpc.post_msg(observer, commit_msg));

  rpc.stop();
  rpc.wait();
  rpc.destroy();
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_rpc.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);

  if (OB_SUCCESS != ObClockGenerator::init()) {
    TRANS_LOG(WARN, "init ob_clock_generator error!");
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
