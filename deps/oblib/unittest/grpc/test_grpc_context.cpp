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

#include "lib/oblog/ob_log_module.h"
#include "grpc/newlogstorepb.grpc.pb.h"
#include "lib/utility/ob_test_util.h"
#include "grpc/ob_grpc_context.h"
#include <gtest/gtest.h>
#include "rpc/frame/ob_req_transport.h"
#include "lib/coro/testing.h"
#include <linux/futex.h>

namespace oceanbase
{
using namespace common;
using namespace newlogstorepb;

namespace unittest
{

int concurrency = 0;
int packet_size = 0;
int64_t use_multi_channel = 0;
int64_t use_async = 1;

class TestGrpc : public ::testing::Test
{
public:
  TestGrpc() {}
  virtual ~TestGrpc() {}
public:
  virtual void SetUp();
  virtual void TearDown();
};

const int64_t RPC_TIMEOUT = 2000 * 1000 * 1000L;

void TestGrpc::SetUp()
{

}

void TestGrpc::TearDown()
{
}

TEST_F(TestGrpc, test_init)
{
  // logstorepb对应logstorepb.proto的文件名
  // LogStore对应logstorepb.proto里面的service名是LogStore
  //oceanbase::obgrpc::ObGrpcClient<newlogstorepb::LogStore> grpc_client_;

  //oceanbase::common::ObAddr addr;
  //addr.set_ip_addr("127.0.0.1", 0);
  //ASSERT_EQ(OB_INVALID_ARGUMENT, grpc_client_.init(addr, RPC_TIMEOUT, 1, 500));
  //addr.set_ip_addr("127.0.0.1", 60000);
  //int ret = grpc_client_.init(addr, RPC_TIMEOUT, 1, 500);
  //ASSERT_EQ(OB_SUCCESS, ret);
  //newlogstorepb::HelloReq req;
  //newlogstorepb::HelloResp resp;
  //// Hello对应service里面定义的rpc接口
  //if (OB_FAIL(GRPC_CALL(grpc_client_, Hello, req, &resp))) {
  //    COMMON_LOG(INFO, "grpc call failed");
  //} else {
  //  COMMON_LOG(INFO, "grpc call success");
  //}
  //ASSERT_EQ(OB_RPC_SEND_ERROR, ret);
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  unlink("test_grpc_context.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_grpc_context.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
