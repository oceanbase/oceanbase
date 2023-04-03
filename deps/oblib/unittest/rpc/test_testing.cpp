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

#include <gtest/gtest.h>
#include "rpc/testing.h"

using namespace oceanbase::common;
using namespace oceanbase::obrpc;

class TestProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(TestProxy);

  RPC_S(PR5 test, OB_TEST_PCODE, (int));
};

class Processor : public TestProxy::Processor<OB_TEST_PCODE>
{
public:
  int process()
  {
    return arg_;
  }
};

TEST(TestTesting, DISABLED_Listen)
{
  rpctesting::Service service;
  ASSERT_EQ(OB_SUCCESS, service.init());
  ASSERT_EQ(33244, service.get_listen_port());
  rpctesting::Service service2;
  ASSERT_EQ(OB_SUCCESS, service2.init());
  ASSERT_EQ(33245, service2.get_listen_port());
}

TEST(TestTesting, Basic)
{
  rpctesting::Service service;
  Processor p;

  ASSERT_EQ(OB_SUCCESS, service.init());
  ASSERT_EQ(OB_SUCCESS, service.reg_processor(&p));
  TestProxy proxy;
  ASSERT_EQ(OB_SUCCESS, service.get_proxy(proxy));
  ASSERT_EQ(2234, proxy.to(service.get_dst()).test(2234));
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
