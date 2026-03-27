/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "rpc/obrpc/ob_net_client.h"
#include <gtest/gtest.h>

using namespace oceanbase::obrpc;
using namespace oceanbase::common;

class TestNetClient
    : public ::testing::Test
{
public:
  virtual void SetUp()
  {
    client_.init();
  }

  virtual void TearDown()
  {
    client_.destroy();
  }

protected:
  ObNetClient client_;
};

TEST_F(TestNetClient, TestName)
{

}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
