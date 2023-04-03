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
