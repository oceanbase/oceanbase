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
#include "rpc/obrpc/ob_rpc_packet.h"
#include "observer/ob_rpc_translator.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;

class TestObRpcTranslator
    : public ::testing::Test
{
public:
  TestObRpcTranslator()
      : t_(gctx_)
  {}

  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
protected:
  ObGlobalContext gctx_;
  ObRpcTranslator t_;
};

TEST_F(TestObRpcTranslator, TestName)
{
  ObRequestProcessor *p = NULL;
  ObPacket pkt;

  EXPECT_EQ(OB_SUCCESS, t_.init_thread_local());

  EXPECT_EQ(OB_ERR_NULL_VALUE, t_.translate_packet(NULL, p));
  EXPECT_TRUE(NULL == p);

  pkt.set_pcode(OB_SET_CONFIG);
  EXPECT_EQ(OB_INNER_STAT_ERROR, t_.translate_packet(&pkt, p));
  EXPECT_TRUE(NULL == p);

  pkt.set_target_id(OB_SELF_FLAG);
  EXPECT_EQ(OB_SUCCESS, t_.translate_packet(&pkt, p));
  EXPECT_FALSE(NULL == p);

  pkt.set_pcode(OB_GET_REQUEST);
  EXPECT_EQ(OB_NOT_SUPPORTED, t_.translate_packet(&pkt, p));
  EXPECT_TRUE(NULL == p);
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
