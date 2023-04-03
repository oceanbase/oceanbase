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

using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;

class TestObrpcPacket
    : public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};

TEST_F(TestObrpcPacket, NameIndex)
{
  ObRpcPacketSet &set = ObRpcPacketSet::instance();
  EXPECT_EQ(OB_RENEW_LEASE, set.pcode_of_idx(set.idx_of_pcode(OB_RENEW_LEASE)));
  EXPECT_EQ(OB_BOOTSTRAP, set.pcode_of_idx(set.idx_of_pcode(OB_BOOTSTRAP)));
  EXPECT_STREQ("OB_BOOTSTRAP", set.name_of_idx(set.idx_of_pcode(OB_BOOTSTRAP)));
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
