/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "rpc/obrpc/ob_rpc_packet.h"
#include <gtest/gtest.h>

using namespace oceanbase::obrpc;

TEST(TestRpcStruct, Basic)
{
  ObRpcPacket pkt;
  int64_t sz = pkt.get_encoded_size();
  EXPECT_EQ(0, sz);
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
