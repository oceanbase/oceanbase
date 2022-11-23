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
#include "rpc/obrpc/ob_rpc_stat.h"

using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;

class TestObrpcStat
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

namespace oceanbase
{
namespace rpc
{
RpcStatService *get_stat_srv_by_tenant_id(uint64_t)
{
  static RpcStatService stat;
  return &stat;
}
}
}

TEST_F(TestObrpcStat, Basic)
{
  RpcStatPiece piece;
  piece.size_ = 100;
  piece.time_ = ObTimeUtility::current_time();
  RPC_STAT(OB_BOOTSTRAP, 1, piece);

  const int64_t idx = ObRpcPacketSet::instance().idx_of_pcode(OB_BOOTSTRAP);
  RpcStatItem item;
  ASSERT_EQ(OB_SUCCESS, RPC_STAT_GET(idx, 1, item));
  EXPECT_EQ(piece.size_, item.size_);
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
