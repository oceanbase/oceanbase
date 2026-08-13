/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "logservice/palf/log_quorum_policy.h"
#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace palf;

TEST(TestLogMajorityQuorumPolicy, majority_quorum)
{
  LogQuorumPolicy quorum_policy;
  for (int64_t replica_num = 1; replica_num <= 9; ++replica_num) {
    const int64_t expected_quorum = replica_num / 2 + 1;
    EXPECT_EQ(expected_quorum, quorum_policy.get_accept_quorum(replica_num));
    EXPECT_EQ(expected_quorum, quorum_policy.get_prepare_quorum(replica_num));
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
