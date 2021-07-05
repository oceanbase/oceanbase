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

#include <unistd.h>
#include <gtest/gtest.h>
#include "common/ob_clock_generator.h"
#include "election/ob_election_async_log.h"
#include "lib/net/ob_addr.h"
#include "lib/net/tbnetutil.h"
#include "election/ob_election_base.h"

namespace oceanbase {
namespace unittest {
using namespace common;
using namespace election;
class TestObElectionBase : public ::testing::Test {
public:
  TestObElectionBase()
  {}
  ~TestObElectionBase()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
};

TEST_F(TestObElectionBase, get_self_addr)
{
  common::ObAddr addr;
  const int32_t port = 1000;

  EXPECT_EQ(OB_INVALID_ARGUMENT, get_self_addr(addr, NULL, 0));
  {
    const char* dev = "null";
    EXPECT_EQ(OB_INVALID_ARGUMENT, get_self_addr(addr, dev, port));
  }
  {
    const char* dev = "bond0";
    EXPECT_EQ(OB_SUCCESS, get_self_addr(addr, dev, port));
    uint32_t ip = obsys::CNetUtil::getLocalAddr(dev);
    EXPECT_EQ(ip, addr.get_ipv4());
    EXPECT_EQ(port, addr.get_port());
  }
}

TEST_F(TestObElectionBase, election_member_name)
{
  ASSERT_STREQ("VOTER", ObElectionMemberName(MEMBER_TYPE_VOTER));
  ASSERT_STREQ("CANDIDATE", ObElectionMemberName(MEMBER_TYPE_CANDIDATE));
  ASSERT_STREQ("UNKNOWN", ObElectionMemberName(MEMBER_TYPE_UNKNOWN));
}

TEST_F(TestObElectionBase, election_role_name)
{
  ASSERT_STREQ("SLAVE", ObElectionRoleName(ROLE_SLAVE));
  ASSERT_STREQ("LEADER", ObElectionRoleName(ROLE_LEADER));
  ASSERT_STREQ("UNKNOWN", ObElectionRoleName(ROLE_UNKNOWN));
}

TEST_F(TestObElectionBase, election_stage_name)
{
  ASSERT_STREQ("PREPARE", ObElectionStageName(STAGE_PREPARE));
  ASSERT_STREQ("VOTING", ObElectionStageName(STAGE_VOTING));
  ASSERT_STREQ("COUNTING", ObElectionStageName(STAGE_COUNTING));
  ASSERT_STREQ("UNKNOWN", ObElectionStageName(STAGE_UNKNOWN));
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  int ret = -1;

  oceanbase::election::ASYNC_LOG_INIT("test_election_base.log", OB_LOG_LEVEL_INFO, true);
  if (OB_FAIL(oceanbase::common::ObClockGenerator::init())) {
    ELECT_LOG(WARN, "clock generator init error.", K(ret));
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  oceanbase::election::ASYNC_LOG_DESTROY();
  (void)oceanbase::common::ObClockGenerator::destroy();

  return ret;
}
