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
#include "lib/net/ob_addr.h"
#include "common/ob_clock_generator.h"
#include "election/ob_election_async_log.h"
#include "election/ob_election_mem_stat.h"

namespace oceanbase {
namespace unittest {
using namespace common;
using namespace election;
class TestObElectionMemStat : public ::testing::Test {
public:
  TestObElectionMemStat()
  {}
  virtual ~TestObElectionMemStat()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

public:
  ObElectionMemStat mem_stat_;
};

TEST_F(TestObElectionMemStat, init_and_destory)
{
  common::ObAddr addr;
  const int64_t alloc_count = 10;
  const int64_t release_count = 10;

  EXPECT_EQ(true, addr.set_ip_addr("127.0.0.1", 12345));
  EXPECT_EQ(OB_SUCCESS, mem_stat_.init(addr, "election_mem_stat", alloc_count, release_count));
  mem_stat_.destroy();
  EXPECT_EQ(OB_SUCCESS, mem_stat_.init(addr, "election_mem_stat_abcdefg", alloc_count, release_count));
  mem_stat_.reset();
  EXPECT_EQ(OB_SUCCESS, mem_stat_.init(addr, "abc", alloc_count, release_count));

  ObElectionMemStat new_mem_stat;
  new_mem_stat = mem_stat_;

  ASSERT_STREQ("abc", new_mem_stat.get_type_name());
  EXPECT_EQ(addr, new_mem_stat.get_addr());
  EXPECT_EQ(alloc_count, new_mem_stat.get_alloc_count());
  EXPECT_EQ(release_count, new_mem_stat.get_release_count());
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  int ret = -1;

  oceanbase::election::ASYNC_LOG_INIT("test_election_mem_stat.log", OB_LOG_LEVEL_INFO, true);
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
