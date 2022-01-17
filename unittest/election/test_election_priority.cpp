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
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "election/ob_election_msg.h"
#include "election/ob_election_priority.h"
#include "election/ob_election_msg_pool.h"
#include "election/ob_election_time_def.h"
#include "common/ob_clock_generator.h"
#include "lib/container/ob_array_serialization.h"
#include "mock_election.h"

namespace oceanbase {
namespace unittest {
using namespace election;
using namespace common;

class TestObElectionPriority : public ::testing::Test {
public:
  TestObElectionPriority()
  {}
  ~TestObElectionPriority()
  {}

  virtual void SetUp()
  {
    init();
  }
  virtual void TearDown()
  {}

private:
  int init();

public:
  int create_deprepare_msg(common::ObSArray<ObElectionPriority>& priority_array, ObElectionVoteMsgPool& msg_pool);
  ObElectionVoteMsgPool msg_pool_;
  MockElection election_;
  int64_t t1_;
  int64_t cur_ts_;
  int64_t lease_time_;
};

int TestObElectionPriority::init()
{
  t1_ = ((ObClockGenerator::getClock() + T_ELECT2) / T_ELECT2) * T_ELECT2;
  cur_ts_ = t1_ + 10;
  ;
  lease_time_ = T_LEADER_LEASE_EXTENDS * T_ELECT2;

  return OB_SUCCESS;
}

int TestObElectionPriority::create_deprepare_msg(
    ObSArray<ObElectionPriority>& priority_array, ObElectionVoteMsgPool& msg_pool)
{
  msg_pool.reset();
  msg_pool_.init(ObAddr(ObAddr::IPV4, "127.0.0.1", 123456), &election_);

  for (int32_t i = 0; i < priority_array.count(); i++) {
    ObAddr self;
    EXPECT_EQ(true, self.set_ip_addr("127.0.0.1", 34506 + i));
    ObElectionMsgDEPrepare msg(priority_array.at(i), t1_, cur_ts_, self, T_LEADER_LEASE_EXTENDS * T_ELECT2);
    msg_pool.store(msg);
  }

  return OB_SUCCESS;
}

TEST_F(TestObElectionPriority, normal_decentrialize_msg)
{
  ObElectionPriority priority;
  ObAddr addr;

  ObSArray<ObElectionPriority> priority_array;
  priority_array.reset();

  {
    ObElectionPriority p0;
    p0.init(true, 1000, 100, 0);
    ObElectionPriority p1;
    p1.init(true, 1001, 101, 0);
    ObElectionPriority p2;
    p2.init(true, 1002, 102, 0);
    ObElectionPriority p3;
    p3.init(true, 1003, 103, 0);
    ObElectionPriority p4;
    p4.init(true, 1004, 104, 0);
    ObElectionPriority p5;
    p5.init(true, 1005, 105, 0);
    ObElectionPriority p6;
    p6.init(true, 1006, 106, 0);

    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p0));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p1));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p2));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p3));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p4));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p5));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p6));
    create_deprepare_msg(priority_array, msg_pool_);

    EXPECT_EQ(
        OB_SUCCESS, msg_pool_.get_decentralized_candidate(addr, priority, priority_array.count(), t1_, lease_time_));
    EXPECT_EQ(0, priority.compare_with_accurate_logid(p6));
  }
}

TEST_F(TestObElectionPriority, candidate_is_false)
{
  ObElectionPriority priority;
  ObAddr addr;

  ObSArray<ObElectionPriority> priority_array;
  priority_array.reset();

  {
    ObElectionPriority p0;
    p0.init(true, 1000, 100, 0);
    ObElectionPriority p1;
    p1.init(true, 1001, 101, 0);
    ObElectionPriority p2;
    p2.init(false, 1002, 102, 0);
    ObElectionPriority p3;
    p3.init(true, 1003, 103, 0);
    ObElectionPriority p4;
    p4.init(false, 1004, 104, 0);
    ObElectionPriority p5;
    p5.init(true, 1005, 105, 0);
    ObElectionPriority p6;
    p6.init(false, 1006, 106, 0);
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p0));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p1));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p2));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p3));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p4));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p5));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p6));
    create_deprepare_msg(priority_array, msg_pool_);

    EXPECT_EQ(
        OB_SUCCESS, msg_pool_.get_decentralized_candidate(addr, priority, priority_array.count(), t1_, lease_time_));
    EXPECT_EQ(0, priority.compare_with_accurate_logid(p5));
  }
}

TEST_F(TestObElectionPriority, log_id_compare)
{
  ObElectionPriority priority;
  ObAddr addr;

  ObSArray<ObElectionPriority> priority_array;
  priority_array.reset();

  {
    ObElectionPriority p0;
    p0.init(true, 1000, 100, 0);
    ObElectionPriority p1;
    p1.init(true, 1001, 101, 0);
    ObElectionPriority p2;
    p2.init(false, 1002, 102, 0);
    ObElectionPriority p3;
    p3.init(true, 1003, 103, 0);
    ObElectionPriority p4;
    p4.init(true, 1005, 104, 0);
    ObElectionPriority p5;
    p5.init(true, 1005, 105, 0);
    ObElectionPriority p6;
    p6.init(false, 1006, 106, 0);
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p0));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p1));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p2));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p3));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p4));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p5));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p6));
    create_deprepare_msg(priority_array, msg_pool_);

    EXPECT_EQ(
        OB_SUCCESS, msg_pool_.get_decentralized_candidate(addr, priority, priority_array.count(), t1_, lease_time_));
    EXPECT_EQ(0, priority.compare_with_accurate_logid(p5));
  }
}

TEST_F(TestObElectionPriority, data_version_compare)
{
  ObElectionPriority priority;
  ObAddr addr;

  ObSArray<ObElectionPriority> priority_array;
  priority_array.reset();

  {
    ObElectionPriority p0;
    p0.init(true, 1000, 100, 0);
    ObElectionPriority p1;
    p1.init(true, 1001, 101, 0);
    ObElectionPriority p2;
    p2.init(true, 1002, 102, 0);
    ObElectionPriority p3;
    p3.init(true, 1005, 103, 0);
    ObElectionPriority p4;
    p4.init(true, 1005, 104, 0);
    ObElectionPriority p5;
    p5.init(true, 1005, 105, 0);
    ObElectionPriority p6;
    p6.init(true, 1005, 106, 0);
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p0));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p1));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p2));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p3));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p4));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p5));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p6));
    create_deprepare_msg(priority_array, msg_pool_);

    EXPECT_EQ(
        OB_SUCCESS, msg_pool_.get_decentralized_candidate(addr, priority, priority_array.count(), t1_, lease_time_));
    EXPECT_EQ(0, priority.compare_with_accurate_logid(p6));
  }
}

TEST_F(TestObElectionPriority, no_majority)
{
  ObElectionPriority priority;
  ObAddr addr;

  ObSArray<ObElectionPriority> priority_array;
  priority_array.reset();

  {
    ObElectionPriority p0;
    p0.init(true, 1000, 100, 0);
    ObElectionPriority p1;
    p1.init(true, 1001, 101, 0);
    ObElectionPriority p2;
    p2.init(false, 1002, 102, 0);
    ObElectionPriority p3;
    p3.init(false, 1003, 103, 0);
    ObElectionPriority p4;
    p4.init(false, 1005, 104, 0);
    ObElectionPriority p5;
    p5.init(false, 1005, 105, 0);
    ObElectionPriority p6;
    p6.init(false, 1006, 106, 0);
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p0));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p1));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p2));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p3));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p4));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p5));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p6));
    create_deprepare_msg(priority_array, msg_pool_);

    EXPECT_EQ(
        OB_SUCCESS, msg_pool_.get_decentralized_candidate(addr, priority, priority_array.count(), t1_, lease_time_));
  }
}

TEST_F(TestObElectionPriority, no_all_message)
{
  ObElectionPriority priority;
  ObAddr addr;

  ObSArray<ObElectionPriority> priority_array;
  priority_array.reset();

  {
    ObElectionPriority p0;
    p0.init(true, 1000, 100, 0);
    ObElectionPriority p1;
    p1.init(true, 1001, 101, 0);
    ObElectionPriority p2;
    p2.init(false, 1002, 102, 0);
    ObElectionPriority p3;
    p3.init(false, 1003, 103, 0);
    ObElectionPriority p4;
    p4.init(false, 1005, 104, 0);
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p0));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p1));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p2));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p3));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p4));
    create_deprepare_msg(priority_array, msg_pool_);

    EXPECT_EQ(
        OB_SUCCESS, msg_pool_.get_decentralized_candidate(addr, priority, priority_array.count(), t1_, lease_time_));
  }
}

TEST_F(TestObElectionPriority, no_all_message2)
{
  ObElectionPriority priority;
  ObAddr addr;

  ObSArray<ObElectionPriority> priority_array;
  priority_array.reset();

  {
    ObElectionPriority p0;
    p0.init(true, 1000, 100, 0);
    ObElectionPriority p1;
    p1.init(true, 1001, 101, 0);
    ObElectionPriority p2;
    p2.init(true, 1002, 102, 0);
    ObElectionPriority p3;
    p3.init(false, 1003, 103, 0);
    ObElectionPriority p4;
    p4.init(false, 1005, 104, 0);
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p0));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p1));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p2));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p3));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p4));
    create_deprepare_msg(priority_array, msg_pool_);
    EXPECT_EQ(
        OB_SUCCESS, msg_pool_.get_decentralized_candidate(addr, priority, priority_array.count(), t1_, lease_time_));
  }
}

TEST_F(TestObElectionPriority, no_enough_message)
{
  ObElectionPriority priority;
  ObAddr addr;

  ObSArray<ObElectionPriority> priority_array;
  priority_array.reset();

  {
    ObElectionPriority p0;
    p0.init(true, 1000, 100, 0);
    ObElectionPriority p1;
    p1.init(true, 1001, 101, 0);
    ObElectionPriority p2;
    p2.init(true, 1002, 102, 0);
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p0));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p1));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p2));
    create_deprepare_msg(priority_array, msg_pool_);
    EXPECT_EQ(
        OB_SUCCESS, msg_pool_.get_decentralized_candidate(addr, priority, priority_array.count(), t1_, lease_time_));
  }
}

TEST_F(TestObElectionPriority, no_all_message_but_false)
{
  ObElectionPriority priority;
  ObAddr addr;

  ObSArray<ObElectionPriority> priority_array;
  priority_array.reset();

  {
    ObElectionPriority p0;
    p0.init(true, 1000, 100, 0);
    ObElectionPriority p1;
    p1.init(true, 1001, 101, 0);
    ObElectionPriority p2;
    p2.init(true, 1002, 102, 0);
    ObElectionPriority p3;
    p3.init(false, 1003, 103, 0);
    ObElectionPriority p4;
    p4.init(false, 1005, 104, 0);
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p0));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p1));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p2));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p3));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p4));
    create_deprepare_msg(priority_array, msg_pool_);
    EXPECT_EQ(
        OB_SUCCESS, msg_pool_.get_decentralized_candidate(addr, priority, priority_array.count(), t1_, lease_time_));
  }
}

TEST_F(TestObElectionPriority, no_all_message_candidate)
{
  ObElectionPriority priority;
  ObAddr addr;

  ObSArray<ObElectionPriority> priority_array;
  priority_array.reset();

  {
    ObElectionPriority p0;
    p0.init(true, 1000, 100, 0);
    ObElectionPriority p1;
    p1.init(true, 1001, 101, 0);
    ObElectionPriority p2;
    p2.init(false, 1002, 102, 0);
    ObElectionPriority p3;
    p3.init(true, 1003, 103, 0);
    ObElectionPriority p4;
    p4.init(false, 1005, 104, 0);
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p0));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p1));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p2));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p3));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p4));
    create_deprepare_msg(priority_array, msg_pool_);
    EXPECT_EQ(
        OB_SUCCESS, msg_pool_.get_decentralized_candidate(addr, priority, priority_array.count(), t1_, lease_time_));
    EXPECT_EQ(0, priority.compare_with_accurate_logid(p3));
  }
}

TEST_F(TestObElectionPriority, no_all_message_candidate_log_id)
{
  ObElectionPriority priority;
  ObAddr addr;

  ObSArray<ObElectionPriority> priority_array;
  priority_array.reset();

  {
    ObElectionPriority p0;
    p0.init(true, 1000, 100, 0);
    ObElectionPriority p1;
    p1.init(true, 1001, 101, 0);
    ObElectionPriority p2;
    p2.init(false, 1002, 102, 0);
    ObElectionPriority p3;
    p3.init(true, 1005, 103, 0);
    ObElectionPriority p4;
    p4.init(true, 1005, 104, 0);
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p0));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p1));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p2));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p3));
    EXPECT_EQ(OB_SUCCESS, priority_array.push_back(p4));
    create_deprepare_msg(priority_array, msg_pool_);
    EXPECT_EQ(
        OB_SUCCESS, msg_pool_.get_decentralized_candidate(addr, priority, priority_array.count(), t1_, lease_time_));
    EXPECT_EQ(0, priority.compare_with_accurate_logid(p4));
  }
}

TEST_F(TestObElectionPriority, locality)
{
  ObElectionPriority p0;
  p0.init(true, 1000, 10000, 1);
  ObElectionPriority p1;
  p1 = p0;
  EXPECT_EQ(0, p1.compare_with_accurate_logid(p0));
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  int ret = -1;

  oceanbase::election::ASYNC_LOG_INIT("test_election_priority.log", OB_LOG_LEVEL_INFO, true);

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
