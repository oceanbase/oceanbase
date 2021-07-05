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
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "election/ob_election_msg.h"
#include "election/ob_election_priority.h"
#include "election/ob_election_cb.h"
#include "election/ob_election_time_def.h"
#include "common/ob_clock_generator.h"
#include "election/ob_election_rpc.h"
#include "election/ob_election_async_log.h"

namespace oceanbase {
namespace unittest {
using namespace election;
using namespace common;
class TestObElectionMsg : public ::testing::Test {
public:
  TestObElectionMsg()
  {}
  ~TestObElectionMsg()
  {}
  virtual void SetUp()
  {
    init();
  }
  virtual void TearDown()
  {}

public:
  void init();
  int64_t get_current_ts(ObPartitionKey& pkey) const;

public:
  static const int64_t OB_ELECTION_HASH_TABLE_NUM = 100;
  ObAddr self_;
  int64_t t1_;
  int64_t send_ts_;
  ObAddr current_leader_;
};

void TestObElectionMsg::init()
{
  EXPECT_EQ(true, self_.set_ip_addr("127.0.0.1", 8021));
  send_ts_ = t1_ + 10;
}

int64_t TestObElectionMsg::get_current_ts(ObPartitionKey& pkey) const
{
  return (ObClockGenerator::getClock() - (pkey.hash() % OB_ELECTION_HASH_TABLE_NUM) * OB_ELECTION_HASH_TABLE_NUM);
}

TEST_F(TestObElectionMsg, DePrepare)
{
  t1_ = ((ObClockGenerator::getClock() + T_ELECT2) / T_ELECT2) * T_ELECT2;

  ObElectionPriority priority;
  priority.init(true, 100, 100, 0);
  ObElectionMsgDEPrepare de_prepare(priority, t1_, send_ts_, self_, T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
  EXPECT_EQ(true, de_prepare.is_valid());

  ObElectionMsgDEPrepare de_prepare1(priority, t1_ + 100, send_ts_, self_, T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
  EXPECT_EQ(true, !de_prepare1.is_valid());

  ObElectionPriority priority1;
  priority1.init(true, -1, 100, 0);
  ObElectionMsgDEPrepare de_prepare2(priority1, t1_, send_ts_, self_, T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
  EXPECT_EQ(true, !de_prepare2.is_valid());

  ObPartitionKey pkey(1, 1, 1);
  int64_t cur_ts = get_current_ts(pkey);
  int64_t delay = T1_TIMESTAMP(t1_) - cur_ts;
  usleep((useconds_t)delay);
}

TEST_F(TestObElectionMsg, DeVote)
{
  t1_ = ((ObClockGenerator::getClock() + T_ELECT2) / T_ELECT2) * T_ELECT2;

  ObElectionMsgDEVote de_vote(self_, t1_, send_ts_, self_);
  EXPECT_EQ(true, de_vote.is_valid());
  ObPartitionKey pkey(1, 1, 1);

  int64_t cur_ts = get_current_ts(pkey);
  int64_t delay = T2_TIMESTAMP(t1_) - cur_ts;
  usleep((useconds_t)delay);
}

TEST_F(TestObElectionMsg, DeSucc)
{
  t1_ = ((ObClockGenerator::getClock() + T_ELECT2) / T_ELECT2) * T_ELECT2;
  ObElectionMsgDESuccess de_succ(self_, t1_, send_ts_, self_, T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
  EXPECT_EQ(true, de_succ.is_valid());
  ObPartitionKey pkey(1, 1, 1);

  int64_t cur_ts = get_current_ts(pkey);
  int64_t delay = T3_TIMESTAMP(t1_) - cur_ts;
  usleep((useconds_t)delay);
}

TEST_F(TestObElectionMsg, Prepare)
{
  t1_ = ((ObClockGenerator::getClock() + T_ELECT2) / T_ELECT2) * T_ELECT2;
  ObElectionMsgPrepare prepare(self_, self_, t1_, send_ts_, self_, T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
  EXPECT_EQ(true, prepare.is_valid());

  ObPartitionKey pkey(1, 1, 1);
  int64_t cur_ts = get_current_ts(pkey);
  int64_t delay = T1_TIMESTAMP(t1_) - cur_ts;
  usleep((useconds_t)delay);
}

TEST_F(TestObElectionMsg, Vote)
{
  t1_ = ((ObClockGenerator::getClock() + T_ELECT2) / T_ELECT2) * T_ELECT2;
  ObElectionPriority priority;
  priority.init(true, 100, 100, 0);
  ObElectionMsgVote vote(priority, self_, self_, t1_, send_ts_, self_);
  EXPECT_EQ(true, vote.is_valid());

  ObElectionPriority priority1;
  priority1.init(true, -1, 100, 0);
  ObElectionMsgVote vote1(priority1, self_, self_, t1_, send_ts_, self_);
  EXPECT_EQ(true, !vote1.is_valid());

  ObPartitionKey pkey(1, 1, 1);
  int64_t cur_ts = get_current_ts(pkey);
  int64_t delay = T2_TIMESTAMP(t1_) - cur_ts;
  usleep((useconds_t)delay);
}

TEST_F(TestObElectionMsg, Succ)
{
  t1_ = ((ObClockGenerator::getClock() + T_ELECT2) / T_ELECT2) * T_ELECT2;
  ObElectionMsgSuccess succ(self_, self_, t1_, send_ts_, self_, T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2, 100, 100);
  EXPECT_EQ(true, succ.is_valid());
  ObPartitionKey pkey(1, 1, 1);

  int64_t cur_ts = get_current_ts(pkey);
  int64_t delay = T3_TIMESTAMP(t1_) - cur_ts;
  usleep((useconds_t)delay);
}

TEST_F(TestObElectionMsg, QueryLedaer)
{
  t1_ = ((ObClockGenerator::getClock() + T_ELECT2) / T_ELECT2) * T_ELECT2;
  ObElectionQueryLeader query(send_ts_, self_);
  EXPECT_EQ(true, query.is_valid());
}

TEST_F(TestObElectionMsg, QueryLedaerResp)
{
  t1_ = ((ObClockGenerator::getClock() + T_ELECT2) / T_ELECT2) * T_ELECT2;
  int64_t epoch_time = 1;
  ObElectionQueryLeaderResponse response(
      send_ts_, self_, self_, epoch_time, t1_, T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
  EXPECT_EQ(true, response.is_valid());
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  int ret = -1;

  oceanbase::election::ASYNC_LOG_INIT("test_election_msg.log", OB_LOG_LEVEL_INFO, true);

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
