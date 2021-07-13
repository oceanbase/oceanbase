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
#include "mock_election.h"
#include <vector>
#include <iostream>
using namespace std;

namespace oceanbase {
namespace unittest {
using namespace election;
using namespace common;

const vector<ObAddr> ADDR = {ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 34500),
    ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 34501),
    ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 34502),
    ObAddr(ObAddr::VER::IPV4, "127.0.0.4", 34503),
    ObAddr(ObAddr::VER::IPV4, "127.0.0.5", 34504),
    ObAddr(ObAddr::VER::IPV4, "127.0.0.6", 34505),
    ObAddr(ObAddr::VER::IPV4, "127.0.0.7", 34506),
    ObAddr(ObAddr::VER::IPV4, "127.0.0.8", 34507)};
const vector<int> REPLICA_NUM = {3, 5, 7};

class TestObElectionMsgPool : public ::testing::Test {
public:
  TestObElectionMsgPool()
  {}
  ~TestObElectionMsgPool()
  {}

  virtual void SetUp()
  {
    init();
  }
  virtual void TearDown()
  {}

  void reset()
  {
    srand(std::chrono::system_clock::now().time_since_epoch().count());
    election_.set_current_ts(t1_ + rand() % (4 * T_DIFF + T_ST) - 2 * T_DIFF);
    msg_pool_.reset();
    msg_pool_.init(ADDR[0], &election_);
  }

private:
  int init();

public:
  ObElectionVoteMsgPool msg_pool_;
  int64_t t1_;
  int64_t send_ts_;
  int64_t lease_time_;
  ObAddr current_leader_;
  MockElection election_;
};

int TestObElectionMsgPool::init()
{
  t1_ = T_ELECT2;
  send_ts_ = t1_ + 10;
  lease_time_ = T_LEADER_LEASE_EXTENDS * T_ELECT2;

  return OB_SUCCESS;
}

TEST_F(TestObElectionMsgPool, store_de_prepare)
{
  reset();

  ObElectionPriority priority;
  priority.init(true, 1000, ObVersion(1), 100);

  ObElectionMsgDEPrepare msg1(priority, t1_, send_ts_, ADDR[0], T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg1));

  // same sender message
  EXPECT_EQ(OB_ELECTION_ERROR_DUPLICATED_MSG, msg_pool_.store(msg1));

  ObElectionMsgDEPrepare msg2(priority, t1_, send_ts_, ADDR[1], T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg2));
}

TEST_F(TestObElectionMsgPool, overrange_msg)
{
  for (auto replica_num : REPLICA_NUM) {
    reset();
    ObAddr self;
    ObElectionPriority priority;
    priority.init(true, 1000, ObVersion(1), 100);

    for (int i = 0; i < replica_num; i++) {
      ObElectionMsgDEPrepare msg(priority, t1_, send_ts_, ADDR[i], T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
      EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg));
    }

    {
      ObElectionMsgDEPrepare msg1(priority, t1_, send_ts_, ADDR[0], T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
      if (replica_num <= 3) {
        EXPECT_EQ(OB_ELECTION_ERROR_DUPLICATED_MSG, msg_pool_.store(msg1));
      } else {
        EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg1));
      }
    }

    {
      ObElectionMsgDEPrepare msg1(priority, t1_, send_ts_, ADDR[replica_num], T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
      EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg1));
    }
  }
}

TEST_F(TestObElectionMsgPool, get_decentralized_candidate)
{
  for (int i = 0; i < 10000; ++i) {
    for (auto replica_num : REPLICA_NUM) {
      reset();

      vector<ObElectionMsgDEPrepare> v_msg;
      for (int i = 0; i < replica_num / 2 + 1; i++) {
        ObElectionPriority priority;
        priority.init(true, 1000 + i, ObVersion(1), 100 + i);
        v_msg.emplace_back(priority, t1_, send_ts_, ADDR[i], T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
        EXPECT_EQ(OB_SUCCESS, msg_pool_.store(v_msg.back()));
      }

      {
        ObAddr ob_addr;
        ObElectionPriority priority;

        EXPECT_EQ(OB_SUCCESS, msg_pool_.get_decentralized_candidate(ob_addr, priority, replica_num, t1_, lease_time_));
        EXPECT_EQ(v_msg.back().get_sender(), ob_addr);
        EXPECT_EQ(0, priority.compare_with_accurate_logid(v_msg.back().get_priority()));

        EXPECT_EQ(OB_SUCCESS, msg_pool_.get_decentralized_candidate(ob_addr, priority, replica_num, t1_, lease_time_));
        EXPECT_EQ(v_msg.back().get_sender(), ob_addr);
        EXPECT_EQ(0, priority.compare_with_accurate_logid(v_msg.back().get_priority()));

        election_.set_current_ts(t1_ + T_ELECT2 + rand() % (4 * T_DIFF + T_ST) - 2 * T_DIFF);
        ObElectionPriority lower_priority;
        lower_priority.init(true, 1, ObVersion(1), 1);
        ObElectionMsgDEPrepare msg(
            lower_priority, t1_ + T_ELECT2, send_ts_, ADDR[7], T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
        EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg));

        EXPECT_EQ(OB_SUCCESS,
            msg_pool_.get_decentralized_candidate(ob_addr, priority, replica_num, t1_ + T_ELECT2, lease_time_));
        EXPECT_EQ(ADDR[7], ob_addr);
        EXPECT_EQ(0, priority.compare_with_accurate_logid(lower_priority));

        EXPECT_EQ(OB_ELECTION_WARN_T1_NOT_MATCH,
            msg_pool_.get_decentralized_candidate(ob_addr, priority, replica_num, t1_, lease_time_));
        EXPECT_EQ(OB_ELECTION_WARN_T1_NOT_MATCH,
            msg_pool_.get_decentralized_candidate(ob_addr, priority, replica_num, t1_ + 2 * T_ELECT2, lease_time_));

        ObElectionPriority higher_priority;
        higher_priority.init(true, 99999, ObVersion(1), 99999);
        ObElectionMsgDEPrepare msg2(priority, t1_, send_ts_, ADDR[6], T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
        EXPECT_EQ(OB_ELECTION_WARN_MESSAGE_NOT_INTIME, msg_pool_.store(msg2));

        EXPECT_EQ(OB_SUCCESS,
            msg_pool_.get_decentralized_candidate(ob_addr, priority, replica_num, t1_ + T_ELECT2, lease_time_));
        EXPECT_EQ(ADDR[7], ob_addr);
        EXPECT_EQ(0, priority.compare_with_accurate_logid(lower_priority));
      }
    }
  }
}

TEST_F(TestObElectionMsgPool, centralized_multi_prepare_message)
{
  reset();

  ObElectionPriority priority;
  priority.init(true, 1000, ObVersion(1), 100);
  ObElectionMsgPrepare msg(ADDR[0], ADDR[0], t1_, send_ts_, ADDR[0], T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg));
  EXPECT_EQ(OB_ELECTION_ERROR_DUPLICATED_MSG, msg_pool_.store(msg));
  ObElectionMsgPrepare msg2(ADDR[1], ADDR[2], t1_, send_ts_, ADDR[3], T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
  EXPECT_EQ(OB_ELECTION_ERROR_DUPLICATED_MSG, msg_pool_.store(msg2));

  ObAddr cur_leader, new_leader;
  EXPECT_EQ(OB_SUCCESS, msg_pool_.get_centralized_candidate(cur_leader, new_leader, t1_));
  EXPECT_EQ(cur_leader, ADDR[0]);
  EXPECT_EQ(new_leader, ADDR[0]);
}

TEST_F(TestObElectionMsgPool, check_decentralized_majority)
{
  reset();

  ObElectionMsgDEVote msg1(ADDR[0], t1_, send_ts_ + 400000, ADDR[0]);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg1));

  ObElectionMsgDEVote msg2(ADDR[0], t1_, send_ts_ + 400000, ADDR[1]);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg2));

  ObAddr new_leader;
  int64_t ticket = 0;
  EXPECT_EQ(OB_SUCCESS, msg_pool_.check_decentralized_majority(new_leader, ticket, 3, t1_));
  EXPECT_EQ(2, ticket);
  EXPECT_EQ(ADDR[0], new_leader);
}

TEST_F(TestObElectionMsgPool, check_decentralized_majority_diff_vote_leader)
{
  reset();

  ObElectionMsgDEVote msg1(ADDR[0], t1_, send_ts_ + 400000, ADDR[0]);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg1));

  ObElectionMsgDEVote msg2(ADDR[0], t1_, send_ts_ + 400000, ADDR[1]);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg2));

  ObElectionMsgDEVote msg3(ADDR[1], t1_, send_ts_ + 400000, ADDR[2]);
  EXPECT_EQ(OB_ERR_UNEXPECTED, msg_pool_.store(msg3));

  ObAddr new_leader;
  int64_t ticket = 0;
  EXPECT_EQ(OB_SUCCESS, msg_pool_.check_decentralized_majority(new_leader, ticket, 3, t1_));
  EXPECT_EQ(new_leader, ADDR[0]);
  EXPECT_EQ(ticket, 2);

  EXPECT_EQ(OB_ELECTION_WARN_NOT_REACH_MAJORITY, msg_pool_.check_decentralized_majority(new_leader, ticket, 5, t1_));
}

TEST_F(TestObElectionMsgPool, get_centralized_majority_one_ticket)
{
  reset();

  ObElectionPriority priority;
  priority.init(true, 1000, ObVersion(1), 100);
  ObElectionMsgVote msg1(priority, ADDR[0], ADDR[0], t1_, send_ts_ + 400000, ADDR[0]);

  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg1));

  ObAddr msg_cur_leader, msg_new_leader;
  ObElectionPriority leader_priority;

  EXPECT_EQ(OB_ELECTION_WARN_NOT_REACH_MAJORITY,
      msg_pool_.check_centralized_majority(msg_cur_leader, msg_new_leader, leader_priority, 3, t1_));
}

TEST_F(TestObElectionMsgPool, check_centralized_majority)
{
  reset();

  ObElectionPriority priority;
  priority.init(true, 1000, ObVersion(1), 100);
  ObElectionMsgVote msg1(priority, ADDR[0], ADDR[1], t1_, send_ts_ + 400000, ADDR[1]);

  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg1));

  ObElectionMsgVote msg2(priority, ADDR[0], ADDR[1], t1_, send_ts_ + 400000, ADDR[2]);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg2));

  ObAddr msg_cur_leader, msg_new_leader;
  ObElectionPriority leader_priority;

  EXPECT_EQ(OB_SUCCESS, msg_pool_.check_centralized_majority(msg_cur_leader, msg_new_leader, leader_priority, 3, t1_));
  EXPECT_EQ(0, leader_priority.compare_with_accurate_logid(priority));
  EXPECT_EQ(ADDR[0], msg_cur_leader);
  EXPECT_EQ(ADDR[1], msg_new_leader);
}

TEST_F(TestObElectionMsgPool, check_centralized_majority_no_sender)
{
  reset();

  ObElectionPriority priority;
  priority.init(true, 1000, ObVersion(1), 100);

  ObElectionMsgVote msg1(priority, ADDR[0], ADDR[2], t1_, send_ts_ + 400000, ADDR[0]);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg1));

  ObElectionMsgVote msg2(priority, ADDR[0], ADDR[2], t1_, send_ts_ + 400000, ADDR[1]);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg2));

  ObAddr msg_cur_leader, msg_new_leader;
  ObElectionPriority leader_priority;

  EXPECT_EQ(OB_ELECTION_WAIT_LEADER_MESSAGE,
      msg_pool_.check_centralized_majority(msg_cur_leader,
          msg_new_leader,

          leader_priority,
          3,
          t1_));

  ObElectionMsgVote msg3(priority, ADDR[0], ADDR[2], t1_, send_ts_ + 400000, ADDR[2]);

  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg3));

  EXPECT_EQ(OB_SUCCESS, msg_pool_.check_centralized_majority(msg_cur_leader, msg_new_leader, leader_priority, 3, t1_));
  EXPECT_EQ(ADDR[0], msg_cur_leader);
  EXPECT_EQ(ADDR[2], msg_new_leader);
}

TEST_F(TestObElectionMsgPool, check_centralized_change_leader_and_vote)
{
  reset();

  ObElectionPriority priority;
  priority.init(true, 1000, ObVersion(1), 100);
  ObElectionMsgVote msg1(priority, ADDR[0], ADDR[1], t1_, send_ts_ + 400000, ADDR[0]);

  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg1));

  ObElectionMsgVote msg2(priority, ADDR[0], ADDR[1], t1_, send_ts_ + 400000, ADDR[1]);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg2));

  ObAddr msg_cur_leader, msg_new_leader;
  ObElectionPriority leader_priority;

  EXPECT_EQ(OB_SUCCESS, msg_pool_.check_centralized_majority(msg_cur_leader, msg_new_leader, leader_priority, 3, t1_));
  EXPECT_EQ(0, leader_priority.compare_with_accurate_logid(priority));
  EXPECT_EQ(ADDR[1], msg_new_leader);
  EXPECT_EQ(ADDR[0], msg_cur_leader);

  ObElectionPriority priority2;
  priority2.init(false, 2000, ObVersion(1), 100);
  ObElectionMsgVote msg3(priority2, ADDR[0], ADDR[0], t1_, send_ts_ + 400000, ADDR[2]);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg3));

  EXPECT_EQ(OB_ELECTION_WARN_NOT_REACH_MAJORITY,
      msg_pool_.check_centralized_majority(msg_cur_leader, msg_new_leader, leader_priority, 3, t1_));
}

TEST_F(TestObElectionMsgPool, check_centralized_majority_diff_new_leader)
{
  reset();

  ObElectionPriority priority;
  priority.init(true, 1000, ObVersion(1), 100);
  ObElectionMsgVote msg1(priority, ADDR[0], ADDR[1], t1_, send_ts_ + 400000, ADDR[0]);

  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg1));

  ObElectionMsgVote msg2(priority, ADDR[0], ADDR[2], t1_, send_ts_ + 400000, ADDR[2]);
  EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg2));

  ObAddr msg_cur_leader, msg_new_leader;
  ObElectionPriority leader_priority;

  EXPECT_EQ(OB_ELECTION_WARN_NOT_REACH_MAJORITY,
      msg_pool_.check_centralized_majority(msg_cur_leader, msg_new_leader, leader_priority, 3, t1_));
}

TEST_F(TestObElectionMsgPool, check_new_T1_timestamp_clear_old_msg)
{

  for (int i = 0; i < 10000; ++i) {
    for (auto replica_num : REPLICA_NUM) {
      reset();

      vector<ObElectionMsgVote> v_msg;
      for (int i = 0; i < replica_num / 2 + 1; i++) {
        ObElectionPriority priority;
        priority.init(true, 1000 + i, ObVersion(1), 100 + i);
        v_msg.emplace_back(priority, ADDR[0], ADDR[1], t1_, send_ts_ + 400000, ADDR[i]);
        EXPECT_EQ(OB_SUCCESS, msg_pool_.store(v_msg.back()));
      }
      ObAddr msg_cur_leader, msg_new_leader;
      ObElectionPriority leader_priority;

      EXPECT_EQ(
          OB_SUCCESS, msg_pool_.check_centralized_majority(msg_cur_leader, msg_new_leader, leader_priority, 3, t1_));

      election_.set_current_ts(t1_ + T_ELECT2 + rand() % (4 * T_DIFF + T_ST) - 2 * T_DIFF);
      ObElectionPriority priority;
      priority.init(true, 1, ObVersion(1), 100 + i);
      ObElectionMsgVote msg3(priority, ADDR[0], ADDR[1], t1_ + T_ELECT2, send_ts_ + 400000, ADDR[1]);
      EXPECT_EQ(OB_SUCCESS, msg_pool_.store(msg3));

      EXPECT_EQ(OB_ELECTION_WARN_T1_NOT_MATCH,
          msg_pool_.check_centralized_majority(msg_cur_leader, msg_new_leader, leader_priority, replica_num, t1_));
      EXPECT_EQ(OB_ELECTION_WARN_NOT_REACH_MAJORITY,
          msg_pool_.check_centralized_majority(
              msg_cur_leader, msg_new_leader, leader_priority, replica_num, t1_ + T_ELECT2));
    }
  }
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  int ret = -1;

  oceanbase::election::ASYNC_LOG_INIT("test_election_msg_pool.log", OB_LOG_LEVEL_INFO, true);

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
