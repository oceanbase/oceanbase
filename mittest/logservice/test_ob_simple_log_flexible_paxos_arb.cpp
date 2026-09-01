// owner: tengqi.tq
// owner group: log

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "logservice/palf/log_quorum_policy.h"
#define private public
#include "env/ob_simple_log_cluster_env.h"
#undef private

const std::string TEST_NAME = "flexible_paxos_arb";
using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
using namespace palf;
namespace unittest
{

class TestObSimpleLogFlexiblePaxosArb : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogFlexiblePaxosArb() : ObSimpleLogClusterTestEnv()
  {}

protected:
  static int64_t get_another_full_replica_idx(
      const int64_t leader_idx,
      const int64_t arb_replica_idx)
  {
    for (int64_t i = 0; i < 3; ++i) {
      if (i != leader_idx && i != arb_replica_idx) {
        return i;
      }
    }
    return -1;
  }
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 5;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_ = true;
bool ObSimpleLogClusterTestBase::need_shared_storage_ = false;

// Case 1: 2F1A/Q2=1 basic write - leader local commit without Full/Arb ACK
TEST_F(TestObSimpleLogFlexiblePaxosArb, test_2f1a_q2_1_basic_write)
{
  SET_CASE_LOG_FILE(TEST_NAME, "2f1a_q2_1_basic_write");
  OB_LOGGER.set_log_level("INFO");

  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));

  const int64_t full_follower_idx = get_another_full_replica_idx(leader_idx, arb_replica_idx);
  ASSERT_NE(-1, full_follower_idx);
  EXPECT_EQ(1, leader.palf_handle_impl_->quorum_policy_.get_accept_quorum(2));
  EXPECT_EQ(3, leader.palf_handle_impl_->quorum_policy_.get_prepare_quorum(3));

  PalfHandleLiteGuard arb_guard;
  ASSERT_EQ(OB_SUCCESS, get_arb_member_guard(id, arb_guard));
  ASSERT_TRUE(NULL != arb_guard.palf_handle_lite_);
  EXPECT_EQ(1, arb_guard.palf_handle_lite_->quorum_policy_.get_accept_quorum(2));
  EXPECT_EQ(3, arb_guard.palf_handle_lite_->quorum_policy_.get_prepare_quorum(3));

  block_net(leader_idx, full_follower_idx);
  block_net(leader_idx, arb_replica_idx);
  sleep(1);

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 50, id));
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn(), leader.palf_handle_impl_->get_end_lsn());

  unblock_net(leader_idx, full_follower_idx);
  unblock_net(leader_idx, arb_replica_idx);

  arb_guard.reset();
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_q2_1_basic_write", K(id), K(arb_replica_idx), K(full_follower_idx));
}

// Case 2: 2F1A/Q1 requires both Full replicas; Arb cannot replace the isolated old leader
TEST_F(TestObSimpleLogFlexiblePaxosArb, test_2f1a_q1_requires_both_full_replicas)
{
  SET_CASE_LOG_FILE(TEST_NAME, "2f1a_q1_requires_both_full_replicas");
  OB_LOGGER.set_log_level("INFO");

  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));

  const int64_t full_follower_idx = get_another_full_replica_idx(leader_idx, arb_replica_idx);
  ASSERT_NE(-1, full_follower_idx);

  // The old leader commits locally, then becomes unreachable. The remaining Full
  // and Arb may win election, but reconfirm must not become active without the
  // old Full replica that may hold the only accepted copy of the committed log.
  block_net(leader_idx, full_follower_idx);
  block_net(leader_idx, arb_replica_idx);
  sleep(1);

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 50, id));
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn(), leader.palf_handle_impl_->get_end_lsn());

  bool is_pending_state = false;
  bool is_active_leader = false;
  for (int64_t retry = 0; retry < 100 && !is_pending_state && !is_active_leader; ++retry) {
    IPalfHandleImpl *handle = NULL;
    ObRole role = FOLLOWER;
    int64_t proposal_id = INVALID_PROPOSAL_ID;
    const int ret = get_cluster()[full_follower_idx]->get_palf_env()->get_palf_handle_impl(id, handle);
    EXPECT_EQ(OB_SUCCESS, ret);
    if (OB_SUCCESS == ret) {
      EXPECT_EQ(OB_SUCCESS, handle->get_role(role, proposal_id, is_pending_state));
      is_active_leader = LEADER == role && false == is_pending_state;
      get_cluster()[full_follower_idx]->get_palf_env()->revert_palf_handle_impl(handle);
    }
    if (!is_pending_state && !is_active_leader) {
      ob_usleep(100 * 1000L);
    }
  }
  EXPECT_TRUE(is_pending_state);
  EXPECT_FALSE(is_active_leader);

  unblock_net(leader_idx, full_follower_idx);
  unblock_net(leader_idx, arb_replica_idx);

  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_q1_requires_both_full_replicas",
      K(id), K(arb_replica_idx), K(full_follower_idx));
}

// Case 3: 2F1A degrade/upgrade keeps the classic freeze path and Q2=1 write path
TEST_F(TestObSimpleLogFlexiblePaxosArb, test_2f1a_degrade_upgrade_in_flex)
{
  SET_CASE_LOG_FILE(TEST_NAME, "2f1a_degrade_upgrade_in_flex");
  OB_LOGGER.set_log_level("INFO");
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L;

  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));

  const int64_t full_follower_idx = get_another_full_replica_idx(leader_idx, arb_replica_idx);
  ASSERT_NE(-1, full_follower_idx);
  const ObMember full_follower(get_cluster()[full_follower_idx]->get_addr(), 1);

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 20, id));
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn(), leader.palf_handle_impl_->get_end_lsn());

  LogConfigChangeArgs degrade_args(full_follower, 0, DEGRADE_ACCEPTOR_TO_LEARNER);
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->one_stage_config_change_(degrade_args, CONFIG_CHANGE_TIMEOUT));
  EXPECT_TRUE(is_degraded(leader, full_follower_idx));

  // After degrading the other Full to learner, block Arb as well. The only Full
  // acceptor is the leader, so this write can commit only when Q2=1 is effective.
  block_net(leader_idx, arb_replica_idx);
  sleep(1);
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 20, id));
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn(), leader.palf_handle_impl_->get_end_lsn());
  unblock_net(leader_idx, arb_replica_idx);

  LogConfigChangeArgs upgrade_args(full_follower, 0, UPGRADE_LEARNER_TO_ACCEPTOR);
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->one_stage_config_change_(upgrade_args, CONFIG_CHANGE_TIMEOUT));
  EXPECT_TRUE(is_upgraded(leader, id));

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 20, id));
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn(), leader.palf_handle_impl_->get_end_lsn());

  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_degrade_upgrade_in_flex", K(id), K(arb_replica_idx), K(full_follower_idx));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
