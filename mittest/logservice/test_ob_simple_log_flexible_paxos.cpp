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

const std::string TEST_NAME = "flexible_paxos";
using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
using namespace palf;
namespace unittest
{

class TestObSimpleLogFlexiblePaxos : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogFlexiblePaxos() : ObSimpleLogClusterTestEnv()
  {}
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 5;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_ = false;
bool ObSimpleLogClusterTestBase::need_shared_storage_ = false;

// Case 1: FLEX requires the target replica_num to equal the resulting member count
TEST_F(TestObSimpleLogFlexiblePaxos, test_memberlist_count_must_equal_replica_num)
{
  SET_CASE_LOG_FILE(TEST_NAME, "memberlist_count_must_equal_replica_num");
  OB_LOGGER.set_log_level("INFO");

  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t config_change_timeout_us = 10 * 1000 * 1000L;
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));

  const int64_t follower_idx = (leader_idx + 1) % 3;
  const ObAddr follower_addr = get_cluster()[follower_idx]->get_addr();
  const ObMember follower(follower_addr, 1);
  ObMemberList member_list;
  int64_t replica_num = 0;

  // Removing one member while keeping replica_num=3 would leave a 2/3 config.
  // It satisfies classic majority semantics but must be rejected by FLEX.
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      leader.palf_handle_impl_->remove_member(follower, 3, config_change_timeout_us));
  ASSERT_EQ(OB_SUCCESS,
      leader.palf_handle_impl_->get_paxos_member_list(member_list, replica_num));
  EXPECT_EQ(3, member_list.get_member_number());
  EXPECT_EQ(3, replica_num);
  EXPECT_TRUE(member_list.contains(follower_addr));

  // The same removal is valid when the target replica_num matches the new member count.
  ASSERT_EQ(OB_SUCCESS,
      leader.palf_handle_impl_->remove_member(follower, 2, config_change_timeout_us));
  ASSERT_EQ(OB_SUCCESS,
      leader.palf_handle_impl_->get_paxos_member_list(member_list, replica_num));
  EXPECT_EQ(2, member_list.get_member_number());
  EXPECT_EQ(2, replica_num);
  EXPECT_FALSE(member_list.contains(follower_addr));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 50, id));
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn(), leader.palf_handle_impl_->get_end_lsn());

  leader.reset();
  delete_paxos_group(id);
}

// Case 2: 3F/Q2=1 basic write - leader local commit needs no follower log ACK
TEST_F(TestObSimpleLogFlexiblePaxos, test_3f_q2_1_basic_write)
{
  SET_CASE_LOG_FILE(TEST_NAME, "3f_q2_1_basic_write");
  OB_LOGGER.set_log_level("INFO");

  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));

  PalfHandleImpl *leader_impl = leader.get_palf_handle_impl();
  EXPECT_EQ(1, leader_impl->quorum_policy_.get_accept_quorum(3));
  EXPECT_EQ(3, leader_impl->quorum_policy_.get_prepare_quorum(3));

  // While the current leader lease is valid, Q2=1 means local flush is enough
  // to commit even when neither follower can return a log ACK.
  const int64_t f1_idx = (leader_idx + 1) % 3;
  const int64_t f2_idx = (leader_idx + 2) % 3;
  block_net(leader_idx, f1_idx);
  block_net(leader_idx, f2_idx);
  sleep(1);

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 50, id));
  EXPECT_UNTIL_EQ(leader_impl->get_max_lsn(), leader_impl->get_end_lsn());

  unblock_net(leader_idx, f1_idx);
  unblock_net(leader_idx, f2_idx);

  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_3f_q2_1_basic_write", K(id));
}

// Case 3: 3F/Q1=N reconfirm protects and recovers a leader-local committed log
TEST_F(TestObSimpleLogFlexiblePaxos, test_3f_q1_all_reconfirm_recovers_local_commit)
{
  SET_CASE_LOG_FILE(TEST_NAME, "3f_q1_all_reconfirm_recovers_local_commit");
  OB_LOGGER.set_log_level("INFO");

  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));

  const int64_t follower1_idx = (leader_idx + 1) % 3;
  const int64_t follower2_idx = (leader_idx + 2) % 3;
  block_net(leader_idx, follower1_idx);
  block_net(leader_idx, follower2_idx);
  sleep(1);

  // The old leader commits a log which exists only on itself.
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 50, id));
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn(), leader.palf_handle_impl_->get_end_lsn());
  const LSN committed_end_lsn = leader.palf_handle_impl_->get_end_lsn();

  // After its lease expires, the two connected followers can elect a candidate,
  // but Q1=N keeps that candidate pending until it can reconfirm with the old
  // leader which may hold the only copy of the committed log.
  bool has_pending_follower = false;
  bool has_active_leader = false;
  for (int64_t retry = 0; retry < 100 && !has_pending_follower && !has_active_leader; ++retry) {
    for (int64_t i = 0; i < 3; ++i) {
      if (i != leader_idx) {
        IPalfHandleImpl *handle = NULL;
        ObRole role = FOLLOWER;
        int64_t proposal_id = INVALID_PROPOSAL_ID;
        bool is_pending_state = false;
        const int ret = get_cluster()[i]->get_palf_env()->get_palf_handle_impl(id, handle);
        EXPECT_EQ(OB_SUCCESS, ret);
        if (OB_SUCCESS == ret) {
          EXPECT_EQ(OB_SUCCESS, handle->get_role(role, proposal_id, is_pending_state));
          has_pending_follower = has_pending_follower || is_pending_state;
          has_active_leader = has_active_leader || (LEADER == role && false == is_pending_state);
          get_cluster()[i]->get_palf_env()->revert_palf_handle_impl(handle);
        }
      }
    }
    if (!has_pending_follower && !has_active_leader) {
      ob_usleep(100 * 1000L);
    }
  }
  EXPECT_TRUE(has_pending_follower);
  EXPECT_FALSE(has_active_leader);

  // Once all three Full replicas are connected, reconfirm can collect Q1=N and
  // the new leader must recover the leader-local committed log.
  unblock_net(leader_idx, follower1_idx);
  unblock_net(leader_idx, follower2_idx);

  PalfHandleImplGuard new_leader;
  ASSERT_EQ(OB_SUCCESS, switch_leader(id, follower1_idx, new_leader));
  EXPECT_LE(committed_end_lsn, new_leader.palf_handle_impl_->get_end_lsn());
  ASSERT_EQ(OB_SUCCESS, submit_log(new_leader, 20, id));
  EXPECT_UNTIL_EQ(new_leader.palf_handle_impl_->get_max_lsn(), new_leader.palf_handle_impl_->get_end_lsn());

  leader.reset();
  new_leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_3f_q1_all_reconfirm_recovers_local_commit", K(id), K(committed_end_lsn));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
