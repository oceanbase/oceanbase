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

#include <cstdio>
#include <gtest/gtest.h>
#include <signal.h>
#include <share/scn.h>
#define private public
#include "logservice/palf/log_config_mgr.h"
#include "env/ob_simple_log_cluster_env.h"
#undef private

const std::string TEST_NAME = "config_change";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class TestObSimpleLogClusterConfigChange : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterConfigChange() :  ObSimpleLogClusterTestEnv()
  {}
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 7;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

bool check_children_valid(const std::vector<PalfHandleImplGuard*> &palf_list, const LogLearnerList &all_learner)
{
  int ret = OB_SUCCESS;
  LogLearnerList all_children;
  for (auto palf : palf_list) {
    const LogLearnerList &self_children = palf->palf_handle_impl_->config_mgr_.children_;
    int64_t children_cnt = self_children.get_member_number();
    for (int i = 0; i < children_cnt; ++i) {
      LogLearner tmp_server;
      if (OB_FAIL(self_children.get_learner(i, tmp_server))) {
      } else if (OB_FAIL(all_children.add_learner(tmp_server))) {
      }
    }
  }
  ret = all_children.learner_addr_equal(all_learner);
  PALF_LOG(INFO, "check_children", K(ret), K(all_children), K(all_learner));
  return ret;
}

bool check_parent(const std::vector<PalfHandleImplGuard*> &palf_list, const LogLearnerList &all_learner, const ObAddr &parent)
{
  bool bool_ret = true;
  for (auto palf : palf_list) {
    const ObAddr &self = palf->palf_handle_impl_->self_;
    if (all_learner.contains(self)) {
      const ObAddr &my_parent = palf->palf_handle_impl_->config_mgr_.parent_;
      if (my_parent.is_valid() && (my_parent == parent)) {
        continue;
      } else {
        bool_ret = false;
        break;
      }
    }
  }
  return bool_ret;
}

int check_log_sync(const std::vector<PalfHandleImplGuard*> &palf_list,
                   const common::ObMemberList &member_list,
                   const LogLearnerList &learner_list,
                   PalfHandleImplGuard &leader)
{
  int ret = OB_SUCCESS;
  const int64_t max_flushed_proposal_id = leader.palf_handle_impl_->sw_.max_flushed_log_pid_;
  const LSN max_flushed_end_lsn = leader.palf_handle_impl_->sw_.max_flushed_end_lsn_;
  PALF_LOG(INFO, "before check_log_sync", K(max_flushed_proposal_id), K(max_flushed_end_lsn));
  for (auto palf : palf_list) {
    const common::ObAddr parent_addr = palf->palf_handle_impl_->config_mgr_.parent_;
    // for paxos member, log must sync with leader
    // for learner, sync with parent's committed_end_lsn
    const int64_t this_max_flushed_proposal_id = palf->palf_handle_impl_->sw_.max_flushed_log_pid_;
    const LSN this_max_flushed_end_lsn = palf->palf_handle_impl_->sw_.max_flushed_end_lsn_;
    const common::ObAddr self = palf->palf_handle_impl_->self_;
    if (member_list.contains(self)) {
      PALF_LOG(INFO, "before check", K(max_flushed_proposal_id), K(max_flushed_end_lsn), K(this_max_flushed_proposal_id),
          K(this_max_flushed_end_lsn), K(self));
      EXPECT_EQ(max_flushed_proposal_id, this_max_flushed_proposal_id);
      EXPECT_EQ(max_flushed_end_lsn, this_max_flushed_end_lsn) \
          << max_flushed_end_lsn.val_ << "," << this_max_flushed_end_lsn.val_;
    } else if (!learner_list.contains(self)) {
    } else if (!parent_addr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      break;
    } else {
      for (auto parent: palf_list) {
        if (parent->palf_handle_impl_->self_ == parent_addr) {
          const LSN parent_committed_end_lsn = parent->palf_handle_impl_->sw_.committed_end_lsn_;
          EXPECT_EQ(parent_committed_end_lsn, this_max_flushed_end_lsn) \
              << parent_committed_end_lsn.val_ << "," << this_max_flushed_end_lsn.val_;
          if (parent_committed_end_lsn != this_max_flushed_end_lsn) {
            PALF_LOG(ERROR, "log not sync", K(parent_addr), "self", palf->palf_handle_impl_->self_,
                K(parent_committed_end_lsn), K(this_max_flushed_end_lsn));
          }
        }
      }
    }
  }
  return ret;
}

MockLocCB loc_cb;

TEST_F(TestObSimpleLogClusterConfigChange, split_brain)
{
  SET_CASE_LOG_FILE(TEST_NAME, "split_brain");
  std::vector<PalfHandleImplGuard*> palf_list;
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test split_brain", K(id));
	int64_t leader_idx = 0;
  // 1. A, B, C
  // 2. B block_net
  // 3. A, C, D
  // 4. A, D. delete C
  // 5. unblock_net B, create new C
  // 6. may be split-brain {B, C} {A, D}
  {
    PalfHandleImplGuard leader;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    PALF_LOG(INFO, "after submit_log");
    const int64_t follower_B_idx = (leader_idx + 1);
    const int64_t follower_C_idx = (leader_idx + 2);
    const int64_t follower_D_idx = (leader_idx + 3);
    // step 2
    EXPECT_EQ(OB_SUCCESS, get_cluster()[follower_B_idx]->simple_close(false));
    // step 3
    const ObAddr follower_b_addr = get_cluster()[follower_B_idx]->get_addr();
    PALF_LOG(INFO, "before remove member");
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(follower_b_addr, 1), 3, CONFIG_CHANGE_TIMEOUT));
    PALF_LOG(INFO, "after remove member");

    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", loc_cb.leader_);

    LogConfigVersion config_version;
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[follower_D_idx]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    // step 4
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(get_cluster()[follower_C_idx]->get_addr(), 1), 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, get_cluster()[follower_C_idx]->get_palf_env()->remove_palf_handle_impl(id));
    // step 5
    const int64_t node_id = follower_B_idx * 2 + get_node_idx_base();
    EXPECT_EQ(OB_SUCCESS, get_cluster()[follower_B_idx]->simple_init(get_test_name(), follower_b_addr, node_id, false));
    EXPECT_EQ(OB_SUCCESS, get_cluster()[follower_B_idx]->simple_start(false));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    PalfBaseInfo palf_base_info;
    IPalfHandleImpl* follower_C_handle = NULL;
    palf_base_info.generate_by_default();
    //PalfHandleImplGuard leader;
    palf_base_info.prev_log_info_.scn_ = share::SCN::min_scn();
    EXPECT_EQ(OB_SUCCESS, get_cluster()[follower_C_idx]->get_palf_env()->create_palf_handle_impl(id, palf::AccessMode::APPEND, palf_base_info, follower_C_handle));
    get_cluster()[follower_C_idx]->get_palf_env()->revert_palf_handle_impl(follower_C_handle);
    sleep(5);
    // check if split-brain happens
    int64_t leader_cnt = 0;
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    for (auto palf: palf_list) {
      ObRole role;
      palf::ObReplicaState unused_state;
      palf->palf_handle_impl_->state_mgr_.get_role_and_state(role, unused_state);
      if (role == LEADER) {
        leader_cnt += 1;
      }
    }
    EXPECT_EQ(leader_cnt, 1);
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test split_brain", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_config_change_defensive)
{
  SET_CASE_LOG_FILE(TEST_NAME, "config_change_defensive");
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_config_change_defensive");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
    common::ObMember dummy_member;
    const LogConfigChangeArgs args(dummy_member, 1, STARTWORKING);
	EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    const int64_t lag_follower_idx = (leader_idx+1)%3;
    const int64_t remove_follower_idx = (leader_idx+2)%3;
    block_net(leader_idx, lag_follower_idx);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    sleep(1);
    // remove member who is in majority
    EXPECT_EQ(OB_TIMEOUT, leader.palf_handle_impl_->remove_member(ObMember(palf_list[remove_follower_idx]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT / 2));
    unblock_net(leader_idx, lag_follower_idx);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[remove_follower_idx]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));

    int64_t new_leader_idx;
    PalfHandleImplGuard new_leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());

    LogConfigVersion config_version;
    ASSERT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[remove_follower_idx]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id));
    // majority of members should be normal
    palf_list[lag_follower_idx]->palf_handle_impl_->disable_vote(false);
    EXPECT_EQ(OB_TIMEOUT, leader.palf_handle_impl_->remove_member(ObMember(palf_list[remove_follower_idx]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT / 2));
    palf_list[lag_follower_idx]->palf_handle_impl_->enable_vote();

    block_net(leader_idx, lag_follower_idx);

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    sleep(3);

    // add member without any logs
    // we will send config log to 3 before add_member(3, 4), so 3 can know who is leader and fetch log from it.
    // so just comment this line, after leader forbit fetch log req that comes from node who is not follower or
    // children of leader, we will uncomment this line
    // EXPECT_EQ(OB_TIMEOUT, leader.add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT / 2));
    unblock_net(leader_idx, lag_follower_idx);
    ASSERT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    //test add_member_with_check
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->remove_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    LogConfigVersion invalid_config_version;
    int64_t follower_idx = (new_leader_idx +1) % 3;
    EXPECT_EQ(OB_STATE_NOT_MATCH, new_leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, invalid_config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_NOT_MASTER, palf_list[follower_idx]->palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(true, config_version.is_valid());
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));

    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_config_change_defensive", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_change_replica_num)
{
  SET_CASE_LOG_FILE(TEST_NAME, "change_replica_num");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_FAA(&palf_id_, 1);
  PALF_LOG(INFO, "begin test_change_replica_num", K(id));
  {
	  int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    // 3->5
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(get_member_list(), 3, 5, CONFIG_CHANGE_TIMEOUT));
    LogConfigVersion config_version;
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[4]->get_addr(), 1), 5, config_version, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[5]->get_addr(), 1), 5, config_version, CONFIG_CHANGE_TIMEOUT));
    common::ObMemberList curr_member_list;
    int64_t curr_replica_num;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(get_cluster()[4]->get_addr(), 1), 5, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(get_cluster()[5]->get_addr(), 1), 5, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_paxos_member_list(curr_member_list, curr_replica_num));
    // 5->3
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(curr_member_list, curr_replica_num, 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    // 3->4
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_paxos_member_list(curr_member_list, curr_replica_num));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(curr_member_list, curr_replica_num, 4, CONFIG_CHANGE_TIMEOUT));
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_change_replica_num", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_basic_config_change)
{
  SET_CASE_LOG_FILE(TEST_NAME, "config_change");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test config change", K(id));
  {
	  int64_t leader_idx = 0;
    std::vector<PalfHandleImplGuard*> palf_list;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    // add member when no log
    LogConfigVersion config_version;
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[(leader_idx+2)%3]->palf_handle_impl_->self_, 1), 1, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 2, config_version, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+2)%3]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // add member when contains log
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), 5, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // remove member
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));


    EXPECT_EQ(OB_STATE_NOT_MATCH, leader.palf_handle_impl_->replace_member(ObMember(palf_list[5]->palf_handle_impl_->self_, 1),
                                                                           ObMember(palf_list[3]->palf_handle_impl_->self_, 1),
                                                                           config_version,
                                                                           CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    // replace member
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member(ObMember(palf_list[5]->palf_handle_impl_->self_, 1),
                                                                   ObMember(palf_list[3]->palf_handle_impl_->self_, 1),
                                                                   config_version,
                                                                   CONFIG_CHANGE_TIMEOUT));

    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    // switch acceptor to learner
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(palf_list[5]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    // add learner
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[6]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test config change", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_basic_config_change_for_migration)
{
  SET_CASE_LOG_FILE(TEST_NAME, "config_change_for_migration");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test config change", K(id));
  {
	  int64_t leader_idx = 0;
    std::vector<PalfHandleImplGuard*> palf_list;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    const common::ObAddr &addr2 = get_cluster()[(leader_idx+2)%3]->get_addr();
    const common::ObAddr &addr3 = get_cluster()[3]->get_addr();
    const common::ObAddr &addr4 = get_cluster()[4]->get_addr();
    const common::ObAddr &addr5 = get_cluster()[5]->get_addr();
    // 1. replicate an FULL replica
    {
      PALF_LOG(INFO, "CASE1: replicate an FULL replica", K(id));
      common::ObMember added_member = ObMember(addr3, 1);
      added_member.set_migrating();
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_member, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);

      // clean
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(added_member, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      // add again
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_member, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));

      LogConfigVersion config_version;
      ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_learner_to_acceptor(added_member, 4, config_version, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      // member with flag do not exist
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member.get_server()));
      EXPECT_EQ(4, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      // reentrant, do not get config_version again
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_learner_to_acceptor(added_member, 4, config_version, CONFIG_CHANGE_TIMEOUT));
      // reset environment
      added_member.reset_migrating();
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(added_member, 3, CONFIG_CHANGE_TIMEOUT));
    }

    // 2. migrate an FULL replica (addr2 -> addr3)
    {
      PALF_LOG(INFO, "CASE2: migrate an FULL replica", K(id));
      common::ObMember added_member = ObMember(addr3, 1);
      added_member.set_migrating();
      common::ObMember replaced_member = ObMember(addr2, 1);
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_member, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      LogConfigVersion config_version;
      ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
      LogConfigChangeArgs args(added_member, 0, config_version, SWITCH_LEARNER_TO_ACCEPTOR_AND_NUM);
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->one_stage_config_change_(args, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member.get_server()));
      EXPECT_EQ(4, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);

      ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member_with_learner(added_member, replaced_member, config_version, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      // member with flag do not exist
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member.get_server()));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(replaced_member.get_server()));
      // reentrant and check
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member_with_learner(added_member, replaced_member, config_version, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      // member with flag do not exist
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member.get_server()));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(replaced_member.get_server()));
      // reset environment
      added_member.reset_migrating();
      ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member(replaced_member, added_member, config_version, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member.get_server()));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(replaced_member.get_server()));
    }

    // 3. replicate an READONLY replica
    {
      PALF_LOG(INFO, "CASE3: replicate an READONLY replica", K(id));
      // learner's addr must be different from members'
      common::ObMember migrating_member = ObMember(addr2, 1);
      EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->add_learner(migrating_member, CONFIG_CHANGE_TIMEOUT));

      common::ObMember added_migrating_learner = ObMember(addr3, 1);
      common::ObMember added_learner = ObMember(addr3, 1);
      added_migrating_learner.set_migrating();
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_migrating_learner, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_migrating_learner));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      ObMemberList added_learners, removed_learners;
      EXPECT_EQ(OB_SUCCESS, added_learners.add_member(added_learner));
      EXPECT_EQ(OB_SUCCESS, removed_learners.add_member(added_migrating_learner));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_learner));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_migrating_learner));
      // reentrant and check
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_learner));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_migrating_learner));
      // reset environment
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(added_learner, CONFIG_CHANGE_TIMEOUT));
    }

    // 4. migrate an READONLY replica, addr4 -> addr3
    {
      PALF_LOG(INFO, "CASE4: migrate an READONLY replica", K(id));
      common::ObMember removed_learner = ObMember(addr4, 1);
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(removed_learner, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(removed_learner));

      common::ObMember added_migrating_learner = ObMember(addr3, 1);
      common::ObMember added_learner = ObMember(addr3, 1);
      added_migrating_learner.set_migrating();
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_migrating_learner, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_migrating_learner));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      ObMemberList added_learners, removed_learners;
      EXPECT_EQ(OB_SUCCESS, added_learners.add_member(added_learner));
      EXPECT_EQ(OB_SUCCESS, removed_learners.add_member(added_migrating_learner));
      EXPECT_EQ(OB_SUCCESS, removed_learners.add_member(removed_learner));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_learner));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_migrating_learner));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(removed_learner));
      // reentrant and check
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_learner));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_migrating_learner));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(removed_learner));
      // reset environment
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(added_learner, CONFIG_CHANGE_TIMEOUT));
    }
    // 5. replace_learners (addr3, addr4) -> (addr3, addr5)
    {
      PALF_LOG(INFO, "CASE5: replace_learners", K(id));
      const common::ObMember member2 = ObMember(addr2, 1);
      const common::ObMember member3 = ObMember(addr3, 1);
      const common::ObMember member4 = ObMember(addr4, 1);
      const common::ObMember member5 = ObMember(addr5, 1);
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(member3, CONFIG_CHANGE_TIMEOUT));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(member4, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member3));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member4));

      ObMemberList added_learners, removed_learners;
      EXPECT_EQ(OB_SUCCESS, added_learners.add_member(member3));
      EXPECT_EQ(OB_SUCCESS, added_learners.add_member(member5));
      EXPECT_EQ(OB_SUCCESS, removed_learners.add_member(member2));
      EXPECT_EQ(OB_SUCCESS, removed_learners.add_member(member4));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member3));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member5));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member2));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member4));
      // reentrant and check
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member3));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member5));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member2));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member4));
      // reset environment
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(member3, CONFIG_CHANGE_TIMEOUT));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(member5, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member3));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member5));
    }
    // 6. defensive
    {
      PALF_LOG(INFO, "CASE6: defensive", K(id));
      const common::ObMember member2 = ObMember(addr2, 1);
      const common::ObMember member3 = ObMember(addr3, 1);
      const common::ObMember member4 = ObMember(addr4, 1);
      const common::ObMember member5 = ObMember(addr5, 1);
      common::ObMember migrating_member3 = member3;
      migrating_member3.set_migrating();

      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(migrating_member3, CONFIG_CHANGE_TIMEOUT));
      EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->add_learner(member3, CONFIG_CHANGE_TIMEOUT));
      LogConfigVersion config_version;
      ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
      EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->add_member(member3, 4, config_version, CONFIG_CHANGE_TIMEOUT));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(member3, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(migrating_member3));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(migrating_member3, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(migrating_member3));

      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(migrating_member3, CONFIG_CHANGE_TIMEOUT));
      ObMemberList added_learners, removed_learners;
      EXPECT_EQ(OB_SUCCESS, added_learners.add_member(member3));
      EXPECT_EQ(OB_SUCCESS, removed_learners.add_member(member4));
      EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));

      common::ObMember migrating_member2 = member2;
      migrating_member2.set_migrating();
      EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->remove_member(migrating_member2, 2, CONFIG_CHANGE_TIMEOUT));
    }
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test config change", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_replace_member)
{
  SET_CASE_LOG_FILE(TEST_NAME, "replace_member");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test replace_member", K(id));
  {
    std::vector<PalfHandleImplGuard*> palf_list;
	  int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    // replace member when no log
    LogConfigVersion config_version;
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[(leader_idx+2)%3]->palf_handle_impl_->self_, 1), 1, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 2, config_version, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+2)%3]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // add member when contains log
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), 5, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // remove member
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // replace member
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member(ObMember(palf_list[5]->palf_handle_impl_->self_, 1),
                                               ObMember(palf_list[3]->palf_handle_impl_->self_, 1),
                                               config_version,
                                               CONFIG_CHANGE_TIMEOUT));

    // switch acceptor to learner
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(palf_list[5]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    // add learner
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[6]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test replace_member", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, learner)
{
  SET_CASE_LOG_FILE(TEST_NAME, "learner");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
	int64_t leader_idx = 0;
  int64_t log_ts = 1;
  PalfHandleImplGuard leader;
  LogLearnerList all_learner;
  std::vector<PalfHandleImplGuard*> palf_list;
  std::vector<ObRegion> region_list;
  common::ObRegion default_region(DEFAULT_REGION_NAME);
  LogMemberRegionMap region_map;
  EXPECT_EQ(OB_SUCCESS, region_map.init("localmap", OB_MAX_MEMBER_NUMBER));
  region_list.push_back(ObRegion("BEIJING"));
  region_list.push_back(ObRegion("SHANGHAI"));
  region_list.push_back(ObRegion("TIANJIN"));
  region_list.push_back(ObRegion("SHENZHEN"));
  region_list.push_back(ObRegion("GUANGZHOU"));
  const ObMemberList &node_list = get_node_list();
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  loc_cb.leader_ = get_cluster()[leader_idx]->get_addr();
  PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[leader_idx]->get_addr());

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  // case 1: set region and switch_acceptor_to_learner
  // add_learner
  for (int64_t i = 3; i < ObSimpleLogClusterTestBase::node_cnt_; ++i) {
    PalfHandleImplGuard tmp_handle;
    common::ObMember added_learner;
    EXPECT_EQ(OB_SUCCESS, node_list.get_member_by_index(i, added_learner));
    LogLearner learner(added_learner.get_server(), 1);
    EXPECT_EQ(OB_SUCCESS, all_learner.add_learner(learner));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_learner, CONFIG_CHANGE_TIMEOUT));
  }
  // check children_cnt
  while (false == check_children_valid(palf_list, all_learner))
  {
    sleep(1);
    PALF_LOG(INFO, "check_children_valid 1");
  }
  // change region of one follower
  bool has_change_region = false;
  int64_t diff_region_follower_idx = -1;
  int64_t another_follower_idx = -1;
  for (int i = 0; i < ObSimpleLogClusterTestBase::member_cnt_; i++) {
    const bool not_leader = palf_list[i]->palf_handle_impl_->self_ != leader.palf_handle_impl_->self_;
    if (!has_change_region && not_leader) {
      EXPECT_EQ(OB_SUCCESS, palf_list[i]->palf_handle_impl_->set_region(region_list[0]));
      region_map.insert(palf_list[i]->palf_handle_impl_->self_, region_list[0]);
      has_change_region = true;
      diff_region_follower_idx = i;
    } else {
      if (not_leader) {
        another_follower_idx = i;
      }
      region_map.insert(palf_list[0]->palf_handle_impl_->self_, default_region);
    }
  }
  // notify leader region of follower i has changed
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->set_paxos_member_region_map(region_map));
  // check children_cnt again
  while (false == check_children_valid(palf_list, all_learner))
  {
    sleep(1);
    PALF_LOG(INFO, "check_children_valid 2");
  }
  // after setting region of a follower, parents of all learners should be another follower
  EXPECT_GE(another_follower_idx, 0);
  EXPECT_LE(another_follower_idx, 2);
  ObAddr curr_parent = palf_list[another_follower_idx]->palf_handle_impl_->self_;
  while (false == check_parent(palf_list, all_learner, curr_parent))
  {
    sleep(1);
    PALF_LOG(INFO, "check_parent 1");
  }
  // continue submitting log
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, id));
  PALF_LOG(INFO, "all_learner", K(all_learner));
  // EXPECT_EQ(OB_SUCCESS, check_log_sync(palf_list, get_member_list(), all_learner, leader));

  // switch current unique parent to learner
  EXPECT_EQ(OB_SUCCESS, all_learner.add_learner(LogLearner(curr_parent, 1)));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(curr_parent, 1), 2, CONFIG_CHANGE_TIMEOUT));
  // after switch follower 1 to learner, a learner will be registered to leader, and other learners will
  // be registerd to this learner
  while (false == check_children_valid(palf_list, all_learner))
  {
    sleep(1);
    PALF_LOG(INFO, "check_children_valid 3");
  }
  // check learner topology
  ObAddr leaderschild;
  PalfHandleImplGuard leaderschild_handle;
  LogLearnerList expect_children;
  EXPECT_EQ(1, leader.palf_handle_impl_->config_mgr_.children_.get_member_number());
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.children_.get_server_by_index(0, leaderschild));
  EXPECT_EQ(OB_SUCCESS, get_palf_handle_guard(palf_list, leaderschild, leaderschild_handle));
  expect_children = all_learner;
  EXPECT_EQ(OB_SUCCESS, expect_children.remove_learner(leaderschild));
  EXPECT_TRUE(expect_children.learner_addr_equal(leaderschild_handle.palf_handle_impl_->config_mgr_.children_));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, id));
  // EXPECT_EQ(OB_SUCCESS, check_log_sync(palf_list, get_member_list(), all_learner, leader));
  // learners' regions are different from paxos member, so parent of all learners is leader
  // set regions
  for (int64_t i = 3; i < ObSimpleLogClusterTestBase::node_cnt_; ++i) {
    PalfHandleImplGuard tmp_handle;
    common::ObMember learner;
    EXPECT_EQ(OB_SUCCESS, node_list.get_member_by_index(i, learner));
    EXPECT_EQ(OB_SUCCESS, get_palf_handle_guard(palf_list, learner.get_server(), tmp_handle));
    EXPECT_EQ(OB_SUCCESS, tmp_handle.palf_handle_impl_->set_region(region_list[i-2]));
  }
  sleep(1);
  // check children_cnt
  while (false == check_children_valid(palf_list, all_learner))
  {
    sleep(1);
    PALF_LOG(INFO, "check_children_valid 4");
  }
  while (false == check_parent(palf_list, all_learner, leader.palf_handle_impl_->self_))
  {
    sleep(1);
    PALF_LOG(INFO, "check_parent 2");
  }

  // switch leader, after switching leader, the parent of all learners is the new leader
  const int64_t new_leader_idx = diff_region_follower_idx;
  PalfHandleImplGuard new_leader;
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, 0, new_leader));

  while (false == check_children_valid(palf_list, all_learner))
  {
    sleep(1);
    PALF_LOG(INFO, "check_children_valid 5");
  }
  while (false == check_parent(palf_list, all_learner, new_leader.palf_handle_impl_->self_))
  {
    sleep(1);
    PALF_LOG(INFO, "check_parent 3");
  }

  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end test learner", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_config_change_lock)
{
  SET_CASE_LOG_FILE(TEST_NAME, "config_change_lock");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test config change", K(id));
	int64_t leader_idx = 0;
  int64_t log_ts = 1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  loc_cb.leader_ = get_cluster()[leader_idx]->get_addr();
  PALF_LOG(INFO, "set leader for loc_cb", "leader", loc_cb.leader_);
  int64_t lock_owner_out = -1;
  bool lock_stat = false;
  //ASSERT_EQ(OB_NOT_SUPPORTED, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  //ASSERT_EQ(OB_NOT_SUPPORTED, leader.palf_handle_impl_->try_lock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  //ASSERT_EQ(OB_NOT_SUPPORTED, leader.palf_handle_impl_->unlock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  //oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_2_0_0;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(OB_INVALID_CONFIG_CHANGE_LOCK_OWNER, lock_owner_out);
  ASSERT_EQ(false, lock_stat);

  //invalid arguments
  ASSERT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->try_lock_config_change(-1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->try_lock_config_change(1, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->unlock_config_change(-1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->unlock_config_change(1, 0));

  //test try lock
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->try_lock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->try_lock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_TRY_LOCK_CONFIG_CHANGE_CONFLICT, leader.palf_handle_impl_->try_lock_config_change(2, CONFIG_CHANGE_TIMEOUT));

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(1, lock_owner_out);
  ASSERT_EQ(true, lock_stat);

  //test unlock
  ASSERT_EQ(OB_STATE_NOT_MATCH, leader.palf_handle_impl_->unlock_config_change(2, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->unlock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->unlock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(1, lock_owner_out);
  ASSERT_EQ(false, lock_stat);

  //changing stat, test locking stat
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->try_lock_config_change(3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(3, lock_owner_out);
  ASSERT_EQ(true, lock_stat);

  //block add_member

  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));

  //ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->remove_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->remove_member(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->add_member(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->switch_learner_to_acceptor(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->change_replica_num(get_member_list(), 3, 4, CONFIG_CHANGE_TIMEOUT));


  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->unlock_config_change(3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(3, lock_owner_out);
  ASSERT_EQ(false, lock_stat);

  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_STATE_NOT_MATCH, leader.palf_handle_impl_->switch_learner_to_acceptor(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_learner_to_acceptor(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(get_member_list(), 3, 4, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(get_member_list(), 4, 3, CONFIG_CHANGE_TIMEOUT));

  //switch leader

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end test config change lock", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_switch_leader)
{
  SET_CASE_LOG_FILE(TEST_NAME, "switch_leader");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test switch_leader", K(id));
	int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_2_0_0;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
  // try lock config change
  //
  const int64_t follower_idx = (leader_idx+1)%3;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s

  int64_t lock_owner_out = -1;
  bool lock_stat = false;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->try_lock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(1, lock_owner_out);
  ASSERT_EQ(true, lock_stat);

  PalfHandleImplGuard new_leader;
  const int64_t new_leader_idx = (leader_idx+1)%3;
  PalfHandleImplGuard &follower = *palf_list[new_leader_idx];
  ASSERT_EQ(OB_NOT_MASTER, follower.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, new_leader_idx, new_leader));
  sleep(5);
  ObRole role;
  int64_t curr_proposal_id = 0;
  bool is_pending_stat = false;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(role, curr_proposal_id, is_pending_stat));
  EXPECT_EQ(ObRole::FOLLOWER, role);

  lock_owner_out = -1;
  lock_stat = false;

  ASSERT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(1, lock_owner_out);
  ASSERT_EQ(true, lock_stat);
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 200, id));


  ASSERT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->unlock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  sleep(3);
  //switch leader during lock_config_change

  LogConfigChangeArgs args(2, ConfigChangeLockType::LOCK_PAXOS_MEMBER_CHANGE, TRY_LOCK_CONFIG_CHANGE);
  LogConfigVersion config_version;
  const int64_t proposal_id = new_leader.palf_handle_impl_->state_mgr_.get_proposal_id();
  const int64_t leader_epoch = new_leader.palf_handle_impl_->state_mgr_.get_leader_epoch();
  ASSERT_EQ(OB_EAGAIN, new_leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, leader_epoch, config_version));
  ASSERT_EQ(LogConfigMgr::ConfigChangeState::CHANGING, new_leader.palf_handle_impl_->config_mgr_.state_);
  ASSERT_EQ(OB_EAGAIN, new_leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  //block_net
  const int64_t follower_idx1 = (new_leader_idx+1)%3;
  const int64_t follower_idx2 = (new_leader_idx+2)%3;
  block_net(follower_idx1, new_leader_idx, true);
  block_net(follower_idx2, new_leader_idx, true);
  //send info to follower
  ASSERT_EQ(OB_EAGAIN, new_leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, leader_epoch, config_version));
  sleep(1);

  unblock_net(follower_idx1, new_leader_idx);
  unblock_net(follower_idx2, new_leader_idx);
  PalfHandleImplGuard old_leader;
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, leader_idx, old_leader));

  EXPECT_EQ(OB_SUCCESS, old_leader.palf_handle_impl_->get_role(role, curr_proposal_id, is_pending_stat));
  EXPECT_EQ(ObRole::LEADER, role);

  //check config change stat
  sleep(1);
  ASSERT_EQ(OB_SUCCESS, old_leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(2, lock_owner_out);
  ASSERT_EQ(true, lock_stat);


  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end test switch_leader", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
