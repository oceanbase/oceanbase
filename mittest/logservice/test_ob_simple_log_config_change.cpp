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

    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[follower_D_idx]->get_addr(), 1), 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    // step 4
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(get_cluster()[follower_C_idx]->get_addr(), 1), 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, get_cluster()[follower_C_idx]->get_palf_env()->remove_palf_handle_impl(id));
    // step 5
    EXPECT_EQ(OB_SUCCESS, get_cluster()[follower_B_idx]->simple_init(get_test_name(), follower_b_addr, follower_B_idx, false));
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
    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());

    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[remove_follower_idx]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    block_net(leader_idx, lag_follower_idx);

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    sleep(3);

    // add member without any logs
    // we will send config log to 3 before add_member(3, 4), so 3 can know who is leader and fetch log from it.
    // so just comment this line, after leader forbit fetch log req that comes from node who is not follower or
    // children of leader, we will uncomment this line
    // EXPECT_EQ(OB_TIMEOUT, leader.add_member(ObMember(palf_list[3]->palf_handle_.palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT / 2));
    unblock_net(leader_idx, lag_follower_idx);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));

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
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[4]->get_addr(), 1), 5, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[5]->get_addr(), 1), 5, CONFIG_CHANGE_TIMEOUT));
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
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[(leader_idx+2)%3]->palf_handle_impl_->self_, 1), 1, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+2)%3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // add member when contains log
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), 5, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // remove member
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // replace member
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member(ObMember(palf_list[5]->palf_handle_impl_->self_, 1),
                                               ObMember(palf_list[3]->palf_handle_impl_->self_, 1),
                                               CONFIG_CHANGE_TIMEOUT));

    // switch acceptor to learner
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(palf_list[5]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));
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
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[(leader_idx+2)%3]->palf_handle_impl_->self_, 1), 1, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+2)%3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // add member when contains log
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), 5, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // remove member
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // replace member
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member(ObMember(palf_list[5]->palf_handle_impl_->self_, 1),
                                               ObMember(palf_list[3]->palf_handle_impl_->self_, 1),
                                               CONFIG_CHANGE_TIMEOUT));

    // switch acceptor to learner
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(palf_list[5]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));
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

// TODO: config_mgr need support location_cb to get leader for learner
/*
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
  const ObMemberList &node_list = get_node_list();
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  PalfHandleGuard new_leader;
  int64_t new_leader_idx;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
  PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());

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
  }
  // change region of one follower
  bool has_change_region = false;
  int64_t another_follower_idx = -1;
  for (int i = 0; i < ObSimpleLogClusterTestBase::member_cnt_; i++) {
    const bool not_leader = palf_list[i]->palf_handle_impl_->self_ != leader.palf_handle_impl_->self_;
    if (!has_change_region && not_leader) {
      EXPECT_EQ(OB_SUCCESS, palf_list[i]->palf_handle_impl_->set_region(region_list[0]));
      region_map.insert(palf_list[i]->palf_handle_impl_->self_, region_list[0]);
      has_change_region = true;
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
  }
  // after setting region of a follower, parents of all learners should be another follower
  EXPECT_GE(another_follower_idx, 0);
  EXPECT_LE(another_follower_idx, 2);
  ObAddr curr_parent = palf_list[another_follower_idx]->palf_handle_impl_->self_;
  while (false == check_parent(palf_list, all_learner, curr_parent))
  {
    sleep(1);
  }
  // continue submitting log
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, id));
  PALF_LOG(INFO, "all_learner", K(all_learner));
  // EXPECT_EQ(OB_SUCCESS, check_log_sync(palf_list, get_member_list(), all_learner, leader));

  // switch current unique parent to learner
  EXPECT_EQ(OB_SUCCESS, all_learner.add_learner(LogLearner(curr_parent, 1)));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(curr_parent, 1), CONFIG_CHANGE_TIMEOUT));
  // after switch follower 1 to learner, a learner will be registered to leader, and other learners will
  // be registerd to this learner
  while (false == check_children_valid(palf_list, all_learner))
  {
    sleep(1);
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
  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end test learner", K(id));
  // TODO by yunlong:: after mit test supports sync RPC and switch_leader func become stable,
  // add switch_acceptor_to_learner case and switch_leader case
  // // case 2: switch leader
  // // learners' regions are different from paxos member, so parent of all learners is leader
  // // set regions
  // for (int64_t i = 3; i < 7; ++i) {
  //   PalfHandleImplGuard tmp_handle;
  //   common::ObMember added_learner;
  //   EXPECT_EQ(OB_SUCCESS, node_list.get_member_by_index(i, added_learner));
  //   EXPECT_EQ(OB_SUCCESS, all_learner.add_learner(LogLearner(added_learner.get_server(), region_list[i - 3])));
  //   EXPECT_EQ(OB_SUCCESS, get_palf_handle_guard(id, added_learner.get_server(), tmp_handle));
  //   EXPECT_EQ(OB_SUCCESS, tmp_handle.set_region(region_list[i-3]));
  //   EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_learner, CONFIG_CHANGE_TIMEOUT));
  // }
  // sleep(1);
  // // check children_cnt
  // check_children_valid(palf_list, all_learner);
  // check_parent(palf_list, leader.palf_handle_impl_->self_);
  // // switch leader
  // const int64_t new_leader_idx = 1;
  // PalfHandleImplGuard new_leader;
  // switch_leader(id, new_leader_idx, new_leader);
  // sleep(5);
  // check_children_valid(palf_list, all_learner);
  // check_parent(palf_list, new_leader.palf_handle_impl_->self_);
}
*/

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
