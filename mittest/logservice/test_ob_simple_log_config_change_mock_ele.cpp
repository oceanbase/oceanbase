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

const std::string TEST_NAME = "config_change_mock_ele";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class TestObSimpleLogClusterConfigChangeMockEle : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterConfigChangeMockEle() :  ObSimpleLogClusterTestEnv()
  {}
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 7;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

MockLocCB loc_cb;

// switch leader after appending config log
TEST_F(TestObSimpleLogClusterConfigChangeMockEle, switch_leader_during_removing_member1)
{
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  SET_CASE_LOG_FILE(TEST_NAME, "switch_leader_during_removing_member1");
  PALF_LOG(INFO, "begin test switch_leader_during_removing_member1", K(id));
  {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_mock_election(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

    const int64_t b_idx = (leader_idx + 1) % 3;
    const int64_t c_idx = (leader_idx + 2) % 3;
    PalfHandleImplGuard *a_handle = palf_list[leader_idx];
    PalfHandleImplGuard *b_handle = palf_list[b_idx];
    PalfHandleImplGuard *c_handle = palf_list[c_idx];

    // 1. A starts to remove B
    int64_t remove_b_pid = 0;
    int64_t remove_b_ele_epoch = 0;
    LogConfigVersion remove_b_version;
    const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
    const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
    LogConfigChangeArgs args(common::ObMember(b_addr, 1), 2, REMOVE_MEMBER);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(remove_b_pid, remove_b_ele_epoch, args.type_));
    EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(args, remove_b_pid, remove_b_ele_epoch, remove_b_version));

    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(c_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));

    // 2. B is elected to be the leader
    block_net(leader_idx, b_idx);
    for (auto srv: get_cluster()) {
      srv->set_leader(id, b_addr);
    }
    EXPECT_UNTIL_EQ(true, b_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // 3. A can not remove B successfully
    EXPECT_EQ(OB_NOT_MASTER, leader.palf_handle_impl_->config_mgr_.change_config_(args, remove_b_pid, remove_b_ele_epoch, remove_b_version));
    unblock_net(leader_idx, b_idx);

    // 4. A's memberlist will be reset
    EXPECT_UNTIL_EQ(true, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    leader.reset();

    // 5. B remove C
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    LogConfigChangeArgs remove_c_args(common::ObMember(c_addr, 1), 2, REMOVE_MEMBER);
    int64_t remove_c_pid = 0;
    int64_t remove_c_ele_epoch = 0;
    LogConfigVersion remove_c_version;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(remove_c_pid, remove_c_ele_epoch, remove_c_args.type_));
    EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(remove_c_args, remove_c_pid, remove_c_ele_epoch, remove_c_version));

    EXPECT_TRUE(a_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(c_addr));
    EXPECT_FALSE(b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(c_addr));
    EXPECT_TRUE(c_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(c_addr));

    // 6. the leader B revokes and takeover again
    for (auto srv: get_cluster()) {
      const ObAddr addr1(ObAddr::IPV4, "0.0.0.0", 0);
      srv->set_leader(id, addr1);
    }
    EXPECT_UNTIL_EQ(false, b_handle->palf_handle_impl_->state_mgr_.is_leader_active());
    for (auto srv: get_cluster()) {
      srv->set_leader(id, b_addr, 3);
    }
    EXPECT_UNTIL_EQ(true, b_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // 7. C will be removed successfully
    EXPECT_FALSE(a_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(c_addr));
    EXPECT_FALSE(b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(c_addr));

    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test switch_leader_during_removing_member1", K(id));
}

// switch leader after sending config meta to followers
TEST_F(TestObSimpleLogClusterConfigChangeMockEle, switch_leader_during_removing_member2)
{
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  SET_CASE_LOG_FILE(TEST_NAME, "switch_leader_during_removing_member2");
  PALF_LOG(INFO, "begin test switch_leader_during_removing_member2", K(id));
  {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_mock_election(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

    const int64_t b_idx = (leader_idx + 1) % 3;
    const int64_t c_idx = (leader_idx + 2) % 3;
    PalfHandleImplGuard *b_handle = palf_list[b_idx];
    PalfHandleImplGuard *c_handle = palf_list[c_idx];

    // 1. A starts to remove B
    int64_t remove_b_pid = 0;
    int64_t remove_b_ele_epoch = 0;
    LogConfigVersion remove_b_version;
    const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
    LogConfigChangeArgs args(common::ObMember(b_addr, 1), 2, REMOVE_MEMBER);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(remove_b_pid, remove_b_ele_epoch, args.type_));
    EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(args, remove_b_pid, remove_b_ele_epoch, remove_b_version));

    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(c_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));

    // 1. A sends config meta to C
    block_net(leader_idx, b_idx);
    while (-1 == leader.palf_handle_impl_->config_mgr_.last_submit_config_log_time_us_ ||
           c_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr)) {
      EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(args, remove_b_pid, remove_b_ele_epoch, remove_b_version));
      usleep(500);
    }

    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_UNTIL_EQ(true, b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_UNTIL_EQ(false, c_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));

    // 2. B is elected to be the leader
    for (auto srv: get_cluster()) {
      srv->set_leader(id, b_addr);
    }
    EXPECT_UNTIL_EQ(true, b_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // 3. A can not remove B successfully
    EXPECT_EQ(OB_NOT_MASTER, leader.palf_handle_impl_->config_mgr_.change_config_(args, remove_b_pid, remove_b_ele_epoch, remove_b_version));
    unblock_net(leader_idx, b_idx);

    // 4. A's memberlist will be reset
    EXPECT_UNTIL_EQ(true, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));

    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test switch_leader_during_removing_member2", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChangeMockEle, switch_leader_during_removing_member3)
{
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  SET_CASE_LOG_FILE(TEST_NAME, "switch_leader_during_removing_member3");
  PALF_LOG(INFO, "begin test switch_leader_during_removing_member3", K(id));
  {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_mock_election(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

    const int64_t b_idx = (leader_idx + 1) % 3;
    const int64_t c_idx = (leader_idx + 2) % 3;
    PalfHandleImplGuard *a_handle = palf_list[leader_idx];
    PalfHandleImplGuard *b_handle = palf_list[b_idx];
    PalfHandleImplGuard *c_handle = palf_list[c_idx];

    // 1. A starts to remove B
    int64_t remove_b_pid = 0;
    int64_t remove_b_ele_epoch = 0;
    LogConfigVersion remove_b_version;
    const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
    const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
    LogConfigChangeArgs args(common::ObMember(b_addr, 1), 2, REMOVE_MEMBER);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(remove_b_pid, remove_b_ele_epoch, args.type_));
    EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(args, remove_b_pid, remove_b_ele_epoch, remove_b_version));

    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(c_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));

    // A sends config meta to C
    block_net(leader_idx, b_idx);
    while (-1 == leader.palf_handle_impl_->config_mgr_.last_submit_config_log_time_us_ ||
           c_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr)) {
      EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(args, remove_b_pid, remove_b_ele_epoch, remove_b_version));
      usleep(500);
    }

    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_UNTIL_EQ(true, b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_UNTIL_EQ(false, c_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));

    // 2. C is elected to be the leader
    for (auto srv: get_cluster()) {
      srv->set_leader(id, c_addr);
    }
    EXPECT_UNTIL_EQ(true, c_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // 3. A can not remove B successfully
    EXPECT_EQ(OB_NOT_MASTER, leader.palf_handle_impl_->config_mgr_.change_config_(args, remove_b_pid, remove_b_ele_epoch, remove_b_version));
    unblock_net(leader_idx, b_idx);

    // 4. A's memberlist will be re-sync by C successfully
    EXPECT_UNTIL_EQ(false, a_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_UNTIL_EQ(false, c_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));

    leader.reset();
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test switch_leader_during_removing_member3", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}