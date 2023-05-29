// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include <cstdio>
#include <gtest/gtest.h>
#include <signal.h>
#define private public
#include "env/ob_simple_log_cluster_env.h"
#undef private

const std::string TEST_NAME = "arb_mock_ele";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;

namespace logservice
{

void ObArbitrationService::update_arb_timeout_()
{
  arb_timeout_us_ = 2 * 1000 * 1000L;
  if (REACH_TIME_INTERVAL(2 * 1000 * 1000)) {
    CLOG_LOG_RET(WARN, OB_ERR_UNEXPECTED, "update_arb_timeout_", K_(self), K_(arb_timeout_us));
  }
}
}

namespace unittest
{

class TestObSimpleLogClusterArbMockEleService : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterArbMockEleService() :  ObSimpleLogClusterTestEnv()
  {}
  bool is_degraded(const PalfHandleImplGuard &leader,
                  const int64_t degraded_server_idx)
  {
    bool has_degraded = false;
    while (!has_degraded) {
      common::GlobalLearnerList degraded_learner_list;
      leader.palf_handle_impl_->config_mgr_.get_degraded_learner_list(degraded_learner_list);
      has_degraded = degraded_learner_list.contains(get_cluster()[degraded_server_idx]->get_addr());
      sleep(1);
      PALF_LOG(INFO, "wait degrade");
    }
    return has_degraded;
  }

  bool is_upgraded(PalfHandleImplGuard &leader, const int64_t palf_id)
  {
    bool has_upgraded = false;
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, palf_id));
    while (!has_upgraded) {
      EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, palf_id));
      common::GlobalLearnerList degraded_learner_list;
      leader.palf_handle_impl_->config_mgr_.get_degraded_learner_list(degraded_learner_list);
      has_upgraded = (0 == degraded_learner_list.get_member_number());
      sleep(1);
      PALF_LOG(INFO, "wait upgrade");
    }
    return has_upgraded;
  }
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 5;
bool ObSimpleLogClusterTestBase::need_add_arb_server_ = true;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;

TEST_F(TestObSimpleLogClusterArbMockEleService, switch_leader_during_degrading)
{
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t TIMEOUT_US = 10 * 1000 * 1000L;
  SET_CASE_LOG_FILE(TEST_NAME, "switch_leader_during_degrading");
  PALF_LOG(INFO, "begin test switch_leader_during_degrading", K(id));
  {
    int64_t leader_idx = 0;
    int64_t arb_replica_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb_mock_election(id, arb_replica_idx, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

    const int64_t b_idx = (leader_idx + 1) % 3;
    const int64_t c_idx = (leader_idx + 2) % 3;
    const common::ObAddr a_addr = get_cluster()[leader_idx]->get_addr();
    const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
    const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
    PalfHandleImplGuard *a_handle = palf_list[leader_idx];
    PalfHandleImplGuard *b_handle = palf_list[b_idx];
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->stop();
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[b_idx])->log_service_.get_arbitration_service()->stop();

    // 1. A tries to degrade B
    int64_t degrade_b_pid = 0;
    int64_t degrade_b_ele_epoch = 0;
    LogConfigVersion degrade_b_version;
    LogConfigChangeArgs degrade_b_args(ObMember(b_addr, 1), 0, DEGRADE_ACCEPTOR_TO_LEARNER);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(degrade_b_pid, degrade_b_ele_epoch, degrade_b_args.type_));

    while (0 == leader.palf_handle_impl_->config_mgr_.state_) {
      EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(degrade_b_args, degrade_b_pid, degrade_b_ele_epoch, degrade_b_version));
    }
    const LSN &leader_last_committed_end_lsn = leader.palf_handle_impl_->sw_.committed_end_lsn_;
    sleep(2);
    EXPECT_EQ(leader_last_committed_end_lsn, leader.palf_handle_impl_->sw_.committed_end_lsn_);

    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.config_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(b_handle->palf_handle_impl_->config_mgr_.config_meta_.curr_.log_sync_memberlist_.contains(b_addr));

    // 2. the leader A revokes and takeover again
    for (auto srv: get_cluster()) {
      const ObAddr addr1(ObAddr::IPV4, "0.0.0.0", 0);
      srv->set_leader(id, addr1);
    }
    EXPECT_UNTIL_EQ(false, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // A can not degrades B successfully
    EXPECT_EQ(OB_NOT_MASTER, a_handle->palf_handle_impl_->config_mgr_.change_config_(degrade_b_args, degrade_b_pid, degrade_b_ele_epoch, degrade_b_version));

    for (auto srv: get_cluster()) {
      srv->set_leader(id, a_addr, 3);
    }
    EXPECT_UNTIL_EQ(true, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // 3. leader A degrades B again
    leader.reset();
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_LE(leader_last_committed_end_lsn, leader.palf_handle_impl_->sw_.committed_end_lsn_);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->one_stage_config_change_(degrade_b_args, TIMEOUT_US));
    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.config_meta_.curr_.log_sync_memberlist_.contains(b_addr));

    // 4. leader A tries to upgrade B
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    int64_t upgrade_b_pid = 0;
    int64_t upgrade_b_ele_epoch = 0;
    LogConfigVersion upgrade_b_version;
    LogConfigChangeArgs upgrade_b_args(ObMember(b_addr, 1), 0, UPGRADE_LEARNER_TO_ACCEPTOR);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(upgrade_b_pid, upgrade_b_ele_epoch, upgrade_b_args.type_));

    while (0 == leader.palf_handle_impl_->config_mgr_.state_) {
      leader.palf_handle_impl_->config_mgr_.change_config_(upgrade_b_args, upgrade_b_pid, upgrade_b_ele_epoch, upgrade_b_version);
    }
    const LSN &leader_max_flushed_end_lsn = leader.palf_handle_impl_->sw_.max_flushed_end_lsn_;
    EXPECT_EQ(leader.palf_handle_impl_->sw_.max_flushed_end_lsn_, b_handle->palf_handle_impl_->sw_.max_flushed_end_lsn_);

    // 5. the leader A revokes and takeover again
    for (auto srv: get_cluster()) {
      const ObAddr addr1(ObAddr::IPV4, "0.0.0.0", 0);
      srv->set_leader(id, addr1);
    }
    EXPECT_UNTIL_EQ(false, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // A can not upgrades B successfully
    EXPECT_EQ(OB_NOT_MASTER, leader.palf_handle_impl_->config_mgr_.change_config_(upgrade_b_args, upgrade_b_pid, upgrade_b_ele_epoch, upgrade_b_version));

    for (auto srv: get_cluster()) {
      srv->set_leader(id, a_addr, 3);
    }
    EXPECT_UNTIL_EQ(true, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.config_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_EQ(leader_max_flushed_end_lsn, leader.palf_handle_impl_->sw_.committed_end_lsn_);

    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test switch_leader_during_degrading", K(id));
}

TEST_F(TestObSimpleLogClusterArbMockEleService, switch_leader_to_other_during_degrading)
{
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t TIMEOUT_US = 10 * 1000 * 1000L;
  SET_CASE_LOG_FILE(TEST_NAME, "switch_leader_to_other_during_degrading");
  PALF_LOG(INFO, "begin test switch_leader_to_other_during_degrading", K(id));
  {
    int64_t leader_idx = 0;
    int64_t arb_replica_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb_mock_election(id, arb_replica_idx, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

    const int64_t b_idx = (leader_idx + 1) % 3;
    const int64_t c_idx = (leader_idx + 2) % 3;
    const common::ObAddr a_addr = get_cluster()[leader_idx]->get_addr();
    const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
    const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
    PalfHandleImplGuard *a_handle = palf_list[leader_idx];
    PalfHandleImplGuard *b_handle = palf_list[b_idx];
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->stop();
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[b_idx])->log_service_.get_arbitration_service()->stop();

    // 1. A tries to degrade B
    int64_t degrade_b_pid = 0;
    int64_t degrade_b_ele_epoch = 0;
    LogConfigVersion degrade_b_version;
    LogConfigChangeArgs degrade_b_args(ObMember(b_addr, 1), 0, DEGRADE_ACCEPTOR_TO_LEARNER);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(degrade_b_pid, degrade_b_ele_epoch, degrade_b_args.type_));

    while (0 == leader.palf_handle_impl_->config_mgr_.state_) {
      EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(degrade_b_args, degrade_b_pid, degrade_b_ele_epoch, degrade_b_version));
    }
    const LSN &leader_last_committed_end_lsn = leader.palf_handle_impl_->sw_.committed_end_lsn_;
    sleep(2);
    EXPECT_EQ(leader_last_committed_end_lsn, leader.palf_handle_impl_->sw_.committed_end_lsn_);

    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.config_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(b_handle->palf_handle_impl_->config_mgr_.config_meta_.curr_.log_sync_memberlist_.contains(b_addr));

    // 2. the leader A revokes and B takeover
    for (auto srv: get_cluster()) {
      const ObAddr addr1(ObAddr::IPV4, "0.0.0.0", 0);
      srv->set_leader(id, addr1);
    }
    EXPECT_UNTIL_EQ(false, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // A can not degrades B successfully
    EXPECT_EQ(OB_NOT_MASTER, a_handle->palf_handle_impl_->config_mgr_.change_config_(degrade_b_args, degrade_b_pid, degrade_b_ele_epoch, degrade_b_version));

    for (auto srv: get_cluster()) {
      srv->set_leader(id, b_addr, 3);
    }
    EXPECT_UNTIL_EQ(true, b_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // 3. leader B degrades A
    leader.reset();
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.config_meta_.curr_.log_sync_memberlist_.contains(a_addr));
    EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.config_meta_.curr_.log_sync_memberlist_.contains(a_addr));

    LogConfigChangeArgs degrade_a_args(ObMember(a_addr, 1), 0, DEGRADE_ACCEPTOR_TO_LEARNER);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->one_stage_config_change_(degrade_a_args, TIMEOUT_US));

    // 4. leader B tries to upgrade A
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    int64_t upgrade_a_pid = 0;
    int64_t upgrade_a_ele_epoch = 0;
    LogConfigVersion upgrade_a_version;
    LogConfigChangeArgs upgrade_a_args(ObMember(a_addr, 1), 0, UPGRADE_LEARNER_TO_ACCEPTOR);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(upgrade_a_pid, upgrade_a_ele_epoch, upgrade_a_args.type_));

    while (0 == leader.palf_handle_impl_->config_mgr_.state_) {
      leader.palf_handle_impl_->config_mgr_.change_config_(upgrade_a_args, upgrade_a_pid, upgrade_a_ele_epoch, upgrade_a_version);
    }
    EXPECT_EQ(a_handle->palf_handle_impl_->sw_.max_flushed_end_lsn_, b_handle->palf_handle_impl_->sw_.max_flushed_end_lsn_);
    EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(upgrade_a_args, upgrade_a_pid, upgrade_a_ele_epoch, upgrade_a_version));
    EXPECT_UNTIL_EQ(a_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_version_, b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_version_);

    // 5. the leader B revokes and A takeover
    for (auto srv: get_cluster()) {
      const ObAddr addr1(ObAddr::IPV4, "0.0.0.0", 0);
      srv->set_leader(id, addr1);
    }
    // A can not upgrades B successfully
    EXPECT_EQ(OB_NOT_MASTER, leader.palf_handle_impl_->config_mgr_.change_config_(upgrade_a_args, upgrade_a_pid, upgrade_a_ele_epoch, upgrade_a_version));
    EXPECT_UNTIL_EQ(false, leader.palf_handle_impl_->state_mgr_.is_leader_active());

    for (auto srv: get_cluster()) {
      srv->set_leader(id, a_addr, 3);
    }
    EXPECT_UNTIL_EQ(true, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    EXPECT_TRUE(a_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(a_handle->palf_handle_impl_->config_mgr_.config_meta_.curr_.log_sync_memberlist_.contains(b_addr));

    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test switch_leader_to_other_during_degrading", K(id));
}


} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}