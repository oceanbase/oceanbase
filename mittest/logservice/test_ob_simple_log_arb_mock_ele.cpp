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
int64_t global_timeous_us = 2 * 1000 * 1000L;

void ObArbitrationService::update_arb_timeout_()
{
  arb_timeout_us_ = global_timeous_us;
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

    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));

    // 2. the leader A revokes and takeover again
    for (auto srv: get_cluster()) {
      const ObAddr addr1(ObAddr::IPV4, "0.0.0.0", 0);
      srv->set_leader(id, addr1);
    }
    EXPECT_UNTIL_EQ(false, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // A can not degrades B successfully
    EXPECT_EQ(OB_NOT_MASTER, a_handle->palf_handle_impl_->config_mgr_.change_config_(degrade_b_args, degrade_b_pid, degrade_b_ele_epoch, degrade_b_version));
    a_handle->palf_handle_impl_->config_mgr_.end_degrade();

    for (auto srv: get_cluster()) {
      srv->set_leader(id, a_addr, 3);
    }
    EXPECT_UNTIL_EQ(true, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // 3. leader A degrades B again
    leader.reset();
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_LE(leader_last_committed_end_lsn, leader.palf_handle_impl_->sw_.committed_end_lsn_);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->one_stage_config_change_(degrade_b_args, TIMEOUT_US));
    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));

    // 4. leader A tries to upgrade B
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    int64_t upgrade_b_pid = 0;
    int64_t upgrade_b_ele_epoch = 0;
    LogConfigVersion upgrade_b_version;
    LogConfigChangeArgs upgrade_b_args(ObMember(b_addr, 1), 0, UPGRADE_LEARNER_TO_ACCEPTOR);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(upgrade_b_pid, upgrade_b_ele_epoch, upgrade_b_args.type_));

    TimeoutChecker not_timeout(TIMEOUT_US);
    bool is_already_finished = false;
    LogConfigInfoV2 new_config_info;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->check_args_and_generate_config_(upgrade_b_args, upgrade_b_pid, upgrade_b_ele_epoch,
      is_already_finished, new_config_info));
    EXPECT_UNTIL_EQ(OB_SUCCESS, leader.palf_handle_impl_->wait_log_barrier_(upgrade_b_args, new_config_info, not_timeout));

    const LSN &leader_max_flushed_end_lsn = leader.palf_handle_impl_->sw_.max_flushed_end_lsn_;
    EXPECT_GT(leader.palf_handle_impl_->sw_.max_flushed_end_lsn_, leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_lsn_);
    EXPECT_GT(b_handle->palf_handle_impl_->sw_.max_flushed_end_lsn_, leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_lsn_);
    while (0 == leader.palf_handle_impl_->config_mgr_.state_) {
      leader.palf_handle_impl_->config_mgr_.change_config_(upgrade_b_args, upgrade_b_pid, upgrade_b_ele_epoch, upgrade_b_version);
    }

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

    EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));
    EXPECT_EQ(leader_max_flushed_end_lsn, leader.palf_handle_impl_->sw_.committed_end_lsn_);

    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->start();
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[b_idx])->log_service_.get_arbitration_service()->start();
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

    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));

    // 2. the leader A revokes and B takeover
    for (auto srv: get_cluster()) {
      const ObAddr addr1(ObAddr::IPV4, "0.0.0.0", 0);
      srv->set_leader(id, addr1);
    }
    EXPECT_UNTIL_EQ(false, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // A can not degrades B successfully
    EXPECT_EQ(OB_NOT_MASTER, a_handle->palf_handle_impl_->config_mgr_.change_config_(degrade_b_args, degrade_b_pid, degrade_b_ele_epoch, degrade_b_version));
    a_handle->palf_handle_impl_->config_mgr_.end_degrade();

    for (auto srv: get_cluster()) {
      srv->set_leader(id, b_addr, 3);
    }
    EXPECT_UNTIL_EQ(true, b_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // 3. leader B degrades A
    leader.reset();
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));
    EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));

    LogConfigChangeArgs degrade_a_args(ObMember(a_addr, 1), 0, DEGRADE_ACCEPTOR_TO_LEARNER);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->one_stage_config_change_(degrade_a_args, TIMEOUT_US));

    // 4. leader B tries to upgrade A
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    int64_t upgrade_a_pid = 0;
    int64_t upgrade_a_ele_epoch = 0;
    LogConfigVersion upgrade_a_version;
    LogConfigChangeArgs upgrade_a_args(ObMember(a_addr, 1), 0, UPGRADE_LEARNER_TO_ACCEPTOR);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(upgrade_a_pid, upgrade_a_ele_epoch, upgrade_a_args.type_));

    TimeoutChecker not_timeout(TIMEOUT_US);
    bool is_already_finished = false;
    LogConfigInfoV2 new_config_info;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->check_args_and_generate_config_(upgrade_a_args, upgrade_a_pid, upgrade_a_ele_epoch,
      is_already_finished, new_config_info));
    EXPECT_UNTIL_EQ(OB_SUCCESS, leader.palf_handle_impl_->wait_log_barrier_(upgrade_a_args, new_config_info, not_timeout));
    EXPECT_GT(leader.palf_handle_impl_->sw_.max_flushed_end_lsn_, leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_lsn_);
    EXPECT_GT(b_handle->palf_handle_impl_->sw_.max_flushed_end_lsn_, leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_lsn_);

    while (0 == leader.palf_handle_impl_->config_mgr_.state_) {
      leader.palf_handle_impl_->config_mgr_.change_config_(upgrade_a_args, upgrade_a_pid, upgrade_a_ele_epoch, upgrade_a_version);
    }
    EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(upgrade_a_args, upgrade_a_pid, upgrade_a_ele_epoch, upgrade_a_version));
    EXPECT_UNTIL_EQ(a_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.config_version_, b_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.config_version_);

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

    EXPECT_TRUE(a_handle->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));

    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->start();
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[b_idx])->log_service_.get_arbitration_service()->start();
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test switch_leader_to_other_during_degrading", K(id));
}

// 1. 2F1A, the leader starts to degrade another F
// 2. after the config log has been accepted by the arb member, the leader revoked
// 3. the previous leader has been elected as the new leader
// 4. reconfirm may fail because leader's config_version is not same to that of the follower
TEST_F(TestObSimpleLogClusterArbMockEleService, test_2f1a_degrade_when_no_leader2)
{
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t TIMEOUT_US = 10 * 1000 * 1000L;
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_degrade_when_no_leader2");
  PALF_LOG(INFO, "begin test test_2f1a_degrade_when_no_leader2", K(id));
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
    IPalfHandleImpl *c_ihandle = NULL;
    get_cluster()[c_idx]->get_palf_env()->get_palf_handle_impl(id, c_ihandle);
    palflite::PalfHandleLite *c_handle = dynamic_cast<palflite::PalfHandleLite*>(c_ihandle);
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->stop();
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[b_idx])->log_service_.get_arbitration_service()->stop();

    // 1. A tries to degrade B
    block_net(leader_idx, b_idx);
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

    // A sends config meta to C
    while (-1 == leader.palf_handle_impl_->config_mgr_.last_submit_config_log_time_us_ ||
           c_handle->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr)) {
      EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(degrade_b_args, degrade_b_pid, degrade_b_ele_epoch, degrade_b_version));
      usleep(500);
    }

    EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));

    // 2. the leader A revokes and takeover again
    for (auto srv: get_cluster()) {
      const ObAddr addr1(ObAddr::IPV4, "0.0.0.0", 0);
      srv->set_leader(id, addr1);
    }
    EXPECT_UNTIL_EQ(false, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    // A can not degrades B successfully
    EXPECT_EQ(OB_NOT_MASTER, a_handle->palf_handle_impl_->config_mgr_.change_config_(degrade_b_args, degrade_b_pid, degrade_b_ele_epoch, degrade_b_version));
    a_handle->palf_handle_impl_->config_mgr_.end_degrade();

    for (auto srv: get_cluster()) {
      srv->set_leader(id, a_addr, 3);
    }
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->start();
    EXPECT_UNTIL_EQ(true, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());
    EXPECT_UNTIL_EQ(true, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.contains(b_addr));

    unblock_net(leader_idx, b_idx);
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[b_idx])->log_service_.get_arbitration_service()->start();
    get_cluster()[c_idx]->get_palf_env()->revert_palf_handle_impl(c_ihandle);
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test test_2f1a_degrade_when_no_leader2", K(id));
}

TEST_F(TestObSimpleLogClusterArbMockEleService, test_2f1a_change_config_fail)
{
  OB_LOGGER.set_log_level("INFO");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t TIMEOUT_US = 10 * 1000 * 1000L;
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_change_config_fail");
  PALF_LOG(INFO, "begin test test_2f1a_change_config_fail", K(id));
  {
    int64_t leader_idx = 0;
    int64_t arb_replica_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb_mock_election(id, arb_replica_idx, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->stop();

    const int64_t b_idx = (leader_idx + 1) % 4;
    const int64_t c_idx = (leader_idx + 2) % 4;
    const int64_t d_idx = (leader_idx + 3) % 4;
    const common::ObAddr a_addr = get_cluster()[leader_idx]->get_addr();
    const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
    const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
    const common::ObAddr d_addr = get_cluster()[d_idx]->get_addr();
    PalfHandleImplGuard *a_handle = palf_list[leader_idx];
    PalfHandleImplGuard *b_handle = palf_list[b_idx];
    PalfHandleImplGuard *d_handle = palf_list[d_idx];

    LogConfigVersion config_version;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    LogConfigChangeArgs add_d_arg(common::ObMember(d_addr, 1), 4, config_version, ADD_MEMBER);
    int64_t add_d_pid = 0;
    int64_t add_d_epoch = 0;
    LogConfigVersion add_d_version;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(add_d_pid, add_d_epoch, add_d_arg.type_));

    // block the network from the leader to the follower
    block_net(leader_idx, d_idx);
    TimeoutChecker not_timeout(TIMEOUT_US);
    bool is_already_finished = false;
    bool has_new_version = true;
    LogConfigInfoV2 new_config_info;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->check_args_and_generate_config_(add_d_arg, add_d_pid, add_d_epoch,
      is_already_finished, new_config_info));
    EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.check_follower_sync_status(add_d_arg, new_config_info, has_new_version));
    EXPECT_FALSE(has_new_version);
    EXPECT_EQ(OB_LOG_NOT_SYNC, leader.palf_handle_impl_->wait_log_barrier_(add_d_arg, new_config_info, not_timeout));
    // EXPECT_UNTIL_EQ(OB_LOG_NOT_SYNC, leader.palf_handle_impl_->config_mgr_.change_config_(add_d_arg, add_d_pid, add_d_epoch, add_d_version));
    // EXPECT_FALSE(add_d_version.is_valid());

    unblock_net(leader_idx, d_idx);
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->start();
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test test_2f1a_change_config_fail", K(id));
}

TEST_F(TestObSimpleLogClusterArbMockEleService, test_2f1a_degrade_when_arb_crash)
{
  OB_LOGGER.set_log_level("INFO");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t TIMEOUT_US = 10 * 1000 * 1000L;
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_degrade_when_arb_crash");
  PALF_LOG(INFO, "begin test test_2f1a_degrade_when_arb_crash", K(id));
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

    // block the network from the leader to the arb member
    block_net(leader_idx, arb_replica_idx);
    // block the network from the leader to the follower
    block_net(leader_idx, b_idx);
    sleep(4);
    // the leader can not degrade successfully
    EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));

    // start to degrade B manually, do not allow
    LogConfigChangeArgs args(ObMember(b_addr, 1), 0, DEGRADE_ACCEPTOR_TO_LEARNER);
    EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->one_stage_config_change_(args, 10 * 1000 * 1000));
    EXPECT_EQ(false, leader.palf_handle_impl_->config_mgr_.is_sw_interrupted_by_degrade_);
    EXPECT_EQ(false, leader.palf_handle_impl_->state_mgr_.is_changing_config_with_arb_);

    // start to degrade B manually, do not allow
    int64_t degrade_b_pid = 0;
    int64_t degrade_b_ele_epoch = 0;
    LogConfigVersion degrade_b_version;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(degrade_b_pid, degrade_b_ele_epoch, args.type_));
    EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config(args, degrade_b_pid, degrade_b_ele_epoch, degrade_b_version));
    EXPECT_EQ(true, leader.palf_handle_impl_->config_mgr_.is_sw_interrupted_by_degrade_);
    // EXPECT_EQ(true, leader.palf_handle_impl_->state_mgr_.is_changing_config_with_arb_);

    unblock_net(leader_idx, arb_replica_idx);
    unblock_net(leader_idx, b_idx);
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test test_2f1a_degrade_when_arb_crash", K(id));
}

TEST_F(TestObSimpleLogClusterArbMockEleService, test_arb_degrade_probe)
{
  int ret = OB_SUCCESS;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L;
  OB_LOGGER.set_log_level("TRACE");
  SET_CASE_LOG_FILE(TEST_NAME, "test_arb_degrade_probe");
  PALF_LOG(INFO, "begin test test_arb_degrade_probe", K(id));
  {
    int64_t leader_idx = 0;
    int64_t arb_replica_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb_mock_election(id, arb_replica_idx, leader_idx, leader));
    LogConfigVersion config_version;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[3]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[4]->get_addr(), 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->start();

    const int64_t b_idx = (leader_idx + 1) % 5;
    const int64_t c_idx = (leader_idx + 2) % 5;
    const int64_t d_idx = (leader_idx + 3) % 5;
    const int64_t e_idx = (leader_idx + 4) % 5;
    const common::ObAddr a_addr = get_cluster()[leader_idx]->get_addr();
    const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
    const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
    const common::ObAddr d_addr = get_cluster()[d_idx]->get_addr();
    const common::ObAddr e_addr = get_cluster()[e_idx]->get_addr();
    PalfHandleImplGuard *a_handle = palf_list[leader_idx];
    PalfHandleImplGuard *b_handle = palf_list[b_idx];

    // CASE 1. D and E UNKNOWN, do not degrade
    block_pcode(d_idx, ObRpcPacketCode::OB_LOG_ARB_PROBE_MSG);
    block_pcode(e_idx, ObRpcPacketCode::OB_LOG_ARB_PROBE_MSG);
    EXPECT_TRUE(dynamic_cast<ObSimpleLogServer*>(get_cluster()[d_idx])->deliver_.need_filter_packet_by_pcode_blacklist(ObRpcPacketCode::OB_LOG_ARB_PROBE_MSG));
    EXPECT_TRUE(dynamic_cast<ObSimpleLogServer*>(get_cluster()[e_idx])->deliver_.need_filter_packet_by_pcode_blacklist(ObRpcPacketCode::OB_LOG_ARB_PROBE_MSG));
    sleep(6);
    EXPECT_EQ(0, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.get_member_number());

    // CASE 2. D block_net, E UNKNOWN, do not degrade
    block_net(leader_idx, d_idx);
    sleep(2);
    EXPECT_EQ(0, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.get_member_number());

    // CASE 3. D block_net, E block_net, degrade
    block_net(leader_idx, e_idx);
    EXPECT_TRUE(is_degraded(leader, d_idx));
    EXPECT_TRUE(is_degraded(leader, e_idx));

    // CASE 4. D and E unblock_net but block_pcode, do not upgrade
    unblock_net(leader_idx, d_idx);
    unblock_net(leader_idx, e_idx);
    sleep(2);
    EXPECT_EQ(2, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.get_member_number());

    // CASE 5. D unblock_net and unblock_pcode, upgrade E
    unblock_pcode(d_idx, ObRpcPacketCode::OB_LOG_ARB_PROBE_MSG);
    EXPECT_UNTIL_EQ(false, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.contains(d_addr));

    // CASE 6. E unblock_net and unblock_pcode, upgrade E
    unblock_pcode(e_idx, ObRpcPacketCode::OB_LOG_ARB_PROBE_MSG);
    EXPECT_UNTIL_EQ(false, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.contains(d_addr));

    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test test_arb_degrade_probe", K(id));
}

// 4F1A
// 1. A reconfiguration (add D) has been executed successfully with log_barrier 100
// 2. The leader A submits logs and commits logs
// 3. Another reconfiguration (remove D) advances log_barrier to 200, but has not been executed
// 4. A and B may commit logs in (100, 200) with previous memberlist (A, B, C)
// 5. A and B crash, new leader's committed_end_lsn is smaller than that of A
TEST_F(TestObSimpleLogClusterArbMockEleService, test_add_remove_lose_logs)
{
  int ret = OB_SUCCESS;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L;
  OB_LOGGER.set_log_level("TRACE");
  SET_CASE_LOG_FILE(TEST_NAME, "test_add_remove_lose_logs");
  PALF_LOG(INFO, "begin test test_add_remove_lose_logs", K(id));
  {
    int64_t leader_idx = 0;
    int64_t arb_replica_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb_mock_election(id, arb_replica_idx, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    LogConfigVersion config_version;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[3]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[4]->get_addr(), 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->stop();

    const int64_t b_idx = (leader_idx + 1) % 5;
    const int64_t c_idx = (leader_idx + 2) % 5;
    const int64_t d_idx = (leader_idx + 3) % 5;
    const int64_t e_idx = (leader_idx + 4) % 5;
    const common::ObAddr a_addr = get_cluster()[leader_idx]->get_addr();
    const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
    const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
    const common::ObAddr d_addr = get_cluster()[d_idx]->get_addr();
    const common::ObAddr e_addr = get_cluster()[e_idx]->get_addr();
    PalfHandleImplGuard *a_handle = palf_list[leader_idx];
    PalfHandleImplGuard *b_handle = palf_list[b_idx];
    PalfHandleImplGuard *d_handle = palf_list[d_idx];

    const LSN &add_e_barrier = leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_end_lsn_;
    sleep(2);
    block_net(leader_idx, d_idx);
    block_net(leader_idx, e_idx);
    block_net(b_idx, d_idx);
    block_net(b_idx, e_idx);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));

    // advance log_barrier
    int64_t remove_e_pid = 0;
    int64_t remove_e_ele_epoch = 0;
    LogConfigVersion remove_e_version;
    LogConfigChangeArgs remove_e_args(ObMember(e_addr, 1), 3, REMOVE_MEMBER);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(remove_e_pid, remove_e_ele_epoch, remove_e_args.type_));

    TimeoutChecker not_timeout(CONFIG_CHANGE_TIMEOUT);
    bool is_already_finished = false;
    LogConfigInfoV2 new_config_info;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->check_args_and_generate_config_(remove_e_args, remove_e_pid, remove_e_ele_epoch,
      is_already_finished, new_config_info));
    EXPECT_UNTIL_EQ(OB_SUCCESS, leader.palf_handle_impl_->wait_log_barrier_(remove_e_args, new_config_info, not_timeout));
    const LSN &remove_e_barrier = leader.palf_handle_impl_->config_mgr_.checking_barrier_.prev_end_lsn_;
    leader.palf_handle_impl_->state_mgr_.reset_changing_config_with_arb();

    sleep(5);
    // check if logs before remove_e_barrier has been committed
    EXPECT_GT(remove_e_barrier, leader.palf_handle_impl_->sw_.committed_end_lsn_) << \
    remove_e_barrier.val_ << leader.palf_handle_impl_->sw_.committed_end_lsn_.val_;

    // D has been elected to be the leader
    for (auto srv: get_cluster()) {
      srv->set_leader(id, d_addr);
    }
    EXPECT_UNTIL_EQ(true, d_handle->palf_handle_impl_->state_mgr_.is_leader_active());
    EXPECT_EQ(d_handle->palf_handle_impl_->sw_.committed_end_lsn_, leader.palf_handle_impl_->sw_.committed_end_lsn_);

    unblock_net(leader_idx, d_idx);
    unblock_net(leader_idx, e_idx);
    unblock_net(b_idx, d_idx);
    unblock_net(b_idx, e_idx);
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->start();
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test test_add_remove_lose_logs", K(id));
}

// 1. 2F1A, the leader degraded B
// 2. migrate B to D
TEST_F(TestObSimpleLogClusterArbMockEleService, test_2f1a_degrade_migrate)
{
  int ret = OB_SUCCESS;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L;
  OB_LOGGER.set_log_level("TRACE");
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_degrade_migrate");
  PALF_LOG(INFO, "begin test_2f1a_degrade_migrate", K(id));
  {
    int64_t leader_idx = 0;
    int64_t arb_replica_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb_mock_election(id, arb_replica_idx, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->start();

    const int64_t b_idx = (leader_idx + 1) % 4;
    const int64_t c_idx = (leader_idx + 2) % 4;
    const int64_t d_idx = (leader_idx + 3) % 4;
    const common::ObAddr a_addr = get_cluster()[leader_idx]->get_addr();
    const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
    const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
    const common::ObAddr d_addr = get_cluster()[d_idx]->get_addr();
    PalfHandleImplGuard *a_handle = palf_list[leader_idx];
    PalfHandleImplGuard *b_handle = palf_list[b_idx];

    // 1. degrade B
    block_net(leader_idx, b_idx);
    EXPECT_TRUE(is_degraded(leader, b_idx));

    // 2. migrate B to D
    common::ObMember b_member = common::ObMember(b_addr, 1);
    common::ObMember migrating_d = common::ObMember(d_addr, 1);
    migrating_d.set_migrating();

    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(migrating_d, CONFIG_CHANGE_TIMEOUT));
    LogConfigVersion config_version;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member_with_learner(migrating_d, b_member, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(0, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.get_member_number());
    EXPECT_EQ(2, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.get_member_number());
    EXPECT_EQ(2, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
    unblock_net(leader_idx, b_idx);

    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_degrade_migrate", K(id));
}

TEST_F(TestObSimpleLogClusterArbMockEleService, test_2f1a_disk_full_reconfirm)
{
  int ret = OB_SUCCESS;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L;
  OB_LOGGER.set_log_level("TRACE");
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_disk_full_reconfirm");
  PALF_LOG(INFO, "begin test_2f1a_disk_full_reconfirm", K(id));
  {
    int64_t leader_idx = 0;
    int64_t arb_replica_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb_mock_election(id, arb_replica_idx, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->stop();

    const int64_t b_idx = (leader_idx + 1) % 3;
    const common::ObAddr a_addr = get_cluster()[leader_idx]->get_addr();
    const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
    PalfHandleImplGuard *a_handle = palf_list[leader_idx];
    PalfHandleImplGuard *b_handle = palf_list[b_idx];

    // 1. the leader has more log than follower
    int64_t proposal_id = 0;
    int64_t mode_version = 0;
    proposal_id = leader.palf_handle_impl_->state_mgr_.get_proposal_id();
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->mode_mgr_.get_mode_version(mode_version));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(proposal_id, mode_version, AccessMode::RAW_WRITE, SCN::min_scn()));
    EXPECT_UNTIL_EQ(true, (leader.palf_handle_impl_->get_max_lsn() == b_handle->palf_handle_impl_->get_end_lsn()));
    b_handle->palf_handle_impl_->disable_sync();
    proposal_id = leader.palf_handle_impl_->state_mgr_.get_proposal_id();
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->mode_mgr_.get_mode_version(mode_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(proposal_id, mode_version, AccessMode::APPEND, SCN::min_scn()));
    (void) submit_log(leader, 100, id);
    EXPECT_UNTIL_EQ(true, (leader.palf_handle_impl_->get_max_lsn() == leader.palf_handle_impl_->sw_.max_flushed_end_lsn_));

    // 2. the leader revokes and takes over again
    for (auto srv: get_cluster()) {
      const ObAddr addr1(ObAddr::IPV4, "0.0.0.0", 0);
      srv->set_leader(id, addr1);
    }
    EXPECT_UNTIL_EQ(false, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());
    global_timeous_us = 15 * 1000 * 1000;
    dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->start();

    for (auto srv: get_cluster()) {
      srv->set_leader(id, a_addr, 3);
    }
    EXPECT_UNTIL_EQ(true, a_handle->palf_handle_impl_->state_mgr_.is_leader_active());

    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  global_timeous_us = 2 * 1000 * 1000;
  PALF_LOG(INFO, "end test_2f1a_disk_full_reconfirm", K(id));
}
} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
