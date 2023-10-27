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

const std::string TEST_NAME = "arb_service";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;

int64_t ARB_TIMEOUT_ARG = 2 * 1000 * 1000L;

namespace logservice
{

void ObArbitrationService::update_arb_timeout_()
{
  arb_timeout_us_ = ARB_TIMEOUT_ARG;
  if (REACH_TIME_INTERVAL(2 * 1000 * 1000)) {
    CLOG_LOG_RET(WARN, OB_ERR_UNEXPECTED, "update_arb_timeout_", K_(self), K_(arb_timeout_us));
  }
}
}

namespace unittest
{

class TestObSimpleLogClusterArbService : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterArbService() :  ObSimpleLogClusterTestEnv()
  {}
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 5;
bool ObSimpleLogClusterTestBase::need_add_arb_server_ = true;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_degrade_upgrade)
{
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_1_0_0;
  SET_CASE_LOG_FILE(TEST_NAME, "arb_2f1a_degrade_upgrade");
  OB_LOGGER.set_log_level("TRACE");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_degrade_upgrade");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  const int64_t another_f_idx = (leader_idx+1)%3;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  // 为备副本设置location cb，用于备副本找leader
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  block_net(leader_idx, another_f_idx);
  // do not check OB_SUCCESS, may return OB_NOT_MASTER during degrading member
  submit_log(leader, 100, id);

  PALF_LOG(INFO, "CASE[1] degrade caused by block_net ");
  EXPECT_TRUE(is_degraded(leader, another_f_idx));

  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  unblock_net(leader_idx, another_f_idx);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id));

  EXPECT_TRUE(is_upgraded(leader, id));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id));

  // set clog disk error
  ObTenantEnv::set_tenant(get_cluster()[another_f_idx]->get_tenant_base());
  logservice::coordinator::ObFailureDetector *detector = MTL(logservice::coordinator::ObFailureDetector *);
  if (NULL != detector) {
    PALF_LOG(INFO, "set clog full event");
    detector->has_add_clog_full_event_ = true;
  }

  PALF_LOG(INFO, "CASE[2] degrade caused by clog disk error");
  EXPECT_TRUE(is_degraded(leader, another_f_idx));

  if (NULL != detector) {
    detector->has_add_clog_full_event_ = false;
  }

  EXPECT_TRUE(is_upgraded(leader, id));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id));

  // test disable sync
  PALF_LOG(INFO, "CASE[3] degrade caused by disable_sync");
  palf_list[another_f_idx]->palf_handle_impl_->disable_sync();
  EXPECT_TRUE(is_degraded(leader, another_f_idx));
  palf_list[another_f_idx]->palf_handle_impl_->enable_sync();
  EXPECT_TRUE(is_upgraded(leader, id));

  PALF_LOG(INFO, "CASE[4] degrade caused by disable_vote");
  // test disbale vote
  palf_list[another_f_idx]->palf_handle_impl_->disable_vote(false/*no need check log missing*/);
  EXPECT_TRUE(is_degraded(leader, another_f_idx));
  palf_list[another_f_idx]->palf_handle_impl_->enable_vote();
  EXPECT_TRUE(is_upgraded(leader, id));

  // test revoking the leader when arb service is degrading
  block_all_net(another_f_idx);
  const common::ObAddr follower_addr = get_cluster()[another_f_idx]->get_addr();
  LogConfigChangeArgs args(common::ObMember(follower_addr, 1), 0, DEGRADE_ACCEPTOR_TO_LEARNER);
  int64_t ele_epoch;
  common::ObRole ele_role;
  int64_t proposal_id = leader.palf_handle_impl_->state_mgr_.get_proposal_id();
  leader.palf_handle_impl_->election_.get_role(ele_role, ele_epoch);
  LogConfigVersion config_version;
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(args, proposal_id, ele_epoch, config_version));
  EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.alive_paxos_memberlist_.contains(follower_addr));
  EXPECT_EQ(leader.palf_handle_impl_->config_mgr_.state_, 1);

  // reset status supposing the lease is expried
  block_net(leader_idx, another_f_idx);
  leader.palf_handle_impl_->config_mgr_.reset_status();
  EXPECT_TRUE(is_degraded(leader, another_f_idx));
  unblock_net(leader_idx, another_f_idx);
  unblock_all_net(another_f_idx);

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_degrade_upgrade", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_4f1a_degrade_upgrade)
{
  SET_CASE_LOG_FILE(TEST_NAME, "arb_4f1a_degrade_upgrade");
  OB_LOGGER.set_log_level("TRACE");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_4f1a_degrade_upgrade");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
  std::vector<PalfHandleImplGuard*> palf_list;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[3]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[4]->get_addr(), 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

  const int64_t another_f1_idx = (leader_idx+3)%5;
  const int64_t another_f2_idx = (leader_idx+4)%5;
  palf_list[another_f1_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  palf_list[another_f2_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);
  block_all_net(another_f1_idx);
  block_all_net(another_f2_idx);


  EXPECT_TRUE(is_degraded(leader, another_f1_idx));
  EXPECT_TRUE(is_degraded(leader, another_f2_idx));

  unblock_all_net(another_f1_idx);
  unblock_all_net(another_f2_idx);
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id));

  EXPECT_TRUE(is_upgraded(leader, id));

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_4f1a_degrade_upgrade", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_reconfirm_degrade_upgrade)
{
  SET_CASE_LOG_FILE(TEST_NAME, "arb_2f1a_reconfirm_test");
  OB_LOGGER.set_log_level("TRACE");
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_reconfirm_degrade_upgrade");
  MockLocCB loc_cb;
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  const int64_t another_f_idx = (leader_idx+1)%3;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);
  palf_list[leader_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  palf_list[another_f_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  // block net of old leader, new leader will be elected
  // and degrade in RECONFIRM state
  ARB_TIMEOUT_ARG = 15 * 1000 * 1000;
  block_net(leader_idx, another_f_idx);
  block_net(leader_idx, arb_replica_idx);
  // block_net后会理解进行降级操作，导致旧主上有些单副本写成功的日志被committed
  submit_log(leader, 20, id);
  // submit some logs which will be truncated

  EXPECT_TRUE(is_degraded(*palf_list[another_f_idx], leader_idx));

  int64_t new_leader_idx = -1;
  PalfHandleImplGuard new_leader;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  loc_cb.leader_ = new_leader.palf_handle_impl_->self_;
  unblock_net(leader_idx, another_f_idx);
  unblock_net(leader_idx, arb_replica_idx);
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 100, id));

  EXPECT_TRUE(is_upgraded(new_leader, id));
  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  new_leader.reset();
  delete_paxos_group(id);
  ARB_TIMEOUT_ARG = 2 * 1000 * 1000;
  PALF_LOG(INFO, "end test_2f1a_reconfirm_degrade_upgrade", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_4f1a_reconfirm_degrade_upgrade)
{
  SET_CASE_LOG_FILE(TEST_NAME, "arb_4f1a_reconfirm_test");
  OB_LOGGER.set_log_level("TRACE");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_4f1a_reconfirm_degrade_upgrade");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  auto cluster = get_cluster();
  PalfHandleImplGuard leader;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
  std::vector<PalfHandleImplGuard*> palf_list;

	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[3]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[4]->get_addr(), 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));

  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

  const int64_t another_f1_idx = 3;
  const int64_t another_f2_idx = 4;
  palf_list[leader_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  palf_list[another_f1_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);
  // stop leader and a follower
  block_all_net(leader_idx);
  block_all_net(another_f1_idx);

  //EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  // wait for new leader is elected
  int64_t new_leader_idx = leader_idx;
  PalfHandleImplGuard new_leader;
  while (leader_idx == new_leader_idx) {
    new_leader.reset();
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  }

  EXPECT_TRUE(is_degraded(new_leader, another_f1_idx));
  EXPECT_TRUE(is_degraded(new_leader, leader_idx));

  loc_cb.leader_ = new_leader.palf_handle_impl_->self_;
  // restart two servers
  unblock_all_net(leader_idx);
  unblock_all_net(another_f1_idx);

  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 100, id));

  EXPECT_TRUE(is_upgraded(new_leader, id));
  leader.reset();
  new_leader.reset();
  revert_cluster_palf_handle_guard(palf_list);
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_4f1a_reconfirm_degrade_upgrade", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_config_change)
{
  SET_CASE_LOG_FILE(TEST_NAME, "arb_2f1a_config_change");
  OB_LOGGER.set_log_level("DEBUG");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin arb_2f1a_config_change");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  // 为备副本设置location cb，用于备副本找leader
  const int64_t another_f_idx = (leader_idx+1)%3;
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  palf_list[3]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  palf_list[4]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);

  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  // replace member
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member(
      ObMember(palf_list[3]->palf_handle_impl_->self_, 1),
      ObMember(palf_list[another_f_idx]->palf_handle_impl_->self_, 1),
      config_version,
      CONFIG_CHANGE_TIMEOUT));

  // add learner
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(
      ObMember(palf_list[4]->palf_handle_impl_->self_, 1),
      CONFIG_CHANGE_TIMEOUT));

  // switch learner
  EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->switch_learner_to_acceptor(
      ObMember(palf_list[4]->palf_handle_impl_->self_, 1),
      2,
      config_version,
      CONFIG_CHANGE_TIMEOUT));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_learner_to_acceptor(
      ObMember(palf_list[4]->palf_handle_impl_->self_, 1),
      3,
      config_version,
      CONFIG_CHANGE_TIMEOUT));
  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end arb_2f1a_config_change", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_arb_with_highest_version)
{
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_1_0_0;
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_arb_with_highest_version");
  OB_LOGGER.set_log_level("DEBUG");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_arb_with_highest_version");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  // 为备副本设置location cb，用于备副本找leader
  const int64_t another_f_idx = (leader_idx+1)%3;
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 500, id));
  sleep(2);

  LogConfigChangeArgs args(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 0, ADD_LEARNER);
  int64_t proposal_id = 0;
  int64_t election_epoch = 0;
  LogConfigVersion config_version;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(proposal_id, election_epoch, args.type_));
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, election_epoch, config_version));
  // learner list and state_ has been changed
  EXPECT_TRUE(config_version.is_valid());
  EXPECT_EQ(1, leader.palf_handle_impl_->config_mgr_.state_);
  // only send config log to arb member
  ObMemberList member_list;
  member_list.add_server(get_cluster()[2]->get_addr());
  const int64_t prev_log_proposal_id = leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_log_proposal_id_;
  const LSN prev_lsn = leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_lsn_;
  const int64_t prev_mode_pid = leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_mode_pid_;
  const LogConfigMeta config_meta = leader.palf_handle_impl_->config_mgr_.log_ms_meta_;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->log_engine_.submit_change_config_meta_req( \
    member_list, proposal_id, prev_log_proposal_id, prev_lsn, prev_mode_pid, config_meta));
  sleep(1);
  // check if arb member has received and persisted the config log
  while (true) {
    PalfHandleLiteGuard arb_member;
    if (OB_FAIL(get_arb_member_guard(id, arb_member))) {
    } else if (arb_member.palf_handle_lite_->config_mgr_.persistent_config_version_ == config_version) {
      break;
    } else {
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->log_engine_.submit_change_config_meta_req( \
        member_list, proposal_id, prev_log_proposal_id, prev_lsn, prev_mode_pid, config_meta));
    }
    ::ob_usleep(10 * 1000);
  }
  EXPECT_GT(config_version, leader.palf_handle_impl_->config_mgr_.persistent_config_version_);
  EXPECT_GT(config_version, palf_list[1]->palf_handle_impl_->config_mgr_.persistent_config_version_);

  // restart cluster, close a follower, restart leader
  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  // block_net, so two F cann't reach majority
  block_net(another_f_idx, leader_idx);
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  const int64_t restart_finish_time_us_ = common::ObTimeUtility::current_time();
  PalfHandleImplGuard new_leader;
  int64_t new_leader_idx;
  get_leader(id, new_leader, new_leader_idx);
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 500, id));
  PALF_LOG(ERROR, "RTO", "RTO", common::ObTimeUtility::current_time() - restart_finish_time_us_);

  new_leader.reset();
  // must delete paxos group in here, otherwise memory of
  // MockLocCB will be relcaimed and core dump will occur
  // blacklist will not be deleted after reboot, clean it manually
  unblock_net(another_f_idx, leader_idx);
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_arb_with_highest_version", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_defensive)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_defensive");
  OB_LOGGER.set_log_level("DEBUG");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_defensive");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  // 为备副本设置location cb，用于备副本找leader
  const int64_t another_f_idx = (leader_idx+1)%3;
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);
  const int64_t added_member_idx = 3;
  const common::ObMember added_member = ObMember(palf_list[added_member_idx]->palf_handle_impl_->self_, 1);

  // add a member, do not allow to append logs until config log reaches majority
  LogConfigVersion cur_config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(cur_config_version));
  LogConfigChangeArgs args(added_member, 3, cur_config_version, ADD_MEMBER);
  int64_t proposal_id = 0;
  int64_t election_epoch = 0;
  LogConfigVersion config_version;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(proposal_id, election_epoch, args.type_));
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, election_epoch, config_version));
  // do not allow to append log when changing config with arb
  // EXPECT_TRUE(leader.palf_handle_impl_->state_mgr_.is_changing_config_with_arb());
  while (true) {
    if (OB_SUCC(leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, election_epoch, config_version))) {
      break;
    } else {
      (void) leader.palf_handle_impl_->config_mgr_.pre_sync_config_log_and_mode_meta(args.server_, proposal_id);
      ::ob_usleep(10 * 1000);
    }
  }

  // flashback one follower
  LogEntryHeader header_origin;
  SCN base_scn;
  base_scn.set_base();
  SCN flashback_scn;
  palf::AccessMode unused_access_mode;
  int64_t mode_version;
  EXPECT_EQ(OB_SUCCESS, get_middle_scn(50, leader, flashback_scn, header_origin));
  switch_append_to_flashback(leader, mode_version);
  sleep(1);
  EXPECT_EQ(OB_SUCCESS, palf_list[another_f_idx]->palf_handle_impl_->flashback(mode_version, flashback_scn, CONFIG_CHANGE_TIMEOUT));

  // remove another follower
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(added_member, 2, CONFIG_CHANGE_TIMEOUT));

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_defensive", K(id));
}

int get_palf_handle_lite(const int64_t tenant_id,
                         const int64_t palf_id,
                         ObSimpleArbServer *server,
                         IPalfHandleImplGuard &handle_guard)
{
  int ret = OB_SUCCESS;
  PalfEnvLiteGuard env_guard;
  if (NULL == server) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(server->get_palf_env_lite(tenant_id, env_guard))) {
    PALF_LOG(ERROR, "get_palf_env_lite failed", K(tenant_id), K(palf_id));
  } else if (OB_FAIL(env_guard.palf_env_lite_->get_palf_handle_impl(palf_id, handle_guard))) {
    PALF_LOG(ERROR, "get_palf_handle_impl failed", K(tenant_id), K(palf_id));
  } else {
  }
  return ret;
}

using namespace palflite;

TEST_F(TestObSimpleLogClusterArbService, test_multi_meta_block)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_mutli_meta_block");
  OB_LOGGER.set_log_level("INFO");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_multi_meta_block");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  // 为备副本设置location cb，用于备副本找leader
  const int64_t another_f_idx = (leader_idx+1)%3;
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);
  ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(get_cluster()[arb_replica_idx]);
  IPalfHandleImplGuard arb_guard;
  ASSERT_EQ(OB_SUCCESS, get_palf_handle_lite(OB_SERVER_TENANT_ID, id, arb_server, arb_guard));
  PalfHandleLite *arb_palf = dynamic_cast<PalfHandleLite *>(arb_guard.palf_handle_impl_);
  LogEngine *log_engine = &arb_palf->log_engine_;
  LSN meta_tail = log_engine->log_meta_storage_.log_tail_;
  LogStorage *meta_storage = &log_engine->log_meta_storage_;
  {
    while (1) {
      if (meta_storage->log_tail_ < LSN(meta_storage->logical_block_size_)) {
        EXPECT_EQ(OB_SUCCESS, log_engine->append_log_meta_(log_engine->log_meta_));
      } else {
        break;
      }
    }
  }
  meta_tail = log_engine->log_meta_storage_.log_tail_;
  ASSERT_EQ(meta_tail, LSN(log_engine->log_meta_storage_.logical_block_size_));
  revert_cluster_palf_handle_guard(palf_list);
  arb_guard.reset();
  leader.reset();
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(get_cluster()[arb_replica_idx]);
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    IPalfHandleImplGuard arb_guard;
    ASSERT_EQ(OB_SUCCESS, get_palf_handle_lite(OB_SERVER_TENANT_ID, id, arb_server, arb_guard));
    PalfHandleLite *arb_palf = dynamic_cast<PalfHandleLite *>(arb_guard.palf_handle_impl_);
    LogEngine *log_engine = &arb_palf->log_engine_;
    LogStorage *meta_storage = &log_engine->log_meta_storage_;
    EXPECT_EQ(OB_SUCCESS, log_engine->append_log_meta_(log_engine->log_meta_));
    LSN meta_tail = log_engine->log_meta_storage_.log_tail_;
    ASSERT_NE(meta_tail, LSN(log_engine->log_meta_storage_.logical_block_size_));
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(get_cluster()[arb_replica_idx]);
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    IPalfHandleImplGuard arb_guard;
    ASSERT_EQ(OB_SUCCESS, get_palf_handle_lite(OB_SERVER_TENANT_ID, id, arb_server, arb_guard));
    PalfHandleLite *arb_palf = dynamic_cast<PalfHandleLite *>(arb_guard.palf_handle_impl_);
    LogEngine *log_engine = &arb_palf->log_engine_;
    LSN meta_tail = log_engine->log_meta_storage_.log_tail_;
    LogStorage *meta_storage = &log_engine->log_meta_storage_;
    while (1) {
      if (meta_storage->log_tail_ < LSN(32 * meta_storage->logical_block_size_)) {
        EXPECT_EQ(OB_SUCCESS, log_engine->append_log_meta_(log_engine->log_meta_));
      } else {
        break;
      }
    }
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(get_cluster()[arb_replica_idx]);
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 4000, id));
    IPalfHandleImplGuard arb_guard;
    ASSERT_EQ(OB_SUCCESS, get_palf_handle_lite(OB_SERVER_TENANT_ID, id, arb_server, arb_guard));
    PalfHandleLite *arb_palf = dynamic_cast<PalfHandleLite *>(arb_guard.palf_handle_impl_);
    LogEngine *log_engine = &arb_palf->log_engine_;
    LSN meta_tail = log_engine->log_meta_storage_.log_tail_;
    LogStorage *meta_storage = &log_engine->log_meta_storage_;
    while (1) {
      if (meta_storage->log_tail_ < LSN(34 * meta_storage->logical_block_size_ + 4*4*1024)) {
        EXPECT_EQ(OB_SUCCESS, log_engine->append_log_meta_(log_engine->log_meta_));
      } else {
        break;
      }
    }
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_mutli_meta_block", K(id));
}

// 1. 2F1A, the leader starts to degrade another F
// 2. after the config log has been accepted by another F, the leader revoked
// 3. the previous leader has been elected as the new leader
// 4. reconfirm may fail because leader's config_version is not same to that of the follower
TEST_F(TestObSimpleLogClusterArbService, test_2f1a_degrade_when_no_leader)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_degrade_when_no_leader");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_degrade_when_no_leader");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  // 为备副本设置location cb，用于备副本找leader
  const int64_t another_f_idx = (leader_idx+1)%3;
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);

  const common::ObAddr b_addr = palf_list[another_f_idx]->palf_handle_impl_->self_;
  LogConfigChangeArgs args(ObMember(b_addr, 1), 0, DEGRADE_ACCEPTOR_TO_LEARNER);
  int64_t proposal_id = 0;
  int64_t election_epoch = 0;
  LogConfigVersion config_version;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(proposal_id, election_epoch, args.type_));
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, election_epoch, config_version));

  // leader appended config meta
  EXPECT_FALSE(palf_list[leader_idx]->get_palf_handle_impl()->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));
  EXPECT_TRUE(palf_list[another_f_idx]->get_palf_handle_impl()->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));

  // block all networks of arb member, and the network from the follower to the leader
  block_net(arb_replica_idx, another_f_idx, true);
  block_net(arb_replica_idx, leader_idx, true);
  block_net(another_f_idx, leader_idx, true);

  // waiting for leader revoke
  while (leader.palf_handle_impl_->state_mgr_.role_ == common::ObRole::LEADER) {
    sleep(1);
  }

  // unblock_net
  unblock_net(another_f_idx, leader_idx);
  unblock_net(arb_replica_idx, leader_idx);

  common::ObMemberList leader_member_list;
  int64_t leader_replica_num = 0;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.get_log_sync_member_list( \
      leader_member_list, leader_replica_num));
  EXPECT_EQ(1, leader_member_list.get_member_number());
  EXPECT_EQ(1, leader_replica_num);

  int64_t new_leader_idx = 0;
  PalfHandleImplGuard new_leader;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));

  EXPECT_EQ(leader.palf_handle_impl_->self_, new_leader.palf_handle_impl_->self_);

  // waiting for upgrading
  is_upgraded(leader, id);
  EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->config_mgr_.get_log_sync_member_list( \
      leader_member_list, leader_replica_num));
  EXPECT_EQ(2, leader_member_list.get_member_number());
  EXPECT_EQ(2, leader_replica_num);

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  new_leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_degrade_when_no_leader", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_upgrade_when_no_leader)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_upgrade_when_no_leader");
//  OB_LOGGER.set_log_level("TRACE");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_upgrade_when_no_leader");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  // 为备副本设置location cb，用于备副本找leader
  const int64_t another_f_idx = (leader_idx+1)%3;
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);

  // block the network from the follower to the leader
  block_net(another_f_idx, leader_idx, true);
  is_degraded(leader, another_f_idx);

  // upgrade follower manually
  int64_t proposal_id;
  int64_t election_epoch;
  LogConfigVersion config_version;
  LogConfigChangeArgs args(common::ObMember(get_cluster()[another_f_idx]->get_addr(), 1), 0, UPGRADE_LEARNER_TO_ACCEPTOR);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(proposal_id, election_epoch, args.type_));

  block_net(arb_replica_idx, leader_idx, true);
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, election_epoch, config_version));
  EXPECT_EQ(1, leader.palf_handle_impl_->config_mgr_.state_);
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, election_epoch, config_version));
  EXPECT_EQ(1, leader.palf_handle_impl_->config_mgr_.state_);

  // waiting for leader revoke
  while (leader.palf_handle_impl_->state_mgr_.role_ == LEADER) {
    sleep(1);
  }

  // avoid the follower is elected to be leader
  block_net(arb_replica_idx, another_f_idx, true);
  unblock_all_net(leader_idx);

  // waiting for leader takeover
  while (!leader.palf_handle_impl_->state_mgr_.is_leader_active()) {
    sleep(1);
  }
  // waiting for upgrading
  is_upgraded(leader, id);

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_upgrade_when_no_leader", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
