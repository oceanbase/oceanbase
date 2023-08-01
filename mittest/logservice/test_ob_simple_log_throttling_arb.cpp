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

const std::string TEST_NAME = "throttling_arb";

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

class TestObSimpleLogThrottleArb : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogThrottleArb() :  ObSimpleLogClusterTestEnv()
  {}

  void set_palf_disk_options(PalfEnvImpl &palf_env_impl, const PalfDiskOptions &disk_options)
  {
    palf_env_impl.disk_options_wrapper_.disk_opts_for_recycling_blocks_ = disk_options;
    palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_ = disk_options;
  }
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 5;
bool ObSimpleLogClusterTestBase::need_add_arb_server_ = true;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;

MockLocCB loc_cb;

const int64_t KB = 1024L;
const int64_t MB = 1024 * 1024L;
TEST_F(TestObSimpleLogThrottleArb, test_2f1a_throttling_major)
{
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_1_0_0;
  SET_CASE_LOG_FILE(TEST_NAME, "arb_throttling_major");
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_throttling_major");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	ASSERT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, &loc_cb, arb_replica_idx, leader_idx, false, leader));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  loc_cb.leader_ = leader.palf_handle_impl_->self_;

  PalfEnv *palf_env = NULL;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  const PalfDiskOptions disk_options = palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_;

  const int64_t another_f_idx = (leader_idx+1)%3;
  const int64_t follower_D_idx = (leader_idx + 3);

  set_disk_options_for_throttling(palf_env->palf_env_impl_);
  ASSERT_EQ(OB_SUCCESS, get_palf_env(another_f_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);
  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);

  PALF_LOG(INFO, "[CASE 1]: prepare for throttling");
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 140, id, 2 * MB));
  LSN max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);

  int64_t throttling_percentage = 60;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;

  ASSERT_EQ(OB_SUCCESS, get_palf_env(another_f_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;

  PALF_LOG(INFO, "[CASE 1.1]: MAJOR degrade");
  block_net(leader_idx, another_f_idx);
  sleep(1);

  // do not check OB_SUCCESS, may return OB_NOT_MASTER during degrading member
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128 * KB));
  int64_t begin_ts = common::ObClockGenerator::getClock();
  EXPECT_TRUE(is_degraded(leader, another_f_idx));
  int64_t end_ts = common::ObClockGenerator::getClock();
  int64_t used_time = end_ts - begin_ts;
  ASSERT_TRUE(used_time < 2 * 1000 * 1000L);
  PALF_LOG(INFO, " MAJOR degrade", K(used_time));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128));

  //submit some log
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 100;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 1 * MB));

  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  PALF_LOG(INFO, "[CASE 1.2] MAJOR upgrade");
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 2, id, 512 * KB));
  unblock_net(leader_idx, another_f_idx);
  begin_ts = common::ObClockGenerator::getClock();
  PALF_LOG(INFO, "[CASE 1.2] begin MAJOR upgrade", K(used_time));
  ASSERT_TRUE(is_upgraded(leader, id));
  end_ts = common::ObClockGenerator::getClock();
  used_time = end_ts - begin_ts;
  PALF_LOG(INFO, "[CASE 1.2] end MAJOR upgrade", K(used_time));
  ASSERT_TRUE(used_time < 5 * 1000 * 1000L);
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128));

  PALF_LOG(INFO, "[CASE 1.3]: MAJOR replace_member(OB_TIMEOUT)");
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[follower_D_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 128 * KB));

  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ret = leader.palf_handle_impl_->replace_member(ObMember(get_cluster()[follower_D_idx]->get_addr(), 1),
                                                 ObMember(get_cluster()[another_f_idx]->get_addr(), 1),
                                                 config_version,
                                                 CONFIG_CHANGE_TIMEOUT);
  //timeout because added member can flush new meta when prev log is throttling
  ASSERT_TRUE(OB_TIMEOUT == ret || OB_SUCCESS == ret);
  int64_t new_leader_idx = OB_TIMEOUT == ret ? another_f_idx : follower_D_idx;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128));

  PALF_LOG(INFO, "[CASE 1.4]: MAJOR switch_leader");
  PalfHandleImplGuard new_leader;
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[follower_D_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128 * KB));
  int64_t switch_start_ts = common::ObClockGenerator::getClock();
  ASSERT_EQ(OB_SUCCESS, switch_leader(id, new_leader_idx, new_leader));
  int64_t switch_end_ts = common::ObClockGenerator::getClock();
  used_time  = switch_end_ts - switch_start_ts;
  PALF_LOG(INFO, "[CASE 1.4 end ] end switch_leader", K(used_time));
  // ASSERT_EQ(true, used_time < 2 * 1000 * 1000);

  new_leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(new_leader, 1, id, 1 * KB));
  max_lsn = new_leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, new_leader);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(another_f_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  new_leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_degrade_upgrade", K(id));
}

TEST_F(TestObSimpleLogThrottleArb, test_2f1a_throttling_minor_leader)
{
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_1_0_0;
  SET_CASE_LOG_FILE(TEST_NAME, "arb_throttling_minor_leader");
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin arb_throttling_minor_leader");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	ASSERT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, &loc_cb, arb_replica_idx, leader_idx, false, leader));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  loc_cb.leader_ = leader.palf_handle_impl_->self_;

  PalfEnv *palf_env = NULL;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  const PalfDiskOptions disk_options = palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_;

  const int64_t another_f_idx = (leader_idx+1)%3;
  const int64_t follower_D_idx = (leader_idx + 3);

  set_disk_options_for_throttling(palf_env->palf_env_impl_);
  ASSERT_EQ(OB_SUCCESS, get_palf_env(another_f_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);
  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);

  PALF_LOG(INFO, "[CASE 2]: prepare for throttling");
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 140, id, 2 * MB));
  LSN max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);

  int64_t throttling_percentage = 60;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;

  PALF_LOG(INFO, "[CASE 2.1]: MONOR_LEADER degrade");
  block_net(leader_idx, another_f_idx);
  sleep(1);

  // do not check OB_SUCCESS, may return OB_NOT_MASTER during degrading member
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128 * KB));
  int64_t begin_ts = common::ObClockGenerator::getClock();
  EXPECT_TRUE(is_degraded(leader, another_f_idx));
  int64_t end_ts = common::ObClockGenerator::getClock();
  int64_t used_time = end_ts - begin_ts;
  ASSERT_TRUE(used_time < 2 * 1000 * 1000L);
  PALF_LOG(INFO, " MINOR_LEADER degrade", K(used_time));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128));

  //submit some log
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 100;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 1 * MB));

  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  PALF_LOG(INFO, "[CASE 2.2] MINOR_LEADER upgrade");
  //upgrade may need many time because of target replica has not fetch log
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128 * KB));
  unblock_net(leader_idx, another_f_idx);
  begin_ts = common::ObClockGenerator::getClock();
  PALF_LOG(INFO, "[CASE 2.2] begin MINOR_LEADER upgrade", K(used_time));
  ASSERT_TRUE(is_upgraded(leader, id));
  end_ts = common::ObClockGenerator::getClock();
  used_time = end_ts - begin_ts;
  PALF_LOG(INFO, "[CASE 2.2] end MINOR_LEADER upgrade", K(used_time));
  ASSERT_TRUE(used_time < 5 * 1000 * 1000L);
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128));

//  PALF_LOG(INFO, "[CASE 2.3]: MINOR_LEADER replace_member");
//  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
//
//  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128 * KB));
//
//  LogConfigVersion config_version;
//
//  const int64_t CONFIG_CHANGE_TIMEOUT_NEW = 20 * 1000 * 1000L; // 10s
//  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
//  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member(ObMember(get_cluster()[follower_D_idx]->get_addr(), 1),
//                                                                 ObMember(get_cluster()[another_f_idx]->get_addr(), 1),
//                                                                 config_version,
//                                                                 CONFIG_CHANGE_TIMEOUT_NEW));
//  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128));

  PALF_LOG(INFO, "[CASE 2.4]: MINOR_LEADER switch_leader");
  int64_t new_leader_idx = another_f_idx;
  PalfHandleImplGuard new_leader;
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128 * KB));
  int64_t switch_start_ts = common::ObClockGenerator::getClock();
  ASSERT_EQ(OB_SUCCESS, switch_leader(id, new_leader_idx, new_leader));
  int64_t switch_end_ts = common::ObClockGenerator::getClock();
  used_time  = switch_end_ts - switch_start_ts;
  PALF_LOG(INFO, "[CASE 2.4 end ] end switch_leader", K(used_time));
  // ASSERT_EQ(true, used_time < 2 * 1000 * 1000);

  new_leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(new_leader, 1, id, 1 * KB));
  max_lsn = new_leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, new_leader);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(another_f_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  new_leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_degrade_upgrade", K(id));
}

TEST_F(TestObSimpleLogThrottleArb, test_2f1a_throttling_minor_follower)
{
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_1_0_0;
  SET_CASE_LOG_FILE(TEST_NAME, "arb_throttling_minor_follower");
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin arb_throttling_minor_follower");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	ASSERT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, &loc_cb, arb_replica_idx, leader_idx, false, leader));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  loc_cb.leader_ = leader.palf_handle_impl_->self_;

  PalfEnv *palf_env = NULL;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  const PalfDiskOptions disk_options = palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_;

  const int64_t another_f_idx = (leader_idx+1)%3;
  const int64_t follower_D_idx = (leader_idx + 3);

  set_disk_options_for_throttling(palf_env->palf_env_impl_);
  ASSERT_EQ(OB_SUCCESS, get_palf_env(another_f_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);
  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);

  PALF_LOG(INFO, "[CASE 3]: prepare for throttling");
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[follower_D_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 140, id, 2 * MB));
  LSN max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);

  int64_t throttling_percentage = 60;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(another_f_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;

  PALF_LOG(INFO, "[CASE 3.1]: MONOR_LEADER degrade");
  block_net(leader_idx, another_f_idx);
  sleep(1);

  // do not check OB_SUCCESS, may return OB_NOT_MASTER during degrading member
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[follower_D_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128 * KB));
  int64_t begin_ts = common::ObClockGenerator::getClock();
  EXPECT_TRUE(is_degraded(leader, another_f_idx));
  int64_t end_ts = common::ObClockGenerator::getClock();
  int64_t used_time = end_ts - begin_ts;
  ASSERT_TRUE(used_time < 2 * 1000 * 1000L);
  PALF_LOG(INFO, " MINOR_FOLLOWER degrade", K(used_time));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128));

  //submit some log
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 100;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 1 * MB));

//palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
 // usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  PALF_LOG(INFO, "[CASE 3.2] MINOR_FOLLOWER upgrade");
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[follower_D_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128 * KB));
  unblock_net(leader_idx, another_f_idx);
  begin_ts = common::ObClockGenerator::getClock();
  PALF_LOG(INFO, "[CASE 3.2] begin MINOR_FOLLOWER upgrade", K(used_time));
  ASSERT_TRUE(is_upgraded(leader, id));
  end_ts = common::ObClockGenerator::getClock();
  used_time = end_ts - begin_ts;
  PALF_LOG(INFO, "[CASE 3.2] end MINOR_FOLLOWER upgrade", K(used_time));
  ASSERT_TRUE(used_time < 3 * 1000 * 1000L);
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128));

  PALF_LOG(INFO, "[CASE 3.3]: MINOR_FOLLOWER replace_member");
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[follower_D_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128 * KB));
  ASSERT_EQ(OB_SUCCESS, get_palf_env(another_f_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;

  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member(ObMember(get_cluster()[follower_D_idx]->get_addr(), 1),
                                                                 ObMember(get_cluster()[another_f_idx]->get_addr(), 1),
                                                                 config_version,
                                                                 CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128));
  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);

  PALF_LOG(INFO, "[CASE 3.4]: MINOR_FOLLOWER switch_leader");
  int64_t new_leader_idx = follower_D_idx;
  PalfHandleImplGuard new_leader;
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[follower_D_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, id, 128 * KB));
  int64_t switch_start_ts = common::ObClockGenerator::getClock();
  ASSERT_EQ(OB_SUCCESS, switch_leader(id, new_leader_idx, new_leader));
  int64_t switch_end_ts = common::ObClockGenerator::getClock();
  used_time  = switch_end_ts - switch_start_ts;
  PALF_LOG(INFO, "[CASE 3.4 end ] end switch_leader", K(used_time));
  // ASSERT_EQ(true, used_time < 2 * 1000 * 1000);

  new_leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(new_leader, 1, id, 1 * KB));
  max_lsn = new_leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, new_leader);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(another_f_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  new_leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_degrade_upgrade", K(id));
}


TEST_F(TestObSimpleLogThrottleArb, test_4f1a_degrade_upgrade)
{
  SET_CASE_LOG_FILE(TEST_NAME, "arb_4f1a_throttling");
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_4f1a_throttling");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
  std::vector<PalfHandleImplGuard*> palf_list;
	ASSERT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, &loc_cb, arb_replica_idx, leader_idx, false, leader));

  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[3]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));

  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[4]->get_addr(), 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  loc_cb.leader_ = leader.palf_handle_impl_->self_;

  const int64_t another_f1_idx = (leader_idx+1)%5;
  const int64_t another_f2_idx = (leader_idx+3)%5;
  const int64_t another_f3_idx = (leader_idx+4)%5;

  PalfEnv *palf_env = NULL;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);
  ASSERT_EQ(OB_SUCCESS, get_palf_env(another_f1_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);
  ASSERT_EQ(OB_SUCCESS, get_palf_env(another_f2_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);
  ASSERT_EQ(OB_SUCCESS, get_palf_env(another_f3_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);


  PALF_LOG(INFO, "[CASE 4]: prepare for throttling");
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f1_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f2_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f3_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 140, id, 2 * MB));
  LSN max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);

  int64_t throttling_percentage = 60;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  sleep(2);
  PALF_LOG(INFO, "[CASE 4.1]: 4f1A   B is throttling while A is leader, degrade C & D");
  block_all_net(another_f2_idx);
  block_all_net(another_f3_idx);

  int64_t begin_ts = common::ObClockGenerator::getClock();
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f1_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f2_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  palf_list[another_f3_idx]->palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 256 * KB));
  usleep(10 * 1000);
  ASSERT_TRUE(is_degraded(leader, another_f2_idx));
  ASSERT_TRUE(is_degraded(leader, another_f3_idx));
  int64_t end_ts = common::ObClockGenerator::getClock();
  int64_t used_time = end_ts - begin_ts;
  PALF_LOG(INFO, " [CASE] 4f1a degrade", K(used_time));

  // 确保lease过期，验证loc_cb是否可以找到leader拉日志
  sleep(5);
  unblock_all_net(another_f2_idx);
  unblock_all_net(another_f3_idx);
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 256));

  PALF_LOG(INFO, " [CASE] 4f1a before upgrade", K(used_time));
  ASSERT_TRUE(is_upgraded(leader, id));
  PALF_LOG(INFO, " [CASE] end upgrade", K(used_time));

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_4f1a_degrade_upgrade", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
