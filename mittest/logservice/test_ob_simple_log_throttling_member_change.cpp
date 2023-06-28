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
#include "env/ob_simple_log_server.h"
#undef private

const std::string TEST_NAME = "throttling_member_change";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
using namespace palf;
namespace unittest
{
class TestObSimpleLogIOWorkerThrottlingV2 : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogIOWorkerThrottlingV2() :  ObSimpleLogClusterTestEnv()
  {}
  void set_palf_disk_options(PalfEnvImpl &palf_env_impl, const PalfDiskOptions &disk_options)
  {
    palf_env_impl.disk_options_wrapper_.disk_opts_for_recycling_blocks_ = disk_options;
    palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_ = disk_options;
  }
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 5;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

MockLocCB loc_cb;

TEST_F(TestObSimpleLogIOWorkerThrottlingV2, test_throttling_majority)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_throttling_majority");
  int ret = OB_SUCCESS;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test throttling_majority", K(id));
  int64_t leader_idx = 0;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
  PalfHandleImplGuard leader;
  PalfEnv *palf_env = NULL;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  const PalfDiskOptions disk_options = palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_;
  set_disk_options_for_throttling(palf_env->palf_env_impl_);
  loc_cb.leader_ = get_cluster()[leader_idx]->get_addr();
  std::vector<PalfHandleImplGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  ASSERT_EQ(5, palf_list.size());

  const int64_t follower_B_idx = (leader_idx + 1);
  const int64_t follower_C_idx = (leader_idx + 2);
  const int64_t follower_D_idx = (leader_idx + 3);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_B_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_C_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);


  const int64_t KB = 1024L;
  const int64_t MB = 1024 * 1024L;
  PALF_LOG(INFO, "prepare for throttling");
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 140, id, 2 * MB));
  LSN max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  LSN follower_max_lsn = palf_list[1]->palf_handle_impl_->sw_.get_max_lsn();
  PALF_LOG(INFO, "prepare max_lsn", K(max_lsn), K(follower_max_lsn));
  wait_lsn_until_flushed(max_lsn, leader);

  PALF_LOG(INFO, "[CASE 1]test throttling interval");
  int64_t throttling_percentage = 60;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_B_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_C_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  int64_t cur_ts = common::ObClockGenerator::getClock();
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 512 * KB));
  max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);
  wait_lsn_until_flushed(max_lsn, *palf_list[1]);
  wait_lsn_until_flushed(max_lsn, *palf_list[2]);
  int64_t break_ts = common::ObClockGenerator::getClock();
  int64_t used_time = break_ts- cur_ts;
  PALF_LOG(INFO, "[CASE 1] ", K(used_time));
  ASSERT_EQ(true, (used_time) > 14 * 1000 * 1000);

  PALF_LOG(INFO, "[1.1] before change_replica_num 3->5");
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(get_member_list(), 3, 5, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * KB));
  max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);
  PALF_LOG(INFO, "[1.2] before change_replica_num 5->3");
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(get_member_list(), 5, 3, CONFIG_CHANGE_TIMEOUT));

  PALF_LOG(INFO, "CASE[1.3] test remove_member while throttling");
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 2, id, 512 * KB));
  usleep(500 * 1000);
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(get_cluster()[follower_C_idx]->get_addr(), 1), 3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * KB));

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 100;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 2, id, 512 * KB));
  usleep(500 * 1000);
  PALF_LOG(INFO, "CASE[1.4] test add_member while throttling");

  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[follower_D_idx]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * KB));

  PALF_LOG(INFO, "CASE[1.5] test switch_leader while throttling[major]");
  int64_t new_leader_idx = 1;
  PalfHandleImplGuard new_leader;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 2, id, 512 * KB));
  int64_t switch_start_ts = common::ObClockGenerator::getClock();
  ASSERT_EQ(OB_SUCCESS, switch_leader(id, new_leader_idx, new_leader));
  int64_t switch_end_ts = common::ObClockGenerator::getClock();
  used_time  = switch_end_ts - switch_start_ts;
  PALF_LOG(INFO, "[CASE 1.5 end ] end switch_leader", K(used_time));
  // ASSERT_EQ(true, used_time < 2 * 1000 * 1000);

  new_leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(new_leader, 1, id, 1 * KB));
  max_lsn = new_leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, new_leader);
  loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();

  PALF_LOG(INFO, "end test throttling_major", K(id));
  leader.reset();
  new_leader.reset();

  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_B_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_C_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  revert_cluster_palf_handle_guard(palf_list);
  delete_paxos_group(id);
  PALF_LOG(INFO, "destroy", K(id));
}

TEST_F(TestObSimpleLogIOWorkerThrottlingV2, test_throttling_minor_leader)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_throttling_minor_leader");
  int ret = OB_SUCCESS;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test throttling_minor_leader", K(id));
  int64_t leader_idx = 0;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
  PalfHandleImplGuard leader;
  PalfEnv *palf_env = NULL;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  const PalfDiskOptions disk_options = palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_;
  set_disk_options_for_throttling(palf_env->palf_env_impl_);
  loc_cb.leader_ = get_cluster()[leader_idx]->get_addr();
  std::vector<PalfHandleImplGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

  const int64_t follower_B_idx = (leader_idx + 1);
  const int64_t follower_C_idx = (leader_idx + 2);
  const int64_t follower_D_idx = (leader_idx + 3);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_B_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_C_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);


  const int64_t KB = 1024L;
  const int64_t MB = 1024 * 1024L;
  PALF_LOG(INFO, "[CASE 2]prepare for throttling");
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 140, id, 2 * MB));
  LSN max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  LSN follower_max_lsn = palf_list[1]->palf_handle_impl_->sw_.get_max_lsn();
  PALF_LOG(INFO, "prepare max_lsn", K(max_lsn), K(follower_max_lsn));
  wait_lsn_until_flushed(max_lsn, leader);

  PALF_LOG(INFO, "[CASE 2]test throttling interval");
  int64_t throttling_percentage = 60;
  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  int64_t cur_ts = common::ObClockGenerator::getClock();
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 512 * KB));
  max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);
  int64_t break_ts = common::ObClockGenerator::getClock();
  ASSERT_EQ(true, (break_ts - cur_ts) > 13 * 1000 * 1000);
  PALF_LOG(INFO, "YYY ", K(break_ts- cur_ts));

  PALF_LOG(INFO, "[CASE 2.1] before change_replica_num 3->5");
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 512 * KB));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(get_member_list(), 3, 5, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * KB));
  max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 512 * KB));
  PALF_LOG(INFO, "[CASE 2.2] before change_replica_num 5->3");
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(get_member_list(), 5, 3, CONFIG_CHANGE_TIMEOUT));
  wait_lsn_until_flushed(max_lsn, leader);
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * KB));

  PALF_LOG(INFO, "[CASE 2.3] test remove_member");
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 2, id, 512 * KB));
  usleep(500 * 1000);

  //OB_TIMEOUT: committed_end_lsn of new majority is smaller than commited_end_lsn of leader
  ASSERT_EQ(OB_TIMEOUT, leader.palf_handle_impl_->remove_member(ObMember(get_cluster()[follower_C_idx]->get_addr(), 1), 3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * KB));

  /*
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 2, id, 512 * KB));
  usleep(500 * 1000);
  PALF_LOG(INFO, "[CASE 2.4] test add_member");
  ASSERT_EQ(OB_TIMEOUT, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[follower_D_idx]->get_addr(), 1), 4, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * KB));
 */

  max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);

  PALF_LOG(INFO, "[CASE 2.5] test switch_leader");
  int64_t new_leader_idx = 1;
  PalfHandleImplGuard new_leader;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 2, id, 512 * KB));
  int64_t switch_start_ts = common::ObClockGenerator::getClock();
  ASSERT_EQ(OB_SUCCESS, switch_leader(id, new_leader_idx, new_leader));
  int64_t switch_end_ts = common::ObClockGenerator::getClock();
  const int64_t used_time  = switch_end_ts - switch_start_ts;
  PALF_LOG(INFO, "[CASE 2.5 end ] end switch_leader", K(used_time));
  // ASSERT_EQ(true, used_time < 2 * 1000 * 1000);
  new_leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(new_leader, 1, id, 1 * KB));
  max_lsn = new_leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, new_leader);
  loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();

  usleep(5*1000*1000);//wait follower_c log sync
  LSN old_max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  LSN leader_old_max_lsn = new_leader.palf_handle_impl_->sw_.get_max_lsn();
  new_leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(new_leader, 2, id, 512 * KB));
  LSN cur_max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  LSN leader_cur_max_lsn = new_leader.palf_handle_impl_->sw_.get_max_lsn();
  usleep(500 * 1000);
  LSN new_max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  LSN leader_new_max_lsn = new_leader.palf_handle_impl_->sw_.get_max_lsn();
  PALF_LOG(INFO, "before remove member",K(leader_old_max_lsn), K(leader_cur_max_lsn), K(leader_new_max_lsn), K(old_max_lsn), K(cur_max_lsn), K(new_max_lsn));
  ASSERT_EQ(OB_TIMEOUT, new_leader.palf_handle_impl_->remove_member(ObMember(get_cluster()[follower_C_idx]->get_addr(), 1), 3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(new_leader, 1, id, 1 * KB));
  usleep(500 * 1000);
  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->add_member(ObMember(get_cluster()[follower_D_idx]->get_addr(), 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(new_leader, 1, id, 1 * KB));

  PALF_LOG(INFO, "end test throttling_minor_leader", K(id));
  leader.reset();
  new_leader.reset();

  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_B_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_C_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  set_palf_disk_options(palf_env->palf_env_impl_, disk_options);


  revert_cluster_palf_handle_guard(palf_list);
  delete_paxos_group(id);
  PALF_LOG(INFO, "destroy", K(id));
}

TEST_F(TestObSimpleLogIOWorkerThrottlingV2, test_throttling_minor_follower)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_throttling_minor_follower");
  int ret = OB_SUCCESS;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test throttling_minor_follower", K(id));
  int64_t leader_idx = 0;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
  PalfHandleImplGuard leader;
  PalfEnv *palf_env = NULL;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
  ASSERT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);
  loc_cb.leader_ = get_cluster()[leader_idx]->get_addr();
  std::vector<PalfHandleImplGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

  const int64_t follower_B_idx = (leader_idx + 1);
  const int64_t follower_C_idx = (leader_idx + 2);
  const int64_t follower_D_idx = (leader_idx + 3);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_B_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_C_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);

  ASSERT_EQ(OB_SUCCESS, get_palf_env(follower_D_idx, palf_env));
  set_disk_options_for_throttling(palf_env->palf_env_impl_);


  const int64_t KB = 1024L;
  const int64_t MB = 1024 * 1024L;
  PALF_LOG(INFO, "[CASE 3]prepare for throttling");
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 140, id, 2 * MB));
  LSN max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  LSN follower_max_lsn = palf_list[1]->palf_handle_impl_->sw_.get_max_lsn();
  PALF_LOG(INFO, "prepare max_lsn", K(max_lsn), K(follower_max_lsn));
  wait_lsn_until_flushed(max_lsn, leader);

  PALF_LOG(INFO, "[CASE 3.1] before change_replica_num 3->5");
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(get_member_list(), 3, 5, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * KB));
  max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);
  PALF_LOG(INFO, "[CASE 3.2] before change_replica_num 5->3");
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 512 * KB));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(get_member_list(), 5, 3, CONFIG_CHANGE_TIMEOUT));

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 512 * KB));
  usleep(500 * 1000);
  PALF_LOG(INFO, "[CASE 3.4] test add_member(3-4)");
  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[follower_D_idx]->get_addr(), 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * KB));

  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(get_cluster()[follower_D_idx]->get_addr(), 1), 3, CONFIG_CHANGE_TIMEOUT));

  PALF_LOG(INFO, "[CASE 3.3] test remove_member");
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 512 * KB));
  usleep(500 * 1000);
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(get_cluster()[follower_B_idx]->get_addr(), 1), 3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * KB));

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 512 * KB));
  usleep(500 * 1000);
  PALF_LOG(INFO, "[CASE 3.4] test add_member");
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[follower_B_idx]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * KB));

  PALF_LOG(INFO, "[CASE 3.5] test switch_leader to C (not throttled replica)");
  int64_t new_leader_idx = 2;
  PalfHandleImplGuard new_leader;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 512 * KB));
  int64_t switch_start_ts = common::ObClockGenerator::getClock();
  ASSERT_EQ(OB_SUCCESS, switch_leader(id, new_leader_idx, new_leader));
  int64_t switch_end_ts = common::ObClockGenerator::getClock();
  int64_t used_time  = switch_end_ts - switch_start_ts;
  PALF_LOG(INFO, "[CASE 3.5 end ] end switch_leader", K(used_time));
  // ASSERT_EQ(true, used_time < 2 * 1000 * 1000);
  new_leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(new_leader, 1, id, 1 * KB));
  max_lsn = new_leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, new_leader);
  loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();

  PALF_LOG(INFO, "[CASE 3.6] test switch_leader to C(throttled replica)");
  int64_t new_leader_idx_v2 = 1;
  PalfHandleImplGuard new_leader_v2;
  ASSERT_EQ(OB_SUCCESS, submit_log(new_leader, 1, id, 512 * KB));
  switch_start_ts = common::ObClockGenerator::getClock();
  ASSERT_EQ(OB_SUCCESS, switch_leader(id, new_leader_idx_v2, new_leader_v2));
  switch_end_ts = common::ObClockGenerator::getClock();
  used_time  = switch_end_ts - switch_start_ts;
  PALF_LOG(INFO, "[CASE 3.5 end ] end switch_leader", K(used_time));
  // ASSERT_EQ(true, used_time < 2 * 1000 * 1000);
  new_leader_v2.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(new_leader_v2, 1, id, 1 * KB));
  max_lsn = new_leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, new_leader);
  loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();

  PALF_LOG(INFO, "end test throttling_minor_follower", K(id));
  leader.reset();
  new_leader.reset();
  new_leader_v2.reset();
  revert_cluster_palf_handle_guard(palf_list);
  delete_paxos_group(id);
  PALF_LOG(INFO, "destroy", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
