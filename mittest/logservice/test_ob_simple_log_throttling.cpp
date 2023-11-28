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

const std::string TEST_NAME = "log_throttling";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
using namespace palf;
namespace unittest
{
class TestObSimpleLogClusterLogThrottling : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterLogThrottling() :  ObSimpleLogClusterTestEnv()
  {}
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

MockLocCB loc_cb;

TEST_F(TestObSimpleLogClusterLogThrottling, test_throttling_sys)
{
  SET_CASE_LOG_FILE(TEST_NAME, "log_throttling_sys_log_stream");
  int ret = OB_SUCCESS;
  const int64_t id = 1;
  PALF_LOG(INFO, "begin test throttling_sy", K(id));
  int64_t leader_idx = 0;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
  PalfHandleImplGuard leader;
  PalfEnv *palf_env = NULL;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  const PalfDiskOptions disk_options = palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_;
  loc_cb.leader_ = get_cluster()[leader_idx]->get_addr();

  const int64_t MB = 1024 * 1024L;
  int64_t total_disk_size = 400 * MB ;
  int64_t utilization_limit_threshold = 95;
  int64_t throttling_percentage = 60;

  PalfEnvImpl &palf_env_impl = palf_env->palf_env_impl_;
  palf_env_impl.is_inited_ = true;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ = total_disk_size;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_threshold_ = 90;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_limit_threshold_ = utilization_limit_threshold;
  int64_t unrecyclable_size = 0;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  PalfThrottleOptions throttle_options;
  palf_env_impl.disk_options_wrapper_.get_throttling_options(throttle_options);
  LogIOWorker *log_io_worker = leader.palf_handle_impl_->log_engine_.log_io_worker_;
  LogWritingThrottle *throttle = log_io_worker->throttle_;
  // sys log stream no need throttling
  PalfThrottleOptions invalid_throttle_options;

  PALF_LOG(INFO, "prepare for throttling");
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 140, id, 2 * MB));
  LSN max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);

  PALF_LOG(INFO, "test sys log stream will not been throttled");
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  int64_t cur_ts = common::ObClockGenerator::getClock();
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 2 * MB));
  max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);
  ASSERT_EQ(invalid_throttle_options, throttle->throttling_options_);
  ASSERT_EQ(false, throttle->need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(false, throttle->need_throttling_not_guarded_by_lock_(log_io_worker->need_purging_throttling_func_));
  int64_t break_ts = common::ObClockGenerator::getClock();
  ASSERT_EQ(true, (break_ts - cur_ts) < 1 * 1000 * 1000);

  PALF_LOG(INFO, "end test throttling_sys_log_stream", K(id));
  leader.reset();

  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_ = disk_options;
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_ = disk_options;
  delete_paxos_group(id);
}

TEST_F(TestObSimpleLogClusterLogThrottling, test_throttling_basic)
{
  SET_CASE_LOG_FILE(TEST_NAME, "log_throttling_basic");
  int ret = OB_SUCCESS;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test throttlin_basic", K(id));
  int64_t leader_idx = 0;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
  PalfHandleImplGuard leader;
  PalfEnv *palf_env = NULL;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  loc_cb.leader_ = get_cluster()[leader_idx]->get_addr();

  const int64_t MB = 1024 * 1024L;
  int64_t total_disk_size = 300 * MB ;
  int64_t utilization_limit_threshold = 95;
  int64_t throttling_percentage = 40;

  PalfEnvImpl &palf_env_impl = palf_env->palf_env_impl_;
  palf_env_impl.is_inited_ = true;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ = total_disk_size;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_threshold_ = 80;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_limit_threshold_ = utilization_limit_threshold;
  int64_t unrecyclable_size = 0;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  PalfThrottleOptions throttle_options;
  palf_env_impl.disk_options_wrapper_.get_throttling_options(throttle_options);
  LogIOWorker *log_io_worker = leader.palf_handle_impl_->log_engine_.log_io_worker_;
  LogWritingThrottle *throttle = log_io_worker->throttle_;
  PalfThrottleOptions invalid_throttle_options;

  PALF_LOG(INFO, "[CASE 1]: test no need throttling while unrecyclable_log_disk_size is no more than trigger_size");
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 40, id, 1 * MB));
  LSN max_lsn_1 = leader.palf_handle_impl_->sw_.get_max_lsn();
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  wait_lsn_until_flushed(max_lsn_1, leader);
  ASSERT_EQ(true, throttle->last_update_ts_ != OB_INVALID_TIMESTAMP);
  ASSERT_EQ(invalid_throttle_options, throttle->throttling_options_);
  ASSERT_EQ(false, throttle->need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(false, throttle->need_throttling_not_guarded_by_lock_(log_io_worker->need_purging_throttling_func_));

  PALF_LOG(INFO, "[CASE 2] no need throttling while log_disk_throttling_percentage_ is off");
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 100;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 31, id, 1 * MB));
  max_lsn_1 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_1, leader);
  ASSERT_EQ(invalid_throttle_options, throttle->throttling_options_);
  ASSERT_EQ(false, throttle->need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(false, throttle->need_throttling_not_guarded_by_lock_(log_io_worker->need_purging_throttling_func_));

  PALF_LOG(INFO, "[CASE 3] need throttling");
  LogEntryHeader log_entry_header;
  LogGroupEntryHeader group_header;
  const int64_t log_entry_header_size = log_entry_header.get_serialize_size();
  const int64_t log_group_header_size = group_header.get_serialize_size();
  const int64_t header_size =log_entry_header_size + log_group_header_size;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  palf_env_impl.disk_options_wrapper_.get_throttling_options(throttle_options);
  LSN before_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * MB));
  LSN end_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  const int64_t log_size = end_lsn - before_lsn;
  max_lsn_1 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_1, leader);
  PALF_LOG(INFO, "case 3: after submit_log", K(before_lsn), K(end_lsn), K(max_lsn_1));
  ASSERT_EQ(throttle_options, throttle->throttling_options_);
  ASSERT_EQ(true, throttle->need_throttling_not_guarded_by_lock_(log_io_worker->need_purging_throttling_func_));
  ASSERT_EQ(true, throttle->stat_.has_ever_throttled());
  ASSERT_EQ(log_size, throttle->stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle->stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle->stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle->stat_.total_skipped_size_);
  ASSERT_EQ(log_size, throttle->appended_log_size_cur_round_);
  ASSERT_EQ(5, log_io_worker->purge_throttling_task_submitted_seq_);
  ASSERT_EQ(5, log_io_worker->purge_throttling_task_handled_seq_);

  PALF_LOG(INFO, "[CASE 4] no need throttling because trigger_percentage is set to 100", KPC(throttle));
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 100;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * MB));
  max_lsn_1 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_1, leader);
  ASSERT_EQ(invalid_throttle_options, throttle->throttling_options_);
  ASSERT_EQ(false, throttle->need_throttling_not_guarded_by_lock_(log_io_worker->need_purging_throttling_func_));
  ASSERT_EQ(true, throttle->stat_.has_ever_throttled());
  ASSERT_EQ(log_size, throttle->stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle->stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle->stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle->stat_.total_skipped_size_);
  ASSERT_EQ(0, throttle->appended_log_size_cur_round_);
  ASSERT_EQ(5, log_io_worker->purge_throttling_task_submitted_seq_);
  ASSERT_EQ(5, log_io_worker->purge_throttling_task_handled_seq_);

  PALF_LOG(INFO, "[CASE 5] no need throttling because log_disk_size is set to 500M", KPC(throttle));
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 40;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  palf_env_impl.disk_options_wrapper_.get_throttling_options(throttle_options);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * MB));
  max_lsn_1 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_1, leader);
  ASSERT_EQ(throttle_options, throttle->throttling_options_);
  ASSERT_EQ(true, throttle->need_throttling_not_guarded_by_lock_(log_io_worker->need_purging_throttling_func_));
  ASSERT_EQ(true, throttle->stat_.has_ever_throttled());
  PALF_LOG(INFO, "[CASE 5] no need throttling because log_disk_size is set to 500M", K(throttle));
  ASSERT_EQ(log_size, throttle->stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle->stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle->stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle->stat_.total_skipped_size_);
  ASSERT_EQ(log_size, throttle->appended_log_size_cur_round_);
  ASSERT_EQ(5, log_io_worker->purge_throttling_task_submitted_seq_);
  ASSERT_EQ(5, log_io_worker->purge_throttling_task_handled_seq_);
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ = 500 * MB;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  palf_env_impl.disk_options_wrapper_.get_throttling_options(throttle_options);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1 * MB));
  max_lsn_1 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_1, leader);
  ASSERT_EQ(invalid_throttle_options, throttle->throttling_options_);
  ASSERT_EQ(false, throttle->need_throttling_not_guarded_by_lock_(log_io_worker->need_purging_throttling_func_));
  ASSERT_EQ(true, throttle->stat_.has_ever_throttled());
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != throttle->stat_.stop_ts_);
  ASSERT_EQ(log_size, throttle->stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle->stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle->stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle->stat_.total_skipped_size_);
  ASSERT_EQ(0, throttle->appended_log_size_cur_round_);
  max_lsn_1 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_1, leader);

  int64_t prev_has_batched_size = log_io_worker->batch_io_task_mgr_.handle_count_;
  PALF_LOG(INFO, "[CASE 6] flush meta task no need throttling", K(max_lsn_1));
  int64_t cur_ts = common::ObClockGenerator::getClock();
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ = 300 * MB;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  LogEngine *log_engine = &leader.palf_handle_impl_->log_engine_;
  IOTaskCond io_task_cond_1(id, log_engine->palf_epoch_);
  EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_1));
  ASSERT_EQ(6, log_io_worker->purge_throttling_task_submitted_seq_);
  ASSERT_EQ(5, log_io_worker->purge_throttling_task_handled_seq_);
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 1 * MB));
  max_lsn_1 = leader.palf_handle_impl_->sw_.get_max_lsn();
  //wait all flush log tasks pushed into queue of LogIOWorker
  wait_lsn_until_submitted(max_lsn_1, leader);
  IOTaskCond io_task_cond_2(id, log_engine->palf_epoch_);
  EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_2));
  ASSERT_EQ(7, log_io_worker->purge_throttling_task_submitted_seq_);
  ASSERT_EQ(5, log_io_worker->purge_throttling_task_handled_seq_);
  io_task_cond_1.cond_.signal();
  wait_lsn_until_flushed(max_lsn_1, leader);
  usleep(10 * 1000);
  ASSERT_EQ(7, log_io_worker->purge_throttling_task_submitted_seq_);
  ASSERT_EQ(6, log_io_worker->purge_throttling_task_handled_seq_);
  io_task_cond_2.cond_.signal();
  usleep(10 * 1000);
  ASSERT_EQ(7, log_io_worker->purge_throttling_task_submitted_seq_);
  ASSERT_EQ(7, log_io_worker->purge_throttling_task_handled_seq_);
  palf_env_impl.disk_options_wrapper_.get_throttling_options(throttle_options);
  ASSERT_EQ(true, throttle->need_throttling_not_guarded_by_lock_(log_io_worker->need_purging_throttling_func_));
  ASSERT_EQ(true, throttle->stat_.has_ever_throttled());
  ASSERT_EQ(true, throttle->stat_.start_ts_ > cur_ts);
  ASSERT_EQ(0, throttle->stat_.total_throttling_size_);
  ASSERT_EQ(0, throttle->stat_.total_throttling_task_cnt_);
  ASSERT_EQ(10, throttle->stat_.total_skipped_task_cnt_);
  ASSERT_EQ(10 * (log_size), throttle->stat_.total_skipped_size_);
  int64_t cur_has_batched_size = log_io_worker->batch_io_task_mgr_.handle_count_;
  // no io reduce during writing throttling
  ASSERT_EQ(cur_has_batched_size, prev_has_batched_size);
  const double old_decay_factor = throttle->decay_factor_;

  PALF_LOG(INFO, "[CASE 7] defactor is will change when log_disk_throttling_maximum_duration changes", K(max_lsn_1));
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_maximum_duration_ = 1800 * 1000 * 1000L;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, id, 1024));
  max_lsn_1 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_1, leader);
  PALF_LOG(INFO, "YYY  change when log_disk_throttling_maximum_duration changes", K(old_decay_factor), KPC(throttle));
  ASSERT_EQ(true, throttle->decay_factor_ < old_decay_factor);


  PALF_LOG(INFO, "[CASE 8] need break from writing throttling while unrecyclable size fallbacks", K(max_lsn_1));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, id, 1 * MB));

  cur_ts = common::ObClockGenerator::getClock();
  leader.get_palf_handle_impl()->log_engine_.update_base_lsn_used_for_gc(LSN(70 * MB));
  LSN max_lsn_2 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_2, leader);
  int64_t break_ts = common::ObClockGenerator::getClock();
  EXPECT_EQ(true, (break_ts-cur_ts) < 1 * 1000 * 1000);
  ASSERT_EQ(false, throttle->need_throttling_not_guarded_by_lock_(log_io_worker->need_purging_throttling_func_));
  ASSERT_EQ(true, throttle->stat_.has_ever_throttled());


  palf_env_impl.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_throttling_percentage_ = throttling_percentage;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_ = 400 * MB;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_utilization_threshold_ = 70;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_utilization_limit_threshold_ = utilization_limit_threshold;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ = 400 * MB;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 100;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 85, id, 2 * MB));
  max_lsn_2 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_2, leader);
  leader.get_palf_handle_impl()->log_engine_.update_base_lsn_used_for_gc(LSN(130 * MB));

  usleep(1000 * 1000);//wait for gc
  LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
  LSN begin_lsn;
  leader.get_palf_handle_impl()->get_begin_lsn(begin_lsn);
  ASSERT_EQ(true, begin_lsn > LSN(0));

  palf_env_impl.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_utilization_threshold_ = 94;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_threshold_ = 94;
  //EXPECT_EQ(OB_SUCCESS, submit_log(leader, 35, id, 2 * MB));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, id, 2 * MB));
  max_lsn_2 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_2, leader);

  max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
  leader.get_palf_handle_impl()->get_begin_lsn(begin_lsn);

  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 40;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);

  cur_ts = common::ObClockGenerator::getClock();
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 2 * MB));
  max_lsn_2 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_2, leader);
  break_ts = common::ObClockGenerator::getClock();
  EXPECT_EQ(true, (break_ts - cur_ts) > 1 * 1000 * 1000);

  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  PALF_LOG(INFO, "[CASE 8] need break from writing throttling while log_disk_throttling_percentage_ is off", K(throttle));
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 2 * MB));
  usleep(1 * 1000);//sleep to come into throttling
  cur_ts = common::ObClockGenerator::getClock();
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 100;
  LSN max_lsn_3 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_3, leader);
  break_ts = common::ObClockGenerator::getClock();
  EXPECT_EQ(true, (break_ts - cur_ts) < 1 * 1000 * 1000);

  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 40;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  PALF_LOG(INFO, "[CASE 9] need break from writing throttling while flush meta task is submitted", K(throttle));
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 2 * MB));
  usleep(1000 * 1000);
  IOTaskCond io_task_cond_3(id, log_engine->palf_epoch_);
  EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_3));
  cur_ts = common::ObClockGenerator::getClock();
  max_lsn_3 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_3, leader);
  break_ts = common::ObClockGenerator::getClock();
  EXPECT_EQ(true, (break_ts - cur_ts) < 1 * 1000 * 1000);
  usleep(100 * 1000);
  io_task_cond_3.cond_.signal();
  usleep(100 * 1000);

  PALF_LOG(INFO, "[CASE 10] no io reduce during writing throttling", K(throttle));
  int64_t batched_size = log_io_worker->batch_io_task_mgr_.handle_count_;
  leader.palf_handle_impl_->sw_.freeze_mode_ = PERIOD_FREEZE_MODE;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id, 128));
  max_lsn_3 = leader.palf_handle_impl_->sw_.get_max_lsn();
  wait_lsn_until_flushed(max_lsn_3, leader);
  int64_t new_batched_size = log_io_worker->batch_io_task_mgr_.handle_count_;
  EXPECT_EQ(batched_size, new_batched_size);

  PALF_LOG(INFO, "[CASE 11] need break from writing throttling while flashback task is submitted", K(throttle));
  int64_t mode_version = INVALID_PROPOSAL_ID;
  SCN max_scn_before_flashback = leader.palf_handle_impl_->get_max_scn();
  LSN max_lsn_before_flashback = leader.palf_handle_impl_->sw_.get_max_lsn();
  cur_ts = common::ObClockGenerator::getClock();
  const int64_t KB = 1024;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 1024 * KB));
  usleep(20 * 1000);
  switch_append_to_flashback(leader, mode_version);
  constexpr int64_t timeout_ts_us = 3 * 1000 * 1000;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, max_scn_before_flashback, timeout_ts_us));
  LSN max_lsn_after_flashback = leader.palf_handle_impl_->sw_.get_max_lsn();
  SCN max_scn_after_flashback = leader.palf_handle_impl_->sw_.get_max_scn();
  ASSERT_EQ(max_lsn_after_flashback, max_lsn_before_flashback);
  ASSERT_EQ(max_scn_after_flashback, max_scn_before_flashback);

  PALF_LOG(INFO, "end test throttling_basic", K(id));
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "destroy", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
