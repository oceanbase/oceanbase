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

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include <cstdio>
#include <gtest/gtest.h>
#include <signal.h>
#include <stdexcept>
#define private public
#include "env/ob_simple_log_cluster_env.h"
#undef private
#include "logservice/palf/log_reader_utils.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/log_io_worker.h"
#include "logservice/palf/lsn.h"

const std::string TEST_NAME = "single_replica";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class TestObSimpleLogClusterSingleReplica : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterSingleReplica() : ObSimpleLogClusterTestEnv()
  {
    int ret = init();
    if (OB_SUCCESS != ret) {
      throw std::runtime_error("TestObSimpleLogClusterLogEngine init failed");
    }
  }
  ~TestObSimpleLogClusterSingleReplica()
  {
    destroy();
  }
  int init()
  {
    return OB_SUCCESS;
  }
  void destroy()
  {}
  int64_t id_;
  PalfHandleGuard leader_;
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;

TEST_F(TestObSimpleLogClusterSingleReplica, update_disk_options)
{
  SET_CASE_LOG_FILE(TEST_NAME, "update_disk_options");
  OB_LOGGER.set_log_level("TRACE");
  const int64_t id = ATOMIC_FAA(&palf_id_, 1);
  PALF_LOG(INFO, "start update_disk_options", K(id));
  int64_t leader_idx = 0;
  PalfHandleGuard leader;
  PalfEnv *palf_env = NULL;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  PalfDiskOptions disk_opts;
  EXPECT_EQ(OB_SUCCESS, palf_env->get_disk_options(disk_opts));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 4 * 32 + 10, id, MAX_LOG_BODY_SIZE));
  while (leader.palf_handle_.palf_handle_impl_->log_engine_.log_storage_.log_tail_
         < LSN(4 * 32 * MAX_LOG_BODY_SIZE)) {
    sleep(1);
  }
  block_id_t min_block_id, max_block_id;
  EXPECT_EQ(OB_SUCCESS,
            leader.palf_handle_.palf_handle_impl_->log_engine_.get_block_id_range(
                min_block_id, max_block_id));
  EXPECT_EQ(4, max_block_id);
  disk_opts.log_disk_usage_limit_size_ = 8 * PALF_PHY_BLOCK_SIZE;
  EXPECT_EQ(OB_SUCCESS, palf_env->update_disk_options(disk_opts));
  sleep(1);
  disk_opts.log_disk_utilization_limit_threshold_ = 50;
  disk_opts.log_disk_utilization_threshold_ = 40;
  EXPECT_EQ(OB_SUCCESS, palf_env->update_disk_options(disk_opts));

  disk_opts.log_disk_usage_limit_size_ = 4 * PALF_PHY_BLOCK_SIZE;
  EXPECT_EQ(OB_STATE_NOT_MATCH, palf_env->update_disk_options(disk_opts));

  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_ = palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_;
  palf_env->palf_env_impl_.disk_options_wrapper_.status_ = PalfDiskOptionsWrapper::Status::NORMAL_STATUS;

  disk_opts.log_disk_utilization_limit_threshold_ = 95;
  disk_opts.log_disk_utilization_threshold_ = 80;
  EXPECT_EQ(OB_SUCCESS, palf_env->update_disk_options(disk_opts));
  EXPECT_EQ(OB_STATE_NOT_MATCH, palf_env->update_disk_options(disk_opts));
  sleep(1);
  EXPECT_EQ(PalfDiskOptionsWrapper::Status::SHRINKING_STATUS,
            palf_env->palf_env_impl_.disk_options_wrapper_.status_);
  EXPECT_GT(palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_,
            palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_);
  EXPECT_EQ(OB_SUCCESS, leader.advance_base_lsn(LSN(4 * PALF_BLOCK_SIZE)));
  EXPECT_EQ(OB_SUCCESS, leader.advance_base_lsn(LSN(2 * PALF_BLOCK_SIZE)));
  EXPECT_EQ(LSN(4*PALF_BLOCK_SIZE), leader.palf_handle_.palf_handle_impl_->log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_);
  while (leader.palf_handle_.palf_handle_impl_->log_engine_.base_lsn_for_block_gc_
         != LSN(4 * PALF_BLOCK_SIZE)) {
    sleep(1);
  }
  // wait blocks to be recycled
  while (leader.palf_handle_.palf_handle_impl_->log_engine_.get_base_lsn_used_for_block_gc() != LSN(4*PALF_BLOCK_SIZE)) {
    sleep(1);
  }
  sleep(1);
  EXPECT_EQ(PalfDiskOptionsWrapper::Status::NORMAL_STATUS,
            palf_env->palf_env_impl_.disk_options_wrapper_.status_);
  EXPECT_EQ(
      disk_opts,
      palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_);
  EXPECT_EQ(
      disk_opts,
      palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_);
  PALF_LOG(INFO, "runlin trace", K(disk_opts),
           "holders:", palf_env->palf_env_impl_.disk_options_wrapper_);
  // test expand
  disk_opts.log_disk_usage_limit_size_ = 5 * 1024 * 1024 * 1024ul;
  EXPECT_EQ(OB_SUCCESS, palf_env->update_disk_options(disk_opts));
  disk_opts.log_disk_usage_limit_size_ = 6 * 1024 * 1024 * 1024ul;
  EXPECT_EQ(OB_SUCCESS, palf_env->update_disk_options(disk_opts));
  EXPECT_EQ(
      disk_opts,
      palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_);
  EXPECT_EQ(
      disk_opts,
      palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_);
}

TEST_F(TestObSimpleLogClusterSingleReplica, delete_paxos_group)
{
  SET_CASE_LOG_FILE(TEST_NAME, "delete_paxos_group");
  const int64_t id = ATOMIC_FAA(&palf_id_, 1);
  PALF_LOG(INFO, "start test delete_paxos_group", K(id));
  int64_t leader_idx = 0;
  {
    palf::PalfHandleGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, leader_idx));
  }
  sleep(1);
  // EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
  // TODO by yunlong: check log sync
  PALF_LOG(INFO, "end test delete_paxos_group", K(id));
}

TEST_F(TestObSimpleLogClusterSingleReplica, advance_base_lsn)
{
  SET_CASE_LOG_FILE(TEST_NAME, "advance_base_lsn");
  OB_LOGGER.set_log_level("INFO");
  const int64_t id = ATOMIC_FAA(&palf_id_, 1);
  PALF_LOG(INFO, "start advance_base_lsn", K(id));
  int64_t leader_idx = 0;
  int64_t log_ts = 1;
  {
    PalfHandleGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    sleep(2);
    LSN log_tail =
        leader.palf_handle_.palf_handle_impl_->log_engine_.log_meta_storage_.log_tail_;
    for (int64_t i = 0; i < 4096; i++) {
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_.enable_vote());
    }
    sleep(3);
    EXPECT_EQ(
        LSN(4096 * 4096 + log_tail.val_),
        leader.palf_handle_.palf_handle_impl_->log_engine_.log_meta_storage_.log_tail_);
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    PalfHandleGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_.advance_base_lsn(LSN(0)));
  }
}

TEST_F(TestObSimpleLogClusterSingleReplica, test_truncate_failed)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_truncate_failed");
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  int64_t file_size = 0;
  {
    PalfHandleGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 1000));
    wait_lsn_until_flushed(leader.palf_handle_.palf_handle_impl_->get_max_lsn(), leader);
    LSN max_lsn = leader.palf_handle_.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 1000));
    wait_lsn_until_flushed(leader.palf_handle_.palf_handle_impl_->get_max_lsn(), leader);
    int64_t fd = leader.palf_handle_.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.curr_writable_handler_.io_fd_;
    block_id_t block_id = leader.palf_handle_.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.curr_writable_block_id_;
    char *log_dir = leader.palf_handle_.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.log_dir_;
    convert_to_normal_block(log_dir, block_id, block_path, OB_MAX_FILE_NAME_LENGTH);
    EXPECT_EQ(OB_ITER_END, read_log(leader));
    PALF_LOG(ERROR, "truncate pos", K(max_lsn));
    EXPECT_EQ(0, ftruncate(fd, max_lsn.val_));
    FileDirectoryUtils::get_file_size(block_path, file_size);
    EXPECT_EQ(file_size, max_lsn.val_);
  }
  PalfHandleGuard leader;
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());;
  FileDirectoryUtils::get_file_size(block_path, file_size);
  EXPECT_EQ(file_size, PALF_PHY_BLOCK_SIZE);
  get_leader(id, leader, leader_idx);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 1000));
  EXPECT_EQ(OB_ITER_END, read_log(leader));
}

TEST_F(TestObSimpleLogClusterSingleReplica, test_meta)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_meta");
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  LSN upper_aligned_log_tail;
  {
    PalfHandleGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    sleep(1);
    // 测试meta文件刚好写满的重启场景
    LogEngine *log_engine = &leader.palf_handle_.palf_handle_impl_->log_engine_;
    LogStorage *log_meta_storage = &log_engine->log_meta_storage_;
    LSN log_meta_tail = log_meta_storage->log_tail_;
    upper_aligned_log_tail.val_ = (lsn_2_block(log_meta_tail, PALF_META_BLOCK_SIZE) + 1) * PALF_META_BLOCK_SIZE;
    int64_t delta = upper_aligned_log_tail - log_meta_tail;
    int64_t delta_cnt = delta / MAX_META_ENTRY_SIZE;
    while (delta_cnt-- > 0) {
      log_engine->append_log_meta_(log_engine->log_meta_);
    }
    EXPECT_EQ(upper_aligned_log_tail, log_meta_storage->log_tail_);
    PALF_LOG(ERROR, "runlin trace before restart", K(upper_aligned_log_tail), KPC(log_meta_storage));
  }

  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  {
    PalfHandleGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    LogEngine *log_engine = &leader.palf_handle_.palf_handle_impl_->log_engine_;
    LogStorage *log_meta_storage = &log_engine->log_meta_storage_;
    LSN log_meta_tail = log_meta_storage->log_tail_;
    upper_aligned_log_tail.val_ = (lsn_2_block(log_meta_tail, PALF_META_BLOCK_SIZE) + 1) * PALF_META_BLOCK_SIZE;
    int64_t delta = upper_aligned_log_tail - log_meta_tail;
    int64_t delta_cnt = delta / MAX_META_ENTRY_SIZE;
    while (delta_cnt-- > 0) {
      log_engine->append_log_meta_(log_engine->log_meta_);
    }
    EXPECT_EQ(upper_aligned_log_tail, log_meta_storage->log_tail_);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 32, id, MAX_LOG_BODY_SIZE));
    sleep(1);
    wait_lsn_until_flushed(leader.palf_handle_.palf_handle_impl_->get_max_lsn(), leader);
    block_id_t min_block_id, max_block_id;
    EXPECT_EQ(OB_SUCCESS, log_meta_storage->get_block_id_range(min_block_id, max_block_id));
    EXPECT_EQ(min_block_id, max_block_id);
  }
}

TEST_F(TestObSimpleLogClusterSingleReplica, test_gc_block)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_gc_block");
  OB_LOGGER.set_log_level("TRACE");
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  LSN upper_aligned_log_tail;
  PalfHandleGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  LogEngine *log_engine = &leader.palf_handle_.palf_handle_impl_->log_engine_;
  LogStorage *log_meta_storage = &log_engine->log_meta_storage_;
  block_id_t min_block_id;
  share::SCN min_block_scn;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, log_engine->get_min_block_info_for_gc(min_block_id, min_block_scn));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 31, leader_idx, MAX_LOG_BODY_SIZE));
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_.palf_handle_impl_->get_max_lsn(), leader));
  EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND, log_engine->get_min_block_info_for_gc(min_block_id, min_block_scn));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, MAX_LOG_BODY_SIZE));
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_.palf_handle_impl_->get_max_lsn(), leader));
  block_id_t expect_block_id = 1;
  share::SCN expect_scn;
  EXPECT_EQ(OB_SUCCESS, log_engine->get_min_block_info_for_gc(min_block_id, min_block_scn));
  EXPECT_EQ(OB_SUCCESS, log_engine->get_block_min_scn(expect_block_id, expect_scn));
  EXPECT_EQ(expect_scn, min_block_scn);
  EXPECT_EQ(OB_SUCCESS, log_engine->delete_block(0));
  EXPECT_EQ(false, log_engine->min_block_max_scn_.is_valid());

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, leader_idx, MAX_LOG_BODY_SIZE));
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_.palf_handle_impl_->get_max_lsn(), leader));
  expect_block_id = 2;
  EXPECT_EQ(OB_SUCCESS, log_engine->get_min_block_info_for_gc(min_block_id, min_block_scn));
  EXPECT_EQ(OB_SUCCESS, log_engine->get_block_min_scn(expect_block_id, expect_scn));
  EXPECT_EQ(expect_scn, min_block_scn);
  EXPECT_EQ(OB_SUCCESS, log_engine->delete_block(1));
  expect_block_id = 3;
  EXPECT_EQ(OB_SUCCESS, log_engine->get_min_block_info_for_gc(min_block_id, min_block_scn));
  EXPECT_EQ(OB_SUCCESS, log_engine->get_block_min_scn(expect_block_id, expect_scn));
  EXPECT_EQ(expect_scn, min_block_scn);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
