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
#define protected public
#include "env/ob_simple_log_cluster_env.h"
#undef private
#undef protected
#include "logservice/palf/log_reader_utils.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/log_io_worker.h"
#include "logservice/palf/lsn.h"

const std::string TEST_NAME = "log_disk_mgr";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class TestObSimpleLogDiskMgr : public ObSimpleLogClusterTestEnv
  {
  public:
    TestObSimpleLogDiskMgr() : ObSimpleLogClusterTestEnv()
    {
      int ret = init();
      if (OB_SUCCESS != ret) {
        throw std::runtime_error("TestObSimpleLogDiskMgr init failed");
      }
    }
    ~TestObSimpleLogDiskMgr()
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
  };

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;
int64_t log_entry_size = 2 * 1024 * 1024 + 16 * 1024;

TEST_F(TestObSimpleLogDiskMgr, out_of_disk_space)
{
  update_server_log_disk(10*1024*1024*1024ul);
  SET_CASE_LOG_FILE(TEST_NAME, "out_of_disk_space");
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int server_idx = 0;
  PalfEnv *palf_env = NULL;
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  share::SCN create_scn = share::SCN::base_scn();
  EXPECT_EQ(OB_SUCCESS, get_palf_env(server_idx, palf_env));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_scn, leader_idx, leader));
  update_disk_options(leader_idx, MIN_DISK_SIZE_PER_PALF_INSTANCE/PALF_PHY_BLOCK_SIZE + 2);
  sleep(2);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 8*31+1, id, log_entry_size));
  LogStorage *log_storage = &leader.palf_handle_impl_->log_engine_.log_storage_;
  while (LSN(6*PALF_BLOCK_SIZE) > log_storage->log_tail_) {
    usleep(500);
  }
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, id, log_entry_size));
  while (LSN(6*PALF_BLOCK_SIZE + 20 * log_entry_size) > log_storage->log_tail_) {
    usleep(500);
  }
  LSN max_lsn = leader.palf_handle_impl_->get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);
  PALF_LOG(INFO, "out of disk max_lsn", K(max_lsn));
  sleep(2);
  EXPECT_EQ(OB_LOG_OUTOF_DISK_SPACE, submit_log(leader, 1, id, MAX_LOG_BODY_SIZE));
  // shrinking 后继续停写
  update_disk_options(leader_idx, MIN_DISK_SIZE_PER_PALF_INSTANCE/PALF_PHY_BLOCK_SIZE);
  EXPECT_EQ(OB_LOG_OUTOF_DISK_SPACE, submit_log(leader, 1, id, MAX_LOG_BODY_SIZE));
  usleep(ObLooper::INTERVAL_US*2);
}

TEST_F(TestObSimpleLogDiskMgr, update_disk_options_basic)
{
  SET_CASE_LOG_FILE(TEST_NAME, "update_disk_options_basic");
  OB_LOGGER.set_log_level("INFO");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  // 将日志盘空间设置为10GB
  update_disk_options(10*1024*1024*1024ul/PALF_PHY_BLOCK_SIZE);
  sleep(2);
  PALF_LOG(INFO, "start update_disk_options_basic", K(id));
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  PalfEnv *palf_env = NULL;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));

  // 提交1G的日志
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 500, id, log_entry_size));
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));

  EXPECT_EQ(OB_SUCCESS, update_disk_options(leader_idx, 20));
  sleep(2);
  EXPECT_EQ(PalfDiskOptionsWrapper::Status::SHRINKING_STATUS,
            palf_env->palf_env_impl_.disk_options_wrapper_.status_);
  EXPECT_EQ(palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_,
            20*PALF_PHY_BLOCK_SIZE);

  // case1: 在上一次未缩容完成之前，可以继续缩容
  EXPECT_EQ(OB_SUCCESS, update_disk_options(leader_idx, 10));
  sleep(2);
  EXPECT_EQ(PalfDiskOptionsWrapper::Status::SHRINKING_STATUS,
            palf_env->palf_env_impl_.disk_options_wrapper_.status_);
  EXPECT_EQ(palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_,
            10*PALF_PHY_BLOCK_SIZE);
  // 此时日志盘依旧未完成缩容，ObSimpleLogServer维护的disk_opts_依旧是10GB
  {
    PalfDiskOptions opts;
    EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
    EXPECT_EQ(opts.log_disk_usage_limit_size_, 10*1024*1024*1024ul);
  }
  // case2: 在上一次未缩容完成之前，可以继续扩容, 同时由于扩容后日志盘依旧小于第一次缩容，因此依旧处于缩容状态.
  EXPECT_EQ(OB_SUCCESS, update_disk_options(leader_idx, 11));
  sleep(2);
  EXPECT_EQ(PalfDiskOptionsWrapper::Status::SHRINKING_STATUS,
            palf_env->palf_env_impl_.disk_options_wrapper_.status_);
  EXPECT_EQ(palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_,
            11*PALF_PHY_BLOCK_SIZE);
  // 此时日志盘依旧未完成缩容，ObSimpleLogServer维护的disk_opts_依旧是10GB
  {
    PalfDiskOptions opts;
    EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
    EXPECT_EQ(opts.log_disk_usage_limit_size_, 10*1024*1024*1024ul);
  }
  sleep(2);
  EXPECT_EQ(PalfDiskOptionsWrapper::Status::SHRINKING_STATUS,
            palf_env->palf_env_impl_.disk_options_wrapper_.status_);
  EXPECT_EQ(palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_,
            11*PALF_PHY_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
  const LSN base_lsn(12*PALF_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->set_base_lsn(base_lsn));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1000));
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
  // wait until disk space enough
  EXPECT_EQ(OB_SUCCESS, wait_until_disk_space_to(leader_idx, (11*PALF_PHY_BLOCK_SIZE*80+100)/100));
  sleep(2);
  EXPECT_EQ(PalfDiskOptionsWrapper::Status::NORMAL_STATUS,
            palf_env->palf_env_impl_.disk_options_wrapper_.status_);
  EXPECT_EQ(palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_,
            11*PALF_PHY_BLOCK_SIZE);
  // 等待后台线程再次执行update_disk_options操作，预期本地持久化的disk_opts会变为11*PALF_PHY_BLOCK_SIZE
  {
    sleep(2);
    PalfDiskOptions opts;
    EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
    EXPECT_EQ(opts.log_disk_usage_limit_size_, 11*PALF_PHY_BLOCK_SIZE);
  }
}

TEST_F(TestObSimpleLogDiskMgr, shrink_log_disk)
{
  SET_CASE_LOG_FILE(TEST_NAME, "shrink_log_disk");
  OB_LOGGER.set_log_level("INFO");
  // 验证缩容由于单日志流最少需要512MB日志盘失败
  // 保证能同时容纳两个日志流
  PalfEnv *palf_env = NULL;
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  share::SCN create_scn = share::SCN::base_scn();
  int server_idx = 0;
  EXPECT_EQ(OB_SUCCESS, get_palf_env(server_idx, palf_env));
  EXPECT_EQ(OB_SUCCESS, update_disk_options(16));
  EXPECT_EQ(PalfDiskOptionsWrapper::Status::NORMAL_STATUS,
            palf_env->palf_env_impl_.disk_options_wrapper_.status_);
  EXPECT_EQ(palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_,
            16*PALF_PHY_BLOCK_SIZE);
  int64_t tmp_id1 = ATOMIC_AAF(&palf_id_, 1);
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(tmp_id1, leader_idx, leader));
  }
  int64_t tmp_id2 = ATOMIC_AAF(&palf_id_, 1);
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(tmp_id2, leader_idx, leader));
  }
  EXPECT_EQ(OB_NOT_SUPPORTED, update_disk_options(9));
  EXPECT_EQ(OB_SUCCESS, delete_paxos_group(tmp_id1));
}

TEST_F(TestObSimpleLogDiskMgr, update_disk_options_restart)
{
  disable_hot_cache_ = true;
  SET_CASE_LOG_FILE(TEST_NAME, "update_disk_options_restart");
  OB_LOGGER.set_log_level("INFO");
  // 扩容操作
  EXPECT_EQ(OB_SUCCESS, update_disk_options(10*1024*1024*1024ul/PALF_PHY_BLOCK_SIZE));
  sleep(2);
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  PalfEnv *palf_env = NULL;
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    {
      PalfDiskOptions opts;
      EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
      EXPECT_EQ(opts.log_disk_usage_limit_size_, 10*1024*1024*1024ul);
    }
    EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
    {
      EXPECT_EQ(PalfDiskOptionsWrapper::Status::NORMAL_STATUS,
                palf_env->palf_env_impl_.disk_options_wrapper_.status_);
      EXPECT_EQ(10*1024*1024*1024ul,
                palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_);
    }
    // 产生10个文件的数据
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10*32, id, log_entry_size));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    // 最小的log_disk_size要求是存在8个日志文件
    EXPECT_EQ(OB_SUCCESS, update_disk_options(leader_idx, 8));
    sleep(2);
    // 宕机前，缩容不会正式生效，因此不会导致停写
    EXPECT_EQ(true, palf_env->palf_env_impl_.diskspace_enough_);
    EXPECT_EQ(PalfDiskOptionsWrapper::Status::SHRINKING_STATUS,
              palf_env->palf_env_impl_.disk_options_wrapper_.status_);
    EXPECT_EQ(10*1024*1024*1024ul, palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_);
    int64_t log_disk_usage, total_log_disk_size;
    EXPECT_EQ(OB_SUCCESS, palf_env->palf_env_impl_.get_disk_usage(log_disk_usage, total_log_disk_size));
    PALF_LOG(INFO, "log_disk_usage:", K(log_disk_usage), K(total_log_disk_size));
    // 缩容未成功前，log_disk_usage_limit_size_依旧保持10G.
    // 本地持久化的log_disk_size为10G
    // 内部表中持久化的log_disk_size为8*PALF_PHY_BLOCK_SIZE
    PalfDiskOptions opts;
    EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
    EXPECT_EQ(opts.log_disk_usage_limit_size_, 10*1024*1024*1024ul);
  }
  // 物理缩容未成功前，宕机重启预期不会停写
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    // 内部表中持久化的log_disk_size依旧是8*PALF_PHY_BLOCK_SIZE
    // 重启后继续缩容
    int64_t log_disk_usage, total_log_disk_size;
    EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
    usleep(2*1000*1000 + BlockGCTimerTask::BLOCK_GC_TIMER_INTERVAL_MS);
    EXPECT_EQ(true, palf_env->palf_env_impl_.diskspace_enough_);
    EXPECT_EQ(PalfDiskOptionsWrapper::Status::SHRINKING_STATUS,
              palf_env->palf_env_impl_.disk_options_wrapper_.status_);
    // 本地持久化的（slog）记录的依旧是10G，因此不会停写
    EXPECT_EQ(10*1024*1024*1024ul, palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_);
    EXPECT_EQ(OB_SUCCESS, palf_env->palf_env_impl_.get_disk_usage(log_disk_usage, total_log_disk_size));
    PALF_LOG(INFO, "log_disk_usage:", K(log_disk_usage), K(total_log_disk_size));
    PalfDiskOptions opts;
    EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
    EXPECT_EQ(opts.log_disk_usage_limit_size_, 10*1024*1024*1024ul);
  }
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    PalfDiskOptions opts;
    EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
    EXPECT_EQ(opts.log_disk_usage_limit_size_, 10*1024*1024*1024ul);

    // 物理上保证不缩容，内部表中持久化的变为16*PALF_PHY_BLOCK_SIZE, 由于palf的log_disk_size是10G,
    // 因此本次对palf是一次缩容操作. 但下一轮GC任务运行时，发现当前的使用的日志盘空间不会导致停写,
    // 于是日志盘变为正常状态
    EXPECT_EQ(OB_SUCCESS, update_disk_options(leader_idx, 16));
    // 在下一轮GC任务运行后，本地持久化的log_disk_size也会变为16*PALF_PHY_BLOCK_SIZE
    usleep(2*1000*1000+palf::BlockGCTimerTask::BLOCK_GC_TIMER_INTERVAL_MS);
    // 经过一轮GC后，会变为NORMAL_STATUS
    EXPECT_EQ(true, palf_env->palf_env_impl_.diskspace_enough_);
    EXPECT_EQ(PalfDiskOptionsWrapper::Status::NORMAL_STATUS,
              palf_env->palf_env_impl_.disk_options_wrapper_.status_);
    EXPECT_EQ(16*PALF_PHY_BLOCK_SIZE,
              palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_);
    // 后台线程会完成缩容操作，最终本地持久化的变为16*PALF_PHY_BLOCK_SIZE
    usleep(2*1000*1000+ObLooper::INTERVAL_US*2);
    EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
    EXPECT_EQ(opts.log_disk_usage_limit_size_, 16*PALF_PHY_BLOCK_SIZE);
  }
}

TEST_F(TestObSimpleLogDiskMgr, overshelling)
{
  disable_hot_cache_ = true;
  SET_CASE_LOG_FILE(TEST_NAME, "overshelling");
  OB_LOGGER.set_log_level("INFO");
  ObServerLogBlockMgr *log_pool = nullptr;
  EXPECT_EQ(OB_SUCCESS, get_log_pool(0, log_pool));
  ASSERT_NE(nullptr, log_pool);
  // 验证扩缩容场景下的LogPool字段的正确性
  EXPECT_EQ(16*PALF_PHY_BLOCK_SIZE, log_pool->min_log_disk_size_for_all_tenants_);
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  PalfEnv *palf_env = NULL;
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  }
  EXPECT_EQ(OB_SUCCESS, update_disk_options(leader_idx, 15));
  int64_t log_disk_size_used_for_tenants = log_pool->min_log_disk_size_for_all_tenants_;
  // 缩容还未成功，预期log_disk_size_used_for_tenants一定是16*PALF_PHY_BLOCK_SIZE
  PalfDiskOptions opts;
  EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
  if (opts.log_disk_usage_limit_size_ == 16*PALF_PHY_BLOCK_SIZE) {
    EXPECT_EQ(16*PALF_PHY_BLOCK_SIZE, log_disk_size_used_for_tenants);
    // 缩容不会立马生效
    usleep(2*1000*1000+ObLooper::INTERVAL_US*2);
    EXPECT_EQ(15*PALF_PHY_BLOCK_SIZE, log_pool->min_log_disk_size_for_all_tenants_);
  } else {
    PALF_LOG(INFO, "update_disk_options successfully", K(log_disk_size_used_for_tenants), K(opts));
  }

  // 扩容预期立马成功
  EXPECT_EQ(OB_SUCCESS, update_disk_options(leader_idx, 16));
  EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
  EXPECT_EQ(16*PALF_PHY_BLOCK_SIZE, log_disk_size_used_for_tenants);
  EXPECT_EQ(16*PALF_PHY_BLOCK_SIZE, opts.log_disk_usage_limit_size_);

  // 直接扩容为LogPool上限值
  const int64_t limit_log_disk_size = log_pool->log_pool_meta_.curr_total_size_;
  EXPECT_EQ(OB_SUCCESS, update_disk_options(leader_idx, limit_log_disk_size/PALF_PHY_BLOCK_SIZE));
  EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
  log_disk_size_used_for_tenants = log_pool->min_log_disk_size_for_all_tenants_;
  EXPECT_EQ(limit_log_disk_size, log_disk_size_used_for_tenants);
  EXPECT_EQ(limit_log_disk_size, opts.log_disk_usage_limit_size_);

  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    // 生成10个文件
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10*32, id, log_entry_size));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    EXPECT_EQ(OB_SUCCESS, update_disk_options(leader_idx, 10));
    // 缩容一定不会成功，租户日志盘规格依旧为上限值
    usleep(2*1000*1000+ObLooper::INTERVAL_US * 2);
    EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
    EXPECT_EQ(PalfDiskOptionsWrapper::Status::SHRINKING_STATUS,
              palf_env->palf_env_impl_.disk_options_wrapper_.status_);
    EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
    log_disk_size_used_for_tenants = log_pool->min_log_disk_size_for_all_tenants_;
    EXPECT_EQ(limit_log_disk_size, log_disk_size_used_for_tenants);
    EXPECT_EQ(limit_log_disk_size, opts.log_disk_usage_limit_size_);
    EXPECT_EQ(OB_MACHINE_RESOURCE_NOT_ENOUGH, log_pool->create_tenant(MIN_DISK_SIZE_PER_PALF_INSTANCE));
    const LSN base_lsn(8*PALF_BLOCK_SIZE);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->set_base_lsn(base_lsn));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1000));
    EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
    EXPECT_EQ(OB_SUCCESS, wait_until_disk_space_to(leader_idx, (10*PALF_PHY_BLOCK_SIZE*80+100)/100));
    sleep(2);
    EXPECT_EQ(PalfDiskOptionsWrapper::Status::NORMAL_STATUS,
              palf_env->palf_env_impl_.disk_options_wrapper_.status_);
    EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
    log_disk_size_used_for_tenants = log_pool->min_log_disk_size_for_all_tenants_;
    EXPECT_EQ(10*PALF_PHY_BLOCK_SIZE, log_disk_size_used_for_tenants);
    EXPECT_EQ(10*PALF_PHY_BLOCK_SIZE, opts.log_disk_usage_limit_size_);
    EXPECT_EQ(OB_SUCCESS, log_pool->create_tenant(MIN_DISK_SIZE_PER_PALF_INSTANCE));
    log_pool->abort_create_tenant(MIN_DISK_SIZE_PER_PALF_INSTANCE);

    // 扩容预计一定成功
    EXPECT_EQ(OB_SUCCESS, update_disk_options(leader_idx, limit_log_disk_size/PALF_PHY_BLOCK_SIZE));
    EXPECT_EQ(PalfDiskOptionsWrapper::Status::NORMAL_STATUS,
              palf_env->palf_env_impl_.disk_options_wrapper_.status_);
    EXPECT_EQ(OB_SUCCESS, get_disk_options(leader_idx, opts));
    log_disk_size_used_for_tenants = log_pool->min_log_disk_size_for_all_tenants_;
    EXPECT_EQ(limit_log_disk_size, log_disk_size_used_for_tenants);
    EXPECT_EQ(limit_log_disk_size, opts.log_disk_usage_limit_size_);
    EXPECT_EQ(OB_MACHINE_RESOURCE_NOT_ENOUGH, log_pool->create_tenant(MIN_DISK_SIZE_PER_PALF_INSTANCE));

  }

}

TEST_F(TestObSimpleLogDiskMgr, hidden_sys)
{
  SET_CASE_LOG_FILE(TEST_NAME, "hidden_sys");
  int server_idx = 0;
  PalfEnv *palf_env = NULL;
  int64_t leader_idx = 0;
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t total_used_size = 0, total_size = 0;
  share::SCN create_scn = share::SCN::base_scn();
  EXPECT_EQ(OB_SUCCESS, get_palf_env(0, palf_env));
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, palf_env->get_stable_disk_usage(total_used_size, total_size));
    EXPECT_EQ(0, total_used_size);
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_scn, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, palf_env->get_stable_disk_usage(total_used_size, total_size));
    EXPECT_NE(0, total_used_size);
    EXPECT_EQ(OB_NOT_SUPPORTED, update_disk_options(0));
  }
  EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
  EXPECT_EQ(OB_SUCCESS, update_disk_options(0));
  // tenant unit中记录的disk_opts直接生效
  PalfDiskOptions disk_opts;
  EXPECT_EQ(OB_SUCCESS, get_disk_options(0, disk_opts));
  EXPECT_EQ(0, disk_opts.log_disk_usage_limit_size_);
  EXPECT_EQ(PalfDiskOptionsWrapper::Status::SHRINKING_STATUS,
            palf_env->palf_env_impl_.disk_options_wrapper_.status_);
  sleep(2);
  EXPECT_EQ(PalfDiskOptionsWrapper::Status::NORMAL_STATUS,
            palf_env->palf_env_impl_.disk_options_wrapper_.status_);
  EXPECT_EQ(OB_SUCCESS, palf_env->get_stable_disk_usage(total_used_size, total_size));
  EXPECT_EQ(0, total_used_size);
  EXPECT_EQ(0, total_size);
  EXPECT_EQ(OB_SUCCESS, update_disk_options(8));
  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_scn, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, palf_env->get_stable_disk_usage(total_used_size, total_size));
  EXPECT_NE(0, total_used_size);
}

TEST_F(TestObSimpleLogDiskMgr, test_big_log)
{
  update_server_log_disk(10*1024*1024*1024ul);
  update_disk_options(10*1024*1024*1024ul/palf::PALF_PHY_BLOCK_SIZE);
  SET_CASE_LOG_FILE(TEST_NAME, "test_big_log");
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int server_idx = 0;
  PalfEnv *palf_env = NULL;
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  share::SCN create_scn = share::SCN::base_scn();
  EXPECT_EQ(OB_SUCCESS, get_palf_env(server_idx, palf_env));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_scn, leader_idx, leader));
  update_disk_options(leader_idx, MIN_DISK_SIZE_PER_PALF_INSTANCE/PALF_PHY_BLOCK_SIZE);
  // write big log
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 11, id, MAX_LOG_BODY_SIZE));
  LogStorage *log_storage = &leader.palf_handle_impl_->log_engine_.log_storage_;
  while(LSN(10*MAX_LOG_BODY_SIZE) > log_storage->log_tail_) {
    usleep(500);
  }
  LSN max_lsn = leader.palf_handle_impl_->get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);
  PALF_LOG(INFO, "test_big_log max_lsn after writing big log", K(max_lsn));

  // write small log
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 11, id, log_entry_size));
  while(LSN(10*log_entry_size) > log_storage->log_tail_) {
    usleep(500);
  }

  max_lsn = leader.palf_handle_impl_->get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);
  PALF_LOG(INFO, "test_big_log max_lsn after writing small log", K(max_lsn));
  EXPECT_EQ(OB_ITER_END, read_log(leader));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
