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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include <gtest/gtest.h>

#define protected public
#define private public
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_monitor_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_define.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheMonitorTask : public ::testing::Test
{
public:
  TestSSMicroCacheMonitorTask() {}
  virtual ~TestSSMicroCacheMonitorTask() {}
  void set_basic_read_io_info(ObIOInfo &io_info);
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSMicroCacheMonitorTask::set_basic_read_io_info(ObIOInfo &io_info)
{
  io_info.tenant_id_ = MTL_ID();
  io_info.timeout_us_ = 5 * 1000 * 1000L; // 5s
  io_info.flag_.set_read();
  io_info.flag_.set_wait_event(1);
}

void TestSSMicroCacheMonitorTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheMonitorTask::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheMonitorTask::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tnt_file_mgr);
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 30))); // 1GB
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
}

void TestSSMicroCacheMonitorTask::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

// Test 1: Monitor 任务初始化和基本执行逻辑
TEST_F(TestSSMicroCacheMonitorTask, test_monitor_task_init_and_execute)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_monitor_task_init_and_execute");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  ObSSMicroCacheMonitorTask &monitor_task = task_runner.monitor_task_;
  ObSSMicroCacheMonitorOp &monitor_op = monitor_task.monitor_op_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;

  // 验证 monitor_task 已初始化
  ASSERT_TRUE(monitor_task.is_inited_);
  ASSERT_TRUE(monitor_op.is_inited_);

  // 验证 abnormal_flag_ 初始值为 0
  uint64_t abnormal_flag = cache_stat.task_stat().get_abnormal_flag();
  ASSERT_EQ(0, abnormal_flag);

  // 手动调用 execute()
  ASSERT_EQ(OB_SUCCESS, monitor_op.execute());

  // 正常情况下execute()执行后 abnormal_flag_ 仍然为 0
  abnormal_flag = cache_stat.task_stat().get_abnormal_flag();
  ASSERT_EQ(0, abnormal_flag);

  LOG_INFO("TEST_CASE: finish test_monitor_task_init_and_execute");
}

// Test 2: Recovery Flag 读写操作
TEST_F(TestSSMicroCacheMonitorTask, test_abnormal_flag_read_write)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_abnormal_flag_read_write");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat();

  uint64_t abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_EQ(0, abnormal_flag);

  // 设置 FLAG_WRITE_STUCK
  task_stat.add_abnormal_flag(ObSSMicroCacheTaskStat::FLAG_WRITE_STUCK);
  abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_WRITE_STUCK) != 0);
  ASSERT_TRUE(task_stat.is_abnormal());
  // 再次设置 FLAG_WRITE_STUCK，标志位不变
  task_stat.add_abnormal_flag(ObSSMicroCacheTaskStat::FLAG_WRITE_STUCK);
  abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_WRITE_STUCK) != 0);
  ASSERT_TRUE(task_stat.is_abnormal());

  // 设置 FLAG_INVALID_STAT
  task_stat.add_abnormal_flag(ObSSMicroCacheTaskStat::FLAG_INVALID_STAT);
  abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_WRITE_STUCK) != 0);
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_INVALID_STAT) != 0);
  ASSERT_TRUE(task_stat.is_abnormal());
  // 再次设置 FLAG_INVALID_STAT，标志位不变
  task_stat.add_abnormal_flag(ObSSMicroCacheTaskStat::FLAG_INVALID_STAT);
  abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_WRITE_STUCK) != 0);
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_INVALID_STAT) != 0);
  ASSERT_TRUE(task_stat.is_abnormal());

  // 设置 FLAG_TASK_STUCK
  task_stat.add_abnormal_flag(ObSSMicroCacheTaskStat::FLAG_TASK_STUCK);
  abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_WRITE_STUCK) != 0);
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_INVALID_STAT) != 0);
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_TASK_STUCK) != 0);
  ASSERT_TRUE(task_stat.is_abnormal());
  // 再次设置 FLAG_TASK_STUCK，标志位不变
  task_stat.add_abnormal_flag(ObSSMicroCacheTaskStat::FLAG_TASK_STUCK);
  abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_WRITE_STUCK) != 0);
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_INVALID_STAT) != 0);
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_TASK_STUCK) != 0);
  ASSERT_TRUE(task_stat.is_abnormal());

  // 清除所有标志位
  task_stat.clear_abnormal_flag();
  abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_EQ(0, abnormal_flag);
  ASSERT_FALSE(task_stat.is_abnormal());

  LOG_INFO("TEST_CASE: finish test_abnormal_flag_read_write");
}

// Test 3.1: 模拟写入卡住（应该触发异常）
TEST_F(TestSSMicroCacheMonitorTask, test_write_stuck_detection)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_write_stuck_detection");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  ObSSMicroCacheMonitorTask &monitor_task = task_runner.monitor_task_;
  ObSSMicroCacheMonitorOp &monitor_op = monitor_task.monitor_op_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat();
  SSMicroCacheMonitorCtx &monitor_ctx = monitor_op.get_monitor_ctx();

  // 验证初始状态
  ASSERT_EQ(0, task_stat.get_abnormal_flag());
  ASSERT_EQ(0, monitor_ctx.pre_add_cnt_);
  ASSERT_EQ(0, monitor_ctx.cur_add_cnt_);
  ASSERT_EQ(0, monitor_ctx.pre_fail_add_cnt_);
  ASSERT_EQ(0, monitor_ctx.cur_fail_add_cnt_);
  ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_);
  monitor_ctx.last_check_time_us_ = 0;

  // 注入错误，写入100个微块，所有写入都应该失败
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ADD_MICRO_BLOCK_ERR, OB_TIMEOUT, 0, 1);
  const int64_t batch_micro_cnt = 100;
  const int64_t micro_size = 4 * 1024; // 4KB
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);
  for (int64_t i = 0; i < batch_micro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(50/*tablet_id*/, i);
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 128/*offset*/, micro_size);
    ASSERT_EQ(OB_TIMEOUT, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, 50/*effective_tablet_id*/,
              ObSSMicroCacheAccessType::COMMON_IO_TYPE));
  }
  ASSERT_EQ(batch_micro_cnt, cache_stat.io_stat().common_io_param_.add_cnt_);
  ASSERT_EQ(batch_micro_cnt, cache_stat.hit_stat().fail_add_cnt_);

  // 第一次调用 check_write_abnormal()，会调用 update_newest_add_info
  bool is_abnormal = false;
  ASSERT_EQ(OB_SUCCESS, monitor_op.check_write_abnormal(is_abnormal));
  ASSERT_FALSE(is_abnormal);
  ASSERT_FALSE(task_stat.is_abnormal());
  ASSERT_EQ(batch_micro_cnt, monitor_ctx.cur_add_cnt_);
  ASSERT_EQ(batch_micro_cnt, monitor_ctx.cur_fail_add_cnt_);
  ASSERT_EQ(0, monitor_ctx.pre_add_cnt_);
  ASSERT_EQ(0, monitor_ctx.pre_fail_add_cnt_);
  ASSERT_EQ(1, monitor_ctx.write_stuck_cnt_);

  // 连续调用 check_write_abnormal() 9 次（总共 10 次）
  for (int i = 1; i < ObSSMicroCacheMonitorOp::MICRO_CACHE_ABNORMAL_WRITE_STUCK_CNT; ++i) {
    for (int64_t j = 0; j < batch_micro_cnt; ++j) {
      const int64_t tablet_id = i * 100;
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(tablet_id, j);
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 128/*offset*/, micro_size);
      ASSERT_EQ(OB_TIMEOUT, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, tablet_id/*effective_tablet_id*/,
                ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(batch_micro_cnt * (i + 1), cache_stat.io_stat().common_io_param_.add_cnt_);
    ASSERT_EQ(batch_micro_cnt * (i + 1), cache_stat.hit_stat().fail_add_cnt_);

    monitor_ctx.last_check_time_us_ -= ObSSMicroCacheMonitorOp::WRITE_STUCK_CHECK_INTERVAL_US;
    ASSERT_EQ(OB_SUCCESS, monitor_op.check_write_abnormal(is_abnormal));
    ASSERT_EQ(batch_micro_cnt * (i + 1), monitor_ctx.cur_add_cnt_);
    ASSERT_EQ(batch_micro_cnt * (i + 1), monitor_ctx.cur_fail_add_cnt_);
    ASSERT_EQ(batch_micro_cnt * i, monitor_ctx.pre_add_cnt_);
    ASSERT_EQ(batch_micro_cnt * i, monitor_ctx.pre_fail_add_cnt_);
    ASSERT_EQ(i + 1, monitor_ctx.write_stuck_cnt_);

    ob_usleep(1000); // 1ms
  }

  // 验证 write_stuck_cnt_ 递增到 10
  ASSERT_EQ(ObSSMicroCacheMonitorOp::MICRO_CACHE_ABNORMAL_WRITE_STUCK_CNT, monitor_ctx.write_stuck_cnt_);
  // 验证 abnormal_flag_ 包含 FLAG_WRITE_STUCK
  uint64_t abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_WRITE_STUCK) != 0);
  ASSERT_TRUE(task_stat.is_abnormal());

  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ADD_MICRO_BLOCK_ERR, OB_TIMEOUT, 0, 0);
  LOG_INFO("TEST_CASE: finish test_write_stuck_detection", K(abnormal_flag), K(monitor_ctx.write_stuck_cnt_));
}

//Test 3.2: 正常写入场景（不应触发异常）
TEST_F(TestSSMicroCacheMonitorTask, test_normal_write_detection)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_normal_write_detection");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  ObSSMicroCacheMonitorTask &monitor_task = task_runner.monitor_task_;
  ObSSMicroCacheMonitorOp &monitor_op = monitor_task.monitor_op_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat();
  SSMicroCacheMonitorCtx &monitor_ctx = monitor_op.get_monitor_ctx();

  // 验证初始状态
  ASSERT_EQ(0, task_stat.get_abnormal_flag());
  ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_);
  bool is_abnormal = false;
  const int64_t batch_micro_cnt = 100;
  const int64_t micro_size = 4 * 1024;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  // 模拟10次的检查逻辑
  const int64_t check_cnt = ObSSMicroCacheMonitorOp::MICRO_CACHE_ABNORMAL_WRITE_STUCK_CNT;
  for (int i = 1; i <= check_cnt; ++i) {
    // 每次正常写入100个micro_block
    for (int64_t j = 0; j < batch_micro_cnt; ++j) {
      const int64_t tablet_id = 10 * i + 1000 * j;
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(tablet_id, 128/*offset*/);
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 128/*offset*/, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, tablet_id,
                ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(i * batch_micro_cnt, cache_stat.io_stat().common_io_param_.add_cnt_);
    ASSERT_EQ(0, cache_stat.hit_stat().fail_add_cnt_);
    // 执行monitor_op，前推last_check_time_us_，模拟正常间隔的检查逻辑
    monitor_ctx.last_check_time_us_ -= ObSSMicroCacheMonitorOp::WRITE_STUCK_CHECK_INTERVAL_US;
    ASSERT_EQ(OB_SUCCESS, monitor_op.execute());
    ASSERT_EQ((i - 1) * batch_micro_cnt, monitor_ctx.pre_add_cnt_);
    ASSERT_EQ(i * batch_micro_cnt, monitor_ctx.cur_add_cnt_);
    ASSERT_EQ(0, monitor_ctx.pre_fail_add_cnt_);
    ASSERT_EQ(0, monitor_ctx.cur_fail_add_cnt_);
    ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_);
    ASSERT_FALSE(task_stat.is_abnormal());
  }

  LOG_INFO("TEST_CASE: finish test_normal_write_detection");
}

// Test 3.3: 写入卡住后恢复（标志位应保持）
TEST_F(TestSSMicroCacheMonitorTask, test_write_stuck_recovery)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_write_stuck_recovery");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  ObSSMicroCacheMonitorTask &monitor_task = task_runner.monitor_task_;
  ObSSMicroCacheMonitorOp &monitor_op = monitor_task.monitor_op_;
  SSMicroCacheMonitorCtx &monitor_ctx = monitor_op.get_monitor_ctx();
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat();
  ObSSARCInfo &arc_info = micro_cache->micro_meta_mgr_.arc_info_;

  // 1. 注入错误，触发10次循环。每次循环写入一批数据，然后触发写入检查逻辑，应该出现异常
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ADD_MICRO_BLOCK_ERR, OB_TIMEOUT, 0, 1);
  const int64_t batch_micro_cnt = 100;
  const int64_t micro_size = 4 * 1024;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  const int64_t check_cnt = ObSSMicroCacheMonitorOp::MICRO_CACHE_ABNORMAL_WRITE_STUCK_CNT;
  for (int i = 1; i <= check_cnt; ++i) {
    for (int64_t j = 0; j < batch_micro_cnt; ++j) {
      const int64_t tablet_id = 10 * i + 1000 * j;
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(tablet_id, 128/*offset*/);
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 128/*offset*/, micro_size);
      ASSERT_EQ(OB_TIMEOUT, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, tablet_id,
                ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(i * batch_micro_cnt, cache_stat.io_stat().common_io_param_.add_cnt_);
    ASSERT_EQ(i * batch_micro_cnt, cache_stat.hit_stat().fail_add_cnt_);

    // 执行monitor_op，前推last_check_time_us_，模拟正常间隔的检查逻辑
    monitor_ctx.last_check_time_us_ -= ObSSMicroCacheMonitorOp::WRITE_STUCK_CHECK_INTERVAL_US;
    ASSERT_EQ(OB_SUCCESS, monitor_op.execute());
    ASSERT_EQ((i - 1) * batch_micro_cnt, monitor_ctx.pre_add_cnt_);
    ASSERT_EQ(i * batch_micro_cnt, monitor_ctx.cur_add_cnt_);
    ASSERT_EQ((i - 1) * batch_micro_cnt, monitor_ctx.pre_fail_add_cnt_);
    ASSERT_EQ(i * batch_micro_cnt, monitor_ctx.cur_fail_add_cnt_);
    ASSERT_EQ(i, monitor_ctx.write_stuck_cnt_);
  }
  ASSERT_TRUE(task_stat.is_abnormal());
  ASSERT_NE(0, task_stat.get_abnormal_flag() & ObSSMicroCacheTaskStat::FLAG_WRITE_STUCK);

  // 正常写入，检查标志位是否保存
  const int64_t curr_add_cnt = check_cnt * batch_micro_cnt;
  const int64_t curr_fail_add_cnt = check_cnt * batch_micro_cnt;
  ASSERT_EQ(curr_add_cnt, cache_stat.io_stat().common_io_param_.add_cnt_);
  ASSERT_EQ(curr_fail_add_cnt, cache_stat.hit_stat().fail_add_cnt_);
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ADD_MICRO_BLOCK_ERR, OB_TIMEOUT, 0, 0);
  ObArray<ObSSMicroBlockCacheKey> micro_key_arr;
  for (int i = 1; i <= check_cnt; ++i) {
    for (int64_t j = 0; j < batch_micro_cnt; ++j) {
      const int64_t tablet_id = 5 + 10 * i + 1000 * j;
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(tablet_id, 128/*offset*/);
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 128/*offset*/, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, tablet_id,
                ObSSMicroCacheAccessType::COMMON_IO_TYPE));
      ASSERT_EQ(OB_SUCCESS, micro_key_arr.push_back(micro_key));
    }
    ASSERT_EQ(curr_add_cnt + i * batch_micro_cnt, cache_stat.io_stat().common_io_param_.add_cnt_);
    ASSERT_EQ(curr_fail_add_cnt, cache_stat.hit_stat().fail_add_cnt_);
    // 执行monitor_op，前推last_check_time_us_，模拟正常间隔的检查逻辑
    monitor_ctx.last_check_time_us_ -= ObSSMicroCacheMonitorOp::WRITE_STUCK_CHECK_INTERVAL_US;
    ASSERT_EQ(OB_SUCCESS, monitor_op.execute());
    ASSERT_EQ(curr_add_cnt + (i - 1) * batch_micro_cnt, monitor_ctx.pre_add_cnt_);
    ASSERT_EQ(curr_add_cnt + i * batch_micro_cnt, monitor_ctx.cur_add_cnt_);
    ASSERT_EQ(curr_fail_add_cnt, monitor_ctx.cur_fail_add_cnt_);
    ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_); // 写入卡住后恢复，write_stuck_cnt_应为0
    ASSERT_TRUE(task_stat.is_abnormal()); // 但是不会更新标志位
  }
  ASSERT_EQ(curr_fail_add_cnt, monitor_ctx.pre_fail_add_cnt_);

  // 读出第二次写入的所有数据
  char *read_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, read_buf);
  ObIOInfo io_info;
  for (int64_t i = 0; i < micro_key_arr.count(); ++i) {
    io_info.reset();
    set_basic_read_io_info(io_info);
    io_info.buf_ = read_buf;
    io_info.user_data_buf_ = read_buf;
    io_info.callback_ = nullptr;
    const ObSSMicroBlockCacheKey micro_key = micro_key_arr.at(i);
    io_info.offset_ = micro_key.micro_id_.offset_;
    io_info.size_ = micro_size;
    io_info.effective_tablet_id_ = micro_key.get_macro_tablet_id().id();
    ObSSMicroBlockId micro_id = ObSSMicroBlockId(micro_key.micro_id_.macro_id_, micro_key.micro_id_.offset_, micro_key.micro_id_.size_);
    ObStorageObjectHandle obj_handle;
    bool is_hit = false;
    ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(micro_key, micro_id,
              ObSSMicroCacheGetType::GET_CACHE_HIT_DATA, io_info, obj_handle,
              ObSSMicroCacheAccessType::COMMON_IO_TYPE, is_hit));
    ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
    ASSERT_EQ(micro_size, obj_handle.get_data_size());
    obj_handle.reset();
  }
  ASSERT_EQ(check_cnt * batch_micro_cnt, arc_info.seg_info_arr_[SS_ARC_T2].cnt_);
  ASSERT_EQ(check_cnt * batch_micro_cnt * micro_size , arc_info.seg_info_arr_[SS_ARC_T2].size_);

  LOG_INFO("TEST_CASE: finish test_write_stuck_recovery");
}

// Test 3.4: 写入增量不足（不应触发异常）
TEST_F(TestSSMicroCacheMonitorTask, test_write_increment_not_enough)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_write_increment_not_enough");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  ObSSMicroCacheMonitorTask &monitor_task = task_runner.monitor_task_;
  ObSSMicroCacheMonitorOp &monitor_op = monitor_task.monitor_op_;
  SSMicroCacheMonitorCtx &monitor_ctx = monitor_op.get_monitor_ctx();
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat();

  // 验证初始状态
  ASSERT_EQ(0, task_stat.get_abnormal_flag());
  ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_);
  bool is_abnormal = false;
  const int64_t batch_micro_cnt = 5;
  const int64_t micro_size = 4 * 1024;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  // 模拟检查逻辑，每次只写入5个micro_block，因为增量不足不应该触发异常
  const int64_t check_cnt = ObSSMicroCacheMonitorOp::MICRO_CACHE_ABNORMAL_WRITE_STUCK_CNT;
  for (int i = 1; i <= check_cnt; ++i) {
    for (int64_t j = 0; j < batch_micro_cnt; ++j) {
      const int64_t tablet_id = 10 * i + 1000 * j;
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(tablet_id, 128/*offset*/);
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 128/*offset*/, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, tablet_id,
                ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(i * batch_micro_cnt, cache_stat.io_stat().common_io_param_.add_cnt_);
    ASSERT_EQ(0, cache_stat.hit_stat().fail_add_cnt_);
    // 执行monitor_op，前推last_check_time_us_，模拟正常间隔的检查逻辑
    monitor_ctx.last_check_time_us_ -= ObSSMicroCacheMonitorOp::WRITE_STUCK_CHECK_INTERVAL_US;
    ASSERT_EQ(OB_SUCCESS, monitor_op.execute());
    ASSERT_EQ((i - 1) * batch_micro_cnt, monitor_ctx.pre_add_cnt_);
    ASSERT_EQ(i * batch_micro_cnt, monitor_ctx.cur_add_cnt_);
    ASSERT_EQ(0, monitor_ctx.pre_fail_add_cnt_);
    ASSERT_EQ(0, monitor_ctx.cur_fail_add_cnt_);
    ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_); // 因为写入增量不足，所以不会触发异常
    ASSERT_FALSE(task_stat.is_abnormal());
  }

  LOG_INFO("TEST_CASE: finish test_write_increment_not_enough");
}

// Test 3.5: 写入部分失败（不应触发异常）
TEST_F(TestSSMicroCacheMonitorTask, test_write_part_failed)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_write_part_failed");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  ObSSMicroCacheMonitorTask &monitor_task = task_runner.monitor_task_;
  ObSSMicroCacheMonitorOp &monitor_op = monitor_task.monitor_op_;
  SSMicroCacheMonitorCtx &monitor_ctx = monitor_op.get_monitor_ctx();
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat();

  // 验证初始状态
  ASSERT_EQ(0, task_stat.get_abnormal_flag());
  ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_);
  bool is_abnormal = false;
  const int64_t batch_micro_cnt = 100;
  const int64_t micro_size = 4 * 1024;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  // 模拟检查逻辑，每次写入100个micro_block，其中20个失败，因为存在写入成功的情况，所以不应该触发异常
  const int64_t check_cnt = ObSSMicroCacheMonitorOp::MICRO_CACHE_ABNORMAL_WRITE_STUCK_CNT;
  for (int i = 1; i <= check_cnt; ++i) {
    int64_t fail_cnt = 0;
    for (int64_t j = 0; j < batch_micro_cnt; ++j) {
      const int64_t tablet_id = 10 * i + 1000 * j;
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(tablet_id, 128/*offset*/);
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 128/*offset*/, micro_size);
      if (j % 20 == 0) {
        // 每写入20条，失败一条
        TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ADD_MICRO_BLOCK_ERR, OB_TIMEOUT, 0, 1);
        ASSERT_EQ(OB_TIMEOUT, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, tablet_id,
                  ObSSMicroCacheAccessType::COMMON_IO_TYPE));
        ++fail_cnt;
        TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ADD_MICRO_BLOCK_ERR, OB_TIMEOUT, 0, 0);
      } else {
        ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, tablet_id,
                  ObSSMicroCacheAccessType::COMMON_IO_TYPE));
      }
    }
    ASSERT_GE(fail_cnt, 0);
    ASSERT_EQ(i * batch_micro_cnt, cache_stat.io_stat().common_io_param_.add_cnt_);
    ASSERT_EQ(i * fail_cnt, cache_stat.hit_stat().fail_add_cnt_);
    // 执行monitor_op，前推last_check_time_us_，模拟正常间隔的检查逻辑
    monitor_ctx.last_check_time_us_ -= ObSSMicroCacheMonitorOp::WRITE_STUCK_CHECK_INTERVAL_US;
    ASSERT_EQ(OB_SUCCESS, monitor_op.execute());
    ASSERT_EQ((i - 1) * batch_micro_cnt, monitor_ctx.pre_add_cnt_);
    ASSERT_EQ(i * batch_micro_cnt, monitor_ctx.cur_add_cnt_);
    ASSERT_EQ((i - 1) * fail_cnt, monitor_ctx.pre_fail_add_cnt_);
    ASSERT_EQ(i * fail_cnt, monitor_ctx.cur_fail_add_cnt_);
    ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_); // 因为写入部分失败，所以不会触发异常
    ASSERT_FALSE(task_stat.is_abnormal());
  }

  LOG_INFO("TEST_CASE: finish test_write_part_failed");
}

// Test 4: 检查间隔控制（时间间隔内触发，没有异常，达到间隔后，累计了中间的所有信息）
TEST_F(TestSSMicroCacheMonitorTask, test_check_interval_control)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_check_interval_control");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  ObSSMicroCacheMonitorTask &monitor_task = task_runner.monitor_task_;
  ObSSMicroCacheMonitorOp &monitor_op = monitor_task.monitor_op_;
  SSMicroCacheMonitorCtx &monitor_ctx = monitor_op.get_monitor_ctx();
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat();

  // 验证初始状态
  ASSERT_EQ(0, task_stat.get_abnormal_flag());
  ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_);
  bool is_abnormal = false;
  const int64_t batch_micro_cnt = 100;
  const int64_t micro_size = 4 * 1024;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  // 模拟10次的检查逻辑
  // 注入错误
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ADD_MICRO_BLOCK_ERR, OB_TIMEOUT, 0, 1);
  const int64_t check_cnt = ObSSMicroCacheMonitorOp::MICRO_CACHE_ABNORMAL_WRITE_STUCK_CNT;
  for (int i = 1; i <= check_cnt; ++i) {
    for (int64_t j = 0; j < batch_micro_cnt; ++j) {
      const int64_t tablet_id = 10 * i + 1000 * j;
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(tablet_id, 128/*offset*/);
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 128/*offset*/, micro_size);
      ASSERT_EQ(OB_TIMEOUT, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, tablet_id,
                ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    // cache_stat的统计信息会更新
    ASSERT_EQ(i * batch_micro_cnt, cache_stat.io_stat().common_io_param_.add_cnt_);
    ASSERT_EQ(i * batch_micro_cnt, cache_stat.hit_stat().fail_add_cnt_);
    ASSERT_EQ(OB_SUCCESS, monitor_op.execute());
    // 第一次调度时会更新统计信息
    // 后续都不满足调度间隔，统计信息不会更新
    ASSERT_EQ(0, monitor_ctx.pre_add_cnt_);
    ASSERT_EQ(batch_micro_cnt, monitor_ctx.cur_add_cnt_);
    ASSERT_EQ(0, monitor_ctx.pre_fail_add_cnt_);
    ASSERT_EQ(batch_micro_cnt, monitor_ctx.cur_fail_add_cnt_);
    ASSERT_EQ(1, monitor_ctx.write_stuck_cnt_); // 只有第一次会增加计数值
    ASSERT_FALSE(task_stat.is_abnormal());
  }
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ADD_MICRO_BLOCK_ERR, OB_TIMEOUT, 0, 0);

  LOG_INFO("TEST_CASE: finish test_check_interval_control");
}

// Test 5: 模拟前九次检查写入失败，最后一次正常，没有异常出现
TEST_F(TestSSMicroCacheMonitorTask, test_write_failed_last_normal)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_write_failed_last_normal");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  ObSSMicroCacheMonitorTask &monitor_task = task_runner.monitor_task_;
  ObSSMicroCacheMonitorOp &monitor_op = monitor_task.monitor_op_;
  SSMicroCacheMonitorCtx &monitor_ctx = monitor_op.get_monitor_ctx();
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat();

  // 验证初始状态
  ASSERT_EQ(0, task_stat.get_abnormal_flag());
  ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_);
  bool is_abnormal = false;
  const int64_t batch_micro_cnt = 100;
  const int64_t micro_size = 4 * 1024;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  // 模拟检查逻辑，前九次检查写入失败，最后一次正常
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ADD_MICRO_BLOCK_ERR, OB_TIMEOUT, 0, 1);
  const int64_t check_cnt = ObSSMicroCacheMonitorOp::MICRO_CACHE_ABNORMAL_WRITE_STUCK_CNT;
  for (int i = 1; i < check_cnt; ++i) {
    for (int64_t j = 0; j < batch_micro_cnt; ++j) {
      const int64_t tablet_id = 10 * i + 1000 * j;
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(tablet_id, 128/*offset*/);
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 128/*offset*/, micro_size);
      ASSERT_EQ(OB_TIMEOUT, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, tablet_id,
                ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(i * batch_micro_cnt, cache_stat.io_stat().common_io_param_.add_cnt_);
    ASSERT_EQ(i * batch_micro_cnt, cache_stat.hit_stat().fail_add_cnt_);
    monitor_ctx.last_check_time_us_ -= ObSSMicroCacheMonitorOp::WRITE_STUCK_CHECK_INTERVAL_US;
    ASSERT_EQ(OB_SUCCESS, monitor_op.execute());
    ASSERT_EQ((i - 1) * batch_micro_cnt, monitor_ctx.pre_add_cnt_);
    ASSERT_EQ(i * batch_micro_cnt, monitor_ctx.cur_add_cnt_);
    ASSERT_EQ((i - 1) * batch_micro_cnt, monitor_ctx.pre_fail_add_cnt_);
    ASSERT_EQ(i * batch_micro_cnt, monitor_ctx.cur_fail_add_cnt_);
    ASSERT_EQ(i, monitor_ctx.write_stuck_cnt_);
    ASSERT_FALSE(task_stat.is_abnormal());
  }
  // 最后一次正常
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ADD_MICRO_BLOCK_ERR, OB_TIMEOUT, 0, 0);
  {
    for (int64_t j = 0; j < batch_micro_cnt; ++j) {
      const int64_t tablet_id = check_cnt + 1000 * j;
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(tablet_id, 128/*offset*/);
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 128/*offset*/, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, tablet_id,
                ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(check_cnt * batch_micro_cnt, cache_stat.io_stat().common_io_param_.add_cnt_);
    ASSERT_EQ((check_cnt - 1) * batch_micro_cnt, cache_stat.hit_stat().fail_add_cnt_);
    monitor_ctx.last_check_time_us_ -= ObSSMicroCacheMonitorOp::WRITE_STUCK_CHECK_INTERVAL_US;
    ASSERT_EQ(OB_SUCCESS, monitor_op.execute());
    ASSERT_EQ((check_cnt - 1) * batch_micro_cnt, monitor_ctx.pre_add_cnt_);
    ASSERT_EQ(check_cnt * batch_micro_cnt, monitor_ctx.cur_add_cnt_);
    ASSERT_EQ((check_cnt - 1) * batch_micro_cnt, monitor_ctx.pre_fail_add_cnt_);
    ASSERT_EQ((check_cnt - 1) * batch_micro_cnt, monitor_ctx.cur_fail_add_cnt_);
    ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_); // 恢复正常，就重置
    ASSERT_FALSE(task_stat.is_abnormal());
  }

  LOG_INFO("TEST_CASE: finish test_write_failed_last_normal");
}

// Test 6: ARC Segment 计数为负数（应触发异常）
TEST_F(TestSSMicroCacheMonitorTask, test_arc_segment_negative_count)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_arc_segment_negative_count");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  ObSSMicroCacheMonitorTask &monitor_task = task_runner.monitor_task_;
  ObSSMicroCacheMonitorOp &monitor_op = monitor_task.monitor_op_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat();
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;

  // 验证初始状态
  ASSERT_EQ(0, task_stat.get_abnormal_flag());
  ASSERT_FALSE(arc_info.is_abnormal());
  ASSERT_FALSE(task_stat.is_abnormal());

  // 直接修改 segment 的 cnt_ 为负数（模拟异常）
  bool is_abnormal = false;
  arc_info.seg_info_arr_[SS_ARC_T1].cnt_ = -1;
  ASSERT_EQ(OB_SUCCESS, monitor_op.inner_check_arc_info(is_abnormal));
  ASSERT_TRUE(is_abnormal);
  ASSERT_TRUE(task_stat.is_abnormal());
  ASSERT_EQ(-1, arc_info.seg_info_arr_[SS_ARC_T1].cnt_);

  // 验证 abnormal_flag_ 包含 FLAG_INVALID_STAT
  uint64_t abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_INVALID_STAT) != 0);

  // 验证 arc_info.is_abnormal() 返回 true
  ASSERT_TRUE(arc_info.is_abnormal());
  ASSERT_TRUE(task_stat.is_abnormal());
  arc_info.seg_info_arr_[SS_ARC_T1].cnt_ = 0;

  LOG_INFO("TEST_CASE: finish test_arc_segment_negative_count", K(abnormal_flag));
}

// Test 6.1: ARC t1_cnt 负值场景下，循环执行 monitor_op 300 秒（每次 sleep 200ms），
// 用于验证异常出现后日志打印频率是否符合预期（SS_MONITOR_ABNORMAL_LOG_INTERVAL_US = 60s，20s 内应打印 1 次）
// TEST_F(TestSSMicroCacheMonitorTask, test_arc_abnormal_log_frequency)
// {
//   int ret = OB_SUCCESS;
//   LOG_INFO("TEST_CASE: start test_arc_abnormal_log_frequency");
//   ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
//   ASSERT_NE(nullptr, micro_cache);
//   ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
//   ObSSMicroCacheMonitorTask &monitor_task = task_runner.monitor_task_;
//   ObSSMicroCacheMonitorOp &monitor_op = monitor_task.monitor_op_;
//   ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
//   ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;

//   // 直接设定 arc_info 的 t1_cnt 为负值，模拟异常
//   arc_info.seg_info_arr_[SS_ARC_T1].cnt_ = -1;
//   ASSERT_TRUE(arc_info.is_abnormal());

//   const int64_t run_duration_us = 300 * 1000 * 1000;  // 300s
//   int64_t execute_cnt = 0;
//   int64_t start_us = ObTimeUtility::current_time_us();

//   while (ObTimeUtility::current_time_us() - start_us < run_duration_us) {
//     ASSERT_EQ(OB_SUCCESS, monitor_op.execute());
//     ++execute_cnt;
//     ob_usleep(200 * 1000);  // 200ms
//   }

//   LOG_INFO("TEST_CASE: finish test_arc_abnormal_log_frequency");
// }

// Test 7: MemBlkStat 异常（应触发异常）
TEST_F(TestSSMicroCacheMonitorTask, test_mem_blk_stat_abnormal)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_mem_blk_stat_abnormal");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  ObSSMicroCacheMonitorTask &monitor_task = task_runner.monitor_task_;
  ObSSMicroCacheMonitorOp &monitor_op = monitor_task.monitor_op_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat();

  // 验证初始状态
  ASSERT_EQ(0, task_stat.get_abnormal_flag());
  ASSERT_FALSE(cache_stat.is_state_abnormal());

  // 设置 mem_blk_stat 的 fg_used_cnt 为负数（模拟异常）
  cache_stat.mem_blk_stat().update_mem_blk_fg_used_cnt(-1);
  // 验证 is_state_abnormal() 返回 true
  ASSERT_TRUE(cache_stat.is_state_abnormal());

  // 调用检查函数
  bool is_abnormal = false;
  ASSERT_EQ(OB_SUCCESS, monitor_op.inner_check_cache_stat(is_abnormal));
  ASSERT_TRUE(is_abnormal);
  ASSERT_TRUE(task_stat.is_abnormal());

  // 验证 abnormal_flag_ 包含 FLAG_INVALID_STAT
  uint64_t abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_TRUE((abnormal_flag & ObSSMicroCacheTaskStat::FLAG_INVALID_STAT) != 0);
  cache_stat.mem_blk_stat().update_mem_blk_fg_used_cnt(1);

  LOG_INFO("TEST_CASE: finish test_mem_blk_stat_abnormal", K(abnormal_flag));
}

// Test 8: 测试后台任务卡住 （应触发异常）
TEST_F(TestSSMicroCacheMonitorTask, test_task_stuck_detection)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_task_stuck_detection");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  ObSSMicroCacheMonitorTask &monitor_task = task_runner.monitor_task_;
  ObSSMicroCacheMonitorOp &monitor_op = monitor_task.monitor_op_;
  SSMicroCacheMonitorCtx &monitor_ctx = monitor_op.get_monitor_ctx();
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat_;

  // 验证初始状态
  ASSERT_EQ(0, task_stat.get_abnormal_flag());
  ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_);

  sleep(60); // 用于触发1min间隔的日志打印
  // 手动设置task_stat里后台任务的时间，前推超时时间
  const int64_t p_data_task_idx = static_cast<int64_t>(ObSSMicroCacheTaskOpType::SS_PERSIST_MICRO_DATA_OP);
  const int64_t p_meta_task_idx = static_cast<int64_t>(ObSSMicroCacheTaskOpType::SS_PERSIST_MICRO_META_OP);
  task_stat.task_start_time_us_arr_[p_data_task_idx] = ObTimeUtility::current_time_us()
                                                       - ObSSMicroCacheMonitorOp::TASK_STUCK_TIMEOUT_US;
  task_stat.task_start_time_us_arr_[p_meta_task_idx] = ObTimeUtility::current_time_us()
                                                       - ObSSMicroCacheMonitorOp::TASK_STUCK_TIMEOUT_US;
  // 执行monitor_op，验证是否触发异常
  // 记录执行时间ms
  int64_t start_time_us = ObTimeUtility::current_time_us();
  ASSERT_EQ(OB_SUCCESS, monitor_op.execute());
  int64_t end_time_us = ObTimeUtility::current_time_us();
  int64_t cost_time_us = end_time_us - start_time_us;
  // std::cout << "TEST_CASE: test_task_stuck_detection, cost_time_us: " << cost_time_us
  //           << ", cost_time_ms: " << cost_time_us / 1000 << std::endl;
  ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_);
  // 执行check_task_abnormal，验证是否触发异常
  bool is_abnormal = false;
  ASSERT_EQ(OB_SUCCESS, monitor_op.check_task_abnormal(is_abnormal));
  ASSERT_TRUE(is_abnormal);
  ASSERT_TRUE(task_stat.is_abnormal());
  // 验证 abnormal_flag_ 包含 FLAG_TASK_STUCK
  uint64_t abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_NE(0, (abnormal_flag & ObSSMicroCacheTaskStat::FLAG_TASK_STUCK));

  // 恢复时间为正常值，验证是否触发异常
  task_stat.task_start_time_us_arr_[p_data_task_idx] = ObTimeUtility::current_time_us();
  task_stat.task_start_time_us_arr_[p_meta_task_idx] = ObTimeUtility::current_time_us();
  ASSERT_EQ(OB_SUCCESS, monitor_op.execute());
  is_abnormal = false;
  ASSERT_EQ(OB_SUCCESS, monitor_op.check_task_abnormal(is_abnormal));
  ASSERT_FALSE(is_abnormal); // 恢复后，不触发异常
  ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_);
  ASSERT_TRUE(task_stat.is_abnormal()); // 但状态位仍然保留
  abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_NE(0, (abnormal_flag & ObSSMicroCacheTaskStat::FLAG_TASK_STUCK)); // 恢复后，标志位仍然保留

  // 再次触发检查，应该仍然不触发异常，但异常标志仍然保留
  ASSERT_EQ(OB_SUCCESS, monitor_op.execute());
  is_abnormal = false;
  ASSERT_EQ(OB_SUCCESS, monitor_op.check_task_abnormal(is_abnormal));
  ASSERT_FALSE(is_abnormal);
  ASSERT_EQ(0, monitor_ctx.write_stuck_cnt_);
  ASSERT_TRUE(task_stat.is_abnormal());
  abnormal_flag = task_stat.get_abnormal_flag();
  ASSERT_NE(0, (abnormal_flag & ObSSMicroCacheTaskStat::FLAG_TASK_STUCK));

  LOG_INFO("TEST_CASE: finish test_task_stuck_detection");
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_monitor_task.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_monitor_task.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
