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
#include "gtest/gtest.h"

#define private public
#define protected public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_reader.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_writer.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
class TestSSBatchMicroCheckpointTask : public ::testing::Test
{
public:
  TestSSBatchMicroCheckpointTask()
    : allocator_(), micro_cache_(nullptr)
  {}
  ~TestSSBatchMicroCheckpointTask() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

public:
  ObArenaAllocator allocator_;
  ObSSMicroCache *micro_cache_;
  uint64_t tenant_id_;
};

void TestSSBatchMicroCheckpointTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSBatchMicroCheckpointTask::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSBatchMicroCheckpointTask::SetUp()
{
  micro_cache_ = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache_);
  tenant_id_ = MTL_ID();
}

void TestSSBatchMicroCheckpointTask::TearDown()
{
  ASSERT_NE(nullptr, micro_cache_);
  micro_cache_->stop();
  micro_cache_->wait();
  micro_cache_->destroy();
}

TEST_F(TestSSBatchMicroCheckpointTask, test_batch_micro_meta_ckpt)
{
  ASSERT_NE(nullptr, micro_cache_);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSMicroRangeManager &micro_range_mgr = micro_cache_->micro_range_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  ObSSPersistMicroMetaTask &persist_meta_task = micro_cache_->task_runner_.persist_meta_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache_->task_runner_.blk_ckpt_task_;
  const int64_t cache_file_size = micro_cache_->cache_file_size_;
  const int64_t block_size = micro_cache_->phy_blk_size_;
  const uint64_t tenant_id = MTL_ID();

  ObArray<TestSSCommonUtil::MicroBlockInfo> micro_block_arr;
  const int64_t MAX_WAIT_MICRO_CKPT_TIME_S = 180;
  int64_t start_time_s = ObTimeUtility::current_time_s();

  // 1. ckpt_split_cnt = 1
  int64_t ckpt_split_cnt = 1;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::restart_micro_cache(micro_cache_, tenant_id, cache_file_size, ckpt_split_cnt));
  ASSERT_NE(nullptr, micro_cache_);
  ASSERT_EQ(ckpt_split_cnt, phy_blk_mgr.super_blk_.get_ckpt_split_cnt());
  ASSERT_EQ(true, phy_blk_mgr.super_blk_.micro_ckpt_info_.check_init(ckpt_split_cnt));
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.exist_micro_checkpoint());
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.exist_blk_checkpoint());
  ASSERT_EQ(0, cache_stat.task_stat().blk_ckpt_item_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().blk_ckpt_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().micro_ckpt_item_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().micro_ckpt_op_cnt_);

  // 1.1 add some micro_block into cache
  const int64_t macro_block_cnt = 50;
  const int32_t min_micro_size = 8 * 1024;
  const int32_t max_micro_size = 16 * 1024;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::add_micro_blocks(macro_block_cnt, block_size, micro_block_arr, 1, true, min_micro_size, max_micro_size));
  ASSERT_EQ(macro_block_cnt, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);
  ASSERT_EQ(macro_block_cnt, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  ASSERT_LT(0, micro_block_arr.count());
  ASSERT_EQ(micro_block_arr.count(), cache_stat.micro_stat().valid_micro_cnt_);

  // 1.2 execute micro_meta ckpt and blk_info ckpt
  {
    int64_t ori_blk_ckpt_cnt = cache_stat.task_stat().blk_ckpt_cnt_;
    blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
    start_time_s = ObTimeUtility::current_time_s();
    bool finish_check = false;
    do {
      if (cache_stat.task_stat().blk_ckpt_cnt_ > ori_blk_ckpt_cnt) {
        finish_check = true;
      } else {
        LOG_INFO("still waiting for blk_ckpt", K(ori_blk_ckpt_cnt), K(cache_stat.task_stat()));
        ob_usleep(1000 * 1000);
      }
    } while (!finish_check && (ObTimeUtility::current_time_s() - start_time_s < MAX_WAIT_MICRO_CKPT_TIME_S));
    ASSERT_EQ(ori_blk_ckpt_cnt + 1, cache_stat.task_stat().blk_ckpt_cnt_);
    ASSERT_LT(0, phy_blk_mgr.super_blk_.blk_ckpt_used_blk_list().count());
    ASSERT_LT(0, cache_stat.task_stat().blk_ckpt_item_cnt_);
  }
  {
    int64_t ori_micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
    int64_t ori_micro_ckpt_op_cnt = cache_stat.task_stat().micro_ckpt_op_cnt_;
    persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
    start_time_s = ObTimeUtility::current_time_s();
    bool finish_check = false;
    do {
      if (cache_stat.task_stat().micro_ckpt_cnt_ > ori_micro_ckpt_cnt) {
        finish_check = true;
      } else {
        LOG_INFO("still waiting for micro_ckpt", K(ori_micro_ckpt_cnt), K(cache_stat.task_stat()));
        ob_usleep(1000 * 1000);
      }
    } while (!finish_check && (ObTimeUtility::current_time_s() - start_time_s < MAX_WAIT_MICRO_CKPT_TIME_S));
    ASSERT_EQ(ori_micro_ckpt_cnt + 1, cache_stat.task_stat().micro_ckpt_cnt_);
    ASSERT_EQ(ori_micro_ckpt_op_cnt + 1, cache_stat.task_stat().micro_ckpt_op_cnt_);
    ASSERT_EQ(true, persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.complete_full_ckpt_);
    ASSERT_LT(0, phy_blk_mgr.super_blk_.get_micro_ckpt_used_blk_list_cnt());
    ASSERT_LT(0, cache_stat.task_stat().micro_ckpt_item_cnt_);
  }

  // 2. ckpt_split_cnt = 50
  int64_t prev_used_data_blk_cnt = cache_stat.phy_blk_stat().data_blk_used_cnt_;
  ASSERT_LT(0, prev_used_data_blk_cnt);
  int64_t prev_persisted_micro_cnt = cache_stat.task_stat().micro_ckpt_item_cnt_;
  ASSERT_LT(0, prev_persisted_micro_cnt);
  ckpt_split_cnt = 50;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::restart_micro_cache(micro_cache_, tenant_id, cache_file_size, ckpt_split_cnt));
  ASSERT_NE(nullptr, micro_cache_);
  ASSERT_EQ(ckpt_split_cnt, phy_blk_mgr.super_blk_.get_ckpt_split_cnt());
  ASSERT_EQ(true, phy_blk_mgr.super_blk_.micro_ckpt_info_.check_init(ckpt_split_cnt));
  ASSERT_EQ(false, phy_blk_mgr.super_blk_.micro_ckpt_info_.exist_valid_ckpt());
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.blk_ckpt_info_.blk_ckpt_used_blks_.count());
  ASSERT_EQ(SS_INVALID_PHY_BLK_ID, phy_blk_mgr.super_blk_.blk_ckpt_info_.blk_ckpt_entry_);
  ASSERT_EQ(prev_persisted_micro_cnt, cache_stat.micro_stat().valid_micro_cnt_);
  int64_t total_range_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.get_total_range_micro_cnt(total_range_micro_cnt));
  ASSERT_EQ(prev_persisted_micro_cnt, total_range_micro_cnt);
  ASSERT_EQ(prev_used_data_blk_cnt, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().micro_ckpt_item_cnt_);

  // 2.2 execute micro_meta ckpt and blk_info ckpt
  {
    int64_t ori_blk_ckpt_cnt = cache_stat.task_stat().blk_ckpt_cnt_;
    blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
    start_time_s = ObTimeUtility::current_time_s();
    bool finish_check = false;
    do {
      if (cache_stat.task_stat().blk_ckpt_cnt_ > ori_blk_ckpt_cnt) {
        finish_check = true;
      } else {
        LOG_INFO("still waiting for blk_ckpt", K(ori_blk_ckpt_cnt), K(cache_stat.task_stat()));
        ob_usleep(1000 * 1000);
      }
    } while (!finish_check && (ObTimeUtility::current_time_s() - start_time_s < MAX_WAIT_MICRO_CKPT_TIME_S));
    ASSERT_EQ(ori_blk_ckpt_cnt + 1, cache_stat.task_stat().blk_ckpt_cnt_);
    ASSERT_LT(0, phy_blk_mgr.super_blk_.blk_ckpt_used_blk_list().count());
    ASSERT_LT(0, cache_stat.task_stat().blk_ckpt_item_cnt_);
  }
  {
    int64_t ori_micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
    int64_t ori_micro_ckpt_op_cnt = cache_stat.task_stat().micro_ckpt_op_cnt_;
    persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
    start_time_s = ObTimeUtility::current_time_s();
    bool finish_check = false;
    do {
      if (cache_stat.task_stat().micro_ckpt_cnt_ > ori_micro_ckpt_cnt) {
        finish_check = true;
      } else {
        LOG_INFO("still waiting for micro_ckpt", K(ori_micro_ckpt_cnt), K(cache_stat.task_stat()));
        ob_usleep(1000 * 1000);
      }
    } while (!finish_check && (ObTimeUtility::current_time_s() - start_time_s < MAX_WAIT_MICRO_CKPT_TIME_S));
    ASSERT_EQ(ori_micro_ckpt_cnt + 1, cache_stat.task_stat().micro_ckpt_cnt_);
    ASSERT_EQ(ori_micro_ckpt_op_cnt + ckpt_split_cnt, cache_stat.task_stat().micro_ckpt_op_cnt_);
    ASSERT_EQ(true, persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.complete_full_ckpt_);
    ASSERT_LT(0, phy_blk_mgr.super_blk_.get_micro_ckpt_used_blk_list_cnt());
    ASSERT_EQ(ckpt_split_cnt, phy_blk_mgr.super_blk_.micro_ckpt_entries().count());
    ASSERT_LT(0, cache_stat.task_stat().micro_ckpt_item_cnt_);
    ASSERT_GE(prev_persisted_micro_cnt, cache_stat.task_stat().micro_ckpt_item_cnt_);
  }

  // 3. ckpt_split_cnt = 100
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_batch_micro_checkpoint.log*");
  OB_LOGGER.set_file_name("test_ss_batch_micro_checkpoint.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
