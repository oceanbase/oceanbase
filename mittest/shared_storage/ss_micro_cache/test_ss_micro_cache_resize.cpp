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

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheResize : public ::testing::Test
{
public:
  TestSSMicroCacheResize() {}
  virtual ~TestSSMicroCacheResize() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  int resize_micro_cache_file(
      int idx, int thread_num, ObArray<TestSSCommonUtil::MicroBlockInfo> &micro_block_info_arr);
  int batch_add_micro_block_ignore_eagain(
      const uint64_t tablet_id,
      const int64_t micro_cnt,
      const int64_t min_micro_size,
      const int64_t max_micro_size,
      int64_t &add_cnt);

private:
  ObSSMicroCache *micro_cache_;
};

void TestSSMicroCacheResize::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheResize::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheResize::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 30))); // 1GB
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
  micro_cache_ = micro_cache;
}

void TestSSMicroCacheResize::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  micro_cache_ = nullptr;
}

int TestSSMicroCacheResize::resize_micro_cache_file(
      int idx,
      int thread_num,
      ObArray<TestSSCommonUtil::MicroBlockInfo> &micro_block_info_arr)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  char *read_buf = nullptr;
  if (OB_ISNULL(read_buf = static_cast<char*>(allocator.alloc(DEFAULT_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else {
    for (int64_t epoch = 0; OB_SUCC(ret) && epoch < 3; epoch++) {
      if (idx == 1 && epoch == 1) {
        if (OB_FAIL(micro_cache_->resize_micro_cache_file_size(micro_cache_->cache_file_size_ * 2))) {
          LOG_WARN("fail to resize file size", KR(ret));
        }
      }
      for (int64_t i = idx; OB_SUCC(ret) && i < micro_block_info_arr.count(); i += thread_num) {
        TestSSCommonUtil::MicroBlockInfo micro_info = micro_block_info_arr[i];
        if (OB_FAIL(TestSSCommonUtil::get_micro_block(micro_info, read_buf))) {
          LOG_WARN("fail to get micro_block", KR(ret), K(micro_info));
        }
      }
    }
  }
  return ret;
}

int TestSSMicroCacheResize::batch_add_micro_block_ignore_eagain(
    const uint64_t tablet_id,
    const int64_t micro_cnt,
    const int64_t min_micro_size,
    const int64_t max_micro_size,
    int64_t &add_cnt)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObArenaAllocator allocator;
  char *data_buf = nullptr;
  if (OB_UNLIKELY(micro_cnt <= 0 || min_micro_size <= 0 || max_micro_size <= 0 || max_micro_size < min_micro_size ||
                  tablet_id == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_cnt), K(min_micro_size), K(max_micro_size), K(tablet_id));
  } else if (OB_ISNULL(micro_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_cache should not be null", KR(ret), KP(micro_cache));
  } else if (OB_ISNULL(data_buf = static_cast<char *>(allocator.alloc(max_micro_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(max_micro_size));
  } else {
    add_cnt = 0;
    const int32_t phy_blk_size = micro_cache->phy_blk_size_;
    const int64_t payload_offset = ObSSMemBlock::get_reserved_size();
    const int64_t offset = payload_offset;
    for (int64_t i = 1; OB_SUCC(ret) && (i <= micro_cnt); ++i) {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(tablet_id, i);
      int64_t micro_size = 0;
      if (max_micro_size == min_micro_size) {
        micro_size = min_micro_size;
      } else {
        micro_size = min_micro_size + (i % 10) * (max_micro_size - min_micro_size) / 10;
      }
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      const uint32_t effective_tablet_id = macro_id.second_id();
      char c = micro_key.hash() % 26 + 'a';
      MEMSET(data_buf, c, micro_size);
      if (OB_FAIL(micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, effective_tablet_id,
                                                     ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
        if (OB_EAGAIN == ret) { // if add too fast, ignore current micro block
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to add micro_block cache", KR(ret), K(i), K(micro_key), K(tablet_id));
        }
      } else {
        ++add_cnt;
      }
    }
    allocator.clear();
    LOG_INFO("finish batch add micro_block", K(add_cnt), K(tablet_id), K(micro_cnt));
  }
  return ret;
}

TEST_F(TestSSMicroCacheResize, test_decrease_micro_cache_size_pct)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_decrease_micro_cache_size_pct");
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tnt_disk_space_mgr);
  const int64_t ori_total_disk_size = tnt_disk_space_mgr->total_disk_size_;
  const int64_t ori_micro_cache_size = tnt_disk_space_mgr->micro_cache_file_size_;
  const uint64_t tenant_id = MTL_ID();
  {
    // 1. decrease micro_cache max pct should not shrink file size; adaptive logic only grows
    const int64_t new_pct = 10;
    omt::ObTenantConfigGuard tenant_config_guard(TENANT_CONF(tenant_id));
    if (tenant_config_guard.is_valid()) {
      tenant_config_guard->_ss_micro_cache_size_max_percentage = new_pct;
    }
    ASSERT_EQ(new_pct, ObTenantDiskSpaceManager::get_micro_cache_size_pct(tenant_id));
    ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->try_adjust_micro_cache_file_size());
    ASSERT_EQ(ori_micro_cache_size, tnt_disk_space_mgr->micro_cache_file_size_);
  }

  {
    // 2. decrease pct and increase total disk size should NOT shrink nor auto-reset micro cache file
    const int64_t new_micro_cache_pct = 15;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      tenant_config->_ss_micro_cache_size_max_percentage = new_micro_cache_pct;
    }
    ASSERT_EQ(new_micro_cache_pct, ObTenantDiskSpaceManager::get_micro_cache_size_pct(tenant_id));
    bool succ_resize = false;
    const int64_t new_total_size = ori_total_disk_size * 2 - 100 * 1024 * 1024L;
    ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->resize_total_disk_size_(new_total_size, succ_resize));
    ASSERT_EQ(true, succ_resize);
    // micro_cache_file_size_ remains unchanged; adaptive growth happens separately
    ASSERT_EQ(ori_micro_cache_size, tnt_disk_space_mgr->micro_cache_file_size_);
  }
}

TEST_F(TestSSMicroCacheResize, test_basic_resize_cache_file)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_basic_resize_cache_file");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  const int64_t ori_total_phy_blk_cnt = phy_blk_mgr.blk_cnt_info_.total_blk_cnt_;
  const int64_t ori_arc_limit = micro_meta_mgr.arc_info_.limit_;
  const int64_t new_cache_file_size = micro_cache->cache_file_size_ * 2;
  ASSERT_EQ(OB_SUCCESS, micro_cache->resize_micro_cache_file_size(new_cache_file_size));
  ASSERT_EQ(new_cache_file_size, micro_cache->cache_file_size_);
  ASSERT_EQ(new_cache_file_size, phy_blk_mgr.super_blk_.cache_file_size_);
  ASSERT_LT(ori_total_phy_blk_cnt, phy_blk_mgr.blk_cnt_info_.total_blk_cnt_);
  ASSERT_LT(ori_arc_limit, micro_meta_mgr.arc_info_.limit_);
}

/*
  resize cache_file_szie between begin_free_space_for_prewarm() and finish_free_space_for_prewarm()
*/
TEST_F(TestSSMicroCacheResize, test_resize_between_free_space_for_prewarm)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_resize_between_free_space_for_prewarm");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSARCInfo &arc_info = micro_cache->micro_meta_mgr_.arc_info_;

  const int64_t old_limit = arc_info.limit_;
  micro_cache->begin_free_space_for_prewarm();
  const int64_t prewarm_work_limit = static_cast<int64_t>((static_cast<double>(old_limit * SS_LIMIT_PREWARM_SHRINK_PCT) / 100.0));
  ASSERT_EQ(prewarm_work_limit, arc_info.work_limit_);

  const int64_t new_cache_file_size = micro_cache->phy_blk_mgr_.cache_file_size_ * 2;
  ASSERT_EQ(OB_SUCCESS, micro_cache->resize_micro_cache_file_size(new_cache_file_size));
  const int64_t new_limit = arc_info.limit_;
  ASSERT_LE(old_limit * 2, new_limit);
  ASSERT_LT(prewarm_work_limit * 2, arc_info.work_limit_);

  micro_cache->finish_free_space_for_prewarm();
  ASSERT_EQ(new_limit, arc_info.work_limit_);
  ASSERT_EQ(new_limit, arc_info.limit_);
  const int64_t p = static_cast<int64_t>((static_cast<double>(new_limit * arc_info.DEF_ARC_P_OF_LIMIT_PCT) / 100.0));
  const int64_t max_p = new_limit * arc_info.MAX_ARC_P_OF_LIMIT_PCT / 100;
  const int64_t min_p = new_limit * arc_info.MIN_ARC_P_OF_LIMIT_PCT / 100;
  ASSERT_LE(abs(p - arc_info.p_), 20); // the conversion between 'int64_t' and 'double' causes some deviations
  ASSERT_EQ(max_p, arc_info.max_p_);
  ASSERT_EQ(min_p, arc_info.min_p_);
}

/*
  Multiple threads read micro blocks in parallel, and thread 0 will resize the file size from 128M to 256M. Check the
  data_blk_used_cnt exceeds total_data_blk_cnt.
*/
TEST_F(TestSSMicroCacheResize, test_get_micro_block_and_resize_larger)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_get_micro_block_and_resize_larger");
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  ASSERT_EQ(true, tenant_config.is_valid());
  tenant_config->_ss_micro_cache_max_block_size = 2 * 1024 * 1024; // 2MB
  micro_cache->micro_meta_mgr_.max_micro_blk_size_ = 2 * 1024 * 1024;

  const int64_t origin_data_blk_cnt = phy_blk_mgr.blk_cnt_info_.micro_data_blk_max_cnt();
  const int64_t total_macro_blk_cnt = origin_data_blk_cnt * 2;
  const int32_t block_size = phy_blk_mgr.block_size_;
  ObArray<TestSSCommonUtil::MicroBlockInfo> micro_block_info_arr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::prepare_micro_blocks(total_macro_blk_cnt, block_size, micro_block_info_arr, 1, true, 100 * 1024, 160 * 1024));

  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;

  {
    const int64_t thread_num = 4;
    ObTenantBase *tenant_base = MTL_CTX();

    auto test_func = [&](const int64_t idx) {
      ObTenantEnv::set_tenant(tenant_base);
      ASSERT_EQ(OB_SUCCESS, resize_micro_cache_file(idx, thread_num, micro_block_info_arr));
    };

    std::vector<std::thread> ths;
    for (int64_t i = 0; i < thread_num; ++i) {
      std::thread th(test_func, i);
      ths.push_back(std::move(th));
    }
    for (int64_t i = 0; i < thread_num; ++i) {
      ths[i].join();
    }

    ASSERT_LT(origin_data_blk_cnt, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);
  }

}

/*
  Test micro block cache execution correctness under different sizes:
  1. Start with 810MB, add data much larger than 810MB, ensure blk and meta checkpoints are executed, then execute clear;
  2. Resize to 10250MB;
  3. Add data much larger than 10250MB, ensure blk and meta checkpoints are executed, then execute clear;
  4. Resize to 20GB;
  5. Add data much larger than 20GB, ensure blk and meta checkpoints are executed, then execute clear;
*/
TEST_F(TestSSMicroCacheResize, test_micro_block_resize_to_different_size)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_micro_block_resize_to_different_size");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  ObSSPersistMicroMetaTask &persist_meta_task = micro_cache->task_runner_.persist_meta_task_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat();
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tnt_file_mgr);
  const uint64_t tenant_id = MTL_ID();
  char micro_cache_file_path[512] = {0};
  ASSERT_EQ(OB_SUCCESS, tnt_file_mgr->get_micro_cache_file_path(micro_cache_file_path, sizeof(micro_cache_file_path), tenant_id, MTL_EPOCH_ID()));

  const int64_t TIMEOUT_S = 60;
  const int32_t micro_size = 16 * 1024;
  const int64_t max_required_size = 20LL * 1024 * 1024 * 1024; // 20GB

  // Get real file size before test
  int64_t ori_file_size = 0;
  {
    ObIODFileStat f_stat;
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(micro_cache_file_path, f_stat));
    ori_file_size = f_stat.size_;
    LOG_INFO("TEST_CASE: real micro cache file size before test", K(ori_file_size));
  }

  int64_t new_file_size = ori_file_size;
  if (ori_file_size < max_required_size) {
    LOG_INFO("TEST_CASE: file size is less than max required size, will expand file",
      K(ori_file_size), K(max_required_size));
    int file_fd = tnt_file_mgr->get_micro_cache_file_fd();
    ASSERT_NE(OB_INVALID_FD, file_fd);

    const int64_t delta_size = max_required_size - ori_file_size;
    ASSERT_EQ(0, ::fallocate(file_fd, 0/*MODE*/, new_file_size, delta_size));
    LOG_INFO("TEST_CASE: succ to expand micro cache file", K(new_file_size), K(max_required_size), K(delta_size));

    // Verify file size after expansion
    ObIODFileStat f_stat_after;
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(micro_cache_file_path, f_stat_after));
    new_file_size = f_stat_after.size_;
    LOG_INFO("TEST_CASE: file size after expansion", "actual_file_size", f_stat_after.size_, "expected_size", max_required_size);
    ASSERT_GE(f_stat_after.size_, max_required_size);
  }

  // Stage 1: 810MB
  {
    int64_t stage_start_time_s = ObTimeUtility::current_time_s();
    LOG_INFO("TEST_CASE: Stage 1 - 810MB, start");
    const int64_t cache_file_size = 810 * 1024 * 1024; // 810MB
    const uint64_t tablet_id = 1000;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::restart_micro_cache(micro_cache, tenant_id, cache_file_size));
    ASSERT_EQ(cache_file_size, micro_cache->cache_file_size_);

    // Add 5 times of cache file size
    const int64_t target_write_size = cache_file_size * 5;
    const int64_t micro_cnt = target_write_size / micro_size;
    int64_t write_start_time_s = ObTimeUtility::current_time_s();
    int64_t add_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, batch_add_micro_block_ignore_eagain(tablet_id, micro_cnt,
              micro_size, micro_size, add_cnt));
    int64_t write_cost_time_s = ObTimeUtility::current_time_s() - write_start_time_s;
    ASSERT_LT(0, add_cnt);
    ASSERT_LT(0, cache_stat.micro_stat().valid_micro_cnt_);
    LOG_INFO("TEST_CASE: Stage 1: finish add micro_blocks", K(add_cnt), K(micro_cnt), K(write_cost_time_s));

    // Wait for blk and meta checkpoints to complete
    int64_t ckpt_start_time_s = ObTimeUtility::current_time_s();
    {
      int64_t ori_blk_ckpt_cnt = task_stat.blk_ckpt_cnt_;
      int64_t ori_micro_ckpt_cnt = task_stat.micro_ckpt_cnt_;
      blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
      persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
      bool finish_ckpt = false;
      int64_t cur_time_s = ObTimeUtility::current_time_s();
      do {
        if (task_stat.blk_ckpt_cnt_ > ori_blk_ckpt_cnt &&
            (task_stat.micro_ckpt_cnt_ > ori_micro_ckpt_cnt) &&
            (persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.complete_full_ckpt_)) {
          finish_ckpt = true;
        } else {
          LOG_INFO("still waiting for checkpoint", K(ori_blk_ckpt_cnt), K(ori_micro_ckpt_cnt), K(task_stat));
          ob_usleep(1000 * 1000);
        }
      } while (!finish_ckpt && ObTimeUtility::current_time_s() - cur_time_s <= TIMEOUT_S);
      ASSERT_EQ(true, finish_ckpt);
      ASSERT_LT(ori_blk_ckpt_cnt, task_stat.blk_ckpt_cnt_);
      ASSERT_LT(ori_micro_ckpt_cnt, task_stat.micro_ckpt_cnt_);
    }
    int64_t ckpt_cost_time_s = ObTimeUtility::current_time_s() - ckpt_start_time_s;
    LOG_INFO("TEST_CASE: Stage 1: finish checkpoint", K(ckpt_cost_time_s));

    // Verify tablet_id in super_blk
    ObSSMicroCacheSuperBlk &super_blk = phy_blk_mgr.super_blk_;
    ASSERT_EQ(1, super_blk.tablet_info_list_.count());
    ASSERT_EQ(tablet_id, super_blk.tablet_info_list_.at(0).tablet_id_.id());

    // Read and verify
    int64_t read_start_time_s = ObTimeUtility::current_time_s();
    int64_t get_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_get_micro_block_meta(tablet_id, micro_cnt,
              micro_size, micro_size, get_cnt));
    int64_t read_cost_time_s = ObTimeUtility::current_time_s() - read_start_time_s;
    ASSERT_LT(0, get_cnt);
    LOG_INFO("TEST_CASE: Stage 1: finish read micro_blocks", K(get_cnt), K(micro_cnt), K(read_cost_time_s));

    // Execute clear and verify
    ASSERT_EQ(OB_SUCCESS, micro_cache->clear_micro_cache());
    ASSERT_EQ(0, cache_stat.micro_stat().total_micro_cnt_);
    ASSERT_EQ(0, cache_stat.micro_stat().total_micro_size_);
    ASSERT_EQ(0, cache_stat.micro_stat().valid_micro_cnt_);
    ASSERT_EQ(0, cache_stat.micro_stat().valid_micro_size_);
    for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
      ASSERT_EQ(0, arc_info.seg_info_arr_[i].cnt_);
      ASSERT_EQ(0, arc_info.seg_info_arr_[i].size_);
    }
    int64_t stage_cost_time_s = ObTimeUtility::current_time_s() - stage_start_time_s;
    LOG_INFO("TEST_CASE: Stage 1: finish", K(stage_cost_time_s), K(write_cost_time_s),
             K(ckpt_cost_time_s), K(read_cost_time_s));
  }

  // Stage 2: resize to 10250MB
  {
    int64_t stage_start_time_s = ObTimeUtility::current_time_s();
    LOG_INFO("TEST_CASE: Stage 2 - 10250MB, start");
    const int64_t cache_file_size = 10250LL * 1024 * 1024; // 10250MB
    const uint64_t tablet_id = 2000;
    ASSERT_EQ(OB_SUCCESS, micro_cache->resize_micro_cache_file_size(cache_file_size));
    ASSERT_EQ(cache_file_size, micro_cache->cache_file_size_);
    ASSERT_EQ(cache_file_size, phy_blk_mgr.super_blk_.cache_file_size_);

    const int64_t target_write_size = cache_file_size * 5;
    const int64_t micro_cnt = target_write_size / micro_size;

    // Write micro blocks
    int64_t write_start_time_s = ObTimeUtility::current_time_s();
    int64_t add_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, batch_add_micro_block_ignore_eagain(tablet_id, micro_cnt,
              micro_size, micro_size, add_cnt));
    int64_t write_cost_time_s = ObTimeUtility::current_time_s() - write_start_time_s;
    ASSERT_LT(0, add_cnt);
    ASSERT_LT(0, cache_stat.micro_stat().valid_micro_cnt_);
    LOG_INFO("TEST_CASE: Stage 2: finish add micro_blocks", K(add_cnt), K(micro_cnt), K(write_cost_time_s));

    // Wait for blk and meta checkpoints to complete
    int64_t ckpt_start_time_s = ObTimeUtility::current_time_s();
    {
      int64_t ori_blk_ckpt_cnt = task_stat.blk_ckpt_cnt_;
      int64_t ori_micro_ckpt_cnt = task_stat.micro_ckpt_cnt_;
      blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
      persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
      bool finish_ckpt = false;
      int64_t cur_time_s = ObTimeUtility::current_time_s();
      do {
        if (task_stat.blk_ckpt_cnt_ > ori_blk_ckpt_cnt &&
            (task_stat.micro_ckpt_cnt_ > ori_micro_ckpt_cnt) &&
            (persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.complete_full_ckpt_)) {
          finish_ckpt = true;
        } else {
          LOG_INFO("still waiting for checkpoint", K(ori_blk_ckpt_cnt), K(ori_micro_ckpt_cnt), K(task_stat));
          ob_usleep(1000 * 1000);
        }
      } while (!finish_ckpt && ObTimeUtility::current_time_s() - cur_time_s <= TIMEOUT_S);
      ASSERT_EQ(true, finish_ckpt);
      ASSERT_LT(ori_blk_ckpt_cnt, task_stat.blk_ckpt_cnt_);
      ASSERT_LT(ori_micro_ckpt_cnt, task_stat.micro_ckpt_cnt_);
    }
    int64_t ckpt_cost_time_s = ObTimeUtility::current_time_s() - ckpt_start_time_s;
    LOG_INFO("TEST_CASE: Stage 2: finish checkpoint", K(ckpt_cost_time_s));

    // Verify tablet_id in super_blk
    ObSSMicroCacheSuperBlk &super_blk = phy_blk_mgr.super_blk_;
    ASSERT_EQ(1, super_blk.tablet_info_list_.count());
    ASSERT_EQ(tablet_id, super_blk.tablet_info_list_.at(0).tablet_id_.id());

    // Read and verify
    int64_t read_start_time_s = ObTimeUtility::current_time_s();
    int64_t get_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_get_micro_block_meta(tablet_id, micro_cnt,
              micro_size, micro_size, get_cnt));
    int64_t read_cost_time_s = ObTimeUtility::current_time_s() - read_start_time_s;
    ASSERT_LT(0, get_cnt);
    LOG_INFO("TEST_CASE: Stage 2: finish read micro_blocks", K(get_cnt), K(micro_cnt), K(read_cost_time_s));

    ASSERT_EQ(OB_SUCCESS, micro_cache->clear_micro_cache());
    ASSERT_EQ(0, cache_stat.micro_stat().total_micro_cnt_);
    ASSERT_EQ(0, cache_stat.micro_stat().total_micro_size_);
    ASSERT_EQ(0, cache_stat.micro_stat().valid_micro_cnt_);
    ASSERT_EQ(0, cache_stat.micro_stat().valid_micro_size_);
    for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
      ASSERT_EQ(0, arc_info.seg_info_arr_[i].cnt_);
      ASSERT_EQ(0, arc_info.seg_info_arr_[i].size_);
    }
    int64_t stage_cost_time_s = ObTimeUtility::current_time_s() - stage_start_time_s;
    LOG_INFO("TEST_CASE: Stage 2: finish", K(stage_cost_time_s), K(write_cost_time_s),
             K(ckpt_cost_time_s), K(read_cost_time_s));
  }

  // Stage 3: resize to 20GB
  {
    int64_t stage_start_time_s = ObTimeUtility::current_time_s();
    LOG_INFO("TEST_CASE: Stage 3 - 20GB, start");
    const int64_t cache_file_size = max_required_size; // 20GB
    const uint64_t tablet_id = 3000;
    ASSERT_EQ(OB_SUCCESS, micro_cache->resize_micro_cache_file_size(cache_file_size));
    ASSERT_EQ(cache_file_size, micro_cache->cache_file_size_);
    ASSERT_EQ(cache_file_size, phy_blk_mgr.super_blk_.cache_file_size_);

    const int64_t target_write_size = cache_file_size * 5;
    const int64_t micro_cnt = target_write_size / micro_size;

    // Write micro blocks
    int64_t write_start_time_s = ObTimeUtility::current_time_s();
    int64_t add_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, batch_add_micro_block_ignore_eagain(tablet_id, micro_cnt,
              micro_size, micro_size, add_cnt));
    int64_t write_cost_time_s = ObTimeUtility::current_time_s() - write_start_time_s;
    ASSERT_LT(0, add_cnt);
    ASSERT_LT(0, cache_stat.micro_stat().valid_micro_cnt_);
    LOG_INFO("TEST_CASE: Stage 3: finish add micro_blocks", K(add_cnt), K(micro_cnt), K(write_cost_time_s));

    // Wait for blk and meta checkpoints to complete
    int64_t ckpt_start_time_s = ObTimeUtility::current_time_s();
    {
      int64_t ori_blk_ckpt_cnt = task_stat.blk_ckpt_cnt_;
      int64_t ori_micro_ckpt_cnt = task_stat.micro_ckpt_cnt_;
      blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
      persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
      bool finish_ckpt = false;
      int64_t cur_time_s = ObTimeUtility::current_time_s();
      do {
        if (task_stat.blk_ckpt_cnt_ > ori_blk_ckpt_cnt &&
            (task_stat.micro_ckpt_cnt_ > ori_micro_ckpt_cnt) &&
            (persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.complete_full_ckpt_)) {
          finish_ckpt = true;
        } else {
          LOG_INFO("still waiting for checkpoint", K(ori_blk_ckpt_cnt), K(ori_micro_ckpt_cnt), K(task_stat));
          ob_usleep(1000 * 1000);
        }
      } while (!finish_ckpt && ObTimeUtility::current_time_s() - cur_time_s <= TIMEOUT_S);
      ASSERT_EQ(true, finish_ckpt);
      ASSERT_LT(ori_blk_ckpt_cnt, task_stat.blk_ckpt_cnt_);
      ASSERT_LT(ori_micro_ckpt_cnt, task_stat.micro_ckpt_cnt_);
    }
    int64_t ckpt_cost_time_s = ObTimeUtility::current_time_s() - ckpt_start_time_s;
    LOG_INFO("TEST_CASE: Stage 3: finish checkpoint", K(ckpt_cost_time_s));

    // Verify tablet_id in super_blk
    ObSSMicroCacheSuperBlk &super_blk = phy_blk_mgr.super_blk_;
    ASSERT_EQ(1, super_blk.tablet_info_list_.count());
    ASSERT_EQ(tablet_id, super_blk.tablet_info_list_.at(0).tablet_id_.id());

    // Read and verify
    int64_t read_start_time_s = ObTimeUtility::current_time_s();
    int64_t get_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_get_micro_block_meta(tablet_id, micro_cnt,
              micro_size, micro_size, get_cnt));
    int64_t read_cost_time_s = ObTimeUtility::current_time_s() - read_start_time_s;
    ASSERT_LT(0, get_cnt);
    LOG_INFO("TEST_CASE: Stage 3: finish read micro_blocks", K(get_cnt), K(micro_cnt), K(read_cost_time_s));

    ASSERT_EQ(OB_SUCCESS, micro_cache->clear_micro_cache());
    ASSERT_EQ(0, cache_stat.micro_stat().total_micro_cnt_);
    ASSERT_EQ(0, cache_stat.micro_stat().total_micro_size_);
    ASSERT_EQ(0, cache_stat.micro_stat().valid_micro_cnt_);
    ASSERT_EQ(0, cache_stat.micro_stat().valid_micro_size_);
    for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
      ASSERT_EQ(0, arc_info.seg_info_arr_[i].cnt_);
      ASSERT_EQ(0, arc_info.seg_info_arr_[i].size_);
    }
    int64_t stage_cost_time_s = ObTimeUtility::current_time_s() - stage_start_time_s;
    LOG_INFO("TEST_CASE: Stage 3: finish", K(stage_cost_time_s), K(write_cost_time_s),
             K(ckpt_cost_time_s), K(read_cost_time_s));
  }

  // Get real file size before test
  int64_t real_file_size_after_test = 0;
  {
    ObIODFileStat f_stat;
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(micro_cache_file_path, f_stat));
    real_file_size_after_test = f_stat.size_;
    LOG_INFO("TEST_CASE: real micro cache file size after test", K(real_file_size_after_test));
    ASSERT_EQ(real_file_size_after_test, new_file_size); // Ensure file size is not changed
  }

  LOG_INFO("TEST_CASE: finish test_micro_block_resize_to_different_size");
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_resize.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_resize.log", true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}