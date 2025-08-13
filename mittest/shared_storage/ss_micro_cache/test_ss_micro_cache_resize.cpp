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
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 27))); // 128MB
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
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
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

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_resize.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_resize.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}