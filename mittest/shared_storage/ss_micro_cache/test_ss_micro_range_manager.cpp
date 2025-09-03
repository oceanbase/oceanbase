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
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroRangeManager : public ::testing::Test
{
public:
  TestSSMicroRangeManager() = default;
  virtual ~TestSSMicroRangeManager() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

  int mock_create_sub_range(ObConcurrentFIFOAllocator &allocator, ObSSMicroSubRangeInfo *&sub_rng);
  void mock_free_sub_range(ObConcurrentFIFOAllocator &allocator, ObSSMicroSubRangeInfo *sub_rng);

private:
  static const int32_t BLOCK_SIZE = 2 * 1024 * 1024;
};

void TestSSMicroRangeManager::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroRangeManager::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroRangeManager::SetUp()
{}

void TestSSMicroRangeManager::TearDown()
{}

int TestSSMicroRangeManager::mock_create_sub_range(ObConcurrentFIFOAllocator &allocator, ObSSMicroSubRangeInfo *&sub_rng)
{
  int ret = OB_SUCCESS;
  void *ptr = allocator.alloc(sizeof(ObSSMicroSubRangeInfo));
  if (OB_ISNULL(ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(allocator.allocated()));
  } else {
    sub_rng = new(ptr) ObSSMicroSubRangeInfo();
  }
  return ret;
}

void TestSSMicroRangeManager::mock_free_sub_range(ObConcurrentFIFOAllocator &allocator, ObSSMicroSubRangeInfo *sub_rng)
{
  if (nullptr != sub_rng) {
    sub_rng->~ObSSMicroSubRangeInfo();
    allocator.free(sub_rng);
    sub_rng = nullptr;
  }
}

TEST_F(TestSSMicroRangeManager, sub_range_mem_usage)
{
  LOG_INFO("TEST_CASE: start sub_range_mem_usage");
  ObSSMicroRangeManager &micro_range_mgr =  MTL(ObSSMicroCache *)->micro_range_mgr_;
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache *)->cache_stat_;
  ObArray<ObSSMicroSubRangeInfo *> prev_sub_ranges;
  int64_t total_sub_rng_cnt = cache_stat.range_stat().sub_range_cnt_;
  int64_t total_sub_rng_mem = micro_range_mgr.sub_rng_allocator_.hold();
  ObArray<int64_t> sub_rng_cnt_arr;
  const int64_t round_num = 2000;
  LOG_INFO("start small allocator usage check", K(total_sub_rng_cnt), K(total_sub_rng_mem));
  for (int64_t i = 0; i < round_num; ++i) {
    const int64_t prev_sub_rng_cnt = prev_sub_ranges.count();
    if (prev_sub_rng_cnt > 0) {
      for (int64_t i = 0; i < prev_sub_rng_cnt; ++i) {
        ASSERT_NE(nullptr, prev_sub_ranges.at(i));
        micro_range_mgr.destroy_sub_range(prev_sub_ranges.at(i));
      }
    }
    prev_sub_ranges.reset();
    const int64_t new_sub_rng_cnt = ObRandom::rand(10000, 20000);
    ASSERT_LT(0, new_sub_rng_cnt);
    ASSERT_EQ(OB_SUCCESS, sub_rng_cnt_arr.push_back(new_sub_rng_cnt));
    for (int64_t i = 0; i < new_sub_rng_cnt; ++i) {
      ObSSMicroSubRangeInfo *cur_sub_rng = nullptr;
      ASSERT_EQ(OB_SUCCESS, micro_range_mgr.create_sub_range(cur_sub_rng, false));
      ASSERT_NE(nullptr, cur_sub_rng);
      ASSERT_EQ(OB_SUCCESS, prev_sub_ranges.push_back(cur_sub_rng));
    }
    const int64_t cur_sub_rng_cnt = cache_stat.range_stat().sub_range_cnt_;
    const int64_t cur_sub_rng_mem = micro_range_mgr.sub_rng_allocator_.hold();
    LOG_INFO("check small allocator mem usage", K(i), K(cur_sub_rng_cnt), K(cur_sub_rng_mem));
  }
  total_sub_rng_cnt = cache_stat.range_stat().sub_range_cnt_;
  int64_t mem_diff = micro_range_mgr.sub_rng_allocator_.hold() - total_sub_rng_mem;
  total_sub_rng_mem = micro_range_mgr.sub_rng_allocator_.hold();
  LOG_INFO("finish small allocator usage check", K(total_sub_rng_cnt), K(total_sub_rng_mem), K(mem_diff));
  ASSERT_LT(mem_diff, 4194304);
  if (prev_sub_ranges.count() > 0) {
    for (int64_t i = 0; i < prev_sub_ranges.count(); ++i) {
      ASSERT_NE(nullptr, prev_sub_ranges.at(i));
      micro_range_mgr.destroy_sub_range(prev_sub_ranges.at(i));
    }
  }
  prev_sub_ranges.reset();

  ObConcurrentFIFOAllocator cf_allocator;
  const int64_t max_cache_mem_size = 2 * 1024 * 1024 * 1024L;
  ASSERT_EQ(OB_SUCCESS, cf_allocator.init(BLOCK_SIZE, ObMemAttr(MTL_ID(), "TestBlkMgr"), max_cache_mem_size));
  total_sub_rng_mem = cf_allocator.allocated();
  LOG_INFO("start concurrent fifo allocator usage check", K(total_sub_rng_mem));
  for (int64_t i = 0; i < round_num; ++i) {
    const int64_t prev_sub_rng_cnt = prev_sub_ranges.count();
    if (prev_sub_rng_cnt > 0) {
      for (int64_t i = 0; i < prev_sub_rng_cnt; ++i) {
        ASSERT_NE(nullptr, prev_sub_ranges.at(i));
        mock_free_sub_range(cf_allocator, prev_sub_ranges.at(i));
      }
    }
    prev_sub_ranges.reset();
    const int64_t new_sub_rng_cnt = sub_rng_cnt_arr.at(i);
    for (int64_t i = 0; i < new_sub_rng_cnt; ++i) {
      ObSSMicroSubRangeInfo *cur_sub_rng = nullptr;
      ASSERT_EQ(OB_SUCCESS, mock_create_sub_range(cf_allocator, cur_sub_rng));
      ASSERT_NE(nullptr, cur_sub_rng);
      ASSERT_EQ(OB_SUCCESS, prev_sub_ranges.push_back(cur_sub_rng));
    }
    const int64_t cur_sub_rng_mem = cf_allocator.allocated();
    LOG_INFO("check concurrent fifo allocator mem usage", K(i), K(cur_sub_rng_mem));
  }
  mem_diff = cf_allocator.allocated() - total_sub_rng_mem;
  total_sub_rng_mem = cf_allocator.allocated();
  LOG_INFO("finish concurrent fifo allocator usage check", K(total_sub_rng_mem));
  ASSERT_LE(total_sub_rng_mem, 4194304);
}

TEST_F(TestSSMicroRangeManager, basic_test)
{
  LOG_INFO("TEST_CASE: start basic_test");
  ObSSMicroRangeManager &micro_range_mgr =  MTL(ObSSMicroCache *)->micro_range_mgr_;
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache *)->cache_stat_;
  ASSERT_EQ(true, micro_range_mgr.is_inited());

  const int64_t init_range_cnt = micro_range_mgr.init_range_cnt_;
  const int64_t per_range_len = micro_range_mgr.per_range_len_;
  ObSSMicroInitRangeInfo **init_range_arr = micro_range_mgr.init_range_arr_;
  ASSERT_NE(nullptr, init_range_arr);
  for (int64_t i = 0; i < init_range_cnt; ++i) {
    ASSERT_NE(nullptr, init_range_arr[i]);
    if (i < init_range_cnt - 1) {
      ASSERT_EQ((i + 1) * per_range_len - 1, init_range_arr[i]->end_hval_);
    }
  }
  ASSERT_EQ(SS_MICRO_KEY_MAX_HASH_VAL, init_range_arr[init_range_cnt - 1]->end_hval_);

  ObSSMicroInitRangeInfo *init_range = nullptr;
  const uint32_t hash_val = per_range_len;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.inner_get_initial_range(hash_val, init_range));
  ASSERT_NE(nullptr, init_range);
  ASSERT_EQ(2 * per_range_len - 1, init_range->end_hval_);

  ObSSMicroInitRangeInfo *init_range_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.create_init_range(init_range_ptr));
  ASSERT_NE(nullptr, init_range_ptr);
  int64_t sub_rng_cnt = micro_range_mgr.cache_stat_.range_stat().sub_range_cnt_;
  ObSSMicroSubRangeInfo *sub_range_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.create_sub_range(sub_range_ptr, false));
  ASSERT_NE(nullptr, sub_range_ptr);
  ASSERT_EQ(sub_rng_cnt + 1, micro_range_mgr.cache_stat_.range_stat().sub_range_cnt_);

  int64_t total_range_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.get_total_range_cnt(total_range_cnt));
  ASSERT_EQ(0, total_range_cnt);
  int64_t total_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.get_total_range_micro_cnt(total_micro_cnt));
  ASSERT_EQ(0, total_micro_cnt);

  const int64_t micro_meta_cnt = 100;
  ObSSMicroBlockMeta *meta_array = new ObSSMicroBlockMeta[micro_meta_cnt];
  for(int64_t i = 0; i < micro_meta_cnt; ++i) {
    ObSSMicroBlockMetaHandle meta_handle;
    ObSSMicroBlockMeta *meta = &meta_array[i];
    meta->ref_cnt_ = 100;
    uint32_t hval = meta->micro_key_.hash();
    meta_handle.set_ptr(meta);
    ASSERT_EQ(true, meta_handle.is_valid());
    ASSERT_EQ(OB_SUCCESS, micro_range_mgr.insert_micro_meta_into_range(meta_handle));
    ObSSMicroSubRangeHandle sub_range_handle;
    ASSERT_EQ(OB_SUCCESS, micro_range_mgr.inner_get_range(hval, sub_range_handle));
    ASSERT_EQ(true, sub_range_handle.is_valid());
    ObSSMicroInitRangeInfo *init_rng_ptr = nullptr;
    ASSERT_EQ(OB_SUCCESS, micro_range_mgr.inner_get_initial_range(hval, init_rng_ptr));
    ASSERT_NE(nullptr, init_rng_ptr);
  }
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.get_total_range_micro_cnt(total_micro_cnt));
  ASSERT_EQ(micro_meta_cnt, total_micro_cnt);
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.get_total_range_cnt(total_range_cnt));
  ASSERT_NE(0, total_range_cnt);

  // release
  delete [] meta_array;
  micro_range_mgr.clear();
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.get_total_range_cnt(total_range_cnt));
  ASSERT_EQ(0, total_range_cnt);
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.get_total_range_micro_cnt(total_micro_cnt));
  ASSERT_EQ(0, total_micro_cnt);
  micro_range_mgr.destroy();
  ASSERT_EQ(false, micro_range_mgr.is_inited_);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_range_manager.log*");
  OB_LOGGER.set_file_name("test_ss_micro_range_manager.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}