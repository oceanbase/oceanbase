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
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}