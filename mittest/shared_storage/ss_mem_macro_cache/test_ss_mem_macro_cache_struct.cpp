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
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_struct.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_stat.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMemMacroCacheStruct: public ::testing::Test
{
public:
  TestSSMemMacroCacheStruct() {}
  virtual ~TestSSMemMacroCacheStruct() {};
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSMemMacroCacheStruct::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMemMacroCacheStruct::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMemMacroCacheStruct::SetUp()
{
}

void TestSSMemMacroCacheStruct::TearDown()
{
}

TEST_F(TestSSMemMacroCacheStruct, test_mem_block)
{
  ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
  ASSERT_NE(nullptr, mem_macro_cache);
  mem_macro_cache->evict_task_.need_close_ = true;
  int32_t blk_size = mem_macro_cache->blk_size_;
  ob_usleep(1000 * 1000);

  ObSSMacroCacheMemBlockPool &mem_blk_pool = mem_macro_cache->buf_mgr_.mem_blk_pool_;
  const int64_t total_blk_cnt = mem_blk_pool.total_blk_cnt_;
  ASSERT_LT(0, total_blk_cnt);
  ObSSMemMacroCacheStat &cache_stat = mem_macro_cache->cache_stat_;
  ASSERT_EQ(total_blk_cnt, cache_stat.mem_blk_stat().total_blk_cnt_);
  const int64_t bucket_cnt = mem_macro_cache->buf_mgr_.bucket_cnt_;
  ObSSMacroCacheMemBucket **buckets = mem_macro_cache->buf_mgr_.buckets_;

  {
    ASSERT_NE(nullptr, buckets[0]);
    ASSERT_EQ(nullptr, buckets[0]->first_blk_);
    ObSSMacroCacheMemBlock *new_mem_blk = nullptr;
    ASSERT_EQ(OB_SUCCESS, buckets[0]->alloc_new_mem_blk(new_mem_blk));
    ASSERT_NE(nullptr, new_mem_blk);
    ObSSMacroCacheMemBlkHandle mem_blk_handle;
    ASSERT_EQ(OB_SUCCESS, buckets[0]->get_mem_blk_handle_to_write(mem_blk_handle));
    ASSERT_EQ(true, mem_blk_handle.is_valid());
    ObSSMacroCacheMemBlock *new_mem_blk2 = nullptr;
    ASSERT_EQ(OB_SUCCESS, buckets[0]->alloc_new_mem_blk(new_mem_blk2));
    ASSERT_NE(nullptr, new_mem_blk2);
    ASSERT_EQ(mem_blk_handle.get_ptr(), new_mem_blk);
    mem_blk_handle.reset();

    ASSERT_EQ(OB_SUCCESS, buckets[0]->inner_remove_mem_blk_from_list(new_mem_blk2));
    ASSERT_EQ(new_mem_blk, buckets[0]->first_blk_);
    ASSERT_EQ(OB_SUCCESS, buckets[0]->inner_remove_mem_blk_from_list(new_mem_blk));
    ASSERT_EQ(nullptr, buckets[0]->first_blk_);
  }

  ObArray<ObSSMacroCacheMemBlock *> mem_blks;
  for (int64_t i = 0; i < total_blk_cnt; ++i) {
    ObSSMacroCacheMemBlock *mem_blk = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc_mem_block(mem_blk));
    ASSERT_NE(nullptr, mem_blk);
    ASSERT_EQ(0, mem_blk->ref_cnt_);
    ASSERT_EQ(false, mem_blk->is_valid());
    ASSERT_EQ(OB_SUCCESS, mem_blks.push_back(mem_blk));
    ASSERT_EQ(i + 1, cache_stat.mem_blk_stat().alloc_blk_cnt_);

    const int64_t bucket_idx = i % bucket_cnt;
    mem_blk->set_bucket_idx(bucket_idx);
    ASSERT_EQ(true, mem_blk->is_valid());
    buckets[bucket_idx]->inner_add_mem_blk_into_list(mem_blk);
    ASSERT_EQ(1, mem_blk->ref_cnt_);
  }

  {
    ObSSMacroCacheMemBlock *mem_blk = nullptr;
    ASSERT_EQ(OB_EAGAIN, mem_blk_pool.alloc_mem_block(mem_blk));
    ASSERT_EQ(total_blk_cnt, cache_stat.mem_blk_stat().alloc_blk_cnt_);
  }

  {
    ObSSMacroCacheMemBlock *mem_blk = mem_blks.at(0);
    ASSERT_EQ(0, mem_blk->macro_blk_cnt_);
    ASSERT_EQ(0, mem_blk->heat_val_);
    mem_blk->update_state_after_add_macro(2);
    mem_blk->update_state_after_add_macro(8);
    mem_blk->update_state_after_add_macro(20);
    ASSERT_EQ(3, mem_blk->macro_blk_cnt_);
    ASSERT_EQ(10, mem_blk->heat_val_);
    mem_blk->macro_blk_cnt_ = 0;
    mem_blk->heat_val_ = 0;
  }

  {
    const int64_t exp_cold_cnt = 3;
    for (int64_t i = 0; i < exp_cold_cnt; ++i) {
      mem_blks.at(i)->update_state_after_add_macro(i + 1);
    }
    for (int64_t i = exp_cold_cnt; i < mem_blks.count(); ++i) {
      mem_blks.at(i)->update_state_after_add_macro(i + 10000);
    }
    ObArray<ObSSMacroCacheMemBlkHandle> cold_blk_handles;
    for (int64_t i = 0; i < bucket_cnt - 1; ++i) {
      ASSERT_EQ(OB_SUCCESS, buckets[i]->try_acquire_cold_mem_blocks(exp_cold_cnt, cold_blk_handles));
    }
    if (cold_blk_handles.count() < exp_cold_cnt) {
      ASSERT_EQ(OB_SUCCESS, buckets[bucket_cnt - 1]->try_acquire_cold_mem_blocks(exp_cold_cnt, cold_blk_handles));
    }
    ASSERT_EQ(exp_cold_cnt, cold_blk_handles.count());
    for (int64_t i = 0; i < exp_cold_cnt; ++i) {
      bool find_blk = false;
      for (int64_t j = 0; j < exp_cold_cnt; ++j) {
        if (cold_blk_handles.at(i).get_ptr() == mem_blks.at(j)) {
          find_blk = true;
          break;
        }
      }
      ASSERT_EQ(true, find_blk);
    }

    for (int64_t i = 0; i < mem_blks.count(); ++i) {
      mem_blks.at(i)->macro_blk_cnt_ = 0;
      mem_blks.at(i)->heat_val_ = 0;
    }
  }

  {
    ObSSMacroCacheMemBlock *mem_blk = mem_blks.at(0);
    const int64_t macro_blk_size = 64 * 1024;
    ObArenaAllocator allocator;
    char * buf = static_cast<char *>(allocator.alloc(macro_blk_size));
    ASSERT_NE(nullptr, buf);
    MEMSET(buf, 'a', macro_blk_size);

    const int32_t macro_blk_cnt = blk_size / macro_blk_size;
    for (int64_t i = 0; i < macro_blk_cnt; ++i) {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
      int32_t offset = -1;
      ASSERT_EQ(OB_SUCCESS, mem_blk->add_data(macro_id, buf, macro_blk_size, offset));
      ASSERT_EQ(macro_blk_size * i, offset);
    }
    {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + macro_blk_cnt);
      int32_t offset = -1;
      ASSERT_EQ(OB_BUF_NOT_ENOUGH, mem_blk->add_data(macro_id, buf, macro_blk_size, offset));
    }
  }

  // for (int64_t i = 0; i < total_blk_cnt; ++i) {
  //   ObSSMacroCacheMemBlkHandle mem_blk_handle;
  //   mem_blk_handle.set_ptr(mem_blks.at(i));
  //   ASSERT_EQ(true, mem_blk_handle.is_valid());
  //   ASSERT_EQ(2, mem_blk_handle()->ref_cnt_);

  //   mem_blk_handle.reset();
  //   mem_blks.at(i)->dec_ref_count();
  //   ASSERT_EQ(total_blk_cnt - i - 1, cache_stat.mem_blk_stat().alloc_blk_cnt_);
  // }
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_mem_macro_cache_struct.log*");
  OB_LOGGER.set_file_name("test_ss_mem_macro_cache_struct.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}