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
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_struct.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_stat.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_evict_task.h"
#include "storage/shared_storage/ob_ss_preread_cache_manager.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMemMacroCache: public ::testing::Test
{
public:
  TestSSMemMacroCache() {}
  virtual ~TestSSMemMacroCache() {};
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSMemMacroCache::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMemMacroCache::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMemMacroCache::SetUp()
{
}

void TestSSMemMacroCache::TearDown()
{
}

TEST_F(TestSSMemMacroCache, test_basic)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_basic");
  ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
  ASSERT_NE(nullptr, mem_macro_cache);
  ObSSMacroCacheMemBlockPool &mem_blk_pool = mem_macro_cache->buf_mgr_.mem_blk_pool_;
  const int64_t total_blk_cnt = mem_blk_pool.total_blk_cnt_;
  int32_t blk_size = mem_macro_cache->blk_size_;
  const int64_t macro_blk_size = 64 * 1024;

  ObArenaAllocator allocator;
  int64_t macro_blk_cnt = 0;
  {
    TestSSCommonCheckTimeGuard time_guard("put macro_block");
    // 1. put macro_block
    char * buf = static_cast<char *>(allocator.alloc(macro_blk_size));
    ASSERT_NE(nullptr, buf);

    macro_blk_cnt = total_blk_cnt / 2 * blk_size / macro_blk_size;
    for (int64_t i = 0; i < macro_blk_cnt; ++i) {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
      const uint64_t effective_tablet_id = macro_id.second_id();
      char c = macro_id.hash() % 26 + 'a';
      MEMSET(buf, c, macro_blk_size);
      int32_t offset = -1;
      if (OB_FAIL(mem_macro_cache->put(macro_id, effective_tablet_id, buf, macro_blk_size))) {
        LOG_WARN("fail to put macro_block", KR(ret), K(i), K(macro_id));
      }
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }

  {
    TestSSCommonCheckTimeGuard time_guard("check mem_blk stat");
    // 2. check mem_block stat
    int64_t tmp_macro_blk_cnt = 0;
    ObSSMacroCacheMemBucket **buckets = mem_macro_cache->buf_mgr_.buckets_;
    const int64_t bucket_cnt = mem_macro_cache->buf_mgr_.bucket_cnt_;
    ASSERT_NE(nullptr, buckets);
    ASSERT_LT(0, bucket_cnt);
    for (int64_t i = 0; i < bucket_cnt; ++i) {
      ASSERT_NE(nullptr, buckets[i]);
      if (nullptr != buckets[i]->first_blk_) {
        ObSSMacroCacheMemBlock *cur_mem_blk = buckets[i]->first_blk_;
        while (nullptr != cur_mem_blk) {
          ASSERT_EQ(blk_size / macro_blk_size + 1, cur_mem_blk->ref_cnt_);
          if (cur_mem_blk->macro_blk_cnt_ > 0) {
            tmp_macro_blk_cnt += cur_mem_blk->macro_blk_cnt_;
            ASSERT_EQ(blk_size / macro_blk_size, cur_mem_blk->macro_blk_cnt_);
            ASSERT_LT(0, cur_mem_blk->heat_val_);
          }
          cur_mem_blk = cur_mem_blk->next_;
        }
      }
    }
    ASSERT_EQ(macro_blk_cnt, tmp_macro_blk_cnt);
  }

  {
    TestSSCommonCheckTimeGuard time_guard("get macro_block");
    // 3. get macro_block meta
    char * read_buf = static_cast<char *>(allocator.alloc(macro_blk_size));
    ASSERT_NE(nullptr, read_buf);
    MEMSET(read_buf, '\0', macro_blk_size);

    for (int64_t i = 0; i < macro_blk_cnt; ++i) {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
      char c = macro_id.hash() % 26 + 'a';
      ObIOInfo io_info;
      ObStorageObjectHandle obj_handle;
      bool is_hit_cache = false;
      if (OB_FAIL(TestSSCommonUtil::init_io_info(io_info, macro_id, 0, macro_blk_size, read_buf))) {
        LOG_WARN("fail to init io info", KR(ret), K(i));
      } else if (OB_FAIL(mem_macro_cache->get(macro_id, io_info, obj_handle, is_hit_cache))) {
        LOG_WARN("fail to get_micro_block_cache", KR(ret), K(macro_id));
      } else if (OB_UNLIKELY(!is_hit_cache)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("not hit mem_macro_cache", KR(ret), K(i));
      } else if (OB_FAIL(obj_handle.wait())) {
        LOG_WARN("fail to wait until get micro block data", KR(ret), K(i));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < macro_blk_size; ++j) {
          if (OB_ISNULL(obj_handle.get_buffer())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("buffer should not be null", KR(ret), K(j), K(macro_blk_size), K(obj_handle));
          } else if (obj_handle.get_buffer()[j] != c) {
            ret = OB_IO_ERROR;
            LOG_WARN("data error", KR(ret), K(i), K(j), K(macro_blk_size), K(c), K(obj_handle.get_buffer()));
          }
        }
      }
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
}

TEST_F(TestSSMemMacroCache, test_huge_data_evict)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_huge_data_evict");
  ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
  ASSERT_NE(nullptr, mem_macro_cache);
  ASSERT_EQ(OB_SUCCESS, mem_macro_cache->clear_mem_macro_cache());

  ObSSMemMacroCacheStat &cache_stat = mem_macro_cache->cache_stat_;
  ObSSMacroCacheMemBlockPool &mem_blk_pool = mem_macro_cache->buf_mgr_.mem_blk_pool_;
  const int64_t total_blk_cnt = mem_blk_pool.total_blk_cnt_;
  const int64_t write_blk_cnt = total_blk_cnt * 5;
  int32_t blk_size = mem_macro_cache->blk_size_;
  const int64_t min_macro_blk_size = 120 * 1024;
  const int64_t max_macro_blk_size = 2 * 1024 * 1024;
  const int64_t align_size = 4 * 1024;
  ObArenaAllocator allocator;
  char * buf = static_cast<char *>(allocator.alloc(max_macro_blk_size));
  ASSERT_NE(nullptr, buf);

  const int64_t macro_blk_cnt = write_blk_cnt * blk_size / min_macro_blk_size;
  {
    for (int64_t i = 0; i < macro_blk_cnt; ++i) {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
      const uint64_t effective_tablet_id = macro_id.second_id();
      char c = macro_id.hash() % 26 + 'a';
      MEMSET(buf, c, min_macro_blk_size);
      int32_t offset = -1;
      if (OB_FAIL(mem_macro_cache->put(macro_id, effective_tablet_id, buf, min_macro_blk_size))) {
        LOG_WARN("fail to put macro_block", KR(ret), K(i), K(macro_id));
      }
    }

    ob_usleep(3 * 1000 * 1000);
    ASSERT_LT(0, cache_stat.mem_blk_stat().evict_blk_cnt_);
    ASSERT_LT(0, cache_stat.mem_blk_stat().alloc_blk_cnt_);
    ASSERT_LT(0, cache_stat.mem_usage_stat().macro_meta_cnt_);

    ObSSLocalCacheService *local_cache = MTL(ObSSLocalCacheService *);
    ASSERT_NE(nullptr, local_cache);
    local_cache->local_cache_param_refresh_task_.is_inited_ = false;
    ob_usleep(1000 * 1000);

    mem_macro_cache->update_memory_limit_size(0);
    ob_usleep(3 * 1000 * 1000);
    ASSERT_EQ(0, cache_stat.mem_blk_stat().alloc_blk_cnt_);
    ASSERT_EQ(0, cache_stat.mem_usage_stat().macro_meta_cnt_);
    LOG_INFO("TEST_CASE_CHECK: step 1", K(macro_blk_cnt), K(cache_stat));
  }

  ObTenantBase *tenant_base = MTL_CTX();
  {
    mem_macro_cache->update_memory_limit_size(total_blk_cnt * blk_size);
    auto add_func = [&](const int64_t idx) {
      ObTenantEnv::set_tenant(tenant_base);
      char * cur_buf = static_cast<char *>(allocator.alloc(max_macro_blk_size));
      ASSERT_NE(nullptr, cur_buf);
      MEMSET(cur_buf, '\0', max_macro_blk_size);

      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < macro_blk_cnt; ++i) {
        MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
        const uint64_t effective_tablet_id = macro_id.second_id();
        char c = macro_id.hash() % 26 + 'a';
        const int64_t macro_blk_size = (min_macro_blk_size + (c - 'a') * 1024) / align_size * align_size;
        MEMSET(cur_buf, c, macro_blk_size);
        int32_t offset = -1;
        if (OB_TMP_FAIL(mem_macro_cache->put(macro_id, effective_tablet_id, cur_buf, macro_blk_size))) {
          LOG_WARN("fail to put macro_block", KR(tmp_ret), K(idx), K(i), K(macro_id));
        }
        if (OB_EAGAIN == tmp_ret) {
          tmp_ret = OB_SUCCESS;
        }
        ASSERT_EQ(OB_SUCCESS, tmp_ret);
        const int64_t sleep_time_us = ObRandom::rand(1, 100);
        ob_usleep(sleep_time_us);

        ObSSMacroCacheBlkMetaHandle macro_meta_handle;
        if (OB_TMP_FAIL(mem_macro_cache->meta_mgr_.get_mem_macro_block_meta(macro_id, macro_meta_handle))) {
          LOG_WARN("fail to get mem_macro_block meta", KR(tmp_ret), K(i), K(macro_id));
        } else {
          ASSERT_EQ(true, macro_meta_handle.is_valid());
          macro_meta_handle()->update_access_info(1, 50);
          ASSERT_EQ(1, macro_meta_handle()->access_start_);
          ASSERT_EQ(50, macro_meta_handle()->access_end_);
          ASSERT_EQ(macro_blk_size, macro_meta_handle()->size_);
          if (OB_TMP_FAIL(macro_meta_handle()->check_crc())) {
            LOG_WARN("fail to check crc", KR(tmp_ret), K(i), K(macro_id), KPC(macro_meta_handle.get_ptr()));
          }
        }

        if (OB_HASH_NOT_EXIST == tmp_ret) {
          tmp_ret = OB_SUCCESS;
        }
        ASSERT_EQ(OB_SUCCESS, tmp_ret);
      }

      int64_t succ_cnt = 0;
      for (int64_t i = 0; i < macro_blk_cnt; ++i) {
        MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
        ObSSMacroCacheBlkMetaHandle macro_meta_handle;
        if (OB_TMP_FAIL(mem_macro_cache->meta_mgr_.get_mem_macro_block_meta(macro_id, macro_meta_handle))) {
          LOG_WARN("fail to get mem_macro_block meta", KR(tmp_ret), K(i), K(macro_id));
        } else {
          ++succ_cnt;
          ASSERT_EQ(true, macro_meta_handle.is_valid());
          if (OB_TMP_FAIL(macro_meta_handle()->check_crc())) {
            LOG_WARN("fail to check crc", KR(tmp_ret), K(i), K(macro_id), KPC(macro_meta_handle.get_ptr()));
          }
        }

        if (OB_HASH_NOT_EXIST == tmp_ret) {
          tmp_ret = OB_SUCCESS;
        }
        ASSERT_EQ(OB_SUCCESS, tmp_ret);
      }
      ASSERT_LT(0, succ_cnt);
    };

    std::vector<std::thread> ths;
    const int64_t thread_num = 3;
    for (int64_t i = 0; i < thread_num; ++i) {
      std::thread th(add_func, i);
      ths.push_back(std::move(th));
    }
    for (int64_t i = 0; i < thread_num; ++i) {
      ths[i].join();
    }

    ASSERT_LT(0, cache_stat.macro_blk_stat().valid_macro_cnt_);
    LOG_INFO("TEST_CASE_CHECK: step 2", K(cache_stat));
  }

  {
    ASSERT_EQ(0, cache_stat.macro_blk_stat_.expire_macro_cnt_);
    ASSERT_EQ(0, cache_stat.mem_blk_stat_.expire_blk_cnt_);
    const int64_t prev_macro_cnt = cache_stat.macro_blk_stat_.valid_macro_cnt_;
    mem_macro_cache->evict_task_.close_task();
    ob_usleep(2 * 1000 * 1000L);
    const int64_t TTL_MAX_MACRO_CNT = 10;
    int64_t ttl_macro_cnt = 0;
    for (int64_t i = 0; i < macro_blk_cnt && ttl_macro_cnt < TTL_MAX_MACRO_CNT; ++i) {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
      ObSSMacroCacheBlkMetaHandle macro_meta_handle;
      if (OB_SUCCESS == mem_macro_cache->meta_mgr_.get_mem_macro_block_meta(macro_id, macro_meta_handle)) {
        ++ttl_macro_cnt;
        ASSERT_EQ(true, macro_meta_handle.is_valid());
        macro_meta_handle.get_ptr()->create_time_us_ -= SS_DEF_MACRO_BLOCK_TTL_TIME_US;
      }
    }
    mem_macro_cache->evict_task_.evict_ctx_.prev_check_ttl_us_ = ObTimeUtility::current_time_us() - SS_DEF_CHECK_MACRO_TTL_INTERVAL_US;
    mem_macro_cache->evict_task_.open_task();
    ob_usleep(3 * 1000 * 1000L);
    ASSERT_LT(cache_stat.macro_blk_stat_.valid_macro_cnt_, prev_macro_cnt);
    ASSERT_LE(cache_stat.macro_blk_stat_.valid_macro_cnt_ + TTL_MAX_MACRO_CNT, prev_macro_cnt);
    ASSERT_LT(0, cache_stat.macro_blk_stat_.expire_macro_cnt_);
    ASSERT_LT(0, cache_stat.mem_blk_stat_.expire_blk_cnt_);
    LOG_INFO("TEST_CASE_CHECK: step 3", K(cache_stat));
  }

  {
    ASSERT_EQ(OB_SUCCESS, mem_macro_cache->clear_mem_macro_cache());
    ASSERT_EQ(0, cache_stat.macro_blk_stat().valid_macro_cnt_);
    LOG_INFO("TEST_CASE_CHECK: step 4", K(cache_stat));
  }
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_mem_macro_cache.log*");
  OB_LOGGER.set_file_name("test_ss_mem_macro_cache.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}