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
#include "storage/shared_storage/ob_ss_reader_writer.h"
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

TEST_F(TestSSMemMacroCache, test_basic_tmp_file_put_get)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_basic_tmp_file_put_get");
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
      // Generate temporary file macro_id
      MacroBlockId macro_id;
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
      macro_id.set_second_id(100 + i);  // tmp_file_id
      macro_id.set_third_id(0);         // segment_id

      // For temporary files, effective_tablet_id is not used
      const uint64_t effective_tablet_id = ObTabletID::INVALID_TABLET_ID;
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
      // Generate temporary file macro_id (same as put phase)
      MacroBlockId macro_id;
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
      macro_id.set_second_id(100 + i);  // tmp_file_id
      macro_id.set_third_id(0);         // segment_id

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

TEST_F(TestSSMemMacroCache, test_tmp_file_aio_read_from_mem_macro_cache)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_tmp_file_aio_read_from_mem_macro_cache");
  ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
  ASSERT_NE(nullptr, mem_macro_cache);

  // Clear memory cache to avoid interference from previous tests
  ASSERT_EQ(OB_SUCCESS, mem_macro_cache->clear_mem_macro_cache());

  const int64_t macro_blk_size = 64 * 1024;
  const int64_t test_macro_cnt = 5;

  ObArenaAllocator allocator;
  char *write_buf = static_cast<char *>(allocator.alloc(macro_blk_size));
  char *read_buf = static_cast<char *>(allocator.alloc(macro_blk_size));
  ASSERT_NE(nullptr, write_buf);
  ASSERT_NE(nullptr, read_buf);

  // Step 1: Put test data into mem_macro_cache
  MacroBlockId test_macro_ids[test_macro_cnt];
  for (int64_t i = 0; i < test_macro_cnt; ++i) {
    // Generate temporary file macro_id
    MacroBlockId &macro_id = test_macro_ids[i];
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
    macro_id.set_second_id(200 + i);  // tmp_file_id
    macro_id.set_third_id(0);         // segment_id

    // For temporary files, effective_tablet_id is not used
    const uint64_t effective_tablet_id = ObTabletID::INVALID_TABLET_ID;
    char pattern = 'A' + (i % 26);
    MEMSET(write_buf, pattern, macro_blk_size);

    if (OB_FAIL(mem_macro_cache->put(macro_id, effective_tablet_id, write_buf, macro_blk_size))) {
      LOG_WARN("fail to put macro_block", KR(ret), K(i), K(macro_id));
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("Successfully put macro block to mem_macro_cache", K(i), K(macro_id), K(pattern));
  }

  // Step 2: Test aio_read using ObSSTmpFileReader
  ObSSTmpFileReader tmp_file_reader;
  for (int64_t i = 0; i < test_macro_cnt; ++i) {
    const MacroBlockId &macro_id = test_macro_ids[i];
    char expected_pattern = 'A' + (i % 26);
    MEMSET(read_buf, '\0', macro_blk_size);

    // Prepare read info
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = macro_id;
    read_info.offset_ = 0;
    read_info.size_ = macro_blk_size;
    read_info.buf_ = read_buf;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.io_timeout_ms_ = 5000; // 5 seconds
    read_info.io_desc_.set_wait_event(1);
    read_info.io_desc_.set_read();
    read_info.set_ls_epoch_id(0);  // Required for temporary files

    // Perform aio_read
    ObStorageObjectHandle object_handle;
    if (OB_FAIL(tmp_file_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio_read", KR(ret), K(i), K(macro_id));
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    // Wait for completion
    if (OB_FAIL(object_handle.wait())) {
      LOG_WARN("fail to wait for aio_read completion", KR(ret), K(i), K(macro_id));
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    // Verify data
    ASSERT_NE(nullptr, object_handle.get_buffer());
    ASSERT_EQ(macro_blk_size, object_handle.get_data_size());

    // Check if data matches expected pattern
    const char *read_data = object_handle.get_buffer();
    for (int64_t j = 0; j < macro_blk_size; ++j) {
      if (read_data[j] != expected_pattern) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data mismatch", KR(ret), K(i), K(j), "expected", expected_pattern,
                "actual", read_data[j], K(macro_id));
        break;
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    // Verify that data was read from mem_macro_cache by checking IO flag
    ObIOFlag io_flag;
    if (OB_FAIL(object_handle.get_io_handle().get_io_flag(io_flag))) {
      LOG_WARN("fail to get io flag", KR(ret), K(i), K(macro_id));
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    // When reading from mem_macro_cache, the IO should be synchronous
    ASSERT_TRUE(io_flag.is_sync());

    LOG_INFO("Successfully verified aio_read from mem_macro_cache", K(i), K(macro_id),
             K(expected_pattern), "is_sync", io_flag.is_sync());
  }
}

TEST_F(TestSSMemMacroCache, test_tmp_file_read_mem_macro_cache_flow)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_tmp_file_read_mem_macro_cache_flow");
  ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
  ASSERT_NE(nullptr, mem_macro_cache);

  // Clear memory cache to avoid interference from previous tests
  ASSERT_EQ(OB_SUCCESS, mem_macro_cache->clear_mem_macro_cache());

  const int64_t macro_blk_size = 64 * 1024;
  const int64_t test_offset = 1024;
  const int64_t test_size = 32 * 1024;

  ObArenaAllocator allocator;
  char *write_buf = static_cast<char *>(allocator.alloc(macro_blk_size));
  char *read_buf = static_cast<char *>(allocator.alloc(test_size));
  ASSERT_NE(nullptr, write_buf);
  ASSERT_NE(nullptr, read_buf);

  // Generate temporary file macro_id
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(300);  // tmp_file_id
  macro_id.set_third_id(1);     // segment_id

  // Step 1: Put test data with pattern into mem_macro_cache
  const char test_pattern = 'X';
  MEMSET(write_buf, test_pattern, macro_blk_size);
  const uint64_t effective_tablet_id = ObTabletID::INVALID_TABLET_ID;

  if (OB_FAIL(mem_macro_cache->put(macro_id, effective_tablet_id, write_buf, macro_blk_size))) {
    LOG_WARN("fail to put macro_block", KR(ret), K(macro_id));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("Successfully put macro block to mem_macro_cache", K(macro_id), K(test_pattern));

  // Step 2: Verify cache hit statistics before read
  ObSSMemMacroCacheStat &cache_stat = mem_macro_cache->cache_stat_;
  const int64_t hit_cnt_before = cache_stat.hit_stat().hit_cnt_;
  const int64_t miss_cnt_before = cache_stat.hit_stat().miss_cnt_;

  // Step 3: Test partial read using ObSSTmpFileReader
  MEMSET(read_buf, '\0', test_size);

  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = macro_id;
  read_info.offset_ = test_offset;
  read_info.size_ = test_size;
  read_info.buf_ = read_buf;
  read_info.mtl_tenant_id_ = MTL_ID();
  read_info.io_timeout_ms_ = 5000;
  read_info.io_desc_.set_wait_event(1);
  read_info.io_desc_.set_read();
  read_info.set_ls_epoch_id(0);  // Required for temporary files

  ObSSTmpFileReader tmp_file_reader;
  ObStorageObjectHandle object_handle;

  if (OB_FAIL(tmp_file_reader.aio_read(read_info, object_handle))) {
    LOG_WARN("fail to aio_read", KR(ret), K(macro_id));
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  if (OB_FAIL(object_handle.wait())) {
    LOG_WARN("fail to wait for aio_read completion", KR(ret), K(macro_id));
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // Step 4: Verify data correctness
  ASSERT_NE(nullptr, object_handle.get_buffer());
  ASSERT_EQ(test_size, object_handle.get_data_size());

  const char *read_data = object_handle.get_buffer();
  for (int64_t j = 0; j < test_size; ++j) {
    if (read_data[j] != test_pattern) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data mismatch", KR(ret), K(j), "expected", test_pattern,
              "actual", read_data[j], K(macro_id));
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // Step 5: Verify cache hit statistics after read
  const int64_t hit_cnt_after = cache_stat.hit_stat().hit_cnt_;
  const int64_t miss_cnt_after = cache_stat.hit_stat().miss_cnt_;

  ASSERT_EQ(hit_cnt_before + 1, hit_cnt_after);
  ASSERT_EQ(miss_cnt_before, miss_cnt_after); // miss count should not change

  // Step 6: Verify IO flag indicates synchronous read (from cache)
  ObIOFlag io_flag;
  if (OB_FAIL(object_handle.get_io_handle().get_io_flag(io_flag))) {
    LOG_WARN("fail to get io flag", KR(ret), K(macro_id));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(io_flag.is_sync());

  LOG_INFO("Successfully verified read_mem_macro_cache flow", K(macro_id),
           K(test_pattern), "hit_cnt_increase", hit_cnt_after - hit_cnt_before,
           "miss_cnt_unchanged", miss_cnt_after - miss_cnt_before,
           "is_sync", io_flag.is_sync());

  // Step 7: Test cache miss scenario by reading non-existent macro
  // Clear memory cache first to force a cache miss
  ASSERT_EQ(OB_SUCCESS, mem_macro_cache->clear_mem_macro_cache());

  MacroBlockId miss_macro_id;
  miss_macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  miss_macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  miss_macro_id.set_second_id(400);  // tmp_file_id (cleared from cache)
  miss_macro_id.set_third_id(1);     // segment_id

  ObStorageObjectReadInfo miss_read_info;
  miss_read_info.macro_block_id_ = miss_macro_id;
  miss_read_info.offset_ = 0;
  miss_read_info.size_ = test_size;
  miss_read_info.buf_ = read_buf;
  miss_read_info.mtl_tenant_id_ = MTL_ID();
  miss_read_info.io_timeout_ms_ = 5000;
  miss_read_info.io_desc_.set_wait_event(1);
  miss_read_info.io_desc_.set_read();
  miss_read_info.set_ls_epoch_id(0);  // Required for temporary files

  ObStorageObjectHandle miss_handle;
  const int64_t miss_cnt_before_miss = cache_stat.hit_stat().miss_cnt_;

  // This should result in cache miss since we cleared the cache
  // The call may fail due to no actual file, but we can verify cache miss was recorded
  int tmp_ret = tmp_file_reader.aio_read(miss_read_info, miss_handle);

  // Verify cache miss statistics (regardless of final read result)
  const int64_t miss_cnt_after_miss = cache_stat.hit_stat().miss_cnt_;

  // Cache miss should be recorded even if the final read fails
  ASSERT_EQ(miss_cnt_before_miss + 1, miss_cnt_after_miss);

  LOG_INFO("Successfully verified cache miss scenario", K(miss_macro_id),
           "miss_cnt_increase", miss_cnt_after_miss - miss_cnt_before_miss,
           "aio_read_result", tmp_ret);
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