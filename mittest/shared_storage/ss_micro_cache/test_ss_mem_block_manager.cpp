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
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#include "lib/thread/threads.h"
#include "lib/random/ob_random.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/shared_storage/micro_cache/ob_ss_mem_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::hash;
using namespace oceanbase::blocksstable;
using namespace oceanbase::lib;

class TestSSMemBlockManager : public ::testing::Test
{
public:
  struct MicroBlockWriteInfo
  {
    ObSSMicroBlockCacheKey key_;
    ObSSMemBlock *mem_blk_;
    uint32_t crc_;
    MicroBlockWriteInfo() : key_(), mem_blk_(nullptr), crc_(0) {}
    MicroBlockWriteInfo(const ObSSMicroBlockCacheKey key, ObSSMemBlock *mem_blk, const uint32_t crc)
        : key_(key), mem_blk_(mem_blk), crc_(crc) {}
    MicroBlockWriteInfo(const MicroBlockWriteInfo &other)
        : key_(other.key_), mem_blk_(other.mem_blk_), crc_(other.crc_) {}
    TO_STRING_KV(K_(key), KP_(mem_blk), K_(crc));
  };

public:
  TestSSMemBlockManager() {}
  virtual ~TestSSMemBlockManager() {}
  static void SetUpTestCase();
  static void TearDownTestCase();

  int write_micro_block(ObSSMemBlockManager &mem_blk_mgr, const ObSSMicroBlockCacheKey &micro_key, const int32_t size,
                        ObSSMemBlockHandle &mem_blk_handle, uint32_t &crc);
  int check_micro_data_crc(const ObSSMicroBlockCacheKey &micro_key, const ObSSMemBlock &mem_blk, const uint32_t crc);

private:
  static const int32_t BLOCK_SIZE = 2 * 1024 * 1024;
private:
  ObArenaAllocator allocator_;
};

void TestSSMemBlockManager::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMemBlockManager::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

int TestSSMemBlockManager::write_micro_block(
    ObSSMemBlockManager &mem_blk_mgr,
    const ObSSMicroBlockCacheKey &micro_key,
    const int32_t size,
    ObSSMemBlockHandle &mem_blk_handle,
    uint32_t &crc)
{
  int ret = OB_SUCCESS;
  char *data_buf = nullptr;
  bool alloc_new = false;
  if (OB_UNLIKELY(!micro_key.is_valid() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(size));
  } else if (OB_ISNULL(data_buf = static_cast<char*>(allocator_.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate", KR(ret), K(size));
  } else if (OB_FAIL(TestSSCommonUtil::gen_random_data(data_buf, size))) {
    LOG_WARN("fail to gen random data", KR(ret));
  } else if (OB_FAIL(mem_blk_mgr.add_micro_block_data(micro_key, data_buf, size, mem_blk_handle, crc, alloc_new))) {
    LOG_WARN("fail to add micro block", KR(ret), K(micro_key), K(size));
  }
  return ret;
}

int TestSSMemBlockManager::check_micro_data_crc(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSMemBlock &mem_blk,
    const uint32_t crc)
{
  int ret = OB_SUCCESS;
  ObSSMemBlock::ObSSMemMicroInfo mem_micro_info;
  if (OB_UNLIKELY(!micro_key.is_valid() || !mem_blk.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(mem_blk));
  } else if (OB_FAIL(mem_blk.micro_loc_map_.get_refactored(micro_key, mem_micro_info))) {
    LOG_WARN("fail to get mem_micro_info", KR(ret), K(mem_blk), K(micro_key), K(mem_micro_info));
  } else if (!mem_micro_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_info is invalid", KR(ret), K(mem_micro_info));
  } else {
    const int64_t offset = mem_micro_info.offset_;
    const int64_t size = mem_micro_info.size_;
    const int32_t real_crc = static_cast<uint32_t>(ob_crc64(mem_blk.mem_blk_buf_ + offset, size));
    if (crc != real_crc) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("crc dismatch", KR(ret), K(mem_micro_info), K(crc), K(real_crc));
    }
  }
  return ret;
}

TEST_F(TestSSMemBlockManager, check_mem_blk_count)
{
  // 1. mini_mode
  {
    ObSSMicroCacheStat cache_stat;
    ObSSMemBlockManager mem_blk_mgr(cache_stat);
    ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.init(MTL_ID(), BLOCK_SIZE, true/* is_mini_mode */));

    ObSSMemBlockPool &mem_blk_pool = mem_blk_mgr.mem_blk_pool_;
    ASSERT_EQ(SS_MINI_MODE_MAX_FG_MEM_BLK_CNT, mem_blk_pool.max_fg_count_);
    ASSERT_EQ(SS_MINI_MODE_MAX_BG_MEM_BLK_CNT, mem_blk_pool.max_bg_count_);
    ASSERT_EQ(SS_MINI_MODE_MAX_FG_MEM_BLK_CNT, cache_stat.mem_blk_stat().mem_blk_fg_max_cnt_);
    ASSERT_EQ(SS_MINI_MODE_MAX_BG_MEM_BLK_CNT, cache_stat.mem_blk_stat().mem_blk_bg_max_cnt_);
  }

  // 2. non_mini_mode
  {
    ObSSMicroCacheStat cache_stat;
    ObSSMemBlockManager mem_blk_mgr(cache_stat);
    ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.init(MTL_ID(), BLOCK_SIZE, false/* is_mini_mode */));

    ObSSMemBlockPool &mem_blk_pool = mem_blk_mgr.mem_blk_pool_;
    const int64_t memory_limit = lib::get_tenant_memory_limit(MTL_ID());
    const int64_t max_fg_cnt = SS_FG_MEM_BLK_BASE_CNT + (memory_limit / SS_MEMORY_SIZE_PER_MEM_BLK);
    ASSERT_EQ(max_fg_cnt, mem_blk_pool.max_fg_count_);
    ASSERT_EQ(SS_MAX_BG_MEM_BLK_CNT, mem_blk_pool.max_bg_count_);
    ASSERT_EQ(max_fg_cnt, cache_stat.mem_blk_stat().mem_blk_fg_max_cnt_);
    ASSERT_EQ(SS_MAX_BG_MEM_BLK_CNT, cache_stat.mem_blk_stat().mem_blk_bg_max_cnt_);
  }
}

TEST_F(TestSSMemBlockManager, write_micro_block)
{
  ObSSMicroCacheStat cache_stat;
  ObSSMemBlockManager mem_blk_mgr(cache_stat);
  ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.init(MTL_ID(), BLOCK_SIZE, false));

  const int64_t index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const MacroBlockId macro_id =TestSSCommonUtil::gen_macro_block_id(1);
  ObArray<MicroBlockWriteInfo> micro_block_arr;

  // 1. write large micro_block
  {
    int64_t large_size = BLOCK_SIZE - ObSSMemBlock::get_reserved_size() - index_size;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 1, large_size);
    ObSSMemBlockHandle mem_handle;
    uint32_t crc = 0;
    ASSERT_EQ(OB_SUCCESS, write_micro_block(mem_blk_mgr, micro_key, large_size, mem_handle, crc));
    ASSERT_EQ(OB_SUCCESS, micro_block_arr.push_back(MicroBlockWriteInfo(micro_key, mem_handle.get_ptr(), crc)));
  }

  // 2. write small micro_block
  {
    const int64_t small_size = (1L << 14);
    const int64_t write_cnt = (BLOCK_SIZE - ObSSMemBlock::get_reserved_size()) / (small_size + index_size);
    for (int64_t i = 0; i < write_cnt; ++i) {
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, i + 2, small_size);
      ObSSMemBlockHandle mem_handle;
      uint32_t crc = 0;
      ASSERT_EQ(OB_SUCCESS, write_micro_block(mem_blk_mgr, micro_key, small_size, mem_handle, crc));
      ASSERT_EQ(OB_SUCCESS, micro_block_arr.push_back(MicroBlockWriteInfo(micro_key, mem_handle.get_ptr(), crc)));

    }
  }
  ASSERT_EQ(2, cache_stat.mem_blk_stat().mem_blk_fg_used_cnt_);

  // 3. check micro data crc
  {
    for (int64_t i = 0; i < micro_block_arr.count(); ++i) {
      const ObSSMicroBlockCacheKey micro_key = micro_block_arr[i].key_;
      ObSSMemBlock &mem_blk = *micro_block_arr[i].mem_blk_;
      const uint32_t crc = micro_block_arr[i].crc_;
      ASSERT_EQ(OB_SUCCESS, check_micro_data_crc(micro_key, mem_blk, crc));
    }
  }
}

TEST_F(TestSSMemBlockManager, parallel_write_micro_block)
{
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroCacheStat cache_stat;
  ObSSMemBlockManager mem_blk_mgr(cache_stat);
  ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.init(tenant_id, BLOCK_SIZE, false));

  const int64_t index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int64_t micro_size = (1L << 14);
  const int64_t max_fg_cnt = cache_stat.mem_blk_stat().mem_blk_fg_max_cnt_;
  const int64_t thread_num = 10;
  const int64_t avg_micro_cnt =
      (max_fg_cnt * ((BLOCK_SIZE - ObSSMemBlock::get_reserved_size()) / (micro_size + index_size))) / thread_num;
  ASSERT_LT(0, avg_micro_cnt);

  // 1. write micro_block in parallel
  ObHashMap<ObSSMicroBlockCacheKey, MicroBlockWriteInfo> micro_block_map;
  ASSERT_EQ(OB_SUCCESS, micro_block_map.create(1024, ObMemAttr(tenant_id, "test")));
  {
    auto write_func = [&] (const int64_t idx) {
      for (int64_t i = 0; i < avg_micro_cnt; ++i) {
        MacroBlockId macro_id =TestSSCommonUtil::gen_macro_block_id(1 + idx);
        ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, i + 1, micro_size);
        ObSSMemBlockHandle mem_handle;
        uint32_t crc = 0;
        ASSERT_EQ(OB_SUCCESS, write_micro_block(mem_blk_mgr, micro_key, micro_size, mem_handle, crc));
        MicroBlockWriteInfo micro_info(micro_key, mem_handle.get_ptr(), crc);
        ASSERT_EQ(OB_SUCCESS, micro_block_map.set_refactored(micro_key, micro_info, 0/*overwrite*/));
      }
    };

    std::vector<std::thread> ths;
    for (int64_t i = 0; i < thread_num; ++i) {
      std::thread th(write_func, i);
      ths.push_back(std::move(th));
    }
    for (int64_t i = 0; i < thread_num; ++i) {
      ths[i].join();
    }
  }

  // 2. check micro data crc
  {
    for (auto iter = micro_block_map.begin(); iter != micro_block_map.end(); ++iter) {
      MicroBlockWriteInfo micro_info = iter->second;
      const ObSSMicroBlockCacheKey micro_key = micro_info.key_;
      ObSSMemBlock &mem_blk = *micro_info.mem_blk_;
      const uint32_t crc = micro_info.crc_;
      ASSERT_EQ(OB_SUCCESS, check_micro_data_crc(micro_key, mem_blk, crc));
    }
  }

  // 3. check usage of mem_blk reach limit
  {
    ASSERT_EQ(max_fg_cnt, cache_stat.mem_blk_stat().mem_blk_fg_used_cnt_);
    ASSERT_EQ(max_fg_cnt - 1, mem_blk_mgr.sealed_fg_mem_blks_.get_curr_total());

    const int64_t large_micro_size = micro_size * thread_num;
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(888888);
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 1, large_micro_size);
    ObSSMemBlockHandle mem_handle;
    uint32_t crc = 0;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, write_micro_block(mem_blk_mgr, micro_key, large_micro_size, mem_handle, crc));
  }
}

TEST_F(TestSSMemBlockManager, parallel_alloc_and_free_mem_blk)
{
  ObSSMicroCacheStat cache_stat;
  ObSSMemBlockManager mem_blk_mgr(cache_stat);
  ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.init(MTL_ID(), BLOCK_SIZE, false));

  ObSSMemBlockPool &mem_blk_pool = mem_blk_mgr.mem_blk_pool_;
  const int64_t max_fg_cnt = mem_blk_pool.max_fg_count_;
  const int64_t max_bg_cnt = mem_blk_pool.max_bg_count_;

  // 1. thread 0: alloc 2*max_bg_cnt bg_mem_blk
  // 2. thread 1: free 2*max_bg_cnt bg_mem_blk
  // 3. other thread: alloc and free fg_mem_blk
  {
    ObFixedQueue<ObSSMemBlock> mem_blk_queue;
    ASSERT_EQ(OB_SUCCESS, mem_blk_queue.init(max_bg_cnt));
    const int64_t alloc_bg_cnt = max_bg_cnt * 2;

    auto test_func = [&] (const int64_t idx) {
      if (idx == 0) {
        for (int64_t i = 0; i < alloc_bg_cnt; ++i) {
          ObSSMemBlock *mem_blk = nullptr;
          ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.do_alloc_mem_block(mem_blk, false/* is_fg */));
          ASSERT_EQ(OB_SUCCESS, mem_blk_queue.push(mem_blk));
        }
      } else if (idx == 1) {
        ob_usleep(1000 * 1000);
        int64_t free_cnt = 0;
        ObSSMemBlock *mem_blk = nullptr;
        while (free_cnt < alloc_bg_cnt) {
          if (mem_blk_queue.get_curr_total() > 0) {
            ASSERT_EQ(OB_SUCCESS, mem_blk_queue.pop(mem_blk));
            ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.do_free_mem_block(mem_blk));
            ++free_cnt;
          }
        }
      } else {
        for (int64_t epoch = 0; epoch < 10; ++epoch) {
          ObSSMemBlock *mem_blk = nullptr;
          ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.do_alloc_mem_block(mem_blk, true/* is_fg */));
          ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.do_free_mem_block(mem_blk));
        }
      }
    };

    const int64_t thread_num = max_fg_cnt + 2;
    std::vector<std::thread> ths;
    for (int64_t i = 0; i < thread_num; ++i) {
      std::thread th(test_func, i);
      ths.push_back(std::move(th));
    }
    for (int64_t i = 0; i < thread_num; ++i) {
      ths[i].join();
    }
  }
  ASSERT_EQ(0, mem_blk_pool.used_fg_count_);
  ASSERT_EQ(0, mem_blk_pool.used_bg_count_);
}
}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_mem_block_manager.log*");
  OB_LOGGER.set_file_name("test_ss_mem_block_manager.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}