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
#include "test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::lib;

class TestSSMemDataManager : public ::testing::Test
{
public:
  TestSSMemDataManager() {}
  virtual ~TestSSMemDataManager() {}
  static void SetUpTestCase();
  static void TearDownTestCase();

public:
  class TestSSMemDataMgrThread : public Threads
  {
  public:
    enum class TestParallelType
    {
      TEST_PARALLEL_ALLOCATE_MEM_BLOCK,
      TEST_PARALLEL_ALLOC_AND_FREE_BG_MEM_BLOCK, // FARM COMPAT WHITELIST, renamed
      TEST_PARALLEL_ADD_AND_GET_MICRO_DATA,
    };

  public:
    TestSSMemDataMgrThread(ObSSMemDataManager *mem_data_mgr, TestParallelType type)
        : mem_data_mgr_(mem_data_mgr), type_(type), fail_cnt_(0), total_free_bg_cnt_(0),
          total_free_fg_cnt_(0)
    {}
    void run(int64_t idx) final
    {
      if (type_ == TestParallelType::TEST_PARALLEL_ALLOCATE_MEM_BLOCK) {
        parallel_allocate_mem_block(idx);
      } else if (type_ == TestParallelType::TEST_PARALLEL_ALLOC_AND_FREE_BG_MEM_BLOCK) {
        parallel_alloc_and_free_bg_mem_block(idx);
      } else if (type_ == TestParallelType::TEST_PARALLEL_ADD_AND_GET_MICRO_DATA) {
        parallel_add_and_get_micro_data(idx);
      }
    }

    int64_t get_fail_cnt() { return ATOMIC_LOAD(&fail_cnt_); }

  private:
    int parallel_allocate_mem_block(int64_t idx)
    {
      int ret = OB_SUCCESS;
      if (idx % 2 == 0) {
        int64_t allocated_cnt = mem_data_mgr_->mem_block_pool_.def_count_;
        for (int64_t i = 0; OB_SUCC(ret) && i < allocated_cnt; ++i) {
          ObSSMemBlock *tmp_blk = nullptr;
          if (OB_FAIL(mem_data_mgr_->mem_block_pool_.alloc(tmp_blk, true/*is_fg*/))) {
            LOG_WARN("fail to alloc mem_block", KR(ret), K(i));
          } else if (OB_ISNULL(tmp_blk)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mem_block should not be null", KR(ret), K(i));
          } else if (OB_FAIL(mem_blocks_.push_back(tmp_blk))) {
            LOG_WARN("fail to push back mem block", KR(ret), K(tmp_blk));
          }
        }
      } else {
        int def_count = mem_data_mgr_->mem_block_pool_.def_count_;
        int wait_cnt = def_count / 2;
        while (mem_data_mgr_->mem_block_pool_.used_fg_count_ <= wait_cnt) {
          PAUSE();
        }
        for (int i = 0; OB_SUCC(ret) && i < wait_cnt; i++) {
          bool succ_free = false;
          if (OB_FAIL(mem_blocks_[i]->try_free(succ_free))) {
            LOG_WARN("fail to free mem_block", KR(ret), K(mem_blocks_[i]));
          } else if (OB_UNLIKELY(!succ_free)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to free mem_block", KR(ret), K(i), "mem_blk", mem_blocks_[i]);
          }
        }
      }
      if (OB_FAIL(ret)) {
        ATOMIC_INC(&fail_cnt_);
      }
      return ret;
    }

    int parallel_alloc_and_free_bg_mem_block(int64_t idx)
    {
      int ret = OB_SUCCESS;
      int64_t total_bg_cnt = mem_data_mgr_->mem_block_pool_.max_bg_count_ * 2;
      const bool is_fg = false;

      if (idx == 0) { // free bg mem_blk
        const int64_t interval_us = (idx == 0) ? 50 * 1000 : 100 * 1000;
        for (int64_t i = 0; OB_SUCC(ret) && (ATOMIC_LOAD(&total_free_bg_cnt_) < total_bg_cnt); ++i) {
          ObSSMemBlockHandle mem_handle;
          ObSSMemBlock *mem_block = nullptr;
          if (mem_data_mgr_->get_sealed_mem_block_cnt() > 0) {
            if (OB_FAIL(mem_data_mgr_->bg_sealed_mem_blocks_.pop(mem_block))) {
              LOG_WARN("fail to pop", KR(ret));
            } else if (FALSE_IT(mem_handle.set_ptr(mem_block))) {
            } else if (mem_handle.is_valid() && !(mem_handle.get_ptr()->is_fg_)) {
              usleep(interval_us);
              ObSSMemBlock *mem_blk = mem_handle.ptr_;
              mem_handle.reset();
              bool succ_free = false;
              if (OB_FAIL(mem_blk->try_free(succ_free))) {
                LOG_WARN("fail to try free", KR(ret), K(i));
              } else if (succ_free) {
                ATOMIC_AAF(&total_free_bg_cnt_, 1);
                LOG_INFO("succ free bg_mem_block", K(idx), K(i), K_(total_free_bg_cnt));
              }
            }
          }
        }
      } else { // alloc bg_mem_block
        const int64_t interval_us = 30 * 1000;
        for (int64_t j = 0; OB_SUCC(ret) && (j < total_bg_cnt); ++j) {
          ObSSMemBlock *mem_blk = nullptr;
          if (OB_FAIL(mem_data_mgr_->do_alloc_mem_block(mem_blk, is_fg))) {
            LOG_WARN("fail to do alloc mem_block", KR(ret), K(j), K(is_fg));
          } else if (OB_ISNULL(mem_blk)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("bg_mem_block should not be null", KR(ret), K(j));
          } else {
            mem_blk->micro_count_ = 1;
            mem_blk->add_valid_micro_block(100);
            ObSSMemBlockHandle mem_handle;
            mem_handle.set_ptr(mem_blk);
            if (OB_FAIL(mem_data_mgr_->add_into_sealed_block_list(mem_handle))) {
              LOG_WARN("fail to add into sealed block list", KR(ret), K(j), K(is_fg), K(mem_handle));
            } else {
              LOG_INFO("succ alloc mem_block", K(j), K(is_fg), K(mem_handle.get_ptr()->is_completed()));
              usleep(interval_us);
            }
          }
        }
      }

      if (OB_FAIL(ret)) {
        ATOMIC_INC(&fail_cnt_);
      }
      return ret;
    }

    int parallel_add_and_get_micro_data(int64_t idx)
    {
      int ret = OB_SUCCESS;
      DefaultPageAllocator allocator;
      MacroBlockId macro_id;
      int64_t total_micro_data = mem_data_mgr_->mem_block_pool_.def_count_ * (1 << 21) / 20;
      int64_t total_cnt = 0;
      const int64_t LOW_SIZE = (1 << 12);   // 4k
      const int64_t HIGH_SIZE = (1 << 14);  // 16k
      char *buf = nullptr;
      int64_t cur_size = 0;
      ObSSMemBlockHandle tmp_mem_blk_handle;
      while (OB_SUCC(ret) && total_micro_data >= HIGH_SIZE) {
        cur_size = ObRandom::rand(LOW_SIZE, HIGH_SIZE);
        macro_id = TestSSCommonUtil::gen_macro_block_id(idx * 100000 + total_cnt);
        // Ensure micro_key is unique through macroblock id uniqueness
        int32_t offset = 1;
        ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, cur_size);
        total_cnt++;
        uint32_t crc = 0;
        if (OB_FAIL(OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(cur_size))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret), K(cur_size));
        } else if (OB_FAIL(TestSSCommonUtil::gen_random_data(buf, cur_size))) {
          LOG_WARN("fail to generate micro data", K(ret), K(cur_size));
        } else if (OB_FAIL(mem_data_mgr_->add_micro_block_data(micro_key, buf, cur_size, tmp_mem_blk_handle, crc))) {
          LOG_WARN("fail to add micro data", K(ret), K(micro_key), K(cur_size));
        } else {
          total_micro_data -= cur_size;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(mem_data_mgr_->get_micro_block_data(micro_key, tmp_mem_blk_handle, buf, cur_size, crc))) {
            LOG_WARN("fail to get micro data", K(ret), K(micro_key));
          }
        }
        if (buf != nullptr) {
          allocator.free(buf);
        }
      }
      if (OB_FAIL(ret)) {
        ATOMIC_INC(&fail_cnt_);
      }
      return ret;
    }

  private:
    ObSSMemDataManager *mem_data_mgr_;
    TestParallelType type_;
    ObArray<ObSSMemBlock *> mem_blocks_;
    int64_t fail_cnt_;
    int64_t total_free_bg_cnt_;
    int64_t total_free_fg_cnt_;
  };
};

void TestSSMemDataManager::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMemDataManager::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestSSMemDataManager, test_mem_data_mgr)
{
  ObArenaAllocator allocator;
  ObSSMicroCacheStat cache_stat;
  ObSSMemDataManager mem_data_mgr(cache_stat);
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  const int64_t block_size = DEFAULT_BLOCK_SIZE;
  const int64_t cache_file_size = 500L * (1 << 30); // 500G
  const int64_t tenant_mem_limit = 64L * (1 << 30); // 64G

  lib::set_tenant_memory_limit(tenant_id, tenant_mem_limit);
  ASSERT_EQ(OB_SUCCESS, mem_data_mgr.init(tenant_id, cache_file_size, block_size, false/*is_mini_mode*/));
  ObSSMemBlockPool &mem_blk_pool = mem_data_mgr.mem_block_pool_;

  const int64_t def_cnt =
      BASE_MEM_BLK_CNT + MIN(cache_file_size / DISK_SIZE_PER_MEM_BLK, tenant_mem_limit / MEMORY_SIZE_PER_MEM_BLK);
  const int64_t dynamic_cnt = MIN_DYNAMIC_MEM_BLK_CNT + tenant_mem_limit / MEMORY_SIZE_PER_MEM_BLK;
  ASSERT_EQ(def_cnt, mem_blk_pool.def_count_);
  ASSERT_EQ(dynamic_cnt, mem_blk_pool.max_extra_count_);
  ASSERT_EQ(MAX_BG_MEM_BLK_CNT, mem_blk_pool.max_bg_count_);

  const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                 ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int64_t macro_block_cnt = mem_data_mgr.mem_block_pool_.get_fg_max_count();
  const int64_t micro_block_cnt = 128;
  const int64_t micro_size = (block_size - payload_offset) / micro_block_cnt - micro_index_size;
  char *buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, buf);
  const char c = 'a';

  ObSSMemBlockHandle mem_blk_handle;
  uint32_t crc = 0;
  ObSSMicroBlockCacheKey micro_key;
  for (int64_t i = 1; i <= macro_block_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    int64_t offset = 1;
    for (int64_t j = 0; j < micro_block_cnt; ++j) {
      mem_blk_handle.reset();
      const int32_t offset = payload_offset + j * micro_size;
      micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      MEMSET(buf, c, micro_size);
      ASSERT_EQ(OB_SUCCESS, mem_data_mgr.add_micro_block_data(micro_key, buf, micro_size, mem_blk_handle, crc));
      ASSERT_EQ(true, mem_blk_handle.is_valid());
      const uint32_t real_crc = static_cast<int32_t>(ob_crc64(buf, micro_size));
      ASSERT_EQ(crc, real_crc);

      if (i > mem_blk_pool.def_count_) {
        ASSERT_EQ(true, mem_blk_pool.is_alloc_dynamiclly(mem_blk_handle.get_ptr()));
      }
    }
  }
  ASSERT_EQ(mem_data_mgr.get_sealed_mem_block_cnt(), macro_block_cnt - 1);

  // check last micro_block
  ASSERT_EQ(true, mem_blk_handle.is_valid());
  ASSERT_NE(0, micro_size);
  char *read_buf = static_cast<char *>(allocator.alloc(micro_size));
  MEMSET(read_buf, '\0', micro_size);
  ASSERT_EQ(OB_SUCCESS, mem_data_mgr.get_micro_block_data(micro_key, mem_blk_handle, read_buf, micro_size, crc));
  ASSERT_EQ(c, read_buf[0]);
  ASSERT_EQ(OB_CHECKSUM_ERROR, mem_data_mgr.get_micro_block_data(micro_key, mem_blk_handle, read_buf, micro_size, crc + 1));
}

TEST_F(TestSSMemDataManager, test_mem_data_mgr_mini_mode)
{
  ObArenaAllocator allocator;
  ObSSMicroCacheStat cache_stat;
  ObSSMemDataManager mem_data_mgr(cache_stat);
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  const int64_t block_size = DEFAULT_BLOCK_SIZE;
  const int64_t cache_file_size = (1L << 30);

  ASSERT_EQ(OB_SUCCESS, mem_data_mgr.init(tenant_id, cache_file_size, block_size, true/*is_mini_mode*/));

  ObSSMemBlockPool &mem_blk_pool = mem_data_mgr.mem_block_pool_;
  ASSERT_EQ(MINI_MODE_BASE_MEM_BLK_CNT, mem_blk_pool.def_count_);
  ASSERT_EQ(MIN_DYNAMIC_MEM_BLK_CNT, mem_blk_pool.max_extra_count_);
  ASSERT_EQ(MINI_MODE_MAX_BG_MEM_BLK_CNT, mem_blk_pool.max_bg_count_);

  const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                 ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int64_t macro_block_cnt = mem_data_mgr.mem_block_pool_.get_fg_max_count();
  const int64_t micro_block_cnt = 20;
  const int64_t micro_size = (block_size - payload_offset) / micro_block_cnt - micro_index_size;
  char *buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, buf);
  MEMSET(buf, 'a', micro_size);

  ObArray<ObSSMemBlock *> mem_blk_arr;
  ObSSMemBlockHandle mem_blk_handle;
  uint32_t crc = 0;
  for (int64_t i = 1; i <= macro_block_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    int64_t offset = payload_offset;
    for (int64_t j = 0; j < micro_block_cnt; ++j) {
      mem_blk_handle.reset();
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS, mem_data_mgr.add_micro_block_data(micro_key, buf, micro_size, mem_blk_handle, crc));
      ASSERT_EQ(true, mem_blk_handle.is_valid());
      ASSERT_EQ(true, mem_data_mgr.mem_block_pool_.is_alloc_dynamiclly(mem_blk_handle.get_ptr()));
      const uint32_t real_crc = static_cast<int32_t>(ob_crc64(buf, micro_size));
      ASSERT_EQ(crc, real_crc);
    }
    ASSERT_EQ(OB_SUCCESS, mem_blk_arr.push_back(mem_blk_handle.get_ptr()));
  }

  MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + macro_block_cnt + 1);
  int32_t offset = payload_offset;
  ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
  mem_blk_handle.reset();
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mem_data_mgr.add_micro_block_data(micro_key, buf, micro_size, mem_blk_handle, crc));
  ASSERT_EQ(macro_block_cnt, mem_data_mgr.get_sealed_mem_block_cnt());

  const int64_t sealed_cnt = mem_data_mgr.get_sealed_mem_block_cnt();
  for (int64_t i = sealed_cnt - 1; i >= 0; --i) {
    ObSSMemBlock *sealed_mem_blk = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_data_mgr.fg_sealed_mem_blocks_.pop(sealed_mem_blk));
    ASSERT_NE(nullptr, sealed_mem_blk);
    bool succ_free = false;
    ASSERT_EQ(OB_SUCCESS, sealed_mem_blk->try_free(succ_free));
    ASSERT_EQ(true, succ_free);
  }

  ASSERT_EQ(0, mem_data_mgr.mem_block_pool_.used_fg_count_);
  ASSERT_EQ(0, mem_data_mgr.mem_block_pool_.used_bg_count_);

  mem_data_mgr.destroy();
}

/* One thread allocates N mem_blocks and the other thread frees N/2 mem_blocks in parallel. */
TEST_F(TestSSMemDataManager, test_parallel_allocate_mem_block)
{
  const static uint32_t BLOCK_SIZE = (1 << 12);
  ObSSMicroCacheStat cache_stat;
  ObSSMemDataManager mem_data_mgr(cache_stat);
  const uint64_t tenant_id = MTL_ID();
  ASSERT_EQ(true, is_valid_tenant_id(tenant_id));
  const int64_t cache_file_size = (1L << 30);

  ASSERT_EQ(OB_SUCCESS, mem_data_mgr.init(tenant_id, cache_file_size, BLOCK_SIZE, false/*is_mini_mode*/));

  TestSSMemDataManager::TestSSMemDataMgrThread threads(
      &mem_data_mgr, TestSSMemDataMgrThread::TestParallelType::TEST_PARALLEL_ALLOCATE_MEM_BLOCK);
  threads.set_thread_count(2);
  threads.start();
  threads.wait();

  int64_t def_count = mem_data_mgr.mem_block_pool_.def_count_;
  const int64_t allocated_cnt = mem_data_mgr.mem_block_pool_.used_fg_count_ + mem_data_mgr.mem_block_pool_.used_bg_count_;
  ASSERT_EQ(def_count - def_count / 2, allocated_cnt);
  ASSERT_EQ(0, threads.get_fail_cnt());
}

TEST_F(TestSSMemDataManager, test_parallel_alloc_and_free_bg_mem_block)
{
  const static uint32_t BLOCK_SIZE = (1 << 12);
  ObSSMicroCacheStat cache_stat;
  ObSSMemDataManager mem_data_mgr(cache_stat);
  const uint64_t tenant_id = MTL_ID();
  ASSERT_EQ(true, is_valid_tenant_id(tenant_id));
  const int64_t cache_file_size = (1L << 30);
  ASSERT_EQ(OB_SUCCESS, mem_data_mgr.init(tenant_id, cache_file_size, BLOCK_SIZE, false/*is_mini_mode*/));

  TestSSMemDataManager::TestSSMemDataMgrThread threads(
      &mem_data_mgr, TestSSMemDataMgrThread::TestParallelType::TEST_PARALLEL_ALLOC_AND_FREE_BG_MEM_BLOCK);
  threads.set_thread_count(2);
  threads.start();
  threads.wait();

  ASSERT_EQ(0, threads.get_fail_cnt());
}

/* Multiple threads add/get micro block data in parallel. Each thread writes K MB of data, the data is distributed on
 * multiple mem_blocks, and the data is verified to be correct through read requests.  */
TEST_F(TestSSMemDataManager, test_parallel_add_and_get_micro_data)
{
  const static uint32_t BLOCK_SIZE = (1 << 21);
  ObSSMicroCacheStat cache_stat;
  ObSSMemDataManager mem_data_mgr(cache_stat);
  const uint64_t tenant_id = MTL_ID();
  ASSERT_EQ(true, is_valid_tenant_id(tenant_id));
  const int64_t cache_file_size = (1L << 30);

  ASSERT_EQ(OB_SUCCESS, mem_data_mgr.init(tenant_id, cache_file_size, BLOCK_SIZE, false/*is_mini_mode*/));
  TestSSMemDataManager::TestSSMemDataMgrThread threads(
      &mem_data_mgr, TestSSMemDataMgrThread::TestParallelType::TEST_PARALLEL_ADD_AND_GET_MICRO_DATA);
  threads.set_thread_count(10);
  threads.start();
  threads.wait();

  ASSERT_EQ(0, threads.get_fail_cnt());
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_mem_data_manager.log*");
  OB_LOGGER.set_file_name("test_ss_mem_data_manager.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}