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
#include "test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheStruct : public ::testing::Test
{
public:
  TestSSMicroCacheStruct() = default;
  virtual ~TestSSMicroCacheStruct() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

private:
  const static uint32_t ORI_MICRO_REF_CNT = 0;
};

void TestSSMicroCacheStruct::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheStruct::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheStruct::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 31)));
  micro_cache->task_runner_.is_inited_ = false;
  micro_cache->start();

}

void TestSSMicroCacheStruct::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

TEST_F(TestSSMicroCacheStruct, micro_block_meta)
{
  const int32_t micro_size = 20;
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObSSPhysicalBlockHandle phy_blk_handle;
  const int64_t phy_blk_idx = 2;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_block_handle(phy_blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  int64_t reuse_version = phy_blk_handle.ptr_->reuse_version_;

  ObSSMicroBlockMeta *cur_micro_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(cur_micro_meta));
  ASSERT_NE(nullptr, cur_micro_meta);
  ObSSMicroBlockMeta &micro_meta = *(cur_micro_meta);
  micro_meta.reuse_version_ = reuse_version;
  micro_meta.data_dest_ = phy_blk_idx * phy_blk_mgr.block_size_;
  micro_meta.length_ = micro_size;
  micro_meta.is_persisted_ = true;
  micro_meta.access_time_ = 1;
  ASSERT_EQ(0, micro_meta.ref_cnt_);
  ASSERT_EQ(false, micro_meta.is_valid_field()); // cuz no exist valid micro_key
  MacroBlockId macro_id(0, 100, 0);
  int32_t offset = 1;
  ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
  micro_meta.micro_key_ = micro_key;
  ASSERT_EQ(true, micro_meta.is_valid_field());
  ASSERT_EQ(true, micro_meta.is_valid());

  micro_meta.is_persisted_ = false;
  ASSERT_EQ(false, micro_meta.is_persisted_valid());
  micro_meta.is_persisted_ = true;
  micro_meta.reuse_version_ += 1;
  ASSERT_EQ(false, micro_meta.is_persisted_valid());
  micro_meta.reuse_version_ -= 1;
  ASSERT_EQ(true, micro_meta.is_persisted_valid());

  micro_meta.inc_ref_count();
  ASSERT_EQ(1, micro_meta.ref_cnt_);

  ObSSMicroBlockMetaHandle micro_handle;
  micro_handle.set_ptr(cur_micro_meta);
  ASSERT_EQ(2, micro_meta.ref_cnt_);
  micro_handle.reset();
  ASSERT_EQ(1, micro_meta.ref_cnt_);
  micro_meta.dec_ref_count();
}

TEST_F(TestSSMicroCacheStruct, micro_block_meta_2)
{
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  const int64_t fixed_cnt = 100;
  ObSSMicroBlockMeta *micro_meta = nullptr;
  ObArray<ObSSMicroBlockMeta *> micro_meta_arr;
  for (int64_t i = 0; i < fixed_cnt; ++i) {
    ASSERT_EQ(i, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
    micro_meta = nullptr;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(micro_meta));
    ASSERT_NE(nullptr, micro_meta);
    ASSERT_EQ(OB_SUCCESS, micro_meta_arr.push_back(micro_meta));
    ASSERT_EQ(ori_micro_ref_cnt, micro_meta->ref_cnt_);
  }
  micro_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(micro_meta));
  ASSERT_NE(nullptr, micro_meta);
  ASSERT_EQ(ori_micro_ref_cnt, micro_meta->ref_cnt_);
  ASSERT_EQ(fixed_cnt + 1, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());

  micro_meta->inc_ref_count();
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta->ref_cnt_);
  micro_meta->dec_ref_count();
  ASSERT_EQ(ori_micro_ref_cnt, micro_meta->ref_cnt_);
  ASSERT_EQ(fixed_cnt, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
  micro_meta = nullptr;

  micro_meta = micro_meta_arr.at(0);
  ASSERT_EQ(ori_micro_ref_cnt, micro_meta->ref_cnt_);
  micro_meta->inc_ref_count();
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta->ref_cnt_);
  micro_meta->inc_ref_count();
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta->ref_cnt_);
  micro_meta->try_free();
  ASSERT_EQ(fixed_cnt, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta->ref_cnt_);
  micro_meta->dec_ref_count();
  ASSERT_EQ(fixed_cnt - 1, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
  micro_meta = nullptr;

  for (int64_t i = 1; i < fixed_cnt; ++i) {
    ASSERT_EQ(ori_micro_ref_cnt, micro_meta_arr.at(i)->ref_cnt_);
    micro_meta_arr.at(i)->free();
  }
  ASSERT_EQ(0, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
}

TEST_F(TestSSMicroCacheStruct, micro_block_meta_3)
{
  const int32_t micro_size = 100;

  ObSSMicroBlockMeta *micro_meta_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(micro_meta_ptr));
  ASSERT_NE(nullptr, micro_meta_ptr);
  ObSSMicroBlockMetaHandle micro_meta_handle;
  micro_meta_handle.set_ptr(micro_meta_ptr);
  ObSSMicroBlockMeta &micro_meta = *micro_meta_ptr;

  ASSERT_EQ(FALSE, micro_meta.is_valid_field());
  micro_meta.first_val_ = 1;
  micro_meta.length_ = micro_size;
  micro_meta.access_time_ = 20;
  MacroBlockId macro_id(0, 100, 0);
  int32_t offset = 10;
  ObSSMicroBlockCacheKey micro_key;
  micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
  micro_key.micro_id_.macro_id_ = macro_id;
  micro_key.micro_id_.offset_ = offset;
  micro_key.micro_id_.macro_id_ = macro_id;
  micro_key.micro_id_.size_ = micro_size;
  micro_meta.micro_key_ = micro_key;
  ASSERT_EQ(true, micro_meta.is_valid_field());

  micro_meta.mark_invalid();
  ObSSMemBlockPool &mem_blk_pool = MTL(ObSSMicroCache *)->mem_data_mgr_.mem_block_pool_;
  ObSSMemBlock *mem_blk_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_blk_ptr, true/*is_fg*/));
  ObSSMemBlockHandle mem_blk_handle;
  mem_blk_handle.set_ptr(mem_blk_ptr);
  ASSERT_EQ(true, mem_blk_handle.is_valid());
  mem_blk_handle()->reuse_version_ = 5;

  uint32_t crc = 1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, micro_meta.init(micro_key, mem_blk_handle, 0, crc));
  ASSERT_EQ(OB_SUCCESS, micro_meta.init(micro_key, mem_blk_handle, 100, crc));
  ASSERT_EQ(true, micro_meta.is_valid_field());
  ASSERT_EQ(false, micro_meta.is_valid());
  ObSSMemMicroInfo mem_info(offset, 100);
  ASSERT_EQ(OB_SUCCESS, mem_blk_handle()->micro_offset_map_.set_refactored(micro_key, mem_info));
  mem_blk_handle()->reuse_version_ += 1;
  ASSERT_EQ(false, micro_meta.is_valid());
  mem_blk_handle()->reuse_version_ -= 1;
  ASSERT_EQ(true, micro_meta.is_valid());

  ASSERT_EQ(mem_blk_handle.get_ptr(), micro_meta.get_mem_block());
  ASSERT_EQ(mem_blk_handle()->reuse_version_, micro_meta.reuse_version_);

  micro_meta_handle.reset();
  ASSERT_EQ(0, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
}

TEST_F(TestSSMicroCacheStruct, micro_meta_handle)
{
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  const int32_t micro_size = 100;

  ObSSMicroBlockMeta *micro_meta_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(micro_meta_ptr));
  ASSERT_NE(nullptr, micro_meta_ptr);
  ObSSMicroBlockMetaHandle micro_meta_handle;
  micro_meta_handle.set_ptr(micro_meta_ptr);
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta_ptr->ref_cnt_);

  ObSSMicroBlockMetaHandle tmp_micro_handle1(micro_meta_handle);
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta_ptr->ref_cnt_);

  ObSSMicroBlockMetaHandle tmp_micro_handle2 = micro_meta_handle;
  ASSERT_EQ(ori_micro_ref_cnt + 3, micro_meta_ptr->ref_cnt_);
  tmp_micro_handle2 = micro_meta_handle;
  ASSERT_EQ(ori_micro_ref_cnt + 3, micro_meta_ptr->ref_cnt_);
  tmp_micro_handle2.reset();
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta_ptr->ref_cnt_);
  tmp_micro_handle2 = micro_meta_handle;
  ASSERT_EQ(ori_micro_ref_cnt + 3, micro_meta_ptr->ref_cnt_);
  tmp_micro_handle2.reset();
  tmp_micro_handle1.reset();
}

TEST_F(TestSSMicroCacheStruct, micro_handle_hash_map)
{
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  const int32_t micro_size = 100;

  ObSSMicroBlockMeta *micro_meta_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(micro_meta_ptr));
  ASSERT_NE(nullptr, micro_meta_ptr);
  ObSSMicroBlockMetaHandle micro_meta_handle;
  micro_meta_handle.set_ptr(micro_meta_ptr);
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta_ptr->ref_cnt_);

  hash::ObHashMap<int64_t, ObSSMicroBlockMetaHandle> micro_handle_map;
  ASSERT_EQ(OB_SUCCESS, micro_handle_map.create(128, ObMemAttr(1, "TestMap")));

  ASSERT_EQ(OB_SUCCESS, micro_handle_map.set_refactored(1, micro_meta_handle));
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta_ptr->ref_cnt_);

  ObSSMicroBlockMetaHandle tmp_micro_handle;
  ASSERT_EQ(OB_SUCCESS, micro_handle_map.get_refactored(1, tmp_micro_handle));
  ASSERT_EQ(ori_micro_ref_cnt + 3, micro_meta_ptr->ref_cnt_);

  ASSERT_EQ(OB_SUCCESS, micro_handle_map.erase_refactored(1));
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta_ptr->ref_cnt_);

  ASSERT_EQ(1, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
  micro_meta_handle.reset();
  tmp_micro_handle.reset();
  ASSERT_EQ(0, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
}

TEST_F(TestSSMicroCacheStruct, mem_block)
{
  const uint32_t ori_mem_ref_cnt = 1;
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  ObSSMemBlock *mem_blk = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_data_mgr.do_alloc_mem_block(mem_blk, true/*is_fg*/));
  ASSERT_NE(nullptr, mem_blk);
  ASSERT_EQ(true, mem_blk->is_valid());
  ASSERT_EQ(ori_mem_ref_cnt, mem_blk->ref_cnt_);
  ASSERT_EQ(true, mem_blk->is_fg_mem_blk());

  ObSSMemBlockHandle mem_blk_handle;
  mem_blk_handle.set_ptr(mem_blk);
  ASSERT_EQ(true, mem_blk_handle.is_valid());
  ASSERT_EQ(ori_mem_ref_cnt + 1, mem_blk->ref_cnt_);

  mem_blk_handle.reset();
  ASSERT_EQ(ori_mem_ref_cnt, mem_blk->ref_cnt_);
  bool succ_free = false;
  mem_blk->try_free(succ_free);
  ASSERT_EQ(true, succ_free);

  ObSSMemBlock *bg_mem_blk = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_data_mgr.do_alloc_mem_block(bg_mem_blk, false/*is_fg*/));
  ASSERT_NE(nullptr, bg_mem_blk);
  ASSERT_EQ(false, bg_mem_blk->is_fg_mem_blk());
  ASSERT_EQ(ori_mem_ref_cnt, bg_mem_blk->ref_cnt_);
  succ_free = false;
  bg_mem_blk->try_free(succ_free);
  ASSERT_EQ(true, succ_free);
}

TEST_F(TestSSMicroCacheStruct, mem_block1)
{
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  MacroBlockId macro_id(0, 100, 0);
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);

  ObArenaAllocator allocator;
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache *)->cache_stat_;
  ObSSMemBlockPool &mem_blk_pool = MTL(ObSSMicroCache *)->mem_data_mgr_.mem_block_pool_;
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  const int64_t def_count = mem_blk_pool.def_count_;
  const int64_t max_count = mem_blk_pool.def_count_ + mem_blk_pool.max_extra_count_;
  ASSERT_EQ(cache_stat.mem_blk_stat().mem_blk_fg_max_cnt_ + cache_stat.mem_blk_stat().mem_blk_bg_max_cnt_, max_count);
  ASSERT_EQ(cache_stat.mem_blk_stat().mem_blk_def_cnt_, def_count);
  ASSERT_EQ(mem_blk_pool.used_extra_count_, 0);
  ASSERT_EQ(cache_stat.mem_blk_stat().mem_blk_fg_used_cnt_, 0);
  ObSSMemBlock *mem_block_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_block_ptr, true/*is_fg*/));
  ASSERT_EQ(mem_blk_pool.used_extra_count_, 0);
  ASSERT_EQ(cache_stat.mem_blk_stat().mem_blk_fg_used_cnt_, 1);
  ASSERT_NE(nullptr, mem_block_ptr);
  ObSSMemBlock &mem_block = *mem_block_ptr;
  ASSERT_EQ(true, mem_block.is_valid());
  ASSERT_EQ(0, mem_block.data_size_);
  ASSERT_EQ(0, mem_block.index_size_);

  // mem_block can't hold block_size data, that means the last micro_block can't be put into it.
  const int32_t block_size = micro_meta_mgr.block_size_;
  int64_t micro_block_size = 512 * 1024;
  int64_t micro_block_cnt = block_size / micro_block_size;
  char *large_micro_data = static_cast<char *>(allocator.alloc(micro_block_size));
  char *tmp_large_buf = static_cast<char *>(allocator.alloc(micro_block_size));

  for (int64_t i = 0; i < micro_block_cnt; ++i) {
    MEMSET(large_micro_data, (char)('a' + i % 20), micro_block_size);
    ObSSMicroBlockCacheKey micro_key;
    micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
    micro_key.micro_id_.macro_id_ = macro_id;
    micro_key.micro_id_.offset_ = 1 + i * micro_block_size;
    micro_key.micro_id_.size_ = micro_block_size;
    int32_t write_offset = 0;
    int32_t index_offset = 0;
    if (i == micro_block_cnt - 1) {
      // cuz can't have enough space to hold this micro_block data
      ASSERT_EQ(OB_SIZE_OVERFLOW, mem_block.calc_write_location(micro_key, micro_block_size, write_offset, index_offset));
    } else {
      ASSERT_EQ(OB_SUCCESS, mem_block.calc_write_location(micro_key, micro_block_size, write_offset, index_offset));
      uint32_t crc = 0;
      ASSERT_EQ(OB_SUCCESS, mem_block.write_micro_data(micro_key, large_micro_data, micro_block_size, write_offset, index_offset, crc));

      // check written data
      MEMSET(tmp_large_buf, '\0', micro_block_size);
      if (i == 0) {
        // mock checksum_error
        crc += 1;
        ASSERT_EQ(OB_CHECKSUM_ERROR, mem_block.get_micro_data(micro_key, tmp_large_buf, micro_block_size, crc));
      } else {
        ASSERT_EQ(OB_SUCCESS, mem_block.get_micro_data(micro_key, tmp_large_buf, micro_block_size, crc)) << i;
        ASSERT_EQ(0, STRNCMP(large_micro_data, tmp_large_buf, micro_block_size)) << i;
      }
    }
  }
  ASSERT_EQ(micro_block_cnt - 1, mem_block.micro_count_);

  ASSERT_LT(0, mem_block.data_size_);
  ASSERT_LT(0, mem_block.index_size_);
  const int32_t exp_index_offset = mem_block.reserved_size_ + mem_block.data_size_;
  ASSERT_NE(0, MEMCMP(mem_block.data_buf_ + exp_index_offset, mem_block.index_buf_, mem_block.index_size_));
  ASSERT_EQ(OB_SUCCESS, mem_block.handle_when_sealed());
  ASSERT_EQ(0, MEMCMP(mem_block.data_buf_ + exp_index_offset, mem_block.index_buf_, mem_block.index_size_));
  ASSERT_EQ(OB_SUCCESS, mem_data_mgr.fg_sealed_mem_blocks_.push(mem_block_ptr));

  ObSSMemBlock *mem_block_ptr1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_block_ptr1, true/*is_fg*/));
  ObSSMemBlock &mem_block1 = *mem_block_ptr1;
  micro_block_size = 512;
  micro_block_cnt = block_size / micro_block_size;
  char *small_micro_data = static_cast<char *>(allocator.alloc(micro_block_size));

  for (int64_t i = 0; i < 3000; ++i) {
    int32_t write_offset = 0;
    int32_t index_offset = 0;
    MEMSET(small_micro_data, (char)('a' + i % 20), micro_block_size);
    ObSSMicroBlockCacheKey micro_key;
    micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
    micro_key.micro_id_.macro_id_ = macro_id;
    micro_key.micro_id_.offset_ = 1 + i * micro_block_size;
    micro_key.micro_id_.size_ = micro_block_size;
    ASSERT_EQ(OB_SUCCESS, mem_block1.calc_write_location(micro_key, micro_block_size, write_offset, index_offset));
    uint32_t crc = 0;
    ASSERT_EQ(OB_SUCCESS, mem_block1.write_micro_data(micro_key, small_micro_data, micro_block_size, write_offset, index_offset, crc));
  }
  ASSERT_EQ(OB_SUCCESS, mem_data_mgr.fg_sealed_mem_blocks_.push(mem_block_ptr1));
}

TEST_F(TestSSMicroCacheStruct, mem_block_pool)
{
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  ObSSMemBlockPool &mem_blk_pool = mem_data_mgr.mem_block_pool_;
  const int32_t mem_blk_buf_size = mem_blk_pool.mem_blk_data_buf_size_ + mem_blk_pool.mem_blk_index_buf_size_;
  mem_blk_pool.DEF_WAIT_TIMEOUT_MS = 1000; // 1s
  const int64_t def_count = mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_def_cnt_;
  const int64_t max_count = mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_fg_max_cnt_ +
                            mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_bg_max_cnt_;
  const int64_t max_bg_count = mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_bg_max_cnt_;
  ASSERT_LT(0, max_bg_count);
  const int64_t max_fg_count = mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_fg_max_cnt_;
  ASSERT_LT(0, max_fg_count);
  ObArray<ObSSMemBlock *> mem_blk_arr;
  for (int64_t i = 0; i < max_fg_count; ++i) {
    ObSSMemBlock *mem_blk = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_data_mgr.do_alloc_mem_block(mem_blk, true/*is_fg*/));
    ASSERT_NE(nullptr, mem_blk);
    ASSERT_EQ(true, mem_blk->is_valid());
    ASSERT_EQ(1, mem_blk->ref_cnt_);
    ASSERT_EQ(true, mem_blk->is_fg_mem_blk());
    ASSERT_EQ(OB_SUCCESS, mem_blk_arr.push_back(mem_blk));
  }
  ASSERT_EQ(max_fg_count, mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_fg_used_cnt_);

  {
    // can't alloc a free fg_mem_blk
    ObSSMemBlock *mem_blk = nullptr;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, mem_data_mgr.do_alloc_mem_block(mem_blk, true/*is_fg*/));
    ASSERT_EQ(max_fg_count, mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_fg_used_cnt_);
  }

  for (int64_t i = 0; i < max_bg_count; ++i) {
    ObSSMemBlock *mem_blk = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_data_mgr.do_alloc_mem_block(mem_blk, false/*is_fg*/));
    ASSERT_NE(nullptr, mem_blk);
    ASSERT_EQ(true, mem_blk->is_valid());
    ASSERT_EQ(1, mem_blk->ref_cnt_);
    ASSERT_EQ(false, mem_blk->is_fg_mem_blk());
    ASSERT_EQ(OB_SUCCESS, mem_blk_arr.push_back(mem_blk));
  }
  ASSERT_EQ(max_bg_count, mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_bg_used_cnt_);
  ASSERT_EQ(max_count, mem_blk_pool.def_count_ + mem_blk_pool.used_extra_count_);
  ASSERT_EQ((mem_blk_buf_size + sizeof(ObSSMemBlock)) * max_count, mem_data_mgr.cache_stat_.mem_blk_stat().total_mem_blk_size_);

  {
    // can't alloc a free bg_mem_blk
    ObSSMemBlock *mem_blk = nullptr;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, mem_data_mgr.do_alloc_mem_block(mem_blk, false/*is_fg*/));
    ASSERT_EQ(max_bg_count, mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_bg_used_cnt_);
  }

  {
    // free a fg_mem_blk, we can only alloc a fg_mem_blk
    bool succ_free = false;
    ObSSMemBlock *fg_mem_blk = mem_blk_arr.at(0);
    ASSERT_EQ(OB_SUCCESS, fg_mem_blk->try_free(succ_free));
    ASSERT_EQ(true, succ_free);
    ASSERT_EQ(max_fg_count - 1, mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_fg_used_cnt_);
    ASSERT_EQ(1, fg_mem_blk->ref_cnt_);
    ObSSMemBlock *bg_mem_blk = nullptr;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, mem_data_mgr.do_alloc_mem_block(bg_mem_blk, false/*is_fg*/));
    fg_mem_blk = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_data_mgr.do_alloc_mem_block(fg_mem_blk, true/*is_fg*/));
    ASSERT_NE(nullptr, fg_mem_blk);
    ASSERT_EQ(max_fg_count, mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_fg_used_cnt_);

    // free a bg_mem_blk, we can only alloc a bg_mem_blk
    bg_mem_blk = nullptr;
    mem_blk_arr.pop_back(bg_mem_blk);
    ASSERT_NE(nullptr, bg_mem_blk);
    succ_free = false;
    ASSERT_EQ(OB_SUCCESS, bg_mem_blk->try_free(succ_free));
    ASSERT_EQ(true, succ_free);
    ASSERT_EQ(max_bg_count - 1, mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_bg_used_cnt_);
    fg_mem_blk = nullptr;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, mem_data_mgr.do_alloc_mem_block(fg_mem_blk, true/*is_fg*/));
    bg_mem_blk = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_data_mgr.do_alloc_mem_block(bg_mem_blk, false/*is_fg*/));
    ASSERT_NE(nullptr, bg_mem_blk);
    ASSERT_EQ(OB_SUCCESS, mem_blk_arr.push_back(bg_mem_blk));
    ASSERT_EQ(max_bg_count, mem_data_mgr.cache_stat_.mem_blk_stat().mem_blk_bg_used_cnt_);
  }

  for (int64_t i = 0; i < mem_blk_arr.count(); ++i) {
    ObSSMemBlock *mem_blk = mem_blk_arr.at(i);
    if (i < def_count) {
      ASSERT_EQ(false, mem_data_mgr.mem_block_pool_.is_alloc_dynamiclly(mem_blk));
    } else {
      ASSERT_EQ(true, mem_data_mgr.mem_block_pool_.is_alloc_dynamiclly(mem_blk));
    }
  }

  {
    ObSSMemBlockHandle fixed_mem_blk_handle;
    fixed_mem_blk_handle.set_ptr(mem_blk_arr.at(0));
    ObSSMemBlockHandle dynamic_mem_blk_handle;
    dynamic_mem_blk_handle.set_ptr(mem_blk_arr.at(def_count));

    for (int64_t i = 0; i < mem_blk_arr.count(); ++i) {
      bool succ_free = false;
      ASSERT_EQ(OB_SUCCESS, mem_blk_arr.at(i)->try_free(succ_free));
      if (i == 0 || i == def_count) {
        ASSERT_EQ(false, succ_free);
      } else {
        ASSERT_EQ(true, succ_free);
      }
    }
    ASSERT_EQ(1, mem_data_mgr.mem_block_pool_.used_extra_count_);
    ASSERT_EQ((mem_blk_buf_size + sizeof(ObSSMemBlock)) * (def_count + 1), mem_data_mgr.cache_stat_.mem_blk_stat().total_mem_blk_size_);
  }
  ASSERT_EQ(0, mem_data_mgr.mem_block_pool_.used_extra_count_);
  ASSERT_EQ((mem_blk_buf_size + sizeof(ObSSMemBlock)) * def_count, mem_data_mgr.cache_stat_.mem_blk_stat().total_mem_blk_size_);
}

TEST_F(TestSSMicroCacheStruct, mem_block_pool1)
{
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  ObSSMemBlockPool &mem_block_pool = mem_data_mgr.mem_block_pool_;
  mem_block_pool.DEF_WAIT_TIMEOUT_MS = 1000 * 1000; // 1s
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache *)->cache_stat_;
  const int64_t def_count = mem_block_pool.def_count_;
  const int64_t max_count = mem_block_pool.def_count_ + mem_block_pool.max_extra_count_;
  const int64_t max_bg_count = mem_data_mgr.mem_block_pool_.max_bg_count_;
  const int64_t max_fg_count = max_count - max_bg_count;
  ObArray<ObSSMemBlock *> mem_blocks;

  for (int64_t i = 0; i < def_count; ++i) {
    ObSSMemBlock *mem_blk = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_block_pool.alloc(mem_blk, true/*is_fg*/));
    ASSERT_NE(nullptr, mem_blk);
    ASSERT_EQ(true, mem_blk->is_valid());
    ASSERT_EQ(true, mem_blk->is_fg_mem_blk());
    ASSERT_EQ(true, mem_block_pool.is_pre_created_mem_blk(mem_blk));
    ASSERT_EQ(OB_SUCCESS, mem_blocks.push_back(mem_blk));
  }

  // alloc extra mem_block, these mem_blocks' is_temp will be true
  ObArray<ObSSMemBlock *> tmp_mem_blk_arr;
  for (int64_t i = def_count; i < max_fg_count; ++i) {
    ObSSMemBlock *mem_blk = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_block_pool.alloc(mem_blk, true/*is_fg*/));
    ASSERT_NE(nullptr, mem_blk);
    ASSERT_EQ(true, mem_blk->is_valid());
    ASSERT_EQ(true, mem_blk->is_fg_mem_blk());
    ASSERT_EQ(false, mem_block_pool.is_pre_created_mem_blk(mem_blk));
    ASSERT_EQ(OB_SUCCESS, tmp_mem_blk_arr.push_back(mem_blk));
  }
  ASSERT_EQ(max_fg_count - def_count, mem_block_pool.used_extra_count_);

  for (int64_t i = max_fg_count; i < max_count; ++i) {
    ObSSMemBlock *temp_mem_blk = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_block_pool.alloc(temp_mem_blk, false/*is_fg*/));
    ASSERT_NE(nullptr, temp_mem_blk);
    ASSERT_EQ(true, temp_mem_blk->is_valid());
    ASSERT_EQ(false, temp_mem_blk->is_fg_mem_blk());
    ASSERT_EQ(false, mem_block_pool.is_pre_created_mem_blk(temp_mem_blk));
    ASSERT_EQ(OB_SUCCESS, tmp_mem_blk_arr.push_back(temp_mem_blk));
  }

  // alloc another fg_mem_block, failed cuz all mem_blk were allocated.
  ObSSMemBlock *temp_mem_blk2 = nullptr;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mem_block_pool.alloc(temp_mem_blk2, true/*is_fg*/));
  ASSERT_EQ(nullptr, temp_mem_blk2);

  // free one bg_mem_blk.
  ObSSMemBlock *cur_temp_mem_blk = nullptr;
  tmp_mem_blk_arr.pop_back(cur_temp_mem_blk);
  ASSERT_NE(nullptr, cur_temp_mem_blk);
  ASSERT_EQ(false, cur_temp_mem_blk->is_fg_mem_blk());
  bool succ_free = false;
  ASSERT_EQ(OB_SUCCESS, cur_temp_mem_blk->try_free(succ_free));
  ASSERT_EQ(true, succ_free);

  // after free bg_mem_block, can alloc bg_mem_block now
  ASSERT_EQ(OB_SUCCESS, mem_block_pool.alloc(temp_mem_blk2, false/*is_fg*/));
  ASSERT_NE(nullptr, temp_mem_blk2);
  ASSERT_EQ(false, temp_mem_blk2->is_fg_mem_blk());
  ASSERT_EQ(true, mem_block_pool.is_alloc_dynamiclly(temp_mem_blk2));

  succ_free = false;
  ASSERT_EQ(OB_SUCCESS, temp_mem_blk2->try_free(succ_free));
  ASSERT_EQ(true, succ_free);

  ASSERT_EQ(max_count - def_count - 1, mem_block_pool.used_extra_count_);

  ObSSMemBlockHandle mem_blk_handle;
  mem_blk_handle.set_ptr(tmp_mem_blk_arr.at(0));
  for (int64_t i = 0; i < tmp_mem_blk_arr.count(); ++i) {
    succ_free = false;
    ASSERT_EQ(OB_SUCCESS, tmp_mem_blk_arr.at(i)->try_free(succ_free));
    if (i == 0) {
      ASSERT_EQ(false, succ_free);
    } else {
      ASSERT_EQ(true, succ_free);
    }
  }
  ASSERT_EQ(1, mem_block_pool.used_extra_count_);

  mem_blk_handle.reset();
  ASSERT_EQ(0, mem_block_pool.used_extra_count_);

  mem_blk_handle.set_ptr(mem_blocks.at(0));
  for (int64_t i = 0; i < def_count; ++i) {
    ObSSMemBlock *mem_blk = mem_blocks.at(i);
    ASSERT_NE(nullptr, mem_blk);
    succ_free = false;
    ASSERT_EQ(OB_SUCCESS, mem_blk->try_free(succ_free));
    if (i == 0) {
      ASSERT_EQ(false, succ_free);
    } else {
      ASSERT_EQ(true, succ_free);
    }
  }
  ASSERT_EQ(0, mem_block_pool.used_extra_count_);

  mem_blk_handle.reset();
  ASSERT_EQ(0, mem_block_pool.used_extra_count_);
}

TEST_F(TestSSMicroCacheStruct, micro_meta_pool)
{
  const int64_t fixed_cnt = 20;
  const int64_t extra_cnt = 15;
  const int64_t total_cnt = fixed_cnt + extra_cnt;

  ObArray<ObSSMicroBlockMetaHandle> micro_handle_arr;
  for (int64_t i = 0; i < total_cnt; ++i) {
    ObSSMicroBlockMeta *micro_meta = nullptr;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(micro_meta));
    ASSERT_NE(nullptr, micro_meta);
    ObSSMicroBlockMetaHandle micro_handle;
    micro_handle.set_ptr(micro_meta);
    ASSERT_EQ(OB_SUCCESS, micro_handle_arr.push_back(micro_handle));
    ASSERT_EQ(i + 1, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
  }

  for (int64_t i = fixed_cnt; i < total_cnt; ++i) {
    micro_handle_arr.at(i).get_ptr()->try_free();
    ASSERT_EQ(total_cnt - (i - fixed_cnt + 1), SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
    micro_handle_arr.at(i).reset();
  }
  for (int64_t i = 0; i < fixed_cnt; ++i) {
    micro_handle_arr.at(i).get_ptr()->try_free();
    micro_handle_arr.at(i).reset();
  }
  ASSERT_EQ(0, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
}

TEST_F(TestSSMicroCacheStruct, arc_iter_info)
{
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  const int32_t micro_size = 50;
  ObSSARCIterInfo arc_iter_info;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  arc_iter_info.init(tenant_id);
  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    ASSERT_EQ(false, arc_iter_info.need_handle_arc_seg(i));
    ASSERT_EQ(false, arc_iter_info.need_iterate_cold_micro(i));
    ASSERT_EQ(false, arc_iter_info.need_handle_cold_micro(i));
  }

  arc_iter_info.iter_seg_arr_[ARC_T1].seg_cnt_ = 500;
  arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.to_delete_ = false;
  arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_ = 100;
  arc_iter_info.iter_seg_arr_[ARC_B1].seg_cnt_ = 500;
  arc_iter_info.iter_seg_arr_[ARC_B1].op_info_.to_delete_ = true;
  arc_iter_info.iter_seg_arr_[ARC_B1].op_info_.op_cnt_ = 500;
  arc_iter_info.iter_seg_arr_[ARC_T2].seg_cnt_ = 500;
  arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.to_delete_ = false;
  arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_ = 200;
  arc_iter_info.iter_seg_arr_[ARC_B2].seg_cnt_ = 500;
  arc_iter_info.iter_seg_arr_[ARC_B2].op_info_.to_delete_ = true;
  arc_iter_info.iter_seg_arr_[ARC_B2].op_info_.op_cnt_ = 600;
  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(i));
    arc_iter_info.adjust_arc_iter_seg_info(i);
    ASSERT_GE(SS_MAX_ARC_HANDLE_OP_CNT, arc_iter_info.iter_seg_arr_[i].op_info_.op_cnt_);
    ASSERT_LT(0, arc_iter_info.iter_seg_arr_[i].op_info_.op_cnt_);
    ASSERT_GE(SS_MAX_ARC_HANDLE_OP_CNT, arc_iter_info.iter_seg_arr_[i].op_info_.exp_iter_cnt_);
    ASSERT_LT(0, arc_iter_info.iter_seg_arr_[i].op_info_.exp_iter_cnt_);
  }

  ObSSMicroBlockMeta *micro_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(micro_meta));
  ASSERT_NE(nullptr, micro_meta);
  const int64_t seg_cnt = 500;
  const int64_t total_seg_cnt = seg_cnt * SS_ARC_SEG_COUNT;
  for (int64_t i = 1; i <= total_seg_cnt; ++i) {
    blocksstable::MacroBlockId macro_id(0, 8000 + i, 0);
    int64_t offset = i;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);

    micro_meta->first_val_ = 100;
    micro_meta->length_ = micro_size;
    micro_meta->micro_key_ = micro_key;
    micro_meta->access_time_ = i;
    int64_t seg_idx = -1;

    if (i % 4 == 0) {
      micro_meta->is_in_l1_ = false;
      micro_meta->is_in_ghost_ = false;
      seg_idx = ARC_T2;
    } else if (i % 4 == 1) {
      micro_meta->is_in_l1_ = true;
      micro_meta->is_in_ghost_ = false;
      seg_idx = ARC_T1;
    } else if (i % 4 == 2) {
      micro_meta->is_in_l1_ = true;
      micro_meta->is_in_ghost_ = true;
      seg_idx = ARC_B1;
    } else {
      micro_meta->is_in_l1_ = false;
      micro_meta->is_in_ghost_ = true;
      seg_idx = ARC_B2;
    }

    if (arc_iter_info.need_iterate_cold_micro(seg_idx)) {
      ASSERT_EQ(OB_SUCCESS, arc_iter_info.push_cold_micro(micro_key, *micro_meta, seg_idx));
      ObSSMicroBlockMetaHandle cold_micro_handle;
      ASSERT_EQ(OB_SUCCESS, arc_iter_info.get_cold_micro(micro_key, cold_micro_handle));
      ASSERT_EQ(true, cold_micro_handle.is_valid());
      ASSERT_EQ(ori_micro_ref_cnt + 2, cold_micro_handle.get_ptr()->ref_cnt_);
      ASSERT_EQ(micro_meta->first_val_, cold_micro_handle.get_ptr()->first_val_);
      ASSERT_EQ(micro_meta->second_val_, cold_micro_handle.get_ptr()->second_val_);
      ASSERT_EQ(micro_meta->micro_key(), cold_micro_handle.get_ptr()->micro_key());
      ASSERT_EQ(micro_meta->crc(), cold_micro_handle.get_ptr()->crc());
    }
  }
  micro_meta->free();

  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.obtained_cnt_);
  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.obtained_cnt_);
  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT, arc_iter_info.iter_seg_arr_[ARC_B1].op_info_.obtained_cnt_);
  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT, arc_iter_info.iter_seg_arr_[ARC_B2].op_info_.obtained_cnt_);
  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    ASSERT_EQ(false, arc_iter_info.need_iterate_cold_micro(i));
    ASSERT_EQ(true, arc_iter_info.need_handle_cold_micro(i));
  }

  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    while (arc_iter_info.need_handle_cold_micro(i)) {
      ObSSMicroBlockMetaHandle cold_micro_handle;
      ASSERT_EQ(OB_SUCCESS, arc_iter_info.pop_cold_micro(i, cold_micro_handle));
      ASSERT_EQ(true, cold_micro_handle.is_valid());
      ASSERT_EQ(1, cold_micro_handle.get_ptr()->ref_cnt_);
      ASSERT_EQ(OB_SUCCESS, arc_iter_info.finish_handle_cold_micro(i));
    }
  }

  arc_iter_info.destroy();
  ASSERT_EQ(0, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
}

TEST_F(TestSSMicroCacheStruct, reorgan_entry)
{
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  const int32_t micro_size = 51;
  ObArenaAllocator allocator;
  char *buf = static_cast<char *>(allocator.alloc(100));
  ASSERT_NE(nullptr, buf);

  int64_t micro_cnt = 2000;
  ObArray<ObSSMicroBlockMeta *> micro_meta_arr;
  ObArray<ObSSReorganizeMicroOp::SSReorganMicroEntry> entry_arr;
  for (int64_t i = 0; i < micro_cnt; ++i) {
    ObSSMicroBlockMeta *micro_meta = nullptr;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(micro_meta));
    ASSERT_NE(nullptr, micro_meta);
    micro_meta->reset();
    ASSERT_EQ(ori_micro_ref_cnt, micro_meta->ref_cnt_);
    ASSERT_EQ(OB_SUCCESS, micro_meta_arr.push_back(micro_meta));

    ObSSReorganizeMicroOp::SSReorganMicroEntry entry;
    entry.idx_ = i + 1;
    entry.data_ptr_ = buf;
    entry.micro_meta_handle_.set_ptr(micro_meta);
    ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta->ref_cnt_);
    micro_meta->first_val_ = 101;
    micro_meta->length_ = 51;
    micro_meta->access_time_ = 3;
    MacroBlockId macro_id(0, 100 + i, 0);
    int32_t offset = 40;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    micro_meta->micro_key_ = micro_key;

    ASSERT_EQ(true, entry.is_valid());
    ASSERT_EQ(OB_SUCCESS, entry_arr.push_back(entry));
    ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta->ref_cnt_);
  }

  for (int64_t i = 0; i < micro_cnt; ++i) {
    ASSERT_EQ(true, entry_arr.at(i).is_valid());
    ASSERT_EQ(ori_micro_ref_cnt + 1, entry_arr.at(i).micro_meta_handle_.get_ptr()->ref_cnt_);
  }
  entry_arr.reset();
  ASSERT_EQ(0, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_struct.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_struct.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}