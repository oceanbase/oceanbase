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

class TestSSMicroCacheAbnormalCase : public ::testing::Test
{
public:
  TestSSMicroCacheAbnormalCase() = default;
  virtual ~TestSSMicroCacheAbnormalCase() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSMicroCacheAbnormalCase::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheAbnormalCase::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheAbnormalCase::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), 1717986918));
  micro_cache->start();
}

void TestSSMicroCacheAbnormalCase::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

/* Test some micro blocks fail to update meta when sealed_mem_block updates micro_block meta.*/
TEST_F(TestSSMicroCacheAbnormalCase, test_mem_blk_update_meta_fail)
{
  ObArenaAllocator allocator;
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache*)->cache_stat_;
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache*)->mem_data_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache*)->micro_meta_mgr_;
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache*)->phy_blk_mgr_;
  ObSSPersistMicroDataTask &persist_task = MTL(ObSSMicroCache*)->task_runner_.persist_task_;

  ObArray<ObSSMemBlock *> mem_blk_arr;
  const int64_t def_count = cache_stat.mem_blk_stat().mem_blk_def_cnt_;
  for (int64_t i = 0; i < def_count; ++i) {
    ObSSMemBlock *mem_blk = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_data_mgr.do_alloc_mem_block(mem_blk, true/*is_fg*/));
    ASSERT_NE(nullptr, mem_blk);
    ASSERT_EQ(OB_SUCCESS, mem_blk_arr.push_back(mem_blk));
  }

  const int64_t blk_idx = 2;
  ObSSPhysicalBlockHandle phy_blk_handle;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_block_handle(blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());

  const int32_t block_size = micro_meta_mgr.block_size_;
  const int64_t micro_block_cnt = 50;
  const int32_t micro_size = 512;
  const char c = 'a';
  ObSSMemBlockHandle mem_blk_handle;
  uint32_t crc = 0;
  ObSSMicroBlockCacheKey micro_key;
  MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100);
  int64_t offset = 1;
  for (int64_t j = 0; j < micro_block_cnt; ++j) {
    mem_blk_handle.reset();
    micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    char *buf = static_cast<char *>(allocator.alloc(micro_size));
    ASSERT_NE(nullptr, buf);
    MEMSET(buf, c, micro_size);
    ASSERT_EQ(OB_SUCCESS, mem_data_mgr.add_micro_block_data(micro_key, buf, micro_size, mem_blk_handle, crc));
    ASSERT_EQ(true, mem_blk_handle.is_valid());
    const uint32_t real_crc = static_cast<int32_t>(ob_crc64(buf, micro_size));
    ASSERT_EQ(crc, real_crc);
    bool real_add = false;
    ASSERT_EQ(OB_SUCCESS,
        micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, crc, mem_blk_handle, real_add));
    ASSERT_EQ(true, real_add);

    ObSSMicroBlockMetaHandle micro_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_handle, false));
    if (j < micro_block_cnt / 2) {
      // mock some micro_block's reuse_version mismatch, these micro_block will fail to update meta.
      micro_handle.get_ptr()->reuse_version_ = 1000;
    }
    offset += micro_size;
  }
  ASSERT_EQ(micro_block_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(true, mem_data_mgr.mem_block_pool_.is_alloc_dynamiclly(mem_blk_handle.get_ptr()));

  ObSSMemBlockHandle tmp_mem_handle;
  tmp_mem_handle.set_ptr(mem_blk_handle.get_ptr());
  ASSERT_EQ(3, tmp_mem_handle.get_ptr()->ref_cnt_);
  ASSERT_EQ(true, tmp_mem_handle.get_ptr()->is_completed());

  const int64_t block_offset = block_size * blk_idx;
  int64_t updated_micro_size = 0;
  int64_t updated_micro_cnt = 0;
  /* As expected, we do not allow micro blocks to fail to update meta, so OB_ERR_UNEXPECTED will be returned here. */
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      micro_meta_mgr.update_micro_block_meta(
          mem_blk_handle, block_offset, mem_blk_handle()->reuse_version_, updated_micro_size, updated_micro_cnt));
  ASSERT_EQ((micro_block_cnt / 2) * micro_size, updated_micro_size);
  ASSERT_EQ(micro_block_cnt / 2, updated_micro_cnt);
  ASSERT_EQ(1, mem_data_mgr.mem_block_pool_.used_extra_count_);

  // free sealed_mem_block
  ASSERT_EQ(OB_SUCCESS, persist_task.persist_op_.handle_sealed_mem_block(true, updated_micro_cnt, mem_blk_handle));
  ASSERT_EQ(micro_block_cnt / 2, cache_stat.micro_stat().total_micro_cnt_); // micro_block which fail to update meta are deleted from map.
  ASSERT_EQ(1, tmp_mem_handle.get_ptr()->ref_cnt_);
  tmp_mem_handle.reset();
  ASSERT_EQ(0, mem_data_mgr.mem_block_pool_.used_extra_count_);
}

TEST_F(TestSSMicroCacheAbnormalCase, test_alloc_phy_block_abnormal)
{
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache*)->phy_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache*)->cache_stat_;
  int64_t block_idx = 2;
  ObSSPhysicalBlock *phy_blk = phy_blk_mgr.get_phy_block_by_idx_nolock(block_idx);
  ASSERT_NE(nullptr, phy_blk);

  // mock this free phy_block is abnormal, make its valid_len > 0
  phy_blk->valid_len_ = 100;
  int64_t tmp_block_idx = -1;
  ObSSPhysicalBlockHandle phy_blk_handle;
  ASSERT_EQ(OB_INVALID_ARGUMENT, phy_blk_mgr.alloc_block(tmp_block_idx, phy_blk_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
  ASSERT_EQ(block_idx, tmp_block_idx);
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_reusable_block(tmp_block_idx));
  ASSERT_EQ(1, phy_blk_mgr.get_reusable_set_count());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_reusable_block(tmp_block_idx));
  ASSERT_EQ(1, phy_blk_mgr.get_reusable_set_count());
  phy_blk_handle.reset();

  tmp_block_idx = -1;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(tmp_block_idx, phy_blk_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
  ASSERT_EQ(3, tmp_block_idx);
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  phy_blk_handle.reset();

  phy_blk->valid_len_ = 0;
  phy_blk->block_type_ = static_cast<uint64_t>(ObSSPhyBlockType::SS_CACHE_DATA_BLK);
  bool succ_free = false;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(block_idx, succ_free));
  ASSERT_EQ(true, succ_free);
  ASSERT_EQ(0, phy_blk_mgr.get_reusable_set_count());

  tmp_block_idx = -1;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(tmp_block_idx, phy_blk_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
  ASSERT_EQ(2, tmp_block_idx);
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  phy_blk_handle.reset();
}

TEST_F(TestSSMicroCacheAbnormalCase, test_micro_cache_abnormal_health)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache*)->cache_stat_;
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache*)->mem_data_mgr_;
  ObSSExecuteMicroCheckpointTask &micro_ckpt_task = MTL(ObSSMicroCache*)->task_runner_.micro_ckpt_task_;

  // 1. mock used up all mem_block
  mem_data_mgr.mem_block_pool_.used_fg_count_ = mem_data_mgr.mem_block_pool_.get_fg_max_count();

  // 2. write some micro_block into micro_cache
  const int64_t micro_block_cnt = 50;
  const int32_t micro_size = 512;
  const char c = 'a';
  char *buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, buf);
  MEMSET(buf, c, micro_size);

  uint32_t crc = 0;
  ObSSMicroBlockCacheKey micro_key;
  MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100);
  int64_t offset = 1;
  for (int64_t i = 0; i < 5; ++i) {
    micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = ObSSExecuteMicroCheckpointOp::CHECK_CACHE_ABNORMAL_ROUND - 10;
    for (int64_t j = 0; j < micro_block_cnt; ++j) {
      micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ret = micro_cache->add_micro_block_cache(micro_key, buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
      ASSERT_NE(OB_SUCCESS, ret);
      offset += micro_size;
    }
    ob_usleep(2 * 1000 * 1000);
  }

  // check abnormal count
  ASSERT_LE(3, micro_ckpt_task.ckpt_op_.cache_abnormal_cnt_);

  mem_data_mgr.mem_block_pool_.used_fg_count_ = 0;
  {
    micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = ObSSExecuteMicroCheckpointOp::CHECK_CACHE_ABNORMAL_ROUND - 10;
    for (int64_t j = 0; j < micro_block_cnt; ++j) {
      micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ret = micro_cache->add_micro_block_cache(micro_key, buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
      ASSERT_EQ(OB_SUCCESS, ret);
      offset += micro_size;
    }
    ob_usleep(2 * 1000 * 1000);
  }
  ASSERT_EQ(0, micro_ckpt_task.ckpt_op_.cache_abnormal_cnt_);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_abnormal_case.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_abnormal_case.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}