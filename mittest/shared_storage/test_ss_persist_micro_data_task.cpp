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
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "test_ss_common_util.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
class TestSSPersistMicroDataTask : public ::testing::Test
{
public:
  TestSSPersistMicroDataTask() {}
  virtual ~TestSSPersistMicroDataTask() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSPersistMicroDataTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSPersistMicroDataTask::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSPersistMicroDataTask::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 32)));
  micro_cache->start();
}

void TestSSPersistMicroDataTask::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

/* This case tests the basic logic of the persist_micro_data task. */
TEST_F(TestSSPersistMicroDataTask, test_persist_micro_task)
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  const int64_t block_size = micro_cache->phy_block_size_;

  micro_cache->task_runner_.is_inited_ = false;
  micro_cache->task_runner_.is_stopped_ = true;
  ObSSPersistMicroDataTask &persist_task = micro_cache->task_runner_.persist_task_;
  usleep(persist_task.interval_us_);

  ObSSMemDataManager *mem_data_mgr = &(micro_cache->mem_data_mgr_);
  ASSERT_NE(nullptr, mem_data_mgr);
  ObSSMicroMetaManager *micro_meta_mgr = &(micro_cache->micro_meta_mgr_);
  ASSERT_NE(nullptr, micro_meta_mgr);
  ObSSPhysicalBlockManager *phy_blk_mgr = &(micro_cache->phy_blk_mgr_);
  ASSERT_NE(nullptr, phy_blk_mgr);
  const int64_t available_block_cnt = phy_blk_mgr->blk_cnt_info_.cache_limit_blk_cnt();
  const int64_t WRITE_BLK_CNT = 50;
  ASSERT_LT(WRITE_BLK_CNT, available_block_cnt);

  const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                 ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 20; // this size should be small, thus each micro_block data size will be large
  const int32_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  const uint64_t interval_us = persist_task.interval_us_ * 10; // 20ms

  const int64_t ori_pool_def_cnt = mem_data_mgr->mem_block_pool_.def_count_;
  int64_t cur_pool_def_cnt = ori_pool_def_cnt;
  if (0 == cur_pool_def_cnt) {
    cur_pool_def_cnt = 1;
  }
  // 1. write some fulfilled mem_block and write a not fulfilled mem_block
  ObSSMemBlock *first_sealed_mem_blk = nullptr;
  int32_t cur_mem_micro_cnt = micro_cnt;
  for (int64_t i = 0; i < cur_pool_def_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200 + i);
    for (int32_t j = 0; j < cur_mem_micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                                         ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    }
    ASSERT_NE(nullptr, mem_data_mgr->fg_mem_block_);
    if (0 == i) {
      first_sealed_mem_blk = mem_data_mgr->fg_mem_block_;
    }
    bool is_dynamiclly_alloc = mem_data_mgr->mem_block_pool_.is_alloc_dynamiclly(mem_data_mgr->fg_mem_block_);
    if (0 == ori_pool_def_cnt) {
      ASSERT_EQ(true, is_dynamiclly_alloc);
    } else {
      ASSERT_EQ(false, is_dynamiclly_alloc);
    }
    ASSERT_EQ(cur_mem_micro_cnt, mem_data_mgr->fg_mem_block_->micro_count_);
    ASSERT_EQ(micro_size * cur_mem_micro_cnt, mem_data_mgr->fg_mem_block_->data_size_);
    ASSERT_EQ(micro_size * cur_mem_micro_cnt, mem_data_mgr->fg_mem_block_->valid_val_);
  }
  ASSERT_EQ(cur_pool_def_cnt, mem_data_mgr->mem_block_pool_.used_fg_count_);

  {
    cur_mem_micro_cnt = micro_cnt / 2;
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200 + cur_pool_def_cnt);
    for (int32_t j = 0; j < cur_mem_micro_cnt - 1; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                                         ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    }
    {
      // write a same micro_block into cur mem_block(thus there not exist a micro_meta[idx = cur_mem_micro_cnt-1])
      const int32_t offset = payload_offset + (cur_mem_micro_cnt - 2) * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                                         ObSSMicroCacheAccessType::COMMON_IO_TYPE); // fail to add it
      ASSERT_EQ(cur_mem_micro_cnt - 1, mem_data_mgr->fg_mem_block_->micro_count_);

      ObSSMemBlockHandle mem_blk_handle;
      uint32_t micro_crc = 0;
      ASSERT_EQ(OB_SUCCESS, mem_data_mgr->add_micro_block_data(micro_key, data_buf, micro_size, mem_blk_handle, micro_crc));
      ASSERT_EQ(true, mem_blk_handle.is_valid());
      bool is_first_add = false;
      ASSERT_EQ(OB_SUCCESS,
          micro_meta_mgr->add_or_update_micro_block_meta(micro_key, micro_size, micro_crc, mem_blk_handle, is_first_add));
      ASSERT_EQ(false, is_first_add);
    }
    ASSERT_NE(nullptr, mem_data_mgr->fg_mem_block_);
    ASSERT_EQ(1, mem_data_mgr->mem_block_pool_.used_extra_count_);
    bool is_dynamiclly_alloc = mem_data_mgr->mem_block_pool_.is_alloc_dynamiclly(mem_data_mgr->fg_mem_block_);
    ASSERT_EQ(true, is_dynamiclly_alloc);
    ASSERT_EQ(cur_mem_micro_cnt, mem_data_mgr->fg_mem_block_->micro_count_);
    ASSERT_EQ(micro_size * cur_mem_micro_cnt, mem_data_mgr->fg_mem_block_->data_size_);
    ASSERT_EQ(micro_size * (cur_mem_micro_cnt - 1), mem_data_mgr->fg_mem_block_->valid_val_);
  }
  ASSERT_EQ(1, mem_data_mgr->mem_block_pool_.used_extra_count_);

  // 2. check the count of the sealed mem_block
  ASSERT_EQ(cur_pool_def_cnt, mem_data_mgr->fg_sealed_mem_blocks_.get_curr_total());

  // 3. mock the first sealed_mem_block not complete its micro_meta update
  ASSERT_NE(nullptr, first_sealed_mem_blk);
  ASSERT_EQ(true, first_sealed_mem_blk->is_completed());
  first_sealed_mem_blk->micro_count_ += 1;
  ASSERT_EQ(false, first_sealed_mem_blk->is_completed());

  // 4. persist these sealed mem_block, check the micro_block meta
  persist_task.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_task.persist_op_.persist_sealed_mem_blocks());
  for (int64_t i = 0; i < cur_pool_def_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200 + i);
    for (int32_t j = 0; j < cur_mem_micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->get_micro_block_meta_handle(micro_key, micro_handle, false));
      ASSERT_EQ(true, micro_handle.is_valid());
      ASSERT_EQ(true, micro_handle.get_ptr()->is_valid());
      ASSERT_EQ(2, micro_handle.get_ptr()->ref_cnt_);
      if (0 == i) {
        ASSERT_EQ(false, micro_handle.get_ptr()->is_persisted_) << i << " " << j;
      } else {
        ASSERT_EQ(true, micro_handle.get_ptr()->is_persisted_) << i << " " << j;
      }
    }
  }
  ASSERT_EQ(2, mem_data_mgr->mem_block_pool_.used_fg_count_);
  ASSERT_EQ(1, mem_data_mgr->fg_sealed_mem_blocks_.get_curr_total());

  // 4. write the not fulfilled mem_block to make it fulfilled, and seal it
  {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200 + cur_pool_def_cnt);
    for (int32_t j = cur_mem_micro_cnt; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                                         ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    }
    ASSERT_NE(nullptr, mem_data_mgr->fg_mem_block_);
    ASSERT_EQ(micro_cnt, mem_data_mgr->fg_mem_block_->micro_count_);
    ASSERT_EQ(micro_size * micro_cnt, mem_data_mgr->fg_mem_block_->data_size_);
    ASSERT_EQ(micro_size * (micro_cnt - 1), mem_data_mgr->fg_mem_block_->valid_val_);

    {
      // to seal the above mem_block, here will use a mem_block
      macro_id = TestSSCommonUtil::gen_macro_block_id(200 + cur_pool_def_cnt + 1);
      const int32_t offset = payload_offset;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                                         ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    }
  }

  // 5. mock a micro_meta of this dynamiclly allocated mem_block is abnormal(reuse_version changed)
  ObSSMicroBlockCacheKey abnormal_micro_key;
  {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200 + cur_pool_def_cnt);
    const int32_t offset = payload_offset + 2 * micro_size; // the third micro_meta is abnormal
    abnormal_micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    ObSSMicroBlockMetaHandle micro_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->get_micro_block_meta_handle(abnormal_micro_key, micro_handle, false));
    ASSERT_EQ(true, micro_handle.is_valid());
    ASSERT_EQ(true, micro_handle.get_ptr()->is_valid());
    ASSERT_EQ(2, micro_handle.get_ptr()->ref_cnt_);
    ASSERT_EQ(false, micro_handle.get_ptr()->is_persisted_);
    micro_handle.get_ptr()->reuse_version_ += 1; // change its reuse_version
  }

  // 6. make the above not completed mem_block change to completed
  first_sealed_mem_blk->micro_count_ -= 1;
  ASSERT_EQ(true, first_sealed_mem_blk->is_completed());

  // 7. persist these two mem_blocks
  // one is normal sealed_mem_block; another is dynamiclly allocated mem_block which has a abnormal micro_block
  ASSERT_EQ(OB_SUCCESS, persist_task.persist_op_.persist_sealed_mem_blocks());

  ASSERT_EQ(1, mem_data_mgr->mem_block_pool_.used_fg_count_);
  ASSERT_EQ(0, mem_data_mgr->fg_sealed_mem_blocks_.get_curr_total());

  // 8. check the first_sealed_mem_block is persisted successfully
  {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200);
    for (int32_t j = 0; j < cur_mem_micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->get_micro_block_meta_handle(micro_key, micro_handle, false));
      ASSERT_EQ(true, micro_handle.is_valid());
      ASSERT_EQ(true, micro_handle.get_ptr()->is_valid());
      ASSERT_EQ(2, micro_handle.get_ptr()->ref_cnt_);
      ASSERT_EQ(true, micro_handle.get_ptr()->is_persisted_valid()) << " j: " << j;
    }
  }

  // 9. check the abnormal micro_block of this mem_block has already been erased from micro_meta_map
  {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200 + cur_pool_def_cnt);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_handle;
      if (j == 2 || j == micro_cnt / 2 - 1) { // j=micro_cnt/2-1: not exist because didn't add this micro_block
        ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr->get_micro_block_meta_handle(micro_key, micro_handle, false));
      } else {
        ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->get_micro_block_meta_handle(micro_key, micro_handle, false)) << j;
        ASSERT_EQ(true, micro_handle.get_ptr()->is_persisted_valid()) << " j: " << j;
      }
    }
  }

  // 10. this mem_block will be free(destroy directly)
  ASSERT_EQ(1, mem_data_mgr->mem_block_pool_.used_fg_count_);
  if (0 == ori_pool_def_cnt) {
    ASSERT_EQ(1, mem_data_mgr->mem_block_pool_.used_extra_count_);
  } else {
    ASSERT_EQ(0, mem_data_mgr->mem_block_pool_.used_extra_count_);
  }

  micro_cache->task_runner_.is_inited_ = true;
  micro_cache->task_runner_.is_stopped_ = false;
  allocator.clear();
}

/* Test when persist_task fail to alloc any phy_block to flush sealed_mem_block, it will exit directly and not enter a dead loop */
TEST_F(TestSSPersistMicroDataTask, test_persist_task_exit_loop)
{
  ObArenaAllocator allocator;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  SSPhyBlockCntInfo &blk_cnt_info = micro_cache->phy_blk_mgr_.blk_cnt_info_;
  ASSERT_NE(nullptr, micro_cache);

  // 1. mock cache_data_block used up.
  const int64_t max_cache_data_blk_cnt = blk_cnt_info.cache_limit_blk_cnt();
  blk_cnt_info.data_blk_.max_cnt_ = max_cache_data_blk_cnt;
  blk_cnt_info.data_blk_.hold_cnt_ = max_cache_data_blk_cnt;
  blk_cnt_info.data_blk_.used_cnt_ = max_cache_data_blk_cnt;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;

  // 2. write micro data to add some sealed_mem_blocks.
  const int64_t macro_block_cnt = 5;
  const int32_t micro_size = (1 << 16);
  const int64_t payload_offset =
      ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = (DEFAULT_BLOCK_SIZE - payload_offset) / (micro_size + micro_index_size);
  char *data_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  for (int64_t i = 0; i < macro_block_cnt; ++i) {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    }
  }
  const int64_t sealed_mem_block_cnt = micro_cache->mem_data_mgr_.get_sealed_mem_block_cnt();
  ASSERT_LT(0, sealed_mem_block_cnt);
  ob_usleep(1000 * 1000);

  // 3. test persist_task can exit loop.
  ObTenantBase *tenant_base = ObTenantEnv::get_tenant();
  bool stop = false;
  std::thread t([&]() {
    ObTenantEnv::set_tenant(tenant_base);
    micro_cache->task_runner_.stop();
    micro_cache->task_runner_.wait();
    ATOMIC_STORE(&stop, true);
  });

  const int64_t timeout_us = 10 * 1000 * 1000L;
  const int64_t start_us = ObTimeUtility::current_time();
  while (ATOMIC_LOAD(&stop) == false) {
    ob_usleep(1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      break;
    }
  }
  ASSERT_EQ(true, ATOMIC_LOAD(&stop));
  ASSERT_EQ(sealed_mem_block_cnt, micro_cache->mem_data_mgr_.get_sealed_mem_block_cnt());
  t.join();
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_persist_micro_data_task.log*");
  OB_LOGGER.set_file_name("test_ss_persist_micro_data_task.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}