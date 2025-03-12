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
using namespace oceanbase::hash;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheEviction : public ::testing::Test
{
public:
  TestSSMicroCacheEviction() {}
  virtual ~TestSSMicroCacheEviction() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void set_basic_read_io_info(ObIOInfo &io_info);
  // calculate total micro cnt of fg_mem_block and bg_mem_block.
  int64_t cal_unpersisted_micro_cnt();
};

void TestSSMicroCacheEviction::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheEviction::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheEviction::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 30))); // 1G
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
}

void TestSSMicroCacheEviction::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSMicroCacheEviction::set_basic_read_io_info(ObIOInfo &io_info)
{
  io_info.tenant_id_ = MTL_ID();
  io_info.timeout_us_ = 5 * 1000 * 1000L; // 5s
  io_info.flag_.set_read();
  io_info.flag_.set_resource_group_id(0);
  io_info.flag_.set_wait_event(1);
}

int64_t TestSSMicroCacheEviction::cal_unpersisted_micro_cnt()
{
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  int64_t total_unpersisted_micro_cnt = 0;
  if (nullptr != mem_data_mgr.fg_mem_block_) {
    total_unpersisted_micro_cnt += mem_data_mgr.fg_mem_block_->micro_count_;
  }
  if (nullptr != mem_data_mgr.bg_mem_block_) {
    total_unpersisted_micro_cnt += mem_data_mgr.bg_mem_block_->micro_count_;
  }
  return total_unpersisted_micro_cnt;
}

TEST_F(TestSSMicroCacheEviction, test_delete_ghost_micro)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_delete_ghost_micro");
  ObArenaAllocator allocator;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSARCInfo &arc_info = micro_cache->micro_meta_mgr_.arc_info_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMemDataManager &mem_data_mgr = micro_cache->mem_data_mgr_;
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;

  const int64_t payload_offset =
      ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 50;
  const int32_t micro_size = (DEFAULT_BLOCK_SIZE - payload_offset) / micro_cnt - micro_index_size;
  char *data_buf = nullptr;
  char *read_buf = nullptr;
  data_buf = static_cast<char *>(allocator.alloc(micro_size));
  read_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  ASSERT_NE(nullptr, read_buf);
  MEMSET(data_buf, 'a', micro_size);

  // 1. write 2 macro_blocks
  for (int64_t i = 0; i < 2; ++i) {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(1000 + i + 1);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
        ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(OB_SUCCESS,TestSSCommonUtil::wait_for_persist_task());
  }

  // 2. mark the first macro_blocks' micro_blocks as ghost, heat half of second macro_blocks' micro_blocks
  {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(1001);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_cache->micro_meta_mgr_.get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      ASSERT_EQ(true, micro_meta_handle()->is_persisted_);
      ASSERT_EQ(true, micro_meta_handle()->is_in_l1_);
      micro_meta_handle()->is_in_ghost_ = true;
    }
    arc_info.seg_info_arr_[ARC_T1].cnt_ = micro_cnt;
    arc_info.seg_info_arr_[ARC_T1].size_ = micro_cnt * micro_size;
    arc_info.seg_info_arr_[ARC_B1].cnt_ = micro_cnt;
    arc_info.seg_info_arr_[ARC_B1].size_ = micro_cnt * micro_size;
  }
  {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(1002);
    for (int64_t j = 0; j < micro_cnt / 2; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_cache->micro_meta_mgr_.get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      ASSERT_EQ(false, micro_meta_handle()->is_persisted_);
      micro_meta_handle()->is_in_l1_ = false;
    }
    arc_info.seg_info_arr_[ARC_T1].cnt_ = micro_cnt / 2;
    arc_info.seg_info_arr_[ARC_T1].size_ = micro_cnt / 2 * micro_size;
    arc_info.seg_info_arr_[ARC_T2].cnt_ = micro_cnt / 2;
    arc_info.seg_info_arr_[ARC_T2].size_ = micro_cnt / 2 * micro_size;
  }

  // 3. wait for all the B1 micro_meta being deleted
  arc_info.do_update_arc_limit(0, /* need_update_limit */ false); // set work_limit = 0
  arc_task.is_inited_ = true;

  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 60 * 1000 * 1000;
  while (arc_info.seg_info_arr_[ARC_B1].cnt_ > 0) {
    ob_usleep(1000 * 1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      break;
    }
    LOG_INFO("test_delete_ghost", K(micro_cache->cache_stat_.micro_stat()), K(arc_info));
  }
  ASSERT_EQ(0, arc_info.seg_info_arr_[ARC_B1].cnt_);
}

/*
  step1: write cache_file_size * 50% data to T1, then read this part of data again and transfer it to T2.
  step2: write cache_file_size until usage of cache_data_block reach limit.
  step3: adjust arc_limit to 0, and the background thread clears all micro_meta which is persisted.
  step4: check the number of micro_cnt, total_micro_cnt <= bg_mem_blk.max_micro_cnt + fg_mem_blk.max_micro_cnt,
         and all used phy_blk is added into reusable_set.
*/
TEST_F(TestSSMicroCacheEviction, test_delete_all_persisted_micro)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_delete_all_persisted_micro");
  ObArenaAllocator allocator;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSARCInfo &arc_info = micro_cache->micro_meta_mgr_.arc_info_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMemDataManager &mem_data_mgr = micro_cache->mem_data_mgr_;
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;

  // Step 1
  const int64_t payload_offset =
      ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 20;
  const int32_t micro_size = (DEFAULT_BLOCK_SIZE - payload_offset) / micro_cnt - micro_index_size;
  char *data_buf = nullptr;
  char *read_buf = nullptr;
  data_buf = static_cast<char *>(allocator.alloc(micro_size));
  read_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  ASSERT_NE(nullptr, read_buf);
  MEMSET(data_buf, 'a', micro_size);

  const int64_t data_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt();
  const int64_t macro_block_cnt1 = data_blk_cnt / 2;
  for (int64_t i = 0; i < macro_block_cnt1; ++i) {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS,
          micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
      ObStorageObjectHandle obj_handle;
      ObIOInfo io_info;
      ASSERT_EQ(OB_SUCCESS,TestSSCommonUtil::init_io_info(io_info, micro_key, micro_size, read_buf));
      ObSSMicroBlockId phy_micro_id(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(micro_key, phy_micro_id, MicroCacheGetType::FORCE_GET_DATA,
              io_info, obj_handle, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(OB_SUCCESS,TestSSCommonUtil::wait_for_persist_task());
  }

  // Step 2
  const int64_t data_blk_limit_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt();
  const int64_t macro_block_cnt2 = data_blk_limit_cnt - macro_block_cnt1 + 1;
  for (int64_t i = 0; i < macro_block_cnt2; ++i) {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + macro_block_cnt1 + 1);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      // ignore no free cache_data_phy_block error
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }

  // Step 3
  arc_info.do_update_arc_limit(0, /* need_update_limit */false); // set work_limit = 0
  arc_task.is_inited_ = true;

  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 60 * 1000 * 1000;
  int64_t total_unpersisted_micro_cnt = cal_unpersisted_micro_cnt();
  while (micro_cache->cache_stat_.micro_stat().total_micro_cnt_ > total_unpersisted_micro_cnt) {
    ob_usleep(1000 * 1000);
    total_unpersisted_micro_cnt = cal_unpersisted_micro_cnt();
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      break;
    }
    LOG_INFO("test_delete_all_persisted_micro", K(micro_cache->cache_stat_), K(total_unpersisted_micro_cnt), K(arc_info));
  }
  ASSERT_EQ(OB_SUCCESS,TestSSCommonUtil::wait_for_persist_task());

  // print micro map
  LOG_INFO("start print micro map");
  ObSSMicroMetaManager::SSMicroMap &micro_map = micro_cache->micro_meta_mgr_.micro_meta_map_;
  ObSSMicroMetaManager::SSMicroMap::BlurredIterator micro_iter_(micro_map);
  micro_iter_.rewind();
  for (int64_t i = 1; ; ++i) {
    const ObSSMicroBlockCacheKey *micro_key = nullptr;
    ObSSMicroBlockMetaHandle micro_handle;
    if (OB_FAIL(micro_iter_.next(micro_key, micro_handle))) {
      if (OB_ITER_END == ret) {
        break;
      } else {
        LOG_ERROR("fail to next micro_block meta", KR(ret), K(micro_key));
      }
    } else {
      LOG_INFO("succeed to print micro_meta", K(i), K_(micro_iter_.bkt_idx), K_(micro_iter_.key_idx), K(micro_map.get_bkt_cnt()), KPC(micro_handle()));
    }
  }
  LOG_INFO("end print micro map");

  // Step 4
  arc_task.is_inited_ = false;
  total_unpersisted_micro_cnt = cal_unpersisted_micro_cnt();
  ASSERT_EQ(total_unpersisted_micro_cnt, micro_cache->cache_stat_.micro_stat().valid_micro_cnt_);
  ASSERT_LE(micro_cache->cache_stat_.micro_stat().valid_micro_cnt_, micro_cnt * 2);
  ASSERT_EQ(phy_blk_mgr.reusable_set_.size(), phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_eviction.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_eviction.log", true);
  OB_LOGGER.set_log_level("TRACE");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
