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
#include "lib/thread/threads.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
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
  int add_single_micro_block(const ObSSMicroBlockCacheKey &micro_key, const char *content, const int32_t micro_size);
  // calculate total micro cnt of fg_mem_block and bg_mem_block.
  int64_t cal_unpersisted_micro_cnt();
private:
  ObSSMicroCache *micro_cache_;
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

int TestSSMicroCacheEviction::add_single_micro_block(
    const ObSSMicroBlockCacheKey &micro_key,
    const char *content,
    const int32_t micro_size)
{
  int ret = OB_SUCCESS;
  ObSSMemBlockManager &mem_blk_mgr = MTL(ObSSMicroCache *)->mem_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  uint32_t micro_crc = 0;
  bool alloc_new = false;
  bool first_add = false;
  ObSSMemBlockHandle mem_blk_handle;
  const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
  if (OB_UNLIKELY(!micro_key.is_valid() || OB_ISNULL(content) || micro_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), KP(content), K(micro_size));
  } else if (OB_FAIL(mem_blk_mgr.add_micro_block_data(micro_key, content, micro_size, mem_blk_handle, micro_crc, alloc_new))) {
    LOG_WARN("fail to add micro data", K(micro_key), K(micro_size), K(mem_blk_handle), K(micro_crc), K(alloc_new));
  } else if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_blk_handle is invalid");
  } else if (OB_FAIL(micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc,
                                    effective_tablet_id, mem_blk_handle, first_add))) {
    LOG_WARN("fail to add_or_update micro_meta", K(micro_key), K(micro_size), K(micro_crc), K(mem_blk_handle));
  } else if (OB_UNLIKELY(false == first_add)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key is not first_add", K(micro_key));
  }
  return ret;
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
  micro_cache_ = micro_cache;
}

void TestSSMicroCacheEviction::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  micro_cache_ = nullptr;
}

int64_t TestSSMicroCacheEviction::cal_unpersisted_micro_cnt()
{
  ObSSMemBlockManager &mem_blk_mgr = MTL(ObSSMicroCache *)->mem_blk_mgr_;
  int64_t total_unpersisted_micro_cnt = 0;
  if (nullptr != mem_blk_mgr.fg_mem_blk_) {
    total_unpersisted_micro_cnt += mem_blk_mgr.fg_mem_blk_->micro_cnt_;
  }
  if (nullptr != mem_blk_mgr.bg_mem_blk_) {
    total_unpersisted_micro_cnt += mem_blk_mgr.bg_mem_blk_->micro_cnt_;
  }
  return total_unpersisted_micro_cnt;
}

TEST_F(TestSSMicroCacheEviction, test_delete_ghost_micro)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_delete_ghost_micro");
  ObArenaAllocator allocator;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSReleaseCacheTask &release_cache_task = micro_cache_->task_runner_.release_cache_task_;
  release_cache_task.is_inited_ = false;

  const int64_t payload_offset = ObSSMemBlock::get_reserved_size();
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
      ASSERT_EQ(OB_SUCCESS, add_single_micro_block(micro_key, data_buf, micro_size));
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
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta(micro_key, micro_meta_handle, ObTabletID::INVALID_TABLET_ID, false));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      ASSERT_EQ(true, micro_meta_handle()->is_data_persisted_);
      ASSERT_EQ(true, micro_meta_handle()->is_in_l1_);
      micro_meta_handle()->is_in_ghost_ = true;
    }
    arc_info.seg_info_arr_[SS_ARC_T1].cnt_ = micro_cnt;
    arc_info.seg_info_arr_[SS_ARC_T1].size_ = micro_cnt * micro_size;
    arc_info.seg_info_arr_[SS_ARC_B1].cnt_ = micro_cnt;
    arc_info.seg_info_arr_[SS_ARC_B1].size_ = micro_cnt * micro_size;
  }

  {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(1002);
    for (int64_t j = 0; j < micro_cnt / 2; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta(micro_key, micro_meta_handle, ObTabletID::INVALID_TABLET_ID, false));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      ASSERT_EQ(false, micro_meta_handle()->is_data_persisted_);
      micro_meta_handle()->is_in_l1_ = false;
    }
    arc_info.seg_info_arr_[SS_ARC_T1].cnt_ = micro_cnt / 2;
    arc_info.seg_info_arr_[SS_ARC_T1].size_ = micro_cnt / 2 * micro_size;
    arc_info.seg_info_arr_[SS_ARC_T2].cnt_ = micro_cnt / 2;
    arc_info.seg_info_arr_[SS_ARC_T2].size_ = micro_cnt / 2 * micro_size;
  }

  // 3. wait for all the B1 micro_meta being deleted
  arc_info.do_update_arc_limit(0, /* need_update_limit */ false); // set work_limit = 0
  release_cache_task.is_inited_ = true;

  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 60 * 1000 * 1000;
  while (arc_info.seg_info_arr_[SS_ARC_B1].cnt_ > 0) {
    ob_usleep(1000 * 1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      break;
    }
    LOG_INFO("test_delete_ghost", K(micro_cache_->cache_stat_.micro_stat()), K(arc_info));
  }
  ASSERT_EQ(0, arc_info.seg_info_arr_[SS_ARC_B1].cnt_);
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
  ObSSARCInfo &arc_info = micro_cache_->micro_meta_mgr_.arc_info_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSReleaseCacheTask &release_cache_task = micro_cache_->task_runner_.release_cache_task_;
  release_cache_task.is_inited_ = false;
  release_cache_task.reorganize_op_.is_enabled_ = false;

  // Step 1
  const int64_t payload_offset = ObSSMemBlock::get_reserved_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 20;
  const int32_t micro_size = (DEFAULT_BLOCK_SIZE - payload_offset) / micro_cnt - micro_index_size;
  char *data_buf = nullptr;
  data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  const int64_t data_blk_cnt = phy_blk_mgr.blk_cnt_info_.micro_data_blk_max_cnt();
  const int64_t macro_block_cnt1 = data_blk_cnt / 2;
  for (int64_t i = 0; i < macro_block_cnt1; ++i) {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS, add_single_micro_block(micro_key, data_buf, micro_size));
      ObSSMicroBlockMetaHandle micro_meta_handle;
      const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta(micro_key, micro_meta_handle, effective_tablet_id, true));
    }
    ASSERT_EQ(OB_SUCCESS,TestSSCommonUtil::wait_for_persist_task());
  }

  // Step 2
  const int64_t data_blk_limit_cnt = phy_blk_mgr.blk_cnt_info_.micro_data_blk_max_cnt();
  const int64_t macro_block_cnt2 = data_blk_limit_cnt - macro_block_cnt1 + 1;
  for (int64_t i = 0; i < macro_block_cnt2; ++i) {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + macro_block_cnt1 + 1);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      // ignore no free cache_data_phy_block error
      add_single_micro_block(micro_key, data_buf, micro_size);
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }

  // Step 3
  arc_info.do_update_arc_limit(0, /* need_update_limit */false); // set work_limit = 0
  LOG_INFO("TEST: start eviction", K(arc_info));
  release_cache_task.is_inited_ = true;

  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 60 * 1000 * 1000;
  int64_t total_unpersisted_micro_cnt = cal_unpersisted_micro_cnt();
  while (micro_cache_->cache_stat_.micro_stat().total_micro_cnt_ > total_unpersisted_micro_cnt) {
    ob_usleep(1000 * 1000);
    total_unpersisted_micro_cnt = cal_unpersisted_micro_cnt();
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      break;
    }
    LOG_INFO("test_delete_all_persisted_micro", K(micro_cache_->cache_stat_), K(total_unpersisted_micro_cnt), K(arc_info));
  }
  ASSERT_EQ(OB_SUCCESS,TestSSCommonUtil::wait_for_persist_task());

  // print micro map
  LOG_INFO("start print micro map");
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = micro_cache_->micro_meta_mgr_.micro_meta_map_;
  ObSSMicroMetaManager::SSMicroMetaMap::BlurredIterator micro_iter_(micro_map);
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
  release_cache_task.is_inited_ = false;
  total_unpersisted_micro_cnt = cal_unpersisted_micro_cnt();
  ASSERT_EQ(total_unpersisted_micro_cnt, micro_cache_->cache_stat_.micro_stat().valid_micro_cnt_);
  ASSERT_LE(micro_cache_->cache_stat_.micro_stat().valid_micro_cnt_, micro_cnt * 2);
  ASSERT_EQ(phy_blk_mgr.reusable_blks_.size(), phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);
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
