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
#include "mittest/shared_storage/test_ss_common_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
class TestSSMicroCacheStat : public ::testing::Test
{
public:
  TestSSMicroCacheStat() {}
  virtual ~TestSSMicroCacheStat() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSMicroCacheStat::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheStat::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheStat::SetUp()
{}

void TestSSMicroCacheStat::TearDown()
{}

TEST_F(TestSSMicroCacheStat, test_micro_cache_stat)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  const int64_t block_size = micro_cache->phy_blk_size_;

  ObSSPersistMicroDataTask &persist_data_task = micro_cache->task_runner_.persist_data_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  blk_ckpt_task.is_inited_ = false;
  ObSSReleaseCacheTask &release_cache_task = micro_cache->task_runner_.release_cache_task_;
  release_cache_task.is_inited_ = false;

  ObSSMemBlockManager *mem_blk_mgr = &(micro_cache->mem_blk_mgr_);
  ASSERT_NE(nullptr, mem_blk_mgr);
  ObSSMicroMetaManager *micro_meta_mgr = &(micro_cache->micro_meta_mgr_);
  ASSERT_NE(nullptr, micro_meta_mgr);
  ObSSPhysicalBlockManager *phy_blk_mgr = &(micro_cache->phy_blk_mgr_);
  ASSERT_NE(nullptr, phy_blk_mgr);

  ObSSMicroCacheStat &cache_stat = micro_cache->get_micro_cache_stat();
  ASSERT_EQ(cache_stat.phy_blk_stat().total_blk_cnt_, phy_blk_mgr->blk_cnt_info_.total_blk_cnt_);
  ASSERT_EQ(cache_stat.mem_blk_stat().mem_blk_fg_max_cnt_, mem_blk_mgr->mem_blk_pool_.max_fg_count_);
  ASSERT_EQ(cache_stat.mem_blk_stat().mem_blk_bg_max_cnt_, mem_blk_mgr->mem_blk_pool_.max_bg_count_);
  ASSERT_EQ(cache_stat.mem_blk_stat().mem_blk_fg_max_cnt_ + cache_stat.mem_blk_stat().mem_blk_bg_max_cnt_,
      mem_blk_mgr->max_mem_block_cnt());
  ASSERT_EQ(0, cache_stat.micro_stat().total_micro_cnt_);

  const int64_t payload_offset = ObSSMemBlock::get_reserved_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 8; // this size should be small, thus each micro_block data size will be large
  const int32_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);
  char *read_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, read_buf);

  const int64_t available_block_cnt = phy_blk_mgr->blk_cnt_info_.micro_data_blk_max_cnt();
  const int64_t max_fg_cnt = mem_blk_mgr->mem_blk_pool_.max_fg_count_;
  ASSERT_LT(0, max_fg_cnt);

  ASSERT_LT(max_fg_cnt, available_block_cnt);
  // 1. write some fulfilled mem_block
  int32_t cur_mem_micro_cnt = micro_cnt;
  for (int64_t i = 0; i < max_fg_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200 + i);
    for (int32_t j = 0; j < cur_mem_micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                                         macro_id.second_id()/*effective_tablet_id*/,
                                         ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(max_fg_cnt * cur_mem_micro_cnt, cache_stat.hit_stat().new_add_cnt_);
  ASSERT_EQ(max_fg_cnt * cur_mem_micro_cnt, cache_stat.io_stat().common_io_param_.add_cnt_);
  ASSERT_EQ(max_fg_cnt * cur_mem_micro_cnt * micro_size, cache_stat.io_stat().common_io_param_.add_bytes_);
  ASSERT_EQ(0, cache_stat.io_stat().prewarm_io_param_.add_cnt_);

  {
    // to seal the last mem_block and to persist it
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200 + max_fg_cnt);
    const int32_t offset = payload_offset + micro_size;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                                       macro_id.second_id()/*effective_tablet_id*/,
                                       ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(1, cache_stat.prewarm_stat().new_add_cnt_);
  ASSERT_EQ(1, cache_stat.io_stat().prewarm_io_param_.add_cnt_);
  ASSERT_EQ(micro_size, cache_stat.io_stat().prewarm_io_param_.add_bytes_);

  ASSERT_EQ(max_fg_cnt, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  ASSERT_EQ(max_fg_cnt * micro_cnt + 1, cache_stat.micro_stat().total_micro_cnt_);
  ObSSARCInfo &arc_info = micro_meta_mgr->arc_info_;
  ASSERT_EQ(max_fg_cnt * micro_cnt + 1, arc_info.seg_info_arr_[SS_ARC_T1].count());
  ASSERT_EQ((max_fg_cnt * micro_cnt + 1) * micro_size, arc_info.seg_info_arr_[SS_ARC_T1].size());

  // 2. get some micro_block
  {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockId phy_micro_id(macro_id, offset, micro_size);
      ObSSMicroCacheGetType get_type = ObSSMicroCacheGetType::FORCE_GET_DATA;
      ObIOInfo io_info;
      ObStorageObjectHandle obj_handle;
      bool is_hit = false;
      ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(io_info, micro_key, micro_size, read_buf));
      ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(micro_key, phy_micro_id, get_type,
                            io_info, obj_handle, ObSSMicroCacheAccessType::COMMON_IO_TYPE, is_hit));
      ASSERT_EQ(true, is_hit);
      ASSERT_EQ(true, io_info.phy_block_handle_.is_valid());
    }
    ASSERT_EQ(micro_cnt, cache_stat.io_stat().common_io_param_.get_cnt_);
    ASSERT_EQ(micro_cnt * micro_size, cache_stat.io_stat().common_io_param_.get_bytes_);

    {
      macro_id = TestSSCommonUtil::gen_macro_block_id(200 + max_fg_cnt);
      const int32_t offset = payload_offset + micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockId phy_micro_id(macro_id, offset, micro_size);
      ObSSMicroCacheGetType get_type = ObSSMicroCacheGetType::FORCE_GET_DATA;
      ObIOInfo io_info;
      ObStorageObjectHandle obj_handle;
      bool is_hit = false;
      ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(io_info, micro_key, micro_size, read_buf));
      ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(micro_key, phy_micro_id, get_type,
                            io_info, obj_handle, ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE, is_hit));
      ASSERT_EQ(true, is_hit);
      ASSERT_EQ(false, io_info.phy_block_handle_.is_valid());
    }
    ASSERT_EQ(1, cache_stat.io_stat().prewarm_io_param_.get_cnt_);
    ASSERT_EQ(micro_size, cache_stat.io_stat().prewarm_io_param_.get_bytes_);

    ASSERT_EQ(micro_cnt, cache_stat.hit_stat().cache_hit_cnt_);
    // prewarm_io won't update arc info
    ASSERT_EQ((max_fg_cnt - 1) * micro_cnt + 1, arc_info.seg_info_arr_[SS_ARC_T1].count());
    ASSERT_EQ(((max_fg_cnt - 1) * micro_cnt + 1) * micro_size, arc_info.seg_info_arr_[SS_ARC_T1].size());
    ASSERT_EQ(micro_cnt, arc_info.seg_info_arr_[SS_ARC_T2].count());
    ASSERT_EQ(micro_cnt * micro_size, arc_info.seg_info_arr_[SS_ARC_T2].size());
  }

  // 3. still get these micro_block
  {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockId phy_micro_id(macro_id, offset, micro_size);
      ObSSMicroCacheGetType get_type = ObSSMicroCacheGetType::FORCE_GET_DATA;
      ObIOInfo io_info;
      ObStorageObjectHandle obj_handle;
      bool is_hit = false;
      ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(io_info, micro_key, micro_size, read_buf));
      ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(micro_key, phy_micro_id, get_type,
                            io_info, obj_handle, ObSSMicroCacheAccessType::COMMON_IO_TYPE, is_hit));
      ASSERT_EQ(true, is_hit);
      ASSERT_EQ(true, io_info.phy_block_handle_.is_valid());
    }
    {
      macro_id = TestSSCommonUtil::gen_macro_block_id(200 + max_fg_cnt);
      const int32_t offset = payload_offset + micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockId phy_micro_id(macro_id, offset, micro_size);
      ObSSMicroCacheGetType get_type = ObSSMicroCacheGetType::FORCE_GET_DATA;
      ObIOInfo io_info;
      ObStorageObjectHandle obj_handle;
      bool is_hit = false;
      ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(io_info, micro_key, micro_size, read_buf));
      ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(micro_key, phy_micro_id, get_type,
                            io_info, obj_handle, ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE, is_hit));
      ASSERT_EQ(true, is_hit);
      ASSERT_EQ(false, io_info.phy_block_handle_.is_valid());
    }
    ASSERT_EQ(micro_cnt * 2, cache_stat.hit_stat().cache_hit_cnt_);
    ASSERT_EQ((max_fg_cnt - 1) * micro_cnt + 1, arc_info.seg_info_arr_[SS_ARC_T1].count());
    ASSERT_EQ(((max_fg_cnt - 1) * micro_cnt + 1)* micro_size, arc_info.seg_info_arr_[SS_ARC_T1].size());
    ASSERT_EQ(micro_cnt, arc_info.seg_info_arr_[SS_ARC_T2].count());
    ASSERT_EQ(micro_cnt * micro_size, arc_info.seg_info_arr_[SS_ARC_T2].size());
  }

  // 4. try delete some micro_block
  if (max_fg_cnt > 1) {
    int64_t cur_micro_cnt = cache_stat.micro_stat().total_micro_cnt_;
    int64_t del_micro_cnt = 0;
    for (int64_t i = 2; i < max_fg_cnt; ++i) {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200 + i);
      for (int32_t j = 0; j < micro_cnt; ++j) {
        const int32_t offset = payload_offset + j * micro_size;
        ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        ObSSMicroBlockMetaHandle micro_meta_handle;
        ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->get_micro_block_meta(micro_key, micro_meta_handle, ObTabletID::INVALID_TABLET_ID, false));
        ASSERT_EQ(true, micro_meta_handle.is_valid());
        ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->try_force_delete_micro_block_meta(micro_key));
        del_micro_cnt++;
        ASSERT_EQ(del_micro_cnt, cache_stat.micro_stat().mark_del_micro_cnt_);
      }
    }
    ASSERT_EQ(micro_cnt + 1, arc_info.seg_info_arr_[SS_ARC_T1].count());
    ASSERT_EQ((micro_cnt + 1) * micro_size, arc_info.seg_info_arr_[SS_ARC_T1].size());
    ASSERT_EQ(micro_cnt, arc_info.seg_info_arr_[SS_ARC_T2].count());
    ASSERT_EQ(micro_cnt * micro_size, arc_info.seg_info_arr_[SS_ARC_T2].size());
  }

  // 5. try to evict some micro_block from T1 and T2.
  for (int64_t i = 0; i <= 1; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200 + i);
    for (int32_t j = 0; j < 2; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->get_micro_block_meta(micro_key, micro_handle, ObTabletID::INVALID_TABLET_ID, false));
      ObSSMicroBlockMetaInfo cold_micro;
      micro_handle()->get_micro_meta_info(cold_micro);
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->try_evict_micro_block_meta(cold_micro));
    }
  }
  ASSERT_EQ(2, cache_stat.task_stat().t1_evict_cnt_);
  ASSERT_EQ(2, cache_stat.task_stat().t2_evict_cnt_);
  ASSERT_EQ(micro_cnt - 1, arc_info.seg_info_arr_[SS_ARC_T1].count());
  ASSERT_EQ((micro_cnt - 1) * micro_size, arc_info.seg_info_arr_[SS_ARC_T1].size());
  ASSERT_EQ(2, arc_info.seg_info_arr_[SS_ARC_B1].count());
  ASSERT_EQ(2 * micro_size, arc_info.seg_info_arr_[SS_ARC_B1].size());
  ASSERT_EQ(micro_cnt - 2, arc_info.seg_info_arr_[SS_ARC_T2].count());
  ASSERT_EQ((micro_cnt - 2) * micro_size, arc_info.seg_info_arr_[SS_ARC_T2].size());
  ASSERT_EQ(2, arc_info.seg_info_arr_[SS_ARC_B2].count());
  ASSERT_EQ(2 * micro_size, arc_info.seg_info_arr_[SS_ARC_B2].size());

  // 6. try to delete some micro_block from B1 and B2
  for (int64_t i = 0; i <= 1; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200 + i);
    for (int32_t j = 0; j < 2; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->micro_meta_map_.get(&micro_key, micro_handle));
      ASSERT_EQ(true, micro_handle.is_valid());
      ASSERT_EQ(true, micro_handle()->is_in_ghost_);
      ObSSMicroBlockMetaInfo cold_micro;
      micro_handle()->get_micro_meta_info(cold_micro);
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->try_delete_micro_block_meta(cold_micro));
    }
  }
  ASSERT_EQ(2, cache_stat.task_stat().b1_delete_cnt_);
  ASSERT_EQ(2, cache_stat.task_stat().b2_delete_cnt_);
  ASSERT_EQ(0, arc_info.seg_info_arr_[SS_ARC_B1].count());
  ASSERT_EQ(0, arc_info.seg_info_arr_[SS_ARC_B1].size());
  ASSERT_EQ(0, arc_info.seg_info_arr_[SS_ARC_B2].count());
  ASSERT_EQ(0, arc_info.seg_info_arr_[SS_ARC_B2].size());
  ASSERT_EQ(micro_cnt - 1, arc_info.seg_info_arr_[SS_ARC_T1].count());
  ASSERT_EQ(micro_cnt - 2, arc_info.seg_info_arr_[SS_ARC_T2].count());

  // 7. check get_available_space_for_prewarm
  int64_t available_space = 0;
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_available_space_for_prewarm(available_space));
  ASSERT_EQ(arc_info.limit_, available_space);
  const int64_t max_cache_data_blk_cnt = phy_blk_mgr->blk_cnt_info_.micro_data_blk_max_cnt();
  int64_t need_alloc_cnt = max_cache_data_blk_cnt * 2 / 3;
  for (int64_t i = 0; i < need_alloc_cnt; ++i) {
    int64_t tmp_phy_blk_idx = -1;
    ObSSPhyBlockHandle tmp_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->alloc_block(tmp_phy_blk_idx, tmp_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
    ASSERT_EQ(true, tmp_blk_handle.is_valid());
  }

  int64_t cache_limit_blk_cnt = 0;
  int64_t cache_data_blk_usage_pct = 0;
  phy_blk_mgr->get_data_block_info(cache_limit_blk_cnt, cache_data_blk_usage_pct);
  ASSERT_EQ(max_cache_data_blk_cnt, cache_limit_blk_cnt);
  ASSERT_LT(0, cache_data_blk_usage_pct);

  available_space = 0;
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_available_space_for_prewarm(available_space));
  const int64_t avaliable_pct = SS_LIMIT_PREWARM_SHRINK_PCT - cache_data_blk_usage_pct;
  ASSERT_EQ(available_space, max_cache_data_blk_cnt * micro_cache->phy_blk_size_ * avaliable_pct / 100);

  cache_stat.cache_load_.common_io_load_.write_load_.update_load_param(1, 100, 6 * 1024 * 1024);
  cache_stat.cache_load_.common_io_load_.write_load_.update_load_param(1, 200, 12 * 1024 * 1024);
  available_space = 0;
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_available_space_for_prewarm(available_space));
  // When micro cache size is small (e.g., 5% default), the calculated value may be negative,
  // but the actual implementation uses MAX(SS_CLOSE_EVICTION_DIFF_SIZE, calculated_value)
  const int64_t expected_available_space = MAX(SS_CLOSE_EVICTION_DIFF_SIZE,
    max_cache_data_blk_cnt * micro_cache->phy_blk_size_ * avaliable_pct / 100 - SS_CLOSE_EVICTION_DIFF_SIZE);
  ASSERT_EQ(available_space, expected_available_space);

  // 8. make arc work_limit=0
  release_cache_task.is_inited_ = true;
  release_cache_task.ori_interval_us_ = 10 * 1000; // 10ms
  arc_info.do_update_arc_limit(0, false/*need_update_limit*/);
  ob_usleep(1000 * 1000);
  ASSERT_EQ(0, arc_info.work_limit_);
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t MAX_TIMEOUT_US = 60 * 1000 * 1000L;
  while ((arc_info.seg_info_arr_[SS_ARC_T1].count() > 1 ||
          arc_info.seg_info_arr_[SS_ARC_T2].count() > 0) &&
          ObTimeUtility::current_time() - start_us <= MAX_TIMEOUT_US) {
    ob_usleep(1000);
  }
  // cuz still exist a not-persisted micro_meta which is in T1
  ASSERT_EQ(1, arc_info.seg_info_arr_[SS_ARC_T1].count());
  ASSERT_EQ(micro_size, arc_info.seg_info_arr_[SS_ARC_T1].size());
  ASSERT_EQ(0, arc_info.seg_info_arr_[SS_ARC_T2].count());
  ASSERT_EQ(0, arc_info.seg_info_arr_[SS_ARC_T2].size());

  allocator.clear();
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_stat.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_stat.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}