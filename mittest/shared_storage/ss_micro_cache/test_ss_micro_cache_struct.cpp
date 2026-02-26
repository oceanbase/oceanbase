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
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"

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
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache *)->cache_stat_;
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObSSPhyBlockHandle phy_blk_handle;
  const int64_t phy_blk_idx = 2;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_block_handle(phy_blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  int64_t reuse_version = phy_blk_handle.ptr_->reuse_version_;

  ObSSMicroBlockMeta *cur_micro_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(cur_micro_meta));
  ASSERT_NE(nullptr, cur_micro_meta);
  ASSERT_EQ(1, cache_stat.mem_stat().micro_alloc_cnt_);

  ObSSMicroBlockMeta &micro_meta = *(cur_micro_meta);
  const int32_t micro_size = 20;
  micro_meta.reuse_version_ = reuse_version;
  micro_meta.data_loc_ = phy_blk_idx * phy_blk_mgr.block_size_;
  micro_meta.size_ = micro_size;
  micro_meta.is_data_persisted_ = true;
  micro_meta.access_time_s_ = 1;
  ASSERT_EQ(0, micro_meta.ref_cnt_);
  ASSERT_EQ(false, micro_meta.is_valid_field()); // cuz no exist valid micro_key
  MacroBlockId macro_id(0, 100, 0);
  int32_t offset = 1;
  ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
  micro_meta.micro_key_ = micro_key;
  ASSERT_EQ(true, micro_meta.is_valid_field());
  ASSERT_EQ(true, micro_meta.is_valid());

  micro_meta.inc_ref_count();
  ASSERT_EQ(1, micro_meta.ref_cnt_);

  ObSSMicroBlockMetaHandle micro_handle;
  micro_handle.set_ptr(cur_micro_meta);
  ASSERT_EQ(2, micro_meta.ref_cnt_);
  micro_handle.reset();
  micro_meta.dec_ref_count();
  ASSERT_EQ(0, cache_stat.mem_stat().micro_alloc_cnt_);
}

TEST_F(TestSSMicroCacheStruct, micro_block_meta_2)
{
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  const int64_t fixed_cnt = 100;
  ObSSMicroBlockMeta *micro_meta = nullptr;
  ObArray<ObSSMicroBlockMeta *> micro_meta_arr;
  for (int64_t i = 0; i < fixed_cnt; ++i) {
    ASSERT_EQ(i, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
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
  ASSERT_EQ(fixed_cnt + 1, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());

  micro_meta->inc_ref_count();
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta->ref_cnt_);
  micro_meta->dec_ref_count();
  ASSERT_EQ(ori_micro_ref_cnt, micro_meta->ref_cnt_);
  ASSERT_EQ(fixed_cnt, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
  micro_meta = nullptr;

  micro_meta = micro_meta_arr.at(0);
  ASSERT_EQ(ori_micro_ref_cnt, micro_meta->ref_cnt_);
  micro_meta->inc_ref_count();
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta->ref_cnt_);
  micro_meta->inc_ref_count();
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta->ref_cnt_);
  micro_meta->try_free();
  ASSERT_EQ(fixed_cnt, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta->ref_cnt_);
  micro_meta->dec_ref_count();
  ASSERT_EQ(fixed_cnt - 1, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
  micro_meta = nullptr;

  for (int64_t i = 1; i < fixed_cnt; ++i) {
    ASSERT_EQ(ori_micro_ref_cnt, micro_meta_arr.at(i)->ref_cnt_);
    micro_meta_arr.at(i)->free();
  }
  ASSERT_EQ(0, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
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
  micro_meta.size_ = micro_size;
  micro_meta.access_time_s_ = 20;
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
  ObSSMemBlockPool &mem_blk_pool = MTL(ObSSMicroCache *)->mem_blk_mgr_.mem_blk_pool_;
  ObSSMemBlock *mem_blk_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_blk_ptr, true/*is_fg*/));
  ObSSMemBlockHandle mem_blk_handle;
  mem_blk_handle.set_ptr(mem_blk_ptr);
  ASSERT_EQ(true, mem_blk_handle.is_valid());

  uint32_t crc = 1;
  const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
  ASSERT_EQ(OB_INVALID_ARGUMENT, micro_meta.init(micro_key, mem_blk_handle, 0, crc, effective_tablet_id));
  ASSERT_EQ(OB_SUCCESS, micro_meta.init(micro_key, mem_blk_handle, 100, crc, effective_tablet_id));
  ASSERT_EQ(true, micro_meta.is_valid_field());
  ASSERT_EQ(false, micro_meta.is_valid());
  ObSSMemBlock::ObSSMemMicroInfo mem_info(offset, 100);
  ASSERT_EQ(OB_SUCCESS, mem_blk_handle()->micro_loc_map_.set_refactored(micro_key, mem_info));
  ASSERT_EQ(mem_blk_handle.get_ptr(), micro_meta.get_mem_block());

  micro_meta_handle.reset();
  ASSERT_EQ(0, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
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

  ASSERT_EQ(1, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
  micro_meta_handle.reset();
  tmp_micro_handle.reset();
  ASSERT_EQ(0, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
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
    ASSERT_EQ(i + 1, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
  }

  for (int64_t i = fixed_cnt; i < total_cnt; ++i) {
    micro_handle_arr.at(i).get_ptr()->try_free();
    ASSERT_EQ(total_cnt - (i - fixed_cnt + 1), SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
    micro_handle_arr.at(i).reset();
  }
  for (int64_t i = 0; i < fixed_cnt; ++i) {
    micro_handle_arr.at(i).get_ptr()->try_free();
    micro_handle_arr.at(i).reset();
  }
  ASSERT_EQ(0, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
}

TEST_F(TestSSMicroCacheStruct, arc_iter_info)
{
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  const int32_t micro_size = 50;
  ObSSARCIterInfo arc_iter_info;
  arc_iter_info.init(MTL_ID());
  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    ASSERT_EQ(false, arc_iter_info.need_handle_arc_seg(i));
    ASSERT_EQ(false, arc_iter_info.need_iterate_cold_micro(i));
    ASSERT_EQ(false, arc_iter_info.need_handle_cold_micro(i));
  }

  arc_iter_info.iter_seg_arr_[SS_ARC_T1].seg_cnt_ = 500;
  arc_iter_info.iter_seg_arr_[SS_ARC_T1].op_info_.to_delete_ = false;
  arc_iter_info.iter_seg_arr_[SS_ARC_T1].op_info_.op_cnt_ = 100;
  arc_iter_info.iter_seg_arr_[SS_ARC_B1].seg_cnt_ = 500;
  arc_iter_info.iter_seg_arr_[SS_ARC_B1].op_info_.to_delete_ = true;
  arc_iter_info.iter_seg_arr_[SS_ARC_B1].op_info_.op_cnt_ = 500;
  arc_iter_info.iter_seg_arr_[SS_ARC_T2].seg_cnt_ = 500;
  arc_iter_info.iter_seg_arr_[SS_ARC_T2].op_info_.to_delete_ = false;
  arc_iter_info.iter_seg_arr_[SS_ARC_T2].op_info_.op_cnt_ = 200;
  arc_iter_info.iter_seg_arr_[SS_ARC_B2].seg_cnt_ = 500;
  arc_iter_info.iter_seg_arr_[SS_ARC_B2].op_info_.to_delete_ = true;
  arc_iter_info.iter_seg_arr_[SS_ARC_B2].op_info_.op_cnt_ = 600;
  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(i));
    arc_iter_info.adjust_arc_iter_seg_info(i);
    ASSERT_GE(SS_ARC_HANDLE_OP_MAX_COUNT, arc_iter_info.iter_seg_arr_[i].op_info_.op_cnt_);
    ASSERT_LT(0, arc_iter_info.iter_seg_arr_[i].op_info_.op_cnt_);
    ASSERT_GE(SS_ARC_HANDLE_OP_MAX_COUNT, arc_iter_info.iter_seg_arr_[i].op_info_.exp_iter_cnt_);
    ASSERT_LT(0, arc_iter_info.iter_seg_arr_[i].op_info_.exp_iter_cnt_);
  }

  ObSSMicroBlockMeta *micro_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(micro_meta));
  ASSERT_NE(nullptr, micro_meta);
  ObSSMicroBlockMetaHandle micro_handle;
  micro_handle.set_ptr(micro_meta);
  const int64_t seg_cnt = 500;
  const int64_t total_seg_cnt = seg_cnt * SS_ARC_SEG_COUNT;
  for (int64_t i = 1; i <= total_seg_cnt; ++i) {
    blocksstable::MacroBlockId macro_id(0, 8000 + i, 0);
    int64_t offset = i;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);

    micro_meta->first_val_ = 100;
    micro_meta->size_ = micro_size;
    micro_meta->micro_key_ = micro_key;
    micro_meta->access_time_s_ = i;
    int64_t seg_idx = -1;

    if (i % 4 == 0) {
      micro_meta->is_in_l1_ = false;
      micro_meta->is_in_ghost_ = false;
      seg_idx = SS_ARC_T2;
    } else if (i % 4 == 1) {
      micro_meta->is_in_l1_ = true;
      micro_meta->is_in_ghost_ = false;
      seg_idx = SS_ARC_T1;
    } else if (i % 4 == 2) {
      micro_meta->is_in_l1_ = true;
      micro_meta->is_in_ghost_ = true;
      seg_idx = SS_ARC_B1;
    } else {
      micro_meta->is_in_l1_ = false;
      micro_meta->is_in_ghost_ = true;
      seg_idx = SS_ARC_B2;
    }

    if (arc_iter_info.need_iterate_cold_micro(seg_idx)) {
      ObSSMicroBlockMetaInfo tmp_cold_micro;
      micro_meta->get_micro_meta_info(tmp_cold_micro);
      ASSERT_EQ(OB_SUCCESS, arc_iter_info.push_cold_micro(micro_key, tmp_cold_micro, seg_idx));
      ObSSMicroBlockMetaInfo cold_micro;
      ASSERT_EQ(OB_SUCCESS, arc_iter_info.get_cold_micro(micro_key, cold_micro));
      ASSERT_EQ(micro_meta->first_val_, cold_micro.first_val_);
      ASSERT_EQ(micro_meta->second_val_, cold_micro.second_val_);
      ASSERT_EQ(micro_meta->crc(), cold_micro.crc_);
      ASSERT_EQ(micro_meta->get_micro_key(), cold_micro.get_micro_key());
    }
  }

  ASSERT_EQ(SS_ARC_HANDLE_OP_MAX_COUNT, arc_iter_info.iter_seg_arr_[SS_ARC_T1].op_info_.obtained_cnt_);
  ASSERT_EQ(SS_ARC_HANDLE_OP_MAX_COUNT, arc_iter_info.iter_seg_arr_[SS_ARC_T2].op_info_.obtained_cnt_);
  ASSERT_EQ(SS_ARC_HANDLE_OP_MAX_COUNT, arc_iter_info.iter_seg_arr_[SS_ARC_B1].op_info_.obtained_cnt_);
  ASSERT_EQ(SS_ARC_HANDLE_OP_MAX_COUNT, arc_iter_info.iter_seg_arr_[SS_ARC_B2].op_info_.obtained_cnt_);
  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    ASSERT_EQ(false, arc_iter_info.need_iterate_cold_micro(i));
    ASSERT_EQ(true, arc_iter_info.need_handle_cold_micro(i));
  }

  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    while (arc_iter_info.need_handle_cold_micro(i)) {
      ObSSMicroBlockMetaInfo cold_micro;
      ASSERT_EQ(OB_SUCCESS, arc_iter_info.pop_cold_micro(i, cold_micro));
      ASSERT_EQ(OB_SUCCESS, arc_iter_info.finish_handle_cold_micro(i));
    }
  }

  micro_handle.reset();
  arc_iter_info.destroy();
  ASSERT_EQ(0, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
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
  ObArray<ObSSMicroReorganEntry> entry_arr;
  for (int64_t i = 0; i < micro_cnt; ++i) {
    ObSSMicroBlockMeta *micro_meta = nullptr;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(micro_meta));
    ASSERT_NE(nullptr, micro_meta);
    micro_meta->reset();
    ASSERT_EQ(ori_micro_ref_cnt, micro_meta->ref_cnt_);
    ASSERT_EQ(OB_SUCCESS, micro_meta_arr.push_back(micro_meta));

    micro_meta->data_loc_ = 101;
    micro_meta->size_ = 51;
    micro_meta->access_time_s_ = 3;
    micro_meta->crc_ = 35;
    ObSSMicroReorganEntry entry;
    entry.idx_ = i + 1;
    entry.data_ptr_ = buf;
    entry.micro_meta_handle_.set_ptr(micro_meta);
    entry.exp_data_loc_ = micro_meta->data_loc_;
    entry.exp_micro_size_ = micro_meta->size_;
    entry.exp_crc_ = micro_meta->crc_;
    ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta->ref_cnt_);
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
  ASSERT_EQ(0, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());

  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObArray<ObSSPhyBlkReorganEntry> blk_entry_arr;
  {
    int64_t phy_blk_idx = 3;
    ObSSPhysicalBlock *phy_blk = phy_blk_mgr.inner_get_phy_block_by_idx(phy_blk_idx);
    uint64_t reuse_version = phy_blk->get_reuse_version();
    ASSERT_EQ(1, phy_blk->ref_cnt_);
    ObSSPhyBlkReorganEntry reorgan_entry1(phy_blk_idx, phy_blk, reuse_version);
    ASSERT_EQ(2, phy_blk->ref_cnt_);
    ASSERT_EQ(OB_SUCCESS, blk_entry_arr.push_back(reorgan_entry1));
    ASSERT_EQ(3, phy_blk->ref_cnt_);

    phy_blk_idx = 4;
    phy_blk = phy_blk_mgr.inner_get_phy_block_by_idx(phy_blk_idx);
    reuse_version = phy_blk->get_reuse_version();
    ASSERT_EQ(1, phy_blk->ref_cnt_);
    ObSSPhyBlkReorganEntry reorgan_entry2(phy_blk_idx, phy_blk, reuse_version);
    ASSERT_EQ(2, phy_blk->ref_cnt_);
    ASSERT_EQ(OB_SUCCESS, blk_entry_arr.push_back(reorgan_entry2));
    ASSERT_EQ(3, phy_blk->ref_cnt_);

    ObSSPhyBlkReorganEntry &cur_entry = blk_entry_arr.at(0);
    ASSERT_EQ(3, cur_entry.phy_blk_handle_.get_ptr()->ref_cnt_);
  }
  ASSERT_EQ(2, blk_entry_arr.count());
  ASSERT_EQ(true, blk_entry_arr.at(0).is_valid());
  ASSERT_EQ(true, blk_entry_arr.at(1).is_valid());
  ASSERT_EQ(2, blk_entry_arr.at(0).phy_blk_handle_.get_ptr()->ref_cnt_);
  ASSERT_EQ(2, blk_entry_arr.at(1).phy_blk_handle_.get_ptr()->ref_cnt_);
  ObSSPhysicalBlock *phy_blk1 = blk_entry_arr.at(0).phy_blk_handle_.get_ptr();
  ObSSPhysicalBlock *phy_blk2 = blk_entry_arr.at(1).phy_blk_handle_.get_ptr();
  blk_entry_arr.reset();
  ASSERT_NE(nullptr, phy_blk1);
  ASSERT_NE(nullptr, phy_blk2);
  ASSERT_EQ(1, phy_blk1->ref_cnt_);
  ASSERT_EQ(1, phy_blk2->ref_cnt_);
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