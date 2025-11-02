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
#include "lib/ob_errno.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_task_runner.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

struct ReorganSSMicroDataInfo
{
public:
  MacroBlockId macro_id_;
  ObArray<ObSSMicroBlockIndex> micro_index_arr_;
  int32_t get_micro_count() const { return micro_index_arr_.count(); }
  ReorganSSMicroDataInfo() {}
  TO_STRING_KV(K_(macro_id), K_(macro_id), "micro_cnt", get_micro_count());
};

class TestSSReorganizePhyBlock : public ::testing::Test
{
public:
  TestSSReorganizePhyBlock() {}
  virtual ~TestSSReorganizePhyBlock() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void add_batch_micro_block(const int64_t start_macro_id, const int64_t macro_cnt, const int64_t micro_cnt,
      int64_t micro_size, ObArray<ReorganSSMicroDataInfo> &micro_data_info_arr);
private:
  ObSSMicroCache *micro_cache_;
};

void TestSSReorganizePhyBlock::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSReorganizePhyBlock::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSReorganizePhyBlock::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 32)));
  micro_cache->start();
  micro_cache_ = micro_cache;
}

void TestSSReorganizePhyBlock::TearDown()
{
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  // clear micro_meta_ckpt
  ObSSMicroCacheSuperBlk new_super_blk(tenant_id, micro_cache->cache_file_size_, SS_PERSIST_MICRO_CKPT_SPLIT_CNT);
  ASSERT_EQ(OB_SUCCESS, micro_cache->phy_blk_mgr_.update_ss_super_block(new_super_blk));
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  micro_cache_ = nullptr;
}

void TestSSReorganizePhyBlock::add_batch_micro_block(
    const int64_t start_macro_id,
    const int64_t macro_cnt,
    const int64_t micro_cnt,
    int64_t micro_size,
    ObArray<ReorganSSMicroDataInfo> &micro_data_info_arr)
{
  ObArenaAllocator allocator;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;

  const int64_t payload_offset = ObSSMemBlock::get_reserved_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  if (micro_size == 0) {
    micro_size = (DEFAULT_BLOCK_SIZE - payload_offset) / micro_cnt - micro_index_size;
  }
  char *data_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  for (int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(start_macro_id + i);
    ReorganSSMicroDataInfo micro_data_info;
    micro_data_info.macro_id_ = macro_id;
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMemBlockHandle mem_blk_handle;
      uint32_t micro_crc = 0;
      bool alloc_new = false;
      ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.add_micro_block_data(micro_key, data_buf, micro_size, mem_blk_handle, micro_crc, alloc_new));
      ASSERT_EQ(true, mem_blk_handle.is_valid());
      bool first_add = false;
      const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
      ASSERT_EQ(OB_SUCCESS,
          micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc,
          effective_tablet_id, mem_blk_handle, first_add));
      ASSERT_EQ(true, first_add);
      ObSSMicroBlockIndex micro_index(micro_key, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_data_info.micro_index_arr_.push_back(micro_index));
    }
    ASSERT_EQ(OB_SUCCESS, micro_data_info_arr.push_back(micro_data_info));
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
}

// test the result of once reorganization task
TEST_F(TestSSReorganizePhyBlock, test_once_reorganization_result)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_once_reorganization_result");
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  ObSSReleaseCacheTask &release_cache_task = micro_cache_->task_runner_.release_cache_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache_->task_runner_.blk_ckpt_task_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  ASSERT_EQ(0, cache_stat.mem_stat().get_micro_alloc_cnt());
  const int32_t block_size = phy_blk_mgr.get_block_size();

  release_cache_task.is_inited_ = false;
  blk_ckpt_task.is_inited_ = false;
  usleep(1000 * 1000);
  const int64_t available_block_cnt = phy_blk_mgr.blk_cnt_info_.micro_data_blk_max_cnt();
  const int64_t write_blk_cnt = 100;
  ASSERT_LT(write_blk_cnt, available_block_cnt);

  // 1. write 100 fulfilled phy_block and persist them
  const int64_t micro_cnt = 20;
  const int64_t start_macro_id = 1;
  ObArray<ReorganSSMicroDataInfo> micro_data_info_arr;
  add_batch_micro_block(start_macro_id, write_blk_cnt, micro_cnt, 0, micro_data_info_arr);
  ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.inner_seal_and_alloc_mem_block(true /* is_fg */));
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task()); // 等待所有的sealed_mem_blk都持久化到磁盘
  ASSERT_EQ(write_blk_cnt, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);
  const int64_t ori_micro_pool_cnt = cache_stat.mem_stat().get_micro_alloc_cnt();

  // 2. evict 50% micro_block from some phy_blocks
  int32_t evict_pct = 50;
  const int64_t expected_sparse_blk_cnt = write_blk_cnt / 2;
  const int32_t evict_cnt = micro_cnt * evict_pct / 100;
  for (int64_t i = 0; i < expected_sparse_blk_cnt; ++i) {
    // const int32_t evict_cnt = micro_cnt * evict_pct[i / 10] / 100;
    MacroBlockId macro_id = micro_data_info_arr[i].macro_id_;
    for (int32_t j = 0; j < evict_cnt; ++j) {
      const int32_t micro_size = micro_data_info_arr.at(i).micro_index_arr_.at(j).size_;
      const ObSSMicroBlockCacheKey &micro_key = micro_data_info_arr.at(i).micro_index_arr_.at(j).micro_key_;
      ObSSMicroBlockMetaHandle micro_handle;
      ObSSMicroBlockMeta *micro_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&micro_key, micro_handle));
      micro_meta = micro_handle.get_ptr();
      ASSERT_EQ(true, micro_meta->is_in_l1_);
      ASSERT_EQ(false, micro_meta->is_in_ghost_);
      ASSERT_EQ(true, micro_meta->is_data_persisted_);
      micro_handle.reset();
      ASSERT_EQ(1, micro_meta->ref_cnt_);

      int64_t phy_blk_idx = -1;
      phy_blk_idx = micro_meta->get_phy_blk_idx(block_size);
      uint64_t cur_data_loc = micro_meta->data_loc_;
      uint64_t cur_reuse_version = micro_meta->reuse_version_;
      uint64_t cur_micro_size = micro_meta->size_;
      ASSERT_LT(1, phy_blk_idx);

      ObSSMicroBlockMetaInfo evict_micro_info;
      micro_meta->get_micro_meta_info(evict_micro_info);
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_evict_micro_block_meta(evict_micro_info));
      ASSERT_EQ(1, micro_meta->ref_cnt_);
      ASSERT_EQ(true, micro_meta->is_in_l1_);
      ASSERT_EQ(true, micro_meta->is_in_ghost_);
      ASSERT_EQ(false, micro_meta->is_valid_field());
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_block_valid_length(cur_data_loc, cur_reuse_version, cur_micro_size * -1, phy_blk_idx));
    }
  }
  ASSERT_EQ(ori_micro_pool_cnt, cache_stat.mem_stat().get_micro_alloc_cnt());
  ASSERT_EQ(expected_sparse_blk_cnt, phy_blk_mgr.get_sparse_block_cnt());
  const ObSSPhyBlockCountInfo &phy_blk_info = phy_blk_mgr.blk_cnt_info_;
  LOG_INFO("phy_blk info when start", K(phy_blk_info));
  const int64_t &fg_blk_max = micro_cache_->cache_stat_.mem_blk_stat_.mem_blk_fg_max_cnt_;
  const int64_t &fg_blk_used = micro_cache_->cache_stat_.mem_blk_stat_.mem_blk_fg_used_cnt_;
  const int64_t &bg_blk_max = micro_cache_->cache_stat_.mem_blk_stat_.mem_blk_bg_max_cnt_;
  const int64_t &bg_blk_used = micro_cache_->cache_stat_.mem_blk_stat_.mem_blk_bg_used_cnt_;
  LOG_INFO("mem_blk info before reorganzation", K(fg_blk_max), K(fg_blk_used), K(bg_blk_max), K(bg_blk_used));

  // 3. get some low usage phy_blocks to reorganize.
  const int64_t candidate_cnt = expected_sparse_blk_cnt;
  ObSSReorganizeMicroOp &reorgan_op = release_cache_task.reorganize_op_;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_batch_sparse_blocks(reorgan_op.candidate_phy_blks_, candidate_cnt));
  ASSERT_EQ(candidate_cnt, reorgan_op.candidate_phy_blks_.count());

  ObArray<int64_t> candidate_idx_arr;
  for (int64_t i = 0; i < candidate_cnt; ++i) {
    const ObSSPhyBlockHandle &phy_blk_handle = reorgan_op.candidate_phy_blks_.at(i).phy_blk_handle_;
    const int64_t phy_blk_idx = reorgan_op.candidate_phy_blks_.at(i).phy_blk_idx_;
    ASSERT_EQ(OB_SUCCESS, candidate_idx_arr.push_back(phy_blk_idx));
  }

  // 4. read and handle selectd phy_blocks to reorganize
  const int64_t start_us = ObTimeUtility::current_time_us();
  ASSERT_EQ(OB_SUCCESS, reorgan_op.read_sparse_phy_blocks());
  ASSERT_EQ(OB_SUCCESS, reorgan_op.handle_sparse_phy_blocks());
  ASSERT_EQ(OB_SUCCESS, reorgan_op.reaggregate_micro_blocks());
  const int64_t end_us = ObTimeUtility::current_time_us();
  LOG_INFO("phy_blk info when finish reorganize", K(phy_blk_info));
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

  // 5. check reorganization result, candidate phy_blocks were added into reusable_set.
  ASSERT_EQ(candidate_cnt, phy_blk_mgr.reusable_blks_.size());
  LOG_INFO("phy_blk info when finish persistance", K(phy_blk_info));
  LOG_INFO("mem_blk info when finish persistance", K(fg_blk_max), K(fg_blk_used), K(bg_blk_max), K(bg_blk_used));
  ObArray<int64_t> reusable_idx_arr;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_batch_reusable_blocks(reusable_idx_arr, candidate_cnt));
  ASSERT_EQ(candidate_cnt, reusable_idx_arr.count());
  for (int64_t i = 0; i < candidate_cnt; ++i) {
    bool is_exist = false;
    for (int64_t j = 0; !is_exist && j < candidate_cnt; ++j) {
      if (reusable_idx_arr.at(i) == candidate_idx_arr.at(j)) {
        is_exist = true;
      }
    }
    ASSERT_EQ(true, is_exist);
  }
  const int64_t avg_reorganize_time_us = (end_us - start_us) / candidate_cnt;
  LOG_INFO("reorganization task time cost", K(candidate_cnt), K(avg_reorganize_time_us));

  LOG_INFO("TEST_CASE: finish test_once_reorganization_result");
}

/* This case tests the basic logic of the reorganize task. */
TEST_F(TestSSReorganizePhyBlock, test_reorganize_phy_block_task)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_reorganize_phy_block_task");
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  ObSSReleaseCacheTask &release_cache_task = micro_cache_->task_runner_.release_cache_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache_->task_runner_.blk_ckpt_task_;
  ObSSPersistMicroMetaTask &persist_meta_task = micro_cache_->task_runner_.persist_meta_task_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  ASSERT_EQ(0, cache_stat.mem_stat().get_micro_alloc_cnt());
  const int32_t block_size = phy_blk_mgr.get_block_size();

  release_cache_task.is_inited_ = false;
  blk_ckpt_task.is_inited_ = false;
  persist_meta_task.is_inited_ = false;
  usleep(1000 * 1000);
  const int64_t available_block_cnt = phy_blk_mgr.blk_cnt_info_.micro_data_blk_max_cnt();
  const int64_t WRITE_BLK_CNT = 50;
  ASSERT_LT(WRITE_BLK_CNT, available_block_cnt);

  // 1. write 50 fulfilled phy_block and persist them
  const int64_t micro_cnt = 20;
  const int64_t start_macro_id = 1;
  ObArray<ReorganSSMicroDataInfo> micro_data_info_arr;
  add_batch_micro_block(start_macro_id, WRITE_BLK_CNT, micro_cnt, 0, micro_data_info_arr);
  ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.inner_seal_and_alloc_mem_block(true /* is_fg */));
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  ASSERT_EQ(WRITE_BLK_CNT, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);
  const int64_t ori_micro_pool_cnt = cache_stat.mem_stat().get_micro_alloc_cnt();

  // 2. evict some micro_block, to decrease some phy_blocks' usage ratio.
  // idx0~idx9:    evict 0
  // idx10~idx19:  evict 10%
  // idx20~idx29:  evict 10%
  // idx30~idx39:  evict 45%
  // idx40~idx49:  evict 80%
  int32_t evict_pct[5] = {0, 10, 10, 45, 80};
  const int64_t expected_sparse_blk_cnt = 20;
  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    const int32_t evict_cnt = micro_cnt * evict_pct[i / 10] / 100;
    MacroBlockId macro_id = micro_data_info_arr[i].macro_id_;
    for (int32_t j = 0; j < evict_cnt; ++j) {
      const int32_t micro_size = micro_data_info_arr.at(i).micro_index_arr_.at(j).size_;
      const ObSSMicroBlockCacheKey &micro_key = micro_data_info_arr.at(i).micro_index_arr_.at(j).micro_key_;
      ObSSMicroBlockMetaHandle micro_handle;
      ObSSMicroBlockMeta *micro_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&micro_key, micro_handle));
      micro_meta = micro_handle.get_ptr();
      ASSERT_EQ(true, micro_meta->is_in_l1_);
      ASSERT_EQ(false, micro_meta->is_in_ghost_);
      ASSERT_EQ(true, micro_meta->is_data_persisted_);
      micro_handle.reset();
      ASSERT_EQ(1, micro_meta->ref_cnt_);

      int64_t phy_blk_idx = -1;
      phy_blk_idx = micro_meta->get_phy_blk_idx(block_size);
      uint64_t cur_data_loc = micro_meta->data_loc_;
      uint64_t cur_reuse_version = micro_meta->reuse_version_;
      uint64_t cur_micro_size = micro_meta->size_;
      ASSERT_LT(1, phy_blk_idx);

      ObSSMicroBlockMetaInfo evict_micro_info;
      micro_meta->get_micro_meta_info(evict_micro_info);
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_evict_micro_block_meta(evict_micro_info));
      ASSERT_EQ(1, micro_meta->ref_cnt_);
      ASSERT_EQ(true, micro_meta->is_in_l1_);
      ASSERT_EQ(true, micro_meta->is_in_ghost_);
      ASSERT_EQ(false, micro_meta->is_valid_field());
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_block_valid_length(cur_data_loc, cur_reuse_version, cur_micro_size * -1, phy_blk_idx));
    }
  }
  ASSERT_EQ(ori_micro_pool_cnt, cache_stat.mem_stat().get_micro_alloc_cnt());
  ASSERT_EQ(expected_sparse_blk_cnt, phy_blk_mgr.get_sparse_block_cnt());

  // 3. get some low usage phy_blocks to reorganize.
  const int64_t candidate_cnt = 10;
  ObSSReorganizeMicroOp &reorgan_op = release_cache_task.reorganize_op_;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_batch_sparse_blocks(reorgan_op.candidate_phy_blks_, candidate_cnt));
  ASSERT_EQ(candidate_cnt, reorgan_op.candidate_phy_blks_.count());

  ObArray<int64_t> candidate_idx_arr;
  for (int64_t i = 0; i < candidate_cnt; ++i) {
    const ObSSPhyBlockHandle &phy_blk_handle = reorgan_op.candidate_phy_blks_.at(i).phy_blk_handle_;
    const int64_t phy_blk_idx = reorgan_op.candidate_phy_blks_.at(i).phy_blk_idx_;
    ASSERT_EQ(OB_SUCCESS, candidate_idx_arr.push_back(phy_blk_idx));
  }

  // 4. read and handle selectd phy_blocks, set is_reorganizing from false to true
  ASSERT_EQ(OB_SUCCESS, reorgan_op.read_sparse_phy_blocks());
  ASSERT_EQ(OB_SUCCESS, reorgan_op.handle_sparse_phy_blocks());

  // 5. reaggregate micro_blocks
  ASSERT_EQ(OB_SUCCESS, reorgan_op.reaggregate_micro_blocks());
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

  ObSSMicroReorganEntry &cur_entry = reorgan_op.candidate_micro_blks_.at(0);
  ASSERT_EQ(true, cur_entry.micro_meta_handle_.is_valid());
  ObSSMicroBlockMeta *cur_micro_meta = cur_entry.micro_meta_handle_.get_ptr();
  ASSERT_EQ(2, cur_micro_meta->ref_cnt_);

  // 6. check reaggregate result, 10 candidate phy_blocks were added into reusable_set.
  ASSERT_EQ(candidate_cnt, phy_blk_mgr.reusable_blks_.size());
  ObArray<int64_t> reusable_idx_arr;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_batch_reusable_blocks(reusable_idx_arr, candidate_cnt));
  ASSERT_EQ(candidate_cnt, reusable_idx_arr.count());
  for (int64_t i = 0; i < candidate_cnt; ++i) {
    bool is_exist = false;
    for (int64_t j = 0; !is_exist && j < candidate_cnt; ++j) {
      if (reusable_idx_arr.at(i) == candidate_idx_arr.at(j)) {
        is_exist = true;
      }
    }
    ASSERT_EQ(true, is_exist);
  }
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  reorgan_op.clear_for_next_round();
  ASSERT_EQ(1, cur_micro_meta->ref_cnt_);

  ASSERT_LT(0, candidate_idx_arr.count());
  for (int64_t i = 0; i < candidate_idx_arr.count(); ++i) {
    ObSSPhysicalBlock *tmp_phy_blk = phy_blk_mgr.inner_get_phy_block_by_idx(candidate_idx_arr.at(i));
    ASSERT_EQ(1, tmp_phy_blk->ref_cnt_);
  }

  /////////////////////////////////////// Reorganize Again ///////////////////////////////////////////
  LOG_INFO("TEST: Reorganize Again", K(cache_stat));
  // 1. check scan blocks to reorganize
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_batch_sparse_blocks(reorgan_op.candidate_phy_blks_, candidate_cnt));
  ASSERT_EQ(candidate_cnt, reorgan_op.candidate_phy_blks_.count());

  candidate_idx_arr.reset();
  // idx30~idx39, relative phy_blocks will be choosen
  for (int64_t i = 0; i < candidate_cnt; ++i) {
    const int64_t phy_blk_idx = reorgan_op.candidate_phy_blks_.at(i).phy_blk_idx_;
    ASSERT_EQ(OB_SUCCESS, candidate_idx_arr.push_back(phy_blk_idx));
  }

  // 2. mock one micro_meta abnormal, occur reuse_version mismatch
  int64_t tmp_idx = 34;
  const int32_t tmp_micro_key_idx = micro_data_info_arr.at(tmp_idx).get_micro_count() * evict_pct[tmp_idx / 10] / 100 + 1;
  ObSSMicroBlockCacheKey tmp_micro_key = micro_data_info_arr.at(tmp_idx).micro_index_arr_.at(tmp_micro_key_idx).micro_key_;
  ObSSMicroBlockMetaHandle tmp_micro_handle;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&tmp_micro_key, tmp_micro_handle));
  ObSSMicroBlockMeta *tmp_micro_meta = tmp_micro_handle.get_ptr();
  ASSERT_NE(nullptr, tmp_micro_meta);
  int64_t tmp_phy_blk_idx = tmp_idx + SS_SUPER_BLK_COUNT;
  ASSERT_EQ(tmp_phy_blk_idx, tmp_micro_meta->data_loc() / DEFAULT_BLOCK_SIZE);
  tmp_micro_meta->reuse_version_ += 1;
  {
    ObSSPhyBlockHandle blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_block_handle(tmp_phy_blk_idx, blk_handle));
    ASSERT_EQ(true, blk_handle.is_valid());
    ASSERT_LT(0, blk_handle.ptr_->get_valid_len());
  }

  // 3. read and handle selectd phy_blocks
  ASSERT_EQ(OB_SUCCESS, reorgan_op.read_sparse_phy_blocks());
  ASSERT_EQ(OB_SUCCESS, reorgan_op.handle_sparse_phy_blocks());

  // 4. reaggregate micro_blocks
  ASSERT_EQ(OB_SUCCESS, reorgan_op.reaggregate_micro_blocks());
  ObArray<ObSSPhyBlkReorganEntry> candidate_phy_blks;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_batch_sparse_blocks(candidate_phy_blks, candidate_cnt));
  ASSERT_EQ(1, candidate_phy_blks.count());
  ASSERT_EQ(1, phy_blk_mgr.get_sparse_block_cnt());
  // cuz micro_meta reuse_version mismatch, exist one phy_block fail to reorganize, so valid_len > 0
  {
    ObSSPhyBlockHandle blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_block_handle(tmp_phy_blk_idx, blk_handle));
    ASSERT_EQ(true, blk_handle.is_valid());
    ASSERT_LT(0, blk_handle.ptr_->get_valid_len());
  }
  reorgan_op.clear_for_next_round();
  LOG_INFO("TEST_CASE: finish test_reorganize_phy_block_task");
}

TEST_F(TestSSReorganizePhyBlock, test_estimate_reorgan_blk_cnt)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_estimate_reorgan_blk_cnt");
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  const int64_t max_reorgan_task_reserve_cnt = blk_cnt_info.reorgan_blk_cnt_;

  ASSERT_EQ(0, cache_stat.micro_stat().get_avg_micro_size());

  int64_t available_cnt = 0;
  phy_blk_mgr.reserve_reorganize_block(available_cnt);
  ASSERT_EQ(SS_MIN_REORGAN_BLK_CNT, available_cnt);

  // test small avg_micro_size
  int64_t start_macro_id = 1;
  int64_t scale_cnt = 2;
  int64_t micro_cnt = 1;
  const int64_t small_micro_size = SS_REORGAN_MIN_MICRO_SIZE + SS_REORGAN_BLK_SCALING_FACTOR * scale_cnt;
  ObArray<ReorganSSMicroDataInfo> micro_data_info_arr;
  add_batch_micro_block(start_macro_id, 1, micro_cnt, small_micro_size, micro_data_info_arr);
  phy_blk_mgr.reserve_reorganize_block(available_cnt);
  ASSERT_EQ(SS_MIN_REORGAN_BLK_CNT + scale_cnt, available_cnt);

  // test large avg_micro_size
  start_macro_id += micro_cnt;
  scale_cnt = 100;
  const int64_t large_micro_size = SS_REORGAN_MIN_MICRO_SIZE + SS_REORGAN_BLK_SCALING_FACTOR * scale_cnt;
  micro_cnt = DEFAULT_BLOCK_SIZE / large_micro_size;
  micro_data_info_arr.reuse();
  add_batch_micro_block(start_macro_id, 1, micro_cnt, large_micro_size, micro_data_info_arr);
  const int64_t max_micro_size =
      SS_REORGAN_MIN_MICRO_SIZE + (max_reorgan_task_reserve_cnt - SS_MIN_REORGAN_BLK_CNT) * SS_REORGAN_BLK_SCALING_FACTOR;
  ASSERT_LT(max_micro_size, cache_stat.micro_stat().get_avg_micro_size());
  phy_blk_mgr.reserve_reorganize_block(available_cnt);
  ASSERT_EQ(max_reorgan_task_reserve_cnt / 2, available_cnt);

  // test reserve blk for reorganize_task from shared_block
  blk_cnt_info.data_blk_.used_cnt_ = blk_cnt_info.data_blk_.hold_cnt_; // mock data_blk used up.
  cache_stat.phy_blk_stat().data_blk_used_cnt_ = blk_cnt_info.data_blk_.used_cnt_;
  ASSERT_EQ(0, blk_cnt_info.data_blk_.free_blk_cnt());

  phy_blk_mgr.reserve_reorganize_block(available_cnt);
  ASSERT_EQ(max_reorgan_task_reserve_cnt / 2, available_cnt);

  const int64_t data_blk_free_cnt = cache_stat.phy_blk_stat().data_blk_cnt_ - cache_stat.phy_blk_stat().data_blk_used_cnt_;
  ASSERT_EQ(SS_MAX_REORGAN_BLK_CNT / 2, blk_cnt_info.data_blk_.free_blk_cnt());
  ASSERT_EQ(SS_MAX_REORGAN_BLK_CNT / 2, data_blk_free_cnt);
  ASSERT_EQ(SS_MAX_REORGAN_BLK_CNT, max_reorgan_task_reserve_cnt);
  const int64_t shared_blk_used_cnt = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;
  ASSERT_EQ(shared_blk_used_cnt, blk_cnt_info.shared_blk_used_cnt_);
  ASSERT_EQ(shared_blk_used_cnt, cache_stat.phy_blk_stat().shared_blk_used_cnt_);
  LOG_INFO("TEST_CASE: finish test_estimate_reorgan_blk_cnt");
}

// Test two scenarios:
// 1. get_batch_sparse_blk() delete blocks from sparse_blk_map that don't meet reorganize requirements
// 2. After execute blk_ckpt, free_block() will delete sparse_blk from sparse_blk_map
TEST_F(TestSSReorganizePhyBlock, test_delete_block_from_sparse_blk_map)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_delete_block_from_sparse_blk_map");
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSARCInfo &arc_info = micro_cache_->micro_meta_mgr_.arc_info_;
  ObSSReleaseCacheTask &release_cache_task = micro_cache_->task_runner_.release_cache_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache_->task_runner_.blk_ckpt_task_;
  ObSSPersistMicroMetaTask &persist_meta_task = micro_cache_->task_runner_.persist_meta_task_;
  release_cache_task.evict_op_.is_enabled_ = false;
  release_cache_task.reorganize_op_.is_enabled_ = false;
  blk_ckpt_task.is_inited_ = false;
  persist_meta_task.is_inited_ = false;
  ob_usleep(1000 * 1000);

  int64_t start_macro_id = 1;
  int64_t macro_cnt = SS_MIN_REORGAN_BLK_CNT * 3;
  int64_t micro_cnt = 10000;
  const int64_t micro_size = DEFAULT_BLOCK_SIZE / micro_cnt;
  ObArray<ReorganSSMicroDataInfo> micro_data_info_arr;
  add_batch_micro_block(start_macro_id, macro_cnt, micro_cnt, micro_size, micro_data_info_arr);
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  int64_t data_blk_used_cnt = phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_;
  ASSERT_LT(SS_MIN_REORGAN_BLK_CNT, data_blk_used_cnt);

  // scenario 1
  ObSSPhyBlockHandle phy_blk_handle;
  const int64_t blk_idx = 2;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_block_handle(blk_idx, phy_blk_handle));
  ASSERT_LT(static_cast<int64_t>(SS_PHY_BLK_REORGAN_USAGE_PCT * DEFAULT_BLOCK_SIZE), phy_blk_handle()->valid_len_);
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_sparse_block(ObSSPhyBlockIdx(blk_idx)));
  ASSERT_EQ(1, phy_blk_mgr.sparse_blk_map_.count());
  ObArray<ObSSPhyBlkReorganEntry> candidate_phy_blks;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_batch_sparse_blocks(candidate_phy_blks, 10));
  ASSERT_EQ(0, phy_blk_mgr.sparse_blk_map_.count());


  // scenario 2
  int64_t new_arc_limit = micro_cnt * micro_size * 0.001;
  arc_info.do_update_arc_limit(new_arc_limit, /*need_update_limit*/false);
  release_cache_task.evict_op_.is_enabled_ = true;
  ob_usleep(5 * 1000 * 1000);
  ASSERT_EQ(data_blk_used_cnt, phy_blk_mgr.sparse_blk_map_.count());
  release_cache_task.reorganize_op_.is_enabled_ = true;
  blk_ckpt_task.cur_interval_us_ = 1000;
  blk_ckpt_task.is_inited_ = true;
  ob_usleep(5 * 1000 * 1000);
  ASSERT_LT(phy_blk_mgr.sparse_blk_map_.count(), SS_MIN_REORGAN_BLK_CNT);
  LOG_INFO("TEST_CASE: finish test_delete_block_from_sparse_blk_map");
}

// When one phy_block's micro_metas are being reorganized, but some of them are being evicted currently,
// that means, these micro_blocks will be written into mem_blk, but will not succ to update meta.
TEST_F(TestSSReorganizePhyBlock, test_evict_and_reorganize_parallel)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_evict_and_reorganize_parallel");
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  ObSSReleaseCacheTask &release_cache_task = micro_cache_->task_runner_.release_cache_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache_->task_runner_.blk_ckpt_task_;
  ObSSPersistMicroMetaTask &persist_meta_task = micro_cache_->task_runner_.persist_meta_task_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  const int32_t block_size = phy_blk_mgr.get_block_size();

  release_cache_task.is_inited_ = false;
  blk_ckpt_task.is_inited_ = false;
  persist_meta_task.is_inited_ = false;
  usleep(1000 * 1000);

  const int64_t available_block_cnt = phy_blk_mgr.blk_cnt_info_.micro_data_blk_max_cnt();
  const int64_t WRITE_BLK_CNT = 10;
  ASSERT_LT(WRITE_BLK_CNT, available_block_cnt);
  ASSERT_EQ(0, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);

  // 1. write 50 fulfilled phy_block and persist them
  const int64_t micro_cnt = 50;
  const int64_t start_macro_id = 1;
  ObArray<ReorganSSMicroDataInfo> micro_data_info_arr;
  add_batch_micro_block(start_macro_id, WRITE_BLK_CNT, micro_cnt, 0, micro_data_info_arr);
  ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.inner_seal_and_alloc_mem_block(true/*is_fg*/));
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  ASSERT_EQ(WRITE_BLK_CNT, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);

  // 2. evict micro_block, to decrease some phy_blocks' usage ratio.
  const int32_t evict_cnt = 15;
  int64_t start_evict_idx = 0;
  int64_t end_evict_idx = evict_cnt;
  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    MacroBlockId macro_id = micro_data_info_arr[i].macro_id_;
    for (int32_t j = start_evict_idx; j < end_evict_idx; ++j) {
      const int32_t micro_size = micro_data_info_arr.at(i).micro_index_arr_.at(j).size_;
      const ObSSMicroBlockCacheKey &micro_key = micro_data_info_arr.at(i).micro_index_arr_.at(j).micro_key_;
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&micro_key, micro_meta_handle));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      const int64_t phy_blk_idx = micro_meta_handle.get_ptr()->get_phy_blk_idx(block_size);
      ASSERT_EQ(true, micro_meta_handle.get_ptr()->is_valid(phy_blk_idx));

      ObSSMicroBlockMeta *micro_meta = micro_meta_handle.get_ptr();
      uint64_t cur_data_loc = micro_meta->data_loc_;
      uint64_t cur_reuse_version = micro_meta->reuse_version_;
      uint64_t cur_micro_size = micro_meta->size_;

      ObSSMicroBlockMetaInfo evict_micro_info;
      micro_meta->get_micro_meta_info(evict_micro_info);
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_evict_micro_block_meta(evict_micro_info));
      ASSERT_EQ(true, micro_meta->is_in_ghost_);
      ASSERT_EQ(false, micro_meta->is_valid_field());
      int64_t real_blk_idx = 0;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_block_valid_length(cur_data_loc, cur_reuse_version, cur_micro_size * -1, real_blk_idx));
      ASSERT_EQ(phy_blk_idx, real_blk_idx);
    }
  }
  ASSERT_EQ(WRITE_BLK_CNT, phy_blk_mgr.get_sparse_block_cnt());
  ASSERT_EQ(WRITE_BLK_CNT * (micro_cnt - evict_cnt), cache_stat.micro_stat().valid_micro_cnt_);

  // 3. start reorganize_micro_op
  // 3.1 get sparse phy_blocks
  ObSSReorganizeMicroOp &reorgan_op = release_cache_task.reorganize_op_;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_batch_sparse_blocks(reorgan_op.candidate_phy_blks_, WRITE_BLK_CNT));
  ASSERT_EQ(WRITE_BLK_CNT, reorgan_op.candidate_phy_blks_.count());

  // 3.2 read phy_blocks' data
  ASSERT_EQ(OB_SUCCESS, reorgan_op.read_sparse_phy_blocks());

  // 3.3 select micro_block to participate in reorganize_op
  ASSERT_EQ(OB_SUCCESS, reorgan_op.handle_sparse_phy_blocks());
  ASSERT_EQ(cache_stat.micro_stat().valid_micro_cnt_, reorgan_op.reorgan_ctx_.candidate_micro_cnt_);

  // 3.4 continue to evict micro_blocks
  start_evict_idx = evict_cnt + 5;
  end_evict_idx = evict_cnt * 2 + 5;
  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    MacroBlockId macro_id = micro_data_info_arr[i].macro_id_;
    for (int32_t j = start_evict_idx; j < end_evict_idx; ++j) {
      const int32_t micro_size = micro_data_info_arr.at(i).micro_index_arr_.at(j).size_;
      const ObSSMicroBlockCacheKey &micro_key = micro_data_info_arr.at(i).micro_index_arr_.at(j).micro_key_;
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&micro_key, micro_meta_handle));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      const int64_t phy_blk_idx = micro_meta_handle.get_ptr()->get_phy_blk_idx(block_size);
      ASSERT_EQ(true, micro_meta_handle.get_ptr()->is_valid(phy_blk_idx));

      ObSSMicroBlockMeta *micro_meta = micro_meta_handle.get_ptr();
      uint64_t cur_data_loc = micro_meta->data_loc_;
      uint64_t cur_reuse_version = micro_meta->reuse_version_;
      uint64_t cur_micro_size = micro_meta->size_;

      ObSSMicroBlockMetaInfo evict_micro_info;
      micro_meta->get_micro_meta_info(evict_micro_info);
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_evict_micro_block_meta(evict_micro_info));
      ASSERT_EQ(true, micro_meta->is_in_ghost_);
      ASSERT_EQ(false, micro_meta->is_valid_field());
      int64_t real_blk_idx = 0;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_block_valid_length(cur_data_loc, cur_reuse_version, cur_micro_size * -1, real_blk_idx));
      ASSERT_EQ(phy_blk_idx, real_blk_idx);
    }
  }

  // 3.5 handle reorganized micro_blocks
  {
    ObArray<ObSSReorganizeMicroOp::SSCandidateMicroInfo> micro_info_arr;
    const int64_t candidate_micro_cnt = reorgan_op.candidate_micro_blks_.count();
    for (int64_t i = 0; i < candidate_micro_cnt; ++i) {
      const ObSSMicroReorganEntry &micro_reorgan_entry = reorgan_op.candidate_micro_blks_.at(i);
      ObTabletID tablet_id = micro_reorgan_entry.micro_key().get_macro_tablet_id();
      ObLSID ls_id(1);
      const bool is_in_l1 = micro_reorgan_entry.is_in_l1();
      ObSSAggregateMicroInfo aggr_info(tablet_id, ls_id, is_in_l1);
      ObSSReorganizeMicroOp::SSCandidateMicroInfo candidate_micro_info(i, aggr_info);
      ASSERT_EQ(OB_SUCCESS, micro_info_arr.push_back(candidate_micro_info));
    }

    if (micro_info_arr.count() > 0) {
      std::sort(&micro_info_arr.at(0), &micro_info_arr.at(0) + micro_info_arr.count(), ObSSReorganizeMicroOp::SSCandidateMicroInfoCmp());
      ASSERT_EQ(OB_SUCCESS, reorgan_op.write_micro_blocks(micro_info_arr));
      cache_stat.task_stat().update_reorgan_cnt(1);
    }
  }
  ASSERT_EQ(WRITE_BLK_CNT, phy_blk_mgr.get_reusable_blocks_cnt());
  ASSERT_EQ(WRITE_BLK_CNT * (micro_cnt - evict_cnt * 2), cache_stat.micro_stat().valid_micro_cnt_);
  LOG_INFO("TEST_CASE: finish test_evict_and_reorganize_parallel");
}

TEST_F(TestSSReorganizePhyBlock, test_reorganize_all_phy_blk)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_reorganize_all_phy_blk");
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  ObSSReleaseCacheTask &release_cache_task = micro_cache_->task_runner_.release_cache_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache_->task_runner_.blk_ckpt_task_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  const int32_t block_size = phy_blk_mgr.get_block_size();

  TestSSCommonUtil::stop_all_bg_task(micro_cache_);
  micro_cache_->task_runner_.persist_data_task_.is_inited_ = true;
  ob_usleep(1000 * 1000);

  const int64_t data_blk_cnt = phy_blk_mgr.blk_cnt_info_.micro_data_blk_max_cnt();
  ASSERT_LT(0, data_blk_cnt);
  ASSERT_EQ(0, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);

  // 1. fulfill all data_blk
  const int64_t micro_cnt = 50;
  const int64_t start_macro_id = 1;
  ObArray<ReorganSSMicroDataInfo> micro_data_info_arr;
  add_batch_micro_block(start_macro_id, data_blk_cnt, micro_cnt, 0, micro_data_info_arr);
  ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.inner_seal_and_alloc_mem_block(true/*is_fg*/));
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  ASSERT_EQ(data_blk_cnt, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);

  // 2. mock all reorgan_blk were used up
  phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_ += phy_blk_mgr.blk_cnt_info_.reorgan_blk_cnt_;
  {
    int64_t blk_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ASSERT_NE(OB_SUCCESS, phy_blk_mgr.alloc_block(blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_REORGAN_BLK));
  }
  ASSERT_EQ(false, phy_blk_mgr.blk_cnt_info_.has_free_blk(ObSSPhyBlockType::SS_MICRO_DATA_BLK));
  ASSERT_EQ(false, phy_blk_mgr.blk_cnt_info_.has_free_blk(ObSSPhyBlockType::SS_REORGAN_BLK));

  // 3. evict all phy_blk's most micro_block data
  for (int64_t i = 0; i < data_blk_cnt; ++i) {
    MacroBlockId macro_id = micro_data_info_arr[i].macro_id_;
    for (int32_t j = 0; j < micro_cnt / 2; ++j) {
      const int32_t micro_size = micro_data_info_arr.at(i).micro_index_arr_.at(j).size_;
      const ObSSMicroBlockCacheKey &micro_key = micro_data_info_arr.at(i).micro_index_arr_.at(j).micro_key_;
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&micro_key, micro_meta_handle));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      const int64_t phy_blk_idx = micro_meta_handle.get_ptr()->get_phy_blk_idx(block_size);
      ASSERT_EQ(true, micro_meta_handle.get_ptr()->is_valid(phy_blk_idx));

      ObSSMicroBlockMeta *micro_meta = micro_meta_handle.get_ptr();
      uint64_t cur_data_loc = micro_meta->data_loc_;
      uint64_t cur_reuse_version = micro_meta->reuse_version_;
      uint64_t cur_micro_size = micro_meta->size_;

      ObSSMicroBlockMetaInfo evict_micro_info;
      micro_meta->get_micro_meta_info(evict_micro_info);
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_evict_micro_block_meta(evict_micro_info));
      ASSERT_EQ(true, micro_meta->is_in_ghost_);
      ASSERT_EQ(false, micro_meta->is_valid_field());
      int64_t real_blk_idx = 0;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_block_valid_length(cur_data_loc, cur_reuse_version, cur_micro_size * -1, real_blk_idx));
      ASSERT_EQ(phy_blk_idx, real_blk_idx);
    }
  }

  // 4. execute reorganize_op, all reorganized micro_block data will be saved into mem_block
  ASSERT_EQ(0, phy_blk_mgr.get_reusable_blocks_cnt());
  ASSERT_EQ(OB_SUCCESS, micro_cache_->task_runner_.release_cache_task_.reorganize_op_.execute_reorganization());
  const int64_t cur_reusable_blk_cnt = phy_blk_mgr.get_reusable_blocks_cnt();
  ASSERT_LT(0, cur_reusable_blk_cnt);

  // 5. execute blk_checkpoint, to free reusable_blocks
  ASSERT_EQ(OB_SUCCESS, micro_cache_->task_runner_.blk_ckpt_task_.ckpt_op_.execute_checkpoint());
  ASSERT_EQ(0, phy_blk_mgr.get_reusable_blocks_cnt());
  ASSERT_EQ(true, phy_blk_mgr.blk_cnt_info_.has_free_blk(ObSSPhyBlockType::SS_REORGAN_BLK));
  LOG_INFO("TEST_CASE: finish test_reorganize_all_phy_blk", K(phy_blk_mgr.blk_cnt_info_));
}

// TODO @donglou.zl fix this case
// TEST_F(TestSSReorganizePhyBlock, test_reorganize_all_phy_blk)
// {
//   int ret = OB_SUCCESS;
//   LOG_INFO("TEST_CASE: start test_reorganize_all_phy_blk");
//   ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
//   ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
//   ObSSMemDataManager &mem_blk_mgr = micro_cache->mem_data_mgr_;
//   ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
//   ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
//   const int32_t block_size = phy_blk_mgr.get_block_size();

//   TestSSCommonUtil::stop_all_bg_task(micro_cache);
//   micro_cache->task_runner_.persist_task_.is_inited_ = true;
//   ob_usleep(1000 * 1000);

//   const int64_t data_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_data_blk_max_cnt();
//   ASSERT_LT(0, data_blk_cnt);
//   ASSERT_EQ(0, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);

//   // 1. fulfill all data_blk
//   const int64_t micro_cnt = 50;
//   const int64_t start_macro_id = 1;
//   ObArray<ReorganSSMicroDataInfo> micro_data_info_arr;
//   add_batch_micro_block(start_macro_id, data_blk_cnt, micro_cnt, 0, micro_data_info_arr);
//   ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.inner_seal_and_alloc_mem_block(true/*is_fg*/));
//   ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
//   ASSERT_EQ(data_blk_cnt, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);

//   // 2. evict all phy_blk's most micro_block data
//   for (int64_t i = 0; i < data_blk_cnt; ++i) {
//     MacroBlockId macro_id = micro_data_info_arr[i].macro_id_;
//     for (int32_t j = 0; j < micro_cnt / 2; ++j) {
//       const int32_t micro_size = micro_data_info_arr.at(i).micro_index_arr_.at(j).size_;
//       const ObSSMicroBlockCacheKey &micro_key = micro_data_info_arr.at(i).micro_index_arr_.at(j).micro_key_;
//       ObSSMicroBlockMetaHandle micro_meta_handle;
//       ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&micro_key, micro_meta_handle));
//       ASSERT_EQ(true, micro_meta_handle.is_valid());
//       const int64_t phy_blk_idx = micro_meta_handle.get_ptr()->get_phy_blk_idx(block_size);
//       ASSERT_EQ(true, micro_meta_handle.get_ptr()->is_valid(phy_blk_idx));

//       ObSSMicroBlockMeta *micro_meta = micro_meta_handle.get_ptr();
//       uint64_t cur_data_loc = micro_meta->data_loc_;
//       uint64_t cur_reuse_version = micro_meta->reuse_version_;
//       uint64_t cur_micro_size = micro_meta->size_;

//       ObSSMicroBlockMetaInfo evict_micro_info;
//       micro_meta->get_micro_meta_info(evict_micro_info);
//       ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_evict_micro_block_meta(evict_micro_info));
//       ASSERT_EQ(true, micro_meta->is_in_ghost_);
//       ASSERT_EQ(false, micro_meta->is_valid_field());
//       int64_t real_blk_idx = 0;
//       ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_block_valid_length(cur_data_loc, cur_reuse_version, cur_micro_size * -1, real_blk_idx));
//       ASSERT_EQ(phy_blk_idx, real_blk_idx);
//     }
//   }

//   // 3. mock all reorgan_blk were used up
//   phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_ += phy_blk_mgr.blk_cnt_info_.reorgan_blk_cnt_;
//   {
//     int64_t blk_idx = -1;
//     ObSSPhyBlockHandle phy_blk_handle;
//     ASSERT_NE(OB_SUCCESS, phy_blk_mgr.alloc_block(blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_REORGAN_BLK));
//   }
//   ASSERT_EQ(false, phy_blk_mgr.blk_cnt_info_.has_free_blk(ObSSPhyBlockType::SS_CACHE_DATA_BLK));
//   ASSERT_EQ(false, phy_blk_mgr.blk_cnt_info_.has_free_blk(ObSSPhyBlockType::SS_REORGAN_BLK));

//   // 4. execute reorganize_op, all reorganized micro_block data will be saved into mem_block
//   ASSERT_EQ(0, phy_blk_mgr.get_reusable_blocks_cnt());
//   ASSERT_EQ(OB_SUCCESS, micro_cache->task_runner_.release_cache_task_.reorganize_op_.execute_reorganization());
//   ASSERT_LT(0, mem_blk_mgr.sealed_bg_mem_blks_.get_curr_total());
//   const int64_t cur_reusable_blk_cnt = phy_blk_mgr.get_reusable_blocks_cnt();
//   ASSERT_LT(0, cur_reusable_blk_cnt);

//   // 5. execute blk_checkpoint, to free reusable_blocks
//   ASSERT_EQ(OB_SUCCESS, micro_cache->task_runner_.blk_ckpt_task_.ckpt_op_.execute_checkpoint());
//   ASSERT_EQ(0, phy_blk_mgr.get_reusable_blocks_cnt());
//   ASSERT_EQ(true, phy_blk_mgr.blk_cnt_info_.has_free_blk(ObSSPhyBlockType::SS_REORGAN_BLK));
//   LOG_INFO("TEST_CASE: finish test_reorganize_all_phy_blk", K(phy_blk_mgr.blk_cnt_info_));
// }

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_reorganize_phy_block.log*");
  OB_LOGGER.set_file_name("test_ss_reorganize_phy_block.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}