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
#include "test_ss_common_util.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_runner.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

struct TestReorganSSMicroDataInfo : public TestSSMicroDataInfo
{
public:
  int32_t evict_size_;
  TestReorganSSMicroDataInfo() { reset(); }
  void reset() { TestSSMicroDataInfo::reset(); evict_size_ = 0; }

  INHERIT_TO_STRING_KV("micro_data_info", TestSSMicroDataInfo, K_(evict_size));
};

struct TestSSTopNSparseBlkLen
{
public:
  bool operator() (const int32_t le, const int32_t re)
  {
    return le < re; // build max-root heap
  }

  int get_error_code() { return common::OB_SUCCESS; }
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

private:
  const static uint32_t ORI_MICRO_REF_CNT = 0;
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
{}

void TestSSReorganizePhyBlock::TearDown()
{}

/* This case tests the basic logic of the reorganize task. */
TEST_F(TestSSReorganizePhyBlock, test_reorganize_phy_block_task)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  const int64_t block_size = micro_cache->phy_block_size_;

  ObSSPersistMicroDataTask &persist_task = micro_cache->task_runner_.persist_task_;
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;
  arc_task.interval_us_ = 1800 * 1000 * 1000L;
  usleep(1000 * 1000);
  ObSSExecuteMicroCheckpointTask &micro_ckpt_task = micro_cache->task_runner_.micro_ckpt_task_;
  micro_ckpt_task.is_inited_ = false;
  ObSSExecuteBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  blk_ckpt_task.is_inited_ = false;

  ObSSMemDataManager *mem_data_mgr = &(micro_cache->mem_data_mgr_);
  ASSERT_NE(nullptr, mem_data_mgr);
  ObSSMicroMetaManager *micro_meta_mgr = &(micro_cache->micro_meta_mgr_);
  ASSERT_NE(nullptr, micro_meta_mgr);
  ObSSPhysicalBlockManager *phy_blk_mgr = &(micro_cache->phy_blk_mgr_);
  ASSERT_NE(nullptr, phy_blk_mgr);
  const int64_t available_block_cnt = phy_blk_mgr->get_free_normal_block_cnt();
  const int64_t WRITE_BLK_CNT = 50;
  ASSERT_LT(WRITE_BLK_CNT, available_block_cnt);

  const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                 ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  ObArenaAllocator allocator;

  const int32_t min_micro_size = 8 * 1024;
  const int32_t max_micro_size = 16 * 1024;

  ObArray<TestReorganSSMicroDataInfo> micro_data_info_arr;

  // 1. write 50 fulfilled phy_block
  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    TestReorganSSMicroDataInfo micro_data_info;
    micro_data_info.macro_id_ = macro_id;
    int32_t total_written_size = 0;
    bool write_finish = false;
    int64_t micro_cnt = 0;
    while (!write_finish) {
      const int32_t micro_size = ObRandom::rand(min_micro_size, max_micro_size);
      const int32_t offset = payload_offset + micro_data_info.total_micro_size_;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockIndex micro_index(micro_key, micro_size);
      int32_t micro_idx_size = micro_index.get_serialize_size();
      int32_t delta_size = micro_size + micro_idx_size;
      if (nullptr == mem_data_mgr->fg_mem_block_) {
        mem_data_mgr->inner_seal_and_alloc_fg_mem_block();
      } else if (mem_data_mgr->fg_mem_block_->has_enough_space(delta_size, micro_idx_size)) {
        ASSERT_EQ(OB_SUCCESS, micro_data_info.micro_index_arr_.push_back(micro_index));
        char *micro_data = static_cast<char*>(allocator.alloc(micro_size));
        ASSERT_NE(nullptr, micro_data);
        char c = micro_key.hash() % 26 + 'a';
        MEMSET(micro_data, c, micro_size);
        micro_cache->add_micro_block_cache(micro_key, micro_data, micro_size,
                                          ObSSMicroCacheAccessType::COMMON_IO_TYPE);
        ++micro_cnt;
        micro_data_info.total_micro_size_ += micro_size;
        total_written_size += (micro_size + micro_index.get_serialize_size());
        {
          ASSERT_NE(nullptr, mem_data_mgr->fg_mem_block_);
          ASSERT_EQ(micro_cnt, mem_data_mgr->fg_mem_block_->micro_count_);
          ASSERT_EQ(micro_data_info.total_micro_size_, mem_data_mgr->fg_mem_block_->data_size_);
          ASSERT_EQ(micro_data_info.total_micro_size_, mem_data_mgr->fg_mem_block_->valid_val_);
        }
      } else {
        write_finish = true;
        mem_data_mgr->inner_seal_and_alloc_fg_mem_block();
      }
    }
    ASSERT_EQ(OB_SUCCESS, micro_data_info_arr.push_back(micro_data_info));
    ASSERT_EQ(micro_data_info.micro_index_arr_.count(), micro_data_info_arr.at(i).get_micro_count());
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }

  {
    TestReorganSSMicroDataInfo micro_data_info;
    // to sealed the last mem_block
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(WRITE_BLK_CNT + 1);
    micro_data_info.macro_id_ = macro_id;
    const int32_t offset = payload_offset;
    const int32_t micro_size = 8 * 1024;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    ObSSMicroBlockIndex micro_index(micro_key, micro_size);
    micro_data_info.total_micro_size_ += micro_size;
    ASSERT_EQ(OB_SUCCESS, micro_data_info.micro_index_arr_.push_back(micro_index));
    char *micro_data = static_cast<char*>(allocator.alloc(micro_size));
    ASSERT_NE(nullptr, micro_data);
    char c = micro_key.hash() % 26 + 'a';
    micro_cache->add_micro_block_cache(micro_key, micro_data, micro_size,
                                       ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    ASSERT_EQ(OB_SUCCESS, micro_data_info_arr.push_back(micro_data_info));
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }

  // record written phy_block count
  const int64_t max_retry_cnt = 10;
  bool result_match = false;
  for (int64_t i = 0; !result_match && i < max_retry_cnt; ++i) {
    result_match = (phy_blk_mgr->blk_cnt_info_.normal_blk_.used_cnt_ == WRITE_BLK_CNT);
    if (!result_match) {
      ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
    }
  }
  ASSERT_EQ(true, result_match);
  ASSERT_EQ(available_block_cnt - WRITE_BLK_CNT, phy_blk_mgr->get_free_normal_block_cnt());
  usleep(1000 * 1000);

  // 2. evict some micro_block, to decrease some phy_blocks' usage ratio.
  // idx0~idx9:    evict 0
  // idx10~idx19:  evict 10%
  // idx20~idx29:  evict 10%
  // idx30~idx39:  evict 45%
  // idx40~idx49:  evict 80%
  int32_t evict_pct[5] = {0, 10, 10, 45, 80};
  const int64_t candidate_cnt = 10;
  TestSSTopNSparseBlkLen test_top_n;
  ObBinaryHeap<int32_t, TestSSTopNSparseBlkLen, candidate_cnt> sparse_blk_arr(test_top_n);
  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    const int32_t micro_count = micro_data_info_arr.at(i).get_micro_count();
    const int32_t evict_cnt = micro_count * evict_pct[i / 10] / 100;
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    int32_t total_evict_size = 0;
    for (int32_t j = 0; j < evict_cnt; ++j) {
      const int32_t offset = payload_offset + total_evict_size;
      const int32_t micro_size = micro_data_info_arr.at(i).micro_index_arr_.at(j).size_;
      const ObSSMicroBlockCacheKey &micro_key = micro_data_info_arr.at(i).micro_index_arr_.at(j).micro_key_;
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->micro_meta_map_.get(&micro_key, micro_meta_handle));
      ObSSMicroBlockMeta *micro_meta = micro_meta_handle.get_ptr();
      ASSERT_NE(nullptr, micro_meta);
      ASSERT_EQ(true, micro_meta->is_in_l1_);
      ASSERT_EQ(false, micro_meta->is_in_ghost_);
      ASSERT_EQ(true, micro_meta->is_persisted_);
      micro_meta_handle.reset();
      ASSERT_EQ(ORI_MICRO_REF_CNT + 1, micro_meta->ref_cnt_);

      ObSSMicroBlockMeta *tmp_micro_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(tmp_micro_meta));
      ASSERT_NE(nullptr, tmp_micro_meta);
      *tmp_micro_meta = *micro_meta;
      ObSSMicroBlockMetaHandle evict_micro_handle;
      evict_micro_handle.set_ptr(tmp_micro_meta);
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->try_evict_micro_block_meta(evict_micro_handle));

      ASSERT_EQ(ORI_MICRO_REF_CNT + 1, micro_meta->ref_cnt_);
      ASSERT_EQ(true, micro_meta->is_in_l1_);
      ASSERT_EQ(true, micro_meta->is_in_ghost_);
      int64_t phy_blk_idx = -1;
      bool is_empty_blk = false;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->update_block_valid_length(tmp_micro_meta->data_dest_, tmp_micro_meta->reuse_version_,
        tmp_micro_meta->length_ * -1, phy_blk_idx, is_empty_blk));
      ASSERT_LT(1, phy_blk_idx);
      ASSERT_EQ(false, is_empty_blk);
      total_evict_size += micro_data_info_arr.at(i).micro_index_arr_.at(j).size_;
      evict_micro_handle.reset();
    }
    micro_data_info_arr.at(i).evict_size_ = total_evict_size;
    const int32_t remain_valid_len = micro_data_info_arr.at(i).total_micro_size_ - total_evict_size;
    if (i < candidate_cnt) {
      ASSERT_EQ(OB_SUCCESS, sparse_blk_arr.push(remain_valid_len));
    } else {
      if (sparse_blk_arr.top() > remain_valid_len) {
        ASSERT_EQ(OB_SUCCESS, sparse_blk_arr.replace_top(remain_valid_len));
      }
    }
  }
  ObArray<int64_t> sparse_blk_len_arr;
  while (!sparse_blk_arr.empty()) {
    ASSERT_EQ(OB_SUCCESS, sparse_blk_len_arr.push_back(sparse_blk_arr.top()));
    ASSERT_EQ(OB_SUCCESS, sparse_blk_arr.pop());
  }
  ASSERT_EQ(candidate_cnt, sparse_blk_len_arr.count());

  // 3. check scan blocks to reorganize
  arc_task.is_inited_ = true;
  double max_usage_ratio = 0.75;
  const int64_t SCAN_CNT_HINT = phy_blk_mgr->blk_cnt_info_.total_blk_cnt_;
  ASSERT_EQ(OB_SUCCESS, arc_task.reorganize_op_.scan_sparse_phy_blocks(SCAN_CNT_HINT, candidate_cnt, max_usage_ratio));
  ASSERT_EQ(candidate_cnt, arc_task.reorganize_op_.candidate_phy_blks_.count());
  ObArray<int64_t> candidate_idx_arr;
  // the lowest usage ratio phy_block
  for (int64_t i = 0; i < candidate_cnt; ++i) {
    const ObSSPhysicalBlockHandle &phy_blk_handle = arc_task.reorganize_op_.candidate_phy_blks_.at(i).get_phy_block_handle();
    ASSERT_EQ(sparse_blk_len_arr.at(candidate_cnt - i - 1), phy_blk_handle()->get_valid_len()) << "i: " << i;
    const int64_t phy_blk_idx = arc_task.reorganize_op_.candidate_phy_blks_.at(i).phy_blk_idx_;
    ASSERT_LE(40 + SS_CACHE_SUPER_BLOCK_CNT, phy_blk_idx);
    ASSERT_GT(50 + SS_CACHE_SUPER_BLOCK_CNT, phy_blk_idx);
    ASSERT_EQ(OB_SUCCESS, candidate_idx_arr.push_back(phy_blk_idx));
  }

  // 4. handle selectd phy_blocks, set is_reorganizing from false to true
  ASSERT_EQ(OB_SUCCESS, arc_task.reorganize_op_.handle_sparse_phy_blocks());
  for (int64_t i = 0; i < candidate_cnt; ++i) {
    ASSERT_EQ(true, arc_task.reorganize_op_.candidate_phy_blks_.at(i).reorganized_);
  }
  // cuz one macro_block's all micro_blocks' data will be written into the same mem_block(phy_block)
  // Due to we should choose some phy_blocks whose space usage ratio is the smallest, thus the last 10
  // macro_blocks' relative phy_blocks are the choosen blocks.
  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    const int32_t micro_cnt = micro_data_info_arr.at(i).get_micro_count();
    int32_t total_micro_size = 0;
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const ObSSMicroBlockCacheKey &micro_key = micro_data_info_arr.at(i).micro_index_arr_.at(j).micro_key_;
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->micro_meta_map_.get(&micro_key, micro_meta_handle));
      ObSSMicroBlockMeta *micro_meta = micro_meta_handle.get_ptr();
      ASSERT_NE(nullptr, micro_meta);
      if (i < 40) {
        ASSERT_EQ(false, micro_meta->is_reorganizing_);
      } else if (!micro_meta->is_in_ghost_) {
        ASSERT_EQ(true, micro_meta->is_reorganizing_);
      }
    }
  }

  // 5. reaggregate micro_blocks
  ASSERT_EQ(0, phy_blk_mgr->blk_cnt_info_.reorgan_blk_.used_cnt_);
  ASSERT_EQ(OB_SUCCESS, arc_task.reorganize_op_.reaggregate_micro_blocks());
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

  ObSSReorganizeMicroOp::SSReorganMicroEntry &cur_entry = arc_task.reorganize_op_.choosen_micro_blocks_.at(0);
  ASSERT_EQ(true, cur_entry.micro_meta_handle_.is_valid());
  ObSSMicroBlockMeta *cur_micro_meta = cur_entry.micro_meta_handle_.get_ptr();
  ASSERT_EQ(ORI_MICRO_REF_CNT + 2, cur_micro_meta->ref_cnt_);

  // 6. check reaggregate result, 10 candidate phy_blocks were added into reusable_set.
  ASSERT_EQ(candidate_cnt, phy_blk_mgr->reusable_set_.size());
  ObArray<int64_t> reusable_idx_arr;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->get_reusable_blocks(reusable_idx_arr, candidate_cnt));
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
  ASSERT_LT(0, phy_blk_mgr->blk_cnt_info_.reorgan_blk_.used_cnt_);
  arc_task.clear_for_next_round();
  ASSERT_EQ(ORI_MICRO_REF_CNT + 1, cur_micro_meta->ref_cnt_);

  ASSERT_LT(0, candidate_idx_arr.count());
  for (int64_t i = 0; i < candidate_idx_arr.count(); ++i) {
    ObSSPhysicalBlock *tmp_phy_blk = phy_blk_mgr->get_phy_block_by_idx_nolock(candidate_idx_arr.at(i));
    ASSERT_EQ(ORI_MICRO_REF_CNT + 1, tmp_phy_blk->ref_cnt_);
  }

  ///////////////////////////////////////// Reorganize Again ///////////////////////////////////////////
  // 1. check scan blocks to reorganize
  ASSERT_EQ(OB_SUCCESS, arc_task.reorganize_op_.scan_sparse_phy_blocks(SCAN_CNT_HINT, candidate_cnt, max_usage_ratio));
  ASSERT_EQ(candidate_cnt, arc_task.reorganize_op_.candidate_phy_blks_.count());
  candidate_idx_arr.reset();
  // idx30~idx39, relative phy_blocks will be choosen
  for (int64_t i = 0; i < candidate_cnt; ++i) {
    const int64_t phy_blk_idx = arc_task.reorganize_op_.candidate_phy_blks_.at(i).phy_blk_idx_;
    ASSERT_LE(30 + SS_CACHE_SUPER_BLOCK_CNT, phy_blk_idx);
    ASSERT_GT(40 + SS_CACHE_SUPER_BLOCK_CNT, phy_blk_idx);
    ASSERT_EQ(OB_SUCCESS, candidate_idx_arr.push_back(phy_blk_idx));
  }

  // 2. mock one micro_meta abnormal, occur reuse_version mismatch
  int64_t tmp_idx = 34;
  const int32_t tmp_micro_key_idx = micro_data_info_arr.at(tmp_idx).get_micro_count() * evict_pct[tmp_idx / 10] / 100 + 1;
  ObSSMicroBlockCacheKey tmp_micro_key = micro_data_info_arr.at(tmp_idx).micro_index_arr_.at(tmp_micro_key_idx).micro_key_;
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->micro_meta_map_.get(&tmp_micro_key, tmp_micro_meta_handle));
  ObSSMicroBlockMeta *tmp_micro_meta = tmp_micro_meta_handle.get_ptr();
  ASSERT_NE(nullptr, tmp_micro_meta);
  int64_t tmp_phy_blk_idx = tmp_idx + SS_CACHE_SUPER_BLOCK_CNT;
  ASSERT_EQ(tmp_phy_blk_idx, tmp_micro_meta->data_dest_ / block_size);
  tmp_micro_meta->reuse_version_ += 1;
  {
    ObSSPhysicalBlockHandle blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->get_block_handle(tmp_phy_blk_idx, blk_handle));
    ASSERT_EQ(true, blk_handle.is_valid());
    ASSERT_LT(0, blk_handle.ptr_->get_valid_len());
  }

  // 3. handle selectd phy_blocks, set is_reorganizing from false to true
  ASSERT_EQ(OB_SUCCESS, arc_task.reorganize_op_.handle_sparse_phy_blocks());
  for (int64_t i = 0; i < candidate_cnt; ++i) {
    ASSERT_EQ(true, arc_task.reorganize_op_.candidate_phy_blks_.at(i).reorganized_);
  }

  // 4. reaggregate micro_blocks
  ASSERT_EQ(OB_SUCCESS, arc_task.reorganize_op_.reaggregate_micro_blocks());
  // cuz micro_meta reuse_version mismatch, exist one phy_block fail to reorganize, so valid_len > 0
  {
    ObSSPhysicalBlockHandle blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->get_block_handle(tmp_phy_blk_idx, blk_handle));
    ASSERT_EQ(true, blk_handle.is_valid());
    ASSERT_LT(0, blk_handle.ptr_->get_valid_len());
  }
  arc_task.clear_for_next_round();

  allocator.clear();
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_reorganize_phy_block.log*");
  OB_LOGGER.set_file_name("test_ss_reorganize_phy_block.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}