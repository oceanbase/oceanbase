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

class TestSSMicroCacheRestart : public ::testing::Test
{
public:
  TestSSMicroCacheRestart() {}
  virtual ~TestSSMicroCacheRestart() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSMicroCacheRestart::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheRestart::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheRestart::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 27))); // 128MB
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
}

void TestSSMicroCacheRestart::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

TEST_F(TestSSMicroCacheRestart, test_restart_micro_cache)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  const int64_t cache_file_size = micro_cache->cache_file_size_;

  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ASSERT_EQ(tenant_id, cache_stat.tenant_id_);
  ASSERT_EQ(0, cache_stat.micro_stat().total_micro_cnt_);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  const int64_t total_data_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt();
  const int32_t block_size = phy_blk_mgr.block_size_;
  const int64_t macro_blk_cnt = MIN(2, total_data_blk_cnt);
  const int32_t micro_size = 64 * 1024;
  int64_t total_micro_cnt = 0;
  int64_t total_ckpt_micro_cnt = 0;
  ObSSExecuteMicroCheckpointTask &micro_ckpt_task = micro_cache->task_runner_.micro_ckpt_task_;
  ObSSExecuteBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  ObCompressorType compress_type = TestSSCommonUtil::get_random_compress_type();
  micro_cache->config_.set_micro_ckpt_compressor_type(compress_type);
  micro_cache->config_.set_blk_ckpt_compressor_type(compress_type);
  LOG_INFO("TEST: start test_restart_micro_cache", K(compress_type));

  // 1. FIRST START
  // 1.1 trigger micro_meta and phy_info checkpoint(but there not exist any micro_meta), wait for it finish
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = ObSSExecuteMicroCheckpointOp::MICRO_META_CKPT_INTERVAL_ROUND - 2;
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.exe_round_ = ObSSExecuteBlkCheckpointOp::BLK_INFO_CKPT_INTERVAL_ROUND - 2;
  usleep(5 * 1000 * 1000);
  ASSERT_EQ(1, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_EQ(1, cache_stat.task_stat().phy_ckpt_cnt_);
  ASSERT_EQ(0, phy_blk_mgr.super_block_.micro_ckpt_entry_list_.count());
  ASSERT_LT(0, phy_blk_mgr.super_block_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(
      phy_blk_mgr.super_block_.micro_ckpt_entry_list_.count(), cache_stat.phy_blk_stat().meta_blk_used_cnt_);
  ASSERT_EQ(
      phy_blk_mgr.super_block_.blk_ckpt_entry_list_.count(), cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_);

  // 1.2 destroy micro_cache
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();

  // 1.3 revise is_cache_file_exist
  MTL(ObTenantFileManager*)->is_cache_file_exist_ = true;

  // 2. SECOND START
  // 2.1 restart
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), cache_file_size));
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
  ASSERT_EQ(cache_file_size, phy_blk_mgr.super_block_.cache_file_size_);
  ASSERT_EQ(0, phy_blk_mgr.super_block_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(0, phy_blk_mgr.super_block_.blk_ckpt_entry_list_.count()); // cuz only exist blk ckpt, will drop it
  ASSERT_EQ(tenant_id, micro_cache->cache_stat_.tenant_id_);
  micro_cache->config_.set_micro_ckpt_compressor_type(compress_type);
  micro_cache->config_.set_blk_ckpt_compressor_type(compress_type);

  // 2.2 write data into object_storage
  ObArray<TestSSCommonUtil::MicroBlockInfo> micro_arr2;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::prepare_micro_blocks(macro_blk_cnt, block_size, micro_arr2, 1, false,
    micro_size, micro_size));
  ASSERT_LT(0, micro_arr2.count());

  // 2.3 get these micro_blocks and add them into micro_cache
  ObArenaAllocator allocator;
  const int64_t micro_cnt2 = micro_arr2.count();
  total_micro_cnt += micro_cnt2;
  for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt2); ++i) {
    TestSSCommonUtil::MicroBlockInfo &cur_info = micro_arr2.at(i);
    char *read_buf = static_cast<char*>(allocator.alloc(cur_info.size_));
    ASSERT_NE(nullptr, read_buf);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::get_micro_block(cur_info, read_buf));
  }
  ASSERT_EQ(micro_cnt2, cache_stat.micro_stat().total_micro_cnt_);

  // 2.4 wait some time for persist_task
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  int64_t cur_data_blk_used_cnt = cache_stat.phy_blk_stat().data_blk_used_cnt_;

  int64_t persisted_micro_cnt = 0;
  {
    for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt2); ++i) {
      TestSSCommonUtil::MicroBlockInfo &cur_info = micro_arr2.at(i);
      ObSSMicroBlockCacheKey cur_micro_key = TestSSCommonUtil::gen_phy_micro_key(cur_info.macro_id_, cur_info.offset_,
                                                                                 cur_info.size_);
      ObSSMicroBlockMetaHandle cur_micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&cur_micro_key, cur_micro_meta_handle));
      ObSSMicroBlockMeta *cur_micro_meta = cur_micro_meta_handle.get_ptr();
      if (nullptr != cur_micro_meta && cur_micro_meta->is_persisted()) {
        ++persisted_micro_cnt;
      }
    }
  }

  // 2.5 trigger micro_meta and phy_info checkpoint, wait for it finish
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = ObSSExecuteMicroCheckpointOp::MICRO_META_CKPT_INTERVAL_ROUND - 2;
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.exe_round_ = ObSSExecuteBlkCheckpointOp::BLK_INFO_CKPT_INTERVAL_ROUND - 2;
  usleep(5 * 1000 * 1000);
  ASSERT_EQ(1, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_EQ(1, cache_stat.task_stat().phy_ckpt_cnt_);
  ASSERT_LT(0, phy_blk_mgr.super_block_.micro_ckpt_entry_list_.count());
  ASSERT_LT(0, phy_blk_mgr.super_block_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(
      phy_blk_mgr.super_block_.micro_ckpt_entry_list_.count(), cache_stat.phy_blk_stat().meta_blk_used_cnt_);
  ASSERT_EQ(
      phy_blk_mgr.super_block_.blk_ckpt_entry_list_.count(), cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_);

  total_ckpt_micro_cnt = cache_stat.task_stat().cur_micro_ckpt_item_cnt_;
  ASSERT_EQ(persisted_micro_cnt, total_ckpt_micro_cnt);
  ASSERT_EQ(micro_cnt2, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(micro_cnt2 * micro_size, cache_stat.micro_stat().total_micro_size_);

  // 2.6 destroy micro_cache
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();

  // 3. THIRD START
  // 3.1 restart
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), cache_file_size));
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
  ASSERT_EQ(total_ckpt_micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(total_ckpt_micro_cnt * micro_size, cache_stat.micro_stat().total_micro_size_);
  ASSERT_EQ(cur_data_blk_used_cnt, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  ASSERT_EQ(true, phy_blk_mgr.super_block_.is_valid_checkpoint());
  micro_cache->config_.set_micro_ckpt_compressor_type(compress_type);
  micro_cache->config_.set_blk_ckpt_compressor_type(compress_type);

  // 3.2 write new data into object_storage
  ObArray<TestSSCommonUtil::MicroBlockInfo> micro_arr3;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::prepare_micro_blocks(macro_blk_cnt, block_size, micro_arr3, macro_blk_cnt + 1,
    false, micro_size, micro_size));
  ASSERT_LT(0, micro_arr3.count());

  // 3.3 get these micro_blocks and add them into micro_cache
  const int64_t micro_cnt3 = micro_arr3.count();
  total_micro_cnt += micro_cnt3;
  for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt3); ++i) {
    TestSSCommonUtil::MicroBlockInfo &cur_info = micro_arr3.at(i);
    char *read_buf = static_cast<char*>(allocator.alloc(cur_info.size_));
    ASSERT_NE(nullptr, read_buf);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::get_micro_block(cur_info, read_buf));
  }
  ASSERT_EQ(total_ckpt_micro_cnt + micro_cnt3, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ((total_ckpt_micro_cnt + micro_cnt3) * micro_size, cache_stat.micro_stat().total_micro_size_);

  // 3.4 wait some time for persist_task
  usleep(4 * 1000 * 1000);
  cur_data_blk_used_cnt = cache_stat.phy_blk_stat().data_blk_used_cnt_;

  persisted_micro_cnt = 0;
  {
    // 3.4.1 statistic persisted micro_meta count
    for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt2); ++i) {
      TestSSCommonUtil::MicroBlockInfo &cur_info = micro_arr2.at(i);
      ObSSMicroBlockCacheKey cur_micro_key = TestSSCommonUtil::gen_phy_micro_key(cur_info.macro_id_, cur_info.offset_,
                                                                                 cur_info.size_);
      ObSSMicroBlockMetaHandle cur_micro_meta_handle;
      if (OB_SUCCESS == micro_meta_mgr.micro_meta_map_.get(&cur_micro_key, cur_micro_meta_handle)) {
        ObSSMicroBlockMeta *cur_micro_meta = cur_micro_meta_handle.get_ptr();
        ASSERT_NE(nullptr, cur_micro_meta);
        if (cur_micro_meta->is_persisted()) {
          ++persisted_micro_cnt;
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt3); ++i) {
      TestSSCommonUtil::MicroBlockInfo &cur_info = micro_arr3.at(i);
      ObSSMicroBlockCacheKey cur_micro_key = TestSSCommonUtil::gen_phy_micro_key(cur_info.macro_id_, cur_info.offset_,
                                                                                 cur_info.size_);
      ObSSMicroBlockMetaHandle cur_micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&cur_micro_key, cur_micro_meta_handle));
      ObSSMicroBlockMeta *cur_micro_meta = cur_micro_meta_handle.get_ptr();
      if (nullptr != cur_micro_meta && cur_micro_meta->is_persisted()) {
        ++persisted_micro_cnt;
      }
    }
  }

  // 3.5 evict 3 micro_block, delete 1 micro_block, mock 1 T1/T2 micro_block reuse_version mismatch
  ASSERT_LE(4, micro_arr3.count());
  ObArray<ObSSMicroBlockMeta *> evicted_micro_meta;
  for (int64_t i = 0; i < 3; ++i) {
    TestSSCommonUtil::MicroBlockInfo &evict_micro_info = micro_arr3.at(i);
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(evict_micro_info.macro_id_, evict_micro_info.offset_,
                                                                           evict_micro_info.size_);
    ObSSMicroBlockMetaHandle evict_micro_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&micro_key, evict_micro_handle));
    ObSSMicroBlockMeta *micro_meta = evict_micro_handle.get_ptr();
    ASSERT_EQ(true, micro_meta->is_in_l1_);
    ASSERT_EQ(false, micro_meta->is_in_ghost_);
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_evict_micro_block_meta(evict_micro_handle));
    ASSERT_EQ(true, micro_meta->is_in_l1_);
    ASSERT_EQ(true, micro_meta->is_in_ghost_);
    ASSERT_EQ(false, micro_meta->is_valid_field());
    ASSERT_EQ(OB_SUCCESS, evicted_micro_meta.push_back(micro_meta));
    evict_micro_handle.reset();
  }
  ASSERT_EQ(total_ckpt_micro_cnt + micro_cnt3, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ((total_ckpt_micro_cnt + micro_cnt3) * micro_size, cache_stat.micro_stat().total_micro_size_);

  {
    TestSSCommonUtil::MicroBlockInfo &delete_micro_info = micro_arr3.at(3);
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(delete_micro_info.macro_id_, delete_micro_info.offset_, delete_micro_info.size_);
    ObSSMicroBlockMetaHandle delete_micro_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&micro_key, delete_micro_handle));
    ObSSMicroBlockMeta *micro_meta = delete_micro_handle.get_ptr();
    ASSERT_EQ(true, micro_meta->is_in_l1_);
    ASSERT_EQ(false, micro_meta->is_in_ghost_);
    if (micro_meta->is_persisted()) {
      --persisted_micro_cnt;
    }
    delete_micro_handle.set_ptr(micro_meta);
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_delete_micro_block_meta(delete_micro_handle));
    delete_micro_handle.reset();
    ASSERT_EQ(total_ckpt_micro_cnt + micro_cnt3 - 1, cache_stat.micro_stat().total_micro_cnt_);
    ASSERT_EQ((total_ckpt_micro_cnt + micro_cnt3 - 1) * micro_size, cache_stat.micro_stat().total_micro_size_);
  }

  bool succ_update_version = false;
  {
    for (int64_t i = 4; i < micro_arr3.count(); ++i) {
      TestSSCommonUtil::MicroBlockInfo &cur_info = micro_arr3.at(i);
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(cur_info.macro_id_, cur_info.offset_, cur_info.size_);
      ObSSMicroBlockMetaHandle micro_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&micro_key, micro_handle));
      if (micro_handle()->is_persisted()) {
        micro_handle.get_ptr()->reuse_version_ += 1;
        succ_update_version = true;
        break;
      }
    }
  }

  // 3.6 trigger checkpoint, wait for it finish
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = ObSSExecuteMicroCheckpointOp::MICRO_META_CKPT_INTERVAL_ROUND - 2;
  usleep(5 * 1000 * 1000);
  ASSERT_EQ(1, cache_stat.task_stat().micro_ckpt_cnt_);
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.exe_round_ = ObSSExecuteBlkCheckpointOp::BLK_INFO_CKPT_INTERVAL_ROUND - 2;
  usleep(5 * 1000 * 1000);
  ASSERT_EQ(1, cache_stat.task_stat().phy_ckpt_cnt_);
  ASSERT_LT(0, phy_blk_mgr.super_block_.micro_ckpt_entry_list_.count());
  ASSERT_LT(0, phy_blk_mgr.super_block_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(
      phy_blk_mgr.super_block_.micro_ckpt_entry_list_.count(), cache_stat.phy_blk_stat().meta_blk_used_cnt_);
  ASSERT_EQ(
      phy_blk_mgr.super_block_.blk_ckpt_entry_list_.count(), cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_);

  ASSERT_LT(0, cache_stat.task_stat().cur_micro_ckpt_item_cnt_);
  total_ckpt_micro_cnt = cache_stat.task_stat().cur_micro_ckpt_item_cnt_; // include 2 ghost micro_block

  if (succ_update_version) {
    ASSERT_EQ(total_ckpt_micro_cnt, persisted_micro_cnt - 1);
  } else {
    ASSERT_EQ(total_ckpt_micro_cnt, persisted_micro_cnt);
  }

  // 3.7 destroy micro_cache
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();

  // 4. FOURTH START
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), cache_file_size));
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
  ASSERT_EQ(total_ckpt_micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(total_ckpt_micro_cnt * micro_size, cache_stat.micro_stat().total_micro_size_);
  ASSERT_EQ(cur_data_blk_used_cnt, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  ASSERT_EQ(3, micro_cache->micro_meta_mgr_.arc_info_.seg_info_arr_[ARC_B1].count());
  ASSERT_EQ(3 * micro_size, micro_cache->micro_meta_mgr_.arc_info_.seg_info_arr_[ARC_B1].size());
  ASSERT_EQ(total_ckpt_micro_cnt - 3, micro_cache->micro_meta_mgr_.arc_info_.seg_info_arr_[ARC_T1].count());
  ASSERT_EQ((total_ckpt_micro_cnt - 3) * micro_size, micro_cache->micro_meta_mgr_.arc_info_.seg_info_arr_[ARC_T1].size());
  const int64_t ori_persisted_micro_cnt = cache_stat.micro_stat().total_micro_cnt_;
  micro_cache->config_.set_micro_ckpt_compressor_type(compress_type);
  micro_cache->config_.set_blk_ckpt_compressor_type(compress_type);

  // 4.1 check ckpt_blk
  ObSSMicroCacheSuperBlock &super_block = phy_blk_mgr.super_block_;
  ASSERT_EQ(true, super_block.is_valid_checkpoint());
  const int64_t micro_ckpt_blk_cnt = super_block.micro_ckpt_entry_list_.count();
  for (int64_t i = 0 ; i < micro_ckpt_blk_cnt; ++i) {
    ObSSPhysicalBlock *phy_blk = phy_blk_mgr.get_phy_block_by_idx_nolock(super_block.micro_ckpt_entry_list_.at(i));
    ASSERT_NE(nullptr, phy_blk);
    ASSERT_EQ(false, phy_blk->can_reuse());
  }
  const int64_t phy_ckpt_blk_cnt = super_block.blk_ckpt_entry_list_.count();
  for (int64_t i = 0 ; i < phy_ckpt_blk_cnt; ++i) {
    ObSSPhysicalBlock *phy_blk = phy_blk_mgr.get_phy_block_by_idx_nolock(super_block.blk_ckpt_entry_list_.at(i));
    ASSERT_NE(nullptr, phy_blk);
    ASSERT_EQ(false, phy_blk->can_reuse());
  }

  // 4.2 add lots of micro_meta so that micro_ckpt_block is not enough

  // prevent micro_ckpt from using extra blocks from shared_blocks
  phy_blk_mgr.blk_cnt_info_.meta_blk_.max_cnt_ = phy_blk_mgr.blk_cnt_info_.meta_blk_.min_cnt_;
  const int64_t max_micro_ckpt_blk_cnt = phy_blk_mgr.blk_cnt_info_.meta_blk_.max_cnt_;

  const int64_t max_estimate_micro_cnt = max_micro_ckpt_blk_cnt * phy_blk_mgr.get_block_size() / AVG_MICRO_META_PERSIST_COST;
  const int64_t extra_micro_cnt = max_estimate_micro_cnt * 2 - ori_persisted_micro_cnt;
  const int64_t cur_macro_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt() * 2 / 3;
  int64_t each_micro_cnt = extra_micro_cnt / cur_macro_blk_cnt;
  const int64_t payload_offset =
      ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t small_micro_size = (block_size - payload_offset) / each_micro_cnt - micro_index_size;
  ASSERT_LT(0, small_micro_size);

  char *data_buf = static_cast<char *>(allocator.alloc(small_micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', small_micro_size);

  for (int64_t i = 0; i < cur_macro_blk_cnt + 1; ++i) {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(macro_blk_cnt * 2 + 1 + i);
    for (int64_t j = 0; j < each_micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * small_micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, small_micro_size);
      ASSERT_EQ(OB_SUCCESS,
          micro_cache->add_micro_block_cache(micro_key, data_buf, small_micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(OB_SUCCESS,TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_LT(max_estimate_micro_cnt, cache_stat.micro_stat().valid_micro_cnt_);

  int64_t cur_persisted_micro_cnt = ori_persisted_micro_cnt;
  for (int64_t i = 0; i < cur_macro_blk_cnt + 1; ++i) {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(macro_blk_cnt * 2 + 1 + i);
    for (int32_t j = 0; j < each_micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * small_micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, small_micro_size);
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      if (micro_meta_handle()->is_persisted()) {
        ++cur_persisted_micro_cnt;
      }
    }
  }
  ASSERT_LT(0, cur_persisted_micro_cnt);
  ASSERT_LT(max_estimate_micro_cnt, cur_persisted_micro_cnt);
  ASSERT_LE(cur_persisted_micro_cnt, cache_stat.micro_stat().valid_micro_cnt_);

  // 4.3 execute micro_meta ckpt, although micro_ckpt blk cnt is not enough, but it should also succ
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = ObSSExecuteMicroCheckpointOp::MICRO_META_CKPT_INTERVAL_ROUND - 2;
  usleep(5 * 1000 * 1000);
  ASSERT_EQ(1, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_LT(0, cache_stat.task_stat().cur_micro_ckpt_item_cnt_);
  ASSERT_LE(cache_stat.task_stat().cur_micro_ckpt_item_cnt_, cur_persisted_micro_cnt);
  ASSERT_LT(cache_stat.task_stat().cur_micro_ckpt_item_cnt_, cache_stat.micro_stat().total_micro_cnt_);

  allocator.clear();
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_restart.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_restart.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
