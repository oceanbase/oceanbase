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
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_reader.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
class TestSSExecuteCheckpointTask : public ::testing::Test
{
public:
  TestSSExecuteCheckpointTask() : allocator_(), micro_cache_(nullptr), phy_blk_mgr_(nullptr),
                                  micro_meta_mgr_(nullptr), arc_task_(nullptr), micro_ckpt_task_(nullptr),
                                  blk_ckpt_task_(nullptr), persist_task_(nullptr) {}
  virtual ~TestSSExecuteCheckpointTask() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

  void restart_micro_cache();
  int add_batch_micro_block(const int64_t start_idx, const int64_t macro_cnt, const int64_t micro_cnt,
                            ObArray<ObSSMicroBlockMeta *> &micro_meta_arr);
  int alloc_batch_phy_block_to_reuse(const int64_t total_cnt, ObArray<ObSSPhyBlockPersistInfo> &phy_block_arr);
  void check_micro_meta(const ObArray<ObSSMicroBlockMeta *> &micro_meta_arr, const bool is_exist);
  void check_phy_blk_info(const ObArray<ObSSPhyBlockPersistInfo> &phy_block_arr, const int64_t increment);
  void check_super_block(const ObSSMicroCacheSuperBlock &super_block);
public:
  ObArenaAllocator allocator_;
  ObSSMicroCache *micro_cache_;
  ObSSPhysicalBlockManager *phy_blk_mgr_;
  ObSSMicroMetaManager *micro_meta_mgr_;
  ObSSReleaseCacheTask *arc_task_;
  ObSSExecuteMicroCheckpointTask *micro_ckpt_task_;
  ObSSExecuteBlkCheckpointTask *blk_ckpt_task_;
  ObSSPersistMicroDataTask *persist_task_;
};

void TestSSExecuteCheckpointTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSExecuteCheckpointTask::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSExecuteCheckpointTask::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 32)));
  micro_cache->start();
  micro_cache_ = micro_cache;
  phy_blk_mgr_ = &micro_cache->phy_blk_mgr_;
  ASSERT_NE(nullptr, phy_blk_mgr_);
  micro_meta_mgr_ = &micro_cache->micro_meta_mgr_;
  ASSERT_NE(nullptr, micro_meta_mgr_);
  arc_task_ = &micro_cache->task_runner_.release_cache_task_;
  ASSERT_NE(nullptr, arc_task_);
  micro_ckpt_task_ = &micro_cache->task_runner_.micro_ckpt_task_;
  ASSERT_NE(nullptr, micro_ckpt_task_);
  blk_ckpt_task_ = &micro_cache->task_runner_.blk_ckpt_task_;
  ASSERT_NE(nullptr, blk_ckpt_task_);
  persist_task_ =  &micro_cache->task_runner_.persist_task_;
  ASSERT_NE(nullptr, persist_task_);
}

void TestSSExecuteCheckpointTask::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  // clear micro_meta_ckpt
  ObSSMicroCacheSuperBlock new_super_blk(micro_cache->cache_file_size_);
  ASSERT_EQ(OB_SUCCESS, micro_cache->phy_blk_mgr_.update_ss_super_block(new_super_blk));
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSExecuteCheckpointTask::restart_micro_cache()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();

  tnt_file_mgr->is_cache_file_exist_ = true;

  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 32)));
  micro_cache->start();
  micro_ckpt_task_->is_inited_ = false;
  blk_ckpt_task_->is_inited_ = false;
  arc_task_->is_inited_ = false;
  persist_task_->is_inited_ = false;
}

int TestSSExecuteCheckpointTask::add_batch_micro_block(
    const int64_t start_idx,
    const int64_t macro_cnt,
    const int64_t micro_cnt,
    ObArray<ObSSMicroBlockMeta *> &micro_meta_arr)
{
  int ret = OB_SUCCESS;
  persist_task_->is_inited_ = true;
  ob_usleep(100 * 1000);
  const int64_t payload_offset =
      ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_size = (DEFAULT_BLOCK_SIZE - payload_offset) / micro_cnt - micro_index_size;
  char *data_buf = nullptr;
  ObArray<ObSSMicroBlockCacheKey> micro_key_arr;
  if (OB_UNLIKELY(start_idx < 0 || macro_cnt <= 0 || micro_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_idx), K(macro_cnt), K(micro_cnt));
  } else if (OB_ISNULL(data_buf = static_cast<char *>(allocator_.alloc(micro_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(micro_size));
  } else {
    MEMSET(data_buf, 'a', micro_size);
    const int64_t end_idx = start_idx + macro_cnt;
    for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
      const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < micro_cnt; ++j) {
        const int32_t offset = payload_offset + j * micro_size;
        const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        if (OB_FAIL(micro_cache_->add_micro_block_cache(
                micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
          LOG_WARN("fail to add micro_block", KR(ret), K(micro_key), KP(data_buf), K(micro_size));
        } else if (OB_FAIL(micro_key_arr.push_back(micro_key))) {
          LOG_WARN("fail to push micro_key", KR(ret), K(micro_key));
        }
      }
      if (FAILEDx(TestSSCommonUtil::wait_for_persist_task())) {
        LOG_WARN("fail to wait for persist task", K(ret));
      }
    }
    persist_task_->is_inited_ = false;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < micro_key_arr.count(); ++i) {
    ObSSMicroBlockMetaHandle micro_handle;
    ObSSMicroBlockMeta *tmp_micro_meta = nullptr;
    const ObSSMicroBlockCacheKey &micro_key = micro_key_arr[i];
    if (OB_FAIL(micro_meta_mgr_->get_micro_block_meta_handle(micro_key, micro_handle, false))) {
      LOG_WARN("fail to get micro_handle", KR(ret), K(micro_key));
    } else if (OB_UNLIKELY(!micro_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_handle is invalid", KR(ret), K(micro_handle), KPC(micro_handle.get_ptr()));
    } else if (OB_ISNULL(tmp_micro_meta = static_cast<ObSSMicroBlockMeta *>(allocator_.alloc(sizeof(ObSSMicroBlockMeta))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(sizeof(ObSSMicroBlockMeta)));
    } else if (OB_FALSE_IT(tmp_micro_meta->reset())) {
    } else if (OB_FALSE_IT(*tmp_micro_meta = *micro_handle.get_ptr())) {
    } else if (OB_FAIL(micro_meta_arr.push_back(tmp_micro_meta))) {
      LOG_WARN("fail to push micro_meta", KR(ret), KPC(tmp_micro_meta));
    }
  }
  return ret;
}

int TestSSExecuteCheckpointTask::alloc_batch_phy_block_to_reuse(
    const int64_t total_cnt,
    ObArray<ObSSPhyBlockPersistInfo> &phy_block_arr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(total_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(total_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; i++) {
      int64_t block_idx = 0;
      ObSSPhysicalBlockHandle phy_blk_handle;
      if (OB_FAIL(phy_blk_mgr_->alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK))) {
        LOG_WARN("fail to alloc block", KR(ret), K(block_idx), K(phy_blk_handle));
      } else if (OB_UNLIKELY(!phy_blk_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_blk_handle is invalid", KR(ret), K(phy_blk_handle));
      } else {
        phy_blk_handle.get_ptr()->alloc_time_us_ -= PHY_BLK_MAX_REUSE_TIME;  // mock block is reusable
        ObSSPhyBlockPersistInfo phy_info(block_idx, phy_blk_handle.get_ptr()->get_reuse_version());
        if (OB_FAIL(phy_block_arr.push_back(phy_info))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to push phy_info", KR(ret), K(phy_info));
        }
      }
    }
  }
  return ret;
}

void TestSSExecuteCheckpointTask::check_micro_meta(
    const ObArray<ObSSMicroBlockMeta *> &micro_meta_arr,
    const bool is_exist)
{
  int ret = OB_SUCCESS;
  const int64_t micro_cnt = micro_meta_arr.count();
  for (int64_t i = 0; i < micro_cnt; ++i) {
    ObSSMicroBlockMeta *old_micro_meta = micro_meta_arr[i];
    const ObSSMicroBlockCacheKey &micro_key = old_micro_meta->get_micro_key();
    ObSSMicroBlockMetaHandle micro_handle;
    ret = micro_meta_mgr_->get_micro_block_meta_handle(micro_key, micro_handle, false);
    if (is_exist == true) {
      if (old_micro_meta->is_persisted()) {
        if (OB_FAIL(ret)) {
          LOG_WARN("unexpected", KR(ret), K(micro_key), K(i), K(micro_cnt), KPC(old_micro_meta));
        }
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_EQ(true, micro_handle.is_valid());
        ObSSMicroBlockMeta *micro_meta = micro_handle.get_ptr();
        ASSERT_EQ(micro_meta->reuse_version_, old_micro_meta->reuse_version_);
        ASSERT_EQ(micro_meta->data_dest_, old_micro_meta->data_dest_);
        ASSERT_EQ(micro_meta->length_, old_micro_meta->length_);
        ASSERT_EQ(micro_meta->is_in_l1_, old_micro_meta->is_in_l1_);
        ASSERT_EQ(micro_meta->is_in_ghost_, old_micro_meta->is_in_ghost_);
        ASSERT_EQ(micro_meta->is_persisted_, old_micro_meta->is_persisted_);
        ASSERT_EQ(micro_meta->is_reorganizing_, old_micro_meta->is_reorganizing_);
      } else {
        ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
      }
    } else {
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
    }
  }
}

void TestSSExecuteCheckpointTask::check_phy_blk_info(
    const ObArray<ObSSPhyBlockPersistInfo> &phy_block_arr,
    const int64_t increment)
{
  for (int64_t i = 0; i < phy_block_arr.count(); ++i) {
    ObSSPhysicalBlockHandle phy_blk_handle;
    const int64_t block_idx = phy_block_arr[i].blk_idx_;
    const int64_t old_reuse_version = phy_block_arr[i].reuse_version_;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->get_block_handle(block_idx, phy_blk_handle));
    ASSERT_EQ(true, phy_blk_handle.is_valid());
    const int64_t reuse_version = phy_blk_handle.get_ptr()->get_reuse_version();
    ASSERT_EQ(old_reuse_version + increment, reuse_version);
  }
}

void TestSSExecuteCheckpointTask::check_super_block(const ObSSMicroCacheSuperBlock &super_block)
{
  const ObSSMicroCacheSuperBlock &cur_super_block = phy_blk_mgr_->super_block_;
  ASSERT_EQ(cur_super_block.micro_ckpt_time_us_, super_block.micro_ckpt_time_us_);
  ASSERT_EQ(cur_super_block.cache_file_size_, super_block.cache_file_size_);
  ASSERT_EQ(cur_super_block.modify_time_us_, super_block.modify_time_us_);
  ASSERT_EQ(cur_super_block.micro_ckpt_entry_list_.count(), super_block.micro_ckpt_entry_list_.count());
  for (int64_t i = 0; i < super_block.micro_ckpt_entry_list_.count(); ++i) {
    ASSERT_EQ(cur_super_block.micro_ckpt_entry_list_[i], super_block.micro_ckpt_entry_list_[i]);
  }
  ASSERT_EQ(cur_super_block.blk_ckpt_entry_list_.count(), super_block.blk_ckpt_entry_list_.count());
  for (int64_t i = 0; i < super_block.blk_ckpt_entry_list_.count(); ++i) {
    ASSERT_EQ(cur_super_block.blk_ckpt_entry_list_[i], super_block.blk_ckpt_entry_list_[i]);
  }
}

/* This case tests the basic logic of the execute_checkpoint task. */
TEST_F(TestSSExecuteCheckpointTask, test_execute_checkpoint_task)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  const int64_t block_size = micro_cache->phy_block_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;

  ObSSPersistMicroDataTask &persist_task = micro_cache->task_runner_.persist_task_;
  ObSSExecuteMicroCheckpointTask &micro_ckpt_task = micro_cache->task_runner_.micro_ckpt_task_;
  micro_ckpt_task.is_inited_ = false;
  ObSSExecuteBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  blk_ckpt_task.is_inited_ = false;
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;

  ObSSMemDataManager *mem_data_mgr = &(micro_cache->mem_data_mgr_);
  ASSERT_NE(nullptr, mem_data_mgr);
  ObSSMicroMetaManager *micro_meta_mgr = &(micro_cache->micro_meta_mgr_);
  ASSERT_NE(nullptr, micro_meta_mgr);
  ObSSARCInfo &arc_info = micro_meta_mgr->arc_info_;
  ObSSPhysicalBlockManager *phy_blk_mgr = &(micro_cache->phy_blk_mgr_);
  ASSERT_NE(nullptr, phy_blk_mgr);
  const int64_t ori_arc_limit = micro_meta_mgr->get_arc_info().limit_;

  // 1. execute phy_block checkpoint
  micro_ckpt_task.is_inited_ = true;
  blk_ckpt_task.is_inited_ = true;
  micro_ckpt_task.interval_us_ = 3600 * 1000 * 1000L;
  blk_ckpt_task.interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(1000 * 1000);

  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(false, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.is_valid());
  ASSERT_EQ(false, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.is_valid());
  ASSERT_EQ(0, phy_blk_mgr->blk_cnt_info_.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr->blk_cnt_info_.meta_blk_.used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr->reusable_set_.size());
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.gen_checkpoint());

  ASSERT_EQ(phy_blk_mgr->blk_cnt_info_.total_blk_cnt_ - 2, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(true, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.is_valid());
  ASSERT_EQ(0, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.micro_ckpt_entry_list_.count());
  int64_t blk_ckpt_list_cnt = blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.blk_ckpt_entry_list_.count();
  ASSERT_LT(0, blk_ckpt_list_cnt);
  ASSERT_EQ(phy_blk_mgr->blk_cnt_info_.phy_ckpt_blk_used_cnt_, blk_ckpt_list_cnt);
  ObSEArray<int64_t, 8> blk_ckpt_entry_list;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_entry_list.assign(blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.blk_ckpt_entry_list_));

  // 2. execute phy_block checkpoint
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.reuse();
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(false, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.is_valid());
  ASSERT_EQ(false, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.is_valid());
  ASSERT_EQ(0, phy_blk_mgr->reusable_set_.size());
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.gen_checkpoint());

  ASSERT_EQ(phy_blk_mgr->blk_cnt_info_.total_blk_cnt_ - 2, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(true, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.is_valid());
  ASSERT_EQ(true, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.is_valid());
  ASSERT_EQ(0, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(blk_ckpt_list_cnt, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(phy_blk_mgr->blk_cnt_info_.phy_ckpt_blk_used_cnt_, blk_ckpt_list_cnt);

  for (int64_t i = 0; i < blk_ckpt_entry_list.count(); ++i) {
    const int64_t blk_idx = blk_ckpt_entry_list.at(i);
    ObSSPhysicalBlock *phy_blk = phy_blk_mgr->get_phy_block_by_idx_nolock(blk_idx);
    ASSERT_NE(nullptr, phy_blk);
    ASSERT_EQ(true, phy_blk->is_free_);
    ASSERT_EQ(2, phy_blk->reuse_version_);
    ASSERT_EQ(0, phy_blk->valid_len_);
  }
  ASSERT_NE(blk_ckpt_entry_list.at(0), blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.blk_ckpt_entry_list_.at(0));
  blk_ckpt_entry_list.reset();
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_entry_list.assign(blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.blk_ckpt_entry_list_));
  ASSERT_EQ(blk_ckpt_list_cnt, blk_ckpt_entry_list.count());

  // 3. make some micro_data
  const int64_t available_block_cnt = phy_blk_mgr->blk_cnt_info_.cache_limit_blk_cnt();
  const int64_t WRITE_BLK_CNT = 50;
  ASSERT_LT(WRITE_BLK_CNT, available_block_cnt);

  const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                 ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 20;
  const int32_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  // 3.1. write 50 fulfilled phy_block
  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                                         ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    }
    ASSERT_NE(nullptr, mem_data_mgr->fg_mem_block_);
    ASSERT_EQ(true, mem_data_mgr->fg_mem_block_->is_valid()) << i;
    ASSERT_EQ(micro_cnt, mem_data_mgr->fg_mem_block_->micro_count_) << i;
    ASSERT_EQ(micro_cnt, mem_data_mgr->fg_mem_block_->micro_count_);
    ASSERT_EQ(micro_size * micro_cnt, mem_data_mgr->fg_mem_block_->data_size_);
    ASSERT_EQ(micro_size * micro_cnt, mem_data_mgr->fg_mem_block_->valid_val_);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  {
    // to sealed the last mem_block
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(WRITE_BLK_CNT + 1);
    const int32_t offset = payload_offset;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                                       ObSSMicroCacheAccessType::COMMON_IO_TYPE);
  }

  // record written phy_block count
  const int64_t max_retry_cnt = 10;
  bool result_match = false;
  for (int64_t i = 0; !result_match && i < max_retry_cnt; ++i) {
    result_match = (phy_blk_mgr->blk_cnt_info_.data_blk_.used_cnt_ == WRITE_BLK_CNT);
    if (!result_match) {
      ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
    }
  }
  ASSERT_EQ(true, result_match);
  usleep(1000 * 1000);

  // 3.2. evict all micro_block of the first macro_block
  int64_t evict_blk_idx = -1;
  for (int64_t i = 0; i < 1; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->micro_meta_map_.get(&micro_key, micro_meta_handle));
      ObSSMicroBlockMeta *micro_meta = micro_meta_handle.get_ptr();
      ASSERT_NE(nullptr, micro_meta);
      ASSERT_EQ(true, micro_meta->is_in_l1_);
      ASSERT_EQ(false, micro_meta->is_in_ghost_);
      ASSERT_EQ(true, micro_meta->is_persisted_);

      ObSSMicroBlockMeta *tmp_micro_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(tmp_micro_meta));
      ASSERT_NE(nullptr, tmp_micro_meta);
      *tmp_micro_meta = *micro_meta;
      ObSSMicroBlockMetaHandle tmp_micro_handle;
      tmp_micro_handle.set_ptr(tmp_micro_meta);

      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->try_evict_micro_block_meta(tmp_micro_handle));
      ASSERT_EQ(true, micro_meta->is_in_ghost_);
      ASSERT_EQ(false, micro_meta->is_valid_field());
      int64_t phy_blk_idx = -1;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->update_block_valid_length(tmp_micro_meta->data_dest_, tmp_micro_meta->reuse_version_,
        tmp_micro_meta->length_ * -1, phy_blk_idx));
      evict_blk_idx = tmp_micro_meta->data_dest_ / phy_blk_mgr->block_size_;
      tmp_micro_handle.reset();
    }
  }
  ASSERT_NE(-1, evict_blk_idx);
  ASSERT_EQ(((WRITE_BLK_CNT - 1) * micro_cnt + 1) * micro_size, arc_info.get_valid_size());

  // 3.3. invalidate all micro_block of the second macro_block
  int64_t invalid_blk_idx = -1;
  for (int64_t i = 1; i < 2; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      ObSSMicroBlockMeta *micro_meta = micro_meta_handle.get_ptr();
      int64_t phy_blk_idx = -1;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->update_block_valid_length(micro_meta->data_dest_, micro_meta->reuse_version_,
                micro_meta->length_ * -1, phy_blk_idx));
      invalid_blk_idx = micro_meta->data_dest_ / phy_blk_mgr->block_size_;
      micro_meta->mark_invalid();
    }
  }
  ASSERT_NE(-1, invalid_blk_idx);

  // 3.4. check micro_meta and macro_meta count
  ASSERT_EQ(WRITE_BLK_CNT * micro_cnt + 1, micro_meta_mgr->micro_meta_map_.count());

  // 4. execute micro_meta checkpoint
  // 4.1. check ckpt state
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = ObSSExecuteMicroCheckpointOp::MICRO_META_CKPT_INTERVAL_ROUND;
  micro_ckpt_task.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, micro_ckpt_task.ckpt_op_.check_state());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->get_ss_super_block(micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.prev_super_block_));
  ASSERT_EQ(true, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.need_ckpt_);
  ASSERT_EQ(0, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.cur_super_block_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(0, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.cur_super_block_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(blk_ckpt_list_cnt, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.prev_super_block_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(0, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.prev_super_block_.micro_ckpt_entry_list_.count());

  // 4.2. scan reusable phy_blocks
  ASSERT_EQ(2, phy_blk_mgr->reusable_set_.size());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->scan_blocks_to_reuse());
  ASSERT_EQ(2, phy_blk_mgr->reusable_set_.size());

  // 4.3. first, mock all reserved_micro_ckpt_blk were used up, will fail to gen micro_meta ckpt
  const int64_t ori_micro_blk_used_cnt = phy_blk_mgr->blk_cnt_info_.meta_blk_.used_cnt_;
  const int64_t ori_micro_blk_hold_cnt = phy_blk_mgr->blk_cnt_info_.meta_blk_.hold_cnt_;
  const int64_t ori_micro_blk_max_cnt = phy_blk_mgr->blk_cnt_info_.meta_blk_.max_cnt_;
  phy_blk_mgr->blk_cnt_info_.meta_blk_.used_cnt_ = ori_micro_blk_max_cnt;
  phy_blk_mgr->blk_cnt_info_.meta_blk_.hold_cnt_ = ori_micro_blk_max_cnt;
  phy_blk_mgr->blk_cnt_info_.shared_blk_used_cnt_ =
      phy_blk_mgr->blk_cnt_info_.meta_blk_.hold_cnt_ + phy_blk_mgr->blk_cnt_info_.data_blk_.hold_cnt_;

  ASSERT_EQ(OB_SUCCESS, micro_ckpt_task.ckpt_op_.gen_micro_meta_checkpoint());
  ASSERT_EQ(true, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.lack_phy_blk_);
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.lack_phy_blk_ = false;
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.ckpt_item_cnt_ = 0;
  micro_ckpt_task.ckpt_op_.tablet_cache_info_map_.clear();
  ASSERT_EQ(((WRITE_BLK_CNT - 2) * micro_cnt + 1) * micro_size, arc_info.get_valid_size());

  phy_blk_mgr->blk_cnt_info_.meta_blk_.used_cnt_ = ori_micro_blk_used_cnt;
  phy_blk_mgr->blk_cnt_info_.meta_blk_.hold_cnt_ = ori_micro_blk_hold_cnt;
  phy_blk_mgr->blk_cnt_info_.shared_blk_used_cnt_ =
      phy_blk_mgr->blk_cnt_info_.meta_blk_.hold_cnt_ + phy_blk_mgr->blk_cnt_info_.data_blk_.hold_cnt_;

  // 4.4. normal situation
  ObSEArray<uint64_t, 128> reuse_version_arr;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->get_block_reuse_version(reuse_version_arr));
  ASSERT_EQ(OB_SUCCESS, micro_ckpt_task.ckpt_op_.gen_micro_meta_checkpoint());
  ASSERT_EQ(false, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.lack_phy_blk_);
  ASSERT_LT(0, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.cur_super_block_.micro_ckpt_entry_list_.count());
  ASSERT_LT(0, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.cur_super_block_.micro_ckpt_time_us_);
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_LT(0, micro_ckpt_task.ckpt_op_.tablet_cache_info_map_.size());
  ObSSTabletCacheMap::const_iterator iter = micro_ckpt_task.ckpt_op_.tablet_cache_info_map_.begin();
  int64_t total_micro_size = 0;
  int64_t tablet_cnt = 0;
  for (; iter != micro_ckpt_task.ckpt_op_.tablet_cache_info_map_.end(); ++iter) {
    total_micro_size += iter->second.get_valid_size();
    ++tablet_cnt;
  }
  ASSERT_EQ(arc_info.get_valid_size(), total_micro_size);

  // 4.5. update ss_super_block
  ASSERT_EQ(OB_SUCCESS, micro_ckpt_task.ckpt_op_.update_super_block(true));
  ObSSMicroCacheSuperBlock cur_super_blk;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->get_ss_super_block(cur_super_blk));
  ASSERT_LT(0, cur_super_blk.micro_ckpt_entry_list_.count());
  ASSERT_EQ(blk_ckpt_entry_list.at(0), cur_super_blk.blk_ckpt_entry_list_.at(0));
  ObSEArray<int64_t, 8> micro_ckpt_entry_list;
  ASSERT_EQ(OB_SUCCESS, micro_ckpt_entry_list.assign(cur_super_blk.micro_ckpt_entry_list_));
  int64_t micro_ckpt_list_cnt = micro_ckpt_entry_list.count();

  // 4.6. finish checkpoint
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->update_block_gc_reuse_version(reuse_version_arr));
  int64_t free_blk_cnt = 0;
  ASSERT_EQ(OB_SUCCESS,
      phy_blk_mgr->try_free_batch_blocks(micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.prev_super_block_.micro_ckpt_entry_list_, free_blk_cnt));
  ASSERT_EQ(free_blk_cnt, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.prev_super_block_.micro_ckpt_entry_list_.count());

  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = 0;

  // 4.7. read micro_block checkpoint, and check it
  micro_meta_mgr->micro_meta_map_.clear();
  ASSERT_EQ(OB_SUCCESS, micro_cache->read_micro_meta_checkpoint(cur_super_blk.micro_ckpt_entry_list_.at(0),
                                                                cur_super_blk.micro_ckpt_time_us_));
  ASSERT_EQ(ori_arc_limit, micro_meta_mgr->get_arc_info().limit_);
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt, micro_meta_mgr->replay_ctx_.total_replay_cnt_);
  ASSERT_EQ((WRITE_BLK_CNT - 2) * micro_cnt, micro_meta_mgr->arc_info_.seg_info_arr_[ARC_T1].cnt_);
  ASSERT_EQ(micro_cnt, micro_meta_mgr->arc_info_.seg_info_arr_[ARC_B1].cnt_);

  // 5. execute phy_block checkpoint
  // 5.1. check ckpt state
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.exe_round_ = ObSSExecuteBlkCheckpointOp::BLK_INFO_CKPT_INTERVAL_ROUND;
  blk_ckpt_task.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.check_state());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->get_ss_super_block(blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_));
  ASSERT_EQ(true, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.need_ckpt_);
  ASSERT_EQ(0, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(0, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(blk_ckpt_list_cnt, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(micro_ckpt_list_cnt, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.micro_ckpt_entry_list_.count());

  // 5.2. gen phy_blk checkpoint
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.gen_phy_block_checkpoint());
  ASSERT_EQ(blk_ckpt_list_cnt, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.blk_ckpt_entry_list_.count());
  const int64_t total_blk_cnt = phy_blk_mgr->blk_cnt_info_.total_blk_cnt_ - 2;
  ASSERT_EQ(total_blk_cnt, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);

  // 5.3. update ss_super_block
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.update_super_block(false));
  cur_super_blk.reset();
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->get_ss_super_block(cur_super_blk));
  ASSERT_EQ(micro_ckpt_list_cnt, cur_super_blk.micro_ckpt_entry_list_.count());
  ASSERT_EQ(blk_ckpt_list_cnt, cur_super_blk.blk_ckpt_entry_list_.count());
  ASSERT_NE(blk_ckpt_entry_list.at(0), cur_super_blk.blk_ckpt_entry_list_.at(0));
  blk_ckpt_entry_list.reset();
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_entry_list.assign(cur_super_blk.blk_ckpt_entry_list_));
  for (int64_t i = 0; i < micro_ckpt_entry_list.count(); ++i) {
    ASSERT_EQ(micro_ckpt_entry_list.at(i), cur_super_blk.micro_ckpt_entry_list_.at(i));
  }

  // 5.4. finish checkpoint(evicted phy_block and invalid phy_block have already been reused)
  ObSSPhysicalBlockHandle phy_blk_handle;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->get_block_handle(evict_blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  uint64_t ori_reuse_version_1 = phy_blk_handle.get_ptr()->reuse_version_;
  phy_blk_handle.reset();
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->get_block_handle(invalid_blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  uint64_t ori_reuse_version_2 = phy_blk_handle.get_ptr()->reuse_version_;
  phy_blk_handle.reset();

  ASSERT_EQ(OB_SUCCESS,
      phy_blk_mgr->try_free_batch_blocks(blk_ckpt_task.ckpt_op_.reusable_block_idxs_, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.free_blk_cnt_));
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->get_block_handle(evict_blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  ASSERT_EQ(true, phy_blk_handle.ptr_->is_free_);
  ASSERT_EQ(ori_reuse_version_1 + 1, phy_blk_handle.ptr_->reuse_version_);
  ASSERT_EQ(ori_reuse_version_1, phy_blk_handle.ptr_->gc_reuse_version_);
  phy_blk_handle.reset();

  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->get_block_handle(invalid_blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  ASSERT_EQ(true, phy_blk_handle.ptr_->is_free_);
  ASSERT_EQ(ori_reuse_version_2 + 1, phy_blk_handle.ptr_->reuse_version_);
  ASSERT_EQ(ori_reuse_version_2, phy_blk_handle.ptr_->gc_reuse_version_);
  phy_blk_handle.reset();
  ASSERT_EQ(phy_blk_mgr->blk_cnt_info_.meta_blk_.used_cnt_, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(phy_blk_mgr->blk_cnt_info_.phy_ckpt_blk_used_cnt_, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.blk_ckpt_entry_list_.count());

  // 5.5. read phy_block checkpoint, and check it
  ObArray<ObSSPhyBlockReuseInfo> reuse_info_arr1;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->scan_blocks_to_ckpt(reuse_info_arr1));
  ASSERT_LT(0, reuse_info_arr1.count());
  ASSERT_EQ(OB_SUCCESS, micro_cache->read_phy_block_checkpoint(cur_super_blk.blk_ckpt_entry_list_.at(0)));
  ObArray<ObSSPhyBlockReuseInfo> reuse_info_arr2;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->scan_blocks_to_ckpt(reuse_info_arr2));
  ASSERT_EQ(reuse_info_arr1.count(), reuse_info_arr2.count());
  for (int64_t i = 2; i < reuse_info_arr1.count(); ++i) {
    ASSERT_EQ(reuse_info_arr1.at(i).blk_idx_, reuse_info_arr2.at(i).blk_idx_);
    ASSERT_EQ(reuse_info_arr1.at(i).reuse_version_, reuse_info_arr2.at(i).reuse_version_);
  }

  // 6. execute phy_block checkpoint
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.reuse();
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(false, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.is_valid());
  ASSERT_EQ(false, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.is_valid());
  ASSERT_EQ(0, phy_blk_mgr->reusable_set_.size());
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.gen_checkpoint());

  ASSERT_EQ(true, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.is_valid());
  ASSERT_EQ(true, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.is_valid());
  ASSERT_EQ(phy_blk_mgr->blk_cnt_info_.total_blk_cnt_ - 2, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(blk_ckpt_list_cnt, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(micro_ckpt_list_cnt, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.micro_ckpt_entry_list_.count());

  // 7. execute micro_meta checkpoint
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.reuse();
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(false, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.prev_super_block_.is_valid());
  ASSERT_EQ(false, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.cur_super_block_.is_valid());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->scan_blocks_to_reuse());
  ASSERT_EQ(0, phy_blk_mgr->reusable_set_.size());
  ASSERT_EQ(OB_SUCCESS, micro_ckpt_task.ckpt_op_.gen_checkpoint());

  ASSERT_EQ(true, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.prev_super_block_.is_valid());
  ASSERT_EQ(true, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.cur_super_block_.is_valid());
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(blk_ckpt_list_cnt, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.prev_super_block_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(micro_ckpt_entry_list.count(), micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.prev_super_block_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(phy_blk_mgr->blk_cnt_info_.meta_blk_.used_cnt_, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(phy_blk_mgr->blk_cnt_info_.phy_ckpt_blk_used_cnt_, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.blk_ckpt_entry_list_.count());

  // 8. execute phy_block checkpoint
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.reuse();
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(false, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.is_valid());
  ASSERT_EQ(false, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.is_valid());
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.gen_checkpoint());

  ASSERT_EQ(true, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.is_valid());
  ASSERT_EQ(true, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.is_valid());
  ASSERT_EQ(phy_blk_mgr->blk_cnt_info_.total_blk_cnt_ - 2, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(blk_ckpt_list_cnt, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(micro_ckpt_list_cnt, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_super_block_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(phy_blk_mgr->blk_cnt_info_.meta_blk_.used_cnt_, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(phy_blk_mgr->blk_cnt_info_.phy_ckpt_blk_used_cnt_, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.cur_super_block_.blk_ckpt_entry_list_.count());

  allocator.clear();
}

/* This case tests whether the micro cache can be restored to the expected state after restart.  */
TEST_F(TestSSExecuteCheckpointTask, test_micro_cache_ckpt_after_restart)
{
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  micro_ckpt_task_->is_inited_ = false;
  blk_ckpt_task_->is_inited_ = false;
  arc_task_->is_inited_ = false;
  persist_task_->is_inited_ = false;
  const int64_t total_blk_cnt = phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_;
  ob_usleep(1000 * 1000);

  // 1. add some micro_block into cache and record their micro_meta
  ObArray<ObSSMicroBlockMeta *> micro_meta_arr1;
  int64_t macro_start_idx = 1;
  int64_t write_blk_cnt = 10;
  const int64_t micro_cnt = 20;
  ASSERT_EQ(OB_SUCCESS, add_batch_micro_block(macro_start_idx, write_blk_cnt, micro_cnt, micro_meta_arr1));
  const int64_t used_data_blk_cnt1 = phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_;

  ObArray<ObSSPhyBlockPersistInfo> phy_block_arr1;
  int64_t alloc_phy_blk_cnt = 20;
  ASSERT_EQ(OB_SUCCESS, alloc_batch_phy_block_to_reuse(alloc_phy_blk_cnt, phy_block_arr1));

  // 2. do ckpt_task first round
  micro_ckpt_task_->is_inited_ = true;
  blk_ckpt_task_->is_inited_ = true;
  micro_ckpt_task_->interval_us_ = 3600 * 1000 * 1000L;
  blk_ckpt_task_->interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(1000 * 1000);

  micro_ckpt_task_->ckpt_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  micro_ckpt_task_->ckpt_op_.micro_ckpt_ctx_.need_scan_blk_ = true;
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(false, micro_ckpt_task_->ckpt_op_.micro_ckpt_ctx_.prev_super_block_.is_valid());
  ASSERT_EQ(false, micro_ckpt_task_->ckpt_op_.micro_ckpt_ctx_.cur_super_block_.is_valid());
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_block_.is_valid());
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_block_.is_valid());
  ASSERT_EQ(0, phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr_->reusable_set_.size());
  int64_t start_time_us = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, micro_ckpt_task_->ckpt_op_.gen_checkpoint());
  const int64_t micro_exe_time_us = ObTimeUtility::current_time() - start_time_us;
  ASSERT_LT(0, phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_);
  ASSERT_LT(0, phy_blk_mgr_->reusable_set_.size());
  ASSERT_EQ((write_blk_cnt - 1) * 20, micro_ckpt_task_->ckpt_op_.micro_ckpt_ctx_.ckpt_item_cnt_);

  start_time_us = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());
  const int64_t blk_exe_time_us = ObTimeUtility::current_time() - start_time_us;
  ASSERT_LT(0, phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(total_blk_cnt - SS_CACHE_SUPER_BLOCK_CNT, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  micro_ckpt_task_->is_inited_ = false;
  blk_ckpt_task_->is_inited_ = false;
  ObSSMicroCacheSuperBlock super_block1 = phy_blk_mgr_->super_block_;

  // 3. restart micro cache
  LOG_INFO("TEST: start first restart");
  restart_micro_cache();
  LOG_INFO("TEST: finish first restart");

  // 4. check super_block, micro_meta and phy_block_info
  check_phy_blk_info(phy_block_arr1, 1);
  check_micro_meta(micro_meta_arr1, true);
  check_super_block(super_block1);
  ASSERT_EQ(super_block1.micro_ckpt_entry_list_.count(), phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_);
  ASSERT_EQ(super_block1.blk_ckpt_entry_list_.count(), phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(used_data_blk_cnt1, phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_);

  // 5. clear all micro meta
  micro_meta_mgr_->micro_meta_map_.reset();

  // 6. repeat step 1
  ObArray<ObSSMicroBlockMeta *> micro_meta_arr2;
  macro_start_idx += write_blk_cnt;
  write_blk_cnt = 20;
  ASSERT_EQ(OB_SUCCESS, add_batch_micro_block(macro_start_idx, write_blk_cnt, micro_cnt, micro_meta_arr2));
  const int64_t used_data_blk_cnt2 = phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_;

  // 7. do ckpt_task second round and randomly force to end micro_ckpt_task and restart micro_cache
  micro_ckpt_task_->is_inited_ = true;
  blk_ckpt_task_->is_inited_ = true;
  micro_ckpt_task_->interval_us_ = 3600 * 1000 * 1000L;
  blk_ckpt_task_->interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(1000 * 1000);

  micro_ckpt_task_->ckpt_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  micro_ckpt_task_->ckpt_op_.micro_ckpt_ctx_.need_scan_blk_ = true;
  ObSSMicroCacheSuperBlock super_block2 = phy_blk_mgr_->super_block_;

  std::thread micro_t([&]() {
    ObRandom rand;
    const int64_t sleep_us = ObRandom::rand(1, micro_exe_time_us * 2);
    ob_usleep(sleep_us);
    micro_ckpt_task_->ckpt_op_.is_inited_ = false; // force to end micro_ckpt
  });
  micro_ckpt_task_->ckpt_op_.gen_checkpoint();
  micro_t.join();
  ObSSMicroCacheSuperBlock super_block3 = phy_blk_mgr_->super_block_;

  LOG_INFO("TEST: start second restart");
  restart_micro_cache();
  LOG_INFO("TEST: finish second restart");

  // 8. check super_block, micro_meta and phy_block_info
  if (phy_blk_mgr_->super_block_.modify_time_us_ == super_block2.modify_time_us_) {
    check_super_block(super_block2);
    check_micro_meta(micro_meta_arr1, true);
    check_micro_meta(micro_meta_arr2, false);
    ASSERT_EQ(used_data_blk_cnt1, phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_);
  } else {
    check_super_block(super_block3);
    check_micro_meta(micro_meta_arr1, false);
    check_micro_meta(micro_meta_arr2, true);
    ASSERT_EQ(used_data_blk_cnt2 - used_data_blk_cnt1, phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_);
  }
}

/* After micro_ckpt_task execucte scan_blocks_to_reuse, must set need_scan_blk_ = false */
TEST_F(TestSSExecuteCheckpointTask, test_micro_ckpt_task_exec_scan_block)
{
  LOG_INFO("TEST: test_micro_ckpt_task_exec_scan_block");
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSExecuteMicroCheckpointTask &micro_ckpt_task = micro_cache->task_runner_.micro_ckpt_task_;
  ObSSExecuteBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  blk_ckpt_task.is_inited_ = false;

  const int64_t block_cnt = 10;
  for (int64_t i = 0; i < block_cnt; i++) {
    int64_t phy_blk_idx = -1;
    ObSSPhysicalBlockHandle phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(phy_blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
    phy_blk_handle()->is_free_ = false;
    phy_blk_handle()->is_sealed_ = true;
    phy_blk_handle()->valid_len_ = 0;
  }
  micro_ckpt_task.ckpt_op_.ckpt_ctx_->exe_round_ = ObSSExecuteMicroCheckpointOp::SCAN_BLOCK_INTERVAL_ROUND - 1;

  ob_usleep(3 * 1000 * 1000);
  ASSERT_EQ(block_cnt, phy_blk_mgr.get_reusable_set_count());
  ASSERT_EQ(false, micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.need_scan_blk_);
}

/* Test whether extra meta blocks can be dynamically allocated when the number of blocks required
 * by micro_ckpt exceeds meta_blk.min_cnt_ */
TEST_F(TestSSExecuteCheckpointTask, test_reserve_micro_ckpt_blk)
{
  LOG_INFO("TEST_CASE: start test_reserve_micro_ckpt_blk");
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;

  ObSSExecuteMicroCheckpointTask &micro_ckpt_task = micro_cache->task_runner_.micro_ckpt_task_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  SSPhyBlockCntInfo &blk_cnt_info = micro_cache->phy_blk_mgr_.blk_cnt_info_;

  // 1. write enough micro blocks so that the number of blocks required to execute micro_ckpt exceeds meta_blk.min_cnt_
  const int64_t extra_meta_blk_cnt = 5;
  const int64_t estimate_micro_cnt =
      (blk_cnt_info.meta_blk_.hold_cnt_ + extra_meta_blk_cnt) * phy_blk_mgr.get_block_size() / AVG_MICRO_META_PERSIST_COST;
  const int64_t macro_cnt = blk_cnt_info.data_blk_.hold_cnt_ * 0.2;
  const int64_t micro_cnt = estimate_micro_cnt / macro_cnt;
  ObArray<ObSSMicroBlockMeta *> micro_meta_arr;
  ASSERT_EQ(OB_SUCCESS, add_batch_micro_block(1, macro_cnt, micro_cnt, micro_meta_arr));

  // 2. mock shared_block is used up
  blk_cnt_info.data_blk_.hold_cnt_ = blk_cnt_info.data_blk_.max_cnt_;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;

  // 3. execute micro_meta_ckpt
  ASSERT_EQ(0, blk_cnt_info.meta_blk_.used_cnt_);
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = ObSSExecuteMicroCheckpointOp::MICRO_META_CKPT_INTERVAL_ROUND - 1;
  ob_usleep(10 * 1000 * 1000);

  ASSERT_EQ(blk_cnt_info.meta_blk_.hold_cnt_, blk_cnt_info.meta_blk_.used_cnt_);
  ASSERT_EQ(blk_cnt_info.meta_blk_.used_cnt_, phy_blk_mgr.super_block_.micro_ckpt_entry_list_.count());

  // 4. revert step2
  blk_cnt_info.data_blk_.hold_cnt_ = blk_cnt_info.data_blk_.min_cnt_;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;

  // 5. execute micro_meta_ckpt again, check that the number of blocks used by micro_ckpt has increased
  const int64_t origin_meta_blk_usd_cnt = blk_cnt_info.meta_blk_.used_cnt_;
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = ObSSExecuteMicroCheckpointOp::ESTIMATE_MICRO_CKPT_BLK_CNT_ROUND - 1;
  ob_usleep(2 * 1000 * 1000);
  ASSERT_LT(2 * origin_meta_blk_usd_cnt, blk_cnt_info.meta_blk_.hold_cnt_);

  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = ObSSExecuteMicroCheckpointOp::MICRO_META_CKPT_INTERVAL_ROUND - 1;
  ob_usleep(10 * 1000 * 1000);
  ASSERT_EQ(blk_cnt_info.meta_blk_.used_cnt_, phy_blk_mgr.super_block_.micro_ckpt_entry_list_.count());
  ASSERT_LT(origin_meta_blk_usd_cnt, blk_cnt_info.meta_blk_.used_cnt_);
  ASSERT_LT(origin_meta_blk_usd_cnt, phy_blk_mgr.super_block_.micro_ckpt_entry_list_.count());
}


TEST_F(TestSSExecuteCheckpointTask, test_dynamic_update_arc_limit)
{
  LOG_INFO("TEST_CASE: start test_dynamic_update_arc_limit");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSARCInfo &arc_info = micro_cache->micro_meta_mgr_.arc_info_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  SSPhyBlockCntInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  ObSSExecuteMicroCheckpointOp &micro_ckpt_op = micro_cache->task_runner_.micro_ckpt_task_.ckpt_op_;

  const int64_t origin_limit = arc_info.limit_;
  const int64_t origin_work_limit = arc_info.work_limit_;
  const int64_t origin_p = arc_info.p_;
  const int64_t origin_max_p = arc_info.max_p_;
  const int64_t origin_min_p = arc_info.min_p_;

  // mock micro_ckpt use extra block from shared_blocks.
  // this will result in fewer available cache_data_blocks and dynamically lower arc_limit.
  blk_cnt_info.meta_blk_.hold_cnt_ = blk_cnt_info.meta_blk_.max_cnt_;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.meta_blk_.hold_cnt_ + blk_cnt_info.data_blk_.hold_cnt_;
  micro_ckpt_op.dynamic_update_arc_limit();

  ASSERT_GT(origin_limit, arc_info.limit_);
  ASSERT_GT(origin_work_limit, arc_info.work_limit_);
  ASSERT_GT(origin_p, arc_info.p_);
  ASSERT_GT(origin_max_p, arc_info.max_p_);
  ASSERT_GT(origin_min_p, arc_info.min_p_);

  blk_cnt_info.meta_blk_.hold_cnt_ = blk_cnt_info.meta_blk_.min_cnt_;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.meta_blk_.hold_cnt_ + blk_cnt_info.data_blk_.hold_cnt_;

  micro_ckpt_op.dynamic_update_arc_limit();
  ASSERT_EQ(origin_limit, arc_info.limit_);
  ASSERT_EQ(origin_work_limit, arc_info.work_limit_);
  ASSERT_LE(abs(origin_p - arc_info.p_), 5); // the conversion between 'int64_t' and 'double' causes some deviations
  ASSERT_EQ(origin_max_p, arc_info.max_p_);
  ASSERT_EQ(origin_min_p, arc_info.min_p_);

  // test dynamically update arc_limit when prewarming
  micro_cache->begin_free_space_for_prewarm();

  const int64_t prewarm_work_limit = static_cast<int64_t>((static_cast<double>(origin_work_limit * SS_ARC_LIMIT_SHRINK_PCT) / 100.0));
  double ori_pct = static_cast<double>(origin_p * 100) / origin_work_limit;
  const int64_t p1 = static_cast<int64_t>((static_cast<double>(ori_pct * prewarm_work_limit) / 100.0));
  ASSERT_EQ(prewarm_work_limit, arc_info.work_limit_);
  ASSERT_EQ(origin_limit, arc_info.limit_);
  ASSERT_LE(abs(p1 - arc_info.p_), 5); // the conversion between 'int64_t' and 'double' causes some deviations

  // mock micro_ckpt use extra block from shared_blocks.
  // this will result in fewer available cache_data_blocks and dynamically lower arc_limit.
  blk_cnt_info.meta_blk_.hold_cnt_ = blk_cnt_info.meta_blk_.max_cnt_;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.meta_blk_.hold_cnt_ + blk_cnt_info.data_blk_.hold_cnt_;

  micro_ckpt_op.dynamic_update_arc_limit();

  const int64_t new_limit = phy_blk_mgr.get_cache_limit_size() * SS_ARC_LIMIT_PCT / 100;
  const int64_t new_work_limit = (static_cast<double>(prewarm_work_limit) / origin_limit) * new_limit;
  ori_pct = static_cast<double>(p1 * 100) / prewarm_work_limit;
  const int64_t p2 = static_cast<int64_t>((static_cast<double>(new_work_limit * ori_pct) / 100.0));
  ASSERT_EQ(new_limit, arc_info.limit_);
  ASSERT_EQ(new_work_limit, arc_info.work_limit_);
  ASSERT_LE(abs(p2 - arc_info.p_), 5); // the conversion between 'int64_t' and 'double' causes some deviations

  micro_cache->finish_free_space_for_prewarm();

  ori_pct = static_cast<double>(p2 * 100) / new_work_limit;
  const int64_t p3 = static_cast<int64_t>((static_cast<double>(new_limit * ori_pct) / 100.0));
  ASSERT_EQ(new_limit, arc_info.work_limit_);
  ASSERT_EQ(new_limit, arc_info.limit_);
  ASSERT_LE(abs(p3 - arc_info.p_), 5); // the conversion between 'int64_t' and 'double' causes some deviations
}

TEST_F(TestSSExecuteCheckpointTask, test_estimate_micro_meta_persist_cost)
{
  LOG_INFO("TEST_CASE: start test_estimate_micro_meta_persist_cost");
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  ObSSExecuteMicroCheckpointTask &micro_ckpt_task = micro_cache->task_runner_.micro_ckpt_task_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  SSPhyBlockCntInfo &blk_cnt_info = micro_cache->phy_blk_mgr_.blk_cnt_info_;

  const int64_t estimate_micro_cnt =
      (blk_cnt_info.meta_blk_.hold_cnt_) * phy_blk_mgr.get_block_size() / AVG_MICRO_META_PERSIST_COST;
  const int64_t macro_cnt = blk_cnt_info.data_blk_.hold_cnt_ * 0.2;
  const int64_t micro_cnt = estimate_micro_cnt / macro_cnt;
  ObArray<ObSSMicroBlockMeta *> micro_meta_arr;
  ASSERT_EQ(OB_SUCCESS, add_batch_micro_block(1, macro_cnt, micro_cnt, micro_meta_arr));

  // 1. execute micro_meta_ckpt
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = ObSSExecuteMicroCheckpointOp::MICRO_META_CKPT_INTERVAL_ROUND - 1;
  ob_usleep(10 * 1000 * 1000);

  // 2. read ckpt_blk
  ObIArray<int64_t> &meta_blk_arr = phy_blk_mgr.super_block_.micro_ckpt_entry_list_;
  ASSERT_LT(0, meta_blk_arr.count());
  ObSSLinkedPhyBlockReader block_reader;
  const int64_t entry_block_idx = meta_blk_arr.at(0);
  ASSERT_EQ(OB_SUCCESS, block_reader.init(entry_block_idx, MTL_ID(), phy_blk_mgr));

  int64_t total_item_cnt = 0;
  // skip first block, because it is the last written block when execute micro_ckpt
  for (int64_t i = 1; i < meta_blk_arr.count(); i++) {
    const int64_t block_idx = meta_blk_arr.at(i);
    ASSERT_EQ(OB_SUCCESS, block_reader.inner_read_block_(block_idx));
    total_item_cnt += block_reader.linked_header_.item_count_;
    LOG_INFO("print block header", K(i), K(block_idx), K_(block_reader.common_header), K_(block_reader.linked_header));
  }

  const int64_t total_blk_cnt = meta_blk_arr.count() - 1;
  double avg_cost = static_cast<double>(total_blk_cnt * phy_blk_mgr.get_block_size()) / total_item_cnt;
  std::cout << "avg_meta_persist_cost: " << avg_cost << ", total_meta_blk_cnt: " << total_blk_cnt << std::endl;
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_execute_checkpoint_task.log*");
  OB_LOGGER.set_file_name("test_ss_execute_checkpoint_task.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
