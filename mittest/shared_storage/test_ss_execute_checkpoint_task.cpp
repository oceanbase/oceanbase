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
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_reader.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_writer.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
class TestSSExecuteCheckpointTask : public ::testing::Test
{
public:
  TestSSExecuteCheckpointTask()
    : allocator_(), micro_cache_(nullptr), mem_blk_mgr_(nullptr), phy_blk_mgr_(nullptr), micro_meta_mgr_(nullptr),
      release_cache_task_(nullptr), persist_meta_task_(nullptr), blk_ckpt_task_(nullptr), persist_data_task_(nullptr) {}
  virtual ~TestSSExecuteCheckpointTask() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

  int restart_micro_cache();
  int add_batch_micro_block(const int64_t start_idx, const int64_t macro_cnt, const int64_t micro_cnt,
                            ObArray<ObSSMicroBlockMetaInfo> &micro_meta_info_arr);
  int alloc_batch_phy_block_to_reuse(const int64_t total_cnt, ObArray<ObSSPhyBlockInfoCkptItem> &phy_block_arr);
  int check_micro_meta(const ObArray<ObSSMicroBlockMetaInfo> &micro_meta_info_arr, const bool is_exist);
  int check_phy_blk_info(const ObArray<ObSSPhyBlockInfoCkptItem> &phy_block_arr, const int64_t increment);
  int check_super_block(const ObSSMicroCacheSuperBlk &super_blk);
public:
  ObArenaAllocator allocator_;
  ObSSMicroCache *micro_cache_;
  ObSSMemBlockManager *mem_blk_mgr_;
  ObSSPhysicalBlockManager *phy_blk_mgr_;
  ObSSMicroMetaManager *micro_meta_mgr_;
  ObSSReleaseCacheTask *release_cache_task_;
  ObSSPersistMicroMetaTask *persist_meta_task_;
  ObSSDoBlkCheckpointTask *blk_ckpt_task_;
  ObSSPersistMicroDataTask *persist_data_task_;
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
  mem_blk_mgr_ = &micro_cache_->mem_blk_mgr_;
  ASSERT_NE(nullptr, mem_blk_mgr_);
  phy_blk_mgr_ = &micro_cache_->phy_blk_mgr_;
  ASSERT_NE(nullptr, phy_blk_mgr_);
  micro_meta_mgr_ = &micro_cache_->micro_meta_mgr_;
  ASSERT_NE(nullptr, micro_meta_mgr_);
  release_cache_task_ = &micro_cache_->task_runner_.release_cache_task_;
  ASSERT_NE(nullptr, release_cache_task_);
  persist_meta_task_ = &micro_cache_->task_runner_.persist_meta_task_;
  ASSERT_NE(nullptr, persist_meta_task_);
  blk_ckpt_task_ = &micro_cache_->task_runner_.blk_ckpt_task_;
  ASSERT_NE(nullptr, blk_ckpt_task_);
  persist_data_task_ =  &micro_cache_->task_runner_.persist_data_task_;
  ASSERT_NE(nullptr, persist_data_task_);
}

void TestSSExecuteCheckpointTask::TearDown()
{
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  // clear micro_meta_ckpt
  ObSSMicroCacheSuperBlk new_super_blk(tenant_id, micro_cache->cache_file_size_);
  ASSERT_EQ(OB_SUCCESS, micro_cache->phy_blk_mgr_.update_ss_super_block(new_super_blk));
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

int TestSSExecuteCheckpointTask::restart_micro_cache()
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
  micro_cache_->stop();
  micro_cache_->wait();
  micro_cache_->destroy();
  tnt_file_mgr->is_cache_file_exist_ = true;

  if (OB_FAIL(micro_cache_->init(MTL_ID(), (1L << 32)))) {
    LOG_WARN("fail to init micro_cache", KR(ret));
  } else {
    micro_cache_->start();
    persist_meta_task_->is_inited_ = false;
    blk_ckpt_task_->is_inited_ = false;
    release_cache_task_->is_inited_ = false;
    persist_data_task_->is_inited_ = false;
  }
  return ret;
}

int TestSSExecuteCheckpointTask::add_batch_micro_block(
    const int64_t start_idx,
    const int64_t macro_cnt,
    const int64_t micro_cnt,
    ObArray<ObSSMicroBlockMetaInfo> &micro_meta_info_arr)
{
  int ret = OB_SUCCESS;
  persist_data_task_->is_inited_ = true;
  ob_usleep(100 * 1000);
  const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSMicroDataBlockHeader::get_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t block_size = phy_blk_mgr_->get_block_size();
  const int32_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
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
        if (OB_FAIL(micro_cache_->add_micro_block_cache(micro_key, data_buf, micro_size,
            ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
          LOG_WARN("fail to add micro_block", KR(ret), K(micro_key), KP(data_buf), K(micro_size));
        } else if (OB_FAIL(micro_key_arr.push_back(micro_key))) {
          LOG_WARN("fail to push micro_key", KR(ret), K(micro_key));
        }
      }
      if (FAILEDx(TestSSCommonUtil::wait_for_persist_task())) {
        LOG_WARN("fail to wait for persist task", K(ret));
      }
    }
    persist_data_task_->is_inited_ = false;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < micro_key_arr.count(); ++i) {
    ObSSMicroBlockMetaHandle micro_handle;
    const ObSSMicroBlockCacheKey &micro_key = micro_key_arr[i];
    if (OB_FAIL(micro_meta_mgr_->get_micro_block_meta(micro_key, micro_handle, false))) {
      LOG_WARN("fail to get micro_handle", KR(ret), K(micro_key));
    } else if (OB_UNLIKELY(!micro_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_handle is invalid", KR(ret), K(micro_handle), KPC(micro_handle.get_ptr()));
    } else {
      ObSSMicroBlockMetaInfo micro_meta_info;
      micro_handle()->get_micro_meta_info(micro_meta_info);
      if (OB_FAIL(micro_meta_info_arr.push_back(micro_meta_info))) {
        LOG_WARN("fail to push back", KR(ret), K(micro_meta_info));
      }
    }
  }
  return ret;
}

int TestSSExecuteCheckpointTask::alloc_batch_phy_block_to_reuse(
    const int64_t total_cnt,
    ObArray<ObSSPhyBlockInfoCkptItem> &phy_block_arr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(total_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(total_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; i++) {
      int64_t block_idx = 0;
      ObSSPhyBlockHandle phy_blk_handle;
      if (OB_FAIL(phy_blk_mgr_->alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK))) {
        LOG_WARN("fail to alloc block", KR(ret), K(block_idx), K(phy_blk_handle));
      } else if (OB_UNLIKELY(!phy_blk_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_blk_handle is invalid", KR(ret), K(phy_blk_handle));
      } else {
        phy_blk_handle.get_ptr()->alloc_time_s_ -= SS_FREE_BLK_MIN_REUSE_TIME_S;  // mock block is reusable
        ObSSPhyBlockInfoCkptItem phy_info(block_idx, phy_blk_handle.get_ptr()->get_reuse_version());
        if (OB_FAIL(phy_block_arr.push_back(phy_info))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to push phy_info", KR(ret), K(phy_info));
        }
      }
    }
  }
  return ret;
}

int TestSSExecuteCheckpointTask::check_micro_meta(
    const ObArray<ObSSMicroBlockMetaInfo> &micro_meta_info_arr,
    const bool is_exist)
{
  int ret = OB_SUCCESS;
  const int64_t micro_cnt = micro_meta_info_arr.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < micro_cnt; ++i) {
    const ObSSMicroBlockMetaInfo &micro_meta_info = micro_meta_info_arr[i];
    const ObSSMicroBlockCacheKey &micro_key = micro_meta_info.get_micro_key();
    ObSSMicroBlockMetaHandle micro_handle;
    if (OB_FAIL(micro_meta_mgr_->get_micro_block_meta(micro_key, micro_handle, false))) {
      LOG_WARN("fail to get micro_block meta", KR(ret), K(i), K(micro_cnt), K(micro_key));
    }

    if (is_exist == true) {
      if (micro_meta_info.is_data_persisted()) {
        if (OB_SUCC(ret)) {
          ObSSMicroBlockMeta *micro_meta = micro_handle.get_ptr();
          if (OB_UNLIKELY(!micro_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("micro_meta handle should be valid", KR(ret), K(i));
          } else if (OB_UNLIKELY(micro_meta->reuse_version_ != micro_meta_info.reuse_version_ ||
                                 micro_meta->data_loc_ != micro_meta_info.data_loc_ ||
                                 micro_meta->size_ != micro_meta_info.size_ ||
                                 micro_meta->is_in_l1_ != micro_meta_info.is_in_l1_ ||
                                 micro_meta->is_in_ghost_ != micro_meta_info.is_in_ghost_ ||
                                 micro_meta->is_data_persisted_ != micro_meta_info.is_data_persisted_ ||
                                 micro_meta->is_meta_persisted_ != micro_meta_info.is_meta_persisted_ ||
                                 micro_meta->is_meta_dirty_ != micro_meta_info.is_meta_dirty_ ||
                                 micro_meta->is_meta_partial_ != micro_meta_info.is_meta_partial_ ||
                                 micro_meta->is_meta_deleted_ != micro_meta_info.is_meta_deleted_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected micro meta", KR(ret), K(i), K(micro_meta_info), KPC(micro_meta));
          }
        } else {
          LOG_WARN("unexpected", KR(ret), K(micro_key), K(i), K(micro_cnt), K(micro_meta_info));
        }
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int TestSSExecuteCheckpointTask::check_phy_blk_info(
    const ObArray<ObSSPhyBlockInfoCkptItem> &phy_block_info_arr,
    const int64_t reuse_version_delta)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < phy_block_info_arr.count(); ++i) {
    ObSSPhyBlockHandle phy_blk_handle;
    const int64_t block_idx = phy_block_info_arr[i].blk_idx_;
    const int64_t old_reuse_version = phy_block_info_arr[i].reuse_version_;
    if (OB_FAIL(phy_blk_mgr_->get_block_handle(block_idx, phy_blk_handle))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get block handle", KR(ret), K(block_idx));
    } else if (OB_UNLIKELY(!phy_blk_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block handle should be valid", KR(ret), K(block_idx));
    } else {
      const int64_t reuse_version = phy_blk_handle.get_ptr()->get_reuse_version();
      if (OB_UNLIKELY(old_reuse_version + reuse_version_delta != reuse_version)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected reuse_version", KR(ret), K(old_reuse_version), K(reuse_version_delta), K(reuse_version), K(block_idx));
      }
    }
  }
  return ret;
}

int TestSSExecuteCheckpointTask::check_super_block(const ObSSMicroCacheSuperBlk &super_blk)
{
  int ret = OB_SUCCESS;
  const ObSSMicroCacheSuperBlk &cur_super_blk = phy_blk_mgr_->super_blk_;
  if (OB_UNLIKELY(cur_super_blk.micro_ckpt_time_us_ != super_blk.micro_ckpt_time_us_ ||
                  cur_super_blk.cache_file_size_ != super_blk.cache_file_size_ ||
                  cur_super_blk.modify_time_us_ != super_blk.modify_time_us_ ||
                  cur_super_blk.micro_ckpt_entry_list_.count() != super_blk.micro_ckpt_entry_list_.count() ||
                  cur_super_blk.blk_ckpt_entry_list_.count() != super_blk.blk_ckpt_entry_list_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected super block", KR(ret), K(cur_super_blk), K(super_blk));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < super_blk.micro_ckpt_entry_list_.count(); ++i) {
      if (OB_UNLIKELY(cur_super_blk.micro_ckpt_entry_list_[i] != super_blk.micro_ckpt_entry_list_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected super block", KR(ret), K(i), K(cur_super_blk), K(super_blk), K(cur_super_blk.micro_ckpt_entry_list_[i]),
          K(super_blk.micro_ckpt_entry_list_[i]));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < super_blk.blk_ckpt_entry_list_.count(); ++i) {
      if (OB_UNLIKELY(cur_super_blk.blk_ckpt_entry_list_[i] != super_blk.blk_ckpt_entry_list_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected super block", KR(ret), K(i), K(cur_super_blk), K(super_blk), K(cur_super_blk.blk_ckpt_entry_list_[i]),
          K(super_blk.blk_ckpt_entry_list_[i]));
      }
    }
  }
  return ret;
}

/* This case tests the basic logic of the execute_checkpoint task. */
TEST_F(TestSSExecuteCheckpointTask, test_execute_checkpoint_task)
{
  LOG_INFO("TEST: start test_execute_checkpoint_task");
  int ret = OB_SUCCESS;
  ASSERT_NE(nullptr, micro_cache_);
  const int64_t block_size = micro_cache_->phy_blk_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;

  persist_meta_task_->is_inited_ = false;
  blk_ckpt_task_->is_inited_ = false;
  release_cache_task_->is_inited_ = false;

  ObSSARCInfo &arc_info = micro_meta_mgr_->arc_info_;
  const int64_t ori_arc_limit = micro_meta_mgr_->get_arc_info().limit_;

  // 1. allow manully invoke blk_checkpoint function
  persist_meta_task_->is_inited_ = true;
  persist_meta_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  blk_ckpt_task_->is_inited_ = true;
  blk_ckpt_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(1000 * 1000);

  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.is_valid());
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.is_valid());
  ASSERT_EQ(0, phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr_->reusable_blks_.size());
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());

  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_ - 2, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(true, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.is_valid());
  ASSERT_EQ(0, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.micro_ckpt_entry_list_.count());
  int64_t blk_ckpt_list_cnt = blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_entry_list_.count();
  ASSERT_LT(0, blk_ckpt_list_cnt);
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_, blk_ckpt_list_cnt);
  ObSEArray<int64_t, 8> blk_ckpt_entry_list;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_entry_list.assign(blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_entry_list_));

  // 2. execute phy_block checkpoint
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.reuse();
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.is_valid());
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.is_valid());
  ASSERT_EQ(0, phy_blk_mgr_->reusable_blks_.size());
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());

  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_ - 2, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(true, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.is_valid());
  ASSERT_EQ(true, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.is_valid());
  ASSERT_EQ(0, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(blk_ckpt_list_cnt, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_, blk_ckpt_list_cnt);

  for (int64_t i = 0; i < blk_ckpt_entry_list.count(); ++i) {
    const int64_t blk_idx = blk_ckpt_entry_list.at(i);
    ObSSPhysicalBlock *phy_blk = phy_blk_mgr_->inner_get_phy_block_by_idx(blk_idx);
    ASSERT_NE(nullptr, phy_blk);
    ASSERT_EQ(true, phy_blk->is_free_);
    ASSERT_EQ(2, phy_blk->reuse_version_);
    ASSERT_EQ(0, phy_blk->valid_len_);
  }
  ASSERT_NE(blk_ckpt_entry_list.at(0), blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_entry_list_.at(0));
  blk_ckpt_entry_list.reset();
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_entry_list.assign(blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_entry_list_));
  ASSERT_EQ(blk_ckpt_list_cnt, blk_ckpt_entry_list.count());

  // 3. make some micro_data
  const int64_t max_micro_data_blk_cnt = phy_blk_mgr_->blk_cnt_info_.micro_data_blk_max_cnt();
  const int64_t WRITE_BLK_CNT = 50;
  ASSERT_LT(WRITE_BLK_CNT, max_micro_data_blk_cnt);

  const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                 ObSSMicroDataBlockHeader::get_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 100;
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
      ret = micro_cache_->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ASSERT_NE(nullptr, mem_blk_mgr_->fg_mem_blk_);
    ASSERT_EQ(true, mem_blk_mgr_->fg_mem_blk_->is_valid()) << i;
    ASSERT_EQ(micro_cnt, mem_blk_mgr_->fg_mem_blk_->micro_cnt_) << i;
    ASSERT_EQ(micro_cnt, mem_blk_mgr_->fg_mem_blk_->micro_cnt_);
    ASSERT_EQ(micro_size * micro_cnt, mem_blk_mgr_->fg_mem_blk_->valid_val_);
    ASSERT_EQ(micro_cnt, mem_blk_mgr_->fg_mem_blk_->valid_cnt_);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  {
    // to sealed the last mem_block
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(WRITE_BLK_CNT + 1);
    const int32_t offset = payload_offset;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    ret = micro_cache_->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // record written phy_block count
  const int64_t max_retry_cnt = 10;
  bool result_match = false;
  for (int64_t i = 0; !result_match && i < max_retry_cnt; ++i) {
    result_match = (phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_ == WRITE_BLK_CNT);
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
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr_->micro_meta_map_.get(&micro_key, micro_meta_handle));
      ObSSMicroBlockMeta *micro_meta = micro_meta_handle.get_ptr();
      ASSERT_NE(nullptr, micro_meta);
      ASSERT_EQ(true, micro_meta->is_in_l1_);
      ASSERT_EQ(false, micro_meta->is_in_ghost_);
      ASSERT_EQ(true, micro_meta->is_data_persisted_);

      ObSSMicroBlockMetaInfo evict_micro_info;
      micro_meta_handle()->get_micro_meta_info(evict_micro_info);
      ASSERT_EQ(true, evict_micro_info.is_valid_field());

      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr_->try_evict_micro_block_meta(evict_micro_info));
      ASSERT_EQ(true, micro_meta->is_in_ghost_);
      ASSERT_EQ(false, micro_meta->is_valid_field());
      int64_t phy_blk_idx = -1;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->update_block_valid_length(evict_micro_info.data_loc_, evict_micro_info.reuse_version_,
        evict_micro_info.size_ * -1, phy_blk_idx));
      evict_blk_idx = evict_micro_info.data_loc_ / phy_blk_mgr_->block_size_;
    }
  }
  ASSERT_NE(-1, evict_blk_idx);
  ASSERT_EQ(((WRITE_BLK_CNT - 1) * micro_cnt + 1) * micro_size, arc_info.get_valid_size());
  ASSERT_EQ(0, cache_stat.micro_stat().mark_del_micro_cnt_);
  ASSERT_EQ(0, cache_stat.micro_stat().mark_del_micro_size_);

  // 3.3. force delete all micro_block of the second macro_block
  int64_t force_del_blk_idx = -1;
  for (int64_t i = 1; i < 2; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr_->get_micro_block_meta(micro_key, micro_meta_handle, false));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      ObSSMicroBlockMeta *micro_meta = micro_meta_handle.get_ptr();

      ObSSMicroBlockMetaInfo delete_micro_info;
      micro_meta_handle()->get_micro_meta_info(delete_micro_info);
      ASSERT_EQ(true, delete_micro_info.is_valid_field());

      bool succ_delete = true;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr_->try_force_delete_micro_block_meta(micro_key, succ_delete));
      ASSERT_EQ(true, succ_delete);
      int64_t phy_blk_idx = -1;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->update_block_valid_length(delete_micro_info.data_loc_, delete_micro_info.reuse_version_,
        delete_micro_info.size_ * -1, phy_blk_idx));
      force_del_blk_idx = delete_micro_info.data_loc_ / phy_blk_mgr_->block_size_;
    }
  }
  ASSERT_NE(-1, force_del_blk_idx);
  ASSERT_EQ(micro_cnt, cache_stat.micro_stat().mark_del_micro_cnt_);
  ASSERT_EQ(micro_cnt * micro_size, cache_stat.micro_stat().mark_del_micro_size_);

  // 3.4. check micro_meta and macro_meta count
  ASSERT_EQ(WRITE_BLK_CNT * micro_cnt + 1, micro_meta_mgr_->micro_meta_map_.count());

  // 4. execute persist micro_meta
  // 4.1. check ckpt state
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.exe_round_ = persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.ckpt_round_;
  persist_meta_task_->is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.check_state());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->get_ss_super_block(persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_));
  ASSERT_EQ(true, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_);
  ASSERT_EQ(0, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.cur_super_blk_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(0, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.cur_super_blk_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(blk_ckpt_list_cnt, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(0, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_.micro_ckpt_entry_list_.count());

  // 4.2. scan reusable phy_blocks
  ASSERT_EQ(2, phy_blk_mgr_->reusable_blks_.size());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->scan_blocks_to_reuse());
  ASSERT_EQ(2, phy_blk_mgr_->reusable_blks_.size());

  // 4.3. first, mock all reserved blk were used up, will fail to execute persist micro_meta
  const int64_t ori_micro_blk_used_cnt = phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_;
  const int64_t ori_micro_blk_hold_cnt = phy_blk_mgr_->blk_cnt_info_.meta_blk_.hold_cnt_;
  const int64_t ori_micro_blk_max_cnt = phy_blk_mgr_->blk_cnt_info_.meta_blk_.max_cnt_;
  phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_ = ori_micro_blk_max_cnt;
  phy_blk_mgr_->blk_cnt_info_.meta_blk_.hold_cnt_ = ori_micro_blk_max_cnt;
  phy_blk_mgr_->blk_cnt_info_.shared_blk_used_cnt_ =
      phy_blk_mgr_->blk_cnt_info_.meta_blk_.hold_cnt_ + phy_blk_mgr_->blk_cnt_info_.data_blk_.hold_cnt_;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.gen_micro_meta_checkpoint());
  ASSERT_EQ(true, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.lack_phy_blk_);
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt + 1, micro_meta_mgr_->micro_meta_map_.count());

  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.lack_phy_blk_ = false;
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.ckpt_item_cnt_ = 0;
  persist_meta_task_->persist_meta_op_.tablet_info_map_.clear();
  ASSERT_EQ(((WRITE_BLK_CNT - 2) * micro_cnt + 1) * micro_size, arc_info.get_valid_size());

  phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_ = ori_micro_blk_used_cnt;
  phy_blk_mgr_->blk_cnt_info_.meta_blk_.hold_cnt_ = ori_micro_blk_hold_cnt;
  phy_blk_mgr_->blk_cnt_info_.shared_blk_used_cnt_ =
      phy_blk_mgr_->blk_cnt_info_.meta_blk_.hold_cnt_ + phy_blk_mgr_->blk_cnt_info_.data_blk_.hold_cnt_;

  // 4.4. normal situation
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.gen_micro_meta_checkpoint());
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt + 1, micro_meta_mgr_->micro_meta_map_.count());
  ASSERT_EQ(false, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.lack_phy_blk_);
  ASSERT_LT(0, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.cur_super_blk_.micro_ckpt_entry_list_.count());
  ASSERT_LT(0, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.cur_super_blk_.micro_ckpt_time_us_);
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_LT(0, persist_meta_task_->persist_meta_op_.tablet_info_map_.size());

  ObSSMCTabletInfoMap::const_iterator iter = persist_meta_task_->persist_meta_op_.tablet_info_map_.begin();
  int64_t total_micro_size = 0;
  int64_t tablet_cnt = 0;
  for (; iter != persist_meta_task_->persist_meta_op_.tablet_info_map_.end(); ++iter) {
    total_micro_size += iter->second.get_valid_size();
    ++tablet_cnt;
  }
  ASSERT_EQ(arc_info.get_valid_size(), total_micro_size);

  // 4.5. update ss_super_block
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.update_super_block(true));
  ObSSMicroCacheSuperBlk cur_super_blk;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->get_ss_super_block(cur_super_blk));
  ASSERT_LT(0, cur_super_blk.micro_ckpt_entry_list_.count());
  ASSERT_EQ(blk_ckpt_entry_list.at(0), cur_super_blk.blk_ckpt_entry_list_.at(0));
  ObSEArray<int64_t, 8> micro_ckpt_entry_list;
  ASSERT_EQ(OB_SUCCESS, micro_ckpt_entry_list.assign(cur_super_blk.micro_ckpt_entry_list_));
  int64_t micro_ckpt_list_cnt = micro_ckpt_entry_list.count();

  // 4.6. finish checkpoint
  int64_t free_blk_cnt = 0;
  ASSERT_EQ(OB_SUCCESS,
      phy_blk_mgr_->try_free_batch_blocks(persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_.micro_ckpt_entry_list_, free_blk_cnt));
  ASSERT_EQ(free_blk_cnt, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_.micro_ckpt_entry_list_.count());

  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.exe_round_ = 0;

  // TODO @donglou.zl rebuild below logic
  // 4.7. read micro_block checkpoint, and check it
  // 4.7.1 clear some memory state
  micro_meta_mgr_->micro_meta_map_.clear();
  ASSERT_EQ(0, micro_meta_mgr_->micro_meta_map_.count());
  int64_t micro_data_blk_cnt = 0;
  for (int64_t i = 2; i < phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_; ++i) {
    ObSSPhysicalBlock *phy_blk = phy_blk_mgr_->inner_get_phy_block_by_idx(i);
    ASSERT_NE(nullptr, phy_blk);
    if (phy_blk->get_block_type() == ObSSPhyBlockType::SS_MICRO_DATA_BLK) {
      phy_blk->valid_len_ = 0;
      ++micro_data_blk_cnt;
    }
  }
  ASSERT_EQ(WRITE_BLK_CNT, micro_data_blk_cnt);
  ObSSMicroRangeManager *micro_range_mgr = micro_meta_mgr_->micro_range_mgr_;
  ASSERT_NE(nullptr, micro_range_mgr);
  ObSSMicroRangeInfo **range_info_arr = micro_range_mgr->get_range_info_arr();
  const int64_t init_range_cnt = micro_range_mgr->get_init_range_cnt();
  int64_t range_micro_sum = 0;
  for (int64_t i = 0; i < init_range_cnt; ++i) {
    range_micro_sum += range_info_arr[i]->micro_meta_cnt_;
    range_info_arr[i]->micro_meta_cnt_ = 0;
    range_info_arr[i]->micro_header_ = nullptr;
  }
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt + 1, range_micro_sum); // not include force_deleted micro_metas

  // 4.7.2 read micro_meta ckpt
  ASSERT_EQ(OB_SUCCESS, micro_cache_->read_micro_meta_checkpoint(cur_super_blk.micro_ckpt_entry_list_.at(0),
                                                                 cur_super_blk.micro_ckpt_time_us_));
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->super_blk_.assign(cur_super_blk));

  // 4.7.3. check memory state
  micro_data_blk_cnt = 0;
  for (int64_t i = 2; i < phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_; ++i) {
    ObSSPhysicalBlock *phy_blk = phy_blk_mgr_->inner_get_phy_block_by_idx(i);
    ASSERT_NE(nullptr, phy_blk);
    if (phy_blk->get_block_type() == ObSSPhyBlockType::SS_MICRO_DATA_BLK && phy_blk->valid_len_ > 0) {
      ++micro_data_blk_cnt;
    }
  }
  ASSERT_EQ(WRITE_BLK_CNT - 2, micro_data_blk_cnt);
  range_micro_sum = 0;
  for (int64_t i = 0; i < init_range_cnt; ++i) {
    range_micro_sum += range_info_arr[i]->micro_meta_cnt_;
  }
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt, range_micro_sum); // not include force_deleted micro_metas and not persisted micro_meta

  ASSERT_EQ(ori_arc_limit, micro_meta_mgr_->get_arc_info().limit_);
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt, micro_meta_mgr_->replay_ctx_.total_replay_cnt_);
  ASSERT_EQ((WRITE_BLK_CNT - 2) * micro_cnt, micro_meta_mgr_->arc_info_.seg_info_arr_[SS_ARC_T1].cnt_);
  ASSERT_EQ(micro_cnt, micro_meta_mgr_->arc_info_.seg_info_arr_[SS_ARC_B1].cnt_);

  // 5. execute phy_block checkpoint
  // 5.1. check ckpt state
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.exe_round_ = blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_round_;
  blk_ckpt_task_->is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.check_state());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->get_ss_super_block(blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_));
  ASSERT_EQ(true, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_);
  ASSERT_EQ(0, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(0, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(blk_ckpt_list_cnt, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(micro_ckpt_list_cnt, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.micro_ckpt_entry_list_.count());

  // 5.2. gen phy_blk checkpoint
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_phy_block_checkpoint());
  ASSERT_EQ(blk_ckpt_list_cnt, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_entry_list_.count());
  const int64_t super_blk_cnt = 2;
  const int64_t total_blk_cnt = phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_ - super_blk_cnt;
  ASSERT_EQ(total_blk_cnt, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);

  // 5.3. update ss_super_block
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.update_super_block(false));
  cur_super_blk.reset();
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->get_ss_super_block(cur_super_blk));
  ASSERT_EQ(micro_ckpt_list_cnt, cur_super_blk.micro_ckpt_entry_list_.count());
  ASSERT_EQ(blk_ckpt_list_cnt, cur_super_blk.blk_ckpt_entry_list_.count());
  ASSERT_NE(blk_ckpt_entry_list.at(0), cur_super_blk.blk_ckpt_entry_list_.at(0));
  blk_ckpt_entry_list.reset();
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_entry_list.assign(cur_super_blk.blk_ckpt_entry_list_));
  for (int64_t i = 0; i < micro_ckpt_entry_list.count(); ++i) {
    ASSERT_EQ(micro_ckpt_entry_list.at(i), cur_super_blk.micro_ckpt_entry_list_.at(i));
  }

  // 5.4. finish checkpoint(evicted phy_block and force_del phy_block have already been reused)
  ObSSPhyBlockHandle phy_blk_handle;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->get_block_handle(evict_blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  uint64_t ori_reuse_version_1 = phy_blk_handle.get_ptr()->reuse_version_;
  phy_blk_handle.reset();
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->get_block_handle(force_del_blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  uint64_t ori_reuse_version_2 = phy_blk_handle.get_ptr()->reuse_version_;
  phy_blk_handle.reset();

  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->try_free_batch_blocks(blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.blk_ckpt_entry_list_,
                                                            blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.free_blk_cnt_));
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->get_block_handle(evict_blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  ASSERT_EQ(true, phy_blk_handle.ptr_->is_free_);
  ASSERT_EQ(ori_reuse_version_1 + 1, phy_blk_handle.ptr_->reuse_version_);
  phy_blk_handle.reset();

  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->get_block_handle(force_del_blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  ASSERT_EQ(true, phy_blk_handle.ptr_->is_free_);
  ASSERT_EQ(ori_reuse_version_2 + 1, phy_blk_handle.ptr_->reuse_version_);
  phy_blk_handle.reset();
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_entry_list_.count());

  // 5.5. read phy_block checkpoint, and check it
  ObArray<ObSSPhyBlockReuseInfo> reuse_info_arr1;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->scan_blocks_to_ckpt(reuse_info_arr1));
  ASSERT_LT(0, reuse_info_arr1.count());
  ASSERT_EQ(OB_SUCCESS, micro_cache_->read_phy_block_checkpoint(cur_super_blk.blk_ckpt_entry_list_.at(0)));
  ObArray<ObSSPhyBlockReuseInfo> reuse_info_arr2;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->scan_blocks_to_ckpt(reuse_info_arr2));
  ASSERT_EQ(reuse_info_arr1.count(), reuse_info_arr2.count());
  for (int64_t i = 2; i < reuse_info_arr1.count(); ++i) {
    ASSERT_EQ(reuse_info_arr1.at(i).blk_idx_, reuse_info_arr2.at(i).blk_idx_);
    ASSERT_EQ(reuse_info_arr1.at(i).reuse_version_, reuse_info_arr2.at(i).reuse_version_);
  }

  // 6. execute phy_block checkpoint
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.reuse();
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.is_valid());
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.is_valid());
  ASSERT_EQ(0, phy_blk_mgr_->reusable_blks_.size());
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());
  ASSERT_EQ(0, phy_blk_mgr_->reusable_blks_.size());

  ASSERT_EQ(true, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.is_valid());
  ASSERT_EQ(true, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.is_valid());
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_ - 2, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(blk_ckpt_list_cnt, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(micro_ckpt_list_cnt, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.micro_ckpt_entry_list_.count());

  // 7. execute persist micro_meta
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.reuse();
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(false, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_.is_valid());
  ASSERT_EQ(false, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.cur_super_blk_.is_valid());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->scan_blocks_to_reuse());
  ASSERT_EQ(0, phy_blk_mgr_->reusable_blks_.size());
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.gen_checkpoint());

  ASSERT_EQ(true, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_.is_valid());
  ASSERT_EQ(true, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.cur_super_blk_.is_valid());
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(blk_ckpt_list_cnt, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(micro_ckpt_entry_list.count(), persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_entry_list_.count());

  // 8. execute phy_block checkpoint
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.reuse();
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.is_valid());
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.is_valid());
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());

  ASSERT_EQ(true, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.is_valid());
  ASSERT_EQ(true, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.is_valid());
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_ - 2, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(blk_ckpt_list_cnt, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(micro_ckpt_list_cnt, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_entry_list_.count());

  allocator.clear();
  LOG_INFO("TEST: finish test_execute_checkpoint_task");
}

/* This case tests whether the micro cache can be restored to the expected state after restart.  */
TEST_F(TestSSExecuteCheckpointTask, test_micro_cache_ckpt_after_restart)
{
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  persist_meta_task_->is_inited_ = false;
  blk_ckpt_task_->is_inited_ = false;
  release_cache_task_->is_inited_ = false;
  persist_data_task_->is_inited_ = false;
  const int64_t total_blk_cnt = phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_;
  ob_usleep(1000 * 1000);

  // 1. add some micro_block into cache and record their micro_meta
  ObArray<ObSSMicroBlockMetaInfo> micro_meta_info_arr1;
  int64_t macro_start_idx = 1;
  int64_t write_blk_cnt = 10;
  const int64_t micro_cnt = 20;
  ASSERT_EQ(OB_SUCCESS, add_batch_micro_block(macro_start_idx, write_blk_cnt, micro_cnt, micro_meta_info_arr1));
  const int64_t used_data_blk_cnt1 = phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_;

  ObArray<ObSSPhyBlockInfoCkptItem> phy_blk_info_arr1;
  int64_t alloc_phy_blk_cnt = 20;
  ASSERT_EQ(OB_SUCCESS, alloc_batch_phy_block_to_reuse(alloc_phy_blk_cnt, phy_blk_info_arr1));

  // 2. do ckpt_task first round
  persist_meta_task_->is_inited_ = true;
  blk_ckpt_task_->is_inited_ = true;
  persist_meta_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  blk_ckpt_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(1000 * 1000);

  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_scan_phy_blk_ = true;
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(false, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_.is_valid());
  ASSERT_EQ(false, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.cur_super_blk_.is_valid());
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.is_valid());
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.is_valid());
  ASSERT_EQ(0, phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr_->reusable_blks_.size());
  int64_t start_time_us = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.gen_checkpoint());
  const int64_t micro_exe_time_us = ObTimeUtility::current_time() - start_time_us;
  ASSERT_LT(0, phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_);
  ASSERT_EQ(alloc_phy_blk_cnt, phy_blk_mgr_->reusable_blks_.size());
  ASSERT_EQ((write_blk_cnt - 1) * 20, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.ckpt_item_cnt_);

  start_time_us = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());
  const int64_t blk_exe_time_us = ObTimeUtility::current_time() - start_time_us;
  ASSERT_LT(0, phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(total_blk_cnt - SS_SUPER_BLK_COUNT, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  persist_meta_task_->is_inited_ = false;
  blk_ckpt_task_->is_inited_ = false;
  ObSSMicroCacheSuperBlk super_blk1;
  ASSERT_EQ(OB_SUCCESS, super_blk1.assign(phy_blk_mgr_->super_blk_));

  // 3. restart micro cache
  LOG_INFO("TEST: start first restart");
  ASSERT_EQ(OB_SUCCESS, restart_micro_cache());
  LOG_INFO("TEST: finish first restart");

  // 4. check super_block, micro_meta and phy_block_info
  ASSERT_EQ(OB_SUCCESS, check_phy_blk_info(phy_blk_info_arr1, 1));
  ASSERT_EQ(OB_SUCCESS, check_micro_meta(micro_meta_info_arr1, true));
  ASSERT_EQ(OB_SUCCESS, check_super_block(super_blk1));
  ASSERT_EQ(super_blk1.micro_ckpt_entry_list_.count(), phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_);
  ASSERT_EQ(super_blk1.blk_ckpt_entry_list_.count(), phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(used_data_blk_cnt1, phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_);

  // 5. clear all micro meta
  int64_t micro_data_blk_cnt = 0;
  int64_t range_micro_sum = 0;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::clear_micro_cache(micro_meta_mgr_, phy_blk_mgr_, micro_meta_mgr_->micro_range_mgr_,
                        micro_data_blk_cnt, range_micro_sum));
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->super_blk_.assign(super_blk1));

  // 6. repeat step 1
  ObArray<ObSSMicroBlockMetaInfo> micro_meta_info_arr2;
  macro_start_idx += write_blk_cnt;
  write_blk_cnt = 20;
  ASSERT_EQ(OB_SUCCESS, add_batch_micro_block(macro_start_idx, write_blk_cnt, micro_cnt, micro_meta_info_arr2));
  const int64_t used_data_blk_cnt2 = phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_;

  // 7. do ckpt_task second round and randomly force to end persist_meta_task and restart micro_cache
  persist_meta_task_->is_inited_ = true;
  blk_ckpt_task_->is_inited_ = true;
  persist_meta_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  blk_ckpt_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(2 * 1000 * 1000);

  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_scan_phy_blk_ = true;
  ObSSMicroCacheSuperBlk super_blk2;
  ASSERT_EQ(OB_SUCCESS, super_blk2.assign(phy_blk_mgr_->super_blk_));

  std::thread micro_t([&]() {
    ObRandom rand;
    const int64_t sleep_us = ObRandom::rand(1, micro_exe_time_us * 2);
    ob_usleep(sleep_us);
    persist_meta_task_->persist_meta_op_.is_inited_ = false; // force to end micro_ckpt
  });
  persist_meta_task_->persist_meta_op_.gen_checkpoint();
  micro_t.join();
  ObSSMicroCacheSuperBlk super_blk3;
  ASSERT_EQ(OB_SUCCESS, super_blk3.assign(phy_blk_mgr_->super_blk_));

  LOG_INFO("TEST: start second restart");
  ASSERT_EQ(OB_SUCCESS, restart_micro_cache());
  LOG_INFO("TEST: finish second restart");

  // 8. check super_block, micro_meta and phy_block_info
  if (phy_blk_mgr_->super_blk_.modify_time_us_ == super_blk2.modify_time_us_) {
    ASSERT_EQ(OB_SUCCESS, check_super_block(super_blk2));
    ASSERT_EQ(OB_SUCCESS, check_micro_meta(micro_meta_info_arr1, true));
    ASSERT_EQ(OB_SUCCESS, check_micro_meta(micro_meta_info_arr2, false));
    ASSERT_EQ(used_data_blk_cnt1, phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_);
  } else {
    ASSERT_EQ(OB_SUCCESS, check_super_block(super_blk3));
    ASSERT_EQ(OB_SUCCESS, check_micro_meta(micro_meta_info_arr1, false));
    ASSERT_EQ(OB_SUCCESS, check_micro_meta(micro_meta_info_arr2, true));
    ASSERT_EQ(used_data_blk_cnt2, phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_);
  }
}

/* After persist_meta_task execucte scan_blocks_to_reuse, must set need_scan_phy_blk_ = false */
TEST_F(TestSSExecuteCheckpointTask, test_micro_ckpt_task_exec_scan_block)
{
  LOG_INFO("TEST: test_micro_ckpt_task_exec_scan_block");
  int ret = OB_SUCCESS;
  blk_ckpt_task_->is_inited_ = false;

  const int64_t block_cnt = 10;
  for (int64_t i = 0; i < block_cnt; i++) {
    int64_t phy_blk_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->alloc_block(phy_blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
    phy_blk_handle()->is_free_ = false;
    phy_blk_handle()->is_sealed_ = true;
    phy_blk_handle()->valid_len_ = 0;
  }
  persist_meta_task_->persist_meta_op_.ckpt_ctx_->exe_round_ = persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.ckpt_round_ - 1;

  ob_usleep(3 * 1000 * 1000);
  ASSERT_EQ(block_cnt, phy_blk_mgr_->get_reusable_blocks_cnt());
  ASSERT_EQ(false, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_scan_phy_blk_);
}

/* Test whether extra meta blocks can be dynamically allocated when the number of blocks required
 * by micro_ckpt exceeds meta_blk.min_cnt_ */
TEST_F(TestSSExecuteCheckpointTask, test_reserve_micro_ckpt_blk)
{
  LOG_INFO("TEST_CASE: start test_reserve_micro_ckpt_blk");
  int ret = OB_SUCCESS;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr_->blk_cnt_info_;

  // 1. write enough micro blocks so that the number of blocks required to execute micro_ckpt exceeds meta_blk.min_cnt_
  const int64_t extra_meta_blk_cnt = 5;
  const int64_t estimate_micro_cnt =
      (blk_cnt_info.meta_blk_.hold_cnt_ + extra_meta_blk_cnt) * phy_blk_mgr_->get_block_size() / SS_AVG_MICRO_META_PERSIST_SIZE;
  const int64_t macro_cnt = blk_cnt_info.data_blk_.hold_cnt_ * 0.2;
  const int64_t micro_cnt = estimate_micro_cnt / macro_cnt;
  ObArray<ObSSMicroBlockMetaInfo> micro_meta_info_arr;
  ASSERT_EQ(OB_SUCCESS, add_batch_micro_block(1, macro_cnt, micro_cnt, micro_meta_info_arr));

  // 2. mock shared_block is used up
  blk_cnt_info.data_blk_.hold_cnt_ = blk_cnt_info.data_blk_.max_cnt_;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;

  // 3. execute micro_meta_ckpt
  ASSERT_EQ(0, blk_cnt_info.meta_blk_.used_cnt_);
  int64_t micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.exe_round_ = persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.ckpt_round_ - 1;
  ob_usleep(10 * 1000 * 1000);
  ASSERT_EQ(micro_ckpt_cnt + 1, cache_stat.task_stat().micro_ckpt_cnt_);

  ASSERT_EQ(blk_cnt_info.meta_blk_.hold_cnt_, blk_cnt_info.meta_blk_.used_cnt_);
  ASSERT_EQ(blk_cnt_info.meta_blk_.used_cnt_, phy_blk_mgr_->super_blk_.micro_ckpt_entry_list_.count());

  // 4. revert step2
  blk_cnt_info.data_blk_.hold_cnt_ = blk_cnt_info.data_blk_.min_cnt_;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;

  // 5. try to adjust micro_meta_block in micro_meta_ckpt, check that the number of blocks used by micro_ckpt has increased
  const int64_t origin_meta_blk_usd_cnt = blk_cnt_info.meta_blk_.used_cnt_;
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.exe_round_ = persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.print_cache_stat_round_ - 1;
  ob_usleep(2 * 1000 * 1000);
  ASSERT_LT(2 * origin_meta_blk_usd_cnt, blk_cnt_info.meta_blk_.hold_cnt_);

  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.exe_round_ = persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.ckpt_round_ - 1;
  ob_usleep(10 * 1000 * 1000);
  ASSERT_EQ(blk_cnt_info.meta_blk_.used_cnt_, phy_blk_mgr_->super_blk_.micro_ckpt_entry_list_.count());
  ASSERT_LT(origin_meta_blk_usd_cnt, blk_cnt_info.meta_blk_.used_cnt_);
  ASSERT_LT(origin_meta_blk_usd_cnt, phy_blk_mgr_->super_blk_.micro_ckpt_entry_list_.count());
}

TEST_F(TestSSExecuteCheckpointTask, test_dynamic_update_arc_limit)
{
  LOG_INFO("TEST_CASE: start test_dynamic_update_arc_limit");
  ObSSARCInfo &arc_info = micro_cache_->micro_meta_mgr_.arc_info_;
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr_->blk_cnt_info_;
  ObSSPersistMicroMetaOp &micro_ckpt_op = persist_meta_task_->persist_meta_op_;

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
  micro_cache_->begin_free_space_for_prewarm();

  const int64_t prewarm_work_limit = static_cast<int64_t>((static_cast<double>(origin_work_limit * SS_LIMIT_PREWARM_SHRINK_PCT) / 100.0));
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

  const int64_t new_limit = phy_blk_mgr_->get_cache_limit_size() * SS_ARC_LIMIT_PCT / 100;
  const int64_t new_work_limit = (static_cast<double>(prewarm_work_limit) / origin_limit) * new_limit;
  ori_pct = static_cast<double>(p1 * 100) / prewarm_work_limit;
  const int64_t p2 = static_cast<int64_t>((static_cast<double>(new_work_limit * ori_pct) / 100.0));
  ASSERT_EQ(new_limit, arc_info.limit_);
  ASSERT_EQ(new_work_limit, arc_info.work_limit_);
  ASSERT_LE(abs(p2 - arc_info.p_), 5); // the conversion between 'int64_t' and 'double' causes some deviations

  micro_cache_->finish_free_space_for_prewarm();

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
  const uint64_t tenant_id = MTL_ID();
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr_->blk_cnt_info_;
  blk_cnt_info.phy_ckpt_blk_cnt_ = 100; // ensure the blk_info_ckpt block count is enough
  const int32_t blk_size = phy_blk_mgr_->get_block_size();
  persist_meta_task_->cur_interval_us_ = 60 * 60 * 1000 * 1000L;
  ob_usleep(1000 * 1000);

  ObSSMicroBlockMeta micro_meta;
  MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100000);
  micro_meta.micro_key_ = TestSSCommonUtil::gen_phy_micro_key(macro_id, 100, 4 * 1024);
  micro_meta.data_loc_ = 3 * blk_size + 100;
  micro_meta.access_time_ = ObTimeUtility::current_time_s();

  const int64_t buf_size = 4096;
  char buf[buf_size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, micro_meta.serialize(buf, buf_size, pos));
  const int64_t avg_micro_size = micro_meta.get_serialize_size();
  ASSERT_EQ(pos, avg_micro_size);

  ObSSCkptPhyBlockItemWriter item_writer;
  ASSERT_EQ(OB_SUCCESS, item_writer.init(tenant_id, *phy_blk_mgr_, ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK));
  const int64_t total_micro_cnt = 100000;
  const int64_t seg_micro_cnt = ObRandom::rand(300, 500);
  int64_t tmp_cnt = 0;
  for (int64_t i = 0; i < total_micro_cnt; ++i) {
    const bool start_new_seg = (tmp_cnt >= seg_micro_cnt) ? true : false;
    bool is_persisted = false;
    ASSERT_EQ(OB_SUCCESS, item_writer.write_item(buf, avg_micro_size, start_new_seg, is_persisted));
    if (start_new_seg) {
      tmp_cnt = 1;
    } else {
      ++tmp_cnt;
    }
  }
  ASSERT_EQ(OB_SUCCESS, item_writer.close());

  int64_t entry_block_id = -1;
  ObArray<int64_t> block_id_list;
  ASSERT_EQ(OB_SUCCESS, item_writer.get_entry_block(entry_block_id));
  ASSERT_NE(-1, entry_block_id);
  ASSERT_EQ(OB_SUCCESS, item_writer.get_block_id_list(block_id_list));
  ASSERT_LT(1, block_id_list.count());

  ObSSCkptPhyBlockReader block_reader;
  ASSERT_EQ(OB_SUCCESS, block_reader.init(entry_block_id, tenant_id, *phy_blk_mgr_));
  int64_t total_item_cnt = 0;
  int64_t total_blk_size = 0;
  for (int64_t i = 0; i < block_id_list.count(); ++i) {
    const int64_t cur_block_id = block_id_list.at(i);
    int64_t payload_offset = 0;
    if (cur_block_id != entry_block_id) {
      ASSERT_EQ(OB_SUCCESS, block_reader.inner_read_block_(cur_block_id, payload_offset));
      total_item_cnt += block_reader.ckpt_blk_header_.item_count_;
      total_blk_size += blk_size;
      LOG_INFO("print block header", K(i), K(cur_block_id), K_(block_reader.common_header), K_(block_reader.ckpt_blk_header));
    }
  }
  double avg_cost = static_cast<double>(total_blk_size) / total_item_cnt;
  std::cout << "micro_size: " << avg_micro_size << ", micro_item_size: " << avg_cost << std::endl;
  LOG_INFO("TEST: finish test_estimate_micro_meta_persist_cost", K(total_item_cnt), K(total_blk_size), K(avg_cost));
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
