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
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_reader.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_writer.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/ob_file_manager.h"

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
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 32), 1/*micro_split_cnt*/));
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tnt_file_mgr);
  tnt_file_mgr->is_cache_file_exist_ = true;
  micro_cache->start();
  micro_cache_ = micro_cache;
  mem_blk_mgr_ = &micro_cache_->mem_blk_mgr_;
  ASSERT_NE(nullptr, mem_blk_mgr_);
  phy_blk_mgr_ = &micro_cache_->phy_blk_mgr_;
  ASSERT_NE(nullptr, phy_blk_mgr_);
  micro_meta_mgr_ = &micro_cache_->micro_meta_mgr_;
  ASSERT_NE(nullptr, micro_meta_mgr_);
  micro_meta_mgr_->enable_save_meta_mem_ = false; // disable save meta memory
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
  ObSSMicroCacheSuperBlk new_super_blk(tenant_id, micro_cache->cache_file_size_, SS_PERSIST_MICRO_CKPT_SPLIT_CNT);
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

  if (OB_FAIL(micro_cache_->init(MTL_ID(), (1L << 32), 1/*micro_split_cnt*/))) {
    LOG_WARN("fail to init micro_cache", KR(ret));
  } else {
    micro_cache_->start();
    int64_t REPLAY_CKPT_TIMEOUT_S = 120;
    int64_t start_time_s = ObTimeUtility::current_time_s();
    bool is_cache_enabled = false;
    do {
      is_cache_enabled = micro_cache_->is_enabled_;
      if (!is_cache_enabled) {
        LOG_INFO("ss_micro_cache is still disabled");
        ob_usleep(1000 * 1000);
      }
    } while (!is_cache_enabled && ObTimeUtility::current_time_s() - start_time_s < REPLAY_CKPT_TIMEOUT_S);
    if (!is_cache_enabled) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to replay ckpt async", KR(ret), K(is_cache_enabled));
    }
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
  ob_usleep(100 * 1000);
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();
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
            macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
          LOG_WARN("fail to add micro_block", KR(ret), K(micro_key), KP(data_buf), K(micro_size));
        } else if (OB_FAIL(micro_key_arr.push_back(micro_key))) {
          LOG_WARN("fail to push micro_key", KR(ret), K(micro_key));
        }
      }
      if (FAILEDx(TestSSCommonUtil::wait_for_persist_task())) {
        LOG_WARN("fail to wait for persist task", K(ret));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < micro_key_arr.count(); ++i) {
    ObSSMicroBlockMetaHandle micro_handle;
    const ObSSMicroBlockCacheKey &micro_key = micro_key_arr[i];
    if (OB_FAIL(micro_meta_mgr_->get_micro_block_meta(micro_key, micro_handle, ObTabletID::INVALID_TABLET_ID, false))) {
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
    if (OB_FAIL(micro_meta_mgr_->get_micro_block_meta(micro_key, micro_handle, ObTabletID::INVALID_TABLET_ID, false))) {
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
                  cur_super_blk.micro_ckpt_info_ != super_blk.micro_ckpt_info_ ||
                  cur_super_blk.blk_ckpt_info_ != super_blk.blk_ckpt_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected super block", KR(ret), K(cur_super_blk), K(super_blk));
  }
  return ret;
}

TEST_F(TestSSExecuteCheckpointTask, test_phy_ckpt_blk_scan)
{
  LOG_INFO("TEST_CASE: start test test_phy_ckpt_blk_scan");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  const uint64_t tenant_id = MTL_ID();
  ObSSDoBlkCheckpointOp &blk_ckpt_op = micro_cache->task_runner_.blk_ckpt_task_.ckpt_op_;
  ObSSPhysicalBlockManager *phy_blk_mgr = blk_ckpt_op.phy_blk_mgr();
  ObSSMicroCacheSuperBlk &super_blk = phy_blk_mgr->super_blk_;
  ObSSBlkCkptInfo &blk_ckpt_info = super_blk.blk_ckpt_info_;
  const int64_t total_blk_cnt = phy_blk_mgr->blk_cnt_info_.total_blk_cnt_;
  ASSERT_GT(total_blk_cnt, 2);

  ObIArray<int64_t> &prev_ckpt_used_blk_list = super_blk.blk_ckpt_used_blk_list();
  prev_ckpt_used_blk_list.reset();
  // Mark the last two phy_blocks as those used in the previous checkpoint process for writing blk checkpoint data.
  prev_ckpt_used_blk_list.push_back(total_blk_cnt - 1); // the last phy_blk;
  prev_ckpt_used_blk_list.push_back(total_blk_cnt - 2); // the second last phy_blk;
  blk_ckpt_info.blk_ckpt_entry_ = total_blk_cnt - 1;

  ObSEArray<ObSSPhyBlockReuseInfo, 256> all_blk_info_arr;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->scan_blocks_to_ckpt(all_blk_info_arr));
  ASSERT_EQ(all_blk_info_arr.count(), total_blk_cnt - SS_SUPER_BLK_COUNT);
  phy_blk_mgr->reusable_blks_.reuse(); // clear reusable_blks
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_op.mark_reusable_blocks(all_blk_info_arr));

  ObSSPhyBlockReuseInfo reuse_info_last_blk = all_blk_info_arr.at(all_blk_info_arr.count() - 1);
  ASSERT_EQ(true, reuse_info_last_blk.need_reuse_);
  ObSSPhyBlockReuseInfo reuse_info_second_last_blk = all_blk_info_arr.at(all_blk_info_arr.count() - 2);
  ASSERT_EQ(true, reuse_info_second_last_blk.need_reuse_);

  LOG_INFO("TEST_CASE: finish test test_phy_ckpt_blk_scan");
}

TEST_F(TestSSExecuteCheckpointTask, test_micro_ckpt_lack_phy_blk)
{
  LOG_INFO("TEST: start test_micro_ckpt_lack_phy_blk");
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  const int64_t block_size = micro_cache->phy_blk_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroRangeManager &micro_range_mgr = micro_cache->micro_range_mgr_;

  // add block
  const int64_t WRITE_BLK_CNT = 50;
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 200;
  const int32_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
          macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(WRITE_BLK_CNT * micro_cnt, cache_stat.micro_stat_.total_micro_cnt_);
  ASSERT_LT(0, cache_stat.micro_stat_.valid_micro_cnt_);

  persist_meta_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(1000 * 1000);

  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  const int64_t extra_blk_cnt = blk_cnt_info.shared_blk_cnt_ - blk_cnt_info.shared_blk_used_cnt_;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.shared_blk_cnt_;
  blk_cnt_info.data_blk_.hold_cnt_ += extra_blk_cnt;
  blk_cnt_info.meta_blk_.used_cnt_ = blk_cnt_info.meta_blk_.hold_cnt_ - 1;

  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.gen_checkpoint());
  ASSERT_EQ(1, phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_LT(0, cache_stat.task_stat().micro_ckpt_item_cnt_);
  ASSERT_LT(cache_stat.task_stat().micro_ckpt_item_cnt_, block_size / SS_AVG_MICRO_META_PERSIST_SIZE);

  ObArray<ObSSMicroSubRangeInfo *> persisted_sub_rngs;
  const int64_t init_rng_cnt = micro_range_mgr.init_range_cnt_;
  ObSSMicroInitRangeInfo **init_rng_arr = micro_range_mgr.init_range_arr_;
  ASSERT_LT(0, init_rng_cnt);
  ASSERT_NE(nullptr, init_rng_arr);
  for (int64_t i = 0; i < init_rng_cnt; ++i) {
    ObSSMicroSubRangeInfo *cur_sub_rng = init_rng_arr[i]->next_;
    while (nullptr != cur_sub_rng) {
      if (cur_sub_rng->phy_blk_idx_ != -1) {
        ASSERT_EQ(OB_SUCCESS, persisted_sub_rngs.push_back(cur_sub_rng));
      }
      cur_sub_rng = cur_sub_rng->next_;
    }
  }
  ASSERT_LT(0, persisted_sub_rngs.count());

  char *range_buf = static_cast<char *>(allocator.alloc(block_size));
  ASSERT_NE(nullptr, range_buf);
  ObArray<ObSSMicroBlockMetaInfo> micro_infos;
  for (int64_t i = 0; i < persisted_sub_rngs.count(); ++i) {
    ObSSMicroSubRangeInfo *cur_sub_rng = persisted_sub_rngs.at(i);
    const int64_t phy_blk_idx = cur_sub_rng->phy_blk_idx_;
    const int64_t offset = cur_sub_rng->offset_;
    const int64_t size = cur_sub_rng->length_;
    const uint64_t reuse_version = cur_sub_rng->reuse_version_;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.read_persisted_sub_range(range_buf, phy_blk_idx, offset, size, reuse_version));
    ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::parse_range_micro_metas(tenant_id, range_buf, block_size, block_size, micro_infos));
    ASSERT_LT(0, micro_infos.count());
    for (int64_t j = 0; j < micro_infos.count(); ++j) {
      const ObSSMicroBlockMetaInfo &micro_info = micro_infos.at(j);
      ASSERT_LT(0, micro_info.access_time_s_);
      ASSERT_GE(cur_sub_rng->end_hval_, micro_info.get_micro_key().micro_hash());
    }
  }
}

TEST_F(TestSSExecuteCheckpointTask, test_persist_micro_meta_parallel_with_add)
{
  LOG_INFO("TEST: start test_persist_micro_meta_parallel_with_add");
  int ret = OB_SUCCESS;
  ASSERT_NE(nullptr, micro_cache_);
  const int64_t block_size = micro_cache_->phy_blk_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;

  micro_cache_->task_runner_.disable_meta_ckpt();
  micro_cache_->task_runner_.disable_blk_ckpt();
  micro_cache_->task_runner_.disable_release_cache();
  ob_usleep(2 * 1000 * 1000);
  ASSERT_EQ(true, micro_cache_->task_runner_.is_meta_ckpt_closed());
  ASSERT_EQ(true, micro_cache_->task_runner_.is_blk_ckpt_closed());
  ASSERT_EQ(true, micro_cache_->task_runner_.is_release_cache_closed());

  const int64_t WRITE_BLK_CNT = 50;
  ASSERT_LE(WRITE_BLK_CNT * 2, phy_blk_mgr_->blk_cnt_info_.micro_data_blk_max_cnt());
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 100;
  const int32_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  // 1. write 50 fulfilled phy_block
  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_cache_->add_micro_block_cache(micro_key, data_buf, micro_size,
                            macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  {
    // to sealed the last mem_block
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(WRITE_BLK_CNT + 1);
    const int32_t offset = payload_offset;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    ASSERT_EQ(OB_SUCCESS, micro_cache_->add_micro_block_cache(micro_key, data_buf, micro_size,
                          macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
  }
  int64_t total_range_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, micro_cache_->micro_range_mgr_.get_total_range_micro_cnt(total_range_micro_cnt));
  ASSERT_EQ(WRITE_BLK_CNT * micro_cnt + 1, total_range_micro_cnt);

  // 2. execute persist_micro_meta
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.gen_checkpoint());
  ASSERT_EQ(OB_SUCCESS, micro_cache_->micro_range_mgr_.get_total_range_micro_cnt(total_range_micro_cnt));
  ASSERT_EQ(WRITE_BLK_CNT * micro_cnt + 1, total_range_micro_cnt);

  // 3. evict some micro_meta
  {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr_->micro_meta_map_.get(&micro_key, micro_meta_handle));
      ObSSMicroBlockMeta *micro_meta = micro_meta_handle.get_ptr();
      ASSERT_NE(nullptr, micro_meta);
      ASSERT_EQ(true, micro_meta->is_data_persisted_);

      ObSSMicroBlockMetaInfo evict_micro_info;
      micro_meta_handle()->get_micro_meta_info(evict_micro_info);

      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr_->try_evict_micro_block_meta(evict_micro_info));
      ASSERT_EQ(true, micro_meta->is_in_ghost_);
      int64_t phy_blk_idx = -1;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->update_block_valid_length(evict_micro_info.data_loc_,
        evict_micro_info.reuse_version_, evict_micro_info.size_ * -1, phy_blk_idx));
    }
  }

  // 4. execute persist_micro_meta and add micro_block parallel
  micro_cache_->task_runner_.enable_meta_ckpt();
  ob_usleep(1000 * 1000);
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
  for (int64_t i = WRITE_BLK_CNT; i < WRITE_BLK_CNT * 2; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_cache_->add_micro_block_cache(micro_key, data_buf, micro_size,
                            macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  LOG_INFO("TEST: finish test_persist_micro_meta_parallel_with_add");
}

/* This case tests the basic logic of the execute_checkpoint task. */
TEST_F(TestSSExecuteCheckpointTask, test_execute_checkpoint_task)
{
  LOG_INFO("TEST: start test_execute_checkpoint_task");
  int ret = OB_SUCCESS;
  ASSERT_NE(nullptr, micro_cache_);
  const int64_t cache_file_size = micro_cache_->cache_file_size_;
  const int64_t block_size = micro_cache_->phy_blk_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;

  ObSSARCInfo &arc_info = micro_meta_mgr_->arc_info_;
  const int64_t ori_arc_limit = micro_meta_mgr_->get_arc_info().limit_;
  int64_t ori_blk_ckpt_cnt = 0;
  int64_t ori_micro_ckpt_cnt = 0;

  // 1. execute phy_block checkpoint
  release_cache_task_->is_inited_ = false;
  persist_meta_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  blk_ckpt_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(2 * 1000 * 1000);

  ASSERT_EQ(0, phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr_->reusable_blks_.size());
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.exist_blk_checkpoint());
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.exist_blk_checkpoint());
  ASSERT_EQ(0, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);

  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());

  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_ - 2, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_ - 2, cache_stat.task_stat().blk_ckpt_item_cnt_);
  ASSERT_EQ(true, phy_blk_mgr_->super_blk_.exist_blk_checkpoint());
  ASSERT_EQ(false, phy_blk_mgr_->super_blk_.exist_micro_checkpoint());
  ASSERT_EQ(0, phy_blk_mgr_->super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  int64_t blk_ckpt_used_cnt = phy_blk_mgr_->super_blk_.blk_ckpt_info_.get_total_used_blk_cnt();
  ASSERT_LT(0, blk_ckpt_used_cnt);
  ObSEArray<int64_t, 32> blk_ckpt_entry_list;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_entry_list.assign(phy_blk_mgr_->super_blk_.blk_ckpt_info_.blk_ckpt_used_blks_));

  // 2. execute phy_block checkpoint again
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.start_op());
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_super_blk_.exist_blk_checkpoint());
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.cur_super_blk_.exist_blk_checkpoint());
  ASSERT_EQ(0, phy_blk_mgr_->reusable_blks_.size());

  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());

  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_ - 2, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(true, phy_blk_mgr_->super_blk_.exist_blk_checkpoint());
  ASSERT_EQ(0, phy_blk_mgr_->super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(blk_ckpt_used_cnt, phy_blk_mgr_->super_blk_.blk_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_, blk_ckpt_used_cnt);

  for (int64_t i = 0; i < blk_ckpt_entry_list.count(); ++i) {
    const int64_t blk_idx = blk_ckpt_entry_list.at(i);
    ObSSPhysicalBlock *phy_blk = phy_blk_mgr_->inner_get_phy_block_by_idx(blk_idx);
    ASSERT_NE(nullptr, phy_blk);
    ASSERT_EQ(true, phy_blk->is_free_);
    ASSERT_EQ(2, phy_blk->reuse_version_);
    ASSERT_EQ(0, phy_blk->valid_len_);
  }
  ASSERT_NE(blk_ckpt_entry_list.at(0), phy_blk_mgr_->super_blk_.blk_ckpt_info_.blk_ckpt_entry_);
  blk_ckpt_entry_list.reset();
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_entry_list.assign(phy_blk_mgr_->super_blk_.blk_ckpt_info_.blk_ckpt_used_blks_));
  ASSERT_EQ(blk_ckpt_used_cnt, blk_ckpt_entry_list.count());

  // 3. make some micro_data
  const int64_t max_micro_data_blk_cnt = phy_blk_mgr_->blk_cnt_info_.micro_data_blk_max_cnt();
  const int64_t WRITE_BLK_CNT = 50;
  ASSERT_LT(WRITE_BLK_CNT, max_micro_data_blk_cnt);

  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();
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
      const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
      ret = micro_cache_->add_micro_block_cache(micro_key, data_buf, micro_size, effective_tablet_id, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
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
    const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
    ret = micro_cache_->add_micro_block_cache(micro_key, data_buf, micro_size, effective_tablet_id, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
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
      const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr_->get_micro_block_meta(micro_key, micro_meta_handle, effective_tablet_id, false));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      ObSSMicroBlockMeta *micro_meta = micro_meta_handle.get_ptr();

      ObSSMicroBlockMetaInfo delete_micro_info;
      micro_meta_handle()->get_micro_meta_info(delete_micro_info);
      ASSERT_EQ(true, delete_micro_info.is_valid_field());

      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr_->try_force_delete_micro_block_meta(micro_key));
      force_del_blk_idx = delete_micro_info.data_loc_ / phy_blk_mgr_->block_size_;
    }
  }
  ASSERT_NE(-1, force_del_blk_idx);
  ObSSPhysicalBlock* force_phy_blk = phy_blk_mgr_->inner_get_phy_block_by_idx(force_del_blk_idx);
  ASSERT_NE(nullptr, force_phy_blk);
  ASSERT_EQ(0, force_phy_blk->valid_len_);
  ASSERT_EQ(micro_cnt, cache_stat.micro_stat().mark_del_micro_cnt_);
  ASSERT_EQ(micro_cnt * micro_size, cache_stat.micro_stat().mark_del_micro_size_);

  // 3.4. check micro_meta and macro_meta count
  ASSERT_EQ(WRITE_BLK_CNT * micro_cnt + 1, micro_meta_mgr_->micro_meta_map_.count());

  // 4. execute persist micro_meta
  // 4.1. check ckpt state
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.start_op());
  ASSERT_EQ(0, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.cur_super_blk_.exist_micro_checkpoint());
  ASSERT_EQ(0, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.cur_super_blk_.exist_blk_checkpoint());
  ASSERT_EQ(0, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_.exist_micro_checkpoint());
  ASSERT_EQ(0, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_.exist_blk_checkpoint());

  // 4.2. scan reusable phy_blocks
  ASSERT_EQ(2, phy_blk_mgr_->reusable_blks_.size());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->scan_blocks_to_reuse());
  ASSERT_EQ(2, phy_blk_mgr_->reusable_blks_.size());

  // 4.3. first, mock all reserved blk were used up, will fail to execute persist micro_meta
  ori_micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
  const int64_t ori_micro_blk_used_cnt = phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_;
  const int64_t ori_micro_blk_hold_cnt = phy_blk_mgr_->blk_cnt_info_.meta_blk_.hold_cnt_;
  const int64_t ori_micro_blk_max_cnt = phy_blk_mgr_->blk_cnt_info_.meta_blk_.max_cnt_;
  phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_ = ori_micro_blk_max_cnt;
  phy_blk_mgr_->blk_cnt_info_.meta_blk_.hold_cnt_ = ori_micro_blk_max_cnt;
  phy_blk_mgr_->blk_cnt_info_.shared_blk_used_cnt_ =
      phy_blk_mgr_->blk_cnt_info_.meta_blk_.hold_cnt_ + phy_blk_mgr_->blk_cnt_info_.data_blk_.hold_cnt_;

  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.start_op());
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.gen_checkpoint());
  ASSERT_EQ(ori_micro_ckpt_cnt + 1, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_EQ(true, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.lack_phy_blk_);
  ASSERT_LE((WRITE_BLK_CNT - 1) * micro_cnt + 1, micro_meta_mgr_->micro_meta_map_.count());
  ASSERT_LE(((WRITE_BLK_CNT - 2) * micro_cnt + 1) * micro_size, arc_info.get_valid_size());
  int64_t ori_micro_ckpt_item_cnt = cache_stat.task_stat().micro_ckpt_item_cnt_;
  ASSERT_EQ(0, ori_micro_ckpt_item_cnt);
  ASSERT_EQ(0, phy_blk_mgr_->super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());

  phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_ = ori_micro_blk_used_cnt;
  phy_blk_mgr_->blk_cnt_info_.meta_blk_.hold_cnt_ = ori_micro_blk_hold_cnt;
  phy_blk_mgr_->blk_cnt_info_.shared_blk_used_cnt_ =
      phy_blk_mgr_->blk_cnt_info_.meta_blk_.hold_cnt_ + phy_blk_mgr_->blk_cnt_info_.data_blk_.hold_cnt_;

  // 4.4. normal situation
  int64_t prev_micro_ckpt_used_cnt = phy_blk_mgr_->super_blk_.micro_ckpt_info_.get_total_used_blk_cnt();
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.start_op());
  ASSERT_EQ(false, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.lack_phy_blk_);
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  phy_blk_mgr_->super_blk_.modify_time_us_ += 1; // mock ss_super_block was changed
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.gen_checkpoint());
  ASSERT_EQ(ori_micro_ckpt_cnt + 2, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt + 1, micro_meta_mgr_->micro_meta_map_.count());
  ASSERT_EQ(false, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.lack_phy_blk_);
  ASSERT_LT(0, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.cur_super_blk_.micro_ckpt_time_us_);
  ASSERT_LT(0, persist_meta_task_->persist_meta_op_.tablet_info_map_.size());
  ASSERT_LT(0, phy_blk_mgr_->super_blk_.micro_ckpt_time_us_);
  int64_t micro_ckpt_used_cnt = phy_blk_mgr_->super_blk_.micro_ckpt_info_.get_total_used_blk_cnt();
  ASSERT_LT(0, micro_ckpt_used_cnt);
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt, cache_stat.task_stat().micro_ckpt_item_cnt_);
  ASSERT_EQ(prev_micro_ckpt_used_cnt, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.free_blk_cnt_);
  ASSERT_LT(0, phy_blk_mgr_->super_blk_.tablet_info_list_.count());
  ASSERT_LT(0, phy_blk_mgr_->super_blk_.ls_info_list_.count());

  ObSSMCTabletInfoMap::const_iterator iter = persist_meta_task_->persist_meta_op_.tablet_info_map_.begin();
  int64_t total_micro_size = 0;
  int64_t tablet_cnt = 0;
  for (; iter != persist_meta_task_->persist_meta_op_.tablet_info_map_.end(); ++iter) {
    total_micro_size += iter->second.get_valid_size();
    ++tablet_cnt;
  }
  ASSERT_EQ(arc_info.get_valid_size(), total_micro_size);

  // 4.5 execute phy_block checkpoint third time
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.start_op());
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());
  int64_t prev_blk_ckpt_entry = phy_blk_mgr_->super_blk_.blk_ckpt_info_.blk_ckpt_entry_;

  // 4.6 restart micro_cache
  micro_cache_->stop();
  micro_cache_->wait();
  micro_cache_->destroy();
  MTL(ObTenantFileManager*)->is_cache_file_exist_ = true;
  ASSERT_EQ(OB_SUCCESS, micro_cache_->init(MTL_ID(), cache_file_size, 1/*micro_split_cnt*/));
  ASSERT_EQ(OB_SUCCESS, micro_cache_->start());

  const int64_t REPLAY_CKPT_TIMEOUT_S = 120;
  int64_t start_replay_time_s = ObTimeUtility::current_time_s();
  bool is_cache_enabled = false;
  do {
    is_cache_enabled = micro_cache_->is_enabled_;
    if (!is_cache_enabled) {
      LOG_INFO("ss_micro_cache is still disabled");
      ob_usleep(1000 * 1000);
    }
  } while (!is_cache_enabled && ObTimeUtility::current_time_s() - start_replay_time_s < REPLAY_CKPT_TIMEOUT_S);
  ASSERT_EQ(true, is_cache_enabled);

  release_cache_task_->is_inited_ = false;
  persist_meta_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  blk_ckpt_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(2 * 1000 * 1000);

  // 4.7. check memory state
  int64_t micro_data_blk_cnt = 0;
  for (int64_t i = 2; i < phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_; ++i) {
    ObSSPhysicalBlock *phy_blk = phy_blk_mgr_->inner_get_phy_block_by_idx(i);
    ASSERT_NE(nullptr, phy_blk);
    if (phy_blk->get_block_type() == ObSSPhyBlockType::SS_MICRO_DATA_BLK && phy_blk->valid_len_ > 0) {
      ++micro_data_blk_cnt;
    }
  }
  ASSERT_EQ(WRITE_BLK_CNT - 2, micro_data_blk_cnt);

  ObSSPhyBlockHandle phy_blk_handle;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->get_block_handle(evict_blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  ASSERT_EQ(true, phy_blk_handle()->is_free());
  ASSERT_EQ(false, phy_blk_handle()->is_sealed());
  ASSERT_EQ(0, phy_blk_handle()->get_valid_len());
  phy_blk_handle.reset();
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->get_block_handle(force_del_blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  ASSERT_EQ(true, phy_blk_handle()->is_free());
  ASSERT_EQ(false, phy_blk_handle()->is_sealed());
  ASSERT_EQ(0, phy_blk_handle()->get_valid_len());
  phy_blk_handle.reset();

  int64_t total_range_micro_cnt = 0;
  ObSSMicroRangeManager *micro_range_mgr = micro_meta_mgr_->micro_range_mgr_;
  ObSSMicroInitRangeInfo **init_range_arr = micro_range_mgr->get_init_range_arr();
  const int64_t init_range_cnt = micro_range_mgr->get_init_range_cnt();
  for (int64_t i = 0; i < init_range_cnt; ++i) {
    total_range_micro_cnt += init_range_arr[i]->get_total_micro_cnt();
  }
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt, total_range_micro_cnt); // not include force_deleted micro_metas and not persisted micro_meta

  ASSERT_EQ(ori_arc_limit, micro_meta_mgr_->get_arc_info().limit_);
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt, micro_meta_mgr_->replay_ctx_.total_replay_cnt_);
  ASSERT_EQ((WRITE_BLK_CNT - 2) * micro_cnt, micro_meta_mgr_->arc_info_.seg_info_arr_[SS_ARC_T1].cnt_);
  ASSERT_EQ(micro_cnt, micro_meta_mgr_->arc_info_.seg_info_arr_[SS_ARC_B1].cnt_);

  // 5. execute phy_block checkpoint
  ori_blk_ckpt_cnt = cache_stat.task_stat().blk_ckpt_cnt_;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.start_op());
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());
  ASSERT_EQ(ori_blk_ckpt_cnt + 1, cache_stat.task_stat().blk_ckpt_cnt_);
  const int64_t total_blk_cnt = phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_ - 2;
  ASSERT_EQ(total_blk_cnt, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ObSSMicroCacheSuperBlk cur_super_blk;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->get_ss_super_block(cur_super_blk));
  ASSERT_EQ(micro_ckpt_used_cnt, cur_super_blk.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_, cur_super_blk.blk_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_NE(prev_blk_ckpt_entry, cur_super_blk.blk_ckpt_info_.blk_ckpt_entry_);
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_, phy_blk_mgr_->super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_, phy_blk_mgr_->super_blk_.blk_ckpt_info_.get_total_used_blk_cnt());

  // 6. execute persist micro_meta
  int64_t ori_reusable_blk_cnt = phy_blk_mgr_->get_reusable_blocks_cnt();
  ori_micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.ckpt_item_cnt_ = 0; // manully set this value as 0
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.start_op());
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.gen_checkpoint());
  ASSERT_EQ(ori_micro_ckpt_cnt + 1, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_EQ((WRITE_BLK_CNT - 1) * micro_cnt, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(blk_ckpt_used_cnt, phy_blk_mgr_->super_blk_.blk_ckpt_info_.get_total_used_blk_cnt());
  int64_t delta_reusable_blk_cnt = phy_blk_mgr_->get_reusable_blocks_cnt() - ori_reusable_blk_cnt;
  micro_ckpt_used_cnt = phy_blk_mgr_->super_blk_.micro_ckpt_info_.get_total_used_blk_cnt();
  ASSERT_LT(0, micro_ckpt_used_cnt);
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_,
            phy_blk_mgr_->super_blk_.micro_ckpt_info_.get_total_used_blk_cnt() + delta_reusable_blk_cnt);
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_, phy_blk_mgr_->super_blk_.blk_ckpt_info_.get_total_used_blk_cnt());

  // 7. execute phy_block checkpoint
  ori_blk_ckpt_cnt = cache_stat.task_stat().blk_ckpt_cnt_;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.start_op());
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_scan_blk_time_us_ = ObTimeUtility::current_time_us() - SS_SCAN_REUSABLE_BLK_INTERVAL_US;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());
  ASSERT_EQ(ori_blk_ckpt_cnt + 1, cache_stat.task_stat().blk_ckpt_cnt_);
  ASSERT_EQ(0, phy_blk_mgr_->get_reusable_blocks_cnt());
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_ - 2, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_, phy_blk_mgr_->super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_, phy_blk_mgr_->super_blk_.blk_ckpt_info_.get_total_used_blk_cnt());

  allocator.clear();
  LOG_INFO("TEST: finish test_execute_checkpoint_task");
}

/* This case tests whether the micro cache can be restored to the expected state after restart.  */
TEST_F(TestSSExecuteCheckpointTask, test_micro_cache_ckpt_after_restart)
{
  LOG_INFO("TEST: start test_micro_cache_ckpt_after_restart",
    K(persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.cur_super_blk_.get_ckpt_split_cnt()),
    K(persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_super_blk_.get_ckpt_split_cnt()),
    K(phy_blk_mgr_->super_blk_.get_ckpt_split_cnt()));
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  micro_cache_->task_runner_.disable_task();
  micro_cache_->task_runner_.enable_persist_data();
  const int64_t total_blk_cnt = phy_blk_mgr_->blk_cnt_info_.total_blk_cnt_;
  ob_usleep(1000 * 1000);

  // 1. add some micro_block into cache and record their micro_meta
  ObArray<ObSSMicroBlockMetaInfo> micro_meta_info_arr1;
  int64_t macro_start_idx = 1;
  int64_t write_blk_cnt = 10;
  const int64_t micro_cnt = 20;
  ASSERT_EQ(OB_SUCCESS, add_batch_micro_block(macro_start_idx, write_blk_cnt, micro_cnt, micro_meta_info_arr1));
  micro_cache_->task_runner_.disable_persist_data();
  const int64_t used_data_blk_cnt1 = phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_;

  ObArray<ObSSPhyBlockInfoCkptItem> phy_blk_info_arr1;
  int64_t alloc_phy_blk_cnt = 20;
  ASSERT_EQ(OB_SUCCESS, alloc_batch_phy_block_to_reuse(alloc_phy_blk_cnt, phy_blk_info_arr1));

  micro_cache_->task_runner_.enable_meta_ckpt();
  micro_cache_->task_runner_.enable_blk_ckpt();
  persist_meta_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  blk_ckpt_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(1000 * 1000);
  ASSERT_EQ(0, phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr_->reusable_blks_.size());

  // 2. do ckpt_task first round
  int64_t start_time_us = ObTimeUtility::current_time_us();
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.start_op());
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.gen_checkpoint());
  const int64_t micro_exe_time_us = ObTimeUtility::current_time_us() - start_time_us;
  ASSERT_LT(0, phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_);
  ASSERT_EQ((write_blk_cnt - 1) * 20, persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.ckpt_item_cnt_);

  start_time_us = ObTimeUtility::current_time_us();
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.start_op());
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  // ensure scan reusable blks must invoke
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_scan_blk_time_us_ = TestSSCommonUtil::get_prev_scan_reusable_blk_time_us();
  ob_usleep(1000 * 1000);
  ASSERT_EQ(true, blk_ckpt_task_->ckpt_op_.need_scan_reusable_blocks());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->scan_blocks_to_reuse());
  ASSERT_EQ(alloc_phy_blk_cnt, phy_blk_mgr_->reusable_blks_.size());
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.finish_scan_phy_blk();

  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());
  const int64_t blk_exe_time_us = ObTimeUtility::current_time_us() - start_time_us;
  ASSERT_LT(0, phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(total_blk_cnt - SS_SUPER_BLK_COUNT, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);

  persist_meta_task_->is_inited_ = false;
  blk_ckpt_task_->is_inited_ = false;
  ObSSMicroCacheSuperBlk super_blk1;
  ASSERT_EQ(OB_SUCCESS, super_blk1.assign(phy_blk_mgr_->super_blk_));

  // 3. restart micro cache
  LOG_INFO("TEST: start first restart", K(phy_blk_mgr_->super_blk_));
  ASSERT_EQ(OB_SUCCESS, restart_micro_cache());
  micro_cache_->task_runner_.disable_task();
  LOG_INFO("TEST: finish first restart", K(phy_blk_mgr_->super_blk_));

  // 4. check super_block, micro_meta and phy_block_info
  ASSERT_EQ(OB_SUCCESS, check_phy_blk_info(phy_blk_info_arr1, 1));
  ASSERT_EQ(OB_SUCCESS, check_micro_meta(micro_meta_info_arr1, true));
  ASSERT_EQ(OB_SUCCESS, check_super_block(super_blk1));
  ASSERT_EQ(super_blk1.micro_ckpt_info_.get_total_used_blk_cnt(), phy_blk_mgr_->blk_cnt_info_.meta_blk_.used_cnt_);
  ASSERT_EQ(super_blk1.blk_ckpt_info_.get_total_used_blk_cnt(), phy_blk_mgr_->blk_cnt_info_.phy_ckpt_blk_used_cnt_);
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
  micro_cache_->task_runner_.enable_persist_data();
  ASSERT_EQ(OB_SUCCESS, add_batch_micro_block(macro_start_idx, write_blk_cnt, micro_cnt, micro_meta_info_arr2));
  micro_cache_->task_runner_.disable_persist_data();
  const int64_t used_data_blk_cnt2 = phy_blk_mgr_->blk_cnt_info_.data_blk_.used_cnt_;

  // 7. do ckpt_task second round and randomly force to end persist_meta_task and restart micro_cache
  micro_cache_->task_runner_.enable_meta_ckpt();
  micro_cache_->task_runner_.enable_blk_ckpt();
  persist_meta_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  blk_ckpt_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(2 * 1000 * 1000);

  // blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_scan_blk_time_us_ = TestSSCommonUtil::get_prev_scan_reusable_blk_time_us();
  ObSSMicroCacheSuperBlk super_blk2;
  ASSERT_EQ(OB_SUCCESS, super_blk2.assign(phy_blk_mgr_->super_blk_));

  std::thread micro_t([&]() {
    ObRandom rand;
    const int64_t sleep_us = ObRandom::rand(1, micro_exe_time_us * 2);
    ob_usleep(sleep_us);
    persist_meta_task_->persist_meta_op_.is_inited_ = false; // force to end micro_ckpt
  });
  persist_meta_task_->persist_meta_op_.start_op();
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  persist_meta_task_->persist_meta_op_.gen_checkpoint();
  micro_t.join();
  ObSSMicroCacheSuperBlk super_blk3;
  ASSERT_EQ(OB_SUCCESS, super_blk3.assign(phy_blk_mgr_->super_blk_));

  LOG_INFO("TEST: start second restart");
  ASSERT_EQ(OB_SUCCESS, restart_micro_cache());
  micro_cache_->task_runner_.disable_task();
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

TEST_F(TestSSExecuteCheckpointTask, test_micro_cache_ckpt_persist_many_meta)
{
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  micro_cache_->task_runner_.disable_task();
  micro_cache_->task_runner_.enable_persist_data();
  ob_usleep(1000 * 1000);

  // 1. destroy original init_range, build new init_range
  ObSSMicroRangeManager &micro_range_mgr = micro_cache_->micro_range_mgr_;
  micro_range_mgr.inner_destroy_initial_range();
  micro_range_mgr.init_range_cnt_ = 1000;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.inner_build_initial_range(false));

  // 2. add some micro_block into cache and record their micro_meta
  ObArray<ObSSMicroBlockMetaInfo> micro_meta_info_arr1;
  int64_t macro_start_idx = 1;
  int64_t write_blk_cnt = 20;
  const int64_t micro_cnt = 200;
  ASSERT_EQ(OB_SUCCESS, add_batch_micro_block(macro_start_idx, write_blk_cnt, micro_cnt, micro_meta_info_arr1));
  int64_t total_range_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.get_total_range_micro_cnt(total_range_micro_cnt));
  ASSERT_EQ(write_blk_cnt * micro_cnt, total_range_micro_cnt);
  micro_cache_->task_runner_.disable_persist_data();

  // 3. do micro_ckpt op
  micro_cache_->task_runner_.enable_meta_ckpt();
  persist_meta_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(1000 * 1000);
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.start_op());
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.gen_checkpoint());
  ASSERT_LT(0, persist_meta_task_->persist_meta_op_.full_ckpt_item_cnt_);
}

/* After persist_meta_task execucte scan_blocks_to_reuse, need_scan_reusable_blk() will return false */
TEST_F(TestSSExecuteCheckpointTask, test_ckpt_task_exec_scan_block)
{
  LOG_INFO("TEST: test_ckpt_task_exec_scan_block");
  int ret = OB_SUCCESS;
  persist_meta_task_->is_inited_ = false;

  const int64_t block_cnt = 10;
  for (int64_t i = 0; i < block_cnt; i++) {
    int64_t phy_blk_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->alloc_block(phy_blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
    phy_blk_handle()->is_free_ = false;
    phy_blk_handle()->is_sealed_ = true;
    phy_blk_handle()->valid_len_ = 0;
  }
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->scan_blocks_to_reuse());
  ASSERT_EQ(block_cnt, phy_blk_mgr_->get_reusable_blocks_cnt());

  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_scan_blk_time_us_ = TestSSCommonUtil::get_prev_scan_reusable_blk_time_us() - 1000 * 1000L;
  ASSERT_EQ(true, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_scan_reusable_blk());
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.finish_scan_phy_blk();
  ASSERT_EQ(false, blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_scan_reusable_blk());
}

/* Test whether extra meta blocks can be dynamically allocated when the number of blocks required
 * by micro_ckpt exceeds meta_blk.min_cnt_ */
TEST_F(TestSSExecuteCheckpointTask, test_reserve_micro_ckpt_blk)
{
  LOG_INFO("TEST_CASE: start test_reserve_micro_ckpt_blk");
  int ret = OB_SUCCESS;
  const int64_t start_case_us = ObTimeUtility::current_time_us();
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
  LOG_INFO("finish add batch micro_block", K(macro_cnt), K(micro_meta_info_arr.count()));

  // 2. mock shared_block is used up
  blk_cnt_info.data_blk_.hold_cnt_ = blk_cnt_info.data_blk_.max_cnt_;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;

  // 3. execute micro_meta_ckpt
  ASSERT_EQ(0, blk_cnt_info.meta_blk_.used_cnt_);
  int64_t micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
  const int64_t WAIT_TIMEOUT_S = 300;
  bool finish_check = false;
  int64_t check_start_s = ObTimeUtility::current_time_s();
  do {
    if (cache_stat.task_stat().micro_ckpt_cnt_ > micro_ckpt_cnt) {
      finish_check = true;
    } else {
      ob_usleep(2 * 1000 * 1000L);
      LOG_INFO("still waiting micro_ckpt", K(persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_));
    }
  } while (!finish_check && ObTimeUtility::current_time_s() - check_start_s <= WAIT_TIMEOUT_S);
  ASSERT_EQ(micro_ckpt_cnt + 1, cache_stat.task_stat().micro_ckpt_cnt_);

  ASSERT_EQ(blk_cnt_info.meta_blk_.hold_cnt_, blk_cnt_info.meta_blk_.used_cnt_);
  ASSERT_EQ(blk_cnt_info.meta_blk_.used_cnt_, phy_blk_mgr_->super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());

  // 4. revert step2
  blk_cnt_info.data_blk_.hold_cnt_ = blk_cnt_info.data_blk_.min_cnt_;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;

  // 5. try to adjust micro_meta_block in micro_meta_ckpt, check that the number of blocks used by micro_ckpt has increased
  const int64_t origin_meta_blk_usd_cnt = blk_cnt_info.meta_blk_.used_cnt_;
  blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.prev_print_stat_time_us_ = TestSSCommonUtil::get_prev_print_stat_time_us();
  ob_usleep(2 * 1000 * 1000);
  ASSERT_LT(2 * origin_meta_blk_usd_cnt, blk_cnt_info.meta_blk_.hold_cnt_);

  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
  finish_check = false;
  check_start_s = ObTimeUtility::current_time_s();
  do {
    if (cache_stat.task_stat().micro_ckpt_cnt_ > micro_ckpt_cnt + 1) {
      finish_check = true;
    } else {
      ob_usleep(2 * 1000 * 1000L);
      LOG_INFO("2nd: still waiting micro_ckpt", K(persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_));
    }
  } while (!finish_check && ObTimeUtility::current_time_s() - check_start_s <= WAIT_TIMEOUT_S);
  ASSERT_EQ(micro_ckpt_cnt + 2, cache_stat.task_stat().micro_ckpt_cnt_);
  ob_usleep(5 * 1000 * 1000L); // wait for update_super_block and try_free_blocks
  ASSERT_EQ(blk_cnt_info.meta_blk_.used_cnt_, phy_blk_mgr_->super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_LT(origin_meta_blk_usd_cnt, blk_cnt_info.meta_blk_.used_cnt_);
  ASSERT_LT(origin_meta_blk_usd_cnt, phy_blk_mgr_->super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  LOG_INFO("TEST_CASE: finish test_reserve_micro_ckpt_blk", "cost_us", ObTimeUtility::current_time_us() - start_case_us);
}

TEST_F(TestSSExecuteCheckpointTask, test_dynamic_update_cache_limit_size)
{
  LOG_INFO("TEST_CASE: start test_dynamic_update_cache_limit_size");
  ObSSARCInfo &arc_info = micro_cache_->micro_meta_mgr_.arc_info_;
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr_->blk_cnt_info_;
  ObSSPersistMicroMetaOp &micro_ckpt_op = persist_meta_task_->persist_meta_op_;
  ObSSDoBlkCheckpointOp &blk_ckpt_op = blk_ckpt_task_->ckpt_op_;

  const int64_t origin_limit = arc_info.limit_;
  const int64_t origin_work_limit = arc_info.work_limit_;
  const int64_t origin_p = arc_info.p_;
  const int64_t origin_max_p = arc_info.max_p_;
  const int64_t origin_min_p = arc_info.min_p_;

  // mock micro_ckpt use extra block from shared_blocks.
  // this will result in fewer available cache_data_blocks and dynamically lower arc_limit.
  blk_cnt_info.meta_blk_.hold_cnt_ = blk_cnt_info.meta_blk_.max_cnt_;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.meta_blk_.hold_cnt_ + blk_cnt_info.data_blk_.hold_cnt_;
  blk_ckpt_op.dynamic_update_cache_limit_size();

  ASSERT_GT(origin_limit, arc_info.limit_);
  ASSERT_GT(origin_work_limit, arc_info.work_limit_);
  ASSERT_GT(origin_p, arc_info.p_);
  ASSERT_GT(origin_max_p, arc_info.max_p_);
  ASSERT_GT(origin_min_p, arc_info.min_p_);

  blk_cnt_info.meta_blk_.hold_cnt_ = blk_cnt_info.meta_blk_.min_cnt_;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.meta_blk_.hold_cnt_ + blk_cnt_info.data_blk_.hold_cnt_;

  blk_ckpt_op.dynamic_update_cache_limit_size();
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

  blk_ckpt_op.dynamic_update_cache_limit_size();

  const int64_t arc_limit_pct = micro_cache_->is_mini_mode_ ? SS_MINI_MODE_ARC_LIMIT_PCT : SS_ARC_LIMIT_PCT;
  const int64_t new_limit = phy_blk_mgr_->get_cache_limit_size() * arc_limit_pct / 100;
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
  micro_meta.access_time_s_ = ObTimeUtility::current_time_s();

  const int64_t buf_size = 4096;
  char buf[buf_size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, micro_meta.serialize(buf, buf_size, pos));
  const int64_t avg_micro_size = micro_meta.get_serialize_size();
  ASSERT_EQ(pos, avg_micro_size);

  ObSSCkptPhyBlockItemWriter item_writer;
  ASSERT_EQ(OB_SUCCESS, item_writer.init(tenant_id, *phy_blk_mgr_, ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK));
  const int64_t total_micro_cnt = 100000;
  const int64_t seg_micro_cnt = 300;
  int64_t tmp_cnt = 0;
  int64_t total_write_seg_cnt = 0;
  for (int64_t i = 0; i < total_micro_cnt; ++i) {
    ++tmp_cnt;
    ASSERT_EQ(OB_SUCCESS, item_writer.write_item(buf, avg_micro_size));
    if (tmp_cnt == seg_micro_cnt) {
      bool is_persisted = false;
      int64_t seg_offset = -1;
      int64_t seg_len = -1;
      ASSERT_EQ(OB_SUCCESS, item_writer.close_segment(is_persisted, seg_offset, seg_len));
      tmp_cnt = 0;
      ++total_write_seg_cnt;
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
  int64_t total_read_seg_cnt = 0;
  for (int64_t i = 0; i < block_id_list.count(); ++i) {
    const int64_t cur_block_id = block_id_list.at(i);
    int64_t payload_offset = 0;
    if (cur_block_id != entry_block_id) {
      ASSERT_EQ(OB_SUCCESS, block_reader.inner_read_block_(cur_block_id, payload_offset));
      total_item_cnt += block_reader.ckpt_blk_header_.item_count_;
      total_blk_size += blk_size;
      total_read_seg_cnt += block_reader.ckpt_blk_header_.item_count_ / seg_micro_cnt;
      LOG_INFO("print block header", K(i), K(cur_block_id), K_(block_reader.common_header), K_(block_reader.ckpt_blk_header));
    }
  }
  double avg_cost = static_cast<double>(total_blk_size) / total_item_cnt;
  double avg_seg_cnt = static_cast<double>(total_read_seg_cnt) / (block_id_list.count() - 1);
  std::cout << "micro_size: " << avg_micro_size << ", micro_item_size: " << avg_cost << std::endl;
  LOG_INFO("TEST: finish test_estimate_micro_meta_persist_cost", K(total_item_cnt), K(total_blk_size), K(block_id_list.count()), K(total_write_seg_cnt), K(total_read_seg_cnt), K(avg_cost), K(avg_seg_cnt));
}

TEST_F(TestSSExecuteCheckpointTask, test_estimate_blk_info_persist_cost)
{
  LOG_INFO("TEST_CASE: start test_estimate_blk_info_persist_cost");
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr_->blk_cnt_info_;
  blk_cnt_info.phy_ckpt_blk_cnt_ = 100; // ensure the blk_info_ckpt block count is enough
  const int32_t blk_size = phy_blk_mgr_->get_block_size();
  persist_meta_task_->cur_interval_us_ = 60 * 60 * 1000 * 1000L;
  ob_usleep(1000 * 1000);

  ObSSCkptPhyBlockItemWriter item_writer;
  ASSERT_EQ(OB_SUCCESS, item_writer.init(tenant_id, *phy_blk_mgr_, ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK));
  const int64_t BUF_SIZE = 4096;
  char buf[BUF_SIZE];
  int64_t total_blk_info_cnt = 800000;
  int64_t avg_blk_info_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < total_blk_info_cnt; ++i){
    const int64_t cur_block_id = i % 8;
    int64_t pos = 0;
    ObSSPhyBlockInfoCkptItem ckpt_item;
    ckpt_item.blk_idx_ = cur_block_id;
    ckpt_item.reuse_version_ = i + 1;
    ASSERT_EQ(OB_SUCCESS, ckpt_item.serialize(buf, BUF_SIZE, pos));
    avg_blk_info_size = ckpt_item.get_serialize_size();
    ASSERT_EQ(pos, avg_blk_info_size);
    ASSERT_EQ(OB_SUCCESS, item_writer.write_item(buf, avg_blk_info_size));
  }
  std::cout << "avg_blk_info_size = " << avg_blk_info_size << std::endl;
  ASSERT_EQ(OB_SUCCESS, item_writer.close());

  int64_t entry_block_id = -1;
  ASSERT_EQ(OB_SUCCESS, item_writer.get_entry_block(entry_block_id));
  ASSERT_NE(-1, entry_block_id);
  ObArray<int64_t> block_id_list;
  ASSERT_EQ(OB_SUCCESS, item_writer.get_block_id_list(block_id_list));
  ASSERT_LT(1, block_id_list.count());

  int64_t total_item_cnt_2 = 0;
  int64_t total_blk_size_2 = 0;
  ObSSCkptPhyBlockReader block_reader;
  ASSERT_EQ(OB_SUCCESS, block_reader.init(entry_block_id, tenant_id, *phy_blk_mgr_));
  for (int64_t i = 0; i < block_id_list.count(); ++i) {
    const int64_t cur_block_id = block_id_list.at(i);
    int64_t payload_offset = 0;
    if (cur_block_id != entry_block_id) {
      ASSERT_EQ(OB_SUCCESS, block_reader.inner_read_block_(cur_block_id, payload_offset));
      total_item_cnt_2 += block_reader.ckpt_blk_header_.item_count_;
      total_blk_size_2 += blk_size;
      LOG_INFO("print block header", K(i), K(cur_block_id), K_(block_reader.common_header), K_(block_reader.ckpt_blk_header));
    }
  }
  double blk_info_avg_cost = static_cast<double>(total_blk_size_2) / total_item_cnt_2;
  std::cout << "blk_info_cost: " << blk_info_avg_cost << std::endl;
  LOG_INFO("TEST: finish test_estimate_blk_info_persist_cost", K(total_item_cnt_2), K(total_blk_size_2), K(blk_info_avg_cost), K(block_id_list.count()));
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_execute_checkpoint_task.log*");
  OB_LOGGER.set_file_name("test_ss_execute_checkpoint_task.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
