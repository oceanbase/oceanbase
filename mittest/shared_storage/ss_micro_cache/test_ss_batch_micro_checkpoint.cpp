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

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
class TestSSBatchMicroCheckpointTask : public ::testing::Test
{
public:
  TestSSBatchMicroCheckpointTask()
    : allocator_(), micro_cache_(nullptr)
  {}
  ~TestSSBatchMicroCheckpointTask() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

public:
  ObArenaAllocator allocator_;
  ObSSMicroCache *micro_cache_;
  uint64_t tenant_id_;
};

void TestSSBatchMicroCheckpointTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSBatchMicroCheckpointTask::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSBatchMicroCheckpointTask::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 32), 1/*micro_split_cnt*/));
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tnt_file_mgr);
  // tnt_file_mgr->is_cache_file_exist_ = true;
  micro_cache->start();
  micro_cache_ = micro_cache;
  tenant_id_ = MTL_ID();
}

void TestSSBatchMicroCheckpointTask::TearDown()
{
  ASSERT_NE(nullptr, micro_cache_);
  micro_cache_->stop();
  micro_cache_->wait();
  micro_cache_->destroy();
}

TEST_F(TestSSBatchMicroCheckpointTask, test_access_ghost_persisted_meta)
{
  LOG_INFO("TEST: start test_access_ghost_persisted_meta");
  ASSERT_NE(nullptr, micro_cache_);
  const int64_t block_size = micro_cache_->phy_blk_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  ObSSMicroRangeManager &micro_range_mgr = micro_cache_->micro_range_mgr_;
  ObSSPersistMicroMetaTask &persist_meta_task = micro_cache_->task_runner_.persist_meta_task_;
  const int64_t init_rng_cnt = micro_range_mgr.init_range_cnt_;
  ObSSMicroInitRangeInfo **init_rng_arr = micro_range_mgr.init_range_arr_;
  ASSERT_LT(0, init_rng_cnt);
  ASSERT_NE(nullptr, init_rng_arr);
  micro_meta_mgr.enable_save_meta_mem_ = true;

  ObArray<ObSSMicroBlockCacheKey> evict_key_arr;
  // add block
  const int64_t WRITE_BLK_CNT = 500;
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 128;
  const int32_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
  const int32_t min_micro_size = micro_size - 4 * 1024;
  const int32_t max_micro_size = micro_size + 4 * 1024;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(max_micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', max_micro_size);

  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    int32_t offset = payload_offset;
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t cur_micro_size = ObRandom::rand(min_micro_size, max_micro_size);
      offset += cur_micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, cur_micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_cache_->add_micro_block_cache(micro_key, data_buf, cur_micro_size,
          macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
      if (j % 10 == 0) {
        ASSERT_EQ(OB_SUCCESS, evict_key_arr.push_back(micro_key));
      }
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(WRITE_BLK_CNT * micro_cnt, cache_stat.micro_stat_.total_micro_cnt_);
  ASSERT_LT(0, cache_stat.micro_stat_.valid_micro_cnt_);

  persist_meta_task.cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(1000 * 1000);

  ASSERT_EQ(OB_SUCCESS, persist_meta_task.persist_meta_op_.start_op());
  persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task.persist_meta_op_.gen_checkpoint());
  ASSERT_LT(1, phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_LT(0, cache_stat.task_stat().micro_ckpt_item_cnt_);

  int64_t total_micro_cnt = 0;
  for (int64_t i = 0; i < init_rng_cnt; ++i) {
    total_micro_cnt += init_rng_arr[i]->get_total_micro_cnt();
  }
  ASSERT_EQ(total_micro_cnt, cache_stat.micro_stat().valid_micro_cnt_);

  ObArray<ObSSMicroBlockCacheKey> evicted_key_arr;
  for (int64_t i = 0; i < evict_key_arr.count(); ++i) {
    const ObSSMicroBlockCacheKey &micro_key = evict_key_arr.at(i);
    ObSSMicroBlockMetaInfo meta_info;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_meta_info(micro_key, meta_info));
    if (meta_info.can_evict()) {
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_evict_micro_block_meta(meta_info));
      const int64_t delta_size = meta_info.size() * -1;
      const uint64_t data_loc = meta_info.data_loc();
      const uint64_t reuse_version = meta_info.reuse_version();
      int64_t phy_blk_idx = -1;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_block_valid_length(data_loc, reuse_version, delta_size, phy_blk_idx));
      ASSERT_EQ(OB_SUCCESS, evicted_key_arr.push_back(micro_key));
    }
  }
  ASSERT_EQ(0, cache_stat.task_stat().readd_persisted_cnt_);

  ASSERT_EQ(OB_SUCCESS, persist_meta_task.persist_meta_op_.start_op());
  persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task.persist_meta_op_.gen_checkpoint());
  ASSERT_LT(0, cache_stat.task_stat().erase_persisted_cnt_);
  ASSERT_LT(0, cache_stat.range_stat().fe_sub_range_cnt_);
  ASSERT_LT(1, phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());

  ASSERT_EQ(OB_SUCCESS, persist_meta_task.persist_meta_op_.start_op());
  persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task.persist_meta_op_.gen_checkpoint());
  ASSERT_LT(0, cache_stat.task_stat().readd_persisted_cnt_);

  evict_key_arr.reset();
  for (int64_t i = WRITE_BLK_CNT; i < WRITE_BLK_CNT * 2; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    int32_t offset = payload_offset;
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t cur_micro_size = ObRandom::rand(min_micro_size, max_micro_size);
      offset += cur_micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, cur_micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_cache_->add_micro_block_cache(micro_key, data_buf, cur_micro_size,
          macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
      if (j % 10 == 0) {
        ASSERT_EQ(OB_SUCCESS, evict_key_arr.push_back(micro_key));
      }
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }

  ASSERT_LT(0, evicted_key_arr.count()); // NOTICE: evict_key_arr && evicted_key_arr
  for (int64_t i = 0; i < evicted_key_arr.count(); ++i) {
    const ObSSMicroBlockCacheKey &micro_key = evicted_key_arr.at(i);
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr.get_micro_block_meta(micro_key, micro_meta_handle, micro_key.get_macro_tablet_id().id(), true));
    ObSSMicroBlockMetaInfo meta_info;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_meta_info(micro_key, meta_info));
    ASSERT_EQ(true, meta_info.is_in_l1());
    ASSERT_EQ(true, meta_info.is_in_ghost());
  }
  ASSERT_LT(0, cache_stat.task_stat().readd_persisted_cnt_);

  ASSERT_EQ(OB_SUCCESS, micro_cache_->clear_micro_cache());
}

TEST_F(TestSSBatchMicroCheckpointTask, test_batch_micro_meta_ckpt)
{
  LOG_INFO("TEST: start test_batch_micro_meta_ckpt");
  ASSERT_NE(nullptr, micro_cache_);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSMicroRangeManager &micro_range_mgr = micro_cache_->micro_range_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  ObSSPersistMicroMetaTask &persist_meta_task = micro_cache_->task_runner_.persist_meta_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache_->task_runner_.blk_ckpt_task_;
  ObSSARCInfo &arc_info = micro_cache_->micro_meta_mgr_.arc_info_;
  const int64_t cache_file_size = micro_cache_->cache_file_size_;
  const int64_t block_size = micro_cache_->phy_blk_size_;

  ObArray<TestSSCommonUtil::MicroBlockInfo> p_micro_block_arr;
  ObArray<TestSSCommonUtil::MicroBlockInfo> up_micro_block_arr;
  const int64_t MAX_WAIT_MICRO_CKPT_TIME_S = 180;
  int64_t start_time_s = ObTimeUtility::current_time_s();

  // 1. ckpt_split_cnt = 1
  int64_t ckpt_split_cnt = 1;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::restart_micro_cache(micro_cache_, tenant_id_, cache_file_size, ckpt_split_cnt));
  ASSERT_NE(nullptr, micro_cache_);
  ASSERT_EQ(ckpt_split_cnt, phy_blk_mgr.super_blk_.get_ckpt_split_cnt());
  ASSERT_EQ(true, phy_blk_mgr.super_blk_.micro_ckpt_info_.check_init(ckpt_split_cnt));
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.exist_micro_checkpoint());
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.exist_blk_checkpoint());
  ASSERT_EQ(0, cache_stat.task_stat().blk_ckpt_item_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().blk_ckpt_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().micro_ckpt_item_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().micro_ckpt_op_cnt_);

  // 1.1 add some micro_block into cache
  const int64_t macro_block_cnt = 50;
  const int32_t min_micro_size = 8 * 1024;
  const int32_t max_micro_size = 16 * 1024;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::add_micro_blocks(macro_block_cnt, block_size, p_micro_block_arr, up_micro_block_arr,
            1, true, min_micro_size, max_micro_size));
  ASSERT_EQ(macro_block_cnt, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);
  ASSERT_EQ(macro_block_cnt, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  const int64_t total_add_micro_cnt = p_micro_block_arr.count() + up_micro_block_arr.count();
  ASSERT_LT(0, total_add_micro_cnt);
  ASSERT_EQ(total_add_micro_cnt, cache_stat.micro_stat().valid_micro_cnt_);

  // 1.2 execute micro_meta ckpt and blk_info ckpt
  {
    int64_t ori_blk_ckpt_cnt = cache_stat.task_stat().blk_ckpt_cnt_;
    blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
    start_time_s = ObTimeUtility::current_time_s();
    bool finish_check = false;
    do {
      if (cache_stat.task_stat().blk_ckpt_cnt_ > ori_blk_ckpt_cnt) {
        finish_check = true;
      } else {
        LOG_INFO("still waiting for blk_ckpt", K(ori_blk_ckpt_cnt), K(cache_stat.task_stat()));
        ob_usleep(1000 * 1000);
      }
    } while (!finish_check && (ObTimeUtility::current_time_s() - start_time_s < MAX_WAIT_MICRO_CKPT_TIME_S));
    ASSERT_EQ(ori_blk_ckpt_cnt + 1, cache_stat.task_stat().blk_ckpt_cnt_);
    ASSERT_LT(0, phy_blk_mgr.super_blk_.blk_ckpt_used_blk_list().count());
    ASSERT_LT(0, cache_stat.task_stat().blk_ckpt_item_cnt_);
  }
  {
    int64_t ori_micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
    int64_t ori_micro_ckpt_op_cnt = cache_stat.task_stat().micro_ckpt_op_cnt_;
    persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
    start_time_s = ObTimeUtility::current_time_s();
    bool finish_check = false;
    do {
      if (cache_stat.task_stat().micro_ckpt_cnt_ > ori_micro_ckpt_cnt) {
        finish_check = true;
      } else {
        LOG_INFO("still waiting for micro_ckpt", K(ori_micro_ckpt_cnt), K(cache_stat.task_stat()));
        ob_usleep(1000 * 1000);
      }
    } while (!finish_check && (ObTimeUtility::current_time_s() - start_time_s < MAX_WAIT_MICRO_CKPT_TIME_S));
    ASSERT_EQ(ori_micro_ckpt_cnt + 1, cache_stat.task_stat().micro_ckpt_cnt_);
    ASSERT_EQ(ori_micro_ckpt_op_cnt + 1, cache_stat.task_stat().micro_ckpt_op_cnt_);
    ASSERT_EQ(true, persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.complete_full_ckpt_);
    ASSERT_LT(0, phy_blk_mgr.super_blk_.get_micro_ckpt_used_blk_list_cnt());
    ASSERT_LT(0, cache_stat.task_stat().micro_ckpt_item_cnt_);
  }

  // 2. ckpt_split_cnt = 50
  int64_t prev_used_data_blk_cnt = cache_stat.phy_blk_stat().data_blk_used_cnt_;
  ASSERT_LT(0, prev_used_data_blk_cnt);
  int64_t prev_persisted_micro_cnt = cache_stat.task_stat().micro_ckpt_item_cnt_;
  ASSERT_LT(0, prev_persisted_micro_cnt);
  ckpt_split_cnt = 50;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::restart_micro_cache(micro_cache_, tenant_id_, cache_file_size, ckpt_split_cnt));
  ASSERT_NE(nullptr, micro_cache_);
  ASSERT_EQ(ckpt_split_cnt, phy_blk_mgr.super_blk_.get_ckpt_split_cnt());
  ASSERT_EQ(true, phy_blk_mgr.super_blk_.micro_ckpt_info_.check_init(ckpt_split_cnt));
  ASSERT_EQ(false, phy_blk_mgr.super_blk_.micro_ckpt_info_.exist_valid_ckpt());
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.blk_ckpt_info_.blk_ckpt_used_blks_.count());
  ASSERT_EQ(SS_INVALID_PHY_BLK_ID, phy_blk_mgr.super_blk_.blk_ckpt_info_.blk_ckpt_entry_);
  ASSERT_EQ(prev_persisted_micro_cnt, cache_stat.micro_stat().valid_micro_cnt_);
  int64_t total_range_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.get_total_range_micro_cnt(total_range_micro_cnt));
  ASSERT_EQ(prev_persisted_micro_cnt, total_range_micro_cnt);
  ASSERT_EQ(prev_used_data_blk_cnt, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().micro_ckpt_item_cnt_);

  // 2.2 execute micro_meta ckpt and blk_info ckpt
  {
    int64_t ori_blk_ckpt_cnt = cache_stat.task_stat().blk_ckpt_cnt_;
    blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
    start_time_s = ObTimeUtility::current_time_s();
    bool finish_check = false;
    do {
      if (cache_stat.task_stat().blk_ckpt_cnt_ > ori_blk_ckpt_cnt) {
        finish_check = true;
      } else {
        LOG_INFO("still waiting for blk_ckpt", K(ori_blk_ckpt_cnt), K(cache_stat.task_stat()));
        ob_usleep(1000 * 1000);
      }
    } while (!finish_check && (ObTimeUtility::current_time_s() - start_time_s < MAX_WAIT_MICRO_CKPT_TIME_S));
    ASSERT_EQ(ori_blk_ckpt_cnt + 1, cache_stat.task_stat().blk_ckpt_cnt_);
    ASSERT_LT(0, phy_blk_mgr.super_blk_.blk_ckpt_used_blk_list().count());
    ASSERT_LT(0, cache_stat.task_stat().blk_ckpt_item_cnt_);
  }
  {
    int64_t ori_micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
    int64_t ori_micro_ckpt_op_cnt = cache_stat.task_stat().micro_ckpt_op_cnt_;
    persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
    start_time_s = ObTimeUtility::current_time_s();
    bool finish_check = false;
    do {
      if (cache_stat.task_stat().micro_ckpt_cnt_ > ori_micro_ckpt_cnt) {
        finish_check = true;
      } else {
        LOG_INFO("still waiting for micro_ckpt", K(ori_micro_ckpt_cnt), K(cache_stat.task_stat()));
        ob_usleep(1000 * 1000);
      }
    } while (!finish_check && (ObTimeUtility::current_time_s() - start_time_s < MAX_WAIT_MICRO_CKPT_TIME_S));
    ASSERT_EQ(ori_micro_ckpt_cnt + 1, cache_stat.task_stat().micro_ckpt_cnt_);
    ASSERT_EQ(ori_micro_ckpt_op_cnt + ckpt_split_cnt, cache_stat.task_stat().micro_ckpt_op_cnt_);
    ASSERT_EQ(true, persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.complete_full_ckpt_);
    ASSERT_LT(0, phy_blk_mgr.super_blk_.get_micro_ckpt_used_blk_list_cnt());
    ASSERT_EQ(ckpt_split_cnt, phy_blk_mgr.super_blk_.micro_ckpt_entries().count());
    ASSERT_LT(0, cache_stat.task_stat().micro_ckpt_item_cnt_);
    ASSERT_GE(prev_persisted_micro_cnt, cache_stat.task_stat().micro_ckpt_item_cnt_);
  }

  // 3. ckpt_split_cnt = 10
  prev_used_data_blk_cnt = cache_stat.phy_blk_stat().data_blk_used_cnt_;
  ASSERT_LT(0, prev_used_data_blk_cnt);
  prev_persisted_micro_cnt = cache_stat.task_stat().micro_ckpt_item_cnt_;
  ASSERT_LT(0, prev_persisted_micro_cnt);
  ckpt_split_cnt = 10;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::restart_micro_cache(micro_cache_, tenant_id_, cache_file_size, ckpt_split_cnt));
  ASSERT_NE(nullptr, micro_cache_);
  ASSERT_EQ(ckpt_split_cnt, phy_blk_mgr.super_blk_.get_ckpt_split_cnt());
  ASSERT_EQ(true, phy_blk_mgr.super_blk_.micro_ckpt_info_.check_init(ckpt_split_cnt));
  ASSERT_EQ(false, phy_blk_mgr.super_blk_.micro_ckpt_info_.exist_valid_ckpt());
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.blk_ckpt_info_.blk_ckpt_used_blks_.count());
  ASSERT_EQ(SS_INVALID_PHY_BLK_ID, phy_blk_mgr.super_blk_.blk_ckpt_info_.blk_ckpt_entry_);
  ASSERT_EQ(prev_persisted_micro_cnt, cache_stat.micro_stat().valid_micro_cnt_);
  total_range_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.get_total_range_micro_cnt(total_range_micro_cnt));
  ASSERT_EQ(prev_persisted_micro_cnt, total_range_micro_cnt);
  ASSERT_EQ(prev_used_data_blk_cnt, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().micro_ckpt_item_cnt_);

  // 3.2 execute micro_meta ckpt and blk_info ckpt
  {
    int64_t ori_blk_ckpt_cnt = cache_stat.task_stat().blk_ckpt_cnt_;
    blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
    start_time_s = ObTimeUtility::current_time_s();
    bool finish_check = false;
    do {
      if (cache_stat.task_stat().blk_ckpt_cnt_ > ori_blk_ckpt_cnt) {
        finish_check = true;
      } else {
        LOG_INFO("still waiting for blk_ckpt", K(ori_blk_ckpt_cnt), K(cache_stat.task_stat()));
        ob_usleep(1000 * 1000);
      }
    } while (!finish_check && (ObTimeUtility::current_time_s() - start_time_s < MAX_WAIT_MICRO_CKPT_TIME_S));
    ASSERT_EQ(ori_blk_ckpt_cnt + 1, cache_stat.task_stat().blk_ckpt_cnt_);
    ASSERT_LT(0, phy_blk_mgr.super_blk_.blk_ckpt_used_blk_list().count());
    ASSERT_LT(0, cache_stat.task_stat().blk_ckpt_item_cnt_);
  }
  {
    int64_t ori_micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
    int64_t ori_micro_ckpt_op_cnt = cache_stat.task_stat().micro_ckpt_op_cnt_;
    persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
    start_time_s = ObTimeUtility::current_time_s();
    bool finish_check = false;
    do {
      if (cache_stat.task_stat().micro_ckpt_cnt_ > ori_micro_ckpt_cnt) {
        finish_check = true;
      } else {
        LOG_INFO("still waiting for micro_ckpt", K(ori_micro_ckpt_cnt), K(cache_stat.task_stat()));
        ob_usleep(1000 * 1000);
      }
    } while (!finish_check && (ObTimeUtility::current_time_s() - start_time_s < MAX_WAIT_MICRO_CKPT_TIME_S));
    ASSERT_EQ(ori_micro_ckpt_cnt + 1, cache_stat.task_stat().micro_ckpt_cnt_);
    ASSERT_EQ(ori_micro_ckpt_op_cnt + ckpt_split_cnt, cache_stat.task_stat().micro_ckpt_op_cnt_);
    ASSERT_EQ(true, persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.complete_full_ckpt_);
    ASSERT_LT(0, phy_blk_mgr.super_blk_.get_micro_ckpt_used_blk_list_cnt());
    ASSERT_EQ(ckpt_split_cnt, phy_blk_mgr.super_blk_.micro_ckpt_entries().count());
    ASSERT_LT(0, cache_stat.task_stat().micro_ckpt_item_cnt_);
    ASSERT_GE(prev_persisted_micro_cnt, cache_stat.task_stat().micro_ckpt_item_cnt_);
    LOG_INFO("TEST: finish test_batch_micro_meta_ckpt");
  }
}

TEST_F(TestSSBatchMicroCheckpointTask, test_multi_times_ckpt)
{
  LOG_INFO("TEST: start test_multi_times_ckpt");
  ASSERT_NE(nullptr, micro_cache_);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSMicroRangeManager &micro_range_mgr = micro_cache_->micro_range_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  ObSSPersistMicroMetaTask &persist_meta_task = micro_cache_->task_runner_.persist_meta_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache_->task_runner_.blk_ckpt_task_;
  ObSSARCInfo &arc_info = micro_cache_->micro_meta_mgr_.arc_info_;
  const int64_t cache_file_size = micro_cache_->cache_file_size_;
  const int64_t block_size = micro_cache_->phy_blk_size_;

  const int64_t MAX_WAIT_MICRO_CKPT_TIME_S = 180;
  int64_t start_time_s = ObTimeUtility::current_time_s();

  int64_t ckpt_split_cnt = 100;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::restart_micro_cache(micro_cache_, tenant_id_, cache_file_size, ckpt_split_cnt, true));

  persist_meta_task.cur_interval_us_ = 3600 * 1000 * 1000L;
  blk_ckpt_task.cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(2 * 1000 * 1000);

  // 1. add some micro_block into cache
  ObArray<TestSSCommonUtil::MicroBlockInfo> p_micro_block_arr;
  ObArray<TestSSCommonUtil::MicroBlockInfo> up_micro_block_arr;
  const int64_t macro_block_cnt = 100;
  const int32_t min_micro_size = 4 * 1024;
  const int32_t max_micro_size = 8 * 1024;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::add_micro_blocks(macro_block_cnt, block_size, p_micro_block_arr, up_micro_block_arr,
            1, true, min_micro_size, max_micro_size));
  ASSERT_EQ(macro_block_cnt, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);
  ASSERT_EQ(macro_block_cnt, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  const int64_t total_add_micro_cnt = p_micro_block_arr.count() + up_micro_block_arr.count();
  ASSERT_LT(0, total_add_micro_cnt);
  ASSERT_EQ(total_add_micro_cnt, cache_stat.micro_stat().valid_micro_cnt_);

  // 2. execute multi times checkpoint
  const int64_t ckpt_total_cnt = 1000;
  const int64_t blk_ckpt_interval = 10;
  for (int64_t i = 0; i < ckpt_total_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, persist_meta_task.persist_meta_op_.start_op());
    persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
    persist_meta_task.persist_meta_op_.gen_checkpoint();
    if (i % blk_ckpt_interval == 0) {
      ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.start_op());
      blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
      ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.gen_checkpoint());
    }
  }
  ASSERT_LT(0, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_LT(0, cache_stat.task_stat().micro_ckpt_op_cnt_);
  ASSERT_LT(0, cache_stat.task_stat().blk_ckpt_cnt_);

  LOG_INFO("TEST: finish test_multi_times_ckpt", K(cache_stat), K(phy_blk_mgr.blk_cnt_info_));
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_batch_micro_checkpoint.log*");
  OB_LOGGER.set_file_name("test_ss_batch_micro_checkpoint.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
