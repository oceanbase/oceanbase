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
class TestSSPersistMicroMetaTask : public ::testing::Test
{
public:
  TestSSPersistMicroMetaTask()
    : allocator_(), micro_cache_(nullptr)
  {}
  ~TestSSPersistMicroMetaTask() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

public:
  ObArenaAllocator allocator_;
  ObSSMicroCache *micro_cache_;
  uint64_t tenant_id_;
};

void TestSSPersistMicroMetaTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSPersistMicroMetaTask::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSPersistMicroMetaTask::SetUp()
{
  micro_cache_ = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache_);
  tenant_id_ = MTL_ID();
}

void TestSSPersistMicroMetaTask::TearDown()
{
  ASSERT_NE(nullptr, micro_cache_);
  micro_cache_->stop();
  micro_cache_->wait();
  micro_cache_->destroy();
}

TEST_F(TestSSPersistMicroMetaTask, test_persist_micro_meta)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_persist_micro_meta");
  ASSERT_NE(nullptr, micro_cache_);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache_->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  ObSSMicroRangeManager &micro_range_mgr = micro_cache_->micro_range_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  ObSSPersistMicroMetaTask &persist_meta_task = micro_cache_->task_runner_.persist_meta_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache_->task_runner_.blk_ckpt_task_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  const int64_t cache_file_size = micro_cache_->cache_file_size_;
  const int64_t block_size = micro_cache_->phy_blk_size_;

  // NOTICE: enable save meta memory
  micro_meta_mgr.enable_save_meta_mem_ = true;

  ObArray<TestSSCommonUtil::MicroBlockInfo> p_micro_block_arr;
  ObArray<TestSSCommonUtil::MicroBlockInfo> up_micro_block_arr;
  const int64_t MAX_WAIT_MICRO_CKPT_TIME_S = 180;
  int64_t start_time_s = ObTimeUtility::current_time_s();

  ASSERT_EQ(0, cache_stat.task_stat().blk_ckpt_item_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().blk_ckpt_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().micro_ckpt_item_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_EQ(0, cache_stat.task_stat().micro_ckpt_op_cnt_);

  // 1. add some micro_block into cache
  LOG_INFO("TEST: start step 1");
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
  int64_t total_micro_size = 0;
  for (int64_t i = 0; i < p_micro_block_arr.count(); ++i) {
    total_micro_size += p_micro_block_arr.at(i).size_;
  }
  for (int64_t i = 0; i < up_micro_block_arr.count(); ++i) {
    total_micro_size += up_micro_block_arr.at(i).size_;
  }
  ASSERT_LE(total_add_micro_cnt * min_micro_size, total_micro_size);
  ASSERT_GE(total_add_micro_cnt * max_micro_size, total_micro_size);
  ASSERT_EQ(total_micro_size, arc_info.get_l1_size() + arc_info.get_l2_size());
  int64_t total_range_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.get_total_range_micro_cnt(total_range_micro_cnt));
  ASSERT_EQ(total_add_micro_cnt, total_range_micro_cnt);

  // 2. try evict some micro_metas
  LOG_INFO("TEST: start step 2");
  ObArray<ObSSMicroBlockCacheKey> evict_micro_keys;
  ObArray<ObSSMicroBlockMetaInfo> evict_micro_infos;
  const int64_t EVICT_MICRO_CNT = 100;
  int64_t persisted_micro_cnt = 0;
  for (int64_t i = 0; i < p_micro_block_arr.count(); ++i) {
    TestSSCommonUtil::MicroBlockInfo cur_info = p_micro_block_arr.at(i);
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(cur_info.macro_id_, cur_info.offset_, cur_info.size_);
    ObSSMicroBlockMetaInfo cold_micro_info;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_meta_info(micro_key, cold_micro_info));
    if (cold_micro_info.is_data_persisted()) {
      ++persisted_micro_cnt;
      if (evict_micro_keys.count() < EVICT_MICRO_CNT) {
        ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_evict_micro_block_meta(cold_micro_info));
        ASSERT_EQ(OB_SUCCESS, evict_micro_keys.push_back(micro_key));

        cold_micro_info.reset();
        ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_meta_info(micro_key, cold_micro_info));
        ASSERT_EQ(OB_SUCCESS, evict_micro_infos.push_back(cold_micro_info));
      }
    }
  }
  for (int64_t i = 0; i < up_micro_block_arr.count(); ++i) {
    TestSSCommonUtil::MicroBlockInfo cur_info = up_micro_block_arr.at(i);
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(cur_info.macro_id_, cur_info.offset_, cur_info.size_);
    ObSSMicroBlockMetaInfo micro_info;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_meta_info(micro_key, micro_info));
    if (micro_info.is_data_persisted()) {
      ++persisted_micro_cnt;
    }
  }
  ASSERT_EQ(EVICT_MICRO_CNT, evict_micro_keys.count());
  ASSERT_EQ(p_micro_block_arr.count(), persisted_micro_cnt);
  ASSERT_EQ(EVICT_MICRO_CNT, cache_stat.task_stat().t1_evict_cnt_ + cache_stat.task_stat().t2_evict_cnt_);
  ASSERT_EQ(total_add_micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(total_add_micro_cnt - EVICT_MICRO_CNT, cache_stat.micro_stat().valid_micro_cnt_);
  ASSERT_EQ(total_micro_size, arc_info.get_l1_size() + arc_info.get_l2_size());
  ASSERT_EQ(EVICT_MICRO_CNT, arc_info.get_ghost_count());

  // 3. execute micro_meta ckpt and blk_info ckpt
  LOG_INFO("TEST: start step 3");
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
    ASSERT_EQ(blk_cnt_info.total_blk_cnt_ - blk_cnt_info.super_blk_cnt_, cache_stat.task_stat().blk_ckpt_item_cnt_);
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
    ASSERT_LT(0, phy_blk_mgr.super_blk_.get_micro_ckpt_used_blk_list_cnt());
    ASSERT_EQ(p_micro_block_arr.count(), cache_stat.task_stat().micro_ckpt_item_cnt_);
    ASSERT_EQ(total_add_micro_cnt - EVICT_MICRO_CNT, cache_stat.micro_stat().total_micro_cnt_);
    ASSERT_EQ(total_add_micro_cnt - EVICT_MICRO_CNT, cache_stat.micro_stat().valid_micro_cnt_);
    ASSERT_EQ(total_add_micro_cnt - EVICT_MICRO_CNT, micro_meta_mgr.micro_meta_map_.count());
    ASSERT_EQ(total_micro_size, arc_info.get_l1_size() + arc_info.get_l2_size());
    ASSERT_EQ(EVICT_MICRO_CNT, arc_info.get_ghost_count());
    total_range_micro_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, micro_range_mgr.get_total_range_micro_cnt(total_range_micro_cnt));
    ASSERT_EQ(total_add_micro_cnt - EVICT_MICRO_CNT, total_range_micro_cnt);
  }

  // 3.1 check all sub_range state
  LOG_INFO("TEST: start step 3.1");
  const int64_t init_range_cnt = micro_range_mgr.init_range_cnt_;
  ObSSMicroInitRangeInfo **init_range_arr = micro_range_mgr.init_range_arr_;
  int64_t all_pmeta_cnt = 0;
  for (int64_t i = 0; i < init_range_cnt; ++i) {
    ObSSMicroInitRangeInfo *init_range = init_range_arr[i];
    ASSERT_NE(nullptr, init_range);

    ObSSMicroSubRangeInfo *cur_sub_range = init_range->next_;
    while (nullptr != cur_sub_range) {
      if (cur_sub_range->phy_blk_idx_ != -1) {
        all_pmeta_cnt += cur_sub_range->get_all_pmeta_cnt();
      }
      cur_sub_range = cur_sub_range->next_;
    }
  }
  ASSERT_EQ(all_pmeta_cnt, cache_stat.task_stat().micro_ckpt_item_cnt_);

  for (int64_t i = 0; i < evict_micro_keys.count(); ++i) {
    const ObSSMicroBlockCacheKey &cur_micro_key = evict_micro_keys.at(i);
    const uint32_t hval = cur_micro_key.micro_hash();
    ObSSMicroSubRangeHandle sub_range_handle;
    ASSERT_EQ(OB_SUCCESS, micro_range_mgr.inner_get_range(hval, sub_range_handle));
    ASSERT_EQ(true, sub_range_handle.is_valid());
    ASSERT_NE(nullptr, sub_range_handle()->bl_filter_);
    ASSERT_EQ(false, sub_range_handle()->all_mem_meta_);
    ASSERT_LT(0, sub_range_handle()->get_all_pmeta_cnt());
  }

  // 4. get ghost micro_block_meta
  LOG_INFO("TEST: start step 4");
  for (int64_t i = 0; i < evict_micro_keys.count(); ++i) {
    const ObSSMicroBlockCacheKey &cur_micro_key = evict_micro_keys.at(i);
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ObSSMicroBlockMetaInfo micro_meta_info;
    ObSSMemBlockHandle mem_blk_handle;
    ObSSPhyBlockHandle phy_blk_handle;
    ObSSMicroCacheAccessInfo access_info;
    SSMicroMapGetMicroHandleFunc get_func(micro_meta_handle, mem_blk_handle, phy_blk_handle, true, access_info);
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_load_persisted_micro_meta(cur_micro_key, get_func, micro_meta_info));
    ASSERT_EQ(false, micro_meta_handle.is_valid()); // cuz ghost micro_meta is not valid, thus will not return valid micro_meta_handle
    micro_meta_info.reset();
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_meta_info(cur_micro_key, micro_meta_info));
    {
      ASSERT_EQ(evict_micro_infos.at(i).first_val_, micro_meta_info.first_val_);
      ASSERT_EQ(evict_micro_infos.at(i).second_val_, micro_meta_info.second_val_);
      ASSERT_EQ(evict_micro_infos.at(i).reuse_version_, micro_meta_info.reuse_version_);
      ASSERT_EQ(evict_micro_infos.at(i).effective_tablet_id_, micro_meta_info.effective_tablet_id_);
      ASSERT_EQ(evict_micro_infos.at(i).micro_key_, micro_meta_info.micro_key_);
      ASSERT_EQ(evict_micro_infos.at(i).size_, micro_meta_info.size_);
      ASSERT_EQ(evict_micro_infos.at(i).access_type_, micro_meta_info.access_type_);
      ASSERT_EQ(evict_micro_infos.at(i).is_in_l1_, micro_meta_info.is_in_l1_);
      ASSERT_EQ(evict_micro_infos.at(i).is_in_ghost_, micro_meta_info.is_in_ghost_);
      ASSERT_EQ(evict_micro_infos.at(i).is_data_persisted_, micro_meta_info.is_data_persisted_);
      ASSERT_EQ(evict_micro_infos.at(i).is_meta_dirty_, micro_meta_info.is_meta_dirty_);
      ASSERT_EQ(evict_micro_infos.at(i).is_meta_partial_, micro_meta_info.is_meta_partial_);
      ASSERT_EQ(evict_micro_infos.at(i).is_meta_deleted_, micro_meta_info.is_meta_deleted_);
    }
  }
  ASSERT_EQ(total_add_micro_cnt, micro_meta_mgr.micro_meta_map_.count());
  ASSERT_EQ(total_add_micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(total_add_micro_cnt - EVICT_MICRO_CNT, cache_stat.micro_stat().valid_micro_cnt_);

  // 5. continue to add micro_block
  LOG_INFO("TEST: start step 5");
  int64_t cur_total_micro_cnt = 0;
  int64_t cur_total_micro_size = 0;
  ObArray<TestSSCommonUtil::MicroBlockInfo> p_micro_block_arr_2;
  ObArray<TestSSCommonUtil::MicroBlockInfo> up_micro_block_arr_2;
  int64_t change_micro_size = 0;
  for (int64_t i = 0; i < up_micro_block_arr.count(); ++i) { // un-persisted micro_meta will be changed to persisted micro_meta
    ASSERT_EQ(OB_SUCCESS, p_micro_block_arr_2.push_back(up_micro_block_arr.at(i)));
    change_micro_size += up_micro_block_arr.at(i).size_;
  }
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::add_micro_blocks(macro_block_cnt, block_size, p_micro_block_arr_2,
            up_micro_block_arr_2, 101, true, min_micro_size, max_micro_size));
  ASSERT_EQ(macro_block_cnt * 2, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);
  ASSERT_EQ(macro_block_cnt * 2, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  int64_t total_micro_size_2 = 0;
  for (int64_t i = 0; i < p_micro_block_arr_2.count(); ++i) {
    total_micro_size_2 += p_micro_block_arr_2.at(i).size_;
  }
  for (int64_t i = 0; i < up_micro_block_arr_2.count(); ++i) {
    total_micro_size_2 += up_micro_block_arr_2.at(i).size_;
  }
  total_micro_size_2 -= change_micro_size;
  ASSERT_LT(0, total_micro_size_2);
  int64_t total_add_micro_cnt_2 = p_micro_block_arr_2.count() + up_micro_block_arr_2.count() - up_micro_block_arr.count();
  ASSERT_LT(0, total_add_micro_cnt_2);

  cur_total_micro_cnt = total_add_micro_cnt + total_add_micro_cnt_2;
  cur_total_micro_size = total_micro_size + total_micro_size_2;
  ASSERT_EQ(cur_total_micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(cur_total_micro_size, cache_stat.micro_stat().total_micro_size_);

  // 6. continue to try evict some micro_metas
  LOG_INFO("TEST: start step 6");
  for (int64_t i = 0; i < p_micro_block_arr_2.count(); ++i) {
    TestSSCommonUtil::MicroBlockInfo cur_info = p_micro_block_arr_2.at(i);
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(cur_info.macro_id_, cur_info.offset_, cur_info.size_);
    ObSSMicroBlockMetaInfo cold_micro_info;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_meta_info(micro_key, cold_micro_info));
    if (cold_micro_info.is_data_persisted() && evict_micro_keys.count() < EVICT_MICRO_CNT * 2) {
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_evict_micro_block_meta(cold_micro_info));
      ASSERT_EQ(OB_SUCCESS, evict_micro_keys.push_back(micro_key));

      cold_micro_info.reset();
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_meta_info(micro_key, cold_micro_info));
      ASSERT_EQ(OB_SUCCESS, evict_micro_infos.push_back(cold_micro_info));
    }
  }
  ASSERT_EQ(cur_total_micro_cnt - EVICT_MICRO_CNT * 2, cache_stat.micro_stat().valid_micro_cnt_);
  ASSERT_EQ(cur_total_micro_size, arc_info.get_l1_size() + arc_info.get_l2_size());
  ASSERT_EQ(EVICT_MICRO_CNT * 2, arc_info.get_ghost_count());

  // 7. second execute micro_meta ckpt and blk_info ckpt
  LOG_INFO("TEST: start step 7");
  {
    int64_t ori_blk_ckpt_cnt = cache_stat.task_stat().blk_ckpt_cnt_;
    int64_t ori_blk_ckpt_used_cnt = phy_blk_mgr.super_blk_.blk_ckpt_used_blk_list().count();
    int64_t ori_blk_ckpt_entry = phy_blk_mgr.super_blk_.blk_ckpt_info_.blk_ckpt_entry_;
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
    ASSERT_EQ(ori_blk_ckpt_used_cnt, phy_blk_mgr.super_blk_.blk_ckpt_used_blk_list().count());
    ASSERT_EQ(blk_cnt_info.total_blk_cnt_ - blk_cnt_info.super_blk_cnt_, cache_stat.task_stat().blk_ckpt_item_cnt_);
    ASSERT_NE(ori_blk_ckpt_entry, phy_blk_mgr.super_blk_.blk_ckpt_info_.blk_ckpt_entry_);
  }
  {
    int64_t ori_micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
    int64_t ori_micro_ckpt_op_cnt = cache_stat.task_stat().micro_ckpt_op_cnt_;
    int64_t ori_micro_ckpt_used_cnt = phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt();
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
    ASSERT_LE(ori_micro_ckpt_used_cnt, phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
    ASSERT_GE(p_micro_block_arr.count() + p_micro_block_arr_2.count(), cache_stat.task_stat().micro_ckpt_item_cnt_);
    ASSERT_EQ(cur_total_micro_cnt - EVICT_MICRO_CNT * 2, cache_stat.micro_stat().total_micro_cnt_);
    ASSERT_EQ(cur_total_micro_cnt - EVICT_MICRO_CNT * 2, cache_stat.micro_stat().valid_micro_cnt_);
    ASSERT_EQ(cur_total_micro_cnt - EVICT_MICRO_CNT * 2, micro_meta_mgr.micro_meta_map_.count());
    ASSERT_EQ(cur_total_micro_size, arc_info.get_l1_size() + arc_info.get_l2_size());
    ASSERT_EQ(EVICT_MICRO_CNT * 2, arc_info.get_ghost_count());
  }

  // 8. third execute micro_meta ckpt
  LOG_INFO("TEST: start step 8");
  {
    int64_t ori_micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
    int64_t ori_micro_ckpt_op_cnt = cache_stat.task_stat().micro_ckpt_op_cnt_;
    int64_t ori_micro_ckpt_used_cnt = phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt();
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
    ASSERT_LE(ori_micro_ckpt_used_cnt, phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
    ASSERT_GE(p_micro_block_arr.count() + p_micro_block_arr_2.count(), cache_stat.task_stat().micro_ckpt_item_cnt_);
    ASSERT_EQ(cur_total_micro_cnt - EVICT_MICRO_CNT * 2, cache_stat.micro_stat().total_micro_cnt_);
    ASSERT_EQ(cur_total_micro_cnt - EVICT_MICRO_CNT * 2, cache_stat.micro_stat().valid_micro_cnt_);
    ASSERT_EQ(cur_total_micro_cnt - EVICT_MICRO_CNT * 2, micro_meta_mgr.micro_meta_map_.count());
    ASSERT_EQ(cur_total_micro_size, arc_info.get_l1_size() + arc_info.get_l2_size());
    ASSERT_EQ(EVICT_MICRO_CNT * 2, arc_info.get_ghost_count());
  }

  // 9. get extra ghost micro_meta
  LOG_INFO("TEST: start step 9");
  for (int64_t i = EVICT_MICRO_CNT; i < evict_micro_keys.count(); ++i) {
    const ObSSMicroBlockCacheKey &cur_micro_key = evict_micro_keys.at(i);
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ObSSMicroBlockMetaInfo micro_meta_info;
    ObSSMemBlockHandle mem_blk_handle;
    ObSSPhyBlockHandle phy_blk_handle;
    ObSSMicroCacheAccessInfo access_info;
    SSMicroMapGetMicroHandleFunc get_func(micro_meta_handle, mem_blk_handle, phy_blk_handle, true, access_info);
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_load_persisted_micro_meta(cur_micro_key, get_func, micro_meta_info));
    ASSERT_EQ(false, micro_meta_handle.is_valid());
    micro_meta_info.reset();
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_meta_info(cur_micro_key, micro_meta_info));
    {
      ASSERT_EQ(evict_micro_infos.at(i).first_val_, micro_meta_info.first_val_);
      ASSERT_EQ(evict_micro_infos.at(i).second_val_, micro_meta_info.second_val_);
      ASSERT_EQ(evict_micro_infos.at(i).micro_key_, micro_meta_info.micro_key_);
      ASSERT_EQ(evict_micro_infos.at(i).is_in_l1_, micro_meta_info.is_in_l1_);
      ASSERT_EQ(evict_micro_infos.at(i).is_in_ghost_, micro_meta_info.is_in_ghost_);
    }
  }
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_persist_micro_meta_task.log*");
  OB_LOGGER.set_file_name("test_ss_persist_micro_meta_task.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
