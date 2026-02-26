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

public:
  const int64_t DIFF_TIME_US = 200 * 1000;
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
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tnt_file_mgr);
  tnt_file_mgr->persist_disk_space_task_.enable_adjust_size_ = false;
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 30), 1/*micro_split_cnt*/));
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

TEST_F(TestSSMicroCacheRestart, test_restart_without_file)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager *);
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  const int64_t cache_file_size = micro_cache->cache_file_size_;
  const int64_t block_size = micro_cache->phy_blk_size_;
  LOG_INFO("TEST_CASE: start test_restart_without_file");

  char micro_cache_file_path[512] = {0};
  ret = tnt_file_mgr->get_micro_cache_file_path(micro_cache_file_path, sizeof(micro_cache_file_path), tenant_id, MTL_EPOCH_ID());
  ASSERT_EQ(OB_SUCCESS, ret);

  tnt_file_mgr->stop();
  micro_cache->stop();
  tnt_file_mgr->wait();
  micro_cache->wait();
  tnt_file_mgr->destroy();
  micro_cache->destroy();
  LOG_INFO("TEST: start 1st restart");

  ret = ObIODeviceLocalFileOp::unlink(micro_cache_file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS, tnt_file_mgr->init(tenant_id));
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(tenant_id, cache_file_size, 1/*micro_split_cnt*/));
  ASSERT_EQ(OB_SUCCESS, tnt_file_mgr->start());
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
  LOG_INFO("TEST: finish 1st restart without micro_cache_file");

  int64_t REPLAY_CKPT_TIMEOUT_S = 120;
  int64_t start_time_s = ObTimeUtility::current_time_s();
  bool is_cache_enabled = false;
  do {
    is_cache_enabled = micro_cache->is_enabled_;
    if (!is_cache_enabled) {
      LOG_INFO("ss_micro_cache is still disabled");
      ob_usleep(1000 * 1000);
    }
  } while (!is_cache_enabled && ObTimeUtility::current_time_s() - start_time_s < REPLAY_CKPT_TIMEOUT_S);
  ASSERT_EQ(true, is_cache_enabled);

  bool is_file_exist = false;
  ret = ObIODeviceLocalFileOp::exist(micro_cache_file_path, is_file_exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_file_exist);

  const int64_t micro_cnt = 1000;
  const int64_t micro_size = 8 * 1024;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();
  const int32_t offset = payload_offset;
  for (int64_t i = 0; i < micro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    ret = micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, macro_id.second_id(), ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

  ObSSPersistMicroMetaTask &micro_ckpt_task = micro_cache->task_runner_.persist_meta_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  micro_ckpt_task.persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
  usleep(10 * 1000 * 1000);
  ASSERT_EQ(1, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_LT(0, cache_stat.task_stat().micro_ckpt_item_cnt_);
  ASSERT_EQ(1, cache_stat.task_stat().blk_ckpt_cnt_);
  ASSERT_LT(0, cache_stat.task_stat().blk_ckpt_item_cnt_);

  tnt_file_mgr->stop();
  micro_cache->stop();
  tnt_file_mgr->wait();
  micro_cache->wait();
  tnt_file_mgr->destroy();
  micro_cache->destroy();
  LOG_INFO("TEST: start 2nd restart");

  ret = ObIODeviceLocalFileOp::unlink(micro_cache_file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS, tnt_file_mgr->init(tenant_id));
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(tenant_id, cache_file_size, 1/*micro_split_cnt*/));
  ASSERT_EQ(OB_SUCCESS, tnt_file_mgr->start());
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
  LOG_INFO("TEST: finish 2nd restart without micro_cache_file");

  start_time_s = ObTimeUtility::current_time_s();
  is_cache_enabled = false;
  do {
    is_cache_enabled = micro_cache->is_enabled_;
    if (!is_cache_enabled) {
      LOG_INFO("ss_micro_cache is still disabled");
      ob_usleep(1000 * 1000);
    }
  } while (!is_cache_enabled && ObTimeUtility::current_time_s() - start_time_s < REPLAY_CKPT_TIMEOUT_S);
  ASSERT_EQ(true, is_cache_enabled);

  is_file_exist = false;
  ret = ObIODeviceLocalFileOp::exist(micro_cache_file_path, is_file_exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_file_exist);
}

TEST_F(TestSSMicroCacheRestart, test_restart_micro_cache)
{
  int ret = OB_SUCCESS;
  const int64_t TEST_START_TIME = ObTimeUtility::current_time_s();
  LOG_INFO("TEST_CASE: start test_restart_micro_cache");
  ObArenaAllocator allocator;
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  const int64_t cache_file_size = micro_cache->cache_file_size_;
  ASSERT_EQ((1L << 30), cache_file_size);

  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ASSERT_EQ(0, cache_stat.micro_stat().total_micro_cnt_);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroRangeManager &micro_range_mgr = micro_cache->micro_range_mgr_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;

  const int64_t data_blk_max_cnt = phy_blk_mgr.blk_cnt_info_.micro_data_blk_max_cnt();
  const int32_t block_size = phy_blk_mgr.block_size_;
  const int64_t macro_blk_cnt = MIN(10, data_blk_max_cnt);
  const int64_t min_micro_size = 8 * 1024;
  const int64_t max_micro_size = 16 * 1024;
  char *read_buf = static_cast<char *>(allocator.alloc(max_micro_size));
  ASSERT_NE(nullptr, read_buf);

  ObSSPersistMicroMetaTask &micro_ckpt_task = micro_cache->task_runner_.persist_meta_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  ObCompressorType compress_type = TestSSCommonUtil::get_random_compress_type();
  micro_cache->set_micro_meta_compressor_type(compress_type);
  ASSERT_EQ(compress_type, micro_cache->admin_info_.get_micro_meta_compressor_type());
  ASSERT_EQ(compress_type, phy_blk_mgr.super_blk_.admin_info_.get_micro_meta_compressor_type());
  LOG_INFO("TEST: ready to start case", K(compress_type));

  int64_t total_micro_cnt = 0;
  int64_t total_ckpt_micro_cnt = 0;

  // 1. FIRST START
  // 1.1 trigger micro_meta and phy_info checkpoint(but there not exist any micro_meta), wait for it finish
  micro_ckpt_task.persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
  usleep(5 * 1000 * 1000);
  ASSERT_EQ(1, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_EQ(1, cache_stat.task_stat().blk_ckpt_cnt_);
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_LT(0, phy_blk_mgr.super_blk_.blk_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_LE(SS_SUPER_BLK_COUNT, phy_blk_mgr.super_blk_.blk_ckpt_info_.blk_ckpt_entry_);
  ASSERT_EQ(0, cache_stat.phy_blk_stat().meta_blk_used_cnt_);
  ASSERT_EQ(phy_blk_mgr.super_blk_.blk_ckpt_info_.get_total_used_blk_cnt(), cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_);

  // 1.2 destroy micro_cache
  LOG_INFO("TEST: start 1st restart", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();

  // 1.3 revise is_cache_file_exist
  MTL(ObTenantFileManager*)->is_cache_file_exist_ = true;

  // 2. FIRST RESTART
  // 2.1 restart
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), cache_file_size, 1/*micro_split_cnt*/));
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
  LOG_INFO("TEST: finish 1st restart", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME,
    K(cache_stat.micro_stat()), K(cache_stat.task_stat()));

  ASSERT_EQ(cache_file_size, phy_blk_mgr.super_blk_.cache_file_size_);
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.blk_ckpt_info_.get_total_used_blk_cnt()); // cuz only exist blk ckpt, will drop it
  ASSERT_EQ(compress_type, micro_cache->admin_info_.get_micro_meta_compressor_type());
  ASSERT_EQ(compress_type, phy_blk_mgr.super_blk_.admin_info_.get_micro_meta_compressor_type());

  int64_t REPLAY_CKPT_TIMEOUT_S = 120;
  int64_t start_time_s = ObTimeUtility::current_time_s();
  bool is_cache_enabled = false;
  do {
    is_cache_enabled = micro_cache->is_enabled_;
    if (!is_cache_enabled) {
      LOG_INFO("ss_micro_cache is still disabled");
      ob_usleep(1000 * 1000);
    }
  } while (!is_cache_enabled && ObTimeUtility::current_time_s() - start_time_s < REPLAY_CKPT_TIMEOUT_S);
  ASSERT_EQ(true, is_cache_enabled);
  LOG_INFO("TEST: finish 1st replay", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME);

  // 2.2 write data into object_storage
  ObArray<TestSSCommonUtil::MicroBlockInfo> micro_info_arr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::prepare_micro_blocks(macro_blk_cnt, block_size, micro_info_arr, 1, true,
    min_micro_size, max_micro_size));
  int64_t micro_cnt = micro_info_arr.count();
  ASSERT_LT(0, micro_cnt);
  LOG_INFO("TEST: finish 1st prepare micro_blks", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME);

  // 2.3 get these micro_blocks and add them into micro_cache
  total_micro_cnt += micro_cnt;
  for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt); ++i) {
    TestSSCommonUtil::MicroBlockInfo &cur_info = micro_info_arr.at(i);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::get_micro_block(cur_info, read_buf));
  }
  ASSERT_EQ(micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  LOG_INFO("TEST: finish 1st get micro_blks", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME);

  // 2.4 wait some time for persist_task
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  int64_t cur_data_blk_used_cnt = cache_stat.phy_blk_stat().data_blk_used_cnt_;

  int64_t persisted_micro_cnt = 0;
  {
    for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt); ++i) {
      TestSSCommonUtil::MicroBlockInfo &cur_info = micro_info_arr.at(i);
      ObSSMicroBlockCacheKey cur_micro_key = TestSSCommonUtil::gen_phy_micro_key(cur_info.macro_id_, cur_info.offset_,
                                                                                 cur_info.size_);
      ObSSMicroBlockMetaHandle cur_micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&cur_micro_key, cur_micro_meta_handle));
      ObSSMicroBlockMeta *cur_micro_meta = cur_micro_meta_handle.get_ptr();
      if (nullptr != cur_micro_meta && cur_micro_meta->is_data_persisted()) {
        ++persisted_micro_cnt;
      }
    }
  }

  // 2.5 trigger micro_meta and phy_info checkpoint, wait for it finish
  int64_t ori_micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
  int64_t ori_blk_ckpt_cnt = cache_stat.task_stat().blk_ckpt_cnt_;
  micro_ckpt_task.persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
  int64_t EXE_CKPT_TIMEOUT_S = 120;
  start_time_s = ObTimeUtility::current_time_s();
  bool finish_ckpt = false;
  do {
    bool finish_micro_ckpt = (cache_stat.task_stat().micro_ckpt_cnt_ > ori_micro_ckpt_cnt);
    bool finish_blk_ckpt = (cache_stat.task_stat().blk_ckpt_cnt_ > ori_blk_ckpt_cnt);
    if (!finish_micro_ckpt || !finish_blk_ckpt) {
      LOG_INFO("ss_micro_cache checkpoint is still executing");
      ob_usleep(1000 * 1000);
    } else {
      finish_ckpt = true;
    }
  } while (!finish_ckpt && ObTimeUtility::current_time_s() - start_time_s < EXE_CKPT_TIMEOUT_S);
  ASSERT_EQ(true, finish_ckpt);
  int64_t micro_ckpt_entry_cnt = phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt();
  ASSERT_LT(0, micro_ckpt_entry_cnt);
  ASSERT_LE(SS_SUPER_BLK_COUNT, phy_blk_mgr.super_blk_.micro_ckpt_info_.micro_ckpt_entries_.at(0).entry_blk_idx_);
  int64_t blk_ckpt_entry_cnt = phy_blk_mgr.super_blk_.blk_ckpt_info_.get_total_used_blk_cnt();
  ASSERT_LT(0, blk_ckpt_entry_cnt);
  ASSERT_LE(SS_SUPER_BLK_COUNT, phy_blk_mgr.super_blk_.blk_ckpt_info_.blk_ckpt_entry_);
  ASSERT_EQ(micro_ckpt_entry_cnt, cache_stat.phy_blk_stat().meta_blk_used_cnt_);
  ASSERT_EQ(blk_ckpt_entry_cnt, cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_);

  total_ckpt_micro_cnt = cache_stat.task_stat().micro_ckpt_item_cnt_;
  ASSERT_EQ(persisted_micro_cnt, total_ckpt_micro_cnt);
  ASSERT_EQ(micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_LE(micro_cnt * min_micro_size, cache_stat.micro_stat().total_micro_size_);
  ASSERT_GE(micro_cnt * max_micro_size, cache_stat.micro_stat().total_micro_size_);

  // 2.6 destroy micro_cache
  LOG_INFO("TEST: start 2nd restart", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME,
    K(cache_stat.micro_stat()), K(cache_stat.task_stat()));
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();

  // 3. SECOND RESTART
  // 3.1 restart
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), cache_file_size, 1/*micro_split_cnt*/));
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
  LOG_INFO("TEST: finish 2nd restart", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME);
  ASSERT_EQ(false, micro_cache->is_enabled_);

  REPLAY_CKPT_TIMEOUT_S = 120;
  start_time_s = ObTimeUtility::current_time_s();
  is_cache_enabled = false;
  do {
    is_cache_enabled = micro_cache->is_enabled_;
    if (!is_cache_enabled) {
      LOG_INFO("ss_micro_cache is still disabled");
      ob_usleep(1000 * 1000);
    }
  } while (!is_cache_enabled && ObTimeUtility::current_time_s() - start_time_s < REPLAY_CKPT_TIMEOUT_S);
  ASSERT_EQ(true, is_cache_enabled);
  LOG_INFO("TEST: finish 2nd replay", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME);

  ASSERT_EQ(total_ckpt_micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_LE(total_ckpt_micro_cnt * min_micro_size, cache_stat.micro_stat().total_micro_size_);
  ASSERT_GE(total_ckpt_micro_cnt * max_micro_size, cache_stat.micro_stat().total_micro_size_);
  ASSERT_EQ(cur_data_blk_used_cnt, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  ASSERT_EQ(true, phy_blk_mgr.super_blk_.exist_valid_checkpoint());
  micro_cache->admin_info_.set_micro_meta_compressor_type(compress_type);

  // 3.2 write new data into object_storage
  micro_info_arr.reset();
  ASSERT_EQ(0, micro_info_arr.count());
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::prepare_micro_blocks(macro_blk_cnt, block_size, micro_info_arr, macro_blk_cnt + 1,
    true, min_micro_size, max_micro_size));
  ASSERT_LT(0, micro_info_arr.count());
  LOG_INFO("TEST: finish 2nd prepare micro_blks", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME);

  // 3.3 get these micro_blocks and add them into micro_cache
  micro_cnt = micro_info_arr.count();
  total_micro_cnt += micro_cnt;
  for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt); ++i) {
    TestSSCommonUtil::MicroBlockInfo &cur_info = micro_info_arr.at(i);
    char *read_buf = static_cast<char*>(allocator.alloc(cur_info.size_));
    ASSERT_NE(nullptr, read_buf);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::get_micro_block(cur_info, read_buf));
  }
  LOG_INFO("TEST: finish 2nd get micro_blks", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME);
  ASSERT_EQ(total_ckpt_micro_cnt + micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_LE((total_ckpt_micro_cnt + micro_cnt) * min_micro_size, cache_stat.micro_stat().total_micro_size_);
  ASSERT_GE((total_ckpt_micro_cnt + micro_cnt) * max_micro_size, cache_stat.micro_stat().total_micro_size_);

  // 3.4 wait some time for persist_task
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  cur_data_blk_used_cnt = cache_stat.phy_blk_stat().data_blk_used_cnt_;

  // 3.5 statistic persisted micro_meta count, meanwhile mark some micro_metas as expired
  int64_t tmp_total_micro_cnt = 0;
  int64_t tmp_persist_micro_cnt = 0;
  int64_t tmp_mark_expired_cnt = 0;
  const int64_t init_rng_cnt = micro_range_mgr.init_range_cnt_;
  for (int64_t i = 0; i < init_rng_cnt; ++i) {
    ObSSMicroInitRangeInfo *init_range = micro_range_mgr.init_range_arr_[i];
    ASSERT_NE(nullptr, init_range);
    ObSSMicroSubRangeInfo *cur_sub_range = init_range->next_;
    while (nullptr != cur_sub_range) {
      ASSERT_LE(cur_sub_range->end_hval_, init_range->end_hval_);
      ObSSMicroBlockMeta *cur_micro_meta = cur_sub_range->first_micro_;
      while (nullptr != cur_micro_meta) {
        const uint32_t micro_hval = cur_micro_meta->get_micro_key().micro_hash();
        ASSERT_LE(micro_hval, cur_sub_range->end_hval_);
        ++tmp_total_micro_cnt;
        if (i < init_rng_cnt / 2) {
          cur_micro_meta->mark_meta_expired();
          ++tmp_mark_expired_cnt;
        } else {
          if (cur_micro_meta->is_data_persisted() && cur_micro_meta->is_valid_field()) {
            ++tmp_persist_micro_cnt;
          }
        }
        cur_micro_meta = cur_micro_meta->next_;
      }
      cur_sub_range = cur_sub_range->next_;
    }
  }

  // 3.6 execute checkpoint
  ori_micro_ckpt_cnt = cache_stat.task_stat().micro_ckpt_cnt_;
  ori_blk_ckpt_cnt = cache_stat.task_stat().blk_ckpt_cnt_;
  micro_ckpt_task.persist_meta_op_.micro_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_micro_ckpt_time_us();
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
  start_time_s = ObTimeUtility::current_time_s();
  finish_ckpt = false;
  do {
    bool finish_micro_ckpt = (cache_stat.task_stat().micro_ckpt_cnt_ > ori_micro_ckpt_cnt);
    bool finish_blk_ckpt = (cache_stat.task_stat().blk_ckpt_cnt_ > ori_blk_ckpt_cnt);
    if (!finish_micro_ckpt || !finish_blk_ckpt) {
      LOG_INFO("ss_micro_cache checkpoint is still executing");
      ob_usleep(500 * 1000);
    } else {
      finish_ckpt = true;
    }
  } while (!finish_ckpt && ObTimeUtility::current_time_s() - start_time_s < EXE_CKPT_TIMEOUT_S);
  ASSERT_EQ(true, finish_ckpt);

  LOG_INFO("finish second round ckpt", K(tmp_total_micro_cnt), K(tmp_mark_expired_cnt), "ckpt_cost",
    ObTimeUtility::current_time_s() - start_time_s, K(cache_stat));
  ASSERT_EQ(total_ckpt_micro_cnt + micro_cnt - tmp_mark_expired_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(tmp_persist_micro_cnt, cache_stat.task_stat().micro_ckpt_item_cnt_);

  int64_t total_sub_rng_cnt = 0;
  for (int64_t i = 0; i < init_rng_cnt; ++i) {
    ObSSMicroInitRangeInfo *init_range = micro_range_mgr.init_range_arr_[i];
    ASSERT_NE(nullptr, init_range);
    total_sub_rng_cnt += init_range->get_sub_range_cnt();
  }
  ASSERT_EQ(total_sub_rng_cnt, cache_stat.range_stat().sub_range_cnt_);

  LOG_INFO("TEST: start 3rd restart", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME,
    K(cache_stat.micro_stat()), K(cache_stat.task_stat()));
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();

  // 4. THIRD RESTART
  // 4.1 restart, replay checkpoint will return error, thus it will clear micro_cache
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), cache_file_size, 1/*micro_split_cnt*/));
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_REPLAY_CKPT_ERR, OB_TIMEOUT, 0, 1);
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
  LOG_INFO("TEST: finish 3rd restart", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME);
  ASSERT_EQ(false, micro_cache->is_enabled_);

  REPLAY_CKPT_TIMEOUT_S = 120;
  start_time_s = ObTimeUtility::current_time_s();
  is_cache_enabled = false;
  do {
    is_cache_enabled = micro_cache->is_enabled_;
    if (!is_cache_enabled) {
      LOG_INFO("ss_micro_cache is still disabled");
      ob_usleep(1000 * 1000);
    }
  } while (!is_cache_enabled && ObTimeUtility::current_time_s() - start_time_s < REPLAY_CKPT_TIMEOUT_S);
  ASSERT_EQ(true, is_cache_enabled);
  LOG_INFO("TEST: finish 3rd replay", "cost_s", ObTimeUtility::current_time_s() - TEST_START_TIME);
  ASSERT_EQ(0, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(0, cache_stat.micro_stat().total_micro_size_);
  ASSERT_EQ(0, cache_stat.mem_stat().micro_alloc_cnt_);
  ASSERT_EQ(0, cache_stat.range_stat().sub_range_cnt_);
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_REPLAY_CKPT_ERR, OB_TIMEOUT, 0, 0);

  allocator.clear();
  LOG_INFO("TEST_CASE: finish test_restart_micro_cache");
}

TEST_F(TestSSMicroCacheRestart, test_restart_with_small_file)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_restart_with_small_file");
  ObArenaAllocator allocator;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tnt_disk_space_mgr);
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  // increase server total disk size, ensure enough space for start micro cache
  {
    const int64_t ori_total_disk_size = tnt_disk_space_mgr->total_disk_size_;
    const int64_t new_total_size = (1L << 32); // at least 4GB
    const int64_t delta_size = new_total_size - ori_total_disk_size;
    const int64_t server_free_disk_size = OB_SERVER_DISK_SPACE_MGR.get_free_disk_size();
    if (delta_size > server_free_disk_size) {
      const int64_t server_total_disk_size = OB_SERVER_DISK_SPACE_MGR.get_total_disk_size();
      const int64_t new_server_total_disk_size = server_total_disk_size + (delta_size - server_free_disk_size) + 1024 * 1024 * 1024L;
      ASSERT_EQ(OB_SUCCESS, OB_SERVER_DISK_SPACE_MGR.resize(new_server_total_disk_size));
      LOG_INFO("TEST_CASE: finish increase server total disk size", K(new_server_total_disk_size), K(tnt_disk_space_mgr->total_disk_size_));
    }
  }

  const int64_t ori_micro_cache_size = tnt_disk_space_mgr->total_disk_size_ * 0.2;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::restart_micro_cache(micro_cache, tenant_id, ori_micro_cache_size, 1));
  ASSERT_EQ(ori_micro_cache_size, micro_cache->cache_file_size_);
  LOG_INFO("TEST_CASE: finish restart micro cache with 20% of disk size",
    K(micro_cache->cache_file_size_));

  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheMicroStat &micro_stat = cache_stat.micro_stat_;
  ASSERT_EQ(0, micro_stat.total_micro_cnt_);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroRangeManager &micro_range_mgr = micro_cache->micro_range_mgr_;
  ObSSMicroCacheTaskStat &task_stat = cache_stat.task_stat_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  ObSSPersistMicroMetaTask &micro_ckpt_task = micro_cache->task_runner_.persist_meta_task_;
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;

  const int64_t micro_size = 12 * 1024;
  const int64_t micro_cnt = 1000;
  const int64_t TIMEOUT_S = 30;
  {
    // 1.1 add some data into micro_cache
    const uint64_t tablet_id = 100;
    int64_t add_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_add_micro_block(tablet_id, micro_cnt, micro_size, micro_size, add_cnt));
    ASSERT_GT(add_cnt, 0);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

    // 1.2 get micro meta
    int64_t get_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_get_micro_block_meta(tablet_id, micro_cnt, micro_size, micro_size, get_cnt));
    ASSERT_GT(get_cnt, 0);
    ASSERT_GT(micro_stat.total_micro_cnt_, 0);
    ASSERT_GT(micro_stat.valid_micro_cnt_, 0);

    // 1.3 wait for micro_ckpt and blk_ckpt to finish
    int64_t ori_blk_ckpt_cnt = task_stat.blk_ckpt_cnt_;
    int64_t ori_micro_ckpt_cnt = task_stat.micro_ckpt_cnt_;
    ASSERT_EQ(0, ori_micro_ckpt_cnt);
    blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
    bool finish_ckpt = false;
    int64_t cur_time_s = ObTimeUtility::current_time_s();
    do {
      if (task_stat.blk_ckpt_cnt_ > ori_blk_ckpt_cnt &&
          (task_stat.micro_ckpt_cnt_ > ori_micro_ckpt_cnt) &&
          (micro_ckpt_task.persist_meta_op_.micro_ckpt_ctx_.complete_full_ckpt_)) {
        finish_ckpt = true;
      } else if (task_stat.micro_ckpt_cnt_ == ori_micro_ckpt_cnt) {
        ASSERT_EQ(OB_SUCCESS, micro_ckpt_task.persist_meta_op_.start_op());
        micro_ckpt_task.persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
        ASSERT_EQ(OB_SUCCESS, micro_ckpt_task.persist_meta_op_.gen_checkpoint());
        ob_usleep(10 * 1000);
      } else {
        LOG_INFO("still waiting for blk checkpoint", K(ori_blk_ckpt_cnt), K(ori_micro_ckpt_cnt), K(task_stat));
        ob_usleep(1000 * 1000);
      }
    } while (!finish_ckpt && ObTimeUtility::current_time_s() - cur_time_s <= TIMEOUT_S);
    ASSERT_EQ(true, finish_ckpt);
    ASSERT_GT(micro_stat.total_micro_cnt_, 0);
    ASSERT_GT(micro_stat.valid_micro_cnt_, 0);

    // 1.4 clear micro_cache
    ASSERT_EQ(OB_SUCCESS, micro_cache->clear_micro_cache());
    ASSERT_EQ(0, micro_stat.total_micro_cnt_);
    ASSERT_EQ(0, micro_stat.valid_micro_cnt_);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_T1].cnt_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_T1].size_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_T2].cnt_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_T2].size_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_B1].cnt_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_B2].cnt_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_B2].size_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_B1].size_, 0);
  }

  // restart micro cache with 5% of tenant's total disk size
  const int64_t new_macro_cache_size = tnt_disk_space_mgr->total_disk_size_ * 0.05;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::restart_micro_cache(micro_cache, tenant_id, new_macro_cache_size, 1));
  ASSERT_EQ(new_macro_cache_size, micro_cache->cache_file_size_);
  LOG_INFO("TEST_CASE: finish restart micro cache with 5% of disk size",
    K(micro_cache->cache_file_size_));
  {
    // 2.1 add some new data into micro_cache
    const uint64_t tablet_id = 200;
    int64_t add_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_add_micro_block(tablet_id, micro_cnt, micro_size, micro_size, add_cnt));
    ASSERT_GT(add_cnt, 0);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

    // 2.2 get micro meta
    int64_t get_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_get_micro_block_meta(tablet_id, micro_cnt, micro_size, micro_size, get_cnt));
    ASSERT_GT(get_cnt, 0);
    ASSERT_GT(micro_stat.total_micro_cnt_, 0);
    ASSERT_GT(micro_stat.valid_micro_cnt_, 0);

    // 2.3 wait for micro_ckpt and blk_ckpt to finish
    int64_t ori_blk_ckpt_cnt = task_stat.blk_ckpt_cnt_;
    int64_t ori_micro_ckpt_cnt = task_stat.micro_ckpt_cnt_;
    ASSERT_EQ(0, ori_micro_ckpt_cnt);
    blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
    bool finish_ckpt = false;
    int64_t cur_time_s = ObTimeUtility::current_time_s();
    do {
      if (task_stat.blk_ckpt_cnt_ > ori_blk_ckpt_cnt &&
          (task_stat.micro_ckpt_cnt_ > ori_micro_ckpt_cnt) &&
          (micro_ckpt_task.persist_meta_op_.micro_ckpt_ctx_.complete_full_ckpt_)) {
        finish_ckpt = true;
      } else if (task_stat.micro_ckpt_cnt_ == ori_micro_ckpt_cnt) {
        ASSERT_EQ(OB_SUCCESS, micro_ckpt_task.persist_meta_op_.start_op());
        micro_ckpt_task.persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
        ASSERT_EQ(OB_SUCCESS, micro_ckpt_task.persist_meta_op_.gen_checkpoint());
        ob_usleep(10 * 1000);
      } else {
        LOG_INFO("still waiting for blk checkpoint", K(ori_blk_ckpt_cnt), K(ori_micro_ckpt_cnt), K(task_stat));
        ob_usleep(1000 * 1000);
      }
    } while (!finish_ckpt && ObTimeUtility::current_time_s() - cur_time_s <= TIMEOUT_S);
    ASSERT_EQ(true, finish_ckpt);
    ASSERT_GT(micro_stat.total_micro_cnt_, 0);
    ASSERT_GT(micro_stat.valid_micro_cnt_, 0);

    // 2.4 clear micro_cache
    ASSERT_EQ(OB_SUCCESS, micro_cache->clear_micro_cache());
    ASSERT_EQ(0, micro_stat.total_micro_cnt_);
    ASSERT_EQ(0, micro_stat.valid_micro_cnt_);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_T1].cnt_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_T1].size_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_T2].cnt_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_T2].size_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_B1].cnt_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_B2].cnt_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_B2].size_, 0);
    ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_B1].size_, 0);
  }

  LOG_INFO("TEST_CASE: finish test_restart_with_small_file");
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_restart.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_restart.log", true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
