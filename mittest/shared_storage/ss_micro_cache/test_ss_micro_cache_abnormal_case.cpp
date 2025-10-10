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
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_writer.h"


namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheAbnormalCase : public ::testing::Test
{
public:
  TestSSMicroCacheAbnormalCase() = default;
  virtual ~TestSSMicroCacheAbnormalCase() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSMicroCacheAbnormalCase::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheAbnormalCase::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheAbnormalCase::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), 1717986918));
  micro_cache->start();
}

void TestSSMicroCacheAbnormalCase::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

/* Test some micro blocks fail to update meta when sealed_mem_block updates micro_block meta.*/
TEST_F(TestSSMicroCacheAbnormalCase, test_mem_blk_update_meta_fail)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_mem_blk_update_meta_fail");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache->mem_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSPersistMicroDataTask &persist_data_task = micro_cache->task_runner_.persist_data_task_;
  persist_data_task.cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(1000 * 1000);

  const int64_t blk_idx = 2;
  ObSSPhyBlockHandle phy_blk_handle;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_block_handle(blk_idx, phy_blk_handle));
  ASSERT_EQ(true, phy_blk_handle.is_valid());

  ObArenaAllocator allocator;
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t block_size = phy_blk_mgr.get_block_size();
  const int64_t micro_cnt = 50;
  const int32_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
  char *data_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100);
  for (int64_t i = 0; i < micro_cnt; ++i) {
    const int32_t offset = payload_offset + i * micro_size;
    const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    if (OB_FAIL(micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
        macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
      LOG_WARN("fail to add micro_block", KR(ret), K(micro_key), KP(data_buf), K(micro_size));
    } else if (i < micro_cnt / 2) {
      ObSSMicroBlockMetaHandle micro_handle;
      if (OB_FAIL(micro_meta_mgr.get_micro_block_meta(micro_key, micro_handle, macro_id.second_id(), false))) {
        LOG_WARN("fail to get micro_handle", KR(ret), K(micro_key));
      } else if (OB_UNLIKELY(!micro_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_handle is invalid", KR(ret), K(micro_handle), KPC(micro_handle.get_ptr()));
      } else {
        micro_handle()->access_time_s_ = 0;
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ASSERT_EQ(micro_cnt, cache_stat.micro_stat().total_micro_cnt_);

  ASSERT_NE(nullptr, mem_blk_mgr.fg_mem_blk_);
  ASSERT_EQ(micro_cnt, mem_blk_mgr.fg_mem_blk_->micro_cnt_);
  ASSERT_EQ(micro_cnt, mem_blk_mgr.fg_mem_blk_->valid_cnt_);
  ASSERT_EQ(true, mem_blk_mgr.fg_mem_blk_->can_flush());
  ObSSMemBlockHandle tmp_mem_handle;
  tmp_mem_handle.set_ptr(mem_blk_mgr.fg_mem_blk_);

  const int64_t block_offset = block_size * blk_idx;
  int64_t updated_micro_size = 0;
  int64_t updated_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_mem_handle()->memmove_micro_index());
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.update_persisted_micro_block_meta(tmp_mem_handle, block_offset,
            phy_blk_handle()->reuse_version_, updated_micro_size, updated_micro_cnt));
  ASSERT_EQ((micro_cnt / 2) * micro_size, updated_micro_size);
  ASSERT_EQ(micro_cnt / 2, updated_micro_cnt);

  // free sealed_mem_block
  ASSERT_EQ(OB_SUCCESS, persist_data_task.persist_data_op_.handle_sealed_mem_block(updated_micro_cnt, true, tmp_mem_handle));
  ASSERT_EQ(micro_cnt / 2, cache_stat.micro_stat().mark_del_micro_cnt_); // micro_block which fail to update meta are deleted from map.
  mem_blk_mgr.fg_mem_blk_ = nullptr;
  LOG_INFO("TEST_CASE: finish test_mem_blk_update_meta_fail");
}

TEST_F(TestSSMicroCacheAbnormalCase, test_alloc_phy_block_abnormal)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_alloc_phy_block_abnormal");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  int64_t blk_idx = 2;
  ObSSPhysicalBlock *phy_blk = phy_blk_mgr.inner_get_phy_block_by_idx(blk_idx);
  ASSERT_NE(nullptr, phy_blk);

  // mock this free phy_block is abnormal, make its valid_len > 0
  phy_blk->valid_len_ = 100;
  int64_t tmp_blk_idx = -1;
  ObSSPhyBlockHandle phy_blk_handle;
  ASSERT_EQ(OB_INVALID_ARGUMENT, phy_blk_mgr.alloc_block(tmp_blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
  ASSERT_EQ(blk_idx, tmp_blk_idx);
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_reusable_block(tmp_blk_idx));
  ASSERT_EQ(1, phy_blk_mgr.get_reusable_blocks_cnt());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_reusable_block(tmp_blk_idx)); // add the same phy_blk_idx again
  ASSERT_EQ(1, phy_blk_mgr.get_reusable_blocks_cnt());
  phy_blk_handle.reset();

  tmp_blk_idx = -1;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(tmp_blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
  ASSERT_EQ(3, tmp_blk_idx);
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  phy_blk_handle.reset();

  phy_blk->valid_len_ = 0;
  phy_blk->blk_type_ = static_cast<uint64_t>(ObSSPhyBlockType::SS_MICRO_DATA_BLK);
  bool succ_free = false;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(blk_idx, succ_free));
  ASSERT_EQ(true, succ_free);
  ASSERT_EQ(0, phy_blk_mgr.get_reusable_blocks_cnt());

  tmp_blk_idx = -1;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(tmp_blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
  ASSERT_EQ(2, tmp_blk_idx);
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  phy_blk_handle.reset();
  LOG_INFO("TEST_CASE: finish test_alloc_phy_block_abnormal");
}

TEST_F(TestSSMicroCacheAbnormalCase, test_ckpt_write_abnormal)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_ckpt_write_abnormal");
  const uint64_t tenant_id = MTL_ID();
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache*)->phy_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache*)->cache_stat_;
  const int64_t phy_ckpt_blk_cnt = phy_blk_mgr.blk_cnt_info_.phy_ckpt_blk_cnt_;
  ASSERT_EQ(0, phy_blk_mgr.blk_cnt_info_.phy_ckpt_blk_used_cnt_);
  const int64_t block_size = phy_blk_mgr.get_block_size();

  const int64_t item_size = 4 * 1024;
  char buf[item_size];
  MEMSET(buf, 'a', item_size);

  // 1. mock already execute one round phy_blk checkpoint
  const int64_t cur_phy_ckpt_blk_cnt = 10;
  phy_blk_mgr.blk_cnt_info_.phy_ckpt_blk_cnt_ = cur_phy_ckpt_blk_cnt;
  phy_blk_mgr.blk_cnt_info_.phy_ckpt_blk_used_cnt_ = cur_phy_ckpt_blk_cnt / 2;

  // 2. mock write ckpt item abnormal
  ObSSCkptPhyBlockItemWriter item_writer;
  ASSERT_EQ(OB_SUCCESS, item_writer.init(tenant_id, phy_blk_mgr, ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK));
  const int64_t item_cnt = block_size * 2 / item_size;
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_WRITE_DISK_ERR, OB_TIMEOUT, 0, 1);
  for (int64_t i = 0; OB_SUCC(ret) && i < item_cnt; ++i) {
    if (OB_FAIL(item_writer.write_item(buf, item_size))) {
      LOG_WARN("fail to write item", KR(ret), K(i));
    }
  }
  ASSERT_EQ(OB_EAGAIN, ret); // fail to write block, will retry, until use up all blk_ckpt blocks
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_WRITE_DISK_ERR, OB_TIMEOUT, 0, 0);
  LOG_INFO("TEST_CASE: finish test_ckpt_write_abnormal");
}

TEST_F(TestSSMicroCacheAbnormalCase, test_phy_ckpt_timeout)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_phy_ckpt_timeout");
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  ASSERT_EQ(0, blk_cnt_info.phy_ckpt_blk_used_cnt_);
  const int64_t block_size = phy_blk_mgr.get_block_size();
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  const int64_t blk_ckpt_block_cnt = blk_cnt_info.phy_ckpt_blk_cnt_;
  blk_ckpt_task.cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(2 * 1000 * 1000);

  // 1. first execute phy_blk checkpoint task
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.start_op());
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.gen_checkpoint());
  ASSERT_EQ(blk_ckpt_block_cnt / 2, blk_cnt_info.phy_ckpt_blk_used_cnt_);

  // 2. second execute phy_blk checkpoint task
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_WRITE_DISK_ERR, OB_TIMEOUT, 0, 1);
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.start_op());
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_EAGAIN, blk_ckpt_task.ckpt_op_.gen_checkpoint());
  ASSERT_EQ(blk_ckpt_block_cnt, blk_cnt_info.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(blk_ckpt_block_cnt / 2, phy_blk_mgr.get_reusable_blocks_cnt());
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_WRITE_DISK_ERR, OB_TIMEOUT, 0, 0);

  ASSERT_EQ(false, blk_ckpt_task.ckpt_op_.has_free_ckpt_blk());
  bool exist_reusable_ckpt_blks = false;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.scan_reusable_ckpt_blocks_to_free(exist_reusable_ckpt_blks));
  ASSERT_EQ(false, exist_reusable_ckpt_blks);
  ASSERT_EQ(false, blk_ckpt_task.ckpt_op_.has_free_ckpt_blk());
  ASSERT_EQ(blk_ckpt_block_cnt / 2, phy_blk_mgr.get_reusable_blocks_cnt());

  // 3. third execute phy_blk checkpoint task, but no available block, cuz reusable_blocks can't be reused now.
  //    Thus, won't execute blk_ckpt.
  int64_t ori_blk_ckpt_cnt = cache_stat.task_stat().blk_ckpt_cnt_;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.start_op());
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.gen_checkpoint());
  ASSERT_EQ(ori_blk_ckpt_cnt, cache_stat.task_stat().blk_ckpt_cnt_);
  ASSERT_EQ(blk_ckpt_block_cnt, blk_cnt_info.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(blk_ckpt_block_cnt / 2, phy_blk_mgr.get_reusable_blocks_cnt());

  // 3.1 update reusable_blocks' alloc_time_s, make them can be reused now.
  ObHashSet<int64_t>::iterator iter = phy_blk_mgr.reusable_blks_.begin();
  for (; OB_SUCC(ret) && iter != phy_blk_mgr.reusable_blks_.end(); iter++) {
    const int64_t blk_idx = iter->first;
    ObSSPhysicalBlock *phy_blk = phy_blk_mgr.inner_get_phy_block_by_idx(blk_idx);
    ASSERT_NE(nullptr, phy_blk);
    if (is_ss_ckpt_block(phy_blk->get_block_type())) {
      phy_blk->alloc_time_s_ -= SS_FREE_BLK_MIN_REUSE_TIME_S;
    }
  }

  // 4. fourth execute phy_blk checkpoint task
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.start_op());
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.gen_checkpoint());
  ASSERT_EQ(blk_ckpt_block_cnt / 2, blk_cnt_info.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(true, phy_blk_mgr.inner_can_alloc_blk(ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK));
  ASSERT_EQ(true, phy_blk_mgr.inner_can_alloc_blk(ObSSPhyBlockType::SS_MICRO_DATA_BLK));

  LOG_INFO("TEST_CASE: finish test_phy_ckpt_timeout");
}

TEST_F(TestSSMicroCacheAbnormalCase, test_assign_super_blk_error)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_assign_super_blk_error");
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  ASSERT_EQ(0, blk_cnt_info.phy_ckpt_blk_used_cnt_);
  const int64_t block_size = phy_blk_mgr.get_block_size();
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  const int64_t blk_ckpt_block_cnt = blk_cnt_info.phy_ckpt_blk_cnt_;
  blk_ckpt_task.cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(2 * 1000 * 1000);

  // 1. first execute phy_blk checkpoint task
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.start_op());
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.gen_checkpoint());
  ASSERT_EQ(blk_ckpt_block_cnt / 2, blk_cnt_info.phy_ckpt_blk_used_cnt_);

  // 2. try to update super_block
  ObSSMicroCacheSuperBlk new_super_blk;
  ASSERT_EQ(OB_SUCCESS, new_super_blk.handle_init(tenant_id, micro_cache->micro_ckpt_split_cnt_));
  ASSERT_EQ(false, new_super_blk.is_valid());
  ASSERT_EQ(0, new_super_blk.blk_ckpt_used_blk_list().count());
  const int64_t start_us = ObTimeUtility::current_time_us();
  ASSERT_EQ(OB_SUCCESS, new_super_blk.assign(phy_blk_mgr.super_blk_));
  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  ASSERT_EQ(true, new_super_blk.is_valid());
  new_super_blk.micro_ckpt_time_us_ += 10013;
  const int64_t exp_micro_ckpt_time_us = new_super_blk.micro_ckpt_time_us_;
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ASSIGN_SUPER_BLK_ERR, OB_ALLOCATE_MEMORY_FAILED, 0, 1);
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_ss_super_block(new_super_blk));
  ASSERT_EQ(false, phy_blk_mgr.super_blk_.is_valid());
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ASSIGN_SUPER_BLK_ERR, OB_ALLOCATE_MEMORY_FAILED, 0, 0);

  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.inner_check_ss_super_block());
  ASSERT_EQ(true, phy_blk_mgr.super_blk_.is_valid());
  ASSERT_EQ(exp_micro_ckpt_time_us, phy_blk_mgr.super_blk_.micro_ckpt_time_us_);
}

TEST_F(TestSSMicroCacheAbnormalCase, test_restart_read_super_blk_error)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_restart_read_super_blk_error");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  const uint64_t tenant_id = MTL_ID();
  const int64_t write_blk_cnt = blk_cnt_info.data_blk_.free_blk_cnt() / 2;
  const int64_t micro_size = 15 * 1024;
  const int64_t per_micro_cnt = 128;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();

  // 1. write some micro_blocks
  for (int64_t i = 0; i < write_blk_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    for (int32_t j = 0; j < per_micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                                         macro_id.second_id()/*effective_tablet_id*/,
                                         ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }

  // 2. execute blk_ckpt and micro_ckpt
  ObSSDoBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.start_op());
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_task.ckpt_op_.gen_checkpoint());

  ObSSPersistMicroMetaTask &persist_meta_task = micro_cache->task_runner_.persist_meta_task_;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task.persist_meta_op_.start_op());
  persist_meta_task.persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task.persist_meta_op_.gen_checkpoint());

  ASSERT_LT(0, blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.ckpt_item_cnt_);
  ASSERT_LT(0, phy_blk_mgr.super_blk_.blk_ckpt_info_.blk_ckpt_used_blks_.count());
  ASSERT_LT(0, phy_blk_mgr.super_blk_.micro_ckpt_info_.micro_ckpt_entries_.count());
  ASSERT_NE(0, phy_blk_mgr.super_blk_.micro_ckpt_time_us_);

  // 3. restart micro_cache, mock read super_block failed.
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  MTL(ObTenantFileManager*)->is_cache_file_exist_ = true;
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), 1717986918));
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ASSIGN_SUPER_BLK_ERR, OB_ALLOCATE_MEMORY_FAILED, 0, 1);
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ASSIGN_SUPER_BLK_ERR, OB_ALLOCATE_MEMORY_FAILED, 0, 0);

  ObSSMicroCacheSuperBlk new_super_blk;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.read_ss_super_block(new_super_blk));
  ASSERT_EQ(true, phy_blk_mgr.super_blk_.is_valid());
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.blk_ckpt_info_.blk_ckpt_used_blks_.count());
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(0, phy_blk_mgr.super_blk_.micro_ckpt_time_us_);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_abnormal_case.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_abnormal_case.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}