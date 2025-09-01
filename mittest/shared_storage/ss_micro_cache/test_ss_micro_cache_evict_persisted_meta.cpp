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

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
class TestSSMicroCacheEvictPersistedMeta : public ::testing::Test
{
public:
  TestSSMicroCacheEvictPersistedMeta()
    : allocator_(), micro_cache_(nullptr), mem_blk_mgr_(nullptr), phy_blk_mgr_(nullptr), micro_meta_mgr_(nullptr),
      release_cache_task_(nullptr), persist_meta_task_(nullptr), blk_ckpt_task_(nullptr), persist_data_task_(nullptr) {}
  virtual ~TestSSMicroCacheEvictPersistedMeta() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
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

void TestSSMicroCacheEvictPersistedMeta::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheEvictPersistedMeta::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheEvictPersistedMeta::SetUp()
{
  int ret = OB_SUCCESS;
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
  micro_meta_mgr_->enable_save_meta_mem_ = true; // enable save meta memory
  release_cache_task_ = &micro_cache_->task_runner_.release_cache_task_;
  ASSERT_NE(nullptr, release_cache_task_);
  persist_meta_task_ = &micro_cache_->task_runner_.persist_meta_task_;
  ASSERT_NE(nullptr, persist_meta_task_);
  blk_ckpt_task_ = &micro_cache_->task_runner_.blk_ckpt_task_;
  ASSERT_NE(nullptr, blk_ckpt_task_);
  persist_data_task_ =  &micro_cache_->task_runner_.persist_data_task_;
  ASSERT_NE(nullptr, persist_data_task_);
}

void TestSSMicroCacheEvictPersistedMeta::TearDown()
{
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

TEST_F(TestSSMicroCacheEvictPersistedMeta, test_evict_persisted_meta)
{
  LOG_INFO("TEST: start test_evict_persisted_meta");
  int ret = OB_SUCCESS;
  ASSERT_NE(nullptr, micro_cache_);
  const int64_t block_size = micro_cache_->phy_blk_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache_->cache_stat_;
  ObSSARCInfo &arc_info = micro_cache_->micro_meta_mgr_.arc_info_;
  ObSSPhyBlockCountInfo &blk_cnt_info = micro_cache_->phy_blk_mgr_.blk_cnt_info_;

  persist_meta_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  blk_ckpt_task_->cur_interval_us_ = 3600 * 1000 * 1000L;
  release_cache_task_->ori_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(2 * 1000 * 1000);

  const int64_t write_macro_cnt = arc_info.work_limit_ / block_size;
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 64;
  const int32_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  ObArray<ObSSMicroBlockCacheKey> micro_keys;
  ASSERT_EQ(OB_SUCCESS, micro_keys.reserve(write_macro_cnt * micro_cnt));
  // 1. write batch micro_blocks
  for (int64_t i = 0; i < write_macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_keys.push_back(micro_key));
      ASSERT_EQ(OB_SUCCESS, micro_cache_->add_micro_block_cache(micro_key, data_buf, micro_size,
                            macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
    ASSERT_EQ(i, blk_cnt_info.data_blk_.used_cnt_);
  }
  ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_T1].cnt_, write_macro_cnt * micro_cnt);

  ObSSMicroBlockCacheKey micro_key_last;
  {
    // to sealed the last mem_block
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(write_macro_cnt + 1);
    const int32_t offset = payload_offset;
    micro_key_last = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    ASSERT_EQ(OB_SUCCESS, micro_cache_->add_micro_block_cache(micro_key_last, data_buf, micro_size,
      macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_T1].cnt_, write_macro_cnt * micro_cnt + 1);
  ASSERT_EQ(write_macro_cnt, blk_cnt_info.data_blk_.used_cnt_);

  // 2. evict micro_metas
  for (int64_t i = 0; i < micro_keys.count() - 1; ++i) {
    ObSSMicroBlockCacheKey &micro_key = micro_keys.at(i);
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ASSERT_EQ(OB_SUCCESS, micro_cache_->micro_meta_mgr_.get_micro_block_meta(micro_key, micro_meta_handle,
      ObTabletID::INVALID_TABLET_ID, false));
    ASSERT_EQ(true, micro_meta_handle.is_valid());
    ObSSMicroBlockMetaInfo micro_meta_info;
    micro_meta_handle()->get_micro_meta_info(micro_meta_info);
    ASSERT_EQ(OB_SUCCESS, micro_cache_->micro_meta_mgr_.try_evict_micro_block_meta(micro_meta_info));

    const int64_t delta_size = micro_meta_info.size() * -1;
    const uint64_t data_loc = micro_meta_info.data_loc();
    const uint64_t reuse_version = micro_meta_info.reuse_version();
    int64_t phy_blk_idx = -1;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_->update_block_valid_length(data_loc, reuse_version, delta_size, phy_blk_idx));
    ASSERT_NE(-1, phy_blk_idx);
  }
  ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_T1].cnt_, 2);
  ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_B1].cnt_, write_macro_cnt * micro_cnt - 1);
  ASSERT_EQ(cache_stat.micro_stat().total_micro_cnt_, write_macro_cnt * micro_cnt + 1);
  ASSERT_EQ(write_macro_cnt, blk_cnt_info.data_blk_.used_cnt_);
  const int64_t exp_reusable_blk_cnt = write_macro_cnt - 1;
  ASSERT_EQ(exp_reusable_blk_cnt, phy_blk_mgr_->get_reusable_blocks_cnt());

  // 3. execute blk_ckpt
  int64_t blk_ckpt_cnt = exp_reusable_blk_cnt / 100;
  if (exp_reusable_blk_cnt % 100 != 0) {
    ++blk_ckpt_cnt;
  }
  for (int64_t i = 0; i < blk_ckpt_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.start_op());
    blk_ckpt_task_->ckpt_op_.blk_ckpt_ctx_.need_ckpt_ = true;
    ASSERT_EQ(OB_SUCCESS, blk_ckpt_task_->ckpt_op_.gen_checkpoint());
  }
  ASSERT_EQ(1, blk_cnt_info.data_blk_.used_cnt_);
  ASSERT_EQ(0, phy_blk_mgr_->get_reusable_blocks_cnt());

  // 4. persist micro_metas
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.start_op());
  persist_meta_task_->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task_->persist_meta_op_.gen_checkpoint());
  ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_B1].cnt_, write_macro_cnt * micro_cnt - 1);
  ASSERT_EQ(cache_stat.micro_stat().total_micro_cnt_, 2);

  ObArray<ObSSMicroBlockCacheKey> micro_keys2;
  ASSERT_EQ(OB_SUCCESS, micro_keys2.reserve((write_macro_cnt - 1) * micro_cnt));
  ASSERT_EQ(OB_SUCCESS, micro_keys2.push_back(micro_key_last));
  // 5. continue to add micro_blocks
  for (int64_t i = write_macro_cnt; i < write_macro_cnt * 2 - 1; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    int32_t j = (i == write_macro_cnt) ? 1 : 0;
    for (; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_keys2.push_back(micro_key));
      ASSERT_EQ(OB_SUCCESS, micro_cache_->add_micro_block_cache(micro_key, data_buf, micro_size,
        macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_T1].cnt_, (write_macro_cnt - 1) * micro_cnt + 1);
  ASSERT_EQ(arc_info.seg_info_arr_[SS_ARC_B1].cnt_, write_macro_cnt * micro_cnt - 1);
  ASSERT_EQ(cache_stat.micro_stat().total_micro_cnt_, (write_macro_cnt - 1) * micro_cnt + 1);

  // 6. try to execute eviction
  ASSERT_EQ(OB_SUCCESS, release_cache_task_->evict_op_.execute_eviction());
  ASSERT_LT(arc_info.seg_info_arr_[SS_ARC_T1].size_ + arc_info.seg_info_arr_[SS_ARC_B1].size_, arc_info.work_limit_ + micro_size);
  ASSERT_LT(0, cache_stat.task_stat().erase_persisted_cnt_);
  ASSERT_LE(cache_stat.task_stat().erase_persisted_cnt_, write_macro_cnt * micro_cnt - 1);

  LOG_INFO("TEST: finish test_evict_persisted_meta", K(cache_stat), K(arc_info));
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_evict_persisted_meta.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_evict_persisted_meta.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
