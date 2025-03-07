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

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
class TestSSHandleArcSegOp : public ::testing::Test
{
public:
  TestSSHandleArcSegOp() {}
  virtual ~TestSSHandleArcSegOp() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSHandleArcSegOp::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSHandleArcSegOp::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSHandleArcSegOp::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 32)));
  micro_cache->start();
}

void TestSSHandleArcSegOp::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

TEST_F(TestSSHandleArcSegOp, test_arc_evict_delete)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  const int64_t block_size = micro_cache->phy_block_size_;

  ObSSPersistMicroDataTask &persist_task = micro_cache->task_runner_.persist_task_;
  ObSSReleaseCacheTask &arc_cache_task = micro_cache->task_runner_.release_cache_task_;
  arc_cache_task.interval_us_ = 3600 * 1000 * 1000L;
  arc_cache_task.is_inited_ = false;
  usleep(1000 * 1000);

  ObSSMemDataManager *mem_data_mgr = &(micro_cache->mem_data_mgr_);
  ASSERT_NE(nullptr, mem_data_mgr);
  ObSSMicroMetaManager *micro_meta_mgr = &(micro_cache->micro_meta_mgr_);
  ASSERT_NE(nullptr, micro_meta_mgr);
  ObSSARCInfo &arc_info = micro_meta_mgr->arc_info_;
  ObSSPhysicalBlockManager *phy_blk_mgr = &(micro_cache->phy_blk_mgr_);
  ASSERT_NE(nullptr, phy_blk_mgr);
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;

  const int64_t available_block_cnt = phy_blk_mgr->blk_cnt_info_.cache_limit_blk_cnt();
  const int64_t WRITE_BLK_CNT = 230;
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

  // 1. write 230 fulfilled phy_block
  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroCacheAccessType access_type = ObSSMicroCacheAccessType::COMMON_IO_TYPE;
      ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, access_type));
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      micro_meta_handle.get_ptr()->access_time_ = i * 100 + j + 1;
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(WRITE_BLK_CNT * micro_cnt, arc_info.seg_info_arr_[ARC_T1].count());
  ASSERT_EQ(arc_info.seg_info_arr_[ARC_T1].count() * micro_size, arc_info.seg_info_arr_[ARC_T1].size());

  // 2. hit some micro in T1, move them into T2
  ObArray<ObSSMicroBlockCacheKey> micro_key_arr;
  const int64_t t2_macro_cnt = 100;
  for (int64_t i = 0; i < t2_macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_key_arr.push_back(micro_key));
    }
    ASSERT_EQ(OB_SUCCESS, micro_cache->update_micro_block_heat(micro_key_arr, true, false));
    micro_key_arr.reset();
  }
  ASSERT_EQ(t2_macro_cnt * micro_cnt, arc_info.seg_info_arr_[ARC_T2].count());
  ASSERT_EQ(arc_info.seg_info_arr_[ARC_T2].count() * micro_size, arc_info.seg_info_arr_[ARC_T2].size());
  ASSERT_EQ(WRITE_BLK_CNT * micro_cnt - arc_info.seg_info_arr_[ARC_T2].count(), arc_info.seg_info_arr_[ARC_T1].count());
  ASSERT_EQ(arc_info.seg_info_arr_[ARC_T1].count() * micro_size, arc_info.seg_info_arr_[ARC_T1].size());

  // 3. adjust arc_info
  const int64_t new_arc_limit = 200 * micro_cnt * micro_size;
  arc_info.update_arc_limit(new_arc_limit);
  ASSERT_EQ(new_arc_limit, arc_info.limit_);

  // 4. first execute arc_cache_task
  usleep(1000 * 1000);
  ASSERT_EQ(OB_SUCCESS, arc_cache_task.evict_op_.handle_arc_seg_op());
  ASSERT_GE(600, cache_stat.task_stat().t1_evict_cnt_);
  ASSERT_LT(0, cache_stat.task_stat().t1_evict_cnt_);
  arc_cache_task.evict_op_.clear_for_next_round();

  // 5. continue to add some micro into T1
  for (int64_t i = WRITE_BLK_CNT; i < WRITE_BLK_CNT + 75; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroCacheAccessType access_type = ObSSMicroCacheAccessType::COMMON_IO_TYPE;
      ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, access_type));
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      micro_meta_handle.get_ptr()->access_time_ = i * 100 + j + 1;
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }

  // 6. second execute arc_cache_task
  usleep(1000 * 1000);
  cache_stat.task_stat_.reset();
  ASSERT_EQ(OB_SUCCESS, arc_cache_task.evict_op_.handle_arc_seg_op());
  ASSERT_LT(0, cache_stat.task_stat().t1_evict_cnt_);
  int64_t b1_delete_cnt = cache_stat.task_stat().b1_delete_cnt_;
  arc_cache_task.evict_op_.clear_for_next_round();

  // 7. third execute arc_cache_task
  ASSERT_EQ(OB_SUCCESS, arc_cache_task.evict_op_.handle_arc_seg_op());
  b1_delete_cnt += cache_stat.task_stat().b1_delete_cnt_;
  ASSERT_LT(0, b1_delete_cnt);
  arc_cache_task.evict_op_.clear_for_next_round();

  allocator.clear();
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_handle_arc_seg_op.log*");
  OB_LOGGER.set_file_name("test_ss_handle_arc_seg_op.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}