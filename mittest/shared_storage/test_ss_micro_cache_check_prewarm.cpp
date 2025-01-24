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

class TestSSMicroCacheCheckPrewarm : public ::testing::Test
{
public:
  TestSSMicroCacheCheckPrewarm() {}
  virtual ~TestSSMicroCacheCheckPrewarm() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSMicroCacheCheckPrewarm::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheCheckPrewarm::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheCheckPrewarm::SetUp()
{}

void TestSSMicroCacheCheckPrewarm::TearDown()
{}

TEST_F(TestSSMicroCacheCheckPrewarm, test_check_prewarm)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  const int64_t block_size = micro_cache->phy_block_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;

  ObSSPersistMicroDataTask &persist_task = micro_cache->task_runner_.persist_task_;
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;
  ObSSExecuteMicroCheckpointTask &micro_ckpt_task = micro_cache->task_runner_.micro_ckpt_task_;
  micro_ckpt_task.is_inited_ = false;
  ObSSExecuteBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  blk_ckpt_task.is_inited_ = false;
  usleep(1000 * 1000);

  ObSSMemDataManager *mem_data_mgr = &(micro_cache->mem_data_mgr_);
  ASSERT_NE(nullptr, mem_data_mgr);
  ObSSMicroMetaManager *micro_meta_mgr = &(micro_cache->micro_meta_mgr_);
  ASSERT_NE(nullptr, micro_meta_mgr);
  ObSSPhysicalBlockManager *phy_blk_mgr = &(micro_cache->phy_blk_mgr_);
  ASSERT_NE(nullptr, phy_blk_mgr);
  ObSSARCInfo &arc_info = micro_meta_mgr->arc_info_;
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

  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                                         ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    }
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
  ASSERT_EQ(WRITE_BLK_CNT * micro_cnt + 1, arc_info.seg_info_arr_[ARC_T1].cnt_);
  ASSERT_EQ(0, arc_info.seg_info_arr_[ARC_T2].cnt_);
  ASSERT_EQ(0, arc_info.seg_info_arr_[ARC_B1].cnt_);
  ASSERT_EQ(0, arc_info.seg_info_arr_[ARC_B2].cnt_);

  #define CHECK_IN_T1(micro_meta) \
    ASSERT_NE(nullptr, micro_meta); \
    ASSERT_EQ(true, micro_meta->is_in_l1_); \
    ASSERT_EQ(false, micro_meta->is_in_ghost_); \

  #define CHECK_IN_B1(micro_meta) \
    ASSERT_NE(nullptr, micro_meta); \
    ASSERT_EQ(true, micro_meta->is_in_l1_); \
    ASSERT_EQ(true, micro_meta->is_in_ghost_); \

  #define CHECK_IN_T2(micro_meta) \
    ASSERT_NE(nullptr, micro_meta); \
    ASSERT_EQ(false, micro_meta->is_in_l1_); \
    ASSERT_EQ(false, micro_meta->is_in_ghost_); \

  #define CHECK_IN_B2(micro_meta) \
    ASSERT_NE(nullptr, micro_meta); \
    ASSERT_EQ(false, micro_meta->is_in_l1_); \
    ASSERT_EQ(true, micro_meta->is_in_ghost_); \

  #define GET_CURR_ARC_SEG_CNT() \
    int64_t ori_t1_cnt = arc_info.seg_info_arr_[ARC_T1].cnt_; \
    int64_t ori_b1_cnt = arc_info.seg_info_arr_[ARC_B1].cnt_; \
    int64_t ori_t2_cnt = arc_info.seg_info_arr_[ARC_T2].cnt_; \
    int64_t ori_b2_cnt = arc_info.seg_info_arr_[ARC_B2].cnt_; \

  // 1. try to add existed micro_block
  {
    GET_CURR_ARC_SEG_CNT();
    // 1.1 prewarm mode
    // 1.1.1 add a existed T1 micro_block. Res: T1 -> T2
    MacroBlockId tmp_macro_id = TestSSCommonUtil::gen_macro_block_id(1);
    int32_t tmp_offset = payload_offset;
    ObSSMicroBlockCacheKey tmp_micro_key = TestSSCommonUtil::gen_phy_micro_key(tmp_macro_id, tmp_offset, micro_size);
    ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->micro_meta_map_.get(&tmp_micro_key, tmp_micro_meta_handle));
    ASSERT_EQ(true, tmp_micro_meta_handle.is_valid());
    ObSSMicroBlockMeta *tmp_micro_meta = tmp_micro_meta_handle.get_ptr();
    CHECK_IN_T1(tmp_micro_meta);
    micro_cache->add_micro_block_cache(tmp_micro_key, data_buf, micro_size,
                                       ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE);
    CHECK_IN_T2(tmp_micro_meta);
    ASSERT_EQ(ori_t1_cnt - 1, arc_info.seg_info_arr_[ARC_T1].cnt_);
    ASSERT_EQ(ori_t2_cnt + 1, arc_info.seg_info_arr_[ARC_T2].cnt_);

    // 1.1.2 evict this micro_block
    ObSSMicroBlockMetaHandle evict_micro_handle;
    evict_micro_handle.set_ptr(tmp_micro_meta);
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->try_evict_micro_block_meta(evict_micro_handle));
    CHECK_IN_B2(tmp_micro_meta);
    ASSERT_EQ(ori_t2_cnt, arc_info.seg_info_arr_[ARC_T2].cnt_);
    ASSERT_EQ(ori_b2_cnt + 1, arc_info.seg_info_arr_[ARC_B2].cnt_);
    evict_micro_handle.reset();

    // 1.1.3 add a existed B2 micro_block. Res: B2 -> T2
    micro_cache->add_micro_block_cache(tmp_micro_key, data_buf, micro_size,
                                       ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE);
    CHECK_IN_T2(tmp_micro_meta);
    ASSERT_EQ(true, tmp_micro_meta->is_valid_field());
    ASSERT_EQ(ori_t2_cnt + 1, arc_info.seg_info_arr_[ARC_T2].cnt_);
    ASSERT_EQ(ori_b2_cnt, arc_info.seg_info_arr_[ARC_B2].cnt_);

    // 1.2 common mode
    // 1.2.1 add a existed T1 micro_block. Res: T1 -> T2
    tmp_offset = payload_offset + micro_size;
    tmp_micro_key = TestSSCommonUtil::gen_phy_micro_key(tmp_macro_id, tmp_offset, micro_size);
    tmp_micro_meta_handle.reset();
    tmp_micro_meta = nullptr;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->micro_meta_map_.get(&tmp_micro_key, tmp_micro_meta_handle));
    tmp_micro_meta = tmp_micro_meta_handle.get_ptr();
    CHECK_IN_T1(tmp_micro_meta);
    micro_cache->add_micro_block_cache(tmp_micro_key, data_buf, micro_size,
                                       ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    CHECK_IN_T2(tmp_micro_meta);
    ASSERT_EQ(ori_t1_cnt - 2, arc_info.seg_info_arr_[ARC_T1].cnt_);
    ASSERT_EQ(ori_t2_cnt + 2, arc_info.seg_info_arr_[ARC_T2].cnt_);

    // 1.2.2 evict this micro_block
    ObSSMicroBlockMetaHandle evict_micro_handle1;
    evict_micro_handle1.set_ptr(tmp_micro_meta);
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->try_evict_micro_block_meta(evict_micro_handle1));
    CHECK_IN_B2(tmp_micro_meta);
    ASSERT_EQ(ori_t2_cnt + 1, arc_info.seg_info_arr_[ARC_T2].cnt_);
    ASSERT_EQ(ori_b2_cnt + 1, arc_info.seg_info_arr_[ARC_B2].cnt_);
    evict_micro_handle1.reset();

    // 1.2.3 add a existed B2 micro_block. Res: B2 -> T2
    micro_cache->add_micro_block_cache(tmp_micro_key, data_buf, micro_size,
                                       ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    CHECK_IN_T2(tmp_micro_meta);
    ASSERT_EQ(true, tmp_micro_meta->is_valid_field());
    ASSERT_EQ(ori_t2_cnt + 2, arc_info.seg_info_arr_[ARC_T2].cnt_);
    ASSERT_EQ(ori_b2_cnt, arc_info.seg_info_arr_[ARC_B2].cnt_);
  }

  // 2. try to add not-existed micro_block
  {
    GET_CURR_ARC_SEG_CNT();
    // 2.1 prewarm mode
    MacroBlockId tmp_macro_id = TestSSCommonUtil::gen_macro_block_id(WRITE_BLK_CNT + 1);
    int32_t tmp_offset = payload_offset + micro_size;
    ObSSMicroBlockCacheKey tmp_micro_key = TestSSCommonUtil::gen_phy_micro_key(tmp_macro_id, tmp_offset, micro_size);
    micro_cache->add_micro_block_cache(tmp_micro_key, data_buf, micro_size,
                                       ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE);
    ASSERT_EQ(ori_t1_cnt + 1, arc_info.seg_info_arr_[ARC_T1].cnt_);

    // 2.2 common mode
    tmp_offset = payload_offset + micro_size * 2;
    tmp_micro_key = TestSSCommonUtil::gen_phy_micro_key(tmp_macro_id, tmp_offset, micro_size);
    micro_cache->add_micro_block_cache(tmp_micro_key, data_buf, micro_size,
                                       ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    ASSERT_EQ(ori_t1_cnt + 2, arc_info.seg_info_arr_[ARC_T1].cnt_);
  }

  // 3. try to get existed micro_block
  {
    GET_CURR_ARC_SEG_CNT();
    char *read_buf = static_cast<char*>(allocator.alloc(micro_size));
    ASSERT_NE(nullptr, read_buf);
    // 3.1 prewarm mode
    // 3.1.1 get existed T1 micro_block. Res: still in T1
    MacroBlockId tmp_macro_id = TestSSCommonUtil::gen_macro_block_id(2);
    int32_t tmp_offset = payload_offset;
    ObSSMicroBlockCacheKey tmp_micro_key = TestSSCommonUtil::gen_phy_micro_key(tmp_macro_id, tmp_offset, micro_size);
    ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->micro_meta_map_.get(&tmp_micro_key, tmp_micro_meta_handle));
    ObSSMicroBlockMeta *tmp_micro_meta = tmp_micro_meta_handle.get_ptr();
    CHECK_IN_T1(tmp_micro_meta);
    ObSSMicroBlockId phy_micro_id(tmp_macro_id, tmp_offset, micro_size);
    MicroCacheGetType get_type = MicroCacheGetType::GET_CACHE_MISS_DATA;
    ObIOInfo io_info;
    ObStorageObjectHandle obj_handle;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(io_info, tmp_micro_key, micro_size, read_buf));
    ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(tmp_micro_key, phy_micro_id, get_type,
                          io_info, obj_handle, ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE));
    ASSERT_EQ(true, io_info.phy_block_handle_.is_valid());
    CHECK_IN_T1(tmp_micro_meta);
    ASSERT_EQ(true, tmp_micro_meta->is_persisted_valid());
    ASSERT_EQ(ori_t1_cnt, arc_info.seg_info_arr_[ARC_T1].cnt_);
    ASSERT_EQ(ori_t2_cnt, arc_info.seg_info_arr_[ARC_T2].cnt_);

    // 3.1.2 evict this micro_block
    ObSSMicroBlockMetaHandle evict_micro_handle;
    evict_micro_handle.set_ptr(tmp_micro_meta);
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->try_evict_micro_block_meta(evict_micro_handle));
    CHECK_IN_B1(tmp_micro_meta);
    ASSERT_EQ(false, tmp_micro_meta->is_valid_field());
    ASSERT_EQ(ori_t1_cnt - 1, arc_info.seg_info_arr_[ARC_T1].cnt_);
    ASSERT_EQ(ori_b1_cnt + 1, arc_info.seg_info_arr_[ARC_B1].cnt_);
    evict_micro_handle.reset();

    // 3.1.3 get B1 micro_block. Res: cache miss
    io_info.reset();
    ObSSMemBlockHandle mem_blk_handle;
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ObSSCacheHitType hit_type = ObSSCacheHitType::SS_CACHE_MISS;
    ObSSMicroSnapshotInfo micro_snapshot_info;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(io_info, tmp_micro_key, micro_size, read_buf));
    ASSERT_EQ(OB_SUCCESS, micro_cache->inner_get_micro_block_handle(tmp_micro_key, micro_snapshot_info,
      micro_meta_handle, mem_blk_handle, io_info.phy_block_handle_, hit_type, false/*update_arc*/));
    CHECK_IN_B1(tmp_micro_meta);
    ASSERT_EQ(hit_type, ObSSCacheHitType::SS_CACHE_MISS);
    ASSERT_EQ(false, tmp_micro_meta->is_valid_field());
    ASSERT_EQ(true, tmp_micro_meta->is_persisted_);
    ASSERT_EQ(ori_b1_cnt + 1, arc_info.seg_info_arr_[ARC_B1].cnt_);

    // 3.2 common_mode
    // 3.2.1 get existed T1 micro_block. Res: T1 -> T2
    io_info.reset();
    obj_handle.reset();
    tmp_offset = payload_offset + micro_size;
    tmp_micro_key = TestSSCommonUtil::gen_phy_micro_key(tmp_macro_id, tmp_offset, micro_size);
    ObSSMicroBlockId phy_micro_id1(tmp_macro_id, tmp_offset, micro_size);
    tmp_micro_meta_handle.reset();
    tmp_micro_meta = nullptr;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->micro_meta_map_.get(&tmp_micro_key, tmp_micro_meta_handle));
    tmp_micro_meta = tmp_micro_meta_handle.get_ptr();
    CHECK_IN_T1(tmp_micro_meta);
    get_type = MicroCacheGetType::FORCE_GET_DATA;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(io_info, tmp_micro_key, micro_size, read_buf));
    ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(tmp_micro_key, phy_micro_id1, get_type,
                          io_info, obj_handle, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    CHECK_IN_T2(tmp_micro_meta);
    ASSERT_EQ(ori_t1_cnt - 2, arc_info.seg_info_arr_[ARC_T1].cnt_);
    ASSERT_EQ(ori_t2_cnt + 1, arc_info.seg_info_arr_[ARC_T2].cnt_);

    // 3.2.2 evict this micro_block
    ObSSMicroBlockMetaHandle evict_micro_handle1;
    evict_micro_handle1.set_ptr(tmp_micro_meta);
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->try_evict_micro_block_meta(evict_micro_handle1));
    CHECK_IN_B2(tmp_micro_meta);
    ASSERT_EQ(ori_t2_cnt, arc_info.seg_info_arr_[ARC_T2].cnt_);
    ASSERT_EQ(ori_b2_cnt + 1, arc_info.seg_info_arr_[ARC_B2].cnt_);
    evict_micro_handle1.reset();

    // 3.2.3 get existed B2 micro_block. Res: still in B2, but marked as invalid
    io_info.reset();
    mem_blk_handle.reset();
    micro_meta_handle.reset();
    hit_type = ObSSCacheHitType::SS_CACHE_MISS;
    micro_snapshot_info.reset();
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(io_info, tmp_micro_key, micro_size, read_buf));
    ASSERT_EQ(OB_SUCCESS, micro_cache->inner_get_micro_block_handle(tmp_micro_key, micro_snapshot_info,
      micro_meta_handle, mem_blk_handle, io_info.phy_block_handle_, hit_type, true/*update_arc*/));
    CHECK_IN_B2(tmp_micro_meta);
    ASSERT_EQ(hit_type, ObSSCacheHitType::SS_CACHE_MISS);
    ASSERT_EQ(false, tmp_micro_meta->is_valid_field());
    ASSERT_EQ(true, tmp_micro_meta->is_persisted_);
    ASSERT_EQ(ori_b2_cnt + 1, arc_info.seg_info_arr_[ARC_B2].cnt_);
  }

  // 4. check some other prewarm interfaces.
  {
    GET_CURR_ARC_SEG_CNT();
    char *read_buf = static_cast<char*>(allocator.alloc(micro_size));
    ASSERT_NE(nullptr, read_buf);

    MacroBlockId tmp_macro_id = TestSSCommonUtil::gen_macro_block_id(3);
    int32_t tmp_offset = payload_offset;
    ObSSMicroBlockCacheKey tmp_micro_key = TestSSCommonUtil::gen_phy_micro_key(tmp_macro_id, tmp_offset, micro_size);
    ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->micro_meta_map_.get(&tmp_micro_key, tmp_micro_meta_handle));
    ObSSMicroBlockMeta *tmp_micro_meta = tmp_micro_meta_handle.get_ptr();
    CHECK_IN_T1(tmp_micro_meta);

    ObSSCacheHitType hit_type;
    ObSSMicroSnapshotInfo snapshot_info;
    ASSERT_EQ(OB_SUCCESS, micro_cache->check_micro_block_exist(tmp_micro_key, snapshot_info, hit_type));
    ASSERT_EQ(ObSSCacheHitType::SS_CACHE_HIT_DISK, hit_type);
    ASSERT_EQ(true, snapshot_info.is_in_l1_);
    ASSERT_EQ(false, snapshot_info.is_in_ghost_);
    CHECK_IN_T1(tmp_micro_meta);
    ASSERT_EQ(ori_t1_cnt, arc_info.seg_info_arr_[ARC_T1].cnt_);

    tmp_offset = payload_offset + micro_size;
    tmp_micro_key = TestSSCommonUtil::gen_phy_micro_key(tmp_macro_id, tmp_offset, micro_size);
    tmp_micro_meta_handle.reset();
    tmp_micro_meta = nullptr;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->micro_meta_map_.get(&tmp_micro_key, tmp_micro_meta_handle));
    tmp_micro_meta = tmp_micro_meta_handle.get_ptr();
    CHECK_IN_T1(tmp_micro_meta);

    ObIOInfo io_info;
    ObStorageObjectHandle obj_handle;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(io_info, tmp_micro_key, micro_size, read_buf));
    ASSERT_EQ(OB_SUCCESS, micro_cache->get_cached_micro_block(tmp_micro_key, io_info, obj_handle,
                          ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE));
    CHECK_IN_T1(tmp_micro_meta);
    ASSERT_EQ(ori_t1_cnt, arc_info.seg_info_arr_[ARC_T1].cnt_);

    int64_t access_time = tmp_micro_meta->access_time();
    ObArray<ObSSMicroBlockCacheKey> tmp_micro_keys;
    ASSERT_EQ(OB_SUCCESS, tmp_micro_keys.push_back(tmp_micro_key));
    ASSERT_EQ(OB_SUCCESS, micro_cache->update_micro_block_heat(tmp_micro_keys, false, true, 1));
    ASSERT_LT(access_time, tmp_micro_meta->access_time_);
    ASSERT_EQ(ori_t1_cnt, arc_info.seg_info_arr_[ARC_T1].cnt_);

    ASSERT_EQ(OB_SUCCESS, micro_cache->update_micro_block_heat(tmp_micro_keys, true, false, 1));
    CHECK_IN_T2(tmp_micro_meta);
    ASSERT_EQ(ori_t1_cnt - 1, arc_info.seg_info_arr_[ARC_T1].cnt_);
    ASSERT_EQ(ori_t2_cnt + 1, arc_info.seg_info_arr_[ARC_T2].cnt_);
  }

  allocator.clear();
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_check_prewarm.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_check_prewarm.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}