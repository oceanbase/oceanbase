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
#include "test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroMetaManager : public ::testing::Test
{
public:
  TestSSMicroMetaManager() = default;
  virtual ~TestSSMicroMetaManager() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

private:
  const static uint32_t ORI_MICRO_REF_CNT = 0;

public:
  class TestSSMicroMetaMgrThread : public Threads
  {
  public:
    enum class TestParallelType
    {
      TEST_PARALLEL_ADD_AND_GET_MICRO_META,
      TEST_PARALLEL_ADD_SAME_MICRO_META,
    };

  public:
    TestSSMicroMetaMgrThread(ObTenantBase *tenant_base, ObSSMicroMetaManager *micro_meta_mgr,
      ObSSMemBlock *mem_blk, TestParallelType type)
        : tenant_base_(tenant_base), micro_meta_mgr_(micro_meta_mgr), mem_blk_(mem_blk),
          type_(type), fail_cnt_(0), exe_cnt_(0)
    {}
    void run(int64_t idx) final
    {
      ObTenantEnv::set_tenant(tenant_base_);
      if (type_ == TestParallelType::TEST_PARALLEL_ADD_AND_GET_MICRO_META) {
        parallel_add_and_get_micro_meta(idx);
      } else if (type_ == TestParallelType::TEST_PARALLEL_ADD_SAME_MICRO_META) {
        parallel_add_same_micro_meta(idx);
      }
    }
    int64_t get_fail_cnt() { return ATOMIC_LOAD(&fail_cnt_); }
    int64_t get_exe_cnt() { return ATOMIC_LOAD(&exe_cnt_); }

  private:
    int parallel_add_and_get_micro_meta(int64_t idx);
    int parallel_add_same_micro_meta(int64_t idx);

  private:
    ObTenantBase *tenant_base_;
    ObSSMicroMetaManager *micro_meta_mgr_;
    ObSSMemBlock *mem_blk_;
    TestParallelType type_;
    int64_t fail_cnt_;
    int64_t exe_cnt_;
  };
};

void TestSSMicroMetaManager::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroMetaManager::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroMetaManager::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 31)));
  micro_cache->start();
}

void TestSSMicroMetaManager::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

int TestSSMicroMetaManager::TestSSMicroMetaMgrThread::parallel_add_and_get_micro_meta(int64_t idx)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  ObSSMicroSnapshotInfo micro_snapshot_info;
  ObSSMemBlockHandle mem_blk_handle;
  ObSSPhysicalBlockHandle phy_blk_handle;
  bool is_persisted = false;
  const uint32_t block_size = 2 * 1024 * 1024;
  const uint64_t tenant_id = MTL_ID();
  ObMemAttr attr(tenant_id, "test");

  const int32_t micro_size = 100;
  char micro_data[micro_size];
  MEMSET(micro_data, 'a', micro_size);
  for (uint64_t i = 1; OB_SUCC(ret) && i <= 1000; i++) {
    if (i == 1) {
      macro_id = TestSSCommonUtil::gen_macro_block_id(1);
    } else {
      macro_id = TestSSCommonUtil::gen_macro_block_id(idx * 10000 + i);
    }
    int32_t offset = idx + 1;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    ObSSMemMicroInfo mem_info(1, micro_size);
    uint32_t micro_crc = 0;
    int32_t data_offset = -1;
    int32_t idx_offset = -1;
    ObSSMemBlockHandle mem_blk_handle;
    mem_blk_handle.set_ptr(mem_blk_);
    bool real_add = false;
    if (OB_FAIL(mem_blk_->calc_write_location(micro_key, micro_size, data_offset, idx_offset))) {
      LOG_WARN("fail to calc_write_location", KR(ret), K(i), K(micro_key));
    } else if (OB_FAIL(mem_blk_->write_micro_data(micro_key, micro_data, micro_size, data_offset, idx_offset,
               micro_crc))) {
      LOG_WARN("fail to write_micro_data", KR(ret), K(i), K(micro_key));
    } else if (OB_FAIL(micro_meta_mgr_->add_or_update_micro_block_meta(micro_key, micro_size, micro_crc, mem_blk_handle,
              real_add))) {
      LOG_WARN("fail to add or update micro_block meta", KR(ret), K(micro_key), K(micro_size), KPC_(mem_blk));
    }

    ObSSMicroBlockMetaHandle micro_handle;
    if (FAILEDx(micro_meta_mgr_->get_micro_block_meta_handle(micro_key, micro_snapshot_info, micro_handle,
        mem_blk_handle, phy_blk_handle, true))) {
      LOG_WARN("fail to get micro block meta handle", KR(ret), K(micro_key));
    }
  }

  if (OB_FAIL(ret)) {
    ATOMIC_INC(&fail_cnt_);
  }
  return ret;
}

int TestSSMicroMetaManager::TestSSMicroMetaMgrThread::parallel_add_same_micro_meta(int64_t idx)
{
  int ret = OB_SUCCESS;
  const int32_t micro_size = 100;
  MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100);
  int32_t offset = 100;
  ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
  ObSSMicroSnapshotInfo micro_snapshot_info;
  ObSSMemBlockHandle mem_blk_handle;
  ObSSPhysicalBlockHandle phy_blk_handle;
  bool is_persisted = false;
  const uint32_t block_size = 2 * 1024 * 1024;
  const uint64_t tenant_id = MTL_ID();
  ObMemAttr attr(tenant_id, "test");

  uint32_t micro_crc = 0;
  mem_blk_handle.set_ptr(mem_blk_);
  bool real_add = false;
  if (OB_FAIL(micro_meta_mgr_->add_or_update_micro_block_meta(micro_key, micro_size, micro_crc,
      mem_blk_handle, real_add))) {
    LOG_WARN("fail to add or update micro_block meta", KR(ret), K(micro_key), K(micro_size), KPC_(mem_blk));
  } else {
    ATOMIC_INC(&exe_cnt_);
  }

  usleep(3 * 1000 * 1000);

  ObSSMicroBlockMetaHandle micro_handle;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(micro_meta_mgr_->get_micro_block_meta_handle(micro_key, micro_snapshot_info,
             micro_handle, mem_blk_handle, phy_blk_handle, true))) {
    LOG_WARN("fail to get micro block meta handle", KR(ret), K(micro_key));
  }

  if (OB_FAIL(ret)) {
    ATOMIC_INC(&fail_cnt_);
  }
  return ret;
}

TEST_F(TestSSMicroMetaManager, test_micro_meta_mgr)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_micro_meta_mgr");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  const int64_t micro_pool_fixed_cnt = 1000;

  const int64_t micro_cnt = 128;
  const int64_t micro_size = 12 * 1024;
  const int64_t macro_cnt = micro_pool_fixed_cnt / micro_cnt;
  const int64_t extra_micro_cnt = micro_pool_fixed_cnt % micro_cnt;
  char micro_data[micro_size];
  MEMSET(micro_data, 'a', micro_size);

  for (int64_t i = 0; OB_SUCC(ret) && i < macro_cnt; ++i) {
    MacroBlockId macro_id(0, 100 + i, 0);
    for (int64_t j = 0; OB_SUCC(ret) && j < micro_cnt; ++j) {
      int32_t offset = 1 + j;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      if (OB_FAIL(micro_cache->add_micro_block_cache(micro_key, micro_data, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
        LOG_WARN("fail to add micro_block cache", KR(ret), K(i), K(j));
      }
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(macro_cnt * micro_cnt, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());

  if (extra_micro_cnt > 0) {
    MacroBlockId macro_id(0, 100 + macro_cnt, 0);
    for (int64_t j = 0; OB_SUCC(ret) && j < extra_micro_cnt; ++j) {
      int32_t offset = 1 + j;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      if (OB_FAIL(micro_cache->add_micro_block_cache(micro_key, micro_data, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
        LOG_WARN("fail to add micro_block cache", KR(ret), K(j));
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(macro_cnt * micro_cnt + extra_micro_cnt, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
  }

  // add an already existed micro_block meta
  {
    MacroBlockId macro_id(0, 100, 0);
    int32_t offset = 1;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    if (OB_FAIL(micro_cache->add_micro_block_cache(micro_key, micro_data, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
      LOG_WARN("fail to add micro_block cache", KR(ret));
    }
    ASSERT_EQ(macro_cnt * micro_cnt + extra_micro_cnt, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
  }
}

TEST_F(TestSSMicroMetaManager, test_destroy_micro_map)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_destroy_micro_map");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;

  const int64_t micro_cnt = 128;
  const int64_t micro_size = 12 * 1024;
  const int64_t macro_cnt = 2;
  char micro_data[micro_size];
  MEMSET(micro_data, 'a', micro_size);

  for (int64_t i = 0; OB_SUCC(ret) && i < macro_cnt; ++i) {
    MacroBlockId macro_id(0, 10000 + i, 0);
    for (int64_t j = 0; OB_SUCC(ret) && j < micro_cnt; ++j) {
      int32_t offset = 1 + j;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      if (OB_FAIL(micro_cache->add_micro_block_cache(micro_key, micro_data, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
        LOG_WARN("fail to add micro_block cache", KR(ret), K(i), K(j));
      }
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_cnt * micro_cnt, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());

  // add the same micro_key again
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_cnt; ++i) {
    MacroBlockId macro_id(0, 10000 + i, 0);
    for (int64_t j = 0; OB_SUCC(ret) && j < micro_cnt; ++j) {
      int32_t offset = 1 + j;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      if (OB_FAIL(micro_cache->add_micro_block_cache(micro_key, micro_data, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
        LOG_WARN("fail to add micro_block cache", KR(ret), K(i), K(j));
      }
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_cnt * micro_cnt, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());

  micro_cache->task_runner_.is_inited_ = false;
  ob_usleep(2 * 1000 * 1000);

  micro_meta_mgr.micro_meta_map_.destroy();
  ASSERT_EQ(0, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
}

TEST_F(TestSSMicroMetaManager, test_micro_map_iter)
{
  LOG_INFO("TEST_CASE: start test_micro_map_iter");
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  const int32_t micro_size = 100;
  const int64_t macro_cnt = 1000;
  const int64_t micro_cnt = 200;
  const int32_t block_size = micro_meta_mgr.block_size_;
  // generate micro_meta.
  MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(200);
  ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 101, micro_size);
  ObSSMicroBlockMeta *tmp_micro_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(tmp_micro_meta));
  ASSERT_NE(nullptr, tmp_micro_meta);
  tmp_micro_meta->reset();
  // micro meta is in T1.
  tmp_micro_meta->is_in_l1_ = true;
  tmp_micro_meta->is_in_ghost_ = false;
  tmp_micro_meta->length_ = micro_size;
  tmp_micro_meta->is_persisted_ = true;
  tmp_micro_meta->reuse_version_ = 1;
  tmp_micro_meta->data_dest_ = 2 * block_size + 1;
  tmp_micro_meta->access_time_ = 100;
  tmp_micro_meta->micro_key_ = micro_key;
  const int64_t ori_cnt = micro_meta_mgr.arc_info_.seg_info_arr_[ARC_T1].cnt_;
  const int64_t ori_size = micro_meta_mgr.arc_info_.seg_info_arr_[ARC_T1].size_;
  ObSSMicroBlockMetaHandle tmp_micro_handle;
  tmp_micro_handle.set_ptr(tmp_micro_meta);
  ASSERT_EQ(ori_micro_ref_cnt + 1, tmp_micro_meta->ref_cnt_);
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.inner_add_ckpt_micro_block_meta(tmp_micro_handle));
  ASSERT_EQ(ori_micro_ref_cnt + 2, tmp_micro_meta->ref_cnt_);
  ASSERT_EQ(ori_cnt + 1, micro_meta_mgr.arc_info_.seg_info_arr_[ARC_T1].cnt_);
  ASSERT_EQ(ori_size + micro_size, micro_meta_mgr.arc_info_.seg_info_arr_[ARC_T1].size_);


  const ObSSMicroBlockCacheKey *cur_micro_key = nullptr;
  ObSSMicroBlockMetaHandle cur_micro_handle;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.bg_micro_iter_.next(cur_micro_key, cur_micro_handle));
  ASSERT_NE(nullptr, cur_micro_key);
  ASSERT_EQ(micro_key, *cur_micro_key);
  ASSERT_EQ(true, cur_micro_handle.is_valid());
  ASSERT_EQ(ori_micro_ref_cnt + 3, tmp_micro_meta->ref_cnt_);
}

TEST_F(TestSSMicroMetaManager, test_random_acquire_cold_micro_blocks)
{
  LOG_INFO("TEST_CASE: start test_random_acquire_cold_micro_blocks");
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSReleaseCacheTask &arc_task = MTL(ObSSMicroCache *)->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;
  usleep(arc_task.interval_us_);
  ObSSARCIterInfo &arc_iter_info = arc_task.evict_op_.arc_iter_info_;
  const int32_t micro_size = 100;
  const int64_t macro_cnt = 1000;
  const int64_t micro_cnt = 200;
  const int32_t block_size = micro_meta_mgr.block_size_;
  // generate micro_meta.
  for (int i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    for (int j = 0; j < micro_cnt; j++) {
      const int64_t idx = (i * micro_cnt + j + 1);
      const int32_t offset = j + 1;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMeta *tmp_micro_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(tmp_micro_meta));
      ASSERT_NE(nullptr, tmp_micro_meta);
      tmp_micro_meta->reset();
      // micro meta is in T1.
      tmp_micro_meta->is_in_l1_ = true;
      tmp_micro_meta->is_in_ghost_ = false;
      tmp_micro_meta->length_ = micro_size;
      tmp_micro_meta->is_persisted_ = true;
      tmp_micro_meta->reuse_version_ = 1;
      tmp_micro_meta->data_dest_ = (i + 2) * block_size + j * micro_size + 1;
      tmp_micro_meta->access_time_ = idx;
      tmp_micro_meta->micro_key_ = micro_key;
      if (idx % 500 == 0) {
        tmp_micro_meta->is_reorganizing_ = true;
      }
      const int64_t ori_cnt = micro_meta_mgr.arc_info_.seg_info_arr_[ARC_T1].cnt_;
      const int64_t ori_size = micro_meta_mgr.arc_info_.seg_info_arr_[ARC_T1].size_;
      ObSSMicroBlockMetaHandle tmp_micro_handle;
      tmp_micro_handle.set_ptr(tmp_micro_meta);
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.inner_add_ckpt_micro_block_meta(tmp_micro_handle));
      ASSERT_EQ(ori_cnt + 1, micro_meta_mgr.arc_info_.seg_info_arr_[ARC_T1].cnt_);
      ASSERT_EQ(ori_size + micro_size, micro_meta_mgr.arc_info_.seg_info_arr_[ARC_T1].size_);
    }
  }
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  arc_info.update_arc_limit(macro_cnt * micro_cnt * micro_size / 2);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  arc_iter_info.adjust_arc_iter_seg_info(ARC_T1);
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.acquire_cold_micro_blocks(arc_iter_info, ARC_T1));
  ASSERT_LT(0, arc_iter_info.t1_micro_heap_.count());
  ASSERT_LE(arc_iter_info.t1_micro_heap_.count(), arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.exp_iter_cnt_);

  int64_t cold_micro_cnt = 0;
  int ret = OB_SUCCESS;
  bool exist = true;
  do {
    ObSSMicroBlockMetaHandle cold_micro_handle;
    ret = arc_iter_info.pop_cold_micro(ARC_T1, cold_micro_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (cold_micro_handle.is_valid()) {
      ++cold_micro_cnt;
    } else {
      exist = false;
    }
  } while (exist);
  ASSERT_LT(0, cold_micro_cnt);
}

TEST_F(TestSSMicroMetaManager, test_evict_and_delete_micro_meta)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_evict_and_delete_micro_meta");
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSReleaseCacheTask &arc_task = MTL(ObSSMicroCache *)->task_runner_.release_cache_task_;
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache *)->cache_stat_;

  arc_task.is_inited_ = false;
  MacroBlockId macro_id(0, 100, 0);
  const int64_t offset = 1;
  const int64_t micro_size = 256;
  ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
  char micro_data[micro_size];
  MEMSET(micro_data, 'a', micro_size);
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  ObSSMemBlockPool &mem_blk_pool = mem_data_mgr.mem_block_pool_;
  ObSSMemBlock *mem_blk_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_blk_ptr, true/*is_fg*/));
  ASSERT_NE(nullptr, mem_blk_ptr);
  uint32_t micro_crc = 0;
  int32_t data_offset = -1;
  int32_t idx_offset = -1;
  ASSERT_EQ(OB_SUCCESS, mem_blk_ptr->calc_write_location(micro_key, micro_size, data_offset, idx_offset));
  ASSERT_EQ(OB_SUCCESS, mem_blk_ptr->write_micro_data(micro_key, micro_data, micro_size, data_offset, idx_offset, micro_crc));
  ObSSMemBlockHandle mem_blk_handle;
  mem_blk_handle.set_ptr(mem_blk_ptr);
  bool real_add = false;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc, mem_blk_handle, real_add));
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  ASSERT_EQ(1, arc_info.seg_info_arr_[ARC_T1].cnt_);
  ASSERT_EQ(micro_size, arc_info.seg_info_arr_[ARC_T1].size_);

  // get micro meta, T1 -> T2
  ObSSMicroBlockMetaHandle micro_handle;
  ret = micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_handle, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, micro_handle.is_valid());
  micro_handle.get_ptr()->is_persisted_ = true;
  const uint64_t access_time = micro_handle.get_ptr()->access_time();
  ObArray<ObSSMicroBlockCacheKey> tmp_micro_keys;
  ASSERT_EQ(OB_SUCCESS, tmp_micro_keys.push_back(micro_handle.get_ptr()->micro_key()));
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.update_micro_block_meta_heat(tmp_micro_keys, false, false, 120));
  ASSERT_EQ(access_time + 120, micro_handle.get_ptr()->access_time());
  ASSERT_EQ(0, micro_meta_mgr.arc_info_.seg_info_arr_[ARC_T1].cnt_);
  ASSERT_EQ(0, micro_meta_mgr.arc_info_.seg_info_arr_[ARC_T1].size_);
  ASSERT_EQ(1, micro_meta_mgr.arc_info_.seg_info_arr_[ARC_T2].cnt_);
  ASSERT_EQ(micro_size, micro_meta_mgr.arc_info_.seg_info_arr_[ARC_T2].size_);

  // succ to evict.
  ObSSMicroBlockMeta *micro_meta = micro_handle.get_ptr();
  ObSSMicroBlockMeta *tmp_micro_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(tmp_micro_meta));
  ASSERT_NE(nullptr, tmp_micro_meta);
  ASSERT_EQ(0, tmp_micro_meta->ref_cnt_);
  *tmp_micro_meta = *micro_meta;
  ObSSMicroBlockMetaHandle evict_micro_handle;
  evict_micro_handle.set_ptr(tmp_micro_meta);
  ASSERT_EQ(true, evict_micro_handle.is_valid());
  ASSERT_EQ(false, micro_meta->is_in_l1_);
  ASSERT_EQ(false, micro_meta->is_in_ghost_);
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_evict_micro_block_meta(evict_micro_handle));
  ASSERT_EQ(false, micro_meta->is_in_l1_);
  ASSERT_EQ(true, micro_meta->is_in_ghost_);
  ASSERT_EQ(0, arc_info.seg_info_arr_[ARC_T2].cnt_);
  ASSERT_EQ(0, arc_info.seg_info_arr_[ARC_T2].size_);
  ASSERT_EQ(1, arc_info.seg_info_arr_[ARC_B2].cnt_);
  ASSERT_EQ(micro_size, arc_info.seg_info_arr_[ARC_B2].size_);

  ASSERT_EQ(1, tmp_micro_meta->ref_cnt_);
  evict_micro_handle.reset();
  tmp_micro_meta = nullptr;

  // succ to delete.
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::alloc_micro_block_meta(tmp_micro_meta));
  ASSERT_NE(nullptr, tmp_micro_meta);
  *tmp_micro_meta = *micro_meta;
  ObSSMicroBlockMetaHandle delete_micro_handle;
  delete_micro_handle.set_ptr(tmp_micro_meta);
  ASSERT_EQ(1, tmp_micro_meta->ref_cnt_);
  ASSERT_EQ(true, delete_micro_handle.is_valid());
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_delete_micro_block_meta(delete_micro_handle));
  ASSERT_EQ(0, arc_info.seg_info_arr_[ARC_B2].cnt_);
  ASSERT_EQ(0, arc_info.seg_info_arr_[ARC_B2].size_);
  delete_micro_handle.reset();
  tmp_micro_meta = nullptr;
  // check already delete
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr.micro_meta_map_.get(&micro_key, tmp_micro_meta_handle));

  ObSSMicroBlockCacheKey micro_key2 = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset + 1, micro_size);
  ASSERT_EQ(OB_SUCCESS, mem_blk_ptr->calc_write_location(micro_key2, micro_size, data_offset, idx_offset));
  ASSERT_EQ(OB_SUCCESS, mem_blk_ptr->write_micro_data(micro_key2, micro_data, micro_size, data_offset, idx_offset, micro_crc));
  ObSSMemBlockHandle mem_blk_handle2;
  mem_blk_handle2.set_ptr(mem_blk_ptr);
  micro_meta_mgr.set_mem_limited(true);
  const int64_t origin_total_mem_size = cache_stat.micro_stat().micro_total_mem_size_;
  cache_stat.micro_stat().micro_total_mem_size_ = micro_meta_mgr.get_cache_mem_limit();
  real_add = false;
  ASSERT_EQ(OB_SS_CACHE_REACH_MEM_LIMIT,
      micro_meta_mgr.add_or_update_micro_block_meta(micro_key2, micro_size, micro_crc, mem_blk_handle2, real_add));
  cache_stat.micro_stat().micro_total_mem_size_ = origin_total_mem_size;
  ASSERT_EQ(false, micro_meta_mgr.check_cache_mem_limit());
  ASSERT_EQ(OB_SUCCESS,
      micro_meta_mgr.add_or_update_micro_block_meta(micro_key2, micro_size, micro_crc, mem_blk_handle2, real_add));
  ASSERT_EQ(true, real_add);
}

TEST_F(TestSSMicroMetaManager, test_same_micro_update)
{
  LOG_INFO("TEST_CASE: start test_same_micro_update");
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache *)->cache_stat_;
  ObArenaAllocator allocator;
  char *write_buf = nullptr;

  const int64_t micro_cnt = 20;
  const int64_t block_size = 2 * 1024 * 1024;
  const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                 ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int64_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int64_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
  write_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, write_buf);
  MEMSET(write_buf, 'a', micro_size);

  MacroBlockId macro_id(0, 10000, 0);
  ObSSMemBlockHandle mem_blk_handle;
  for (int64_t j = 0; j < micro_cnt; j += 2) {
    uint32_t micro_crc = 0;
    int32_t offset = 1 + j * micro_size;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    ASSERT_EQ(OB_SUCCESS, mem_data_mgr.add_micro_block_data(micro_key, write_buf, micro_size, mem_blk_handle, micro_crc));
    ASSERT_NE(0, micro_crc);
    ASSERT_EQ(true, mem_blk_handle.is_valid());
    bool real_add = false;
    ASSERT_EQ(OB_SUCCESS,
        micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc, mem_blk_handle, real_add));
    ASSERT_EQ(true, real_add);

    micro_crc = 0;
    mem_blk_handle.reset();
    ASSERT_EQ(OB_SUCCESS, mem_data_mgr.add_micro_block_data(micro_key, write_buf, micro_size, mem_blk_handle, micro_crc));
    ASSERT_EQ(true, mem_blk_handle.is_valid());
    ASSERT_NE(0, micro_crc);
    real_add = false;
    ASSERT_EQ(OB_SUCCESS,
        micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc, mem_blk_handle, real_add));
    ASSERT_EQ(false, real_add);
  }

  ASSERT_EQ(true, mem_blk_handle.is_valid());
  ASSERT_EQ(micro_cnt / 2, mem_blk_handle()->valid_count_);
  ASSERT_EQ((micro_cnt / 2) * micro_size, mem_blk_handle()->valid_val_);

  const int64_t block_offset = 2 * block_size; // the 3rd phy_block
  const uint32_t blk_reuse_version = 1;
  int64_t updated_micro_size = 0;
  int64_t updated_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS,
      micro_meta_mgr.update_micro_block_meta(
          mem_blk_handle, block_offset, blk_reuse_version, updated_micro_size, updated_micro_cnt));
  ASSERT_EQ(micro_cnt / 2 * micro_size, updated_micro_size);
  ASSERT_EQ(micro_cnt / 2, updated_micro_cnt);
}

TEST_F(TestSSMicroMetaManager, test_reach_mem_limit_when_add_micro)
{
  LOG_INFO("TEST_CASE: start test_reach_mem_limit_when_add_micro");
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache *)->cache_stat_;
  ObArenaAllocator allocator;
  char *write_buf = nullptr;

  const int64_t micro_cnt = 20;
  const int64_t block_size = 2 * 1024 * 1024;
  const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                 ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int64_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int64_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
  write_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, write_buf);
  MEMSET(write_buf, 'a', micro_size);

  MacroBlockId macro_id(0, 10000, 0);
  ObSSMemBlockHandle mem_blk_handle;
  for (int64_t j = 0; j < micro_cnt; ++j) {
    uint32_t micro_crc = 0;
    int32_t offset = 1 + j * micro_size;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    ASSERT_EQ(OB_SUCCESS, mem_data_mgr.add_micro_block_data(micro_key, write_buf, micro_size, mem_blk_handle, micro_crc));
    ASSERT_NE(0, micro_crc);
    ASSERT_EQ(true, mem_blk_handle.is_valid());
    bool real_add = false;
    if (j == micro_cnt / 2) {
      const int64_t ori_mem_size = cache_stat.micro_stat().micro_total_mem_size_;
      cache_stat.micro_stat().micro_total_mem_size_ = micro_meta_mgr.get_cache_mem_limit();
      ASSERT_EQ(OB_SS_CACHE_REACH_MEM_LIMIT,
          micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc, mem_blk_handle, real_add));
      ASSERT_EQ(false, real_add);
      cache_stat.micro_stat().micro_total_mem_size_ = ori_mem_size;
      micro_meta_mgr.set_mem_limited(false);
    } else {
      ASSERT_EQ(OB_SUCCESS,
          micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc, mem_blk_handle, real_add));
      ASSERT_EQ(true, real_add);
    }
  }
  ASSERT_EQ(true, mem_blk_handle()->is_completed());

  ASSERT_EQ(true, mem_blk_handle.is_valid());
  const int64_t block_offset = 2 * block_size; // the 3rd phy_block
  const uint32_t blk_reuse_version = 1;
  int64_t updated_micro_size = 0;
  int64_t updated_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS,
      micro_meta_mgr.update_micro_block_meta(
          mem_blk_handle, block_offset, blk_reuse_version, updated_micro_size, updated_micro_cnt));
  ASSERT_EQ((micro_cnt - 1) * micro_size, updated_micro_size);
  ASSERT_EQ(micro_cnt - 1, updated_micro_cnt);

  for (int64_t j = 0; j < micro_cnt; ++j) {
    ObSSMicroBlockMetaHandle micro_handle;
    uint32_t micro_crc = 0;
    int32_t offset = 1 + j * micro_size;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    if (j == micro_cnt / 2) {
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr.micro_meta_map_.get(&micro_key, micro_handle)) << "j: " << j;
    } else {
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&micro_key, micro_handle)) << "j: " << j;
      ASSERT_EQ(true, micro_handle.is_valid()) << "j: " << j;
      ASSERT_EQ(true, micro_handle()->is_persisted()) << "j: " << j;
    }
  }
}

TEST_F(TestSSMicroMetaManager, micro_meta_manager)
{
  LOG_INFO("TEST_CASE: start micro_meta_manager");
  const int64_t block_size = 2 * 1024 * 1024; // 2MB
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  const int64_t max_macro_block_cnt = 100;
  const int64_t max_micro_block_cnt = 100;
  const int64_t micro_size = 1024;
  const int64_t cache_limit_size = max_macro_block_cnt * block_size;

  ObConcurrentFIFOAllocator &allocator = MTL(ObSSMicroCache *)->get_allocator();
  ObSSMicroCacheStat cache_stat;
  ObSSMicroMetaManager micro_meta_mgr(cache_stat);
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.init(tenant_id, block_size, cache_limit_size, allocator));

  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  ObSSMemBlockPool &mem_blk_pool = mem_data_mgr.mem_block_pool_;
  ObSSMemBlock *mem_blk_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_blk_ptr, true/*is_fg*/));
  ASSERT_NE(nullptr, mem_blk_ptr);

  for (int64_t i = 0; i < max_macro_block_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    for (int64_t j = 0; j < max_micro_block_cnt; ++j) {
      const int64_t offset = 1 + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMemMicroInfo mem_info(1, micro_size);
      uint32_t micro_crc = 0;
      ASSERT_EQ(OB_SUCCESS, mem_blk_ptr->micro_offset_map_.set_refactored(micro_key, mem_info));
      ObSSMemBlockHandle mem_blk_handle;
      mem_blk_handle.set_ptr(mem_blk_ptr);
      bool real_add = false;
      ASSERT_EQ(OB_SUCCESS,
          micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc, mem_blk_handle, real_add));
      ASSERT_EQ(true, real_add);
    }
  }

  int64_t i = 11; int64_t j = 20;
  MacroBlockId tmp_macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
  const int64_t tmp_offset = 1 + j * micro_size;
  ObSSMicroBlockCacheKey tmp_micro_key = TestSSCommonUtil::gen_phy_micro_key(tmp_macro_id, tmp_offset, micro_size);

  ObSSMicroBlockMeta *tmp_micro_meta = nullptr;
  {
    ObSSMicroSnapshotInfo tmp_micro_snapshot_info;
    ObSSMicroBlockMetaHandle tmp_micro_handle;
    ObSSMemBlockHandle tmp_mem_blk_handle;
    ObSSPhysicalBlockHandle tmp_phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(tmp_micro_key, tmp_micro_snapshot_info,
              tmp_micro_handle, tmp_mem_blk_handle, tmp_phy_blk_handle, true));
    ASSERT_EQ(true, tmp_micro_handle.is_valid());
    ASSERT_EQ(false, tmp_micro_handle.get_ptr()->is_persisted_);
    ASSERT_EQ(false, tmp_micro_handle.get_ptr()->is_in_l1_);
    ASSERT_EQ(true, tmp_micro_snapshot_info.is_valid());
    ASSERT_EQ(true, tmp_micro_snapshot_info.is_in_l1_);
    ASSERT_EQ(false, tmp_micro_snapshot_info.is_in_ghost_);
    ASSERT_EQ(micro_size, tmp_micro_snapshot_info.size_);
    ASSERT_EQ(false, tmp_micro_snapshot_info.is_persisted_);
    tmp_micro_meta = tmp_micro_handle.get_ptr();
    tmp_micro_meta->is_reorganizing_ = 1;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.force_unmark_reorganizing(tmp_micro_key));
    ASSERT_EQ(0, tmp_micro_meta->is_reorganizing_);
  }

  // mock evict
  tmp_micro_meta->is_persisted_ = true;
  tmp_micro_meta->is_in_ghost_ = true;
  const int64_t tmp_data_dest = micro_meta_mgr.block_size_ * 5 + 20;
  tmp_micro_meta->data_dest_ = tmp_data_dest;
  ASSERT_EQ(true, tmp_micro_meta->is_valid_field());

  // fail to get it, cuz ghost micro_meta should be invalid_field.
  {
    ObSSMicroSnapshotInfo tmp_micro_snapshot_info;
    ObSSMicroBlockMetaHandle tmp_micro_handle;
    ObSSMemBlockHandle tmp_mem_blk_handle;
    ObSSPhysicalBlockHandle tmp_phy_blk_handle;
    ASSERT_EQ(OB_ERR_UNEXPECTED, micro_meta_mgr.get_micro_block_meta_handle(tmp_micro_key, tmp_micro_snapshot_info,
              tmp_micro_handle, tmp_mem_blk_handle, tmp_phy_blk_handle, true));
  }
}

/* Test multiple threads add micro meta into cacheMap in parallel and perform read verification. */
TEST_F(TestSSMicroMetaManager, test_parallel_add_and_get_micro_meta)
{
  LOG_INFO("TEST_CASE: start test_parallel_add_and_get_micro_meta");
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  ObSSMemBlockPool &mem_blk_pool = mem_data_mgr.mem_block_pool_;
  ObSSMemBlock *mem_blk_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_blk_ptr, true/*is_fg*/));
  ASSERT_NE(nullptr, mem_blk_ptr);

  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  TestSSMicroMetaManager::TestSSMicroMetaMgrThread threads(ObTenantEnv::get_tenant(),
      &micro_meta_mgr,
      mem_blk_ptr,
      TestSSMicroMetaMgrThread::TestParallelType::TEST_PARALLEL_ADD_AND_GET_MICRO_META);
  threads.set_thread_count(10);
  threads.start();
  threads.wait();

  ASSERT_EQ(0, threads.get_fail_cnt());
}

TEST_F(TestSSMicroMetaManager, test_parallel_add_same_micro_meta)
{
  const int32_t micro_size = 100;
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  ObSSMemBlockPool &mem_blk_pool = mem_data_mgr.mem_block_pool_;
  ObSSMemBlock *mem_blk_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_blk_ptr, true/*is_fg*/));
  ASSERT_NE(nullptr, mem_blk_ptr);

  // need to add micro_key into mem_block, cuz if micro_meta already exist in micro_meta map, we will
  // check this micro_meta 'is_consistent'.
  MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100);
  int32_t offset = 100;
  ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
  ObSSMemMicroInfo mem_info(1, micro_size);
  ASSERT_EQ(OB_SUCCESS, mem_blk_ptr->micro_offset_map_.set_refactored(micro_key, mem_info));
  ObSSMicroMetaManager *micro_meta_mgr = &(MTL(ObSSMicroCache *)->micro_meta_mgr_);
  const int64_t ori_l1_size = micro_meta_mgr->arc_info_.get_l1_size();
  const int64_t ori_l2_size = micro_meta_mgr->arc_info_.get_l2_size();

  TestSSMicroMetaManager::TestSSMicroMetaMgrThread threads(ObTenantEnv::get_tenant(),
      micro_meta_mgr,
      mem_blk_ptr,
      TestSSMicroMetaMgrThread::TestParallelType::TEST_PARALLEL_ADD_SAME_MICRO_META);
  threads.set_thread_count(10);
  threads.start();
  threads.wait();

  ASSERT_EQ(0, threads.get_fail_cnt());
  ASSERT_EQ(10, threads.get_exe_cnt());
  ASSERT_EQ(0, micro_meta_mgr->arc_info_.get_l1_size() - ori_l1_size);
  ASSERT_EQ(micro_size, micro_meta_mgr->arc_info_.get_l2_size() - ori_l2_size);
}

TEST_F(TestSSMicroMetaManager, test_clear_micro_meta_by_tablet_id)
{
  LOG_INFO("TEST_CASE: start test_clear_micro_meta_by_tablet_id");
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache *)->cache_stat_;
  ObArenaAllocator allocator;
  char *write_buf = nullptr;

  const int64_t macro_cnt = 10;
  const int64_t micro_cnt = 20;
  const int64_t block_size = 2 * 1024 * 1024;
  const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                 ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int64_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int64_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
  write_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, write_buf);
  MEMSET(write_buf, 'a', micro_size);

  for (int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id(0, 1000 + i, 0);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      uint32_t micro_crc = 0;
      int32_t offset = 1 + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMemBlockHandle mem_blk_handle;
      ASSERT_EQ(OB_SUCCESS, mem_data_mgr.add_micro_block_data(micro_key, write_buf, micro_size, mem_blk_handle, micro_crc));
      ASSERT_EQ(true, mem_blk_handle.is_valid());
      bool real_add = false;
      ASSERT_EQ(OB_SUCCESS,
          micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc, mem_blk_handle, real_add));
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(macro_cnt * micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(macro_cnt * micro_cnt * micro_size, cache_stat.micro_stat().total_micro_size_);

  MacroBlockId macro_id1(0, 1000, 0);
  ObTabletID tablet_id1;
  tablet_id1 = macro_id1.second_id();
  int64_t remain_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.clear_tablet_micro_meta(tablet_id1, remain_micro_cnt));
  ASSERT_EQ(0, remain_micro_cnt);
  ASSERT_EQ(macro_cnt * micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ((macro_cnt - 1) * micro_cnt, cache_stat.micro_stat().valid_micro_cnt_);
  ASSERT_EQ((macro_cnt - 1) * micro_cnt * micro_size, cache_stat.micro_stat().valid_micro_size_);

  MacroBlockId macro_id2(0, 1009, 0);
  ObTabletID tablet_id2;
  tablet_id2 = macro_id2.second_id();
  remain_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.clear_tablet_micro_meta(tablet_id2, remain_micro_cnt));
  ASSERT_EQ(0, remain_micro_cnt);
  ASSERT_EQ((macro_cnt - 2) * micro_cnt, cache_stat.micro_stat().valid_micro_cnt_);
  ASSERT_EQ((macro_cnt - 2) * micro_cnt * micro_size, cache_stat.micro_stat().valid_micro_size_);

  for (int64_t i = 0; i < micro_cnt; ++i) {
    int32_t offset = 1 + i * micro_size;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id1, offset, micro_size);
    ObSSMicroBlockMetaHandle micro_handle;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_handle, false));
  }
  for (int64_t i = 0; i < micro_cnt; ++i) {
    int32_t offset = 1 + i * micro_size;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id2, offset, micro_size);
    ObSSMicroBlockMetaHandle micro_handle;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_handle, false));
  }
  MacroBlockId macro_id3(0, 1002, 0);
  for (int64_t i = 0; i < micro_cnt; ++i) {
    int32_t offset = 1 + i * micro_size;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id3, offset, micro_size);
    ObSSMicroBlockMetaHandle micro_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_handle, false));
    // mock some micro_meta is reorganizing
    if (i < micro_cnt / 2) {
      micro_handle()->is_reorganizing_ = true;
    }
  }
  ObTabletID tablet_id3;
  tablet_id3 = macro_id3.second_id();
  remain_micro_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.clear_tablet_micro_meta(tablet_id3, remain_micro_cnt));
  ASSERT_EQ(micro_cnt / 2, remain_micro_cnt);
  ASSERT_EQ((macro_cnt - 3) * micro_cnt + micro_cnt / 2, cache_stat.micro_stat().valid_micro_cnt_);
}

// check that arc_info.p_ does not exceed arc_info.max_p_
TEST_F(TestSSMicroMetaManager, test_arc_limit_p)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_arc_limit_p");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 28))); // 256MB
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());

  ObArenaAllocator allocator;
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSARCInfo &arc_info = micro_cache->micro_meta_mgr_.arc_info_;
  const int64_t origin_p = arc_info.p_;
  const int32_t block_size = phy_blk_mgr.block_size_;
  const int64_t macro_blk_cnt = phy_blk_mgr.blk_cnt_info_.data_blk_.max_cnt_;

  int64_t start_macro_id = 1;
  const int64_t micro_size = (1L << 16);  // 64K
  char *read_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, read_buf);
  ObArray<TestSSCommonUtil::MicroBlockInfo> micro_block_info_arr;
  for (int64_t epoch = 0 ; epoch < 2; epoch++) {
    // 1. write some macro_blocks to object_storage
    micro_block_info_arr.reuse();
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::prepare_micro_blocks(
            macro_blk_cnt, block_size, micro_block_info_arr, start_macro_id, false, micro_size, micro_size));
    ASSERT_LT(0, micro_block_info_arr.count());

    start_macro_id += macro_blk_cnt;

    // 2. make half micro_blocks go into T1 and the other half go into T2. At the same time, some micro_blocks will be
    // evicted into B1 and B2.
    for (int64_t i = 0; i < micro_block_info_arr.count(); ++i) {
      TestSSCommonUtil::MicroBlockInfo &cur_info = micro_block_info_arr.at(i);
      ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::get_micro_block(cur_info, read_buf));
      if (i & 1) {
        ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::get_micro_block(cur_info, read_buf));
      }
    }
  }

  ASSERT_EQ(origin_p, arc_info.p_);
  const int64_t B1_size = arc_info.seg_info_arr_[ARC_B1].size();
  ASSERT_LT(arc_info.max_p_ - arc_info.p_, B1_size);

  // 3. collect all micro_blocks in B1
  micro_block_info_arr.reuse();
  ObSSMicroMetaManager::SSMicroMap &micro_map = micro_cache->micro_meta_mgr_.micro_meta_map_;
  ObSSMicroMetaManager::SSMicroMap::BlurredIterator micro_iter_(micro_map);
  micro_iter_.rewind();
  while (OB_SUCC(ret)) {
    const ObSSMicroBlockCacheKey *micro_key = nullptr;
    ObSSMicroBlockMetaHandle micro_handle;
    if (OB_FAIL(micro_iter_.next(micro_key, micro_handle))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else if (micro_handle()->is_in_l1() && micro_handle()->is_in_ghost()) {
      const ObSSMicroBlockId &micro_id = micro_handle()->micro_key().micro_id_;
      ASSERT_EQ(OB_SUCCESS, micro_block_info_arr.push_back(TestSSCommonUtil::MicroBlockInfo(micro_id)));
    }
  }

  // 4. p_ will get larger when req hit micro_block in B1
  for (int64_t i = 0; i < micro_block_info_arr.count(); ++i) {
    TestSSCommonUtil::MicroBlockInfo &cur_info = micro_block_info_arr.at(i);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::get_micro_block(cur_info, read_buf));
  }

  // 5. check that p does not exceed p_max
  ASSERT_LE(arc_info.p_, arc_info.max_p_);
  ASSERT_LE(arc_info.max_p_ - arc_info.p_, micro_size * 1);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_meta_manager.log*");
  OB_LOGGER.set_file_name("test_ss_micro_meta_manager.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}